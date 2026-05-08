"""Per-run LLM override surface on POST /api/runs.

The Studio "Advanced LLM settings" disclosure on /runs/new and the
CLI's interactive override gate both flow through the same backend
plumbing — a Pydantic ``LLMOverrides`` body field on RunRequest +
:func:`_apply_llm_overrides` deriving a one-shot ``AMXConfig`` via
``dataclasses.replace``. These tests pin the validation + non-mutation
contract: a typo in either path silently shipping a bad config or a
saved profile getting clobbered in-place would be a regression worth
catching here, not in production.
"""

from __future__ import annotations

import dataclasses

import pytest
from pydantic import ValidationError

from amx.config import AMXConfig, LLMConfig
from amx.web.routers.runs import (
    LLMOverrides,
    RunRequest,
    _apply_llm_overrides,
)


def test_run_request_accepts_full_override_payload() -> None:
    """Every LLMOverrides field round-trips when present."""
    body = RunRequest.model_validate(
        {
            "scope": {"sales": ["orders"]},
            "llm_overrides": {
                "temperature": 0.05,
                "max_tokens": 8192,
                "n_alternatives": 4,
                "column_batch_size": 12,
                "prompt_detail": "detailed",
                "description_verbosity": "comprehensive",
                "thinking_budget": 4096,
                "logprob_high": 0.9,
                "logprob_medium": 0.55,
                "custom_input_cost_per_mtok": 1.5,
                "custom_output_cost_per_mtok": 3.0,
            },
        }
    )
    assert body.llm_overrides is not None
    assert body.llm_overrides.temperature == 0.05
    assert body.llm_overrides.prompt_detail == "detailed"
    assert body.llm_overrides.description_verbosity == "comprehensive"
    assert body.llm_overrides.non_null() == {
        "temperature": 0.05,
        "max_tokens": 8192,
        "n_alternatives": 4,
        "column_batch_size": 12,
        "prompt_detail": "detailed",
        "description_verbosity": "comprehensive",
        "thinking_budget": 4096,
        "logprob_high": 0.9,
        "logprob_medium": 0.55,
        "custom_input_cost_per_mtok": 1.5,
        "custom_output_cost_per_mtok": 3.0,
    }


def test_run_request_omitting_overrides_is_legal() -> None:
    """Existing callers that don't know about overrides keep working."""
    body = RunRequest.model_validate({"scope": {"sales": ["orders"]}})
    assert body.llm_overrides is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("temperature", -0.1),
        ("temperature", 2.1),
        ("max_tokens", 100),  # below 256 floor
        ("max_tokens", 999_999),  # above 262_144 cap
        ("n_alternatives", 0),
        ("n_alternatives", 6),
        ("column_batch_size", 0),
        ("column_batch_size", 999),
        ("logprob_high", 1.5),
        ("logprob_medium", -0.1),
        ("custom_input_cost_per_mtok", -1.0),
    ],
)
def test_run_request_rejects_out_of_range_numeric(field: str, value: float) -> None:
    """Pydantic Field constraints catch typos before they reach the worker."""
    with pytest.raises(ValidationError):
        RunRequest.model_validate(
            {
                "scope": {"sales": ["orders"]},
                "llm_overrides": {field: value},
            }
        )


def test_run_request_rejects_unknown_prompt_detail() -> None:
    """The custom validator rejects strings outside the allowed set."""
    with pytest.raises(ValidationError):
        RunRequest.model_validate(
            {
                "scope": {"sales": ["orders"]},
                "llm_overrides": {"prompt_detail": "extreme"},
            }
        )


def test_run_request_rejects_unknown_verbosity() -> None:
    with pytest.raises(ValidationError):
        RunRequest.model_validate(
            {
                "scope": {"sales": ["orders"]},
                "llm_overrides": {"description_verbosity": "lengthy"},
            }
        )


def test_apply_llm_overrides_no_op_when_overrides_none() -> None:
    """Without overrides the helper returns the input cfg unchanged."""
    cfg = AMXConfig()
    cfg.llm = LLMConfig(provider="openai", model="gpt-4o", temperature=0.2)
    derived, applied = _apply_llm_overrides(cfg, None)
    assert derived is cfg
    assert applied == {}


def test_apply_llm_overrides_no_op_when_all_fields_omitted() -> None:
    """An LLMOverrides with every field None must not derive a new cfg."""
    cfg = AMXConfig()
    cfg.llm = LLMConfig(provider="openai", model="gpt-4o", temperature=0.2)
    derived, applied = _apply_llm_overrides(cfg, LLMOverrides())
    assert derived is cfg
    assert applied == {}


def test_apply_llm_overrides_does_not_mutate_saved_profile() -> None:
    """The derived cfg's llm carries the overrides; the original stays
    byte-identical so the saved profile on disk is untouched."""
    cfg = AMXConfig()
    saved_llm = LLMConfig(
        provider="openai",
        model="gpt-4o",
        temperature=0.2,
        max_tokens=16_384,
        n_alternatives=3,
        prompt_detail="standard",
    )
    cfg.llm = saved_llm
    saved_snapshot = dataclasses.replace(saved_llm)

    overrides = LLMOverrides(
        temperature=0.05,
        max_tokens=8192,
        n_alternatives=5,
        prompt_detail="detailed",
    )
    derived_cfg, applied = _apply_llm_overrides(cfg, overrides)

    # Derived carries the new values
    assert derived_cfg is not cfg
    assert derived_cfg.llm.temperature == 0.05
    assert derived_cfg.llm.max_tokens == 8192
    assert derived_cfg.llm.n_alternatives == 5
    assert derived_cfg.llm.prompt_detail == "detailed"

    # Original is untouched — every field equal to the snapshot we
    # took before calling the helper. This is the load-bearing
    # assertion: a future refactor that swaps to in-place mutation
    # (`cfg.llm.temperature = 0.05`) would silently corrupt the
    # saved profile and break this test.
    for field in dataclasses.fields(LLMConfig):
        assert getattr(cfg.llm, field.name) == getattr(saved_snapshot, field.name), (
            f"saved profile field {field.name} was mutated during override apply"
        )

    # The applied dict mirrors only the non-null fields, suitable for
    # audit logging.
    assert applied == {
        "temperature": 0.05,
        "max_tokens": 8192,
        "n_alternatives": 5,
        "prompt_detail": "detailed",
    }


def test_apply_llm_overrides_preserves_unrelated_cfg_fields() -> None:
    """``dataclasses.replace`` only touches ``cfg.llm``; the rest of
    AMXConfig (db_profiles, active_db_profile, history settings, etc.)
    must keep its references."""
    cfg = AMXConfig()
    cfg.llm = LLMConfig(provider="openai", model="gpt-4o")
    cfg.active_db_profile = "prod"
    cfg.active_llm_profile = "claude"
    cfg.current_schema = "sales"

    derived, _ = _apply_llm_overrides(cfg, LLMOverrides(temperature=0.05))
    assert derived.active_db_profile == "prod"
    assert derived.active_llm_profile == "claude"
    assert derived.current_schema == "sales"
    # db_profiles is shared by reference — the override has no reason
    # to copy that big dict.
    assert derived.db_profiles is cfg.db_profiles


def test_apply_llm_overrides_can_clear_cost_overrides() -> None:
    """Cost overrides accept None to "clear" — but the LLMOverrides
    helper only forwards non-null fields, so nullification has to
    travel via a separate path. This documents that today's surface
    treats omitted = preserve, value = override; a "set back to None"
    use case would need an explicit sentinel and is intentionally out
    of scope for the per-run override flow."""
    cfg = AMXConfig()
    cfg.llm = LLMConfig(
        provider="openai",
        model="gpt-4o",
        custom_input_cost_per_mtok=1.0,
        custom_output_cost_per_mtok=2.0,
    )
    derived, applied = _apply_llm_overrides(
        cfg,
        LLMOverrides(custom_input_cost_per_mtok=0.5),
    )
    assert applied == {"custom_input_cost_per_mtok": 0.5}
    assert derived.llm.custom_input_cost_per_mtok == 0.5
    # The output rate the user did NOT touch keeps the saved profile
    # value — proving omitted-means-preserve.
    assert derived.llm.custom_output_cost_per_mtok == 2.0
