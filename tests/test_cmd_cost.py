"""Tests for the ``/cost`` CLI command.

Covers the three behaviours of ``cmd_cost``:

* No-args interactive flow: prints the active model's price, then
  opens an ``ask_choice`` picker. Pressing Enter on the active model
  returns silently; selecting another model prints that one's rates.
* Override-set: ``/cost <input> <output>`` writes the per-profile
  override. Regression — the picker rewrite must not break this path.
* Override-reset: ``/cost reset`` clears the override.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from amx.cli_support.commands import profiles as cmd_module
from amx.config import AMXConfig, LLMConfig
from amx.llm import pricing as pricing_mod
from amx.llm.pricing import ModelPrice, reset_state_for_tests


@pytest.fixture()
def cfg_with_active_profile(monkeypatch: pytest.MonkeyPatch) -> AMXConfig:
    """Minimal config with one active LLM profile so cmd_cost has
    something to resolve against. ``cfg.save`` is patched to a no-op
    so the override-set tests don't write to ~/.amx."""
    cfg = AMXConfig()
    profile = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.llm_profiles["unit-test"] = profile
    cfg.active_llm_profile = "unit-test"
    cfg.llm.provider = profile.provider
    cfg.llm.model = profile.model
    monkeypatch.setattr(cfg, "save", lambda: None)
    return cfg


@pytest.fixture(autouse=True)
def _reset_pricing_state():
    reset_state_for_tests()
    yield
    reset_state_for_tests()


def _seed_catalog() -> None:
    """Populate the in-memory price tables with a couple of known
    rows so ``list_all_models`` returns a deterministic set the
    tests can assert against."""
    pricing_mod._ensure_loaded()  # loads bundled fallback (~30 popular models)
    pricing_mod._PRICES["litellm"]["gpt-4o-mini"] = ModelPrice(
        input_per_mtok=0.15,
        output_per_mtok=0.60,
        source="litellm",
        fetched_at=1.0,
    )
    pricing_mod._PRICES["litellm"]["openai/gpt-4o"] = ModelPrice(
        input_per_mtok=2.50,
        output_per_mtok=10.0,
        source="litellm",
        fetched_at=1.0,
    )


def test_cost_no_args_prints_active_then_opens_picker_with_active_default(
    cfg_with_active_profile: AMXConfig, capsys: pytest.CaptureFixture[str]
) -> None:
    """No-args /cost: print active price, then call ask_choice with
    the active model preselected. Pressing Enter (= default) returns
    without printing a second block."""
    _seed_catalog()
    captured_kwargs: dict[str, object] = {}

    def fake_ask_choice(*args, **kwargs):
        captured_kwargs["kwargs"] = kwargs
        captured_kwargs["choices"] = args[1] if len(args) >= 2 else kwargs.get("choices")
        return kwargs.get("default", "")  # simulate "Enter on default"

    with patch.object(cmd_module, "ask_choice", side_effect=fake_ask_choice):
        cmd_module.cmd_cost(cfg_with_active_profile, [])

    out = capsys.readouterr().out
    assert "Cost for 'unit-test' [input  $/Mtok]" in out
    assert "Cost for 'unit-test' [output $/Mtok]" in out
    # Active model's rate (from the seeded litellm cache) appears.
    assert "0.1500" in out and "0.6000" in out
    # The picker received the active model id as the default.
    assert captured_kwargs["kwargs"]["default"] == "gpt-4o-mini"
    # Picker offered the seeded catalog rows.
    assert "gpt-4o-mini" in captured_kwargs["choices"]
    assert "openai/gpt-4o" in captured_kwargs["choices"]
    # No second "Cost for 'gpt-4o-mini'" block (Enter on default = no-op).
    assert out.count("Cost for ") == 2  # two lines for active profile only


def test_cost_no_args_picker_other_model_prints_that_models_rates(
    cfg_with_active_profile: AMXConfig, capsys: pytest.CaptureFixture[str]
) -> None:
    """When the user picks a non-default catalog row, the command
    prints that row's input/output rates with the model id as the
    label."""
    _seed_catalog()

    def fake_ask_choice(*_args, **_kwargs):
        return "openai/gpt-4o"

    with patch.object(cmd_module, "ask_choice", side_effect=fake_ask_choice):
        cmd_module.cmd_cost(cfg_with_active_profile, [])

    out = capsys.readouterr().out
    assert "Cost for 'openai/gpt-4o' [input  $/Mtok]" in out
    assert "Cost for 'openai/gpt-4o' [output $/Mtok]" in out
    assert "2.5000" in out and "10.0000" in out


def test_cost_no_args_picker_cancel_returns_silently(
    cfg_with_active_profile: AMXConfig, capsys: pytest.CaptureFixture[str]
) -> None:
    """Pressing Esc inside the picker raises PromptCancelled. The
    command must catch it and exit cleanly (no traceback to the
    REPL) — the active price was already printed up top."""
    _seed_catalog()

    def fake_ask_choice(*_args, **_kwargs):
        from amx.utils.console import PromptCancelled

        raise PromptCancelled()

    with patch.object(cmd_module, "ask_choice", side_effect=fake_ask_choice):
        cmd_module.cmd_cost(cfg_with_active_profile, [])

    out = capsys.readouterr().out
    assert "Cost for 'unit-test'" in out  # active block printed before cancel
    assert "Traceback" not in out


def test_cost_set_override_still_writes_to_profile(
    cfg_with_active_profile: AMXConfig, capsys: pytest.CaptureFixture[str]
) -> None:
    """Regression: /cost <input> <output> path predates the picker
    and must keep writing the override into the active profile."""
    cmd_module.cmd_cost(cfg_with_active_profile, ["1.50", "5.00"])

    profile = cfg_with_active_profile.llm_profiles["unit-test"]
    assert profile.custom_input_cost_per_mtok == 1.50
    assert profile.custom_output_cost_per_mtok == 5.00
    assert cfg_with_active_profile.llm.custom_input_cost_per_mtok == 1.50
    out = capsys.readouterr().out
    assert "Cost override saved" in out


def test_cost_reset_clears_override(
    cfg_with_active_profile: AMXConfig, capsys: pytest.CaptureFixture[str]
) -> None:
    """Regression: /cost reset must still clear the override fields
    on both the active config and the saved profile."""
    cfg_with_active_profile.llm_profiles["unit-test"].custom_input_cost_per_mtok = 1.50
    cfg_with_active_profile.llm_profiles["unit-test"].custom_output_cost_per_mtok = 5.00
    cfg_with_active_profile.llm.custom_input_cost_per_mtok = 1.50
    cfg_with_active_profile.llm.custom_output_cost_per_mtok = 5.00

    cmd_module.cmd_cost(cfg_with_active_profile, ["reset"])

    profile = cfg_with_active_profile.llm_profiles["unit-test"]
    assert profile.custom_input_cost_per_mtok is None
    assert profile.custom_output_cost_per_mtok is None
    assert cfg_with_active_profile.llm.custom_input_cost_per_mtok is None
    out = capsys.readouterr().out
    assert "Cost override cleared" in out
