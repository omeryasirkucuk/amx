"""CLI interactive override gate for ``amx analyze run``.

The gate hangs off ``_maybe_apply_llm_overrides_interactively`` in
``amx.cli_support.commands.analyze_flow``. It must:

* Skip silently when stdin is not a TTY (CI / pipes shouldn't stall
  on a missed prompt) and return a no-op restore.
* Skip when the user declines the first ``confirm``.
* Walk the user through every field with the saved profile values as
  defaults; only typed-in changes become overrides.
* Mutate ``cfg.llm`` for the duration of the run and return a
  ``restore`` callable that bounces ``cfg.llm`` back to the profile
  values on the way out — no on-disk state ever changes.
"""

from __future__ import annotations

import sys
from typing import Any

import pytest

from amx.cli_support.commands import analyze_flow
from amx.config import AMXConfig, LLMConfig


@pytest.fixture()
def cfg_with_profile() -> AMXConfig:
    cfg = AMXConfig()
    cfg.llm = LLMConfig(
        provider="openai",
        model="gpt-4o",
        temperature=0.2,
        max_tokens=16_384,
        n_alternatives=3,
        column_batch_size=10,
        prompt_detail="standard",
        description_verbosity="brief",
        thinking_budget=1024,
        logprob_high=0.85,
        logprob_medium=0.50,
    )
    return cfg


def test_override_gate_noop_on_non_tty(cfg_with_profile, monkeypatch) -> None:
    """A piped invocation must never stall waiting for input."""
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    saved_llm = cfg_with_profile.llm
    restore, applied = analyze_flow._maybe_apply_llm_overrides_interactively(cfg_with_profile)
    assert applied == {}
    assert cfg_with_profile.llm is saved_llm
    restore()  # no-op
    assert cfg_with_profile.llm is saved_llm


def test_override_gate_noop_when_user_declines(cfg_with_profile, monkeypatch) -> None:
    """User answers ``n`` to the gate → no fields prompted, no
    overrides applied, cfg.llm untouched."""
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(analyze_flow, "confirm", lambda *_a, **_k: False)
    monkeypatch.setattr(
        analyze_flow,
        "ask",
        lambda *_a, **_k: pytest.fail("ask must not be called when user declines"),
    )
    saved_llm = cfg_with_profile.llm
    restore, applied = analyze_flow._maybe_apply_llm_overrides_interactively(cfg_with_profile)
    assert applied == {}
    assert cfg_with_profile.llm is saved_llm
    restore()
    assert cfg_with_profile.llm is saved_llm


def test_override_gate_applies_only_changed_fields(cfg_with_profile, monkeypatch) -> None:
    """User accepts the gate, types a new temperature, accepts every
    other field with Enter. Only the changed field becomes an override
    and ``restore`` puts the saved profile back in place."""
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(analyze_flow, "confirm", lambda *_a, **_k: True)

    # Walk through every numeric prompt: only the temperature row
    # gets a new value; everything else returns the empty string
    # (Enter accepts current). Cost-override prompts also return ""
    # so those stay None.
    answers = iter(
        [
            "0.05",  # temperature
            "",  # max_tokens
            "",  # n_alternatives
            "",  # column_batch_size
            # prompt_detail handled via ask_choice
            # description_verbosity handled via ask_choice
            "",  # thinking_budget
            "",  # logprob_high
            "",  # logprob_medium
            "",  # custom_input_cost_per_mtok
            "",  # custom_output_cost_per_mtok
        ]
    )

    def fake_ask(_question: str, *, default: str = "") -> str:
        try:
            return next(answers)
        except StopIteration:
            return ""

    def fake_ask_choice(_q: str, choices: list[str], *, default: str = "", **_k):
        return default  # accept current

    monkeypatch.setattr(analyze_flow, "ask", fake_ask)
    monkeypatch.setattr(analyze_flow, "ask_choice", fake_ask_choice)

    saved_llm = cfg_with_profile.llm
    restore, applied = analyze_flow._maybe_apply_llm_overrides_interactively(cfg_with_profile)

    assert applied == {"temperature": 0.05}
    assert cfg_with_profile.llm is not saved_llm  # derived dataclass
    assert cfg_with_profile.llm.temperature == 0.05
    # Every other field carries the saved profile's value through
    # ``dataclasses.replace`` — proving the override only changed
    # what the user typed.
    assert cfg_with_profile.llm.max_tokens == saved_llm.max_tokens
    assert cfg_with_profile.llm.prompt_detail == saved_llm.prompt_detail

    restore()
    assert cfg_with_profile.llm is saved_llm  # original instance back


def test_override_gate_out_of_range_keeps_profile_value(cfg_with_profile, monkeypatch) -> None:
    """A typo (temperature = 5) is rejected with a warning rather
    than silently shipping; cfg.llm.temperature stays at the saved
    profile value."""
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(analyze_flow, "confirm", lambda *_a, **_k: True)

    answers = iter(
        [
            "5.0",  # temperature — out of range
            "",  # max_tokens
            "",  # n_alternatives
            "",  # column_batch_size
            "",  # thinking_budget
            "",  # logprob_high
            "",  # logprob_medium
            "",  # custom_input
            "",  # custom_output
        ]
    )
    monkeypatch.setattr(
        analyze_flow,
        "ask",
        lambda *_a, **_k: next(answers, ""),
    )
    monkeypatch.setattr(
        analyze_flow,
        "ask_choice",
        lambda _q, _c, *, default="", **_k: default,
    )

    _, applied = analyze_flow._maybe_apply_llm_overrides_interactively(cfg_with_profile)
    # The bogus 5.0 was discarded — no override slot for temperature.
    assert "temperature" not in applied
    assert cfg_with_profile.llm.temperature == 0.2


def test_override_gate_walks_choice_fields(cfg_with_profile, monkeypatch) -> None:
    """``prompt_detail`` and ``description_verbosity`` are picked from
    a fixed list via ``ask_choice``; a different selection becomes an
    override."""
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(analyze_flow, "confirm", lambda *_a, **_k: True)
    monkeypatch.setattr(
        analyze_flow,
        "ask",
        lambda *_a, **_k: "",  # accept every numeric default
    )

    choice_returns: dict[str, Any] = {
        "Prompt detail": "detailed",
        "Description verbosity": "comprehensive",
    }

    def fake_choice(question: str, choices: list[str], *, default: str = "", **_k):
        for key, value in choice_returns.items():
            if key.lower() in question.lower():
                return value
        return default

    monkeypatch.setattr(analyze_flow, "ask_choice", fake_choice)

    _, applied = analyze_flow._maybe_apply_llm_overrides_interactively(cfg_with_profile)
    assert applied == {
        "prompt_detail": "detailed",
        "description_verbosity": "comprehensive",
    }
    assert cfg_with_profile.llm.prompt_detail == "detailed"
    assert cfg_with_profile.llm.description_verbosity == "comprehensive"
