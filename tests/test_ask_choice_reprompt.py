"""``ask_choice`` re-prompts on unrecognised input.

It also gates DB writeback in the review loop, where silently accepting
the top suggestion on a typo (e.g. "q"/"w") was a data-safety footgun.
Empty input still accepts the default; valid input returns immediately;
unrecognised input re-prompts up to a retry cap, then falls back.
"""

from __future__ import annotations

import pytest

import amx.utils.console as console


def test_invalid_then_valid_reprompts(monkeypatch: pytest.MonkeyPatch) -> None:
    inputs = iter(["nope", "2"])
    monkeypatch.setattr(console, "_safe_pt_prompt", lambda *a, **k: next(inputs))
    assert console.ask_choice("pick", ["a", "b", "c"], default="a") == "b"


def test_empty_accepts_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(console, "_safe_pt_prompt", lambda *a, **k: "")
    assert console.ask_choice("pick", ["a", "b"], default="b") == "b"


def test_label_match_still_works(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(console, "_safe_pt_prompt", lambda *a, **k: "b")
    assert console.ask_choice("pick", ["a", "b"], default="a") == "b"


def test_exhausted_retries_falls_back_to_default(monkeypatch: pytest.MonkeyPatch) -> None:
    # A non-interactive caller that keeps yielding the same invalid value
    # must not loop forever — it falls back to the default after the cap.
    calls = {"n": 0}

    def _always_garbage(*a: object, **k: object) -> str:
        calls["n"] += 1
        return "garbage"

    monkeypatch.setattr(console, "_safe_pt_prompt", _always_garbage)
    assert console.ask_choice("pick", ["a", "b"], default="a") == "a"
    # Bounded, not infinite.
    assert calls["n"] == console._ASK_CHOICE_MAX_RETRIES
