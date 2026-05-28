"""ask_multi_choice reports unrecognised tokens instead of dropping them.

"1,3,tabl" used to quietly run on 1 and 3 only — typo'd / ambiguous
tokens were silently dropped, so a partial selection looked like a full
one. Now the ignored tokens are surfaced.
"""

from __future__ import annotations

import pytest

import amx.utils.console as console


def test_match_token_resolves_and_rejects() -> None:
    ch = ["alpha", "beta", "gamma"]
    assert console._match_choice_token("1", ch) == "alpha"
    assert console._match_choice_token("beta", ch) == "beta"
    assert console._match_choice_token("gam", ch) == "gamma"  # unique prefix
    assert console._match_choice_token("zzz", ch) is None


def test_unmatched_tokens_are_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    ch = ["alpha", "beta", "gamma"]
    monkeypatch.setattr(console, "_safe_pt_prompt", lambda *a, **k: "1,3,tabl")
    warns: list[str] = []
    monkeypatch.setattr(console, "warn", lambda m: warns.append(m))
    out = console.ask_multi_choice("pick", ch)
    assert out == ["alpha", "gamma"]
    assert warns and "tabl" in warns[0]


def test_all_matched_no_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    ch = ["alpha", "beta"]
    monkeypatch.setattr(console, "_safe_pt_prompt", lambda *a, **k: "1,2")
    warns: list[str] = []
    monkeypatch.setattr(console, "warn", lambda m: warns.append(m))
    out = console.ask_multi_choice("pick", ch)
    assert out == ["alpha", "beta"]
    assert warns == []
