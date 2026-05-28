"""A large /run is gated behind a confirmation before it spends tokens.

A Database / wide-scope run could fan dozens-to-hundreds of assets
through the LLM with no upfront "proceed?". Large *interactive* runs now
confirm first; small runs and non-interactive callers (scheduler, Studio
worker, piped input) run straight through.
"""

from __future__ import annotations

import types

import pytest

import amx.cli_support.commands.analyze_flow as af


def test_gate_fires_only_for_large_interactive_runs() -> None:
    big = af._LARGE_RUN_ASSET_THRESHOLD
    assert af._should_confirm_large_run(big, interactive=True) is True
    assert af._should_confirm_large_run(big + 100, interactive=True) is True
    # small run → no nag
    assert af._should_confirm_large_run(big - 1, interactive=True) is False
    # non-interactive (scheduler / Studio / pipe) → never blocked
    assert af._should_confirm_large_run(big + 100, interactive=False) is False


def _fake_cfg() -> types.SimpleNamespace:
    return types.SimpleNamespace(llm=types.SimpleNamespace(provider="openai", model="gpt-5.4-mini"))


def test_confirm_proceeds_on_yes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(af, "warn", lambda _m: None)
    monkeypatch.setattr(af, "confirm", lambda *_a, **_k: True)
    assert af._confirm_large_run(300, 4, _fake_cfg()) is True


def test_confirm_aborts_on_no(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}
    monkeypatch.setattr(af, "warn", lambda m: seen.setdefault("warn", m))
    monkeypatch.setattr(af, "confirm", lambda *_a, **k: k.get("default", False))
    assert af._confirm_large_run(300, 4, _fake_cfg()) is False
    # default must be No (a stray Enter doesn't start a 300-asset run)
    assert "300 assets" in str(seen.get("warn", ""))
