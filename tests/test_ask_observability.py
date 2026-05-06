"""Observability + hardening tests for PR ask-D.

Verify ToolAgentResult carries the multi-profile observability fields,
the system prompt's schema-hint truncation kicks in only when needed,
and the focus-profile heuristic returns ``None`` for low-signal inputs.
"""

from __future__ import annotations

from amx.config import AMXConfig
from amx.search.tool_agent import (
    ToolAgentResult,
    _agent_system_prompt,
    _compute_focus_profile,
)


def test_tool_agent_result_carries_observability_fields() -> None:
    result = ToolAgentResult(
        answer="hi",
        tool_calls=[],
        iterations=1,
        usage={"prompt_tokens": 10},
        finish_reason="stop",
        scope_profiles=["a", "b"],
        focus_profile="a",
        total_latency_ms=2_500,
        per_tool_latency_ms={"list_schemas": 250, "search_tables": 800},
    )
    assert result.scope_profiles == ["a", "b"]
    assert result.focus_profile == "a"
    assert result.total_latency_ms == 2_500
    assert result.per_tool_latency_ms == {"list_schemas": 250, "search_tables": 800}
    payload = result.as_dict()
    assert payload["scope_profiles"] == ["a", "b"]
    assert payload["focus_profile"] == "a"
    assert payload["total_latency_ms"] == 2_500
    assert payload["per_tool_latency_ms"] == {
        "list_schemas": 250,
        "search_tables": 800,
    }


def test_tool_agent_result_default_observability_empty() -> None:
    """Backwards-compat: pre-PR-D callers don't pass the new kwargs."""
    result = ToolAgentResult(
        answer="hi",
        tool_calls=[],
        iterations=0,
        usage={},
        finish_reason=None,
    )
    assert result.scope_profiles is None
    assert result.focus_profile is None
    assert result.total_latency_ms is None
    assert result.per_tool_latency_ms == {}


def test_compute_focus_profile_finds_dominant_mention() -> None:
    """When ≥60% of recent mentions are one profile, the heuristic
    returns it as the soft focus."""
    history = [
        {"role": "assistant", "content": "Looking at SAP profile, SAP has 5 tables"},
        {"role": "assistant", "content": "On SAP, the customer table is large"},
        {"role": "user", "content": "give me more details"},
    ]
    focus = _compute_focus_profile(history, ["SAP", "warehouse", "raw"])
    assert focus == "SAP"


def test_compute_focus_profile_no_dominance_returns_none() -> None:
    history = [
        {"role": "assistant", "content": "SAP has X. warehouse has Y. raw has Z."},
        {"role": "assistant", "content": "warehouse and raw both have Z too."},
    ]
    # Mentions are spread out; no single profile dominates.
    focus = _compute_focus_profile(history, ["SAP", "warehouse", "raw"])
    assert focus is None


def test_compute_focus_profile_low_total_returns_none() -> None:
    """Too few mentions overall means the heuristic shouldn't bias."""
    history = [
        {"role": "assistant", "content": "I checked something."},
    ]
    focus = _compute_focus_profile(history, ["SAP", "warehouse"])
    assert focus is None


def test_compute_focus_profile_single_scope_returns_none() -> None:
    """Single-profile scope: focus is implicit, helper returns None."""
    history = [
        {"role": "assistant", "content": "SAP SAP SAP"},
    ]
    focus = _compute_focus_profile(history, ["SAP"])
    assert focus is None


def test_system_prompt_schema_hint_truncates_at_50() -> None:
    cfg = AMXConfig()
    long_schemas = [f"schema_{i}" for i in range(120)]
    prompt = _agent_system_prompt(cfg, long_schemas)
    # The full list isn't dumped — we cap at 50 with a "more" hint.
    assert "schema_0," in prompt
    assert "schema_49," in prompt
    assert "schema_119" not in prompt
    assert "70 more" in prompt


def test_system_prompt_schema_hint_full_when_short() -> None:
    cfg = AMXConfig()
    short_schemas = [f"s_{i}" for i in range(10)]
    prompt = _agent_system_prompt(cfg, short_schemas)
    # All 10 included, no truncation hint.
    for name in short_schemas:
        assert name in prompt
    assert "more — call list_schemas" not in prompt


def test_system_prompt_multi_profile_block_present() -> None:
    cfg = AMXConfig()
    prompt = _agent_system_prompt(
        cfg,
        ["public"],
        scope_profiles=["alpha", "beta"],
        focus_profile="alpha",
    )
    assert "MULTI-PROFILE MODE" in prompt
    assert "find_joinable_across_profiles" in prompt
    assert "CONVERSATION FOCUS" in prompt
    assert "**alpha**" in prompt


def test_system_prompt_no_multi_profile_block_when_single() -> None:
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"], scope_profiles=["alpha"])
    assert "MULTI-PROFILE MODE" not in prompt
