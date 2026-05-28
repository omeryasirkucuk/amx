"""SELECTION CONTEXT block in the /ask tool-agent system prompt.

The ASK composer chips (Docs / Code / Lineage / Pages / Assets / Scope)
must reach the LLM as ground truth so it can answer "what did I select"
directly and ground retrieval in the picked sources. Each list chip is
tri-state: ``None`` = Auto, ``[]`` = Off, list = custom subset.
"""

from __future__ import annotations

from amx.config import AMXConfig, DBConfig, LLMConfig
from amx.search._tool_agent_prompts import agent_system_prompt


def _render(**kwargs) -> str:
    cfg = AMXConfig(
        db=DBConfig(backend="postgresql", database="testdb"),
        llm=LLMConfig(),
    )
    return agent_system_prompt(
        cfg,
        schema_hint=["public"],
        scope_profiles=["bird-pg"],
        **kwargs,
    )


def test_block_always_rendered_with_auto_states() -> None:
    prompt = _render()
    assert "SELECTION CONTEXT" in prompt
    assert "Docs: Auto (all available)" in prompt
    assert "Code: Auto (all available)" in prompt
    assert "Lineage: Auto (all available)" in prompt
    assert "Pages: Auto" in prompt
    assert "Assets (ingested asset kinds): Auto (all available)" in prompt


def test_off_state_renders() -> None:
    prompt = _render(code_profiles=[])
    assert "Code: Off (disabled for this question)" in prompt


def test_custom_list_renders_names() -> None:
    prompt = _render(asset_kinds=["notebooks", "jobs"])
    assert "Assets (ingested asset kinds): Custom: notebooks, jobs" in prompt


def test_pages_bool_states() -> None:
    assert "Pages: On" in _render(pages_enabled=True)
    assert "Pages: Off" in _render(pages_enabled=False)


def test_scope_row_reflects_scope_profiles() -> None:
    prompt = _render()
    assert "Scope (DB profiles): Custom: bird-pg" in prompt


def test_long_custom_list_is_capped() -> None:
    many = [f"p{i}" for i in range(20)]
    prompt = _render(doc_profiles=many)
    assert "… (+8 more)" in prompt  # 20 names, cap 12


def test_anti_routing_guard_present() -> None:
    prompt = _render()
    assert "Do NOT call list_past_runs or list_chat_sessions" in prompt
