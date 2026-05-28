"""The ASK composer chips reach the tool-agent system prompt.

``run_tool_agent`` accepts ``doc_profiles`` / ``code_profiles`` /
``lineage_profiles`` / ``pages_enabled`` / ``asset_kinds`` and previously
used them only as ToolBox retrieval filters — they never reached the
system prompt, so the LLM could not answer "what did I select". This
test stubs the LLM + ToolBox, runs one round, and asserts the captured
system message renders the SELECTION CONTEXT for the forwarded chips.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


def _stub_llm_response(content: str) -> SimpleNamespace:
    return SimpleNamespace(
        content=content,
        tool_calls=[],
        finish_reason="stop",
        usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        thinking_content="",
    )


class _FakeToolbox:
    @staticmethod
    def schemas() -> list[Any]:
        return []

    def available_schemas(self) -> list[Any]:
        return self.schemas()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.db_profiles: list[str] = ["bird-pg"]
        self.db_profile: str = "bird-pg"

    def __enter__(self) -> _FakeToolbox:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def _live_db(self) -> MagicMock:
        return MagicMock(list_schemas=lambda: [])

    def invoke(self, name: str, args: str) -> str:
        return "{}"


def _build_cfg() -> MagicMock:
    return MagicMock(
        db=SimpleNamespace(catalog="", backend="postgresql", database="testdb", project=""),
        llm=SimpleNamespace(language="english", model="x"),
        active_db_profile="bird-pg",
        active_llm_profile="default",
        current_schema=None,
        current_table=None,
        db_profiles={},
    )


def test_chips_render_in_system_prompt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import amx.search.tool_agent as ta

    monkeypatch.setattr(ta, "ToolBox", _FakeToolbox)

    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    monkeypatch.setattr("amx.storage.sqlite_store.history_store", lambda: store)

    captured: dict[str, str] = {}
    fake_llm = MagicMock()

    def _fake_chat(messages: list[dict[str, Any]], **kwargs: Any) -> SimpleNamespace:
        captured["system"] = str(messages[0]["content"])
        return _stub_llm_response("done")

    fake_llm.chat.side_effect = _fake_chat

    result = ta.run_tool_agent(
        cfg=_build_cfg(),
        catalog=MagicMock(),
        llm=fake_llm,
        question="which assets did I select?",
        answer_language="english",
        session_memory=None,
        db_profiles=["bird-pg"],
        doc_profiles=None,
        code_profiles=[],
        lineage_profiles=["customers-canvas"],
        pages_enabled=False,
        asset_kinds=["jobs"],
    )

    system = captured["system"]
    assert "SELECTION CONTEXT" in system
    assert "Docs: Auto (all available)" in system
    assert "Code: Off (disabled for this question)" in system
    assert "Lineage: Custom: customers-canvas" in system
    assert "Pages: Off" in system
    assert "Assets (ingested asset kinds): Custom: jobs" in system
    assert result.answer == "done"
