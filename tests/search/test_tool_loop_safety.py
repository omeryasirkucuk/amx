"""Defensive guardrails on the /ask tool loop.

Two failure modes used to bring the loop down without graceful
degradation:

1. The `messages` list accumulated unbounded across iterations and
   eventually exceeded the LLM's context window; the failure surfaced
   as an opaque provider error mid-stream rather than a clear
   truncation event.
2. A single tool raising during `ToolBox.invoke` crashed the whole
   loop because the call site had no `try`/`except`. A transient DB
   blip would kill the `/ask` turn for the user.

This module pins the safety net in place: budget enforcement truncates
oldest tool results in place, and tool exceptions surface to the LLM
as structured error envelopes so it can recover (try a different
tool, or compose an answer that admits the failure).
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest


def test_enforce_input_token_budget_no_op_when_under_budget() -> None:
    """A small `messages` list passes through untouched — the safety
    guard must add zero overhead on the happy path."""
    import amx.search.tool_agent as ta

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": "system prompt"},
        {"role": "user", "content": "hi"},
        {"role": "tool", "tool_call_id": "c1", "content": '{"matches": []}'},
    ]
    snapshot = [dict(m) for m in messages]

    truncated = ta._enforce_input_token_budget(messages, budget=1_000_000)

    assert truncated is False
    assert messages == snapshot


def test_enforce_input_token_budget_truncates_oldest_tool_first() -> None:
    """When over budget, oldest tool result is replaced first; newer
    tool results stay intact so the model still sees its latest
    evidence."""
    import amx.search.tool_agent as ta

    big_payload = json.dumps({"rows": ["x" * 100 for _ in range(2000)]})
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": "system prompt"},
        {"role": "user", "content": "question"},
        {"role": "tool", "tool_call_id": "c1", "content": big_payload},
        {"role": "tool", "tool_call_id": "c2", "content": '{"small": true}'},
    ]

    truncated = ta._enforce_input_token_budget(messages, budget=100)

    assert truncated is True
    # Oldest tool result was truncated.
    assert messages[2]["content"] == ta._TRUNCATED_TOOL_PAYLOAD
    # Non-tool messages untouched.
    assert messages[0]["content"] == "system prompt"
    assert messages[1]["content"] == "question"


def test_enforce_input_token_budget_idempotent_on_already_truncated() -> None:
    """Calling the guard again after truncation must not re-mutate
    placeholders or report a fresh truncation — protects callers that
    invoke it every iteration."""
    import amx.search.tool_agent as ta

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": "system"},
        {"role": "tool", "tool_call_id": "c1", "content": ta._TRUNCATED_TOOL_PAYLOAD},
    ]

    first = ta._enforce_input_token_budget(messages, budget=1_000_000)
    second = ta._enforce_input_token_budget(messages, budget=1_000_000)

    assert first is False
    assert second is False


def _stub_llm_response(content: str, tool_calls: list[Any] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        content=content,
        tool_calls=tool_calls or [],
        finish_reason="stop",
        usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        thinking_content="",
    )


class _RaisingToolbox:
    """Stand-in `ToolBox` whose `invoke` always raises.

    Mirrors the minimal surface :mod:`amx.search.tool_agent` reads
    (`db_profiles`, `available_schemas`, `_live_db`, the context-
    manager protocol, and `invoke`) without pulling in a real catalog
    or DB connector.
    """

    @staticmethod
    def schemas() -> list[Any]:
        return []

    def available_schemas(self) -> list[Any]:
        return self.schemas()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.db_profiles: list[str] = ["p1"]
        self.db_profile: str = "p1"

    def __enter__(self) -> _RaisingToolbox:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def _live_db(self) -> MagicMock:
        return MagicMock(list_schemas=lambda: [])

    def invoke(self, name: str, args: str) -> str:
        raise RuntimeError(f"synthetic failure in {name}")


def _build_cfg() -> MagicMock:
    return MagicMock(
        db=SimpleNamespace(catalog="", backend="postgresql", database="", project=""),
        llm=SimpleNamespace(language="english", model="x"),
        active_db_profile="p1",
        active_llm_profile="default",
        current_schema=None,
        current_table=None,
        db_profiles={},
    )


def test_tool_invoke_exception_surfaces_as_structured_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tool raising during invoke must not crash the loop. The
    error reaches the LLM as a JSON envelope on the tool result
    message, the next iteration produces the final answer, and the
    public result includes the failed tool call's metadata."""
    import amx.search.tool_agent as ta

    monkeypatch.setattr(ta, "ToolBox", _RaisingToolbox)

    tool_call = SimpleNamespace(id="c1", name="search_tables_by_concept", arguments="{}")
    chat_calls: list[list[dict[str, Any]]] = []
    responses = [
        _stub_llm_response("", tool_calls=[tool_call]),
        _stub_llm_response("recovered answer"),
    ]
    fake_llm = MagicMock()

    def _fake_chat(messages: list[dict[str, Any]], **kwargs: Any) -> SimpleNamespace:
        chat_calls.append([dict(m) for m in messages])
        return responses[len(chat_calls) - 1]

    fake_llm.chat.side_effect = _fake_chat

    result = ta.run_tool_agent(
        cfg=_build_cfg(),
        catalog=MagicMock(),
        llm=fake_llm,
        question="anything",
        answer_language="english",
        session_memory=None,
    )

    # The loop survived the synthetic exception.
    assert result.answer == "recovered answer"

    # The synthesis round saw a tool message carrying the structured
    # error envelope — the LLM has enough context to choose recovery
    # behavior rather than being told nothing.
    second_round = chat_calls[1]
    tool_msgs = [m for m in second_round if m.get("role") == "tool"]
    assert len(tool_msgs) == 1
    envelope = json.loads(tool_msgs[0]["content"])
    assert envelope["tool_name"] == "search_tables_by_concept"
    assert envelope["category"] == "transient"
    assert "synthetic failure" in envelope["error"]

    # The tool-call log records the call so observability stays honest;
    # the result_preview reflects the error envelope, not a missing
    # entry.
    assert len(result.tool_calls) == 1
    assert "synthetic failure" in result.tool_calls[0]["result_preview"]


class _ValueErrorToolbox(_RaisingToolbox):
    def invoke(self, name: str, args: str) -> str:
        raise ValueError("bad arguments")


def test_tool_invoke_value_error_marked_permanent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Argument-shape errors are tagged `permanent` so the LLM doesn't
    waste an iteration retrying the same call."""
    import amx.search.tool_agent as ta

    monkeypatch.setattr(ta, "ToolBox", _ValueErrorToolbox)

    tool_call = SimpleNamespace(id="c1", name="describe_table", arguments="{}")
    responses = [
        _stub_llm_response("", tool_calls=[tool_call]),
        _stub_llm_response("admitted failure"),
    ]
    fake_llm = MagicMock()
    fake_llm.chat.side_effect = responses

    result = ta.run_tool_agent(
        cfg=_build_cfg(),
        catalog=MagicMock(),
        llm=fake_llm,
        question="describe x",
        answer_language="english",
        session_memory=None,
    )

    assert result.answer == "admitted failure"
    envelope = json.loads(result.tool_calls[0]["result_preview"].rstrip("…"))
    assert envelope["category"] == "permanent"
