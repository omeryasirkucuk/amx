"""Pin the additive callback contract on
:func:`amx.search.tool_agent.run_tool_agent`.

PR-D added three optional kwargs (``on_thinking_delta``,
``on_tool_call``, ``cancel_token``) that the visualizer's
``/api/ask`` endpoint plugs into. Tests here exercise them
without spinning up a real LLM by stubbing :class:`LLMProvider.chat`
and the :class:`ToolBox` constructor.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def _stub_llm_response(content: str, tool_calls=None):
    return SimpleNamespace(
        content=content,
        tool_calls=tool_calls or [],
        finish_reason="stop",
        usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        thinking_content="",
    )


class _FakeToolbox:
    """Stand-in for :class:`amx.search.agent_tools.ToolBox`. Carries
    the ``schemas()`` classmethod the agent loop calls and a
    contextmanager interface so ``with ToolBox(...)`` works."""

    @staticmethod
    def schemas():
        return []

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def _live_db(self):
        return MagicMock(list_schemas=lambda: [])

    def invoke(self, name, args):
        return "result-payload"


def _patch_toolbox(monkeypatch) -> None:
    import amx.search.tool_agent as ta

    monkeypatch.setattr(ta, "ToolBox", _FakeToolbox)


def _build_cfg():
    return MagicMock(
        db=SimpleNamespace(
            catalog="",
            backend="postgresql",
            database="",
            project="",
        ),
        llm=SimpleNamespace(language="english", model="x"),
        active_db_profile="default",
        active_llm_profile="default",
        current_schema=None,
        current_table=None,
        db_profiles={},
    )


def test_run_tool_agent_signature_accepts_new_kwargs(monkeypatch) -> None:
    """The kwargs are optional — existing callers (CLI /ask) keep
    working without them."""
    import amx.search.tool_agent as ta

    _patch_toolbox(monkeypatch)
    fake_llm = MagicMock()
    fake_llm.chat.return_value = _stub_llm_response("done")

    result = ta.run_tool_agent(
        cfg=_build_cfg(),
        catalog=MagicMock(),
        llm=fake_llm,
        question="hi",
        answer_language="english",
        session_memory=None,
    )
    assert result.answer == "done"


def test_on_thinking_delta_is_invoked_when_provider_streams(monkeypatch) -> None:
    import amx.search.tool_agent as ta

    _patch_toolbox(monkeypatch)
    captured: list[str] = []

    fake_llm = MagicMock()

    def fake_chat(*args, **kwargs):
        cb = kwargs.get("on_thinking")
        if cb:
            cb("a delta")
        return _stub_llm_response("done")

    fake_llm.chat.side_effect = fake_chat

    ta.run_tool_agent(
        cfg=_build_cfg(),
        catalog=MagicMock(),
        llm=fake_llm,
        question="hi",
        answer_language="english",
        session_memory=None,
        on_thinking_delta=captured.append,
    )
    assert captured == ["a delta"]


def test_cancel_token_raises_run_cancelled_before_chat(monkeypatch) -> None:
    """Setting the token before submission causes the agent to bail
    on the first iteration without making any LLM call."""
    import amx.search.tool_agent as ta

    _patch_toolbox(monkeypatch)

    from amx.agents.orchestrator import RunCancelled

    fake_llm = MagicMock()
    fake_llm.chat.return_value = _stub_llm_response("done")
    cancel = threading.Event()
    cancel.set()

    with pytest.raises(RunCancelled):
        ta.run_tool_agent(
            cfg=_build_cfg(),
            catalog=MagicMock(),
            llm=fake_llm,
            question="hi",
            answer_language="english",
            session_memory=None,
            cancel_token=cancel,
        )
    fake_llm.chat.assert_not_called()


def test_on_tool_call_fires_for_each_returned_tool(monkeypatch) -> None:
    import amx.search.tool_agent as ta

    _patch_toolbox(monkeypatch)

    tool_call = SimpleNamespace(id="call_1", name="list_schemas", arguments="{}")
    fake_llm = MagicMock()
    fake_llm.chat.side_effect = [
        _stub_llm_response("", tool_calls=[tool_call]),
        _stub_llm_response("done"),
    ]

    captured: list[dict[str, str]] = []
    ta.run_tool_agent(
        cfg=_build_cfg(),
        catalog=MagicMock(),
        llm=fake_llm,
        question="hi",
        answer_language="english",
        session_memory=None,
        on_tool_call=captured.append,
    )
    assert len(captured) == 1
    assert captured[0]["name"] == "list_schemas"
    assert captured[0]["arguments"] == "{}"
    assert "result-payload" in captured[0]["result_preview"]
