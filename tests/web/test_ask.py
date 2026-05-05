"""Ask-router tests — patches ``run_tool_agent`` so the suite never
calls a real LLM. Exercises the SSE event flow, sessions endpoints,
and the additive callback contract on ``run_tool_agent``."""

from __future__ import annotations

import json
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from amx.web.routers import ask as ask_router


def _wait_for_status(client, job_id: str, target: str, timeout: float = 3.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(
            f"/api/ask/{job_id}",
            headers={"Authorization": "Bearer test-studio-token-abc123"},
        )
        body = resp.json()
        if body["status"] == target:
            return body
        time.sleep(0.05)
    raise AssertionError(f"Job {job_id} never reached status {target}; last={body}")


@pytest.fixture()
def stub_session_store(monkeypatch):
    instance = MagicMock()
    monkeypatch.setattr(ask_router, "_session_store_or_none", lambda: instance)
    return instance


def test_sessions_returns_empty_when_history_store_absent(
    client, auth_headers, monkeypatch
) -> None:
    monkeypatch.setattr(ask_router, "_session_store_or_none", lambda: None)
    response = client.get("/api/ask/sessions", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 0
    assert payload["sessions"] == []


def test_sessions_lists_rows_from_store(client, auth_headers, stub_session_store) -> None:
    stub_session_store.list_sessions.return_value = [
        {"id": 1, "title": "test", "first_question": "what tables?"}
    ]
    response = client.get("/api/ask/sessions", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["sessions"][0]["title"] == "test"


def test_get_session_404_when_missing(client, auth_headers, stub_session_store) -> None:
    stub_session_store.get_session.return_value = None
    response = client.get("/api/ask/sessions/9999", headers=auth_headers)
    assert response.status_code == 404


def test_end_session_marks_ended_and_clears_active_pointer(
    client, auth_headers, cfg, stub_session_store
) -> None:
    """End-session endpoint mirrors the CLI's `/session end`: marks
    the chat row ended AND clears `cfg.active_chat_session_id` if it
    pointed at this id, so the next /ask starts a fresh session."""
    stub_session_store.get_session.return_value = {"id": 7, "title": "x"}
    stub_session_store.end_session.return_value = None
    cfg.active_chat_session_id = 7

    response = client.post("/api/ask/sessions/7/end", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["session_id"] == 7
    stub_session_store.end_session.assert_called_once_with(7)
    assert cfg.active_chat_session_id == 0


def test_end_session_404_when_missing(client, auth_headers, stub_session_store) -> None:
    stub_session_store.get_session.return_value = None
    response = client.post("/api/ask/sessions/999/end", headers=auth_headers)
    assert response.status_code == 404


def test_get_session_returns_turns(client, auth_headers, stub_session_store) -> None:
    stub_session_store.get_session.return_value = {"id": 7, "title": "t"}
    stub_session_store.recent_turns = MagicMock(
        return_value=[
            {
                "role": "user",
                "question": "hi",
                "answer_summary": "",
                "turn_index": 0,
                "created_at": 1.0,
            },
            {
                "role": "assistant",
                "question": "",
                "answer_summary": "hello!",
                "turn_index": 1,
                "created_at": 2.0,
            },
        ]
    )
    response = client.get("/api/ask/sessions/7", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["session"]["id"] == 7
    assert len(payload["turns"]) == 2
    assert payload["turns"][0]["role"] == "user"
    assert payload["turns"][0]["question"] == "hi"
    assert payload["turns"][1]["role"] == "assistant"
    assert payload["turns"][1]["answer_summary"] == "hello!"


def test_submit_ask_returns_404_for_unknown_job_after_finish(
    client, auth_headers, monkeypatch, stub_session_store
) -> None:
    """The ask cancel endpoint must 404 cleanly when the job has
    already terminated — not 500. Pin the contract here so a
    JobRegistry refactor can't silently regress it."""
    response = client.post("/api/ask/unknown/cancel", headers=auth_headers)
    assert response.status_code == 404


def test_ask_worker_emits_thinking_as_true_deltas(
    client, auth_headers, monkeypatch, stub_session_store
) -> None:
    """The provider streams CUMULATIVE reasoning text on each callback
    (the CLI display takes a tail of it), but the SSE consumer in the
    browser appends each event into a buffer. The router must convert
    the cumulative stream into incremental deltas before emitting,
    otherwise the user sees ``TheThe userThe user is…``."""
    from amx.search.tool_agent import ToolAgentResult

    stub_session_store.start_session.return_value = 1
    stub_session_store.append_user_turn.return_value = None

    def fake_run_tool_agent(**kwargs):
        cb = kwargs["on_thinking_delta"]
        cb("The")
        cb("The user")
        cb("The user is asking")
        return ToolAgentResult(
            answer="ok",
            tool_calls=[],
            iterations=1,
            usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            finish_reason="stop",
        )

    monkeypatch.setattr(ask_router, "run_tool_agent", fake_run_tool_agent)
    monkeypatch.setattr(ask_router, "_load_catalog", lambda: MagicMock())
    monkeypatch.setattr(ask_router, "LLMProvider", lambda cfg: MagicMock())

    submit = client.post(
        "/api/ask",
        headers=auth_headers,
        json={"question": "test"},
    )
    job_id = submit.json()["job_id"]
    _wait_for_status(client, job_id, "done")
    events = _drain_sse(client, f"/api/ask/{job_id}/events", auth_headers)
    deltas = [e["text"] for e in events if e.get("type") == "thinking.delta"]
    # Backend gets cumulative ("The", "The user", "The user is asking")
    # but emits the new suffix only — joining them must reproduce the
    # original cumulative text exactly once.
    assert "".join(deltas) == "The user is asking"


def test_ask_worker_streams_thinking_and_answer(
    client, auth_headers, monkeypatch, stub_session_store
) -> None:
    """Patch run_tool_agent to fire a thinking delta + a tool call +
    a final answer; confirm the SSE stream surfaces all three event
    types in order and closes with job.done."""
    from amx.search.tool_agent import ToolAgentResult

    stub_session_store.start_session.return_value = 42
    stub_session_store.append_user_turn.return_value = None

    def fake_run_tool_agent(**kwargs):
        kwargs["on_thinking_delta"]("first chunk ")
        kwargs["on_thinking_delta"]("second chunk")
        kwargs["on_tool_call"]({"name": "list_schemas", "arguments": "{}", "result_preview": "[…]"})
        return ToolAgentResult(
            answer="Final answer.",
            tool_calls=[],
            iterations=1,
            usage={"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
            finish_reason="stop",
        )

    monkeypatch.setattr(ask_router, "run_tool_agent", fake_run_tool_agent)
    monkeypatch.setattr(ask_router, "_load_catalog", lambda: MagicMock())
    monkeypatch.setattr(ask_router, "LLMProvider", lambda cfg: MagicMock())

    submit = client.post(
        "/api/ask",
        headers=auth_headers,
        json={"question": "what schemas exist?"},
    )
    assert submit.status_code == 200
    job_id = submit.json()["job_id"]
    assert submit.json()["session_id"] == 42

    body = _wait_for_status(client, job_id, "done")
    assert body["summary"]["iterations"] == 1

    events = _drain_sse(client, f"/api/ask/{job_id}/events", auth_headers)
    types = [e["type"] for e in events]
    assert "thinking.delta" in types
    assert "tool.call" in types
    assert "answer.final" in types
    assert types[-1] == "job.done"


def test_ask_worker_persists_assistant_turn_with_correct_kwargs(
    client, auth_headers, monkeypatch, stub_session_store
) -> None:
    """The worker used to call ``append_assistant_turn(answer=…)`` —
    but the store signature is ``append_assistant_turn(*, run_id,
    answer_summary)``. The TypeError was swallowed by a bare except
    and assistant turns silently disappeared. Pin the kwargs."""
    from amx.search.tool_agent import ToolAgentResult

    stub_session_store.start_session.return_value = 99
    stub_session_store.append_user_turn.return_value = None
    stub_session_store.append_assistant_turn.return_value = None

    def fake_run_tool_agent(**kwargs):
        return ToolAgentResult(
            answer="hello there",
            tool_calls=[],
            iterations=1,
            usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            finish_reason="stop",
        )

    monkeypatch.setattr(ask_router, "run_tool_agent", fake_run_tool_agent)
    monkeypatch.setattr(ask_router, "_load_catalog", lambda: MagicMock())
    monkeypatch.setattr(ask_router, "LLMProvider", lambda cfg: MagicMock())

    submit = client.post(
        "/api/ask",
        headers=auth_headers,
        json={"question": "hi"},
    )
    job_id = submit.json()["job_id"]
    _wait_for_status(client, job_id, "done")

    stub_session_store.append_assistant_turn.assert_called_once()
    args, kwargs = stub_session_store.append_assistant_turn.call_args
    assert args == (99,)
    assert kwargs["answer_summary"] == "hello there"
    assert kwargs["run_id"] is None


def test_ask_worker_failed_when_catalog_missing(
    client, auth_headers, monkeypatch, stub_session_store
) -> None:
    monkeypatch.setattr(ask_router, "_load_catalog", lambda: None)
    response = client.post("/api/ask", headers=auth_headers, json={"question": "hi"})
    job_id = response.json()["job_id"]
    body = _wait_for_status(client, job_id, "failed")
    assert "search catalog" in (body.get("error") or "").lower()


def _drain_sse(client, path: str, auth_headers, timeout: float = 3.0) -> list[dict[str, Any]]:
    url = f"{path}?t=test-studio-token-abc123"
    events: list[dict[str, Any]] = []
    with client.stream("GET", url, headers=auth_headers, timeout=timeout) as response:
        assert response.status_code == 200
        deadline = time.monotonic() + timeout
        for line in response.iter_lines():
            if time.monotonic() > deadline:
                break
            if not line or not line.startswith("data:"):
                continue
            payload = line[len("data:") :].strip()
            if not payload:
                continue
            try:
                event = json.loads(payload)
            except json.JSONDecodeError:
                continue
            events.append(event)
            if str(event.get("type", "")).startswith("job."):
                break
    return events
