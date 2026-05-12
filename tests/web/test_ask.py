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


def test_delete_session_drops_row_and_clears_active_pointer(
    client, auth_headers, cfg, stub_session_store
) -> None:
    """``DELETE /api/ask/sessions/{id}`` hard-removes the session row
    and every turn under it (CLI ``/session end`` only marks ended).
    Mirrors the Studio sidebar trash icon. The active-session pointer
    flips back to 0 if it was referencing the just-deleted id."""
    stub_session_store.get_session.return_value = {"id": 12, "title": "throwaway"}
    stub_session_store.delete_session.return_value = True
    cfg.active_chat_session_id = 12

    response = client.delete("/api/ask/sessions/12", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["session_id"] == 12
    stub_session_store.delete_session.assert_called_once_with(12)
    assert cfg.active_chat_session_id == 0


def test_delete_session_404_when_missing(client, auth_headers, stub_session_store) -> None:
    stub_session_store.get_session.return_value = None
    response = client.delete("/api/ask/sessions/9999", headers=auth_headers)
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
    and assistant turns silently disappeared. Pin the kwargs.

    0.13: ``run_id`` now carries the just-opened ``search.ask``
    analysis_runs row id (was always ``None`` previously) so the
    Audit / RunsList / chat_turns join paths line up. Assertion
    accepts either ``None`` (history store unavailable) or an int.
    """
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
    # Either None (no history store available in test) or a real int
    # (history store is initialised; the search.ask row got created).
    run_id = kwargs["run_id"]
    assert run_id is None or isinstance(run_id, int)


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


def test_ask_worker_opens_search_ask_run_with_tokens(
    client, auth_headers, monkeypatch, stub_session_store
) -> None:
    """0.13: every Studio /ask query opens an ``analysis_runs`` row
    with ``command="search.ask"`` so the Run detail Metrics card,
    /usage aggregator, and /compare all see the LLM cost. Pin the
    contract: create_run + finish_run(tokens={...}) fire on the
    happy path. Without these, the user's complaint -- "ask runda
    cost gözükmüyor" -- regresses silently.
    """
    from amx.search.tool_agent import ToolAgentResult

    stub_session_store.start_session.return_value = 1
    stub_session_store.append_user_turn.return_value = None
    stub_session_store.append_assistant_turn.return_value = None

    fake_hs = MagicMock()
    fake_hs.create_run.return_value = 7
    fake_hs.finish_run.return_value = None
    monkeypatch.setattr(ask_router, "history_store", lambda: fake_hs)

    def fake_run_tool_agent(**kwargs):
        return ToolAgentResult(
            answer="hello",
            tool_calls=[],
            iterations=2,
            usage={"prompt_tokens": 11, "completion_tokens": 3, "total_tokens": 14},
            finish_reason="stop",
            total_latency_ms=42,
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

    fake_hs.create_run.assert_called_once()
    create_kwargs = fake_hs.create_run.call_args.kwargs
    assert create_kwargs["command"] == "search.ask"

    fake_hs.finish_run.assert_called_once()
    finish_args = fake_hs.finish_run.call_args
    assert finish_args.args == (7,)
    finish_kwargs = finish_args.kwargs
    assert finish_kwargs["status"] == "success"
    tokens = finish_kwargs["tokens"] or {}
    # tracker.records() may be empty when run_tool_agent itself is
    # stubbed (no real llm.chat happens) -- but the tokens dict
    # must be present with the canonical keys so the Run detail
    # Metrics card has something to render in production.
    for key in ("total_tokens", "total_cost_usd", "summary", "records"):
        assert key in tokens

    # The chat turn must carry the run_id we just created so /history
    # can join chat_turns with analysis_runs.
    stub_session_store.append_assistant_turn.assert_called_once()
    assert stub_session_store.append_assistant_turn.call_args.kwargs["run_id"] == 7


def test_ask_worker_finishes_run_on_failure(
    client, auth_headers, monkeypatch, stub_session_store
) -> None:
    """A failed ask must still call ``finish_run`` so the Audit /
    /usage / Compare surfaces show cost for partial work, with
    status=failed + error_text. Without this the run row stays in
    ``running`` forever and the Metrics card never renders."""
    fake_hs = MagicMock()
    fake_hs.create_run.return_value = 9
    fake_hs.finish_run.return_value = None
    monkeypatch.setattr(ask_router, "history_store", lambda: fake_hs)

    def boom(**kwargs):
        raise RuntimeError("LLM exploded")

    monkeypatch.setattr(ask_router, "run_tool_agent", boom)
    monkeypatch.setattr(ask_router, "_load_catalog", lambda: MagicMock())
    monkeypatch.setattr(ask_router, "LLMProvider", lambda cfg: MagicMock())

    submit = client.post("/api/ask", headers=auth_headers, json={"question": "hi"})
    job_id = submit.json()["job_id"]
    _wait_for_status(client, job_id, "failed")

    fake_hs.finish_run.assert_called_once()
    kwargs = fake_hs.finish_run.call_args.kwargs
    assert kwargs["status"] == "failed"
    assert "exploded" in (kwargs["error_text"] or "").lower()
