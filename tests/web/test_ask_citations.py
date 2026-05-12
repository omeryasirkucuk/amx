"""Tests for PR E: structured citations on /ask SSE events.

Backs the brief's contract:
- ``tool.call`` events for ``search_docs`` carry a ``citations`` list of
  ``{source, chunk_idx, score, snippet}`` dicts parsed from the tool's
  JSON return value.
- ``answer.final`` carries an aggregated ``citations`` list (insertion
  order, deduped by ``(source, chunk_idx)``) so the SPA can render a
  Sources block under the answer.
"""

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
            if event.get("type") in {"job.done", "job.failed", "job.cancelled"}:
                break
    return events


@pytest.fixture()
def stub_session_store(monkeypatch):
    instance = MagicMock()
    instance.start_session.return_value = 1
    instance.append_user_turn.return_value = None
    instance.append_assistant_turn.return_value = None
    monkeypatch.setattr(ask_router, "_session_store_or_none", lambda: instance)
    return instance


def test_search_docs_tool_call_carries_citations(
    client, auth_headers, monkeypatch, stub_session_store
) -> None:
    """Every ``tool.call`` event for ``search_docs`` is augmented with
    a parsed citations list pulled from the tool's JSON return value
    so the SPA can render a per-hit table inside the expander."""
    from amx.search.tool_agent import ToolAgentResult, _summarise_tool_call

    class _FakeToolCall:
        name = "search_docs"
        arguments = '{"query": "vendor onboarding"}'

    tool_result = json.dumps(
        {
            "hits": [
                {
                    "source": "spec.pdf",
                    "snippet": "Vendors must complete onboarding within 30 days.",
                    "distance": 0.16,
                    "chunk_idx": 5,
                },
                {
                    "source": "runbook.md",
                    "snippet": "Onboarding checklist lives in confluence.",
                    "distance": 0.22,
                    "chunk_idx": 2,
                },
            ],
            "count": 2,
        }
    )
    summary = _summarise_tool_call(_FakeToolCall(), tool_result)
    assert "citations" in summary
    assert len(summary["citations"]) == 2
    first = summary["citations"][0]
    assert first["source"] == "spec.pdf"
    assert first["chunk_idx"] == 5
    assert 0.0 <= first["score"] <= 1.0
    assert "Vendors must complete onboarding" in first["snippet"]

    # Now exercise the SSE path end-to-end with a fake run_tool_agent
    # that calls the on_tool_call hook with the augmented summary.
    def fake_run_tool_agent(**kwargs):
        kwargs["on_tool_call"](summary)
        return ToolAgentResult(
            answer="Vendors onboard in 30 days.",
            tool_calls=[summary],
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
        json={"question": "how long does vendor onboarding take?"},
    )
    job_id = submit.json()["job_id"]
    _wait_for_status(client, job_id, "done")

    events = _drain_sse(client, f"/api/ask/{job_id}/events", auth_headers)
    tool_call_events = [e for e in events if e.get("type") == "tool.call"]
    assert len(tool_call_events) == 1
    assert tool_call_events[0]["citations"]
    assert tool_call_events[0]["citations"][0]["source"] == "spec.pdf"

    final_events = [e for e in events if e.get("type") == "answer.final"]
    assert len(final_events) == 1
    assert final_events[0]["citations"]
    keys = {(c["source"], c["chunk_idx"]) for c in final_events[0]["citations"]}
    assert keys == {("spec.pdf", 5), ("runbook.md", 2)}


def test_answer_final_dedupes_citations_across_tool_calls(
    client, auth_headers, monkeypatch, stub_session_store
) -> None:
    """When the LLM fires ``search_docs`` twice and the second call
    re-hits the same chunk, the aggregated ``citations`` list on
    ``answer.final`` must dedupe by ``(source, chunk_idx)`` while
    preserving insertion order (first occurrence wins)."""
    from amx.search.tool_agent import ToolAgentResult

    summary_a = {
        "name": "search_docs",
        "arguments": "{}",
        "result_preview": "...",
        "citations": [
            {"source": "spec.pdf", "chunk_idx": 5, "score": 0.84, "snippet": "first hit"},
            {"source": "runbook.md", "chunk_idx": 2, "score": 0.78, "snippet": "second hit"},
        ],
    }
    summary_b = {
        "name": "search_docs",
        "arguments": "{}",
        "result_preview": "...",
        "citations": [
            # Duplicate of the first call's chunk; must be skipped.
            {"source": "spec.pdf", "chunk_idx": 5, "score": 0.91, "snippet": "duplicate hit"},
            # New chunk; must be appended.
            {"source": "extra.md", "chunk_idx": 0, "score": 0.71, "snippet": "fresh hit"},
        ],
    }

    def fake_run_tool_agent(**kwargs):
        kwargs["on_tool_call"](summary_a)
        kwargs["on_tool_call"](summary_b)
        return ToolAgentResult(
            answer="aggregated",
            tool_calls=[summary_a, summary_b],
            iterations=2,
            usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            finish_reason="stop",
        )

    monkeypatch.setattr(ask_router, "run_tool_agent", fake_run_tool_agent)
    monkeypatch.setattr(ask_router, "_load_catalog", lambda: MagicMock())
    monkeypatch.setattr(ask_router, "LLMProvider", lambda cfg: MagicMock())

    submit = client.post(
        "/api/ask",
        headers=auth_headers,
        json={"question": "anything?"},
    )
    job_id = submit.json()["job_id"]
    _wait_for_status(client, job_id, "done")
    events = _drain_sse(client, f"/api/ask/{job_id}/events", auth_headers)
    final = next(e for e in events if e.get("type") == "answer.final")
    citations = final["citations"]
    assert [(c["source"], c["chunk_idx"]) for c in citations] == [
        ("spec.pdf", 5),
        ("runbook.md", 2),
        ("extra.md", 0),
    ]


def test_non_search_docs_tool_calls_omit_citations(
    client, auth_headers, monkeypatch, stub_session_store
) -> None:
    """Citations are only attached to ``search_docs`` summaries; other
    tools (``list_schemas``, ``find_joinable_tables`` …) keep the
    pre-PR-E shape so the SPA's old code-paths never break."""
    from amx.search.tool_agent import _summarise_tool_call

    class _NonDocsToolCall:
        name = "list_schemas"
        arguments = "{}"

    summary = _summarise_tool_call(_NonDocsToolCall(), '["sap_test"]')
    assert "citations" not in summary
