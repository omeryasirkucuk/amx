"""Friendly LLM-error handling for /api/ask.

The frontend used to sit on "Reasoning…" forever when the LLM was
broken because:
1. ``submit_ask`` had no pre-flight check on cfg.llm — it spawned the
   worker even when no provider was configured.
2. ``LLMProvider(cfg.llm)`` lived OUTSIDE the worker's try/except, so
   an init failure crashed the thread silently with no terminal SSE
   event for the SPA to consume.

This module verifies both bugs are fixed: pre-flight surfaces a clean
412 + ``configure-llm`` hint, and worker-side LLM init failures emit
a ``job.failed`` event with the same hint.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import AMXConfig
from amx.web.routers import ask as ask_router


@pytest.fixture()
def cfg_no_llm() -> AMXConfig:
    """Fresh AMXConfig with NO LLM provider configured — the
    misconfigured-doctor scenario from the bug report."""
    return AMXConfig()


def test_submit_ask_rejects_when_llm_provider_missing(cfg_no_llm, monkeypatch) -> None:
    """User has not picked an LLM yet → 412 + ``configure-llm`` hint
    so the SPA shows the 'Open Settings' CTA instead of a generic
    error toast."""
    from fastapi.testclient import TestClient

    from amx.web.server import create_app

    app = create_app(cfg_no_llm, token="test-token-123")
    client = TestClient(app)
    response = client.post(
        "/api/ask",
        headers={"Authorization": "Bearer test-token-123"},
        json={"question": "anything"},
    )
    assert response.status_code == 412
    detail = response.json()["detail"]
    assert isinstance(detail, dict)
    assert detail["hint"] == "configure-llm"
    assert "Settings" in detail["message"]


def test_submit_ask_rejects_when_llm_model_missing(monkeypatch) -> None:
    """Provider set but no model — same friendly 412 path."""
    from fastapi.testclient import TestClient

    from amx.web.server import create_app

    cfg = AMXConfig()
    cfg.llm.provider = "openai"
    cfg.llm.model = ""
    app = create_app(cfg, token="test-token-123")
    client = TestClient(app)
    response = client.post(
        "/api/ask",
        headers={"Authorization": "Bearer test-token-123"},
        json={"question": "anything"},
    )
    assert response.status_code == 412
    detail = response.json()["detail"]
    assert detail["hint"] == "configure-llm"
    assert "model" in detail["message"].lower()


def test_ask_worker_emits_failed_when_llm_init_raises(client, auth_headers, monkeypatch) -> None:
    """When LLMProvider(cfg.llm) throws (bad API key, network down to
    the provider), the worker MUST emit job.failed with hint=
    ``configure-llm`` so the SPA renders the "Couldn't reach the LLM"
    banner instead of hanging on 'Reasoning…'."""
    monkeypatch.setattr(ask_router, "_load_catalog", lambda: MagicMock())

    def _boom(_cfg):
        raise RuntimeError("Invalid API key for openai")

    monkeypatch.setattr(ask_router, "LLMProvider", _boom)

    response = client.post(
        "/api/ask",
        headers=auth_headers,
        json={"question": "show me tables"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    # Drain the SSE stream until the terminal event.
    events = _drain_sse(client, f"/api/ask/{job_id}/events", auth_headers)
    failed = [e for e in events if e["type"] == "job.failed"]
    assert len(failed) == 1, f"Expected one job.failed event, got: {events}"
    payload = failed[0]
    assert payload.get("hint") == "configure-llm"
    assert "Settings" in payload.get("error", "")


def test_ask_worker_classifies_llm_runtime_errors(client, auth_headers, monkeypatch) -> None:
    """The classifier inside the worker turns LLM-side runtime
    failures (auth, rate limit, model not found, …) into a
    ``configure-llm`` hint so the SPA shows the friendly banner."""
    monkeypatch.setattr(ask_router, "_load_catalog", lambda: MagicMock())
    monkeypatch.setattr(ask_router, "LLMProvider", lambda _cfg: MagicMock())

    def _boom(*_args, **_kwargs):
        raise RuntimeError("openai authentication failed: invalid api key")

    monkeypatch.setattr(ask_router, "run_tool_agent", _boom)

    response = client.post(
        "/api/ask",
        headers=auth_headers,
        json={"question": "show me tables"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    events = _drain_sse(client, f"/api/ask/{job_id}/events", auth_headers)
    failed = [e for e in events if e["type"] == "job.failed"]
    assert len(failed) == 1
    assert failed[0]["hint"] == "configure-llm"


def test_ask_worker_no_hint_for_generic_errors(client, auth_headers, monkeypatch) -> None:
    """Errors that aren't LLM-shaped (e.g. tool-side bug) don't get
    the configure-llm hint — wouldn't make sense to send the user to
    Settings → LLM for a Python AttributeError."""
    monkeypatch.setattr(ask_router, "_load_catalog", lambda: MagicMock())
    monkeypatch.setattr(ask_router, "LLMProvider", lambda _cfg: MagicMock())

    def _boom(*_args, **_kwargs):
        raise ValueError("invalid scope name")

    monkeypatch.setattr(ask_router, "run_tool_agent", _boom)

    response = client.post(
        "/api/ask",
        headers=auth_headers,
        json={"question": "test"},
    )
    job_id = response.json()["job_id"]
    events = _drain_sse(client, f"/api/ask/{job_id}/events", auth_headers)
    failed = [e for e in events if e["type"] == "job.failed"]
    assert len(failed) == 1
    assert "hint" not in failed[0]


# ── SSE helper ──────────────────────────────────────────────────────────


def _drain_sse(client, path: str, headers: dict[str, str]) -> list[dict]:
    """Read all SSE events from *path* until a terminal event arrives.

    Mirrors the helper used in test_ask.py / test_runs_apply.py — the
    worker thread runs synchronously enough in tests that we can drain
    in one pass.
    """
    import json
    import time

    events: list[dict] = []
    t0 = time.monotonic()
    with client.stream("GET", path, headers=headers) as response:
        assert response.status_code == 200
        current_event = None
        for line in response.iter_lines():
            if not line:
                continue
            if isinstance(line, bytes):
                line = line.decode("utf-8")
            if line.startswith("event:"):
                current_event = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                payload = line.split(":", 1)[1].strip()
                try:
                    parsed = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                parsed["type"] = current_event or parsed.get("type", "")
                events.append(parsed)
                if (current_event or "") in {
                    "job.done",
                    "job.cancelled",
                    "job.failed",
                }:
                    return events
            if time.monotonic() - t0 > 5:
                break
    return events
