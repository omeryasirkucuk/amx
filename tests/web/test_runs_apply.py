"""Run + apply router tests.

Mocks ``apply_review_results_to_db`` so the suite never touches a
real database. Exercises:

* The happy /apply path against an explicit results body.
* The pending-queue fallback when ``results`` is omitted.
* Cancellation (worker observes the cancel token between rows).
* SSE stream framing — JSON-decoded events arrive in the right
  order and a terminal ``job.done`` closes the stream.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any
from unittest.mock import MagicMock

from amx.web.routers import runs as runs_router


def _wait_for_status(client, job_id: str, target: str, timeout: float = 3.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(
            f"/api/apply/{job_id}", headers={"Authorization": "Bearer test-visualizer-token-abc123"}
        )
        body = resp.json()
        if body["status"] == target:
            return body
        time.sleep(0.05)
    raise AssertionError(f"Job {job_id} never reached status {target}; last={body}")


def test_apply_with_empty_body_uses_pending_queue(client, auth_headers, monkeypatch) -> None:
    """When ``results`` is omitted, the worker reads the pending
    queue from disk — we patch ``load_pending`` to return one entry
    so the apply path runs through to completion."""
    from amx.agents.base import Confidence
    from amx.agents.orchestrator import ReviewResult

    fake_result = ReviewResult(
        schema="sales",
        table="orders",
        column="id",
        final_description="Order primary key",
        confidence=Confidence.HIGH,
        source="manual",
        applied=True,
        asset_kind="table",
    )
    monkeypatch.setattr(runs_router, "load_pending", lambda: [fake_result])

    captured: dict[str, Any] = {}

    def fake_apply(db, results, *, on_progress=None, cancel_token=None, **_kw) -> int:
        captured["results"] = list(results)
        captured["cancel_token"] = cancel_token
        if on_progress:
            on_progress(results[0], "applied", 1, 1, "")
        return 1

    monkeypatch.setattr(runs_router, "apply_review_results_to_db", fake_apply)
    monkeypatch.setattr(runs_router, "DatabaseConnector", lambda cfg: MagicMock())

    response = client.post("/api/apply", headers=auth_headers, json={})
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    body = _wait_for_status(client, job_id, "done")
    assert body["summary"]["applied"] == 1
    assert captured["results"][0].column == "id"
    assert isinstance(captured["cancel_token"], threading.Event)


def test_apply_passes_explicit_body_results(client, auth_headers, monkeypatch) -> None:
    """When the body carries explicit results, the worker must coerce
    them into ReviewResult objects without touching the pending file."""
    monkeypatch.setattr(
        runs_router,
        "load_pending",
        lambda: pytest.fail("load_pending must not run"),  # type: ignore[name-defined]
    )

    captured: dict[str, Any] = {}

    def fake_apply(db, results, *, on_progress=None, cancel_token=None, **_kw) -> int:
        captured["results"] = list(results)
        return len(results)

    # Re-patch with a callable that explicitly fails — we don't want
    # load_pending to be touched in this test.
    import pytest  # local import to keep the file standalone-runnable

    monkeypatch.setattr(
        runs_router, "load_pending", lambda: pytest.fail("load_pending must not run")
    )
    monkeypatch.setattr(runs_router, "apply_review_results_to_db", fake_apply)
    monkeypatch.setattr(runs_router, "DatabaseConnector", lambda cfg: MagicMock())

    response = client.post(
        "/api/apply",
        headers=auth_headers,
        json={
            "results": [
                {
                    "schema": "s",
                    "table": "t",
                    "column": "c",
                    "final_description": "desc",
                    "confidence": "high",
                }
            ]
        },
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    _wait_for_status(client, job_id, "done")
    assert captured["results"][0].schema == "s"
    assert captured["results"][0].column == "c"


def test_apply_cancel_flips_status_and_emits_event(client, auth_headers, monkeypatch) -> None:
    """The worker checks ``cancel_token.is_set()`` after the loop;
    when it's set, the job ends as ``cancelled`` (even if
    apply_review_results_to_db returned a partial count)."""
    from amx.agents.base import Confidence
    from amx.agents.orchestrator import ReviewResult

    monkeypatch.setattr(
        runs_router,
        "load_pending",
        lambda: [
            ReviewResult(
                schema="sales",
                table="orders",
                column="id",
                final_description="x",
                confidence=Confidence.HIGH,
                source="manual",
                applied=True,
            )
        ],
    )

    started = threading.Event()
    keep_running = threading.Event()

    def slow_apply(db, results, *, on_progress=None, cancel_token=None, **_kw) -> int:
        started.set()
        # Block until the test releases us, mimicking a slow loop.
        while not cancel_token.is_set():
            if keep_running.wait(timeout=0.05):
                break
        return 0

    monkeypatch.setattr(runs_router, "apply_review_results_to_db", slow_apply)
    monkeypatch.setattr(runs_router, "DatabaseConnector", lambda cfg: MagicMock())

    response = client.post("/api/apply", headers=auth_headers, json={})
    job_id = response.json()["job_id"]
    started.wait(timeout=2.0)

    cancel = client.post(f"/api/apply/{job_id}/cancel", headers=auth_headers)
    assert cancel.status_code == 200

    body = _wait_for_status(client, job_id, "cancelled")
    assert body["summary"]["applied"] == 0


def test_apply_sse_stream_emits_json_events(client, auth_headers, monkeypatch) -> None:
    from amx.agents.base import Confidence
    from amx.agents.orchestrator import ReviewResult

    monkeypatch.setattr(
        runs_router,
        "load_pending",
        lambda: [
            ReviewResult(
                schema="s",
                table="t",
                column="c",
                final_description="d",
                confidence=Confidence.HIGH,
                source="manual",
                applied=True,
            )
        ],
    )

    def fake_apply(db, results, *, on_progress=None, cancel_token=None, **_kw) -> int:
        if on_progress:
            on_progress(results[0], "applied", 1, 1, "")
        return 1

    monkeypatch.setattr(runs_router, "apply_review_results_to_db", fake_apply)
    monkeypatch.setattr(runs_router, "DatabaseConnector", lambda cfg: MagicMock())

    response = client.post("/api/apply", headers=auth_headers, json={})
    job_id = response.json()["job_id"]

    events = _drain_sse(client, f"/api/apply/{job_id}/events", auth_headers)
    types = [e["type"] for e in events]
    assert "activity.added" in types
    assert "writeback.progress" in types
    assert types[-1] == "job.done"


def test_run_endpoint_emits_failed_for_now(client, auth_headers) -> None:
    """``/api/runs`` is a stub in PR-C; it emits a clear job.failed
    event so the SPA can render an actionable toast instead of a
    perpetual spinner. Pin the contract here so the eventual real
    wiring is an obvious diff."""
    response = client.post("/api/runs", headers=auth_headers, json={"scope": {}})
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    body = _wait_for_status(client, job_id, "failed")
    assert "pending" in (body.get("error") or "").lower()


def test_cancel_unknown_job_returns_404(client, auth_headers) -> None:
    response = client.post("/api/runs/unknown/cancel", headers=auth_headers)
    assert response.status_code == 404


def _drain_sse(client, path: str, auth_headers, timeout: float = 3.0) -> list[dict[str, Any]]:
    """Read SSE frames until ``job.*`` arrives. Uses Starlette
    TestClient's streaming context manager."""
    url = f"{path}?t=test-visualizer-token-abc123"
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
