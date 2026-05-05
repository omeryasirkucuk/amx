"""Pending-review queue tests."""

from __future__ import annotations

import json
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from amx.web.routers import runs as runs_router


def _wait_for_status(client, job_id: str, target: str, timeout: float = 3.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(
            f"/api/apply/{job_id}",
            headers={"Authorization": "Bearer test-studio-token-abc123"},
        )
        body = resp.json()
        if body["status"] == target:
            return body
        time.sleep(0.05)
    raise AssertionError(f"Job {job_id} never reached {target}; last={body}")


@pytest.fixture()
def stub_pending(monkeypatch, tmp_path):
    """Stub the on-disk pending file so tests round-trip in memory."""
    from amx.agents.base import Confidence
    from amx.agents.orchestrator import ReviewResult

    state: list[ReviewResult] = []

    def fake_load_pending() -> list[ReviewResult]:
        return list(state)

    def fake_save_pending(results) -> None:
        state.clear()
        state.extend(results)

    def fake_clear_pending() -> None:
        state.clear()

    monkeypatch.setattr("amx.web.routers.pending.load_pending", fake_load_pending)
    monkeypatch.setattr("amx.web.routers.pending.save_pending", fake_save_pending)
    monkeypatch.setattr("amx.web.routers.pending.clear_pending", fake_clear_pending)
    # The runs._apply_worker imports load_pending from
    # amx.web.routers.runs at module level; stub there too so the
    # apply path sees the same in-memory state.
    monkeypatch.setattr("amx.web.routers.runs.load_pending", fake_load_pending)

    def push(rr: ReviewResult) -> None:
        state.append(rr)

    return {"state": state, "push": push, "Confidence": Confidence, "ReviewResult": ReviewResult}


def test_list_pending_empty(client, auth_headers, stub_pending) -> None:
    response = client.get("/api/pending", headers=auth_headers)
    assert response.status_code == 200
    assert response.json() == {"pending": [], "count": 0}


def test_list_pending_serializes_with_idx(client, auth_headers, stub_pending) -> None:
    rr = stub_pending["ReviewResult"](
        schema="sales",
        table="orders",
        column="id",
        final_description="Order primary key",
        confidence=stub_pending["Confidence"].HIGH,
        source="manual",
        applied=True,
    )
    stub_pending["push"](rr)
    response = client.get("/api/pending", headers=auth_headers)
    payload = response.json()
    assert payload["count"] == 1
    row = payload["pending"][0]
    assert row["idx"] == 0
    assert row["confidence"] == "high"


def test_patch_pending_updates_one_row(client, auth_headers, stub_pending) -> None:
    rr = stub_pending["ReviewResult"](
        schema="sales",
        table="orders",
        column="id",
        final_description="old",
        confidence=stub_pending["Confidence"].HIGH,
        source="manual",
        applied=True,
    )
    stub_pending["push"](rr)
    response = client.patch(
        "/api/pending/0",
        headers=auth_headers,
        json={"final_description": "new", "confidence": "medium"},
    )
    assert response.status_code == 200
    assert response.json()["final_description"] == "new"
    assert response.json()["confidence"] == "medium"


def test_patch_pending_404_for_unknown_idx(client, auth_headers, stub_pending) -> None:
    response = client.patch(
        "/api/pending/99",
        headers=auth_headers,
        json={"final_description": "x"},
    )
    assert response.status_code == 404


def test_patch_pending_400_for_invalid_confidence(client, auth_headers, stub_pending) -> None:
    rr = stub_pending["ReviewResult"](
        schema="s",
        table="t",
        column="c",
        final_description="d",
        confidence=stub_pending["Confidence"].HIGH,
        source="manual",
        applied=True,
    )
    stub_pending["push"](rr)
    response = client.patch(
        "/api/pending/0",
        headers=auth_headers,
        json={"confidence": "ULTRA"},
    )
    assert response.status_code == 400


def test_delete_pending_removes_row(client, auth_headers, stub_pending) -> None:
    rr = stub_pending["ReviewResult"](
        schema="s",
        table="t",
        column="c",
        final_description="d",
        confidence=stub_pending["Confidence"].HIGH,
        source="manual",
        applied=True,
    )
    stub_pending["push"](rr)
    response = client.delete("/api/pending/0", headers=auth_headers)
    assert response.status_code == 200
    assert client.get("/api/pending", headers=auth_headers).json()["count"] == 0


def test_clear_pending(client, auth_headers, stub_pending) -> None:
    stub_pending["push"](
        stub_pending["ReviewResult"](
            schema="s",
            table="t",
            column="c",
            final_description="d",
            confidence=stub_pending["Confidence"].HIGH,
            source="manual",
            applied=True,
        )
    )
    response = client.post("/api/pending/clear", headers=auth_headers)
    assert response.status_code == 200
    assert client.get("/api/pending", headers=auth_headers).json()["count"] == 0


def test_pending_apply_spawns_apply_job(client, auth_headers, stub_pending, monkeypatch) -> None:
    """The /api/pending/apply endpoint must reuse runs._apply_worker
    so the SSE event shape is identical to /api/apply."""
    rr = stub_pending["ReviewResult"](
        schema="s",
        table="t",
        column="c",
        final_description="d",
        confidence=stub_pending["Confidence"].HIGH,
        source="manual",
        applied=True,
    )
    stub_pending["push"](rr)

    def fake_apply(db, results, *, on_progress=None, cancel_token=None, **_kw) -> int:
        if on_progress:
            on_progress(results[0], "applied", 1, 1, "")
        return 1

    monkeypatch.setattr(runs_router, "apply_review_results_to_db", fake_apply)
    monkeypatch.setattr(runs_router, "DatabaseConnector", lambda cfg: MagicMock())

    response = client.post("/api/pending/apply", headers=auth_headers)
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    body = _wait_for_status(client, job_id, "done")
    assert body["summary"]["applied"] == 1


def test_pending_apply_streams_via_apply_events_endpoint(
    client, auth_headers, stub_pending, monkeypatch
) -> None:
    rr = stub_pending["ReviewResult"](
        schema="s",
        table="t",
        column="c",
        final_description="d",
        confidence=stub_pending["Confidence"].HIGH,
        source="manual",
        applied=True,
    )
    stub_pending["push"](rr)

    def fake_apply(db, results, *, on_progress=None, cancel_token=None, **_kw) -> int:
        if on_progress:
            on_progress(results[0], "applied", 1, 1, "")
        return 1

    monkeypatch.setattr(runs_router, "apply_review_results_to_db", fake_apply)
    monkeypatch.setattr(runs_router, "DatabaseConnector", lambda cfg: MagicMock())

    response = client.post("/api/pending/apply", headers=auth_headers)
    job_id = response.json()["job_id"]

    events = _drain_sse(client, f"/api/apply/{job_id}/events", auth_headers)
    types = [e["type"] for e in events]
    assert "writeback.progress" in types
    assert types[-1] == "job.done"


def _drain_sse(client, path: str, auth_headers, timeout: float = 3.0):
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
