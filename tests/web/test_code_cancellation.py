"""PR alpha — code scan cancellation endpoint + worker polling.

``POST /api/code/jobs/{job_id}/cancel`` sets the job's ``cancel``
event. The scan worker polls it between files (never mid-file —
that would orphan a partial Chroma upsert) and finalizes the
summary with ``cancelled=True``.
"""

from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import MagicMock


def _wait_for_status(client, job_id: str, target: str, timeout: float = 5.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        body = client.get(
            f"/api/runs/{job_id}",
            headers={"Authorization": "Bearer test-studio-token-abc123"},
        ).json()
        last = body
        if body["status"] == target:
            return body
        time.sleep(0.02)
    raise AssertionError(f"Job {job_id} never reached {target}; last={last}")


def test_cancel_endpoint_returns_404_for_unknown_job(client, auth_headers) -> None:
    resp = client.post("/api/code/jobs/does-not-exist/cancel", headers=auth_headers)
    assert resp.status_code == 404


def test_cancel_endpoint_sets_flag_and_worker_exits(client, auth_headers, cfg, monkeypatch) -> None:
    """Drive the analyzer's progress callback a few times, then cancel
    BEFORE the next ``__advance__`` is delivered. The worker must
    observe ``job.cancel`` and bail with status=cancelled."""
    cfg.code_profiles["repo"] = "/abs/repo"
    cfg.active_code_profile = "repo"

    monkeypatch.setattr(
        "amx.db.connector.DatabaseConnector",
        lambda cfg: MagicMock(
            list_schemas=MagicMock(return_value=["public"]),
            list_tables=MagicMock(return_value=["users"]),
        ),
    )

    first_advance = threading.Event()
    cancel_seen = threading.Event()

    def fake_analyze(path, **kw):
        cb = kw.get("progress_callback")
        if cb is not None:
            cb("__total__", 5)
            cb("__advance__", "a.py")
            # Signal the test to fire the cancel and wait for it.
            first_advance.set()
            cancel_seen.wait(timeout=3.0)
            # Next call should raise out of the analyzer because
            # job.cancel is set — exactly the path the real analyzer
            # would hit between files.
            cb("__advance__", "b.py")
        # Should never get here.
        return MagicMock(
            total_files=5,
            scanned_files=5,
            references={},
            external_mentions={},
        )

    monkeypatch.setattr("amx.codebase.analyzer.analyze_codebase", fake_analyze)

    resp = client.post("/api/code/scan", headers=auth_headers, json={})
    assert resp.status_code == 200
    job_id = resp.json()["job_id"]

    assert first_advance.wait(timeout=3.0), "scan never started"
    cancel = client.post(f"/api/code/jobs/{job_id}/cancel", headers=auth_headers)
    assert cancel.status_code == 200
    assert cancel.json()["cancelled"] is True
    cancel_seen.set()

    body = _wait_for_status(client, job_id, "cancelled")
    assert body["status"] == "cancelled"
    assert (body.get("summary") or {}).get("cancelled") is True
