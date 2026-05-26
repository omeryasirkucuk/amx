"""PR D — ingest cancellation endpoint + worker polling.

``POST /api/docs/jobs/{job_id}/cancel`` sets the job's ``cancel``
event. The ingest worker polls it between documents (never
mid-Chroma write) and finalizes the summary with ``cancelled=True``.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock


def _wait_for_status(client, job_id: str, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(
            f"/api/runs/{job_id}",
            headers={"Authorization": "Bearer test-studio-token-abc123"},
        ).json()
        if body["status"] in {"done", "cancelled", "failed"}:
            return body
        time.sleep(0.05)
    raise AssertionError(f"Job {job_id} never terminated; last={body}")


def test_cancel_endpoint_returns_404_for_unknown_job(client, auth_headers):
    resp = client.post("/api/docs/jobs/does-not-exist/cancel", headers=auth_headers)
    assert resp.status_code == 404


def test_cancel_during_ingest_stops_worker_between_documents(
    client, auth_headers, cfg, monkeypatch
):
    """Spawn an ingest worker that processes 5 documents; cancel after
    the first one completes; expect the worker to bail before the
    remaining 4 are ingested and to emit ``cancelled=True``."""
    cfg.doc_profiles["x"] = ["/abs"]

    docs = [f"doc{i}" for i in range(5)]
    monkeypatch.setattr("amx.docs.scanner.scan_all_sources", lambda paths: docs)
    monkeypatch.setattr("amx.docs.scanner.total_size_mb", lambda _docs: 1.0)

    # Use a real lock so we can deterministically release one document
    # at a time. ``ingest`` blocks until the test releases the gate.
    first_started = threading.Event()
    cancel_seen = threading.Event()
    call_count = {"n": 0}

    from amx.docs.rag import IngestSummary

    def _fake_ingest(doc_list, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Signal the test thread to issue the cancel BEFORE we
            # return from the first ingest. The second iteration of
            # the worker loop will then observe ``job.cancel`` set and
            # break before calling ingest again.
            first_started.set()
            cancel_seen.wait(timeout=3.0)
        return IngestSummary(succeeded=[doc_list[0]], failed=[], chunk_count=3)

    fake_store = MagicMock()
    fake_store.ingest.side_effect = _fake_ingest
    monkeypatch.setattr("amx.docs.rag.RAGStore", lambda *a, **kw: fake_store)

    resp = client.post(
        "/api/docs/index",
        headers=auth_headers,
        json={"profile": "x"},
    )
    assert resp.status_code == 200
    job_id = resp.json()["job_id"]

    # Wait for the first ingest call to begin, then cancel BEFORE
    # letting it return. Once the worker advances to the next loop
    # iteration it observes ``job.cancel`` and bails without calling
    # ingest a second time.
    assert first_started.wait(timeout=3.0), "first ingest never started"
    cancel = client.post(f"/api/docs/jobs/{job_id}/cancel", headers=auth_headers)
    assert cancel.status_code == 200
    assert cancel.json()["cancelled"] is True
    cancel_seen.set()

    body = _wait_for_status(client, job_id)
    assert body["status"] == "cancelled"
    assert body["summary"]["cancelled"] is True
    # Only the first document was ingested before cancellation took
    # effect; the worker did not chew through the remaining 4.
    assert call_count["n"] == 1
