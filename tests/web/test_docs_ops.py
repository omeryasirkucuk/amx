"""Docs ops router tests — scan + ingest + search endpoints.

Heavy paths (RAG store, document scanner) are mocked so the suite
never touches the real Chroma DB or filesystem walks. We're pinning
the HTTP shape + the worker dispatch contract, not the underlying
ingestion logic — that lives in :mod:`amx.docs.rag` tests.
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock


def _wait_for_status(client, job_id: str, target: str, timeout: float = 3.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(
            f"/api/runs/{job_id}",
            headers={"Authorization": "Bearer test-studio-token-abc123"},
        )
        body = resp.json()
        if body["status"] == target:
            return body
        time.sleep(0.05)
    raise AssertionError(f"Job {job_id} never reached status {target}; last={body}")


def test_scan_returns_400_when_no_paths(client, auth_headers) -> None:
    """No paths in body, no profile flag, no active doc profile —
    bail with 400 instead of spawning an empty worker."""
    response = client.post("/api/docs/scan", headers=auth_headers, json={})
    assert response.status_code == 400


def test_scan_resolves_active_profile_when_no_paths_in_body(
    client, auth_headers, cfg, monkeypatch
) -> None:
    cfg.doc_profiles["handbook"] = ["/abs/docs"]
    cfg.active_doc_profile = "handbook"

    fake_doc = MagicMock(path="/abs/docs/intro.md", source_type="local")
    fake_doc.size_bytes = 1024
    monkeypatch.setattr(
        "amx.docs.scanner.scan_all_sources",
        lambda paths: [fake_doc] if paths == ["/abs/docs"] else [],
    )
    monkeypatch.setattr("amx.docs.scanner.total_size_mb", lambda docs: 0.001)

    response = client.post("/api/docs/scan", headers=auth_headers, json={})
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    body = _wait_for_status(client, job_id, "done")
    assert body["summary"]["total"] == 1


def test_ingest_streams_chunk_count(client, auth_headers, cfg, monkeypatch) -> None:
    cfg.doc_profiles["x"] = ["/abs"]

    monkeypatch.setattr("amx.docs.scanner.scan_all_sources", lambda paths: ["doc1", "doc2"])
    monkeypatch.setattr("amx.docs.scanner.total_size_mb", lambda docs: 0.5)

    # Per-document loop (PR D): the worker calls ``ingest`` once per
    # document so it can poll ``job.cancel`` between docs. The
    # ``IngestSummary`` shape is faked here so ``int(...)`` still
    # answers the chunk count.
    from amx.docs.rag import IngestSummary

    fake_store = MagicMock()
    fake_store.ingest = MagicMock(
        side_effect=lambda docs, **kw: IngestSummary(succeeded=[docs[0]], failed=[], chunk_count=21)
    )
    monkeypatch.setattr("amx.docs.rag.RAGStore", lambda *a, **kw: fake_store)

    response = client.post(
        "/api/docs/ingest",
        headers=auth_headers,
        json={"profile": "x"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    body = _wait_for_status(client, job_id, "done")
    # 21 chunks per doc × 2 docs = 42 total chunks across the batch.
    assert body["summary"]["chunks"] == 42
    assert body["summary"]["documents"] == 2
    assert body["summary"]["cancelled"] is False
    assert fake_store.ingest.call_count == 2


def test_search_rejects_empty_query(client, auth_headers) -> None:
    response = client.get("/api/docs/search?q=", headers=auth_headers)
    assert response.status_code == 400


def test_search_returns_empty_when_store_empty(client, auth_headers, monkeypatch) -> None:
    fake_store = MagicMock(doc_count=0)
    monkeypatch.setattr("amx.docs.rag.RAGStore", lambda *a, **kw: fake_store)
    response = client.get("/api/docs/search?q=hello", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 0
    assert "RAG store is empty" in (payload.get("message") or "")


def test_search_returns_hits(client, auth_headers, monkeypatch) -> None:
    fake_store = MagicMock(doc_count=5)
    fake_store.query = MagicMock(
        return_value=[
            {
                "metadata": {"source": "/d/intro.md"},
                "distance": 0.12,
                "text": "Customer master table holds…",
            },
            {
                "metadata": {"source": "/d/orders.md"},
                "distance": 0.18,
                "text": "Orders table joins customer_id to customers.",
            },
        ]
    )
    monkeypatch.setattr("amx.docs.rag.RAGStore", lambda *a, **kw: fake_store)

    response = client.get("/api/docs/search?q=customer&n=5", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    assert payload["hits"][0]["source"] == "/d/intro.md"
    assert payload["hits"][0]["distance"] == 0.12
    fake_store.query.assert_called_once_with("customer", n_results=5)
