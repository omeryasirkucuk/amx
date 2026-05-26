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


def test_index_returns_400_when_no_paths(client, auth_headers) -> None:
    """No paths in body, no profile flag, no active doc profile —
    bail with 400 instead of spawning an empty worker."""
    response = client.post("/api/docs/index", headers=auth_headers, json={})
    assert response.status_code == 400


def test_index_streams_chunk_count(client, auth_headers, cfg, monkeypatch) -> None:
    cfg.doc_profiles["x"] = ["/abs"]

    monkeypatch.setattr("amx.docs.scanner.scan_all_sources", lambda paths: ["doc1", "doc2"])
    monkeypatch.setattr("amx.docs.scanner.total_size_mb", lambda docs: 0.5)

    # Per-document loop: the worker calls ``ingest`` once per document so it
    # can poll ``job.cancel`` between docs. The ``IngestSummary`` shape is
    # faked so ``int(...)`` still answers the chunk count. The store opens
    # cleanly (no mismatch) so index ingests incrementally — reset_collection
    # must NOT be called.
    from amx.docs.rag import IngestSummary

    fake_store = MagicMock()
    fake_store.ingest = MagicMock(
        side_effect=lambda docs, **kw: IngestSummary(succeeded=[docs[0]], failed=[], chunk_count=21)
    )
    monkeypatch.setattr("amx.docs.rag.RAGStore", lambda *a, **kw: fake_store)

    response = client.post("/api/docs/index", headers=auth_headers, json={"profile": "x"})
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    body = _wait_for_status(client, job_id, "done")
    # 21 chunks per doc × 2 docs = 42 total chunks across the batch.
    assert body["summary"]["chunks"] == 42
    assert body["summary"]["documents"] == 2
    assert body["summary"]["cancelled"] is False
    assert fake_store.ingest.call_count == 2
    fake_store.reset_collection.assert_not_called()


def test_index_incremental_when_no_mismatch(client, auth_headers, cfg, monkeypatch) -> None:
    """When the collection opens cleanly (active embedding matches), index
    ingests incrementally and does NOT drop the collection."""
    cfg.doc_profiles["x"] = ["/abs"]
    monkeypatch.setattr("amx.docs.scanner.scan_all_sources", lambda paths: ["doc1"])
    monkeypatch.setattr("amx.docs.scanner.total_size_mb", lambda docs: 0.1)

    from amx.docs.rag import IngestSummary

    fake_store = MagicMock()
    fake_store.ingest = MagicMock(
        side_effect=lambda docs, **kw: IngestSummary(succeeded=[docs[0]], failed=[], chunk_count=7)
    )
    monkeypatch.setattr("amx.docs.rag.RAGStore", lambda *a, **kw: fake_store)

    response = client.post("/api/docs/index", headers=auth_headers, json={"profile": "x"})
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    body = _wait_for_status(client, job_id, "done")

    fake_store.reset_collection.assert_not_called()
    assert fake_store.ingest.call_count == 1
    assert fake_store.ingest.call_args.kwargs.get("refresh") is False
    assert body["summary"]["chunks"] == 7


def test_index_recovers_from_embedding_mismatch(client, auth_headers, cfg, monkeypatch) -> None:
    """When the persisted collection has a stale embedding identity,
    RAGStore() raises on open; index force-drops amx_docs, reset_collection,
    and rebuilds so the collection is re-stamped with the active model."""
    cfg.doc_profiles["x"] = ["/abs"]
    monkeypatch.setattr("amx.docs.scanner.scan_all_sources", lambda paths: ["doc1"])
    monkeypatch.setattr("amx.docs.scanner.total_size_mb", lambda docs: 0.1)

    from amx.docs.rag import EmbeddingProviderMismatch, IngestSummary

    fake_store = MagicMock()
    fake_store.ingest = MagicMock(
        side_effect=lambda docs, **kw: IngestSummary(succeeded=[docs[0]], failed=[], chunk_count=3)
    )
    calls = {"n": 0}

    def _factory(*_a: Any, **_k: Any):
        calls["n"] += 1
        if calls["n"] == 1:
            raise EmbeddingProviderMismatch(
                recorded_provider="minilm",
                recorded_model="minilm-l6-v2",
                active_provider="sentence_transformers",
                active_model="thenlper/gte-small",
            )
        return fake_store

    monkeypatch.setattr("amx.docs.rag.RAGStore", _factory)
    fake_client = MagicMock()
    monkeypatch.setattr("chromadb.PersistentClient", lambda *a, **kw: fake_client)

    response = client.post("/api/docs/index", headers=auth_headers, json={"profile": "x"})
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    _wait_for_status(client, job_id, "done")

    fake_client.delete_collection.assert_called_once_with(name="amx_docs")
    fake_store.reset_collection.assert_called_once()
    assert fake_store.ingest.call_count == 1


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
