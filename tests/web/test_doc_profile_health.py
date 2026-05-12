"""PR D — ``GET /api/profiles/docs/{name}/health``.

Reports per-profile chunk count, last-ingested timestamp, last error,
and the embedding metadata recorded on the Chroma collection.
"""

from __future__ import annotations

from unittest.mock import MagicMock


def test_health_returns_404_for_unknown_profile(client, auth_headers):
    resp = client.get("/api/profiles/docs/nope/health", headers=auth_headers)
    assert resp.status_code == 404


def test_health_combines_telemetry_and_chroma_metadata(client, auth_headers, cfg, monkeypatch):
    cfg.doc_profiles["handbook"] = ["/abs/handbook"]
    cfg.doc_profiles_last_ingested_at["handbook"] = 1715512345.6
    cfg.doc_profiles_last_error["handbook"] = ""

    fake_store = MagicMock()
    fake_store.filtered_doc_count = MagicMock(return_value=37)
    fake_store.collection = MagicMock()
    fake_store.collection.metadata = {
        "embedding_provider": "openai_compatible",
        "embedding_model": "text-embedding-3-small",
    }
    monkeypatch.setattr("amx.docs.rag.RAGStore", lambda *a, **kw: fake_store)

    resp = client.get("/api/profiles/docs/handbook/health", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "handbook"
    assert body["chunk_count"] == 37
    assert body["last_ingested_at"] == 1715512345.6
    assert body["last_error"] is None
    assert body["embedding_provider"] == "openai_compatible"
    assert body["embedding_model"] == "text-embedding-3-small"
    assert body["paths"] == ["/abs/handbook"]


def test_health_handles_missing_chroma_gracefully(client, auth_headers, cfg, monkeypatch):
    """When ``RAGStore`` fails to open (missing deps, no chroma_db,
    etc.) the endpoint still returns 200 with zero chunks instead of
    bubbling a 500 — the user can still see the config-side
    telemetry."""
    cfg.doc_profiles["handbook"] = ["/abs/handbook"]

    def _boom(*a, **kw):
        raise RuntimeError("chroma not installed")

    monkeypatch.setattr("amx.docs.rag.RAGStore", _boom)

    resp = client.get("/api/profiles/docs/handbook/health", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["chunk_count"] == 0
    assert body["embedding_model"] is None
