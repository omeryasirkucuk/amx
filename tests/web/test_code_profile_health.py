"""PR δ — ``GET /api/profiles/code/{name}/health``.

Per-code-profile chunk count, last-indexed timestamp, last error, and
the embedding metadata recorded on the ``amx_code`` Chroma collection.
"""

from __future__ import annotations


def test_code_profile_health_404_for_unknown(client, auth_headers):
    resp = client.get("/api/profiles/code/nope/health", headers=auth_headers)
    assert resp.status_code == 404


def test_code_profile_health_returns_chunk_count_and_metadata(
    client, auth_headers, cfg, monkeypatch
):
    cfg.code_profiles["backend"] = "/abs/backend"
    cfg.code_profile_last_indexed_at["backend"] = 1715512345.6
    cfg.code_profile_last_error["backend"] = ""

    monkeypatch.setattr("amx.codebase.code_rag.code_collection_count", lambda **_kw: 42)
    monkeypatch.setattr(
        "amx.codebase.code_rag.code_collection_metadata",
        lambda **_kw: {
            "embedding_provider": "minilm",
            "embedding_model": "minilm-l6-v2",
        },
    )

    resp = client.get("/api/profiles/code/backend/health", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["name"] == "backend"
    assert body["chunk_count"] == 42
    assert body["last_indexed_at"] == 1715512345.6
    assert body["last_error"] is None
    assert body["embedding_provider"] == "minilm"
    assert body["embedding_model"] == "minilm-l6-v2"
    assert body["paths"] == ["/abs/backend"]


def test_code_profile_health_handles_missing_chroma_gracefully(
    client, auth_headers, cfg, monkeypatch
):
    """When Chroma fails to open, the endpoint still returns 200 with
    zero chunks instead of bubbling a 500 — the user can still see the
    config-side telemetry."""
    cfg.code_profiles["backend"] = "/abs/backend"

    def _boom(**_kw):
        raise RuntimeError("chroma not installed")

    monkeypatch.setattr("amx.codebase.code_rag.code_collection_count", _boom)
    monkeypatch.setattr("amx.codebase.code_rag.code_collection_metadata", lambda **_kw: {})

    resp = client.get("/api/profiles/code/backend/health", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["chunk_count"] == 0
    assert body["embedding_model"] is None


def test_code_profile_health_surfaces_last_error(client, auth_headers, cfg, monkeypatch):
    cfg.code_profiles["etl"] = "/abs/etl"
    cfg.code_profile_last_error["etl"] = "boom: missing dep"

    monkeypatch.setattr("amx.codebase.code_rag.code_collection_count", lambda **_kw: 0)
    monkeypatch.setattr("amx.codebase.code_rag.code_collection_metadata", lambda **_kw: {})

    resp = client.get("/api/profiles/code/etl/health", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["chunk_count"] == 0
    assert body["last_error"] == "boom: missing dep"
