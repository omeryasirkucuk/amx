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


def test_delete_doc_profile_file_removes_disk_and_chunks(
    client, auth_headers, cfg, tmp_path, monkeypatch
):
    """One file in a 10-file profile should be deletable on its own —
    historic UX forced the user to drop the whole profile and re-upload
    the survivors. The endpoint removes the file from disk, deletes any
    Chroma chunks it produced, and prunes the upload-root manifest."""
    import json

    root = tmp_path / "uploads"
    root.mkdir()
    keep = root / "keep.pdf"
    keep.write_bytes(b"%PDF-1.4 keep")
    drop = root / "drop.pdf"
    drop.write_bytes(b"%PDF-1.4 drop")
    manifest = root / ".amx-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "version": 1,
                "files": {
                    "keep.pdf": {"original_name": "keep.pdf", "uploaded_at": 1.0},
                    "drop.pdf": {"original_name": "drop.pdf", "uploaded_at": 2.0},
                },
            }
        )
    )
    cfg.doc_profiles["handbook"] = [str(root)]

    deleted_sources: list[list[str]] = []

    class _FakeStore:
        def delete_chunks_for_sources(self, sources):
            deleted_sources.append(list(sources))
            return 4

    monkeypatch.setattr("amx.docs.rag.RAGStore", lambda *a, **kw: _FakeStore())

    resp = client.delete(
        f"/api/profiles/docs/handbook/files?path={drop}",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["chunks_removed"] == 4
    assert not drop.exists()
    assert keep.exists()
    # Manifest pruned to the survivor only.
    payload = json.loads(manifest.read_text())
    assert "drop.pdf" not in payload["files"]
    assert "keep.pdf" in payload["files"]
    # Chunk deletion was called with the resolved on-disk path.
    assert deleted_sources == [[str(drop.resolve())]]


def test_delete_doc_profile_file_rejects_paths_outside_profile_roots(
    client, auth_headers, cfg, tmp_path
):
    """Security: ``?path=/etc/passwd`` (or any path not inside one of
    the profile's registered roots) must be refused — otherwise the
    endpoint becomes an arbitrary-delete primitive."""
    root = tmp_path / "uploads"
    root.mkdir()
    cfg.doc_profiles["handbook"] = [str(root)]
    other = tmp_path / "elsewhere.pdf"
    other.write_bytes(b"%PDF-1.4 should not be touched")

    resp = client.delete(
        f"/api/profiles/docs/handbook/files?path={other}",
        headers=auth_headers,
    )
    assert resp.status_code == 400
    assert other.exists()


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
