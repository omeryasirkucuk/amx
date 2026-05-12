"""PR D — ``/api/docs/search`` applies the active profile's source filter.

Before PR D the search endpoint instantiated ``RAGStore()`` with no
``source_filters`` so results leaked chunks from every profile's
documents. Now it passes ``cfg.effective_doc_paths()`` so the result
set matches the user's active scope.
"""

from __future__ import annotations

from unittest.mock import MagicMock


def test_search_passes_active_profile_paths_to_ragstore(client, auth_headers, cfg, monkeypatch):
    cfg.doc_profiles["handbook"] = ["/abs/handbook"]
    cfg.doc_profiles["other"] = ["/abs/other"]
    cfg.active_doc_profile = "handbook"

    captured: dict = {}

    def _factory(*args, **kwargs):
        captured["kwargs"] = kwargs
        store = MagicMock()
        store.doc_count = 1
        store.query = MagicMock(
            return_value=[
                {
                    "text": "alpha",
                    "metadata": {"source": "/abs/handbook/a.md"},
                    "distance": 0.1,
                }
            ]
        )
        return store

    monkeypatch.setattr("amx.docs.rag.RAGStore", _factory)

    resp = client.get("/api/docs/search?q=foo", headers=auth_headers)
    assert resp.status_code == 200
    assert captured["kwargs"]["source_filters"] == ["/abs/handbook"]
    body = resp.json()
    assert body["count"] == 1


def test_search_with_no_profile_uses_no_filter(client, auth_headers, monkeypatch):
    captured: dict = {}

    def _factory(*args, **kwargs):
        captured["kwargs"] = kwargs
        store = MagicMock()
        store.doc_count = 0
        return store

    monkeypatch.setattr("amx.docs.rag.RAGStore", _factory)

    resp = client.get("/api/docs/search?q=foo", headers=auth_headers)
    assert resp.status_code == 200
    # With no active profile, ``effective_doc_paths()`` returns an
    # empty list which becomes ``source_filters=None`` — the global
    # collection scope.
    assert captured["kwargs"]["source_filters"] is None
