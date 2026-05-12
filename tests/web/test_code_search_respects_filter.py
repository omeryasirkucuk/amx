"""PR alpha — ``/api/code/search`` honours code-profile source filters.

Before PR alpha the endpoint accepted a single optional ``profile=``
and otherwise ran against the global Chroma collection — leaking
hits from every indexed snippet regardless of the active scope.

Now it:

* accepts ``profile=`` repeatedly (``?profile=foo&profile=bar``) for
  an explicit union override,
* defaults to ``cfg.effective_code_paths()`` when the parameter is
  omitted, matching how ``/api/docs/search`` scopes its results.
"""

from __future__ import annotations

from unittest.mock import MagicMock


def test_default_uses_active_profile_paths(client, auth_headers, cfg, monkeypatch) -> None:
    cfg.code_profiles["main"] = "/abs/main"
    cfg.code_profiles["other"] = "/abs/other"
    cfg.active_code_profile = "main"

    captured: dict = {}

    def _fake_count(persist_dir=None, source_filters=None):
        captured["count_filters"] = list(source_filters or [])
        return 7

    def _fake_query(question, n_results=5, persist_dir=None, source_filters=None):
        captured["query_filters"] = list(source_filters or [])
        return [
            {
                "text": "hit",
                "metadata": {"source": "/abs/main/a.py", "rel_path": "a.py"},
                "distance": 0.1,
            }
        ]

    monkeypatch.setattr("amx.codebase.code_rag.code_collection_count", _fake_count)
    monkeypatch.setattr("amx.codebase.code_rag.query_code_snippets", _fake_query)

    resp = client.get("/api/code/search?q=foo", headers=auth_headers)
    assert resp.status_code == 200
    assert captured["count_filters"] == ["/abs/main"]
    assert captured["query_filters"] == ["/abs/main"]


def test_explicit_profile_overrides_active(client, auth_headers, cfg, monkeypatch) -> None:
    cfg.code_profiles["main"] = "/abs/main"
    cfg.code_profiles["other"] = "/abs/other"
    cfg.active_code_profile = "main"

    captured: dict = {}

    monkeypatch.setattr(
        "amx.codebase.code_rag.code_collection_count",
        lambda persist_dir=None, source_filters=None: (
            captured.update(count_filters=list(source_filters or [])) or 3
        ),
    )
    monkeypatch.setattr(
        "amx.codebase.code_rag.query_code_snippets",
        lambda question, n_results=5, persist_dir=None, source_filters=None: (
            captured.update(query_filters=list(source_filters or [])) or []
        ),
    )

    resp = client.get("/api/code/search?q=foo&profile=other", headers=auth_headers)
    assert resp.status_code == 200
    assert captured["query_filters"] == ["/abs/other"]


def test_multi_profile_union(client, auth_headers, cfg, monkeypatch) -> None:
    cfg.code_profiles["main"] = "/abs/main"
    cfg.code_profiles["other"] = "/abs/other"
    cfg.active_code_profile = "main"

    captured: dict = {}

    def _fake_count(persist_dir=None, source_filters=None):
        captured["filters"] = list(source_filters or [])
        return 0

    monkeypatch.setattr("amx.codebase.code_rag.code_collection_count", _fake_count)
    # ``query_code_snippets`` shouldn't be called when count is 0 —
    # leave it as a sentinel that would explode if it were.
    monkeypatch.setattr(
        "amx.codebase.code_rag.query_code_snippets",
        MagicMock(side_effect=AssertionError("should not be called")),
    )

    resp = client.get(
        "/api/code/search?q=foo&profile=main&profile=other",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert sorted(captured["filters"]) == ["/abs/main", "/abs/other"]


def test_unknown_profile_in_list_returns_404(client, auth_headers, cfg, monkeypatch) -> None:
    cfg.code_profiles["main"] = "/abs/main"
    resp = client.get(
        "/api/code/search?q=foo&profile=main&profile=ghost",
        headers=auth_headers,
    )
    assert resp.status_code == 404
