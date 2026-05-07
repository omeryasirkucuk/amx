"""GET /api/code/search — Studio counterpart of /api/docs/search."""

from __future__ import annotations


def test_code_search_empty_query_400(client, auth_headers) -> None:
    res = client.get("/api/code/search?q=", headers=auth_headers)
    assert res.status_code == 400


def test_code_search_unknown_profile_404(client, auth_headers) -> None:
    res = client.get("/api/code/search?q=foo&profile=ghost", headers=auth_headers)
    assert res.status_code == 404


def test_code_search_empty_collection_returns_message(client, auth_headers) -> None:
    """No code indexed yet — surface a friendly message instead of crashing."""
    res = client.get("/api/code/search?q=customers", headers=auth_headers)
    assert res.status_code == 200
    body = res.json()
    assert body["count"] == 0
    assert "amx_code" in (body.get("message") or "") or "code-scan" in (body.get("message") or "")
