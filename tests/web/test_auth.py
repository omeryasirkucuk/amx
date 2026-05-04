"""Auth middleware contract tests for the AMX visualizer.

Covers the three delivery modes the SPA / SSE clients use:

* ``Authorization: Bearer <token>`` header on regular API calls.
* ``?t=<token>`` query string on EventSource / SSE endpoints (browsers
  can't attach headers to ``EventSource``).
* No auth required for ``/`` (the SPA index) and ``/assets/*`` (the
  static bundles).
"""

from __future__ import annotations

from amx.web.auth import generate_token


def test_health_requires_token(client) -> None:
    """A bare ``GET /api/health`` without a token must 401."""
    response = client.get("/api/health")
    assert response.status_code == 401
    body = response.json()
    assert "detail" in body
    assert "token" in body["detail"].lower()


def test_health_accepts_authorization_header(client, auth_headers) -> None:
    response = client.get("/api/health", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["version"]


def test_health_accepts_query_token_for_sse_clients(client, token) -> None:
    """EventSource can't set headers, so SSE endpoints accept ?t=… as a
    fallback. The auth check itself is path-agnostic — exercise it on
    /api/health for simplicity."""
    response = client.get(f"/api/health?t={token}")
    assert response.status_code == 200


def test_invalid_token_is_rejected(client) -> None:
    response = client.get("/api/health", headers={"Authorization": "Bearer wrong-token"})
    assert response.status_code == 401


def test_malformed_authorization_header_is_rejected(client) -> None:
    """``Authorization: Token <x>`` (wrong scheme) and bare values
    without the ``Bearer`` prefix must not match."""
    response = client.get("/api/health", headers={"Authorization": "Token whatever"})
    assert response.status_code == 401


def test_index_is_not_authenticated(client) -> None:
    """The SPA boots from / unauthenticated so it can capture the
    token from the URL and store it. ``/`` must return 200 even
    without a token."""
    response = client.get("/")
    assert response.status_code == 200
    assert "AMX Visualizer" in response.text


def test_spa_route_falls_back_to_index(client) -> None:
    """Client-side routing — ``/runs/42`` must return the index page,
    not a 404, so the React Router can take over."""
    response = client.get("/runs/42")
    assert response.status_code == 200
    assert "AMX Visualizer" in response.text


def test_generate_token_is_unique() -> None:
    """Sanity-check the token generator before we rely on it for auth."""
    tokens = {generate_token() for _ in range(50)}
    assert len(tokens) == 50
    for tok in tokens:
        assert len(tok) >= 32
