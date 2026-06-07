"""Embedded-mode framing headers (``create_app(embedded=True)``).

IDE hosts render Studio inside a webview iframe, which the default
``frame-ancestors 'none'`` / ``X-Frame-Options: DENY`` pair blocks.
``embedded=True`` relaxes exactly those two knobs — everything else
in the CSP must stay byte-identical to the strict profile.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from amx.config import AMXConfig
from amx.web.server import create_app

_TOKEN = "test-studio-token-embedded"


@pytest.fixture()
def embedded_client() -> TestClient:
    return TestClient(create_app(AMXConfig(), token=_TOKEN, embedded=True))


@pytest.fixture()
def embedded_auth() -> dict[str, str]:
    return {"Authorization": f"Bearer {_TOKEN}"}


def test_embedded_relaxes_frame_ancestors(embedded_client, embedded_auth) -> None:
    r = embedded_client.get("/api/system", headers=embedded_auth)
    csp = r.headers["Content-Security-Policy"]
    # The IDE scheme sources are load-bearing: CSP's `*` matches
    # network schemes only, so without them Chromium blocks the
    # webview iframe chain (vscode-webview:// + vscode-file://).
    assert "frame-ancestors * vscode-webview: vscode-file:" in csp
    assert "frame-ancestors 'none'" not in csp


def test_embedded_drops_x_frame_options(embedded_client, embedded_auth) -> None:
    """There is no "allow any ancestor" X-Frame-Options value, so the
    header must be absent entirely in embedded mode."""
    r = embedded_client.get("/api/system", headers=embedded_auth)
    assert "X-Frame-Options" not in r.headers


def test_embedded_keeps_remaining_csp_tight(embedded_client, embedded_auth) -> None:
    """Only the framing rule is relaxed — script/style/connect stay strict."""
    r = embedded_client.get("/api/system", headers=embedded_auth)
    csp = r.headers["Content-Security-Policy"]
    assert "default-src 'self'" in csp
    assert "script-src 'self'" in csp
    assert "'unsafe-eval'" not in csp
    assert r.headers.get("Referrer-Policy") == "no-referrer"
    assert r.headers.get("X-Content-Type-Options") == "nosniff"


def test_embedded_headers_on_static_index(embedded_client) -> None:
    """The SPA shell itself must be frameable — that's the whole point."""
    r = embedded_client.get("/")
    assert r.status_code == 200
    assert "frame-ancestors *" in r.headers["Content-Security-Policy"]
    assert "X-Frame-Options" not in r.headers


def test_default_remains_strict(client, auth_headers) -> None:
    """A browser launch (``embedded`` omitted) keeps the no-framing pair."""
    r = client.get("/api/system", headers=auth_headers)
    assert "frame-ancestors 'none'" in r.headers["Content-Security-Policy"]
    assert r.headers.get("X-Frame-Options") == "DENY"


def test_embedded_flag_pinned_on_app_state(embedded_client) -> None:
    """Routers that need to branch on the host kind read app.state."""
    assert embedded_client.app.state.embedded is True
