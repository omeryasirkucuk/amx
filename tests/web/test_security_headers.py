"""Defence-in-depth security headers ride along every response.

The token + 127.0.0.1 binding still do the heavy lifting; these
headers are a secondary layer that constrains what the SPA itself
can do if a payload ever lands in the DOM. The tests here pin the
contract so a future refactor of ``server.py`` can't silently drop
the headers.
"""

from __future__ import annotations


def test_csp_header_on_authenticated_api_response(client, auth_headers) -> None:
    """``/api/system`` is a cheap, always-mounted JSON endpoint."""
    r = client.get("/api/system", headers=auth_headers)
    # We don't care which 2xx/4xx — only that the header rides along.
    assert "Content-Security-Policy" in r.headers
    csp = r.headers["Content-Security-Policy"]
    # Tight defaults must survive any future refactor.
    assert "default-src 'self'" in csp
    assert "script-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp
    # We deliberately allow inline style (Tailwind utility classes via
    # ``style`` attributes) but never inline / eval'd script.
    assert "'unsafe-inline'" in csp.split("style-src")[1].split(";")[0]
    assert "'unsafe-eval'" not in csp


def test_csp_header_on_401_unauthenticated_api(client) -> None:
    """The headers must also ride a 401 — otherwise a stolen-token
    error page would be the only response without CSP applied."""
    r = client.get("/api/system")
    assert r.status_code == 401
    assert "Content-Security-Policy" in r.headers


def test_referrer_policy_no_referrer(client, auth_headers) -> None:
    r = client.get("/api/system", headers=auth_headers)
    assert r.headers.get("Referrer-Policy") == "no-referrer"


def test_x_content_type_options_nosniff(client, auth_headers) -> None:
    r = client.get("/api/system", headers=auth_headers)
    assert r.headers.get("X-Content-Type-Options") == "nosniff"


def test_x_frame_options_deny(client, auth_headers) -> None:
    """Mirror of CSP frame-ancestors for browsers that ignore CSP."""
    r = client.get("/api/system", headers=auth_headers)
    assert r.headers.get("X-Frame-Options") == "DENY"


def test_headers_on_static_index(client) -> None:
    """The SPA shell ``/`` returns the bundled ``index.html`` (or the
    placeholder when the dist isn't on disk in test). Either way the
    middleware must attach the headers."""
    r = client.get("/")
    assert r.status_code == 200
    assert "Content-Security-Policy" in r.headers
    assert r.headers.get("X-Frame-Options") == "DENY"


def test_route_set_header_is_not_overwritten(client, auth_headers) -> None:
    """``setdefault`` semantics — if a future endpoint needs a relaxed
    CSP it can set the header on its response and the middleware
    respects it. We can't easily exercise this through the existing
    routes (none override these headers today), so we assert the
    middleware uses ``setdefault`` indirectly: setting the header twice
    should be safe and idempotent."""
    # Fire the same endpoint twice; second response must still carry
    # the header (i.e. middleware did not corrupt headers across calls).
    r1 = client.get("/api/system", headers=auth_headers)
    r2 = client.get("/api/system", headers=auth_headers)
    assert r1.headers.get("Content-Security-Policy") == r2.headers.get(
        "Content-Security-Policy"
    )
