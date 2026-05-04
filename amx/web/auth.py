"""Token-based auth for the local AMX visualizer.

The visualizer always binds to ``127.0.0.1`` so untrusted machines on the
LAN can't reach it. On a multi-user host, though, loopback isn't enough:
any local process can curl 127.0.0.1. We add a single shared secret —
generated fresh per ``/visualize`` invocation — that the SPA carries on
every API call.

Two delivery modes:

* **HTTP**: ``Authorization: Bearer <token>`` header on every ``/api/*``
  request.
* **SSE / EventSource**: browsers can't attach custom headers to
  ``EventSource`` connections, so SSE endpoints accept the same token via
  ``?t=<token>`` query string.

Static assets (``/index.html``, ``/assets/*``) are intentionally
unauthenticated so the SPA can boot before it has a token. The HTML page
captures the token from ``location.search``, stashes it in
``localStorage``, then strips it from the URL.
"""

from __future__ import annotations

import secrets

from fastapi import Request, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

#: Header name the SPA sets for every authenticated API call.
BEARER_HEADER = "Authorization"
#: Query-string key SSE clients use as a fallback (EventSource has no
#: way to set headers).
QUERY_PARAM = "t"
#: Path prefixes that require a valid token. Anything else (the index
#: page, static assets, OpenAPI/docs at /docs) is served without auth.
PROTECTED_PREFIXES: tuple[str, ...] = ("/api/",)


def generate_token() -> str:
    """Return a fresh URL-safe token for one ``/visualize`` invocation.

    32 bytes → 43 base64url characters; way more entropy than we need
    but the URL stays human-readable.
    """
    return secrets.token_urlsafe(32)


class TokenAuthMiddleware(BaseHTTPMiddleware):
    """Reject any ``/api/*`` request that doesn't carry the session token.

    The token lives on the FastAPI app's ``state.token`` attribute (set
    by :func:`amx.web.server.create_app`) so test code can swap it out
    by reassigning ``app.state.token`` between requests.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if not any(path.startswith(prefix) for prefix in PROTECTED_PREFIXES):
            return await call_next(request)

        expected = getattr(request.app.state, "token", None)
        if not expected:
            # Misconfiguration — don't silently accept requests just
            # because the server forgot to set a token.
            return _unauthorized("Visualizer auth is not configured.")

        provided = _extract_token(request)
        if provided is None:
            return _unauthorized("Missing visualizer token.")
        # ``secrets.compare_digest`` keeps the comparison constant-time —
        # paranoid, but free.
        if not secrets.compare_digest(provided, expected):
            return _unauthorized("Invalid visualizer token.")
        return await call_next(request)


def _extract_token(request: Request) -> str | None:
    raw = request.headers.get(BEARER_HEADER, "").strip()
    if raw.lower().startswith("bearer "):
        candidate = raw.split(" ", 1)[1].strip()
        if candidate:
            return candidate
    qs_token = request.query_params.get(QUERY_PARAM, "").strip()
    return qs_token or None


def _unauthorized(detail: str) -> Response:
    """Return a 401 directly so the middleware short-circuits without
    raising — :class:`BaseHTTPMiddleware` propagates exceptions as 500s
    rather than honouring the status code on :class:`HTTPException`.
    """
    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={"detail": detail},
    )
