"""Defence-in-depth response headers for AMX Studio.

The Studio token + 127.0.0.1 binding already prevent cross-origin
clients from reaching ``/api/*``. The headers added here are a
secondary layer: they constrain what the SPA itself is allowed to
do *if* an injected payload ever ends up in the DOM (LLM-rendered
metadata, doc snippets, future user input).

What we ship
------------

* **Content-Security-Policy** — restricts script / style / connect /
  image sources to ``'self'`` (plus ``data:`` for inline assets used
  by markdown render). Inline ``style`` is allowed because Tailwind
  emits utility classes via ``style`` attributes; inline ``script``
  is **not** allowed. ``frame-ancestors 'none'`` blocks clickjacking.
* **Referrer-Policy: no-referrer** — Studio is on 127.0.0.1, never
  send referrer when the user clicks a link to an external doc.
* **X-Content-Type-Options: nosniff** — defence against MIME confusion
  on the served SPA assets.
* **X-Frame-Options: DENY** — legacy mirror of the CSP frame-ancestors
  rule for browsers that ignore CSP.

What we do **not** ship
-----------------------

* No HSTS — Studio runs over plain HTTP on 127.0.0.1. HSTS would
  break the connection on browsers that have learned the loopback
  origin.
* No Permissions-Policy — Studio does not use camera / mic / geo /
  payment APIs and the SPA bundle has no third-party iframes, so the
  default-deny browser behaviour is already correct.

The CSP allowlist is intentionally tight. If a future feature needs
a relaxation (a CDN-hosted font, an external chart library), prefer
adding a single explicit source over moving to ``'unsafe-inline'`` /
``'unsafe-eval'``. The middleware is plain text on purpose so that
diffing the policy between releases reads naturally in PR review.
"""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

# Tight default. ``'self'`` covers everything the SPA loads in the
# normal case (hashed assets under /assets/*, /api/* fetches, /api/*
# SSE). ``data:`` for img-src lets ReactMarkdown render small inline
# images embedded in doc snippets without falling through to a
# blocked-src CSP report. ``'unsafe-inline'`` on style-src is the
# Tailwind concession (utility-class style attrs); it does *not*
# extend to script-src.
_CSP = (
    "default-src 'self'; "
    "script-src 'self'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data:; "
    "connect-src 'self'; "
    "font-src 'self' data:; "
    "frame-ancestors 'none'; "
    "base-uri 'self'; "
    "form-action 'self'"
)

_HEADERS: dict[str, str] = {
    "Content-Security-Policy": _CSP,
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Attach the headers in :data:`_HEADERS` to every response.

    The middleware writes through to *every* response — SPA shell,
    /api/* JSON, SSE streams, error pages — so a future CSP
    violation report covers them all.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        # We never overwrite a header a route has already set — if a
        # specific endpoint needs a relaxed CSP (rare), it can set the
        # header on its response and the middleware respects it.
        for name, value in _HEADERS.items():
            response.headers.setdefault(name, value)
        return response
