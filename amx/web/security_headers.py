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
  is **not** allowed. ``frame-ancestors 'none'`` blocks clickjacking
  by default; embedded hosts (IDE webviews) opt into a relaxed
  ``frame-ancestors *`` via ``create_app(embedded=True)`` — see
  :func:`_headers_for` for why that stays safe.
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
#
# ``https://cdn.simpleicons.org`` is the single CDN allowlisted for
# images: the lineage canvas serves brand marks for AWS, Snowflake,
# Power BI etc. from there so we don't have to bundle ~22 SVGs as
# repo assets. SimpleIcons is the de-facto registry of brand marks
# under CC0; the CDN endpoint is read-only and ships pure SVG with no
# scripts. Per the policy intent above this is the explicit
# single-source relaxation rather than a wildcard.
_CSP_BASE = (
    "default-src 'self'; "
    "script-src 'self'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data: https://cdn.simpleicons.org; "
    "connect-src 'self'; "
    "font-src 'self' data:; "
    "base-uri 'self'; "
    "form-action 'self'"
)

_CSP = _CSP_BASE + "; frame-ancestors 'none'"

# Embedded mode — opted into by IDE hosts (e.g. the VS Code
# extension) that render Studio inside a webview iframe. Permissive
# framing is acceptable here because the server still binds to
# 127.0.0.1 only and every ``/api/*`` request still requires the
# bearer token — framing alone grants an attacker nothing without the
# token, and the token never leaves the host that spawned the server.
#
# The CSP ``*`` source matches **network schemes only** (http/https/
# ws/wss), so it does NOT cover IDE webview ancestor chains: VS Code
# frames sit under ``vscode-webview://`` (the webview wrapper) and
# ``vscode-file://`` (the desktop workbench window), and Chromium
# checks *every* ancestor against this directive. Without the
# explicit scheme sources the browser rejects the iframe with
# ``ERR_BLOCKED_BY_RESPONSE`` and the panel renders blank — verified
# empirically against VS Code 1.123 (vscode-extension's diag suite).
_CSP_EMBEDDED = _CSP_BASE + "; frame-ancestors * vscode-webview: vscode-file:"


def _headers_for(embedded: bool) -> dict[str, str]:
    headers = {
        "Content-Security-Policy": _CSP_EMBEDDED if embedded else _CSP,
        "Referrer-Policy": "no-referrer",
        "X-Content-Type-Options": "nosniff",
    }
    if not embedded:
        # Legacy mirror of the CSP frame-ancestors rule for browsers
        # that ignore CSP. Omitted entirely in embedded mode — there
        # is no "allow any ancestor" value for X-Frame-Options.
        headers["X-Frame-Options"] = "DENY"
    return headers


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Attach the security headers to every response.

    The middleware writes through to *every* response — SPA shell,
    /api/* JSON, SSE streams, error pages — so a future CSP
    violation report covers them all.
    """

    def __init__(self, app, *, embedded: bool = False) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app)
        self._headers = _headers_for(embedded)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        # We never overwrite a header a route has already set — if a
        # specific endpoint needs a relaxed CSP (rare), it can set the
        # header on its response and the middleware respects it.
        for name, value in self._headers.items():
            response.headers.setdefault(name, value)
        return response
