"""FastAPI app factory for AMX Studio.

The app is built once per ``/studio`` invocation. Tests construct
their own app via :func:`create_app`, supplying an in-memory
:class:`AMXConfig` and a fresh token. The launcher
(:func:`amx.web.launcher.launch_studio`) builds one for the real
session, mounts the static SPA bundle, and hands it to uvicorn.
"""

from __future__ import annotations

from importlib.resources import files as _resource_files
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from amx import __version__ as AMX_VERSION
from amx.config import AMXConfig
from amx.web.auth import TokenAuthMiddleware, generate_token
from amx.web.jobs import JobRegistry
from amx.web.routers import (
    ask,
    catalog,
    code_ops,
    comments,
    docs_ops,
    generate,
    history,
    live_db,
    pending,
    pricing,
    profiles,
    rerun,
    runs,
    system,
    system_ops,
)
from amx.web.security_headers import SecurityHeadersMiddleware


def _static_root() -> Path:
    """Return the on-disk path of the bundled SPA assets.

    Uses ``importlib.resources`` so the path resolves correctly whether
    the package is loaded from a wheel, a ``pip install -e .`` checkout,
    or a zipped distribution.
    """
    return Path(str(_resource_files("amx.web").joinpath("static")))


def create_app(
    cfg: AMXConfig,
    *,
    token: str | None = None,
    jobs: JobRegistry | None = None,
    static_root: Path | None = None,
) -> FastAPI:
    """Build a fully wired AMX Studio FastAPI app.

    Parameters
    ----------
    cfg
        The active AMXConfig. Pinned onto ``app.state.cfg`` so DI
        helpers can pull it without going through globals.
    token
        Bearer token required on every ``/api/*`` request. Defaults to
        a fresh :func:`generate_token` value when omitted (tests pass
        their own to assert auth behaviour).
    jobs
        :class:`JobRegistry` instance. A new one is created when
        omitted. Tests may pre-populate jobs and pass that registry in.
    static_root
        Override for the bundled SPA assets directory. Real launches
        let this default to the wheel's ``amx/web/static`` so end
        users get the pre-built dist; tests point it at a temp dir
        with stub files.
    """
    app = FastAPI(
        title="AMX Studio",
        version=AMX_VERSION,
        docs_url=None,  # disable /docs and /redoc — local-only UI doesn't
        redoc_url=None,  # need OpenAPI exposed; SPA fetches /api/* directly.
    )
    app.state.cfg = cfg
    app.state.token = token or generate_token()
    app.state.jobs = jobs or JobRegistry()

    # Auth middleware runs before any router so unauthenticated /api/*
    # requests short-circuit with 401 — never reach the route handler.
    app.add_middleware(TokenAuthMiddleware)
    # SecurityHeadersMiddleware sits in front of auth so the
    # Content-Security-Policy + sibling headers ride along on the 401
    # response too. Starlette runs middleware in the *reverse* of
    # ``add_middleware`` order, so adding it after auth means it wraps
    # the auth middleware — every response goes through it.
    app.add_middleware(SecurityHeadersMiddleware)

    app.include_router(system.router)
    app.include_router(live_db.router)
    app.include_router(catalog.router)
    app.include_router(history.router)
    app.include_router(runs.router)
    app.include_router(comments.router)
    app.include_router(ask.router)
    app.include_router(profiles.router)
    app.include_router(pending.router)
    app.include_router(docs_ops.router)
    app.include_router(system_ops.router)
    app.include_router(code_ops.router)
    app.include_router(generate.router)
    app.include_router(pricing.router)
    app.include_router(rerun.router)

    # Re-Run snapshots are short-lived (worker deletes them in finally).
    # On startup, sweep anything older than 1h that a previous crashed
    # worker may have left behind so the snapshot table never grows
    # beyond a live re-run window.
    try:
        from amx.storage.sqlite_store import history_store as _hs

        _store = _hs()
        if _store is not None:
            _store.gc_orphan_rerun_snapshots()
    except Exception:
        # Startup must never crash on a GC hiccup; the executor's own
        # cleanup keeps the table tidy regardless.
        pass

    root = static_root if static_root is not None else _static_root()
    app.state.static_root = root

    # Mount /assets/* when the bundled SPA dist is present so hashed
    # JS/CSS chunks pick up StaticFiles' cache headers. PR-B is what
    # actually populates ``static/assets/`` — pre-PR-B installs hit
    # the placeholder fallback below instead.
    assets_dir = root / "assets"
    if assets_dir.exists():
        app.mount(
            "/assets",
            StaticFiles(directory=str(assets_dir), check_dir=False),
            name="assets",
        )

    @app.get("/", include_in_schema=False)
    @app.get("/{full_path:path}", include_in_schema=False)
    def _serve_index_for_unknown_path(full_path: str = "") -> Response:
        """Serve the SPA index for any non-API, non-asset path.

        Routing happens client-side, so ``/runs/42`` and ``/ask``
        both need to return ``index.html``. Static asset files match
        the ``/assets/*`` mount above and never reach this fallback.
        """
        if full_path:
            candidate = root / full_path
            if candidate.is_file():
                return FileResponse(candidate)
        index = root / "index.html"
        if index.is_file():
            return FileResponse(index)
        # Pre-PR-B placeholder when the SPA bundle isn't on disk yet.
        # PR-A ships with this fallback so the launcher works end-to-
        # end without a built frontend.
        return Response(
            content=_PLACEHOLDER_HTML,
            media_type="text/html; charset=utf-8",
        )

    return app


_PLACEHOLDER_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>AMX Studio</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         display: flex; align-items: center; justify-content: center;
         min-height: 100vh; margin: 0; padding: 2rem; background: #0f0f0e; color: #f5f4f2; }
  .card { max-width: 36rem; padding: 2rem 2.5rem; border-radius: 14px;
          background: #1a1918; box-shadow: 0 12px 32px rgba(0,0,0,0.35);
          border: 1px solid rgba(245,244,242,0.06); }
  h1 { margin-top: 0; font-size: 1.5rem; letter-spacing: -0.01em; }
  code { background: rgba(255,255,255,0.06); padding: 0.1em 0.35em; border-radius: 4px; }
</style>
</head>
<body>
  <div class="card">
    <h1>AMX Studio — coming soon</h1>
    <p>The AMX Studio backend is running. The frontend bundle has not been built yet.</p>
    <p>Until the SPA lands you can still hit the JSON API directly, e.g.
       <code>curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:&lt;port&gt;/api/health</code>.</p>
  </div>
</body>
</html>
"""
