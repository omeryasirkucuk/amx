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
    installs,
    live_db,
    pending,
    pricing,
    profiles,
    rerun,
    runs,
    schedules,
    style,
    system,
    system_ops,
)
from amx.web.security_headers import SecurityHeadersMiddleware

# Module-level holder for the bootstrap TickReport (Phase 5a).
# Populated by create_app() at lifespan start; consumed by the
# /api/scheduler/bootstrap-report route to drive the catch-up banner.
_bootstrap_report: object | None = None


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
    # Route HTTPS calls made from Studio worker threads (pricing
    # refresh, doc scanner, batch API SDKs) through the OS trust
    # store. ``configure_trust_store`` is idempotent so the CLI entry
    # point and Studio bootstrap can both call it safely.
    from amx.utils.network_trust import configure_trust_store

    configure_trust_store()

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
    app.include_router(installs.router)
    app.include_router(style.router)
    app.include_router(schedules.router)
    app.include_router(schedules.scheduler_router)

    # Re-Run snapshots are short-lived (worker deletes them in finally).
    # On startup, sweep anything older than 1h that a previous crashed
    # worker may have left behind so the snapshot table never grows
    # beyond a live re-run window.
    try:
        from amx.storage.sqlite_store import history_store as _hs

        _store = _hs()
        if _store is not None:
            _store.gc_orphan_rerun_snapshots()
            # Also reap context-cache rows past their TTL so a stale
            # schema doesn't quietly bias re-run prompts after the
            # user altered the table out-of-band.
            _store.gc_run_context_cache()
            # Same for the column-comments cache that backs the
            # bulk-schema metadata path — anything past 1h is wiped
            # so the next sidebar expand triggers a fresh fetch.
            _store.gc_column_comments_cache()
            # And the schemas_cache that backs the catalog-expand
            # bulk path.
            _store.gc_schemas_cache()
    except Exception:
        # Startup must never crash on a GC hiccup; the executor's own
        # cleanup keeps the table tidy regardless.
        pass

    # Scheduler bootstrap pass: surface stale runs + missed schedules
    # for the catch-up banner. Pinned on the module object so the
    # ``/api/scheduler/bootstrap-report`` route can read it without
    # threading state through DI. Failures are swallowed so a broken
    # scheduler never blocks Studio startup.
    global _bootstrap_report
    _bootstrap_report = None
    try:
        from amx.scheduler.tick import tick as _bootstrap_tick
        from amx.storage.sqlite_store import history_store as _hs2

        _hs_obj = _hs2()
        if _hs_obj is not None:
            _bootstrap_report = _bootstrap_tick(store=_hs_obj, source="bootstrap")
    except Exception:
        pass

    # Studio-resident scheduler ticker.
    #
    # The launchd / systemd / Task-Scheduler daemons are the
    # always-on path for firing schedules while AMX is closed, but
    # they're prone to OS-specific permission quirks (macOS TCC
    # blocks the launchd-spawned Python from opening user files;
    # Linux user timers need ``loginctl enable-linger``; Windows
    # Task Scheduler needs explicit ``/ru`` user). The Studio
    # process itself runs in the user's interactive session with
    # full TCC / keychain / env access -- if Studio is open, every
    # 60s tick from inside the Studio event loop is the most
    # reliable path to fire due schedules, regardless of OS.
    #
    # The two paths coexist: the OS daemon (when installed and
    # working) fires while Studio is closed; the Studio-resident
    # ticker takes over the moment Studio comes up. The tick is
    # idempotent (``claim_due_schedule`` atomically transitions
    # pending -> running) so the worst case of both paths racing
    # is a no-op for one side.
    @app.on_event("startup")
    async def _start_studio_scheduler_loop() -> None:
        import asyncio
        import logging

        from amx.runtime.worker import (
            production_run_executor,
            spawn_scheduled_worker,
        )
        from amx.scheduler.tick import tick as _periodic_tick
        from amx.storage.sqlite_store import history_store as _hs

        slog = logging.getLogger("amx.web.scheduler-loop")

        async def _loop() -> None:
            # Stagger the first tick so the lifespan bootstrap tick
            # has finished writing its report before the periodic
            # loop starts claiming due schedules.
            await asyncio.sleep(5.0)
            while True:
                try:
                    store = _hs()
                    if store is None:
                        slog.warning("history store unavailable, retrying")
                    else:

                        def _spawn(payload: dict, _store=store) -> int:
                            return spawn_scheduled_worker(
                                payload,
                                store=_store,
                                background=True,
                                run_executor=production_run_executor,
                            )

                        report = _periodic_tick(
                            store=store,
                            source="daemon",
                            spawn_worker=_spawn,
                        )
                        if report.fired or report.stale_recovered:
                            slog.info(
                                "studio scheduler tick: fired=%s stale_recovered=%s",
                                report.fired,
                                report.stale_recovered,
                            )
                except Exception:  # noqa: BLE001
                    slog.exception("studio scheduler tick crashed")
                await asyncio.sleep(60.0)

        app.state.scheduler_task = asyncio.create_task(_loop())

    @app.on_event("shutdown")
    async def _stop_studio_scheduler_loop() -> None:
        task = getattr(app.state, "scheduler_task", None)
        if task is None:
            return
        task.cancel()
        try:
            await task
        except Exception:  # noqa: BLE001
            pass

    root = static_root if static_root is not None else _static_root()
    app.state.static_root = root

    # Mount /assets/* when the bundled SPA dist is present so hashed
    # JS/CSS chunks pick up StaticFiles' cache headers. PR-B is what
    # actually populates ``static/assets/`` — pre-PR-B installs hit
    # the placeholder fallback below instead.
    #
    # Cache policy is critical for the SPA's dynamic-import flow.
    # Hashed files (``/assets/*-<hash>.js``) are content-addressed —
    # the filename changes whenever the content changes — so we mark
    # them ``immutable`` with a long max-age. ``index.html`` is the
    # opposite: it is the only file whose URL never changes but whose
    # contents change every build (the entry-chunk reference flips to
    # the new hash). Without an explicit ``no-cache`` on it, browsers
    # cache the old ``index.html`` and the dynamic ``import()`` later
    # tries to fetch the previous chunk by name and 404s with
    # ``Failed to fetch dynamically imported module``.
    assets_dir = root / "assets"
    if assets_dir.exists():
        app.mount(
            "/assets",
            StaticFiles(directory=str(assets_dir), check_dir=False),
            name="assets",
        )

    # ``index.html`` is the only file whose URL never changes but
    # whose contents flip every build (the entry-chunk reference
    # points at a new hash). Without an explicit no-cache header,
    # browsers cache the old ``index.html`` and the dynamic
    # ``import()`` later tries to fetch the previous chunk by name
    # and 404s with ``Failed to fetch dynamically imported module``.
    # Hashed assets under ``/assets/*`` are safe — Vite's content-
    # addressed filenames mean a stale cache hit just serves the
    # right bytes — so the StaticFiles mount above keeps its
    # default behaviour.
    _NO_CACHE_HEADER = "no-cache, no-store, must-revalidate"

    @app.get("/__alive", include_in_schema=False)
    def _alive() -> Response:
        """Light-weight liveness probe.

        Lives outside ``/api/*`` so the bearer-token middleware leaves
        it alone, and returns a static 11-byte body without touching
        the filesystem (the SPA fallback below reads ``index.html``
        on every hit, which can spend a couple of seconds -- too slow
        for upstream proxies that timeout aggressively on a per-
        request liveness check).
        """
        return Response(
            content=b'{"ok":true}',
            media_type="application/json",
            headers={"Cache-Control": "no-store"},
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
                return FileResponse(candidate, headers={"Cache-Control": _NO_CACHE_HEADER})
        index = root / "index.html"
        if index.is_file():
            return FileResponse(index, headers={"Cache-Control": _NO_CACHE_HEADER})
        # Pre-PR-B placeholder when the SPA bundle isn't on disk yet.
        # PR-A ships with this fallback so the launcher works end-to-
        # end without a built frontend.
        return Response(
            content=_PLACEHOLDER_HTML,
            media_type="text/html; charset=utf-8",
            headers={"Cache-Control": _NO_CACHE_HEADER},
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
