"""FastAPI dependency-injection helpers for AMX Studio.

AMX Studio reads / mutates the same in-memory :class:`AMXConfig`
the parent CLI uses, plus a singleton :class:`amx.web.jobs.JobRegistry`
that lives for the duration of the ``/studio`` session. Pinning
those onto ``app.state`` (rather than module-globals) keeps tests
isolated — every :class:`fastapi.testclient.TestClient` instance can
swap its own config in.
"""

from __future__ import annotations

from fastapi import Request

from amx.config import AMXConfig
from amx.web.jobs import JobRegistry


def get_cfg(request: Request) -> AMXConfig:
    """Return the active :class:`AMXConfig` bound to the running app.

    The launcher (:func:`amx.web.launcher.launch_studio`) sets
    ``app.state.cfg`` once at startup. Tests construct their own app
    via :func:`amx.web.server.create_app` and pass an in-memory
    config.
    """
    cfg = getattr(request.app.state, "cfg", None)
    if cfg is None:
        # Should be impossible in production — the app factory
        # requires a cfg. Surface as 500 instead of letting an
        # AttributeError leak through.
        raise RuntimeError("AMX Studio is missing its AMXConfig binding.")
    return cfg


def get_jobs(request: Request) -> JobRegistry:
    """Return the singleton :class:`JobRegistry` for this app instance."""
    jobs = getattr(request.app.state, "jobs", None)
    if jobs is None:
        raise RuntimeError("AMX Studio is missing its JobRegistry binding.")
    return jobs


def get_pages_service(request: Request):
    """Return the lazily-built :class:`amx.pages.service.PagesService`.

    Cached on ``app.state.pages_service`` so a single instance is reused
    across requests. The history-store path follows the same resolution
    the rest of AMX uses (``cfg.CONFIG_DIR / history.db``) so the local
    SQLite database picks up ``AMX_CONFIG_DIR`` overrides.
    """
    from pathlib import Path

    svc = getattr(request.app.state, "pages_service", None)
    if svc is not None:
        return svc

    cfg = get_cfg(request)
    from amx.pages._llm import AMXLLMClient
    from amx.pages._resolver import AMXResolver
    from amx.pages.service import PagesService
    from amx.pages.store import PageStore
    from amx.storage.sqlite_store import SQLiteHistoryStore

    config_dir = getattr(cfg, "CONFIG_DIR", str(Path.home() / ".amx"))
    history = SQLiteHistoryStore(Path(config_dir) / "history.db")
    history.init()
    llm = AMXLLMClient(cfg)
    svc = PagesService(
        store=PageStore(history=history),
        llm=llm,
        resolver=AMXResolver(cfg),
        model_name=llm.model_name,
    )
    request.app.state.pages_service = svc
    return svc
