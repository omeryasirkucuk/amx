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
