"""Local web UI for AMX (AMX Studio).

The ``/studio`` slash command boots a FastAPI app at ``127.0.0.1:<port>``
in a daemon thread, opens the user's default browser at a token-protected
URL, and stays alive until Ctrl-C in the parent CLI.

The package layout follows the design in docs/studio.md:

* :mod:`amx.web.server` — FastAPI app factory.
* :mod:`amx.web.launcher` — uvicorn lifecycle + browser open + Ctrl-C handling.
* :mod:`amx.web.auth` — token-based middleware (header + ``?t=`` fallback for SSE).
* :mod:`amx.web.jobs` — per-job cancellation tokens + status registry.
* :mod:`amx.web.progress_bus` — per-job thread-safe event queue powering SSE.
* :mod:`amx.web.routers` — routers grouped by capability (system / live_db /
  catalog / runs / pending / apply / ask / history / comments / profiles).

The single public entry point is :func:`launch_studio`, which is what the
``/studio`` slash command in :mod:`amx.cli_support.session` invokes.
"""

from __future__ import annotations

from amx.web.launcher import launch_studio

__all__ = ["launch_studio"]
