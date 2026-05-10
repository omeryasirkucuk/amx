"""Boot AMX Studio from the ``/studio`` slash command.

The launcher:

1. Picks a port (preferred 47821, otherwise an ephemeral one).
2. Generates a one-shot URL-safe token.
3. Starts uvicorn in a daemon thread — uvicorn's signal handlers are
   intentionally suppressed so Ctrl-C in the parent CLI shows up here
   instead of being swallowed by the server.
4. Waits for the server to report ``started``, then opens the user's
   default browser at ``http://127.0.0.1:<port>/?t=<token>``.
5. Blocks the calling thread until Ctrl-C, then signals uvicorn to
   exit and joins the worker thread.

Returns ``True`` when the server actually came up and ``False`` when
launch failed early (port unavailable, FastAPI extras missing, …) so
the slash-command dispatcher can render an actionable error.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
import webbrowser
from typing import Any

from amx.config import AMXConfig

log = logging.getLogger("amx.web.launcher")

#: Preferred port. Chosen at random — high enough to avoid common
#: dev servers (3000/5000/8000/8080), low enough to fit any firewall
#: rule. Falls back to an ephemeral port when busy.
PREFERRED_PORT = 47821

#: How long to wait for uvicorn to flip ``server.started`` to True
#: before opening the browser. Tarpits past this (rare on localhost)
#: still launch the browser, the user just sees a load spinner.
STARTUP_TIMEOUT_SEC = 5.0

#: Grace period for uvicorn to drain on shutdown. After this we move
#: on so Ctrl-C feels responsive even if a hung connection lingers.
SHUTDOWN_TIMEOUT_SEC = 3.0


def _pick_port(preferred: int) -> int:
    """Return ``preferred`` if free, otherwise an OS-allocated port.

    Uses ``SO_REUSEADDR`` while probing so a recently-closed studio
    on the same port doesn't make us look elsewhere unnecessarily.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", preferred))
        except OSError:
            sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def launch_studio(
    cfg: AMXConfig,
    *,
    port: int | None = None,
    open_browser: bool = True,
    block: bool = True,
) -> bool:
    """Start AMX Studio for one ``/studio`` invocation.

    Parameters
    ----------
    cfg
        Active AMXConfig. The same in-memory instance the parent CLI
        is using — config edits made through the UI take effect for
        the rest of the REPL session.
    port
        Override for the listen port. Tests pass an explicit port to
        avoid colliding with a real running studio.
    open_browser
        Disable when running in a headless test or a remote SSH
        session where ``webbrowser.open_new_tab`` would error or
        silently launch a phantom process.
    block
        When ``True`` (the default), the function blocks until the
        user hits Ctrl-C and joins uvicorn cleanly. Pass ``False`` in
        tests so the function returns once the server is reachable
        and the caller controls shutdown.
    """
    from amx.utils.optional_deps import ensure

    try:
        ensure(
            [
                "fastapi",
                ("uvicorn", "uvicorn[standard]"),
                ("sse_starlette", "sse-starlette"),
                # FastAPI's ``Form(...)`` / ``File(...)`` parsers raise
                # ``RuntimeError: Form data requires "python-multipart"
                # to be installed`` the first time a multipart route is
                # hit (e.g. the doc-upload drag-drop endpoint in
                # ``amx/web/routers/docs_ops.py``). Wheel name is
                # ``python-multipart``; modern versions (>=0.0.12)
                # expose the importable module as ``python_multipart``.
                ("python_multipart", "python-multipart"),
            ],
            feature="AMX Studio (/studio)",
        )
    except RuntimeError as exc:
        log.error("AMX Studio dependencies could not be installed: %s", exc)
        return False
    import uvicorn

    # Silence the parent CLI terminal for the duration of the Studio
    # session. Third-party imports (transformers, litellm, bert-score,
    # uvicorn[standard]) install root logger handlers at INFO/DEBUG;
    # without this, amx.* records propagate to those handlers and the
    # REPL gets flooded with progress noise the file log already
    # captures. See amx.utils.logging.mute_root_logger_for_studio.
    from amx.utils.logging import mute_root_logger_for_studio

    mute_root_logger_for_studio()

    # Pre-install drivers for every saved DB profile BEFORE uvicorn
    # starts. A web request triggering pip-install mid-flight would
    # hang the request for 10–30 s while the browser shows a
    # never-resolving spinner; doing it up front means the user sees
    # the progress in the launching terminal and Studio is fully
    # responsive once the page loads.
    from amx.db.drivers import ensure_backend_driver

    seen_backends: set[str] = set()
    for profile in cfg.db_profiles.values():
        backend = (getattr(profile, "backend", "") or "").strip()
        if backend and backend not in seen_backends:
            seen_backends.add(backend)
            try:
                ensure_backend_driver(backend)
            except RuntimeError as exc:
                log.error(
                    "Could not install driver for backend %r: %s. "
                    "Studio will start, but operations against this "
                    "profile will surface a clearer error in the UI.",
                    backend,
                    exc,
                )

    from amx.web.server import create_app

    chosen_port = port if port is not None else _pick_port(PREFERRED_PORT)
    app = create_app(cfg)
    token = app.state.token
    url = f"http://127.0.0.1:{chosen_port}/?t={token}"

    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=chosen_port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    # Parent CLI keeps the SIGINT/SIGTERM handlers; uvicorn would
    # otherwise install its own and swallow Ctrl-C from the prompt.
    server.install_signal_handlers = lambda: None

    server_thread = threading.Thread(target=server.run, name="amx-studio-uvicorn", daemon=True)
    server_thread.start()

    started = _wait_for_startup(server, STARTUP_TIMEOUT_SEC)
    if not started:
        log.warning(
            "AMX Studio did not report startup within %.1fs; opening the browser anyway.",
            STARTUP_TIMEOUT_SEC,
        )

    print(  # user-facing — keep print, not log
        f"AMX Studio running → {url}\nPress Ctrl-C in this terminal to stop AMX Studio."
    )
    if open_browser:
        try:
            webbrowser.open_new_tab(url)
        except Exception as exc:  # pragma: no cover - browser launch is best-effort
            log.debug("Could not auto-open browser: %s", exc)

    if not block:
        return True

    try:
        while server_thread.is_alive():
            try:
                server_thread.join(timeout=0.5)
            except KeyboardInterrupt:
                break
    except KeyboardInterrupt:
        pass
    finally:
        server.should_exit = True
        server_thread.join(timeout=SHUTDOWN_TIMEOUT_SEC)
        if server_thread.is_alive():  # pragma: no cover - graceful shutdown is best-effort
            log.warning(
                "AMX Studio uvicorn thread did not exit within %.1fs; leaving it as daemon.",
                SHUTDOWN_TIMEOUT_SEC,
            )
        else:
            print("AMX Studio stopped.")
    return True


def _wait_for_startup(server: Any, timeout: float) -> bool:
    """Spin until ``server.started`` flips True or the timeout elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if getattr(server, "started", False):
            return True
        time.sleep(0.05)
    return getattr(server, "started", False)
