"""Boot AMX Studio from the ``/studio`` slash command.

The launcher:

1. Picks a port (preferred 47821, otherwise an ephemeral one).
2. Generates a one-shot URL-safe Bearer token.
3. Spawns a **subprocess** running :mod:`amx.web._studio_subprocess`
   which boots uvicorn in a conventional foreground configuration.
4. Polls ``127.0.0.1:<port>`` until the child binds (or the startup
   timeout elapses), then opens the user's default browser.
5. Blocks the parent on ``proc.wait()``; Ctrl-C in the terminal
   reaches the child via the shared process group and uvicorn's own
   SIGINT handler shuts it down cleanly. The parent then escalates
   to SIGTERM / SIGKILL only if the child overstays its grace
   period.

The subprocess model replaces an earlier in-process daemon-thread
implementation that left stdin in a flushed-but-non-canonical state
on macOS — arrow keys echoed as literal ``^[[C`` after Ctrl-C and
the user had to restart the CLI. termios-level restoration (PRs
#353/#354) did not fully recover. Process isolation does, because
uvicorn's asyncio loop, signal handlers, and file-descriptor edits
all live in a separate Python interpreter.

Returns ``True`` once the launch attempt completes (server may or
may not have responded — the URL is still printed) and ``False``
only when the spawn itself fails — the slash-command dispatcher
renders an actionable error in that case.
"""

from __future__ import annotations

import logging
import signal
import socket
import subprocess
import sys
import time
import webbrowser
from contextlib import suppress
from pathlib import Path as _Path

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

    Studio runs as a CHILD PROCESS — see
    :mod:`amx.web._studio_subprocess`. The parent CLI then has zero
    risk of inheriting uvicorn's asyncio loop, signal handler edits,
    or stdin file-descriptor state; Ctrl-C reaches the child via the
    shared process group and the child exits cleanly via its own
    SIGINT handler. After the child exits, the parent's terminal is
    in exactly the state it was in before ``/studio`` and the next
    ``prompt_toolkit`` session resumes normally.

    Earlier in-process daemon-thread implementation left stdin in a
    flushed-but-non-canonical state on macOS — arrow keys echoed as
    literal ``^[[C`` after Ctrl-C and the user had to restart the
    CLI. termios restoration (PRs #353/#354) didn't fully recover.
    Process isolation does.

    Parameters
    ----------
    cfg
        Active AMXConfig. The child loads its own copy from the same
        YAML on disk; edits Studio makes are saved back to YAML and
        picked up by the parent via PR-351 ``reload_if_stale``.
    port
        Override for the listen port. Tests pass an explicit port.
    open_browser
        Disable in headless test or remote SSH where
        ``webbrowser.open_new_tab`` would error or silently launch a
        phantom process.
    block
        When ``True`` (the default), block until the child exits.
        Pass ``False`` in tests so the function returns once the
        child has been spawned (the caller controls shutdown).
    """
    chosen_port = port if port is not None else _pick_port(PREFERRED_PORT)

    # Generate the bearer token in the PARENT so we can immediately
    # surface the URL to the user without round-tripping through the
    # child's app.state.token. The child accepts the token via CLI
    # arg and seeds it onto the FastAPI app at startup.
    from amx.web.auth import generate_token

    token = generate_token()
    url = f"http://127.0.0.1:{chosen_port}/?t={token}"

    config_path = getattr(cfg, "_config_path", "") or str(_Path(cfg.CONFIG_DIR) / "config.yml")

    cmd = [
        sys.executable,
        "-m",
        "amx.web._studio_subprocess",
        "--port",
        str(chosen_port),
        "--token",
        token,
        "--config-path",
        config_path,
    ]
    proc = subprocess.Popen(cmd)

    started = _wait_for_http(chosen_port, STARTUP_TIMEOUT_SEC)
    if not started:
        log.warning(
            "AMX Studio did not respond on :%d within %.1fs; opening the browser anyway.",
            chosen_port,
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
        proc.wait()
    except KeyboardInterrupt:
        # The terminal Ctrl-C reaches the whole foreground process
        # group, so the child has typically already received SIGINT
        # and is shutting down. Send another SIGINT for good measure
        # and then wait briefly for graceful exit before escalating
        # to SIGTERM / SIGKILL.
        with suppress(ProcessLookupError, OSError):
            proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=SHUTDOWN_TIMEOUT_SEC)
        except subprocess.TimeoutExpired:  # pragma: no cover - graceful path
            log.warning(
                "AMX Studio child did not exit within %.1fs; sending SIGTERM.",
                SHUTDOWN_TIMEOUT_SEC,
            )
            proc.terminate()
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                log.warning("AMX Studio child still alive; sending SIGKILL.")
                proc.kill()
                with suppress(subprocess.TimeoutExpired):
                    proc.wait(timeout=1.0)
    print("AMX Studio stopped.")
    return True


def _wait_for_http(port: int, timeout: float) -> bool:
    """Poll a TCP connect on ``127.0.0.1:port`` until it succeeds or
    the deadline elapses.

    Used to confirm the Studio child has bound its listening socket
    before the parent prints the URL and opens the browser. Replaces
    the old daemon-thread ``server.started`` probe — with the child
    in a separate process the parent no longer has direct access to
    uvicorn's internal state, so we fall back to the network signal.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.05)
    return False
