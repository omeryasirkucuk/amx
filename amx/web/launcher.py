"""Boot AMX Studio from the ``/studio`` slash command.

Architecture (triple-layer defense after PRs #353/#354/#355 each
fixed a layer but left a sibling open):

1. Pick a port (preferred 47821, otherwise an ephemeral one).
2. Generate a one-shot URL-safe Bearer token.
3. Spawn a **subprocess** running :mod:`amx.web._studio_subprocess`
   under ``start_new_session=True`` so the child gets its own
   session, its own process group, and **no controlling tty**. The
   parent's foreground process group keeps Ctrl-C, the child does
   not see it directly; we forward via ``os.killpg`` instead.
4. Redirect the child's stdout and stderr to
   ``~/.amx/logs/studio-<port>.log`` so uvicorn logs and shutdown
   tracebacks land in a file the user can ``tail -f`` instead of
   on the parent's terminal — where they would corrupt
   ``prompt_toolkit``'s rendered state.
5. Poll ``127.0.0.1:<port>`` until the child binds, then open the
   browser.
6. Block the parent on ``proc.wait()``; on Ctrl-C, ``os.killpg``
   the child group with SIGINT and escalate to SIGTERM/SIGKILL on
   timeout. Wrap escalation against a second Ctrl-C so we never
   leak an orphan.

The third layer — :func:`amx.cli_support.session._rebuild_prompt_input`
called by the prompt loop — handles the remaining failure mode
where ``Vt100Input`` has cached a stale ``_fileno`` / parser state.

Returns ``True`` once the launch attempt completes (server may or
may not have responded — the URL is still printed) and ``False``
only when the spawn itself fails — the slash-command dispatcher
renders an actionable error in that case.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import subprocess
import sys
import time
import webbrowser
from contextlib import suppress
from pathlib import Path as _Path
from typing import IO

from amx.config import AMXConfig

log = logging.getLogger("amx.web.launcher")

#: Preferred port. Chosen at random — high enough to avoid common
#: dev servers (3000/5000/8000/8080), low enough to fit any firewall
#: rule. Falls back to an ephemeral port when busy.
PREFERRED_PORT = 47821

#: How long to wait for uvicorn to bind the listening socket before
#: opening the browser. Tarpits past this (rare on localhost) still
#: launch the browser; the user just sees a load spinner.
STARTUP_TIMEOUT_SEC = 5.0

#: Grace period for uvicorn to drain on shutdown after we send
#: SIGINT. With the explicit signal handler in
#: :mod:`amx.web._studio_subprocess`, the child flips
#: ``server.should_exit`` synchronously and uvicorn drains within
#: roughly 300 ms; 1.5 s is a comfortable upper bound for in-flight
#: SSE connections to wrap up before we escalate.
SHUTDOWN_TIMEOUT_SEC = 1.5

#: SIGTERM-to-SIGKILL grace period. The child's SIGTERM handler
#: also flips ``force_exit`` so any in-flight request bypasses the
#: drain wait — this should always succeed quickly.
TERMINATE_TIMEOUT_SEC = 1.0
KILL_TIMEOUT_SEC = 1.0


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


def _studio_log_path(cfg: AMXConfig, port: int) -> _Path:
    """Resolve ``~/.amx/logs/studio-<port>.log`` (creating the dir)."""
    base = getattr(cfg, "CONFIG_DIR", None) or str(_Path.home() / ".amx")
    log_dir = _Path(base) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"studio-{port}.log"


def _signal_child_group(proc: subprocess.Popen, sig: int) -> None:
    """Send ``sig`` to the child's process group.

    The child was spawned with ``start_new_session=True``, so it is
    its own session leader and ``proc.pid`` equals the pgid. Using
    ``killpg`` covers any uvicorn worker subprocesses that may have
    been forked by ``--reload`` etc., even though we don't enable
    those today — costs nothing to be correct.
    """
    with suppress(ProcessLookupError, OSError):
        os.killpg(proc.pid, sig)


def launch_studio(
    cfg: AMXConfig,
    *,
    port: int | None = None,
    open_browser: bool = True,
    block: bool = True,
) -> bool:
    """Start AMX Studio for one ``/studio`` invocation.

    See module docstring for the architecture. The parent never sees
    uvicorn's stdout, never shares a process group with the child,
    and never relies on terminal Ctrl-C reaching the child — every
    aspect of the failing-CLI-after-Ctrl-C symptom is closed at the
    source.

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
        child has been spawned (the caller controls shutdown and is
        responsible for closing the log file).
    """
    chosen_port = port if port is not None else _pick_port(PREFERRED_PORT)

    # Generate the bearer token in the PARENT so we can immediately
    # surface the URL without round-tripping through the child's
    # ``app.state.token``. The child accepts the token via CLI arg
    # and seeds it onto the FastAPI app at startup.
    from amx.web.auth import generate_token

    token = generate_token()
    url = f"http://127.0.0.1:{chosen_port}/?t={token}"

    config_path = getattr(cfg, "_config_path", "") or str(_Path(cfg.CONFIG_DIR) / "config.yml")

    log_path = _studio_log_path(cfg, chosen_port)
    # Line-buffered append so ``tail -f`` shows uvicorn output as it
    # happens, and a second ``/studio`` invocation on the same port
    # appends rather than truncating somebody else's debugging.
    # Lifetime spans the whole ``proc.wait()`` window — closed in the
    # ``finally`` below, so a ``with`` block doesn't fit.
    log_fd: IO[str] = open(log_path, "a", buffering=1, encoding="utf-8")  # noqa: SIM115
    log_fd.write(f"\n--- studio launch {time.strftime('%Y-%m-%d %H:%M:%S')} pid=parent ---\n")
    log_fd.flush()

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
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=log_fd,
        stderr=subprocess.STDOUT,
        # setsid() in the child: new session + new process group +
        # no controlling tty. Terminal-generated SIGINT no longer
        # reaches the child; we forward explicitly via killpg.
        start_new_session=True,
        close_fds=True,
    )

    started = _wait_for_http(chosen_port, STARTUP_TIMEOUT_SEC)

    # Log path also goes to the rotating file logger so a user who
    # needs to debug can find it via ``amx doctor`` or by tailing
    # the logs directory directly. Not printed to the interactive
    # terminal because it adds visual noise for the 99% of users
    # who never need it.
    log.debug("AMX Studio logs at %s", log_path)

    if not started:
        # The server hasn't answered the health check yet. Do NOT fling
        # the browser at a not-ready port — that lands a first-time user
        # on the browser's native "connection refused" page and looks
        # broken. Tell them what's happening and let them open it once
        # it's up, instead of auto-opening into an error.
        log.warning(
            "AMX Studio did not respond on :%d within %.1fs.",
            chosen_port,
            STARTUP_TIMEOUT_SEC,
        )
        print(  # user-facing — keep print, not log
            f"AMX Studio is still starting and didn't respond on :{chosen_port} "
            f"within {STARTUP_TIMEOUT_SEC:.0f}s.\n"
            f"  Open {url} in your browser once it's ready (not auto-opening to "
            "avoid a connection-error page).\n"
            f"  If it never comes up, check the logs at {log_path}.\n"
            "Press Ctrl-C in this terminal to stop AMX Studio."
        )
    else:
        print(  # user-facing — keep print, not log
            f"AMX Studio running → {url}\nPress Ctrl-C in this terminal to stop AMX Studio."
        )
        if open_browser:
            try:
                webbrowser.open_new_tab(url)
            except Exception as exc:  # pragma: no cover - browser launch is best-effort
                log.debug("Could not auto-open browser: %s", exc)

    if not block:
        # Caller (tests) owns the lifecycle; leave the log file open
        # for them to inspect or close.
        return True

    try:
        try:
            proc.wait()
        except KeyboardInterrupt:
            # Child is in its own session — no implicit SIGINT
            # delivery, so forward explicitly. Wrap escalation
            # against a second Ctrl-C so we don't leave an orphan
            # if the user mashes the key twice.
            _shutdown_child(proc)
    finally:
        with suppress(Exception):
            log_fd.close()
    print("AMX Studio stopped.")
    return True


def _shutdown_child(proc: subprocess.Popen) -> None:
    """Send SIGINT to the child group; escalate to SIGTERM then SIGKILL.

    Each escalation is wrapped against a second ``KeyboardInterrupt``
    so a user mashing Ctrl-C never leaves an orphan child process.

    Escalation events go to ``log.debug``, not stdout — with the
    child-side fast-shutdown handler in
    :mod:`amx.web._studio_subprocess` they should never fire in
    normal use. The (rare) cases where they do still write to the
    AMX log file so a curious user can investigate via
    ``~/.amx*/logs/`` without forcing visible noise on every
    Ctrl-C.
    """
    _signal_child_group(proc, signal.SIGINT)
    try:
        proc.wait(timeout=SHUTDOWN_TIMEOUT_SEC)
        return
    except subprocess.TimeoutExpired:
        log.debug(
            "AMX Studio child did not exit within %.1fs; sending SIGTERM.",
            SHUTDOWN_TIMEOUT_SEC,
        )
    except KeyboardInterrupt:  # pragma: no cover - mash-Ctrl-C path
        pass

    _signal_child_group(proc, signal.SIGTERM)
    try:
        proc.wait(timeout=TERMINATE_TIMEOUT_SEC)
        return
    except subprocess.TimeoutExpired:
        log.debug("AMX Studio child still alive after SIGTERM; sending SIGKILL.")
    except KeyboardInterrupt:  # pragma: no cover
        pass

    _signal_child_group(proc, signal.SIGKILL)
    with suppress(subprocess.TimeoutExpired, KeyboardInterrupt):
        proc.wait(timeout=KILL_TIMEOUT_SEC)


def _wait_for_http(port: int, timeout: float) -> bool:
    """Poll a TCP connect on ``127.0.0.1:port`` until it succeeds or
    the deadline elapses.

    Confirms the Studio child has bound its listening socket before
    the parent prints the URL and opens the browser. The parent no
    longer has direct access to uvicorn's ``server.started`` flag
    (it lives in the child interpreter); the network signal is the
    next best thing.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.05)
    return False
