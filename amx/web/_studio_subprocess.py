"""Child-process entry for ``/studio``.

Spawned by :func:`amx.web.launcher.launch_studio` as a fresh Python
process so uvicorn's asyncio event loop, signal handlers, and stdin
file-descriptor state stay entirely isolated from the parent CLI.

Earlier versions ran uvicorn inside a daemon thread in the parent
process, which left stdin in a flushed-but-non-canonical state on
macOS after Ctrl-C — the next ``prompt_toolkit`` session couldn't
re-enter raw mode and arrow keys echoed as literal ``^[[C``
sequences. Termios-level restoration alone did not recover the
broken state; the only reliable fix is process isolation plus a
prompt_toolkit input rebuild (see :func:`amx.cli_support.session._rebuild_prompt_input`).

The child reads the cfg path + port + token from CLI args, loads
its own :class:`AMXConfig` from disk (so any edits Studio makes are
saved back to YAML and picked up by the parent via the PR-351
``reload_if_stale`` hook), wires explicit SIGINT/SIGTERM handlers
so it shuts down in well under a second (uvicorn's default handler
under uvloop occasionally takes 3+ seconds to react, forcing the
parent to escalate to SIGKILL), and runs uvicorn via
``Server.serve`` so we can drive the shutdown ourselves.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys


def main() -> int:
    parser = argparse.ArgumentParser(prog="amx-studio-subprocess")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--token", type=str, required=True)
    parser.add_argument(
        "--config-path",
        type=str,
        default="",
        help="Optional explicit path to the AMXConfig YAML; falls back to the default lookup.",
    )
    parser.add_argument(
        "--embedded",
        action="store_true",
        help=(
            "Relax framing headers so an IDE host can render Studio "
            "inside a webview iframe. Browser launches omit this."
        ),
    )
    parser.add_argument(
        "--owner",
        type=str,
        default="cli",
        help="Label recorded in the discovery file for who started this server.",
    )
    args = parser.parse_args()

    # Match the in-process launcher's third-party logger muting so the
    # child's stdout stays clean of transformers / litellm / uvicorn
    # noise. mute_root_logger_for_studio is idempotent.
    from amx.utils.logging import mute_root_logger_for_studio

    mute_root_logger_for_studio()

    from amx.config import AMXConfig

    cfg = AMXConfig.load(args.config_path) if args.config_path else AMXConfig.load()

    # Bootstrap the history-store singleton the same way the CLI entry
    # point does. Without this, every /api/ask call lands on the
    # ``Search catalog isn't initialised yet — run /search sync first.``
    # branch even when the SQLite file is fully populated, because
    # ``SearchCatalog.from_history_store()`` consults the global
    # singleton that ``init_history_store`` is responsible for setting.
    # The studio subprocess is its own Python process (PR-X spawns it
    # via ``_studio_subprocess.main``), so the parent CLI's init does
    # not carry over.
    from amx.storage.factory import init_history_store

    try:
        init_history_store(cfg)
    except Exception:
        # Best-effort: a failure here still lets the SPA come up so the
        # user can fix profile / DB issues from Settings. The /ask
        # route surfaces a fresh diagnostic if the catalog is still
        # unreachable when a question is asked.
        pass

    from amx.web.server import create_app

    app = create_app(cfg, token=args.token, embedded=args.embedded)

    import uvicorn

    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=args.port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    # Disable uvicorn's built-in signal install — under uvloop on
    # macOS its default handler routinely takes 3+ seconds to react
    # to SIGINT, which forces the parent launcher to escalate all
    # the way to SIGKILL. We install our own loop-aware handler
    # below so the shutdown flag flips inside the running loop,
    # NOT from a Python signal handler that competes with uvloop.
    server.install_signal_handlers = lambda: None

    # Record this server in <config-dir>/studio.json so other local
    # AMX tooling (a second REPL, an IDE integration) can discover and
    # reuse it instead of spawning a duplicate. Best effort on both
    # ends: a failed write never blocks the launch, and a stale file
    # left by a SIGKILL is filtered out by consumers' health checks.
    from amx.web import discovery

    try:
        discovery.write_discovery(args.port, args.token, owner=args.owner)
    except OSError:
        pass
    try:
        asyncio.run(_serve_with_fast_shutdown(server))
    finally:
        discovery.clear_discovery(pid=os.getpid())
    return 0


async def _serve_with_fast_shutdown(server: uvicorn.Server) -> None:  # noqa: F821
    """Run uvicorn with loop-aware signal handlers.

    Python's synchronous :func:`signal.signal` handler can race with
    uvloop's event loop — the flip to ``server.should_exit`` is
    deferred until the loop next checks bytecode, which under
    in-flight ASGI middleware can take several seconds. Asyncio's
    ``loop.add_signal_handler`` instead schedules the callback
    inside the loop itself, so the flag flips on the next event
    tick. Same outcome, much faster (~100ms vs 3s+ in practice).

    SIGTERM also flips ``force_exit`` so anyio cancel-scopes in the
    middleware stack release immediately instead of waiting for
    pending tasks to drain.
    """
    loop = asyncio.get_running_loop()

    def _trigger_shutdown(signum: int) -> None:
        server.should_exit = True
        if signum == signal.SIGTERM:
            server.force_exit = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _trigger_shutdown, sig)
        except NotImplementedError:  # pragma: no cover - Windows
            # asyncio on Windows doesn't support add_signal_handler;
            # fall back to the synchronous variant for those platforms.
            signal.signal(sig, lambda s, _f, _sig=sig: _trigger_shutdown(_sig))

    try:
        await server.serve()
    except KeyboardInterrupt:  # pragma: no cover - safety net
        pass


if __name__ == "__main__":
    sys.exit(main())
