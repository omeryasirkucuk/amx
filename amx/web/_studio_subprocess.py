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
    args = parser.parse_args()

    # Match the in-process launcher's third-party logger muting so the
    # child's stdout stays clean of transformers / litellm / uvicorn
    # noise. mute_root_logger_for_studio is idempotent.
    from amx.utils.logging import mute_root_logger_for_studio

    mute_root_logger_for_studio()

    from amx.config import AMXConfig

    cfg = AMXConfig.load(args.config_path) if args.config_path else AMXConfig.load()

    from amx.web.server import create_app

    app = create_app(cfg, token=args.token)

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
    # the way to SIGKILL. We register a synchronous handler that
    # flips ``server.should_exit`` immediately, so the next loop
    # iteration drains in-flight requests and exits cleanly.
    server.install_signal_handlers = lambda: None

    def _handle_shutdown(signum: int, _frame: object) -> None:
        # ``should_exit`` is uvicorn's documented graceful-shutdown
        # flag; ``force_exit`` short-circuits in-flight request
        # waiting so SIGTERM feels snappy when the user is impatient.
        server.should_exit = True
        if signum == signal.SIGTERM:
            server.force_exit = True

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    try:
        asyncio.run(server.serve())
    except KeyboardInterrupt:
        # Last-ditch safety net; the signal handler above usually
        # arrives first and lets ``serve()`` return normally.
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
