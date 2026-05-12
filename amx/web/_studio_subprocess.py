"""Child-process entry for ``/studio``.

Spawned by :func:`amx.web.launcher.launch_studio` as a fresh Python
process so uvicorn's asyncio event loop, signal handlers, and stdin
file-descriptor state stay entirely isolated from the parent CLI.

Earlier versions ran uvicorn inside a daemon thread in the parent
process, which left stdin in a flushed-but-non-canonical state on
macOS after Ctrl-C — the next ``prompt_toolkit`` session couldn't
re-enter raw mode and arrow keys echoed as literal ``^[[C``
sequences. termios-level restoration alone did not recover the
broken state; the only reliable fix is process isolation.

The child reads the cfg path + port + token from CLI args, loads
its own :class:`AMXConfig` from disk (so any edits Studio makes are
saved back to YAML and picked up by the parent via the PR-351
``reload_if_stale`` hook), and runs uvicorn in the conventional
blocking ``uvicorn.run`` mode. Ctrl-C from the parent terminal
reaches the child via the shared process group; uvicorn's default
signal handler triggers graceful shutdown; the child exits cleanly
and the parent's ``proc.wait()`` returns.
"""

from __future__ import annotations

import argparse
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

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=args.port,
        log_level="warning",
        access_log=False,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
