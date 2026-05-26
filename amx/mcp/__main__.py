"""``python -m amx.mcp`` — the entry an IDE spawns over stdio.

The IDE owns this process's stdin/stdout for the MCP protocol, so nothing
here may print to stdout. All setup output (the one-time dependency
install banner, AMX startup logs) is redirected to stderr; only the MCP
server itself writes to the real stdout.

The MCP SDK is normally installed already (the ``/mcp connect`` flow runs
``ensure("mcp")`` interactively before writing the IDE config), so the
``ensure`` call here is a no-op safety net on the spawn path.
"""

from __future__ import annotations

import argparse
import contextlib
import sys


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m amx.mcp",
        description="Serve AMX's read-only catalog to an IDE code agent over MCP (stdio).",
    )
    parser.add_argument(
        "--profiles",
        default=None,
        help="Comma-separated DB profile names to expose. Omit to use the "
        "active profile scope from AMX's config.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    profiles = [p.strip() for p in args.profiles.split(",") if p.strip()] if args.profiles else None

    # Keep stdout pristine for the protocol: route every byte of setup
    # chatter to stderr (visible in the IDE's MCP server log, harmless to
    # the protocol). Restored automatically on block exit, before the
    # server grabs the real stdout.
    with contextlib.redirect_stdout(sys.stderr):
        from amx.config import AMXConfig
        from amx.utils.optional_deps import ensure

        ensure("mcp")
        from amx.mcp import server as mcp_server

        cfg = AMXConfig.load()

    mcp_server.serve_stdio(cfg, profiles)


if __name__ == "__main__":
    main()
