"""The headless MCP server: a thin protocol adapter over ``ToolBox``.

This module builds the read-only ``ToolBox`` and a Model Context Protocol
stdio server whose ``list_tools`` / ``call_tool`` handlers delegate to
:mod:`amx.mcp.tool_bridge`. It defines no tools of its own — every tool
the IDE sees is one AMX already implements for ``/ask``.

The ``mcp`` SDK is imported lazily inside :func:`serve_stdio` so the rest
of the package stays importable on a plain install (the SDK is installed
on demand via :func:`amx.utils.optional_deps.ensure`).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from amx.mcp import tool_bridge
from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.config import AMXConfig
    from amx.search.agent_tools import ToolBox

log = get_logger("mcp.server")

SERVER_NAME = "amx"
SERVER_INSTRUCTIONS = (
    "AMX exposes a data catalog: database schemas, table and column "
    "descriptions, join keys, lineage, and linked docs/code. Use these "
    "tools to ground answers about the user's data before writing SQL or "
    "code. All tools are read-only and answer from AMX's cached catalog."
)


def _catalog_path(cfg: AMXConfig) -> Path:
    """The history/catalog DB path for this AMX install."""
    config_dir = getattr(cfg, "CONFIG_DIR", None) or str(Path.home() / ".amx")
    return Path(config_dir) / "history.db"


def build_toolbox(cfg: AMXConfig, profiles: list[str] | None = None) -> ToolBox:
    """Construct a cache-only ``ToolBox`` for the MCP server.

    ``allow_live_refresh=False`` is the safety boundary: every live-DB
    tool is rejected at ``invoke``, so the server cannot touch the live
    database even if a client names a live-only tool directly.
    """
    from amx.search.agent_tools import ToolBox
    from amx.search.catalog import SearchCatalog

    catalog = SearchCatalog(_catalog_path(cfg))
    return ToolBox(
        cfg,
        catalog,
        db_profiles=profiles or None,
        allow_live_refresh=False,
    )


def _tool_payloads() -> list[dict[str, Any]]:
    """The MCP tool payloads AMX exposes (cache-only subset)."""
    from amx.search.agent_tools import ToolBox

    return tool_bridge.mcp_tool_payloads(ToolBox.schemas())


def serve_stdio(cfg: AMXConfig, profiles: list[str] | None = None) -> None:
    """Run the MCP server over stdio until the client disconnects.

    Blocks for the lifetime of the IDE-spawned subprocess.
    """
    import anyio
    import mcp.types as types
    from mcp.server.lowlevel import Server
    from mcp.server.stdio import stdio_server

    toolbox = build_toolbox(cfg, profiles)
    payloads = _tool_payloads()
    log.info(
        "AMX MCP server starting: %d tools, profiles=%s",
        len(payloads),
        profiles or "active",
    )

    server: Server = Server(SERVER_NAME, instructions=SERVER_INSTRUCTIONS)

    @server.list_tools()
    async def _list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name=p["name"],
                description=p["description"],
                inputSchema=p["inputSchema"],
            )
            for p in payloads
        ]

    @server.call_tool()
    async def _call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
        # ToolBox.invoke is synchronous and CPU/IO-light (cache reads);
        # run it off the event loop so a slow read never blocks the
        # protocol loop.
        text, _is_error = await anyio.to_thread.run_sync(
            tool_bridge.invoke_tool, toolbox, name, arguments
        )
        return [types.TextContent(type="text", text=text)]

    async def _main() -> None:
        try:
            async with stdio_server() as (read_stream, write_stream):
                await server.run(
                    read_stream,
                    write_stream,
                    server.create_initialization_options(),
                )
        finally:
            toolbox.close()

    anyio.run(_main)
