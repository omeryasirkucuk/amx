"""AMX Model Context Protocol (MCP) server.

This package exposes AMX's cached catalog metadata to IDE code agents
(Cursor, Claude Desktop, VS Code Copilot) through the Model Context
Protocol. An IDE spawns ``python -m amx.mcp`` as a local stdio
subprocess; the server then surfaces AMX's read-only catalog tools so
the IDE's agent can ground its answers in the real schema, join keys,
lineage, and descriptions AMX already knows about.

Two responsibilities live here and are kept apart:

* **The headless server** (``__main__`` / :mod:`amx.mcp.server`) — what
  the IDE launches over stdio. It is a thin protocol adapter over
  :class:`amx.search.agent_tools.ToolBox`; no new tools are defined.
* **The connection plumbing** (:mod:`amx.mcp.ide_targets` /
  :mod:`amx.mcp.config_writer`) — pure, SDK-free helpers the ``/mcp``
  REPL command and the Studio MCP tab use to write/read each IDE's
  config file and report connection status.

The :mod:`amx.mcp.tool_bridge`, :mod:`amx.mcp.ide_targets`, and
:mod:`amx.mcp.config_writer` modules deliberately avoid importing the
``mcp`` SDK so they remain importable (and testable) on a plain
``pip install amx-cli`` before the SDK is lazily installed.
"""

from __future__ import annotations

__all__ = [
    "tool_bridge",
    "ide_targets",
    "config_writer",
]
