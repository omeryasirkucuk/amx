"""Translate AMX's ``ToolBox`` tool surface into MCP terms.

This module is the *only* place that maps between AMX's OpenAI-style
function schemas (produced by :meth:`amx.search.agent_tools.ToolBox.schemas`)
and the Model Context Protocol's ``list_tools`` / ``call_tool`` shapes.

It is intentionally SDK-free: the functions here operate on plain
dicts and a duck-typed ``toolbox`` object, so they import and unit-test
without the ``mcp`` package installed. :mod:`amx.mcp.server` converts
the plain dicts returned here into the SDK's ``types.Tool`` objects.

Safety boundary
---------------
Only tools annotated ``freshness == "cache_ok"`` are exposed. AMX's
live-DB tools (``sample_column_values``, ``check_uniqueness``, …) carry
``freshness == "live_only"`` and are filtered out here, so the MCP
client never even sees a tool that would touch the live database. This
is belt-and-braces on top of constructing the ``ToolBox`` with
``allow_live_refresh=False`` (which already rejects live-only calls at
``invoke``); the two together guarantee read-only, cache-only behavior.
"""

from __future__ import annotations

import json
from typing import Any, Protocol

# Re-exported so callers and tests agree on the literal without importing
# the schema module's private constant by a different name.
FRESHNESS_CACHE_OK = "cache_ok"

# Read-only behavioural hints attached to every exposed tool. The MCP
# server only ever surfaces ``cache_ok`` tools and builds its ``ToolBox``
# with ``allow_live_refresh=False``, so the whole surface is read-only,
# idempotent, and answers from a local cache (never the open web). IDE
# plan / read-only / ask modes (Cursor, VS Code Copilot, Claude Code,
# Claude Desktop) gate tool invocation on ``readOnlyHint``: without it a
# tool is assumed to possibly mutate state and is blocked in plan mode.
# Advertising these hints lets those modes call AMX's catalog tools.
READ_ONLY_ANNOTATIONS: dict[str, bool] = {
    "readOnlyHint": True,
    "idempotentHint": True,
    "openWorldHint": False,
}


class _ToolBoxLike(Protocol):
    """The slice of :class:`ToolBox` this module depends on."""

    def invoke(self, name: str, raw_arguments: str) -> str: ...


def cache_only_schemas(schemas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return only the ``cache_ok`` entries from a full schema list.

    Entries with no ``freshness`` key default to ``cache_ok`` to match
    :meth:`ToolBox._is_live_only_tool`, which treats a missing annotation
    as cache-safe.
    """
    return [
        entry
        for entry in schemas
        if entry.get("freshness", FRESHNESS_CACHE_OK) == FRESHNESS_CACHE_OK
    ]


def mcp_tool_payloads(schemas: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert AMX function schemas into MCP tool payloads.

    Each returned dict has the fields the MCP ``Tool`` type needs:
    ``name``, ``description``, ``inputSchema`` (the JSON Schema for the
    arguments), and ``annotations`` (read-only behavioural hints — see
    :data:`READ_ONLY_ANNOTATIONS`). Only ``cache_ok`` tools are included.

    Returning plain dicts (rather than ``mcp.types.Tool``) keeps this
    function importable without the SDK; :mod:`amx.mcp.server` does the
    final wrap.
    """
    payloads: list[dict[str, Any]] = []
    for entry in cache_only_schemas(schemas):
        fn = entry.get("function") or {}
        name = fn.get("name")
        if not name:
            continue
        parameters = fn.get("parameters") or {"type": "object", "properties": {}}
        payloads.append(
            {
                "name": name,
                "description": (fn.get("description") or "").strip(),
                "inputSchema": parameters,
                # Copied per payload so a caller mutating one tool's hints
                # cannot bleed into the shared module-level constant.
                "annotations": dict(READ_ONLY_ANNOTATIONS),
            }
        )
    return payloads


def exposed_tool_names(schemas: list[dict[str, Any]]) -> list[str]:
    """Names of the tools that would be exposed over MCP, in schema order."""
    return [p["name"] for p in mcp_tool_payloads(schemas)]


def invoke_tool(
    toolbox: _ToolBoxLike,
    name: str,
    arguments: dict[str, Any] | None,
) -> tuple[str, bool]:
    """Run one tool and return ``(text, is_error)``.

    ``arguments`` is the MCP argument object (a dict). It is serialized to
    the JSON string ``ToolBox.invoke`` expects. The returned ``text`` is
    the tool's JSON result string verbatim; ``is_error`` is ``True`` when
    AMX wrapped the result in an ``{"error": ...}`` envelope (or a
    ``needs_live_refresh`` rejection), so the server can mark the MCP tool
    result as an error rather than a normal payload.
    """
    raw = json.dumps(arguments or {})
    result = toolbox.invoke(name, raw)
    is_error = False
    try:
        parsed = json.loads(result)
    except (TypeError, ValueError):
        parsed = None
    if isinstance(parsed, dict) and ("error" in parsed or parsed.get("needs_live_refresh")):
        is_error = True
    return result, is_error
