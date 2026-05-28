"""The ASK fixes reach the MCP surface through shared ToolBox code.

MCP is stateless and cache-only — it has no composer chips, so the
SELECTION CONTEXT block does not apply. But the lineage SQL fix and the
sharpened tool descriptions flow through ``ToolBox.schemas()`` →
``mcp_tool_payloads`` automatically. These assertions lock that in.
"""

from __future__ import annotations

from amx.mcp import tool_bridge
from amx.search._tool_schemas import tool_schemas


def _payloads() -> dict[str, str]:
    return {p["name"]: p["description"] for p in tool_bridge.mcp_tool_payloads(tool_schemas())}


def test_lineage_tools_exposed_over_mcp() -> None:
    payloads = _payloads()
    assert "lineage_for_table" in payloads
    assert "lineage_for_column" in payloads


def test_list_past_runs_history_only_text_reaches_mcp() -> None:
    payloads = _payloads()
    assert "HISTORY ONLY" in payloads["list_past_runs"]


def test_asset_disambiguation_reaches_mcp() -> None:
    payloads = _payloads()
    assert "SELECTED-ASSETS retrieval tool" in payloads["search_assets"]
