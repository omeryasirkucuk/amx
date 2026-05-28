"""Unit tests for the ToolBox -> MCP translation layer."""

from __future__ import annotations

import json

from amx.mcp import tool_bridge


def _schema(name: str, freshness: str | None) -> dict:
    entry: dict = {
        "type": "function",
        "function": {
            "name": name,
            "description": f"desc {name}",
            "parameters": {"type": "object", "properties": {}},
        },
    }
    if freshness is not None:
        entry["freshness"] = freshness
    return entry


def test_cache_only_filters_live_only():
    schemas = [
        _schema("describe_column", "cache_ok"),
        _schema("sample_column_values", "live_only"),
        _schema("legacy_no_freshness", None),  # defaults to cache_ok
    ]
    names = tool_bridge.exposed_tool_names(schemas)
    assert "describe_column" in names
    assert "legacy_no_freshness" in names
    assert "sample_column_values" not in names


def test_mcp_tool_payload_shape():
    payloads = tool_bridge.mcp_tool_payloads([_schema("describe_column", "cache_ok")])
    assert payloads == [
        {
            "name": "describe_column",
            "description": "desc describe_column",
            "inputSchema": {"type": "object", "properties": {}},
            "annotations": {
                "readOnlyHint": True,
                "idempotentHint": True,
                "openWorldHint": False,
            },
        }
    ]


def test_every_payload_is_marked_read_only():
    """IDE plan / read-only modes gate on ``readOnlyHint``; every exposed
    tool must advertise it (and never the open-world / destructive hints)
    so plan mode will invoke AMX's catalog tools."""
    from amx.search.agent_tools import ToolBox

    payloads = tool_bridge.mcp_tool_payloads(ToolBox.schemas())
    assert payloads, "expected at least one exposed tool"
    for p in payloads:
        ann = p["annotations"]
        assert ann["readOnlyHint"] is True, p["name"]
        assert ann["openWorldHint"] is False, p["name"]
        assert ann["idempotentHint"] is True, p["name"]


def test_annotations_are_not_shared_between_payloads():
    """Each payload owns its annotations dict — mutating one must not
    leak into the shared module constant or sibling payloads."""
    payloads = tool_bridge.mcp_tool_payloads([_schema("a", "cache_ok"), _schema("b", "cache_ok")])
    payloads[0]["annotations"]["readOnlyHint"] = False
    assert payloads[1]["annotations"]["readOnlyHint"] is True
    assert tool_bridge.READ_ONLY_ANNOTATIONS["readOnlyHint"] is True


def test_payload_skips_unnamed_entries():
    bad = {"type": "function", "freshness": "cache_ok", "function": {"description": "x"}}
    assert tool_bridge.mcp_tool_payloads([bad]) == []


class _FakeToolBox:
    def __init__(self, payload: dict):
        self._payload = payload
        self.calls: list[tuple[str, str]] = []

    def invoke(self, name: str, raw_arguments: str) -> str:
        self.calls.append((name, raw_arguments))
        return json.dumps(self._payload)


def test_invoke_tool_serializes_args_and_passes_through():
    tb = _FakeToolBox({"rows": 3})
    text, is_error = tool_bridge.invoke_tool(tb, "describe_table", {"table": "orders"})
    assert is_error is False
    assert json.loads(text) == {"rows": 3}
    # arguments were JSON-serialized for ToolBox.invoke
    assert tb.calls == [("describe_table", json.dumps({"table": "orders"}))]


def test_invoke_tool_flags_error_envelope():
    tb = _FakeToolBox({"error": "boom"})
    _text, is_error = tool_bridge.invoke_tool(tb, "x", {})
    assert is_error is True


def test_invoke_tool_flags_needs_live_refresh():
    tb = _FakeToolBox({"needs_live_refresh": True, "tool": "x"})
    _text, is_error = tool_bridge.invoke_tool(tb, "x", {})
    assert is_error is True


def test_invoke_tool_handles_none_arguments():
    tb = _FakeToolBox({"ok": 1})
    text, is_error = tool_bridge.invoke_tool(tb, "x", None)
    assert is_error is False
    assert tb.calls == [("x", "{}")]


def test_real_toolbox_schemas_have_no_live_only_leak():
    """Against the real schema set: every exposed tool is cache_ok, and
    a known live-only tool is filtered out."""
    from amx.search.agent_tools import ToolBox

    names = tool_bridge.exposed_tool_names(ToolBox.schemas())
    assert "describe_column" in names
    assert "search_tables_by_concept" in names
    # Known live-only tools must never be exposed.
    for live in ("sample_column_values", "check_uniqueness", "inspect_data_quality"):
        assert live not in names
    assert len(names) >= 10
