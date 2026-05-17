"""SQL ↔ canvas round-trip tests for the new SQL bridge module."""

from __future__ import annotations

import pytest

from amx.lineage import sql_bridge


def test_parse_simple_select_to_canvas():
    out = sql_bridge.parse_select_to_canvas(
        "SELECT id, name FROM public.customers WHERE status = 'active'"
    )
    table_names = sorted(t["table"] for t in out["tables"] if t["id"] != "select:output")
    assert "customers" in table_names
    op_kinds = [o["kind"] for o in out["operators"]]
    assert "filter" in op_kinds
    # An output node always exists so the edge graph has a terminal.
    assert any(t["id"] == "select:output" for t in out["tables"])


def test_parse_select_with_group_by_emits_aggregate():
    out = sql_bridge.parse_select_to_canvas("SELECT country, COUNT(*) FROM orders GROUP BY country")
    op_kinds = [o["kind"] for o in out["operators"]]
    assert "aggregate" in op_kinds


def test_render_canvas_minimal_round_trip():
    canvas = {
        "tables": [
            {
                "id": "public.customers",
                "schema": "public",
                "table": "customers",
                "columns": ["id", "name"],
            },
            {
                "id": "select:output",
                "schema": "",
                "table": "select_output",
                "columns": ["id", "name"],
            },
        ],
        "operators": [{"id": "op:filter:0", "kind": "filter", "expression": "status = 'active'"}],
    }
    sql = sql_bridge.render_canvas_to_sql(canvas)
    assert "SELECT" in sql.upper()
    assert "customers" in sql.lower()
    assert "WHERE" in sql.upper()


def test_render_canvas_empty_tables_raises():
    with pytest.raises(sql_bridge.SqlBridgeError):
        sql_bridge.render_canvas_to_sql({"tables": [], "operators": []})


def test_render_then_parse_round_trip():
    canvas = {
        "tables": [
            {"id": "public.orders", "schema": "public", "table": "orders", "columns": ["id"]},
            {"id": "select:output", "schema": "", "table": "select_output", "columns": ["id"]},
        ],
        "operators": [],
    }
    sql = sql_bridge.render_canvas_to_sql(canvas)
    reparsed = sql_bridge.parse_select_to_canvas(sql)
    tbl_names = [t["table"] for t in reparsed["tables"] if t["id"] != "select:output"]
    assert "orders" in tbl_names
