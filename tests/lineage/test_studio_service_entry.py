"""Studio service entry: ``lineage_for_studio`` JSON shape."""

from __future__ import annotations

from amx.lineage.service import lineage_for_studio
from amx.lineage.types import ColumnRef, Scope

from .conftest import (
    seed_foreign_key_relationship,
    seed_table_entity,
)


def test_lineage_for_studio_returns_expected_json_shape(hs):
    orders_id = seed_table_entity(hs, schema="public", table="orders")
    customers_id = seed_table_entity(hs, schema="public", table="customers")
    seed_foreign_key_relationship(
        hs,
        from_table_id=orders_id,
        to_table_id=customers_id,
        constrained_columns=["customer_id"],
        referred_columns=["id"],
        referred_table="customers",
    )
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))

    payload = lineage_for_studio(hs=hs, scope=scope)

    assert payload["anchor"] == {
        "database": "",
        "schema": "public",
        "table": "orders",
        "column": None,
    }
    assert isinstance(payload["nodes"], list) and len(payload["nodes"]) >= 2
    assert isinstance(payload["edges"], list) and len(payload["edges"]) >= 1
    assert "extractors_used" in payload
    assert "fk" in payload["extractors_used"]
    assert "partial" in payload
    assert "generated_at" in payload
    anchor_node = next(n for n in payload["nodes"] if n["anchor"])
    assert anchor_node["label"].endswith("orders")


def test_lineage_for_studio_only_anchor_when_catalog_empty(hs):
    seed_table_entity(hs, schema="public", table="orders")
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))
    payload = lineage_for_studio(hs=hs, scope=scope)
    assert len(payload["nodes"]) == 1
    assert payload["edges"] == []


def test_lineage_for_studio_edges_carry_column_and_operator_fields(hs):
    """v4 — edges now expose from_column/to_column always, operator
    when the extractor saw a transform on that flow."""
    orders_id = seed_table_entity(hs, schema="public", table="orders")
    customers_id = seed_table_entity(hs, schema="public", table="customers")
    seed_foreign_key_relationship(
        hs,
        from_table_id=orders_id,
        to_table_id=customers_id,
        constrained_columns=["customer_id"],
        referred_columns=["id"],
        referred_table="customers",
    )
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))
    payload = lineage_for_studio(hs=hs, scope=scope)
    fk_edges = [e for e in payload["edges"] if e["extractor"] == "fk"]
    assert fk_edges, "expected at least one FK-sourced edge"
    e = fk_edges[0]
    # Column round-trip — FK columns flow through to the payload.
    assert {e["from_column"], e["to_column"]} == {"customer_id", "id"}
    # Plain FK has no operator wrapper.
    assert "operator" not in e


def test_lineage_for_studio_splits_operator_edges_into_three_nodes(hs, monkeypatch):
    """v4 S3 — when an extractor flags an operator on an edge, the
    studio payload synthesises a third node and emits two edges so
    the canvas can draw the operator as a first-class shape."""
    from amx.lineage.extractors.fk import FKExtractor
    from amx.lineage.types import Edge, ExtractResult, OperatorMeta

    seed_table_entity(hs, schema="public", table="orders")
    seed_table_entity(hs, schema="public", table="daily_totals")

    op_meta = OperatorMeta(op_kind="aggregate", expression="SUM(amount)")
    fake_edge = Edge(
        source=ColumnRef("", "public", "orders", "amount"),
        target=ColumnRef("", "public", "daily_totals", "gross"),
        relationship_type="lineage_view_ddl",
        extractor="view_ddl",
        confidence=1.0,
        evidence="view daily_totals",
        operator=op_meta,
    )

    def fake_extract(self, *, hs, scope, mode="cache_only"):
        return ExtractResult(edges=[fake_edge], cache_status="hit")

    monkeypatch.setattr(FKExtractor, "extract", fake_extract)

    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))
    payload = lineage_for_studio(hs=hs, scope=scope)

    op_nodes = [n for n in payload["nodes"] if n["kind"] == "operator"]
    assert len(op_nodes) == 1
    op_node = op_nodes[0]
    assert op_node["op_kind"] == "aggregate"
    assert op_node["expression"] == "SUM(amount)"

    # The original edge is split into two halves.
    op_in = [e for e in payload["edges"] if e.get("role") == "operator_input"]
    op_out = [e for e in payload["edges"] if e.get("role") == "operator_output"]
    assert len(op_in) == 1
    assert len(op_out) == 1
    assert op_in[0]["to"] == op_node["id"]
    assert op_out[0]["from"] == op_node["id"]
    assert op_out[0]["operator"]["op_kind"] == "aggregate"
