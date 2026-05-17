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
