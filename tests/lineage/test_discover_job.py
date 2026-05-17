"""``discover_profile_lineage`` — schema-wide cache-only walk."""

from __future__ import annotations

from amx.lineage.discover import discover_profile_lineage

from .conftest import (
    seed_foreign_key_relationship,
    seed_table_entity,
)


def test_discover_returns_anchors_ranked_by_edge_count(hs):
    orders_id = seed_table_entity(hs, schema="public", table="orders")
    customers_id = seed_table_entity(hs, schema="public", table="customers")
    items_id = seed_table_entity(hs, schema="public", table="items")
    seed_foreign_key_relationship(
        hs,
        from_table_id=orders_id,
        to_table_id=customers_id,
        constrained_columns=["customer_id"],
        referred_columns=["id"],
        referred_table="customers",
    )
    seed_foreign_key_relationship(
        hs,
        from_table_id=orders_id,
        to_table_id=items_id,
        constrained_columns=["item_id"],
        referred_columns=["id"],
        referred_table="items",
    )

    result = discover_profile_lineage(hs=hs, profile="p")

    assert result.tables_examined == 3
    assert result.tables_with_edges >= 1
    # orders has the most FK edges → ranks first.
    assert result.anchors
    assert result.anchors[0].table == "orders"
    assert result.anchors[0].edge_count >= 2
    assert all(a.edge_count > 0 for a in result.anchors)


def test_discover_skips_empty_anchors(hs):
    # No edges seeded — no anchors should be returned.
    seed_table_entity(hs, schema="public", table="lonely_table")
    result = discover_profile_lineage(hs=hs, profile="p")
    assert result.tables_examined == 1
    assert result.tables_with_edges == 0
    assert result.anchors == []


def test_discover_respects_max_tables_cap(hs):
    for i in range(5):
        seed_table_entity(hs, schema="public", table=f"t{i}")
    result = discover_profile_lineage(hs=hs, profile="p", max_tables=3)
    assert result.tables_examined == 3
    # Truncation flag fires because cap < total catalog size.
    assert result.truncated is True
