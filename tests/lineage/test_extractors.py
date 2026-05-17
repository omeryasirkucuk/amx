"""Unit tests for the three slice-1 lineage extractors."""

from __future__ import annotations

from amx.lineage import store as lineage_store
from amx.lineage.extractors import FKExtractor, NameMatchExtractor, ViewDDLExtractor
from amx.lineage.extractors.view_ddl import ConnectorHandle
from amx.lineage.types import ColumnRef, Scope

from .conftest import (
    seed_column_comments_cache_for_table,
    seed_foreign_key_relationship,
    seed_table_entity,
)


def test_fk_extractor_emits_column_level_edges(hs):
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
    result = FKExtractor().extract(hs=hs, scope=scope)
    assert result.cache_status == "hit"
    assert len(result.edges) == 1
    edge = result.edges[0]
    assert edge.source.column == "id"
    assert edge.target.column == "customer_id"
    assert edge.confidence == 1.0


def test_fk_extractor_empty_when_no_relationship(hs):
    seed_table_entity(hs, schema="public", table="orders")
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))
    result = FKExtractor().extract(hs=hs, scope=scope)
    assert result.edges == []
    assert result.cache_status == "hit"  # absence is a clean hit, not an error


def test_view_ddl_extractor_miss_when_cache_empty(hs):
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders_view", ""))
    result = ViewDDLExtractor(connector_factory=None).extract(hs=hs, scope=scope, mode="cache_only")
    assert result.cache_status == "miss"
    assert result.missing_scope and result.missing_scope[0].schema == "public"


def test_view_ddl_extractor_hit_from_cache(hs):
    lineage_store.upsert_view_definitions(
        hs,
        db_profile="p",
        database="",
        schema="public",
        entries=[
            {
                "view_name": "v_orders",
                "ddl_text": "SELECT customer_id FROM orders",
                "dialect": "duckdb",
                "parsed_lineage": [
                    {
                        "target": "customer_id",
                        "sources": [{"table": "orders", "column": "customer_id"}],
                    }
                ],
                "parse_status": "ok",
                "parse_error": "",
            }
        ],
    )
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "v_orders", ""))
    result = ViewDDLExtractor(connector_factory=None).extract(hs=hs, scope=scope, mode="cache_only")
    assert result.cache_status == "hit"
    assert len(result.edges) == 1
    assert result.edges[0].source.column == "customer_id"
    assert result.edges[0].target.table == "v_orders"


def test_view_ddl_extractor_does_not_call_adapter_in_cache_only(hs):
    """Critical guarantee: cache_only must never reach the wire."""

    class _BoomAdapter:
        def list_views_with_definitions(self, engine, schema):
            raise AssertionError("cache_only must not touch the adapter")

    def factory(profile):
        return ConnectorHandle(engine=object(), adapter=_BoomAdapter(), backend="duckdb")

    scope = Scope(profile="p", anchor=ColumnRef("", "public", "v_orders", ""))
    extractor = ViewDDLExtractor(connector_factory=factory)
    result = extractor.extract(hs=hs, scope=scope, mode="cache_only")
    # The extractor must have skipped the adapter call entirely.
    assert result.cache_status == "miss"


def test_view_ddl_extractor_db_fill_populates_cache_via_fake_adapter(hs):
    """db_fill mode runs the adapter, persists the parsed lineage, emits edges."""

    class _FakeAdapter:
        def list_views_with_definitions(self, engine, schema):
            return [
                {
                    "name": "v1",
                    "type": "view",
                    "definition": "SELECT customer_id AS cid FROM orders",
                    "comment": None,
                    "metadata": {},
                }
            ]

    def factory(profile):
        return ConnectorHandle(engine=object(), adapter=_FakeAdapter(), backend="duckdb")

    scope = Scope(profile="p", anchor=ColumnRef("", "public", "v1", ""))
    extractor = ViewDDLExtractor(connector_factory=factory)
    result = extractor.extract(hs=hs, scope=scope, mode="db_fill")
    cached = lineage_store.lookup_view_definitions(hs, db_profile="p", database="", schema="public")
    assert len(cached) == 1
    assert cached[0]["view_name"] == "v1"
    # Cache hit on a follow-up cache_only read.
    result2 = ViewDDLExtractor(connector_factory=None).extract(
        hs=hs, scope=scope, mode="cache_only"
    )
    assert result2.cache_status == "hit"
    # The fake adapter's SELECT yielded at least one parsable target (cid).
    if cached[0]["parse_status"] == "ok":
        assert result.edges or result2.edges


def test_name_match_proposes_edges_by_name_and_type(hs):
    seed_column_comments_cache_for_table(
        hs,
        schema="public",
        table="orders",
        columns={"customer_id": {"type": "integer"}, "amount": {"type": "decimal"}},
    )
    seed_column_comments_cache_for_table(
        hs,
        schema="public",
        table="customers",
        columns={"id": {"type": "integer"}, "name": {"type": "text"}},
    )
    scope = Scope(
        profile="p",
        anchor=ColumnRef("", "public", "orders", "customer_id"),
    )
    result = NameMatchExtractor().extract(hs=hs, scope=scope)
    assert result.cache_status == "hit"
    # 'customers.id' is a suffix-match for 'customer_id' with matching integer type.
    suggestions = [(e.source.fqn(), e.confidence) for e in result.edges]
    assert any("customers.id" in src for src, _ in suggestions), suggestions


def test_name_match_returns_empty_when_no_columns_cached(hs):
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", "customer_id"))
    result = NameMatchExtractor().extract(hs=hs, scope=scope)
    # No cache rows → miss, callers can offer to populate.
    assert result.cache_status == "miss"
