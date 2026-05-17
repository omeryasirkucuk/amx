"""Service-layer orchestration: cache-first defaults, fill flow, partial render."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from amx.lineage import service
from amx.lineage.extractors.view_ddl import ConnectorHandle
from amx.lineage.types import ColumnRef, Scope

from .conftest import (
    seed_column_comments_cache_for_table,
    seed_foreign_key_relationship,
    seed_table_entity,
)


@pytest.fixture
def fk_seeded(hs):
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
    seed_column_comments_cache_for_table(
        hs,
        schema="public",
        table="orders",
        columns={"customer_id": {"type": "integer"}},
    )
    seed_column_comments_cache_for_table(
        hs,
        schema="public",
        table="customers",
        columns={"id": {"type": "integer"}},
    )
    return orders_id, customers_id


def _scope_for_orders():
    return Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))


def test_default_skip_flag_keeps_render_partial_when_view_cache_empty(hs, fk_seeded, tmp_path):
    """The 'default skip' contract: no DB hit, partial render flagged."""
    with patch("amx.lineage.service.render_lineage_image", return_value=tmp_path / "x.svg"):
        result = service.create_lineage(
            hs=hs,
            scope=_scope_for_orders(),
            name="orders-default",
            output_path=tmp_path / "x.svg",
            fmt="svg",
            fill_decision="skip",
        )
    assert not result.aborted
    assert result.extractors_partial is True
    assert "fk" in result.extractors_used


def test_fill_decision_abort_returns_without_writing_artifact(hs, fk_seeded, tmp_path):
    from amx.lineage import store as lineage_store

    with patch("amx.lineage.service.render_lineage_image", return_value=tmp_path / "x.svg"):
        result = service.create_lineage(
            hs=hs,
            scope=_scope_for_orders(),
            name="orders-abort",
            output_path=tmp_path / "x.svg",
            fmt="svg",
            fill_decision="abort",
        )
    assert result.aborted
    assert result.artifact_id == 0
    assert lineage_store.list_lineage_artifacts(hs) == []


def test_db_fill_invokes_connector_factory_and_persists_cache(hs, fk_seeded, tmp_path):
    """When the user chooses ``fill``, the connector is opened exactly for the
    extractors with misses and the cache is repopulated."""
    from amx.lineage import store as lineage_store

    class _FakeAdapter:
        def __init__(self):
            self.calls = 0

        def list_views_with_definitions(self, engine, schema):
            self.calls += 1
            return [
                {
                    "name": "v_orders",
                    "type": "view",
                    "definition": "SELECT customer_id FROM orders",
                    "comment": None,
                    "metadata": {},
                }
            ]

    adapter = _FakeAdapter()

    def factory(profile):
        return ConnectorHandle(engine=object(), adapter=adapter, backend="duckdb")

    with patch("amx.lineage.service.render_lineage_image", return_value=tmp_path / "x.svg"):
        result = service.create_lineage(
            hs=hs,
            scope=_scope_for_orders(),
            name="orders-fill",
            output_path=tmp_path / "x.svg",
            fmt="svg",
            fill_decision="fill",
            connector_factory=factory,
        )
    assert not result.aborted
    assert adapter.calls == 1
    cached = lineage_store.lookup_view_definitions(hs, db_profile="p", database="", schema="public")
    assert len(cached) == 1
    assert result.extractors_partial is False


def test_anchor_must_exist_in_catalog(hs, tmp_path):
    with pytest.raises(LookupError):
        service.create_lineage(
            hs=hs,
            scope=Scope(profile="p", anchor=ColumnRef("", "public", "ghost", "")),
            name="ghost",
            output_path=tmp_path / "x.svg",
            fmt="svg",
            fill_decision="skip",
        )


def test_scale_guardrail_blocks_huge_graphs(hs, monkeypatch, tmp_path):
    """Force a tiny hard limit so the synthetic graph trips the guard."""
    orders_id = seed_table_entity(hs, schema="public", table="orders")
    for i in range(5):
        other_id = seed_table_entity(hs, schema="public", table=f"t{i}")
        seed_foreign_key_relationship(
            hs,
            from_table_id=orders_id,
            to_table_id=other_id,
            constrained_columns=[f"c{i}"],
            referred_columns=["id"],
            referred_table=f"t{i}",
        )

    monkeypatch.setattr(service, "HARD_NODE_LIMIT", 2)
    with patch("amx.lineage.service.render_lineage_image", return_value=tmp_path / "x.svg"):
        result = service.create_lineage(
            hs=hs,
            scope=_scope_for_orders(),
            name="huge",
            output_path=tmp_path / "x.svg",
            fmt="svg",
            fill_decision="skip",
        )
    assert result.aborted
    assert "exceeds hard limit" in result.abort_reason
