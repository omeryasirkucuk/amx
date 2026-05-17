"""v4 S4 — GET /api/lineage/column-trace/{anchor}?column= server-side BFS."""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.config import DBConfig
from amx.lineage.operator_ops import create_operator_with_edges, write_column_edge
from amx.storage import sqlite_store as sqlite_store_module
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def seeded_hs(tmp_path: Path, monkeypatch):
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    with hs._connect() as conn:
        for tbl in ("orders", "customers", "daily_totals", "monthly_rollup"):
            conn.execute(
                "INSERT INTO catalog_entities "
                "(db_profile, db_backend, database_name, schema_name, table_name, "
                "entity_kind, asset_kind) VALUES (?,?,?,?,?,?,?)",
                ("local", "postgresql", "", "public", tbl, "table", "table"),
            )
    monkeypatch.setattr(sqlite_store_module, "_store", hs, raising=False)
    return hs


@pytest.fixture()
def cfg(cfg):
    cfg.db_profiles = {"local": DBConfig(backend="postgresql", database="")}
    cfg.active_db_profile = "local"
    return cfg


def _ids(hs: SQLiteHistoryStore, *names: str) -> list[int]:
    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT table_name, id FROM catalog_entities WHERE table_name IN ({})".format(
                ",".join("?" for _ in names)
            ),
            names,
        ).fetchall()
    by_name = {r[0]: int(r[1]) for r in rows}
    return [by_name[n] for n in names]


def test_trace_returns_empty_steps_for_unconnected_column(seeded_hs, client, auth_headers):
    r = client.get(
        "/api/lineage/column-trace/public.orders",
        headers=auth_headers,
        params={"profile": "local", "column": "customer_id"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 0
    assert body["steps"] == []


def test_trace_walks_two_hop_chain(seeded_hs, client, auth_headers):
    orders_id, customers_id, daily_id = _ids(seeded_hs, "orders", "customers", "daily_totals")
    # customers.id -> orders.customer_id (FK style)
    write_column_edge(
        seeded_hs,
        from_entity_id=customers_id,
        from_column="id",
        to_entity_id=orders_id,
        to_column="customer_id",
        relationship_type="lineage_fk",
        score=1.0,
        source="database",
    )
    # orders.customer_id -> daily_totals.gross (manual hop)
    write_column_edge(
        seeded_hs,
        from_entity_id=orders_id,
        from_column="customer_id",
        to_entity_id=daily_id,
        to_column="gross",
        relationship_type="lineage_manual",
        score=1.0,
        source="manual",
    )

    r = client.get(
        "/api/lineage/column-trace/public.daily_totals",
        headers=auth_headers,
        params={"profile": "local", "column": "gross"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    fqns = [s["fqn"] for s in body["steps"]]
    assert "public.orders.customer_id" in fqns
    assert "public.customers.id" in fqns
    assert body["count"] >= 2


def test_trace_surfaces_operator_step(seeded_hs, client, auth_headers):
    orders_id, daily_id = _ids(seeded_hs, "orders", "daily_totals")
    create_operator_with_edges(
        seeded_hs,
        profile="local",
        db_backend="postgresql",
        source_entity_id=orders_id,
        source_column="amount",
        target_entity_id=daily_id,
        target_column="gross",
        target_database="",
        target_schema="public",
        target_table="daily_totals",
        op_kind="aggregate",
        expression="SUM(amount)",
    )
    r = client.get(
        "/api/lineage/column-trace/public.daily_totals",
        headers=auth_headers,
        params={"profile": "local", "column": "gross"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    op_steps = [s for s in body["steps"] if s["kind"] == "operator"]
    assert len(op_steps) == 1
    assert op_steps[0]["operator"]["op_kind"] == "aggregate"
    assert "SUM" in op_steps[0]["operator"]["expression"].upper()


def test_trace_404_when_anchor_missing(seeded_hs, client, auth_headers):
    r = client.get(
        "/api/lineage/column-trace/public.unknown",
        headers=auth_headers,
        params={"profile": "local", "column": "x"},
    )
    assert r.status_code == 404


def test_trace_downstream_direction(seeded_hs, client, auth_headers):
    customers_id, orders_id = _ids(seeded_hs, "customers", "orders")
    write_column_edge(
        seeded_hs,
        from_entity_id=customers_id,
        from_column="id",
        to_entity_id=orders_id,
        to_column="customer_id",
        relationship_type="lineage_fk",
        score=1.0,
        source="database",
    )
    r = client.get(
        "/api/lineage/column-trace/public.customers",
        headers=auth_headers,
        params={"profile": "local", "column": "id", "direction": "downstream"},
    )
    assert r.status_code == 200
    fqns = [s["fqn"] for s in r.json()["steps"]]
    assert "public.orders.customer_id" in fqns


def test_trace_caps_at_max_depth(seeded_hs, client, auth_headers):
    """Build a 5-hop chain a→b→c→d→e and request max_depth=2."""
    ids = []
    with seeded_hs._connect() as conn:
        for letter in ("a", "b", "c", "d", "e"):
            cur = conn.execute(
                "INSERT INTO catalog_entities "
                "(db_profile, db_backend, database_name, schema_name, table_name, "
                "entity_kind, asset_kind) VALUES (?,?,?,?,?,?,?)",
                ("local", "postgresql", "", "public", f"chain_{letter}", "table", "table"),
            )
            ids.append(int(cur.lastrowid))
    for i in range(len(ids) - 1):
        write_column_edge(
            seeded_hs,
            from_entity_id=ids[i],
            from_column="x",
            to_entity_id=ids[i + 1],
            to_column="x",
            relationship_type="lineage_manual",
            score=1.0,
            source="manual",
        )
    r = client.get(
        "/api/lineage/column-trace/public.chain_e",
        headers=auth_headers,
        params={"profile": "local", "column": "x", "max_depth": 2},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["count"] <= 2
    assert body["truncated"] is True
