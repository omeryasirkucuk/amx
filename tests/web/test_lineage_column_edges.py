"""v4 S1 — column-level POST /edges + POST /operators + PATCH /operators."""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.config import DBConfig
from amx.storage import sqlite_store as sqlite_store_module
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def seeded_hs(tmp_path: Path, monkeypatch):
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_entities "
            "(db_profile, db_backend, database_name, schema_name, table_name, "
            "entity_kind, asset_kind) VALUES (?,?,?,?,?,?,?)",
            ("local", "postgresql", "", "public", "orders", "table", "table"),
        )
        conn.execute(
            "INSERT INTO catalog_entities "
            "(db_profile, db_backend, database_name, schema_name, table_name, "
            "entity_kind, asset_kind) VALUES (?,?,?,?,?,?,?)",
            ("local", "postgresql", "", "public", "customers", "table", "table"),
        )
        conn.execute(
            "INSERT INTO catalog_entities "
            "(db_profile, db_backend, database_name, schema_name, table_name, "
            "entity_kind, asset_kind) VALUES (?,?,?,?,?,?,?)",
            ("local", "postgresql", "", "public", "daily_totals", "table", "table"),
        )
    monkeypatch.setattr(sqlite_store_module, "_store", hs, raising=False)
    return hs


@pytest.fixture()
def cfg(cfg):
    cfg.db_profiles = {"local": DBConfig(backend="postgresql", database="")}
    cfg.active_db_profile = "local"
    return cfg


def test_post_edge_persists_columns_from_4_part_fqn(seeded_hs, client, auth_headers):
    r = client.post(
        "/api/lineage/edges",
        headers=auth_headers,
        json={
            "profile": "local",
            "source_fqn": "public.customers.id",
            "target_fqn": "public.orders.customer_id",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["from_column"] == "id"
    assert body["to_column"] == "customer_id"

    with seeded_hs._connect() as conn:
        row = conn.execute(
            "SELECT from_column, to_column FROM catalog_relationships WHERE id = ?",
            (body["id"],),
        ).fetchone()
    assert row[0] == "id"
    assert row[1] == "customer_id"


def test_post_edge_falls_back_to_table_level_when_no_column(seeded_hs, client, auth_headers):
    r = client.post(
        "/api/lineage/edges",
        headers=auth_headers,
        json={
            "profile": "local",
            "source_fqn": "public.customers",
            "target_fqn": "public.orders",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["from_column"] == ""
    assert body["to_column"] == ""


def test_post_edge_body_column_overrides_fqn(seeded_hs, client, auth_headers):
    r = client.post(
        "/api/lineage/edges",
        headers=auth_headers,
        json={
            "profile": "local",
            "source_fqn": "public.customers.id",
            "target_fqn": "public.orders.customer_id",
            "source_column": "primary_key",
            "target_column": "fk_customer",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["from_column"] == "primary_key"
    assert body["to_column"] == "fk_customer"


def test_post_operator_chains_two_edges(seeded_hs, client, auth_headers):
    r = client.post(
        "/api/lineage/operators",
        headers=auth_headers,
        json={
            "profile": "local",
            "source_fqn": "public.orders.amount",
            "target_fqn": "public.daily_totals.gross",
            "op_kind": "aggregate",
            "expression": "SUM(amount)",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["operator_id"] > 0
    assert body["op_kind"] == "aggregate"
    assert body["expression"] == "SUM(amount)"
    assert len(body["edge_ids"]) == 2

    with seeded_hs._connect() as conn:
        op_count = conn.execute(
            "SELECT COUNT(*) FROM catalog_entities WHERE entity_kind = 'operator'"
        ).fetchone()[0]
        edge_count = conn.execute("SELECT COUNT(*) FROM catalog_relationships").fetchone()[0]
    assert op_count == 1
    assert edge_count == 2


def test_post_operator_rejects_invalid_op_kind(seeded_hs, client, auth_headers):
    r = client.post(
        "/api/lineage/operators",
        headers=auth_headers,
        json={
            "profile": "local",
            "source_fqn": "public.orders.amount",
            "target_fqn": "public.daily_totals.gross",
            "op_kind": "frobnicate",
            "expression": "x",
        },
    )
    assert r.status_code == 400


def test_patch_operator_updates_expression(seeded_hs, client, auth_headers):
    create = client.post(
        "/api/lineage/operators",
        headers=auth_headers,
        json={
            "profile": "local",
            "source_fqn": "public.orders.amount",
            "target_fqn": "public.daily_totals.gross",
            "op_kind": "aggregate",
            "expression": "SUM(amount)",
        },
    )
    assert create.status_code == 201, create.text
    op_id = create.json()["operator_id"]

    r = client.patch(
        f"/api/lineage/operators/{op_id}",
        headers=auth_headers,
        json={"expression": "SUM(amount * fx_rate)"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["expression"] == "SUM(amount * fx_rate)"


def test_patch_operator_404_when_missing(seeded_hs, client, auth_headers):
    r = client.patch(
        "/api/lineage/operators/99999",
        headers=auth_headers,
        json={"expression": "x"},
    )
    assert r.status_code == 404
