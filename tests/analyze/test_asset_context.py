"""Tests for ``resolve_asset_context_for_run``.

Seeds a minimal history-store fixture (catalog_entities table + asset
bridge rows + asset_references_table edges + remote_* rows) and
verifies that the resolver:

* loads per-kind text excerpts from the right table,
* maps each asset to the (schema, table) tuples it references via the
  bridge + edge JOIN,
* tolerates malformed refs and missing rows by returning empty blocks.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from amx.analyze.asset_context import AssetRef, resolve_asset_context_for_run
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _seed_table_entity(store: SQLiteHistoryStore, profile: str, schema: str, table: str) -> int:
    with store._lock, store._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, schema_name, table_name, "
            "entity_kind, dtype, pk_flag, fk_flag, updated_at) "
            "VALUES (?, ?, ?, 'table', '', 0, 0, 0)",
            (profile, schema, table),
        )
        return int(cur.lastrowid or 0)


def _seed_notebook_with_bridge(
    store: SQLiteHistoryStore,
    profile: str,
    name: str,
    source: str,
    referenced_entity_ids: list[int],
) -> int:
    """Insert remote_notebooks row + catalog_entities bridge + edges."""
    with store._lock, store._connect() as conn:
        cur = conn.execute(
            "INSERT INTO remote_notebooks (profile_name, platform, external_id, name, "
            "workspace_path, language, source_text, source_hash, ingested_at) "
            "VALUES (?, 'databricks', ?, ?, ?, 'python', ?, 'h', '2026-01-01')",
            (profile, f"ext-{name}", name, f"/ws/{name}", source),
        )
        remote_id = int(cur.lastrowid or 0)
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, column_name, entity_kind, asset_kind, "
            "search_text, source_remote_id, updated_at, last_synced_at) "
            "VALUES (?, '', '', '__assets', ?, NULL, 'notebook', 'notebook', "
            "?, ?, 0, 0)",
            (profile, f"notebook#{remote_id}", name, remote_id),
        )
        bridge_id = int(cur.lastrowid or 0)
        for to_id in referenced_entity_ids:
            conn.execute(
                "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
                "relationship_type, score, source, details_json, last_seen, "
                "from_entity_kind, to_entity_kind) "
                "VALUES (?, ?, 'asset_references_table', 1.0, 'test', '{}', 0, "
                "'notebook', 'table')",
                (bridge_id, to_id),
            )
        return remote_id


def test_resolver_returns_empty_when_no_refs(store: SQLiteHistoryStore) -> None:
    blocks, resolved = resolve_asset_context_for_run(store=store, refs=[])
    assert blocks == {}
    assert resolved == []


def test_resolver_pulls_notebook_block_and_table_map(store: SQLiteHistoryStore) -> None:
    orders_id = _seed_table_entity(store, "db_prod", "sales", "orders")
    customers_id = _seed_table_entity(store, "db_prod", "sales", "customers")
    nb_src = json.dumps(
        {
            "cells": [
                {"cell_type": "markdown", "source": "# Daily metrics ETL"},
                {"cell_type": "code", "source": "spark.read.table('sales.orders')"},
            ]
        }
    )
    remote_id = _seed_notebook_with_bridge(
        store,
        "db_prod",
        "daily_metrics",
        nb_src,
        [orders_id, customers_id],
    )

    blocks, resolved = resolve_asset_context_for_run(
        store=store,
        refs=[AssetRef(kind="asset_notebook", ref=f"db_prod:{remote_id}")],
    )
    assert ("sales", "orders") in blocks
    assert ("sales", "customers") in blocks
    block = blocks[("sales", "orders")][0]
    assert block["kind"] == "notebook"
    assert block["name"] == "daily_metrics"
    assert block["profile"] == "db_prod"
    assert "Daily metrics ETL" in block["excerpt"]
    assert len(resolved) == 1


def test_resolver_handles_malformed_ref(store: SQLiteHistoryStore) -> None:
    blocks, resolved = resolve_asset_context_for_run(
        store=store,
        refs=[AssetRef(kind="asset_notebook", ref="missing-colon")],
    )
    assert blocks == {}
    assert resolved == []


def test_resolver_skips_unknown_kind(store: SQLiteHistoryStore) -> None:
    blocks, resolved = resolve_asset_context_for_run(
        store=store,
        refs=[AssetRef(kind="asset_unknown", ref="p:1")],
    )
    assert blocks == {}
    assert resolved == []


def test_resolver_pulls_query_block(store: SQLiteHistoryStore) -> None:
    orders_id = _seed_table_entity(store, "p", "s", "orders")
    with store._lock, store._connect() as conn:
        cur = conn.execute(
            "INSERT INTO remote_queries (profile_name, platform, kind, external_id, name, "
            "sql_text, sql_hash, ingested_at) VALUES "
            "(?, 'snowflake', 'saved', 'eq', 'metric_q', "
            "'SELECT count(*) FROM s.orders', 'h', '2026-01-01')",
            ("p",),
        )
        q_id = int(cur.lastrowid or 0)
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, column_name, entity_kind, asset_kind, "
            "search_text, source_remote_id, updated_at, last_synced_at) "
            "VALUES ('p', '', '', '__assets', ?, NULL, 'query', 'query', "
            "'metric_q', ?, 0, 0)",
            (f"query#{q_id}", q_id),
        )
        bridge = int(cur.lastrowid or 0)
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) VALUES "
            "(?, ?, 'asset_references_table', 1.0, 't', '{}', 0, 'query', 'table')",
            (bridge, orders_id),
        )

    blocks, _resolved = resolve_asset_context_for_run(
        store=store,
        refs=[AssetRef(kind="asset_query", ref=f"p:{q_id}")],
    )
    assert ("s", "orders") in blocks
    assert "SELECT count(*) FROM s.orders" in blocks[("s", "orders")][0]["excerpt"]
