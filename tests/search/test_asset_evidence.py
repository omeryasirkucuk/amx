"""Tests for ``build_assets_evidence`` — the ASK passive-evidence
surface that pulls notebook / query / stream / pipeline snippets when
the question resolves to a catalog entity those assets reference."""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.search._agent.asset_evidence import build_assets_evidence
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


def _seed_notebook(store: SQLiteHistoryStore, profile: str, name: str, source: str) -> int:
    with store._lock, store._connect() as conn:
        cur = conn.execute(
            "INSERT INTO remote_notebooks (profile_name, platform, external_id, name, "
            "workspace_path, language, source_text, source_hash, ingested_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                profile,
                "databricks",
                f"ext-{name}",
                name,
                f"/ws/{name}",
                "python",
                source,
                "h",
                "2026-01-01",
            ),
        )
        return int(cur.lastrowid or 0)


def _seed_query(store: SQLiteHistoryStore, profile: str, name: str, sql: str) -> int:
    with store._lock, store._connect() as conn:
        cur = conn.execute(
            "INSERT INTO remote_queries (profile_name, platform, kind, external_id, name, "
            "sql_text, sql_hash, ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (profile, "snowflake", "saved", f"ext-{name}", name, sql, "h", "2026-01-01"),
        )
        return int(cur.lastrowid or 0)


def _link_asset(
    store: SQLiteHistoryStore,
    *,
    from_kind: str,
    from_id: int,
    to_entity_id: int,
) -> None:
    with store._lock, store._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) "
            "VALUES (?, ?, 'asset_references_table', 1.0, 'test', '{}', 0, ?, 'table')",
            (from_id, to_entity_id, from_kind),
        )


def test_returns_empty_when_no_entities(store: SQLiteHistoryStore) -> None:
    payload = build_assets_evidence(
        store=store, entity_ids=[], question_terms=["orders"], max_assets=3
    )
    assert payload.is_empty


def test_returns_empty_when_no_edges(store: SQLiteHistoryStore) -> None:
    entity_id = _seed_table_entity(store, "p", "sales", "orders")
    payload = build_assets_evidence(
        store=store, entity_ids=[entity_id], question_terms=["orders"], max_assets=3
    )
    assert payload.is_empty


def test_pulls_notebook_referencing_table(store: SQLiteHistoryStore) -> None:
    entity_id = _seed_table_entity(store, "db_prod", "sales", "orders")
    nb_id = _seed_notebook(
        store,
        "db_prod",
        "daily_orders_etl",
        "# Daily orders ETL\nSELECT * FROM sales.orders WHERE created_at > now()",
    )
    _link_asset(store, from_kind="notebook", from_id=nb_id, to_entity_id=entity_id)

    payload = build_assets_evidence(
        store=store,
        entity_ids=[entity_id],
        question_terms=["orders", "etl"],
        max_assets=3,
    )
    assert not payload.is_empty
    assert payload.items[0].kind == "notebook"
    assert payload.items[0].name == "daily_orders_etl"
    assert payload.items[0].profile == "db_prod"
    assert "orders" in payload.items[0].excerpt.lower()


def test_ranks_higher_keyword_match_first(store: SQLiteHistoryStore) -> None:
    entity_id = _seed_table_entity(store, "db_prod", "sales", "orders")
    high = _seed_notebook(store, "db_prod", "orders_centric", "orders orders orders process orders")
    low = _seed_notebook(store, "db_prod", "tangential", "schema setup nothing about it")
    _link_asset(store, from_kind="notebook", from_id=high, to_entity_id=entity_id)
    _link_asset(store, from_kind="notebook", from_id=low, to_entity_id=entity_id)

    payload = build_assets_evidence(
        store=store,
        entity_ids=[entity_id],
        question_terms=["orders"],
        max_assets=3,
    )
    assert payload.items[0].name == "orders_centric"


def test_mixed_kinds_returned(store: SQLiteHistoryStore) -> None:
    entity_id = _seed_table_entity(store, "db_prod", "sales", "orders")
    nb_id = _seed_notebook(store, "db_prod", "nb", "load orders")
    q_id = _seed_query(store, "db_prod", "metric_q", "SELECT count(*) FROM sales.orders")
    _link_asset(store, from_kind="notebook", from_id=nb_id, to_entity_id=entity_id)
    _link_asset(store, from_kind="query", from_id=q_id, to_entity_id=entity_id)

    payload = build_assets_evidence(
        store=store,
        entity_ids=[entity_id],
        question_terms=["orders"],
        max_assets=5,
    )
    kinds = {item.kind for item in payload.items}
    assert {"notebook", "query"}.issubset(kinds)


def test_respects_max_assets(store: SQLiteHistoryStore) -> None:
    entity_id = _seed_table_entity(store, "p", "s", "t")
    for i in range(5):
        nb = _seed_notebook(store, "p", f"nb{i}", "orders body")
        _link_asset(store, from_kind="notebook", from_id=nb, to_entity_id=entity_id)
    payload = build_assets_evidence(
        store=store, entity_ids=[entity_id], question_terms=["orders"], max_assets=2
    )
    assert len(payload.items) == 2


def test_disabled_returns_empty(store: SQLiteHistoryStore) -> None:
    entity_id = _seed_table_entity(store, "p", "s", "t")
    nb_id = _seed_notebook(store, "p", "nb", "orders")
    _link_asset(store, from_kind="notebook", from_id=nb_id, to_entity_id=entity_id)
    payload = build_assets_evidence(
        store=store,
        entity_ids=[entity_id],
        question_terms=["orders"],
        max_assets=3,
        enabled=False,
    )
    assert payload.is_empty
