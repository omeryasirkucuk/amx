"""Phase 3 (0.11.0): SearchCatalog read methods accept multi-profile filters.

The catalog SQLite tables are already keyed by ``db_profile``; Phase 3
extends the *read path* signatures so a single call can union rows
across several profiles. These tests exercise the IN-clause expansion
end-to-end by:

1. Spinning up an isolated SQLiteHistoryStore in a temp dir.
2. Manually inserting catalog_entities rows for two different
   ``db_profile`` values (the same table+column names live in both).
3. Calling the read methods with a single string AND a sequence of
   strings, asserting the results group correctly per profile.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import time
import uuid
from pathlib import Path

from amx.search._catalog._db_profile_clause import (
    build_db_profile_clause,
    normalise_db_profile_filter,
)
from amx.storage.sqlite_store import SQLiteHistoryStore


def _setup_store() -> tuple[SQLiteHistoryStore, Path]:
    """Initialise a fresh history store in a temp dir.

    Each test gets its own tmpdir + ``SQLiteHistoryStore`` instance — we
    deliberately bypass ``init_history_store`` because that helper caches
    the first instance in a module global, and consecutive tests would
    insert overlapping rows into the same DB and fail
    ``idx_catalog_entities_identity`` UNIQUE.
    """
    tmpdir = Path(tempfile.mkdtemp(prefix=f"amx-multi-profile-{uuid.uuid4().hex[:8]}-"))
    db_path = tmpdir / "history.db"
    store = SQLiteHistoryStore(db_path)
    store.init()
    return store, tmpdir


def _insert_entity(
    store: SQLiteHistoryStore,
    *,
    db_profile: str,
    schema: str,
    table: str,
    column: str | None = None,
    entity_kind: str = "column",
    search_text: str = "",
) -> int:
    with sqlite3.connect(store.db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO catalog_entities (
                db_profile, db_backend, database_name, schema_name, table_name,
                column_name, entity_kind, asset_kind, search_text, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                db_profile,
                "postgresql",
                "demo",
                schema,
                table,
                column,
                entity_kind,
                "table",
                search_text,
                time.time(),
            ),
        )
        return int(cur.lastrowid)


# ── _db_profile_clause helper ─────────────────────────────────────────────


def test_build_db_profile_clause_single_string():
    clause, binds = build_db_profile_clause("prod_pg")
    assert clause == "db_profile = ?"
    assert binds == ["prod_pg"]


def test_build_db_profile_clause_multi():
    clause, binds = build_db_profile_clause(["prod_pg", "analytics_bq", "warehouse_sf"])
    assert clause == "db_profile IN (?, ?, ?)"
    assert binds == ["prod_pg", "analytics_bq", "warehouse_sf"]


def test_build_db_profile_clause_dedupes_preserving_order():
    clause, binds = build_db_profile_clause(["b", "a", "b", " a ", ""])
    assert clause == "db_profile IN (?, ?)"
    assert binds == ["b", "a"]


def test_build_db_profile_clause_empty_falls_back_to_no_match():
    clause, binds = build_db_profile_clause([])
    assert clause == "1=0"
    assert binds == []


def test_build_db_profile_clause_custom_column_prefix():
    clause, binds = build_db_profile_clause(["a"], column="ce.db_profile")
    assert clause == "ce.db_profile = ?"
    assert binds == ["a"]


def test_normalise_filter_handles_empty_string():
    assert normalise_db_profile_filter("") == []
    assert normalise_db_profile_filter("only") == ["only"]


# ── End-to-end against a real catalog store ───────────────────────────────


def test_find_columns_by_exact_name_supports_multi_profile():
    """The same column name lives in two profiles → multi-filter returns both."""
    from amx.search.catalog import SearchCatalog

    store, tmpdir = _setup_store()
    try:
        # Two profiles, both with a 'customer_id' column.
        _insert_entity(
            store,
            db_profile="prod_pg",
            schema="public",
            table="orders",
            column="customer_id",
            entity_kind="column",
        )
        _insert_entity(
            store,
            db_profile="analytics_bq",
            schema="dwh",
            table="fact_sales",
            column="customer_id",
            entity_kind="column",
        )
        # Plus a column in 'prod_pg' that should NOT match the needle.
        _insert_entity(
            store,
            db_profile="prod_pg",
            schema="public",
            table="orders",
            column="order_id",
            entity_kind="column",
        )

        catalog = SearchCatalog(store.db_path)

        # Single profile: only one row.
        single = catalog.find_columns_by_exact_name("prod_pg", "customer_id")
        assert len(single) == 1
        assert single[0]["db_profile"] == "prod_pg"

        # Multi-profile: both rows, ordered by db_profile then schema/table.
        multi = catalog.find_columns_by_exact_name(["prod_pg", "analytics_bq"], "customer_id")
        profiles = sorted(row["db_profile"] for row in multi)
        assert profiles == ["analytics_bq", "prod_pg"]

        # Empty filter → empty result (1=0 fallback).
        empty = catalog.find_columns_by_exact_name([], "customer_id")
        assert empty == []
    finally:
        # Clean up temp files; tolerate WAL journal files.
        for f in tmpdir.glob("*"):
            try:
                os.unlink(f)
            except OSError:
                pass
        os.rmdir(tmpdir)


def test_find_tables_by_exact_name_supports_multi_profile():
    from amx.search.catalog import SearchCatalog

    store, tmpdir = _setup_store()
    try:
        _insert_entity(
            store,
            db_profile="prod_pg",
            schema="public",
            table="orders",
            entity_kind="table",
            search_text="orders public",
        )
        _insert_entity(
            store,
            db_profile="analytics_bq",
            schema="dwh",
            table="orders",
            entity_kind="table",
            search_text="orders dwh",
        )
        _insert_entity(
            store,
            db_profile="prod_pg",
            schema="public",
            table="customers",
            entity_kind="table",
            search_text="customers public",
        )

        catalog = SearchCatalog(store.db_path)
        single = catalog.find_tables_by_exact_name("prod_pg", "orders")
        assert len(single) == 1
        multi = catalog.find_tables_by_exact_name(["prod_pg", "analytics_bq"], "orders")
        assert len(multi) == 2
    finally:
        for f in tmpdir.glob("*"):
            try:
                os.unlink(f)
            except OSError:
                pass
        os.rmdir(tmpdir)


def test_find_table_candidates_supports_multi_profile():
    from amx.search.catalog import SearchCatalog

    store, tmpdir = _setup_store()
    try:
        _insert_entity(
            store,
            db_profile="prod_pg",
            schema="public",
            table="orders",
            entity_kind="table",
            search_text="orders sales transactions",
        )
        _insert_entity(
            store,
            db_profile="analytics_bq",
            schema="dwh",
            table="orders_fact",
            entity_kind="table",
            search_text="orders fact analytics warehouse",
        )

        catalog = SearchCatalog(store.db_path)
        single = catalog.find_table_candidates("prod_pg", "orders")
        assert len(single) == 1
        multi = catalog.find_table_candidates(["prod_pg", "analytics_bq"], "orders")
        # Both tables match the prefix 'orders' across profiles.
        profiles = {row["db_profile"] for row in multi}
        assert profiles == {"prod_pg", "analytics_bq"}
    finally:
        for f in tmpdir.glob("*"):
            try:
                os.unlink(f)
            except OSError:
                pass
        os.rmdir(tmpdir)


def test_name_search_columns_supports_multi_profile():
    from amx.search.catalog import SearchCatalog

    store, tmpdir = _setup_store()
    try:
        _insert_entity(
            store,
            db_profile="prod_pg",
            schema="public",
            table="orders",
            column="customer_id",
            entity_kind="column",
            search_text="customer id orders",
        )
        _insert_entity(
            store,
            db_profile="analytics_bq",
            schema="dwh",
            table="fact_sales",
            column="customer_key",
            entity_kind="column",
            search_text="customer key fact analytics",
        )

        catalog = SearchCatalog(store.db_path)
        single = catalog.name_search_columns("prod_pg", "customer")
        assert len(single) >= 1
        assert all(row["db_profile"] == "prod_pg" for row in single)

        multi = catalog.name_search_columns(["prod_pg", "analytics_bq"], "customer")
        profiles = {row["db_profile"] for row in multi}
        assert profiles == {"prod_pg", "analytics_bq"}
    finally:
        for f in tmpdir.glob("*"):
            try:
                os.unlink(f)
            except OSError:
                pass
        os.rmdir(tmpdir)
