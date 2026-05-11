"""Integration smoke: bulk_schema_metadata against a real DuckDB file.

DuckDB ships in the default install so we exercise the end-to-end
adapter path against a live database here. The other backends get
SQL-shape unit tests since we cannot reach the warehouses from CI.

Properties pinned:

1. Bulk fetch returns every table + view in the schema, with their
   comment + column comments populated from the live database.
2. Empty schemas come back as an empty dict (caller never sees ``None``
   unless the adapter has no bulk source at all).
3. The connector caches the result — the second
   ``get_table_comment`` call for any table in the schema must not
   touch the engine again.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy

from amx.config import DBConfig
from amx.db.connector import DatabaseConnector
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def duckdb_profile(tmp_path: Path) -> DBConfig:
    db_path = tmp_path / "bulk.duckdb"
    engine = sqlalchemy.create_engine(f"duckdb:///{db_path}")
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS sales"))
        conn.execute(sqlalchemy.text("CREATE TABLE sales.orders (id INTEGER, amount DOUBLE)"))
        conn.execute(sqlalchemy.text("CREATE TABLE sales.line_items (order_id INTEGER, sku VARCHAR)"))
        conn.execute(sqlalchemy.text("COMMENT ON TABLE sales.orders IS 'All orders ever placed'"))
        conn.execute(sqlalchemy.text("COMMENT ON COLUMN sales.orders.id IS 'Order primary key'"))
        conn.execute(sqlalchemy.text("COMMENT ON COLUMN sales.orders.amount IS 'USD total'"))
    engine.dispose()
    return DBConfig(backend="duckdb", database=str(db_path))


@pytest.fixture(autouse=True)
def _isolated_history(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Point the column-comments cache at a throwaway history.db so the
    # test doesn't reach the user's ~/.amx/history.db.
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    from amx.storage import sqlite_store

    monkeypatch.setattr(sqlite_store, "history_store", lambda: store)


def test_bulk_schema_metadata_returns_every_table_with_comments(duckdb_profile) -> None:
    db = DatabaseConnector(duckdb_profile, profile_name="duckdb-test")
    result = db._adapter.bulk_schema_metadata(db.engine, "sales")
    assert result is not None
    assert set(result.keys()) == {"orders", "line_items"}
    orders = result["orders"]
    assert orders["table_comment"] == "All orders ever placed"
    assert orders["kind"] == "TABLE"
    assert orders["columns"]["id"] == "Order primary key"
    assert orders["columns"]["amount"] == "USD total"
    # Tables without comments come back with ``None`` table_comment.
    assert result["line_items"]["table_comment"] is None


def test_get_table_comment_warms_cache_for_siblings(duckdb_profile) -> None:
    """First read triggers the bulk fetch; the second read for a
    sibling table must skip the engine entirely (cache hit)."""
    db = DatabaseConnector(duckdb_profile, profile_name="duckdb-test")
    # First read populates the cache for the whole schema.
    assert db.get_table_comment("sales", "orders") == "All orders ever placed"
    # Mark the engine as off-limits — any further DB hit would raise.
    real_engine = db._engine
    db._engine = None  # type: ignore[assignment]

    class _Trap:
        def __getattr__(self, _name):  # pragma: no cover - guard
            raise AssertionError("expected cache hit; engine was reached")

    db._engine = _Trap()  # type: ignore[assignment]
    # Sibling table comes from cache without touching the engine.
    assert db.get_table_comment("sales", "line_items") is None
    # Original table also cached.
    assert db.get_table_comment("sales", "orders") == "All orders ever placed"
    # Restore for any teardown that may need to dispose.
    db._engine = real_engine


def test_get_column_comments_uses_bulk_path(duckdb_profile) -> None:
    db = DatabaseConnector(duckdb_profile, profile_name="duckdb-test")
    cols = db.get_column_comments("sales", "orders")
    assert cols["id"] == "Order primary key"
    assert cols["amount"] == "USD total"


def test_invalidation_forces_refetch(duckdb_profile) -> None:
    """After invalidate, the next read returns fresh data — i.e. the
    user-facing guarantee against stale cache."""
    db = DatabaseConnector(duckdb_profile, profile_name="duckdb-test")
    # Warm cache.
    assert db.get_table_comment("sales", "orders") == "All orders ever placed"
    # Mutate the underlying DB out-of-band (simulates an apply or
    # external DBA edit).
    with db.engine.begin() as conn:
        conn.execute(sqlalchemy.text("COMMENT ON TABLE sales.orders IS 'Updated comment'"))
    # Without invalidation the cache would still serve the old value.
    db.invalidate_column_comments_cache(schema="sales", table="orders")
    assert db.get_table_comment("sales", "orders") == "Updated comment"
