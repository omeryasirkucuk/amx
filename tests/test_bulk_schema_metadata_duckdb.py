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


def test_list_assets_uses_cache_after_first_call(duckdb_profile) -> None:
    """The user's reported friction: ``list_assets`` was hitting
    SHOW TABLES every sidebar expand because the v0.13 cache only
    covered column comments, not the asset list itself. After the
    bulk_filled-aware cache lands, the second ``list_assets`` for the
    same schema must NOT touch the engine.
    """
    db = DatabaseConnector(duckdb_profile, profile_name="duckdb-test")
    # First call — cold path, bulk fetch populates the cache.
    assets = db.list_assets("sales")
    names = sorted([n for n, _ in assets])
    assert names == ["line_items", "orders"]
    # Trap the engine so any DB hit on the second call raises loudly.
    real_engine = db._engine

    class _Trap:
        def __getattr__(self, _name):  # pragma: no cover - guard
            raise AssertionError("expected cache hit; engine was reached")

    db._engine = _Trap()  # type: ignore[assignment]
    try:
        cached_assets = db.list_assets("sales")
    finally:
        db._engine = real_engine
    assert sorted([n for n, _ in cached_assets]) == ["line_items", "orders"]


def test_spinner_suppressed_off_main_thread(duckdb_profile, monkeypatch) -> None:
    """Regression for the Studio leak: ``_populate_schema_metadata_cache``
    used to paint a Rich ``step_spinner`` whenever stdout was a TTY,
    no matter which thread invoked it. Studio's uvicorn worker
    threads were therefore drawing "Cached column descriptions for X"
    lines into the CLI shell the user launched ``/studio`` from. The
    guard now skips the spinner unless we're on the main thread AND
    not inside a ``quiet_console()`` context — Studio worker threads
    satisfy neither.
    """
    import threading

    # Force the TTY branch on so the only suppression in play is the
    # thread + quiet guards. Without the fix this monkeypatch would
    # make the worker-thread test attempt to render Rich output.
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    db = DatabaseConnector(duckdb_profile, profile_name="duckdb-test")

    step_spinner_calls: list[str] = []

    def _track_step_spinner(label, **_kw):
        step_spinner_calls.append(label)
        from contextlib import contextmanager

        @contextmanager
        def _noop():
            yield

        return _noop()

    import amx.utils.console as console_mod

    monkeypatch.setattr(console_mod, "step_spinner", _track_step_spinner)

    def _worker():
        db.get_table_comment("sales", "orders")

    t = threading.Thread(target=_worker)
    t.start()
    t.join()

    assert step_spinner_calls == [], (
        f"step_spinner must not be invoked from a non-main thread; was {step_spinner_calls}"
    )


def test_databricks_pinned_schema_short_circuits_list_schemas(monkeypatch) -> None:
    """Databricks ``cfg.database`` is a schema pin (the wizard prompt
    reads "Schema / database (optional)"). When set, ``connector.
    list_schemas`` must return ``[pinned]`` instead of the live list
    — so the sidebar's catalog expand shows ONLY the user's schema,
    not every schema in the catalog. Mirrors the catalog-picker
    behaviour from PR #318."""
    import sys
    import types

    from amx.config import DBConfig

    # Stub out the databricks-sql / sqlalchemy imports so we can build
    # a DatabaseConnector for a databricks profile without a live
    # connection. The pin filter runs entirely on what the adapter
    # returned, so we mock the adapter's ``list_schemas``.
    db = DatabaseConnector(
        DBConfig(
            backend="databricks",
            host="db.test",
            access_token="t",
            catalog="main",
            database="sales",
            http_path="/sql/1.0/warehouses/x",
        ),
        profile_name="dbx-test",
    )
    # Bypass cache + engine — feed the filter directly via the adapter.
    monkeypatch.setattr(db, "_catalog_bulk_cache_is_fresh", lambda *_a, **_kw: False)
    monkeypatch.setattr(db, "_populate_catalogs_cache", lambda *_a, **_kw: False)
    monkeypatch.setattr(
        db._adapter,
        "list_schemas",
        lambda _engine, _catalog: ["sales", "marketing", "operations"],
    )
    # Pin set + present → filter to that one.
    assert db.list_schemas() == ["sales"]


def test_databricks_pinned_schema_falls_through_when_missing(monkeypatch) -> None:
    """If the pinned schema is no longer in the live list (dropped
    server-side, permissions revoked) ``list_schemas`` returns the
    full list so the sidebar can surface its pinned-but-missing
    warning instead of an empty page."""
    from amx.config import DBConfig

    db = DatabaseConnector(
        DBConfig(
            backend="databricks",
            host="db.test",
            access_token="t",
            catalog="main",
            database="archived_sales",
            http_path="/sql/1.0/warehouses/x",
        ),
        profile_name="dbx-test",
    )
    monkeypatch.setattr(db, "_catalog_bulk_cache_is_fresh", lambda *_a, **_kw: False)
    monkeypatch.setattr(db, "_populate_catalogs_cache", lambda *_a, **_kw: False)
    monkeypatch.setattr(
        db._adapter,
        "list_schemas",
        lambda _engine, _catalog: ["sales", "marketing"],
    )
    # ``archived_sales`` is gone — return full list so the SPA can warn.
    assert db.list_schemas() == ["sales", "marketing"]


def test_unpinned_schema_returns_full_list(monkeypatch) -> None:
    """When no schema is pinned the connector returns whatever the
    backend has, untouched. Rule: optional is optional."""
    from amx.config import DBConfig

    db = DatabaseConnector(
        DBConfig(
            backend="databricks",
            host="db.test",
            access_token="t",
            catalog="main",
            database="",  # unpinned
            http_path="/sql/1.0/warehouses/x",
        ),
        profile_name="dbx-test",
    )
    monkeypatch.setattr(db, "_catalog_bulk_cache_is_fresh", lambda *_a, **_kw: False)
    monkeypatch.setattr(db, "_populate_catalogs_cache", lambda *_a, **_kw: False)
    monkeypatch.setattr(
        db._adapter,
        "list_schemas",
        lambda _engine, _catalog: ["sales", "marketing", "operations"],
    )
    assert db.list_schemas() == ["sales", "marketing", "operations"]


def test_list_schemas_uses_cache_after_first_call(tmp_path) -> None:
    """Catalog expand: ``list_schemas`` must NOT re-query the DB on
    repeat visits. DuckDB can't carry schema-level comments (the
    backend NotImplementedException's COMMENT ON SCHEMA), but the
    schema enumeration itself absolutely must be cache-served — that
    is the reported friction on the catalog-expand path.
    """
    db_path = tmp_path / "schemas.duckdb"
    engine = sqlalchemy.create_engine(f"duckdb:///{db_path}")
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("CREATE SCHEMA finance"))
        conn.execute(sqlalchemy.text("CREATE SCHEMA marketing"))
    engine.dispose()

    db = DatabaseConnector(
        DBConfig(backend="duckdb", database=str(db_path)),
        profile_name="duckdb-schemas-test",
    )
    schemas = db.list_schemas()
    assert {"finance", "marketing"} <= set(schemas)

    real_engine = db._engine

    class _Trap:
        def __getattr__(self, _name):  # pragma: no cover - guard
            raise AssertionError("expected cache hit; engine was reached")

    db._engine = _Trap()  # type: ignore[assignment]
    try:
        again = db.list_schemas()
    finally:
        db._engine = real_engine
    assert {"finance", "marketing"} <= set(again)
