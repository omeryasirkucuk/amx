"""Regression coverage for the explicit DB-cache helpers.

The REPL ``/db cache-{show,stats,clear}`` and the
``/api/db/cache/*`` router both funnel through
:mod:`amx.storage.cache_ops`. These tests pin the contract: scoped
reads return only the requested rows, stats roll up across the three
cache tables, and ``cache_clear`` only deletes what it was asked to
touch.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from amx.storage import sqlite_store as ss
from amx.storage.cache_ops import (
    CACHE_TYPES,
    cache_clear,
    cache_inventory,
    cache_stats,
)
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SQLiteHistoryStore:
    db_path = tmp_path / "history.db"
    s = SQLiteHistoryStore(db_path)
    s.init()
    ss._store = s  # noqa: SLF001
    monkeypatch.setattr(ss, "_store", s, raising=False)  # noqa: SLF001
    yield s
    ss._store = None  # noqa: SLF001


def _seed(store: SQLiteHistoryStore) -> None:
    """Populate every cache table with two profiles x two databases so the
    scope filters have something interesting to filter."""
    # schemas_cache
    store.save_schemas_cache(
        db_profile="prof-a",
        database="db1",
        catalog="",
        entries={"public": None, "analytics": "comment"},
        ttl_seconds=3600.0,
    )
    store.save_schemas_cache(
        db_profile="prof-a",
        database="db2",
        catalog="",
        entries={"aviary": None},
        ttl_seconds=3600.0,
    )
    store.save_schemas_cache(
        db_profile="prof-b",
        database="db1",
        catalog="",
        entries={"public": None},
        ttl_seconds=3600.0,
    )
    # column_comments_cache
    store.save_column_comments_cache(
        db_profile="prof-a",
        database="db1",
        schema="public",
        entries={
            "users": {
                "table_comment": None,
                "columns": {"id": None, "name": None},
                "kind": "TABLE",
            },
        },
        ttl_seconds=3600.0,
    )
    store.save_column_comments_cache(
        db_profile="prof-a",
        database="db2",
        schema="aviary",
        entries={
            "birds": {"table_comment": None, "columns": {"id": None}, "kind": "TABLE"},
        },
        ttl_seconds=3600.0,
    )
    # catalog_entities — raw insert mirroring the skeleton sync's
    # upsert shape. Stamping the rows here lets cache_inventory /
    # cache_clear walk the table without spinning up a full sync.
    with store._connect() as conn:  # noqa: SLF001
        now = time.time()
        for profile, database, schema, table in [
            ("prof-a", "db1", "public", "users"),
            ("prof-a", "db1", "public", "orders"),
            ("prof-a", "db2", "aviary", "birds"),
            ("prof-b", "db1", "public", "users"),
        ]:
            conn.execute(
                """
                INSERT INTO catalog_entities (
                    db_profile, db_backend, database_name, schema_name,
                    table_name, column_name, entity_kind, asset_kind,
                    last_synced_at
                ) VALUES (?, 'postgresql', ?, ?, ?, NULL, 'table', 'table', ?)
                """,
                (profile, database, schema, table, now),
            )


def test_inventory_unscoped_returns_all_rows(store: SQLiteHistoryStore) -> None:
    _seed(store)
    rows = cache_inventory()
    assert {(r.profile, r.database) for r in rows} == {
        ("prof-a", "db1"),
        ("prof-a", "db2"),
        ("prof-b", "db1"),
    }
    by_key = {(r.profile, r.database): r for r in rows}
    assert by_key[("prof-a", "db1")].schemas_rows == 2
    assert by_key[("prof-a", "db1")].columns_rows == 1
    assert by_key[("prof-a", "db1")].catalog_rows == 2


def test_inventory_filters_by_profile(store: SQLiteHistoryStore) -> None:
    _seed(store)
    rows = cache_inventory(profile="prof-b")
    assert {(r.profile, r.database) for r in rows} == {("prof-b", "db1")}


def test_inventory_filters_by_database(store: SQLiteHistoryStore) -> None:
    _seed(store)
    rows = cache_inventory(database="db2")
    assert {(r.profile, r.database) for r in rows} == {("prof-a", "db2")}


def test_stats_reports_per_table_totals(store: SQLiteHistoryStore) -> None:
    _seed(store)
    s = cache_stats()
    assert set(s.keys()) == {"schemas", "columns", "catalog"}
    assert s["schemas"].total_rows == 4
    assert s["schemas"].distinct_profiles == 2
    assert s["schemas"].ttl_aware is True
    assert s["columns"].total_rows == 2
    assert s["catalog"].total_rows == 4
    assert s["catalog"].ttl_aware is False


def test_clear_scoped_by_profile_and_database(store: SQLiteHistoryStore) -> None:
    _seed(store)
    report = cache_clear(profile="prof-a", database="db1")
    assert report.deleted == {"schemas": 2, "columns": 1, "catalog": 2}
    assert report.total == 5
    # Other scopes untouched.
    rows = cache_inventory()
    assert {(r.profile, r.database) for r in rows} == {
        ("prof-a", "db2"),
        ("prof-b", "db1"),
    }


def test_clear_type_filter_does_not_touch_other_tables(
    store: SQLiteHistoryStore,
) -> None:
    _seed(store)
    report = cache_clear(profile="prof-a", database="db1", types=["schemas"])
    assert report.deleted == {"schemas": 2}
    # columns + catalog rows for the same scope still present.
    rows = cache_inventory(profile="prof-a", database="db1")
    assert len(rows) == 1
    assert rows[0].schemas_rows == 0
    assert rows[0].columns_rows == 1
    assert rows[0].catalog_rows == 2


def test_clear_all_types_alias_clears_everything(store: SQLiteHistoryStore) -> None:
    _seed(store)
    report = cache_clear(profile="prof-b", types=["all"])
    assert sorted(report.deleted.keys()) == sorted(CACHE_TYPES)
    rows = cache_inventory(profile="prof-b")
    assert rows == []


def test_clear_global_flush_when_no_filters(store: SQLiteHistoryStore) -> None:
    _seed(store)
    cache_clear()
    assert cache_inventory() == []
    # State table also cleared for whole-profile flushes.
    with store._connect() as conn:  # noqa: SLF001
        count = conn.execute("SELECT COUNT(*) AS n FROM catalog_profile_state").fetchone()
    assert int(count["n"]) == 0


def test_clear_unknown_type_raises(store: SQLiteHistoryStore) -> None:
    with pytest.raises(ValueError, match="Unknown cache types"):
        cache_clear(types=["foo"])
