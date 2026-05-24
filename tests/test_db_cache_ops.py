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
    purge_orphan_profile_rows,
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


def test_stats_valid_profiles_filter_excludes_tombstones(
    store: SQLiteHistoryStore,
) -> None:
    """Rows for a profile not in ``valid_profiles`` are excluded from
    every aggregate — distinct counts AND total_rows. The Studio
    Catalog cache page passes the configured profile set so a
    deleted-profile tombstone never inflates the headline numbers.
    """
    _seed(store)
    # Pretend the user just removed prof-b; prof-a is the only
    # configured profile but prof-b's catalog rows are still on disk.
    s = cache_stats(valid_profiles=("prof-a",))
    assert s["schemas"].distinct_profiles == 1
    assert s["schemas"].total_rows == 3  # 4 seeded rows - 1 for prof-b
    assert s["catalog"].distinct_profiles == 1
    assert s["catalog"].total_rows == 3  # 4 catalog rows - 1 for prof-b


def test_stats_legacy_unfiltered_call_keeps_old_shape(
    store: SQLiteHistoryStore,
) -> None:
    """``valid_profiles=None`` (the default) preserves the unfiltered
    aggregate the REPL ``/db cache-stats`` view depends on."""
    _seed(store)
    s = cache_stats()
    assert s["schemas"].distinct_profiles == 2
    assert s["catalog"].distinct_profiles == 2


def test_purge_orphan_profile_rows_deletes_tombstones(
    store: SQLiteHistoryStore,
) -> None:
    """Eager + startup paths both call this helper; rows for any
    profile outside ``valid_profiles`` are deleted across all three
    cache tables, descriptions, and the per-profile state row."""
    _seed(store)
    # Seed a catalog_profile_state row for prof-b so the helper can
    # prove it cleans up the state table alongside the data tables.
    with store._connect() as conn:  # noqa: SLF001
        conn.execute(
            """INSERT OR REPLACE INTO catalog_profile_state
                   (db_profile, state, last_full_sync_at)
               VALUES ('prof-b', 'done', ?)""",
            (time.time(),),
        )
    counts = purge_orphan_profile_rows(("prof-a",))
    assert counts["catalog_entities"] == 1
    assert counts["schemas_cache"] == 1
    assert counts["column_comments_cache"] == 0
    # prof-b should be gone from every cache surface.
    rows = cache_inventory(profile="prof-b")
    assert rows == []
    with store._connect() as conn:  # noqa: SLF001
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM catalog_profile_state WHERE db_profile = 'prof-b'"
        ).fetchone()
    assert int(n["n"]) == 0
    # prof-a left intact.
    a = cache_inventory(profile="prof-a")
    assert {(r.profile, r.database) for r in a} == {
        ("prof-a", "db1"),
        ("prof-a", "db2"),
    }


def test_purge_orphan_profile_rows_empty_valid_set_clears_everything(
    store: SQLiteHistoryStore,
) -> None:
    """When the user has no configured DB profiles every cached row
    is an orphan. The helper walks every table."""
    _seed(store)
    counts = purge_orphan_profile_rows(())
    assert counts["catalog_entities"] == 4
    assert cache_inventory() == []


def test_purge_orphan_profile_rows_idempotent(store: SQLiteHistoryStore) -> None:
    """Second call returns zeros — the startup sweep can run on every
    boot without flicker."""
    _seed(store)
    purge_orphan_profile_rows(("prof-a",))
    counts = purge_orphan_profile_rows(("prof-a",))
    assert counts == {
        "catalog_entities": 0,
        "schemas_cache": 0,
        "column_comments_cache": 0,
    }
