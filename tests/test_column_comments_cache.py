"""Tests for the column-comments cache backing the bulk-schema path.

The cache lives in :class:`amx.storage.sqlite_store.SQLiteHistoryStore`
alongside the existing ``run_context_cache``. Three properties matter:

1. Save → lookup round-trips the dict verbatim, including ``None``
   comments (which are real on most backends — an empty column comment
   reads as ``None``, not the empty string).
2. ``expires_at`` is honoured on lookup: a row past its TTL is reported
   absent so the caller refetches from the live DB.
3. Invalidate has three correct granularities (single row, whole
   schema, whole profile). A column-comment write must only wipe its
   own table; a schema-comment write wipes that schema's siblings; a
   database-level reset wipes everything for the profile.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _entry(table_comment: str | None, columns: dict[str, str | None]) -> dict[str, object]:
    return {"table_comment": table_comment, "columns": columns, "kind": "TABLE"}


def test_save_then_lookup_roundtrips_entries(store: SQLiteHistoryStore) -> None:
    store.save_column_comments_cache(
        db_profile="prod-databricks",
        database="main",
        schema="sales",
        entries={
            "orders": _entry("All orders", {"id": "PK", "amount": None}),
            "line_items": _entry(None, {"order_id": "FK to orders"}),
        },
    )

    hit = store.lookup_column_comments_cache(
        db_profile="prod-databricks",
        database="main",
        schema="sales",
        table="orders",
    )
    assert hit is not None
    assert hit["table_comment"] == "All orders"
    # None column comment must survive the round-trip (json keeps it).
    assert hit["columns"] == {"id": "PK", "amount": None}
    assert hit["kind"] == "TABLE"


def test_lookup_misses_when_row_is_expired(store: SQLiteHistoryStore) -> None:
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="public",
        entries={"orders": _entry("x", {"id": "PK"})},
        ttl_seconds=-1.0,  # already expired
    )
    hit = store.lookup_column_comments_cache(
        db_profile="prod", database="", schema="public", table="orders"
    )
    # TTL < 0 means expires_at < now, so lookup reports absent even
    # though the row is on disk. ``gc_column_comments_cache`` reaps it.
    assert hit is None


def test_lookup_bulk_returns_only_fresh_rows(store: SQLiteHistoryStore) -> None:
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="public",
        entries={"fresh": _entry("ok", {})},
        ttl_seconds=3600.0,
    )
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="public",
        entries={"stale": _entry("ok", {})},
        ttl_seconds=-1.0,
    )
    bulk = store.lookup_column_comments_cache_bulk(
        db_profile="prod", database="", schema="public"
    )
    assert "fresh" in bulk
    assert "stale" not in bulk


def test_invalidate_single_row_only_drops_target(store: SQLiteHistoryStore) -> None:
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="public",
        entries={
            "orders": _entry("o", {}),
            "users": _entry("u", {}),
        },
    )
    dropped = store.invalidate_column_comments_cache(
        db_profile="prod", database="", schema="public", table="orders"
    )
    assert dropped == 1
    # Sibling row untouched — the user editing one table's comment
    # must not force a refetch of every other table in the schema.
    assert (
        store.lookup_column_comments_cache(
            db_profile="prod", database="", schema="public", table="users"
        )
        is not None
    )
    assert (
        store.lookup_column_comments_cache(
            db_profile="prod", database="", schema="public", table="orders"
        )
        is None
    )


def test_invalidate_schema_drops_every_table_in_schema(store: SQLiteHistoryStore) -> None:
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="sales",
        entries={"orders": _entry("o", {}), "users": _entry("u", {})},
    )
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="marketing",
        entries={"campaigns": _entry("c", {})},
    )
    dropped = store.invalidate_column_comments_cache(
        db_profile="prod", database="", schema="sales"
    )
    assert dropped == 2
    # Different schema untouched.
    assert (
        store.lookup_column_comments_cache(
            db_profile="prod", database="", schema="marketing", table="campaigns"
        )
        is not None
    )


def test_invalidate_profile_drops_every_schema(store: SQLiteHistoryStore) -> None:
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="sales",
        entries={"orders": _entry("o", {})},
    )
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="marketing",
        entries={"campaigns": _entry("c", {})},
    )
    store.save_column_comments_cache(
        db_profile="other",
        database="",
        schema="sales",
        entries={"orders": _entry("o2", {})},
    )
    dropped = store.invalidate_column_comments_cache(db_profile="prod")
    assert dropped == 2
    # Other profile must be untouched even with the same schema name.
    assert (
        store.lookup_column_comments_cache(
            db_profile="other", database="", schema="sales", table="orders"
        )
        is not None
    )


def test_gc_sweeps_expired_rows(store: SQLiteHistoryStore) -> None:
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="public",
        entries={"stale": _entry("x", {})},
        ttl_seconds=-1.0,
    )
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="public",
        entries={"fresh": _entry("x", {})},
        ttl_seconds=3600.0,
    )
    swept = store.gc_column_comments_cache()
    assert swept == 1
    # Fresh row still there after GC.
    assert (
        store.lookup_column_comments_cache(
            db_profile="prod", database="", schema="public", table="fresh"
        )
        is not None
    )


def test_save_is_upsert_on_repeat_writes(store: SQLiteHistoryStore) -> None:
    """A re-fetch of the same (profile, schema, table) must overwrite,
    not append. Otherwise the row count grows unboundedly across
    refresh cycles and `expires_at` reads pick a stale row."""
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="public",
        entries={"orders": _entry("old", {"id": "old"})},
    )
    store.save_column_comments_cache(
        db_profile="prod",
        database="",
        schema="public",
        entries={"orders": _entry("new", {"id": "new"})},
    )
    hit = store.lookup_column_comments_cache(
        db_profile="prod", database="", schema="public", table="orders"
    )
    assert hit is not None
    assert hit["table_comment"] == "new"
    assert hit["columns"] == {"id": "new"}


def test_lookup_is_database_scoped(store: SQLiteHistoryStore) -> None:
    """A multi-database profile (e.g. Postgres with several databases
    or Databricks with multiple catalogs) must not let a table in
    database A shadow the same-named table in database B."""
    store.save_column_comments_cache(
        db_profile="prod",
        database="dwh",
        schema="public",
        entries={"orders": _entry("dwh-version", {})},
    )
    store.save_column_comments_cache(
        db_profile="prod",
        database="staging",
        schema="public",
        entries={"orders": _entry("staging-version", {})},
    )
    a = store.lookup_column_comments_cache(
        db_profile="prod", database="dwh", schema="public", table="orders"
    )
    b = store.lookup_column_comments_cache(
        db_profile="prod", database="staging", schema="public", table="orders"
    )
    assert a is not None and a["table_comment"] == "dwh-version"
    assert b is not None and b["table_comment"] == "staging-version"
