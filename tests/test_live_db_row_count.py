"""The Table page surfaces the synced row count.

The live metadata snapshot is profiling-free (no COUNT(*)), so the
Studio Table page never had a row count to show — even though
``/search sync`` records one in ``catalog_entities``. ``_cached_row_count``
reads that synced value so ``table_snapshot`` can include it.

Contract:
* a stored ``row_count`` > 0 is returned,
* a stored 0 means "never counted" → ``None`` (not a misleading 0),
* when the same schema.table exists in two databases (one counted,
  one skeleton-only), the counted copy wins on an unscoped lookup.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

import amx.web.routers.live_db as live_db
from amx.storage import sqlite_store as ss
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def seeded_history_store(tmp_path: Path):
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    ss._store = SQLiteHistoryStore(db_path)  # noqa: SLF001
    yield db_path
    ss._store = None  # noqa: SLF001


def _seed_table(
    db_path: Path,
    *,
    profile: str,
    database: str,
    schema: str,
    table: str,
    row_count: int,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO catalog_entities (
                db_profile, db_backend, database_name, schema_name,
                table_name, column_name, entity_kind, asset_kind,
                row_count, search_text, updated_at, last_synced_at
            ) VALUES (?, 'postgresql', ?, ?, ?, NULL, 'table', 'table', ?, '', ?, ?)
            """,
            (profile, database, schema, table, row_count, time.time(), time.time()),
        )


def test_returns_synced_positive_count(seeded_history_store: Path) -> None:
    _seed_table(
        seeded_history_store,
        profile="p",
        database="bird_train",
        schema="app_store",
        table="playstore",
        row_count=10840,
    )
    assert (
        live_db._cached_row_count("p", "app_store", "playstore", database_scope="bird_train")
        == 10840
    )


def test_zero_count_reported_as_unknown(seeded_history_store: Path) -> None:
    """Sync records 0 for tables it never counted — surface None, not 0."""
    _seed_table(
        seeded_history_store,
        profile="p",
        database="bird_train",
        schema="airline",
        table="Airports",
        row_count=0,
    )
    assert (
        live_db._cached_row_count("p", "airline", "Airports", database_scope="bird_train") is None
    )


def test_unscoped_lookup_prefers_counted_copy(seeded_history_store: Path) -> None:
    """Same schema.table in two databases — one counted, one
    skeleton-only (0). An unscoped lookup must return the real count,
    not the stale 0."""
    _seed_table(
        seeded_history_store,
        profile="p",
        database="bird_train",
        schema="beer_factory",
        table="customers",
        row_count=554,
    )
    _seed_table(
        seeded_history_store,
        profile="p",
        database="bird_train_desc",
        schema="beer_factory",
        table="customers",
        row_count=0,
    )
    assert live_db._cached_row_count("p", "beer_factory", "customers", database_scope=None) == 554


def test_missing_table_returns_none(seeded_history_store: Path) -> None:
    assert live_db._cached_row_count("p", "nope", "nope", database_scope=None) is None


def test_no_history_store_returns_none(tmp_path: Path) -> None:
    """When the history store singleton isn't initialised, the helper
    degrades to None rather than raising."""
    ss._store = None  # noqa: SLF001
    assert live_db._cached_row_count("p", "s", "t", database_scope=None) is None
