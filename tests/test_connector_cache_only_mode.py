"""Cache-only read mode for connector cache-aware methods.

Reported: Studio's ``Sync all`` button populated the catalog but
subsequent reads still hit live DB. Root cause — even after
``catalog_entities`` / ``schemas_cache`` / ``column_comments_cache``
were warm, ``list_schemas`` / ``get_column_comments`` /
``get_table_comment`` / ``get_schema_comment`` all fell through to
live DB on a cache miss.

This test pins the cache-only contract: when ``is_profile_fully_synced``
returns True (the tightened post-sync state), those four methods
return the empty result on a cache miss instead of running the
live-DB fallback. When the profile is not fully synced, the
fallback path is exercised as before.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from amx.config import DBConfig
from amx.db.connector import DatabaseConnector
from amx.storage import sqlite_store as ss
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SQLiteHistoryStore:
    db_path = tmp_path / "history.db"
    s = SQLiteHistoryStore(db_path)
    s.init()
    monkeypatch.setattr(ss, "_store", s, raising=False)  # noqa: SLF001
    yield s
    monkeypatch.setattr(ss, "_store", None, raising=False)  # noqa: SLF001


def _mark_profile_fully_synced(s: SQLiteHistoryStore, profile: str) -> None:
    """Mirror what a successful Sync all stamps onto
    catalog_profile_state. All four timestamp columns must be set
    for the new is_profile_fully_synced contract."""
    import time

    now = time.time()
    with s._connect() as conn:  # noqa: SLF001
        conn.execute(
            """INSERT OR REPLACE INTO catalog_profile_state (
                   db_profile, state, last_full_sync_at,
                   last_skeleton_sync_at, last_schemas_sync_at,
                   last_columns_sync_at
               ) VALUES (?, 'done', ?, ?, ?, ?)""",
            (profile, now, now, now, now),
        )


def test_is_cache_warm_requires_all_four_timestamps(
    store: SQLiteHistoryStore,
) -> None:
    """``_is_cache_warm`` returns True only when every per-surface
    timestamp is set. A pre-PR catalog with only ``last_full_sync_at``
    populated keeps the live-DB fallback armed."""
    import time

    now = time.time()
    with store._connect() as conn:  # noqa: SLF001
        # Old-shape state row: skeleton timestamps missing.
        conn.execute(
            """INSERT OR REPLACE INTO catalog_profile_state (
                   db_profile, state, last_full_sync_at
               ) VALUES ('legacy', 'done', ?)""",
            (now,),
        )
    conn = DatabaseConnector(
        DBConfig(backend="duckdb", database=":memory:"),
        profile_name="legacy",
    )
    assert conn._is_cache_warm() is False  # noqa: SLF001

    _mark_profile_fully_synced(store, "fresh")
    conn_fresh = DatabaseConnector(
        DBConfig(backend="duckdb", database=":memory:"),
        profile_name="fresh",
    )
    assert conn_fresh._is_cache_warm() is True  # noqa: SLF001


def test_anonymous_connector_is_never_cache_warm(
    store: SQLiteHistoryStore,
) -> None:
    """Connectors built without a profile_name (e.g. an ad-hoc
    DBConfig in a test) must never enter cache-only mode, no matter
    what state rows exist."""
    _mark_profile_fully_synced(store, "any")
    conn = DatabaseConnector(DBConfig(backend="duckdb", database=":memory:"))
    assert conn._is_cache_warm() is False  # noqa: SLF001


class _RaisingConnector(DatabaseConnector):
    """Subclass whose live-DB fallbacks all raise. The cache-only gate
    must short-circuit BEFORE any fallback runs — if the gate is
    broken, the test sees the raising side."""

    def _populate_catalogs_cache(self, catalog: str, *, ttl_seconds: float | None = None) -> bool:
        raise AssertionError("live DB fallback was reached")

    def _populate_schema_metadata_cache(
        self, schema: str, *, ttl_seconds: float | None = None
    ) -> bool:
        raise AssertionError("live DB fallback was reached")

    @property
    def engine(self) -> Any:  # type: ignore[override]
        raise AssertionError("live DB engine was opened")


def test_list_schemas_cache_only_when_warm_and_populated(
    store: SQLiteHistoryStore,
) -> None:
    """Warm profile + NON-EMPTY schemas_cache = serve cached rows, no
    live DB. The cache-only gate is honored when there is something to
    serve."""
    _mark_profile_fully_synced(store, "warm")
    # Populate the schemas_cache for the connector's exact keys:
    # profile='warm', database=':memory:', catalog='' (duckdb).
    store.save_schemas_cache(
        db_profile="warm",
        database=":memory:",
        catalog="",
        entries={"public": None, "analytics": "the analytics schema"},
        bulk_filled=True,
    )
    conn = _RaisingConnector(
        DBConfig(backend="duckdb", database=":memory:"),
        profile_name="warm",
    )
    # No live fallback runs (would raise); cached schemas come back.
    assert sorted(conn.list_schemas()) == ["analytics", "public"]


def test_list_schemas_warm_but_empty_falls_through_to_live(
    store: SQLiteHistoryStore,
) -> None:
    """Warm profile + EMPTY schemas_cache = self-heal by falling through
    to the live path, NOT returning []. A successful sync provably
    produced >=1 schema, so an empty cache here means the rows were
    gc-swept while the never-expiring sync markers stayed set; serving
    [] would flip the freshness pill to a false "no schemas" failure.

    Regression guard for the "Catalog freshness failed on every open"
    bug. With the old gate this returned ``[]``; now it must attempt the
    live path (which the raising connector surfaces)."""
    _mark_profile_fully_synced(store, "warm")
    conn = _RaisingConnector(
        DBConfig(backend="duckdb", database=":memory:"),
        profile_name="warm",
    )
    with pytest.raises(AssertionError, match="live DB"):
        conn.list_schemas()


def test_get_column_comments_cache_only_when_warm(
    store: SQLiteHistoryStore,
) -> None:
    """Cache miss + warm profile = return {}, no live DB."""
    _mark_profile_fully_synced(store, "warm")
    conn = _RaisingConnector(
        DBConfig(backend="duckdb", database=":memory:"),
        profile_name="warm",
    )
    # column_comments capability defaults True for the sqlite stub;
    # the gate fires before the capability check anyway.
    out = conn.get_column_comments("public", "users")
    assert out == {}


def test_get_table_comment_cache_only_when_warm(
    store: SQLiteHistoryStore,
) -> None:
    """Cache miss + warm profile = return None, no live DB."""
    _mark_profile_fully_synced(store, "warm")
    conn = _RaisingConnector(
        DBConfig(backend="duckdb", database=":memory:"),
        profile_name="warm",
    )
    assert conn.get_table_comment("public", "users") is None


def test_get_schema_comment_cache_only_when_warm(
    store: SQLiteHistoryStore,
) -> None:
    """Cache miss + warm profile = return None, no live DB."""
    _mark_profile_fully_synced(store, "warm")
    conn = _RaisingConnector(
        DBConfig(backend="duckdb", database=":memory:"),
        profile_name="warm",
    )
    assert conn.get_schema_comment("public") is None
