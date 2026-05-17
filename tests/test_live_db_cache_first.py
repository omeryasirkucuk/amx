"""``list_schemas`` and ``list_assets`` are cache-first.

The Studio sidebar opens to "Loading schemas…" on every tree expand
when the routers hit the live DB unconditionally. With these endpoints
reading the persistent catalog first, an already-synced profile shows
its schemas instantly and the live DB is only consulted on a miss or
when the SPA explicitly opts in via ``?force_live=true``.
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


def _seed_catalog(
    db_path: Path,
    profile: str,
    schemas: list[str],
    *,
    fully_synced: bool = True,
) -> None:
    """Insert a minimal catalog_entities row per schema so the cache-
    first helper returns the schema list. ``fully_synced`` controls
    whether ``catalog_profile_state`` is set to ``done`` (so the
    completeness gate passes) or left at ``none`` (so the helper
    falls through to the live DB)."""
    now = time.time()
    with sqlite3.connect(db_path) as conn:
        for schema in schemas:
            conn.execute(
                """
                INSERT INTO catalog_entities (
                    db_profile, db_backend, database_name, schema_name,
                    table_name, column_name, entity_kind, asset_kind,
                    search_text, updated_at, last_synced_at
                ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
                """,
                (
                    profile,
                    "postgresql",
                    "appdb",
                    schema,
                    f"{schema}_table",
                    "table",
                    "table",
                    f"path={profile}.{schema}.{schema}_table",
                    now,
                    now,
                ),
            )
        if fully_synced:
            conn.execute(
                """
                INSERT INTO catalog_profile_state (
                    db_profile, state, total_tables, processed_tables,
                    started_at, finished_at, last_full_sync_at, last_error
                ) VALUES (?, 'done', ?, ?, ?, ?, ?, '')
                """,
                (profile, len(schemas), len(schemas), now, now, now),
            )


def test_cached_schemas_helper_returns_rows(seeded_history_store: Path) -> None:
    _seed_catalog(seeded_history_store, "prof-a", ["public", "analytics"])
    out = live_db._cached_schemas_for_profile("prof-a")  # noqa: SLF001
    assert out is not None
    names = {item["name"] for item in out}
    assert names == {"public", "analytics"}


def test_cached_schemas_helper_returns_none_on_miss(seeded_history_store: Path) -> None:
    _seed_catalog(seeded_history_store, "prof-a", ["public"])
    out = live_db._cached_schemas_for_profile("prof-b")  # noqa: SLF001
    assert out is None


def test_cached_assets_helper_returns_rows(seeded_history_store: Path) -> None:
    _seed_catalog(seeded_history_store, "prof-a", ["public"])
    out = live_db._cached_assets_for_profile_schema("prof-a", "public")  # noqa: SLF001
    assert out is not None
    assert {item["name"] for item in out} == {"public_table"}
    # Sidebar uses the kind for icon selection — every catalog-served
    # row defaults to "table" and the Table-detail page falls through
    # to the live DB for the real kind.
    assert all(item["kind"] == "table" for item in out)


def test_cached_assets_helper_returns_none_on_miss(seeded_history_store: Path) -> None:
    _seed_catalog(seeded_history_store, "prof-a", ["public"])
    out = live_db._cached_assets_for_profile_schema("prof-a", "missing")  # noqa: SLF001
    assert out is None


def test_cache_helpers_serve_partial_sync_data(
    seeded_history_store: Path,
) -> None:
    """A profile that has rows in ``catalog_entities`` but no
    ``state='done'`` row in ``catalog_profile_state`` now serves the
    cached rows (the caller stamps ``possibly_partial`` on the
    response). Pre-PR the helper bailed and the sidebar hit the live
    DB on every expand of a half-synced or week-old profile — that's
    exactly the cost the user reported."""
    _seed_catalog(seeded_history_store, "prof-a", ["public"], fully_synced=False)
    schemas = live_db._cached_schemas_for_profile("prof-a")  # noqa: SLF001
    assert schemas is not None
    assert {row["name"] for row in schemas} == {"public"}
    assets = live_db._cached_assets_for_profile_schema(  # noqa: SLF001
        "prof-a", "public"
    )
    assert assets is not None
    # Seeded fixture writes one table named ``public_table`` under
    # each schema (see _seed_catalog above). What matters is that
    # *something* came back from the cache instead of None.
    assert len(assets) >= 1
    # The fully-synced probe should still report False so the route
    # can flag ``possibly_partial`` in the SSE response.
    assert live_db._profile_is_fully_synced("prof-a") is False  # noqa: SLF001
