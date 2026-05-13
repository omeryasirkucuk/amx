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


def _seed_catalog(db_path: Path, profile: str, schemas: list[str]) -> None:
    """Insert a minimal catalog_entities row per schema so the cache-
    first helper returns the schema list."""
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
