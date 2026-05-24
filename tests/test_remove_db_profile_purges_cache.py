"""``/remove-db-profile`` purges the catalog cache.

Reported: Studio's Catalog cache page showed ``catalog_entities``
with 3 profile(s) while only 2 DB profiles were configured. Root
cause — ``AMXConfig.remove_db_profile`` deleted the profile entry
from the config but left every ``catalog_entities`` /
``schemas_cache`` / ``column_comments_cache`` row keyed by the
removed profile name on disk. The next ``/stats`` call surfaced the
tombstone as a phantom third profile.

This test pins the contract: removing a DB profile clears every
cache row keyed by that profile in the same call. The startup
sweep + the read-side ``valid_profiles`` filter are belt-and-braces
for older databases that already have tombstones; this test is
specifically for the eager path.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from amx.config import AMXConfig, DBConfig
from amx.storage import sqlite_store as ss
from amx.storage.cache_ops import cache_inventory
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SQLiteHistoryStore:
    db_path = tmp_path / "history.db"
    s = SQLiteHistoryStore(db_path)
    s.init()
    monkeypatch.setattr(ss, "_store", s, raising=False)  # noqa: SLF001
    yield s
    monkeypatch.setattr(ss, "_store", None, raising=False)  # noqa: SLF001


def _seed_cache_rows(s: SQLiteHistoryStore, profile: str) -> None:
    s.save_schemas_cache(
        db_profile=profile,
        database="db1",
        catalog="",
        entries={"public": None},
        ttl_seconds=3600.0,
    )
    s.save_column_comments_cache(
        db_profile=profile,
        database="db1",
        schema="public",
        entries={
            "users": {
                "table_comment": None,
                "columns": {"id": None},
                "kind": "TABLE",
            },
        },
        ttl_seconds=3600.0,
    )
    with s._connect() as conn:  # noqa: SLF001
        conn.execute(
            """INSERT INTO catalog_entities (
                   db_profile, db_backend, database_name, schema_name,
                   table_name, column_name, entity_kind, asset_kind,
                   last_synced_at
               ) VALUES (?, 'postgresql', 'db1', 'public', 'users', NULL,
                          'table', 'table', ?)""",
            (profile, time.time()),
        )


def test_remove_db_profile_evicts_cache_rows(store: SQLiteHistoryStore) -> None:
    cfg = AMXConfig()
    cfg.db_profiles = {
        "keeper": DBConfig(backend="postgresql", host="k"),
        "doomed": DBConfig(backend="postgresql", host="d"),
    }
    cfg.active_db_profile = "keeper"
    cfg.active_db_profiles = ["keeper", "doomed"]

    _seed_cache_rows(store, "keeper")
    _seed_cache_rows(store, "doomed")
    pre = cache_inventory()
    assert {(r.profile, r.database) for r in pre} == {
        ("keeper", "db1"),
        ("doomed", "db1"),
    }

    cfg.remove_db_profile("doomed")

    post = cache_inventory()
    # Only the surviving profile still has cache rows; the doomed
    # profile's footprint is fully removed.
    assert {(r.profile, r.database) for r in post} == {("keeper", "db1")}
