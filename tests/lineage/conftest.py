"""Shared fixtures for ``tests/lineage``.

Each test wants the same scaffolding: a fresh on-disk SQLite history
store with a seeded catalog. The helpers here keep individual tests
focused on the behaviour they verify.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def hs(tmp_path: Path) -> SQLiteHistoryStore:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    return store


def seed_table_entity(
    hs: SQLiteHistoryStore,
    *,
    profile: str = "p",
    backend: str = "postgresql",
    database: str = "",
    schema: str,
    table: str,
    asset_kind: str = "table",
) -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO catalog_entities
                (db_profile, db_backend, database_name, schema_name, table_name,
                 entity_kind, asset_kind)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (profile, backend, database, schema, table, "table", asset_kind),
        )
    return int(cur.lastrowid)


def seed_column_entity(
    hs: SQLiteHistoryStore,
    *,
    profile: str = "p",
    backend: str = "postgresql",
    database: str = "",
    schema: str,
    table: str,
    column: str,
    dtype: str = "integer",
    with_description_id: int | None = None,
) -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO catalog_entities
                (db_profile, db_backend, database_name, schema_name, table_name,
                 column_name, entity_kind, asset_kind, dtype,
                 effective_description_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile,
                backend,
                database,
                schema,
                table,
                column,
                "column",
                "table",
                dtype,
                with_description_id,
            ),
        )
    return int(cur.lastrowid)


def seed_foreign_key_relationship(
    hs: SQLiteHistoryStore,
    *,
    from_table_id: int,
    to_table_id: int,
    constrained_columns: Iterable[str],
    referred_columns: Iterable[str],
    referred_table: str,
) -> None:
    payload = {
        "constrained_columns": list(constrained_columns),
        "referred_columns": list(referred_columns),
        "referred_table": referred_table,
    }
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO catalog_relationships
                (from_entity_id, to_entity_id, relationship_type, score, source,
                 details_json, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                from_table_id,
                to_table_id,
                "foreign_key",
                10.0,
                "database",
                json.dumps(payload, ensure_ascii=True),
                time.time(),
            ),
        )


def seed_column_comments_cache_for_table(
    hs: SQLiteHistoryStore,
    *,
    profile: str = "p",
    database: str = "",
    schema: str,
    table: str,
    columns: dict[str, dict[str, Any]],
) -> None:
    """Write directly so tests can control the payload shape used by
    NameMatchExtractor (which reads from `columns_json`)."""
    now = time.time()
    cache_key = f"{profile}|{database}|{schema}|{table}"
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO column_comments_cache
                (cache_key, db_profile, database_name, schema_name, table_name,
                 table_comment, columns_json, kind, fetched_at, expires_at, bulk_filled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET columns_json=excluded.columns_json
            """,
            (
                cache_key,
                profile,
                database,
                schema,
                table,
                None,
                json.dumps(columns, ensure_ascii=True),
                "TABLE",
                now,
                now + 3600,
                1,
            ),
        )
