"""Tests for purge_out_of_scope — the migration helper that strips
cached rows belonging to containers outside the profile's pinned
default."""

from __future__ import annotations

import pytest

from amx.storage._history_caches import (
    purge_out_of_scope,
    save_column_comments_cache,
    save_schemas_cache,
)
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def hs(tmp_path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def test_purge_removes_only_out_of_scope_rows(hs: SQLiteHistoryStore) -> None:
    save_column_comments_cache(
        hs,
        db_profile="prof",
        database="prod",
        schema="public",
        entries={"orders": {"table_comment": "ok", "columns": {}, "kind": "TABLE"}},
    )
    save_column_comments_cache(
        hs,
        db_profile="prof",
        database="dev",
        schema="public",
        entries={"orders": {"table_comment": "stale", "columns": {}, "kind": "TABLE"}},
    )
    counts = purge_out_of_scope(hs, db_profile="prof", container="prod")
    assert counts["column_comments_cache"] == 1

    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT database_name FROM column_comments_cache WHERE db_profile = ?",
            ("prof",),
        ).fetchall()
    assert [r["database_name"] for r in rows] == ["prod"]


def test_purge_is_idempotent(hs: SQLiteHistoryStore) -> None:
    save_column_comments_cache(
        hs,
        db_profile="prof",
        database="dev",
        schema="public",
        entries={"orders": {"table_comment": "x", "columns": {}, "kind": "TABLE"}},
    )
    first = purge_out_of_scope(hs, db_profile="prof", container="prod")
    second = purge_out_of_scope(hs, db_profile="prof", container="prod")
    assert first["column_comments_cache"] == 1
    assert second["column_comments_cache"] == 0


def test_purge_leaves_other_profiles_alone(hs: SQLiteHistoryStore) -> None:
    save_column_comments_cache(
        hs,
        db_profile="other",
        database="dev",
        schema="public",
        entries={"orders": {"table_comment": "x", "columns": {}, "kind": "TABLE"}},
    )
    counts = purge_out_of_scope(hs, db_profile="prof", container="prod")
    assert counts["column_comments_cache"] == 0
    with hs._connect() as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM column_comments_cache WHERE db_profile = ?",
            ("other",),
        ).fetchone()["n"]
    assert n == 1


def test_purge_handles_schemas_cache_by_database(hs: SQLiteHistoryStore) -> None:
    save_schemas_cache(
        hs,
        db_profile="prof",
        database="prod",
        catalog="",
        entries={"public": "ok"},
    )
    save_schemas_cache(
        hs,
        db_profile="prof",
        database="dev",
        catalog="",
        entries={"public": "stale"},
    )
    counts = purge_out_of_scope(hs, db_profile="prof", container="prod")
    assert counts["schemas_cache"] == 1


def test_purge_handles_schemas_cache_by_catalog(hs: SQLiteHistoryStore) -> None:
    save_schemas_cache(
        hs,
        db_profile="prof",
        database="",
        catalog="prod",
        entries={"public": "ok"},
    )
    save_schemas_cache(
        hs,
        db_profile="prof",
        database="",
        catalog="dev",
        entries={"public": "stale"},
    )
    counts = purge_out_of_scope(hs, db_profile="prof", container="prod")
    assert counts["schemas_cache"] == 1


def test_purge_handles_catalog_entities(hs: SQLiteHistoryStore) -> None:
    with hs._lock, hs._connect() as conn:
        for db_name in ("prod", "dev"):
            conn.execute(
                "INSERT INTO catalog_entities "
                "(db_profile, db_backend, database_name, schema_name, "
                "table_name, entity_kind) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("prof", "databricks", db_name, "public", "orders", "table"),
            )
    counts = purge_out_of_scope(hs, db_profile="prof", container="prod")
    assert counts["catalog_entities"] == 1
    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT database_name FROM catalog_entities WHERE db_profile = ?",
            ("prof",),
        ).fetchall()
    assert [r["database_name"] for r in rows] == ["prod"]


def test_purge_with_empty_container_is_noop(hs: SQLiteHistoryStore) -> None:
    save_column_comments_cache(
        hs,
        db_profile="prof",
        database="dev",
        schema="public",
        entries={"orders": {"table_comment": "x", "columns": {}, "kind": "TABLE"}},
    )
    counts = purge_out_of_scope(hs, db_profile="prof", container="")
    assert counts == {
        "catalog_entities": 0,
        "schemas_cache": 0,
        "column_comments_cache": 0,
    }
