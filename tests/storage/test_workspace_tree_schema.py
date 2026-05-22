"""PR-E: ``remote_workspace_tree`` cache table + schema descriptions."""

from __future__ import annotations

import sqlite3

from amx.storage.schema_descriptions import SCHEMA_DESCRIPTIONS
from amx.storage.sqlite_store import SQLiteHistoryStore


def test_init_creates_workspace_tree_table(tmp_path):
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(remote_workspace_tree)").fetchall()}
    expected = {
        "profile_name",
        "kind",
        "path",
        "parent_path",
        "name",
        "is_directory",
        "external_id",
        "owner",
        "last_modified",
        "children_fetched_at",
        "fetched_at",
    }
    assert expected.issubset(cols)


def test_init_creates_workspace_tree_parent_index(tmp_path):
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        idx = {r[1] for r in conn.execute("PRAGMA index_list(remote_workspace_tree)").fetchall()}
    assert any("parent" in i for i in idx)


def test_schema_descriptions_cover_workspace_tree():
    desc = SCHEMA_DESCRIPTIONS.get("remote_workspace_tree", {})
    for col in (
        "__table__",
        "profile_name",
        "kind",
        "path",
        "parent_path",
        "name",
        "is_directory",
        "external_id",
        "owner",
        "last_modified",
        "children_fetched_at",
        "fetched_at",
    ):
        assert desc.get(col), f"remote_workspace_tree.{col} missing description"


def test_migration_adds_table_to_legacy_db(tmp_path):
    db_path = tmp_path / "history.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE legacy_marker (x INTEGER)")
        conn.commit()
    SQLiteHistoryStore(db_path).init()
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='remote_workspace_tree'"
        ).fetchall()
    assert rows
