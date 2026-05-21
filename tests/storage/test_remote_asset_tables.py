"""Storage tests: remote_notebooks, remote_jobs, remote_job_tasks, remote_job_runs tables.

Verifies that SQLiteHistoryStore.init() creates the four remote-asset
tables and that every column carries a non-empty description in
SCHEMA_DESCRIPTIONS.
"""

from __future__ import annotations

from pathlib import Path


def _new_store(tmp_path: Path):
    from amx.storage.sqlite_store import SQLiteHistoryStore

    store = SQLiteHistoryStore(tmp_path / "amx.db")
    store.init()
    return store


def test_remote_notebooks_table_created(tmp_path: Path) -> None:
    import sqlite3

    store = _new_store(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        cur = conn.execute("PRAGMA table_info(remote_notebooks)")
        cols = {row[1] for row in cur.fetchall()}
    expected = {
        "id",
        "profile_name",
        "platform",
        "external_id",
        "name",
        "workspace_path",
        "qualified_name",
        "language",
        "source_text",
        "source_hash",
        "last_modified_at",
        "last_modified_by",
        "owner",
        "cell_count",
        "ingested_at",
    }
    assert expected <= cols


def test_remote_jobs_tables_created(tmp_path: Path) -> None:
    import sqlite3

    store = _new_store(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        for tbl in ("remote_jobs", "remote_job_tasks", "remote_job_runs"):
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
            )
            assert cur.fetchone() is not None, f"{tbl} not created"


def test_schema_descriptions_cover_new_tables() -> None:
    from amx.storage.schema_descriptions import SCHEMA_DESCRIPTIONS

    for tbl in ("remote_notebooks", "remote_jobs", "remote_job_tasks", "remote_job_runs"):
        assert tbl in SCHEMA_DESCRIPTIONS, f"{tbl} missing from SCHEMA_DESCRIPTIONS"
        entry = SCHEMA_DESCRIPTIONS[tbl]
        assert entry.get("__table__"), f"{tbl}.__table__ description empty"
        for col, desc in entry.items():
            if col == "__table__":
                continue
            assert desc and desc.strip(), f"{tbl}.{col} description empty"
