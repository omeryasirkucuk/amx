"""Tests for the db_profile + attribution columns on documentation_pages.

Verifies that the shared SQLAlchemy schema and local SQLite store both
carry the four new columns added in PR-2: db_profile, hostname,
client_version, local_id.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.storage.shared_schema import build_metadata
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    db = SQLiteHistoryStore(tmp_path / "history.db")
    db.init()
    return db


def _shared_page_column_names() -> set[str]:
    md = build_metadata("AMX")
    table = md.tables["AMX.documentation_pages"]
    return {c.name for c in table.columns}


def _shared_page_index_names() -> set[str]:
    md = build_metadata("AMX")
    table = md.tables["AMX.documentation_pages"]
    return {idx.name for idx in table.indexes}


def _local_page_column_names(store: SQLiteHistoryStore) -> set[str]:
    with store._connect() as conn:
        rows = conn.execute("PRAGMA table_info(documentation_pages)").fetchall()
    return {str(r[1]) for r in rows}


# ── shared schema tests ───────────────────────────────────────────────────────


def test_documentation_pages_has_db_profile_column() -> None:
    """db_profile column exists in shared schema and has a dedicated index."""
    cols = _shared_page_column_names()
    assert "db_profile" in cols, "db_profile column missing from shared documentation_pages"
    indexes = _shared_page_index_names()
    assert "ix_documentation_pages_db_profile" in indexes, (
        "ix_documentation_pages_db_profile index missing from shared documentation_pages"
    )


def test_documentation_pages_has_attribution_columns() -> None:
    """hostname, client_version, and local_id columns exist in shared schema."""
    cols = _shared_page_column_names()
    for col in ("hostname", "client_version", "local_id"):
        assert col in cols, f"{col} column missing from shared documentation_pages"
    indexes = _shared_page_index_names()
    assert "ix_documentation_pages_local_lookup" in indexes, (
        "ix_documentation_pages_local_lookup composite index missing"
    )


# ── local SQLite tests ────────────────────────────────────────────────────────


def test_local_documentation_pages_has_db_profile_column(store: SQLiteHistoryStore) -> None:
    """db_profile column is present in the local SQLite documentation_pages table."""
    cols = _local_page_column_names(store)
    assert "db_profile" in cols, "db_profile column missing from local SQLite documentation_pages"


def test_local_documentation_pages_has_attribution_columns(store: SQLiteHistoryStore) -> None:
    """hostname, client_version, and local_id columns exist in local SQLite store."""
    cols = _local_page_column_names(store)
    for col in ("hostname", "client_version", "local_id"):
        assert col in cols, f"{col} column missing from local SQLite documentation_pages"


def test_ensure_column_exists_is_idempotent_on_sqlite(tmp_path: Path) -> None:
    """ensure_column_exists can be called twice without raising errors.

    Uses an in-memory SQLite store to exercise the idempotency path of
    the migration helper — the second call must be a silent no-op.
    """
    from sqlalchemy import create_engine, text

    from amx.storage.migration import ensure_column_exists

    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE test_table (id INTEGER PRIMARY KEY)"))

    # First call: adds the column.
    ensure_column_exists(engine, None, "test_table", "extra_col", "TEXT")
    # Second call: must be idempotent (no error).
    ensure_column_exists(engine, None, "test_table", "extra_col", "TEXT")

    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(test_table)")).fetchall()
    col_names = {str(r[1]) for r in rows}
    assert "extra_col" in col_names, "Column was not added by ensure_column_exists"
