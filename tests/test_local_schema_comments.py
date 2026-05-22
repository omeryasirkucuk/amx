"""Dogfooding tests: AMX's own local SQLite history store must ship descriptions.

Companion to ``tests/test_shared_schema_comments.py`` for the local SQLite
DB at ``~/.amx/history.db``. SQLite has no native ``COMMENT ON`` syntax,
so descriptions live in the ``_amx_schema_descriptions`` sidecar table
populated at :meth:`SQLiteHistoryStore.init` time from
:data:`amx.storage.schema_descriptions.SCHEMA_DESCRIPTIONS`.

AMX's product thesis is "every table and column should ship with a
meaningful description." These tests pin that AMX's *own* internal
storage meets that bar. If a contributor adds a new table or column to
``amx/storage/sqlite_store.py`` without a matching entry in
``schema_descriptions.py``, the tests below fail fast — preventing the
embarrassment of shipping a metadata tool that does not annotate its
own outputs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from amx.storage.schema_descriptions import SCHEMA_DESCRIPTIONS
from amx.storage.shared_schema import build_metadata
from amx.storage.sqlite_store import SQLiteHistoryStore

# FTS5 creates shadow tables named ``<fts>_data``, ``_idx``, ``_content``,
# ``_docsize``, ``_config`` that show up in sqlite_master with type='table'.
# These are SQLite implementation details, not AMX-owned tables, and have
# no place in SCHEMA_DESCRIPTIONS.
_FTS_SHADOW_PREFIXES = (
    "catalog_entities_fts_",
    "fts_notebooks_",
    "fts_queries_",
    "fts_jobs_",
    "fts_pipelines_",
    "fts_streams_",
    "fts_streamlit_",
)


def _is_shadow(name: str) -> bool:
    return any(name.startswith(p) for p in _FTS_SHADOW_PREFIXES)


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    db = SQLiteHistoryStore(tmp_path / "history.db")
    db.init()
    return db


def _real_tables_and_columns(store: SQLiteHistoryStore) -> dict[str, list[str]]:
    """Return ``{table_name: [columns]}`` for every AMX-owned table in the DB.

    FTS5 shadow tables are excluded — they are SQLite implementation
    details, not part of AMX's declared schema.
    """
    with store._connect() as conn:
        names = [
            str(r[0])
            for r in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            if not _is_shadow(str(r[0]))
        ]
        out: dict[str, list[str]] = {}
        for table in names:
            cols = [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            out[table] = cols
    return out


def test_every_real_table_has_a_description(store: SQLiteHistoryStore) -> None:
    """Every table SQLiteHistoryStore.init() creates has a ``__table__`` entry."""
    live = _real_tables_and_columns(store)
    missing: list[str] = []
    for table_name in live:
        entry = SCHEMA_DESCRIPTIONS.get(table_name)
        if entry is None or not (entry.get("__table__") or "").strip():
            missing.append(table_name)
    assert not missing, (
        "Tables created by SQLiteHistoryStore.init() but missing a __table__ "
        "description in amx/storage/schema_descriptions.py: "
        f"{missing}"
    )


def test_every_real_column_has_a_description(store: SQLiteHistoryStore) -> None:
    """Every PRAGMA-reported column has a SCHEMA_DESCRIPTIONS entry."""
    live = _real_tables_and_columns(store)
    missing: list[str] = []
    for table, cols in live.items():
        fields = SCHEMA_DESCRIPTIONS.get(table, {})
        for col in cols:
            if not (fields.get(col) or "").strip():
                missing.append(f"{table}.{col}")
    assert not missing, (
        f"{len(missing)} live SQLite columns missing a description "
        f"in amx/storage/schema_descriptions.py (first 10): {missing[:10]}"
    )


def test_sidecar_is_populated(store: SQLiteHistoryStore) -> None:
    """The ``_amx_schema_descriptions`` sidecar gets the expected row counts."""
    live = _real_tables_and_columns(store)
    with store._connect() as conn:
        rows = conn.execute(
            "SELECT object_kind, COUNT(*) FROM _amx_schema_descriptions GROUP BY object_kind"
        ).fetchall()
    by_kind = {str(r[0]): int(r[1]) for r in rows}
    assert by_kind.get("database", 0) == 1, "missing database-level description row"
    # +1 for the sidecar table itself (it self-describes).
    expected_tables = len(live)
    assert by_kind.get("table", 0) == expected_tables, (
        f"sidecar 'table' row count {by_kind.get('table', 0)} != live table count {expected_tables}"
    )
    expected_columns = sum(len(cols) for cols in live.values())
    assert by_kind.get("column", 0) == expected_columns, (
        f"sidecar 'column' row count {by_kind.get('column', 0)} "
        f"!= live column count {expected_columns}"
    )


def test_sidecar_descriptions_are_non_empty(store: SQLiteHistoryStore) -> None:
    """No row in the sidecar carries an empty description string."""
    with store._connect() as conn:
        empties = [
            (str(r[0]), str(r[1]), str(r[2]), str(r[3]))
            for r in conn.execute(
                "SELECT object_kind, schema_name, table_name, column_name "
                "FROM _amx_schema_descriptions "
                "WHERE description IS NULL OR TRIM(description) = ''"
            ).fetchall()
        ]
    assert not empties, f"Sidecar rows with empty description: {empties}"


def test_no_orphan_schema_descriptions_entries(store: SQLiteHistoryStore) -> None:
    """Every SCHEMA_DESCRIPTIONS column entry for a real local table must
    correspond to an actual column either in the local SQLite store or the
    shared SQLAlchemy store (some columns are local-only, some shared-only).
    """
    live = _real_tables_and_columns(store)
    md = build_metadata("AMX")
    shared_cols_by_table: dict[str, set[str]] = {
        t.name: {c.name for c in t.columns} for t in md.tables.values()
    }
    orphans: list[str] = []
    for table, fields in SCHEMA_DESCRIPTIONS.items():
        if table not in live and table not in shared_cols_by_table:
            # Table is declared in SoT but doesn't exist in any store yet.
            # Could be an in-development table — flag it.
            orphans.append(f"<table:{table}>")
            continue
        live_cols = set(live.get(table, []))
        shared_cols = shared_cols_by_table.get(table, set())
        for col_name in fields:
            if col_name == "__table__":
                continue
            if col_name not in live_cols and col_name not in shared_cols:
                orphans.append(f"{table}.{col_name}")
    assert not orphans, (
        "Orphan SCHEMA_DESCRIPTIONS entries (do not match any real local or "
        f"shared schema column): {orphans}"
    )


def test_local_and_shared_agree_on_overlapping_columns(
    store: SQLiteHistoryStore,
) -> None:
    """For tables that exist in both stores, columns present in BOTH must
    carry the same description string byte-for-byte. Catches drift if a
    contributor hardcodes a comment back into shared_schema.py instead of
    sourcing it from SCHEMA_DESCRIPTIONS.
    """
    live = _real_tables_and_columns(store)
    md = build_metadata("AMX")
    mismatches: list[str] = []
    for table in md.tables.values():
        if table.name not in live:
            continue
        live_cols = set(live[table.name])
        for col in table.columns:
            if col.name not in live_cols:
                continue
            local_desc = SCHEMA_DESCRIPTIONS[table.name].get(col.name) or ""
            shared_desc = col.comment or ""
            if local_desc != shared_desc:
                mismatches.append(
                    f"{table.name}.{col.name}: local={local_desc!r} vs shared={shared_desc!r}"
                )
    assert not mismatches, (
        f"Local and shared descriptions diverge for {len(mismatches)} "
        f"overlapping columns (first 5): {mismatches[:5]}"
    )


def test_database_level_description_present(store: SQLiteHistoryStore) -> None:
    """The single ``object_kind='database'`` row has the canonical text."""
    from amx.storage.schema_descriptions import LOCAL_DATABASE_DESCRIPTION

    with store._connect() as conn:
        row = conn.execute(
            "SELECT description FROM _amx_schema_descriptions WHERE object_kind = 'database'"
        ).fetchone()
    assert row is not None, "no database-level description row in sidecar"
    assert str(row[0]) == LOCAL_DATABASE_DESCRIPTION


def test_idempotent_repopulate(store: SQLiteHistoryStore) -> None:
    """Re-running init() does not duplicate sidecar rows."""
    with store._connect() as conn:
        first_count = int(
            conn.execute("SELECT COUNT(*) FROM _amx_schema_descriptions").fetchone()[0]
        )
    store.init()
    store.init()
    with store._connect() as conn:
        second_count = int(
            conn.execute("SELECT COUNT(*) FROM _amx_schema_descriptions").fetchone()[0]
        )
    assert first_count == second_count, (
        f"Re-init() changed sidecar row count: {first_count} -> {second_count}"
    )


def _connect_signature_is_unchanged(store: SQLiteHistoryStore) -> Any:
    """Sanity-check the private API this test file leans on still exists."""
    # If this attribute disappears, every other test in this module is
    # already failing — this only exists to give a clearer error message.
    return store._connect


def test_internal_api_still_present(store: SQLiteHistoryStore) -> None:
    assert callable(_connect_signature_is_unchanged(store))
