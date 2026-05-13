"""Tests for the ``(database, catalog)`` overlay on ``scheduled_runs``.

The picker is database-scoped (ScopeTree fetches schemas via
``/api/live/schemas?profile=…&database=…``) so the schedule needs the
``database`` half persisted alongside the profile. Without it the
scheduler would fire against the profile default at run time and the
saved schema picks would resolve in the wrong DB.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def test_create_persists_database_overlay(store: SQLiteHistoryStore) -> None:
    sid = store.create_scheduled_run(
        name="airline-meta",
        fire_at_utc=1_700_000_000.0,
        fire_at_tz="Europe/Istanbul",
        db_profile="local-postgre",
        database="bird",
        catalog=None,
        scope_json=json.dumps({"mode": "tables", "tables": []}),
        llm_profile="claude",
        review_strategy="auto",
    )
    row = store.get_scheduled_run(sid)
    assert row is not None
    assert row["db_profile"] == "local-postgre"
    assert row["database"] == "bird"
    assert row["catalog"] is None


def test_create_persists_catalog_overlay(store: SQLiteHistoryStore) -> None:
    sid = store.create_scheduled_run(
        name="lakehouse-refresh",
        fire_at_utc=1_700_000_000.0,
        fire_at_tz="UTC",
        db_profile="dbr",
        database=None,
        catalog="main",
        scope_json=json.dumps({"mode": "all"}),
        llm_profile="claude",
        review_strategy="auto",
    )
    row = store.get_scheduled_run(sid)
    assert row is not None
    assert row["catalog"] == "main"
    assert row["database"] is None


def test_create_without_overlay_keeps_both_null(
    store: SQLiteHistoryStore,
) -> None:
    sid = store.create_scheduled_run(
        name="legacy-style",
        fire_at_utc=1_700_000_000.0,
        fire_at_tz="UTC",
        db_profile="sqlite-local",
        scope_json=json.dumps({"mode": "all"}),
        llm_profile="claude",
        review_strategy="auto",
    )
    row = store.get_scheduled_run(sid)
    assert row is not None
    assert row["database"] is None
    assert row["catalog"] is None


def test_update_can_set_and_clear_database(
    store: SQLiteHistoryStore,
) -> None:
    sid = store.create_scheduled_run(
        name="repair-flow",
        fire_at_utc=1_700_000_000.0,
        fire_at_tz="UTC",
        db_profile="local-postgre",
        scope_json=json.dumps({"mode": "all"}),
        llm_profile="claude",
        review_strategy="auto",
    )
    # Fix-up flow: user edits a legacy schedule and picks the right DB.
    store.update_scheduled_run(sid, patch={"database": "bird"})
    assert store.get_scheduled_run(sid)["database"] == "bird"

    # Clearing reverts to the profile default.
    store.update_scheduled_run(sid, patch={"database": None})
    assert store.get_scheduled_run(sid)["database"] is None


def test_idempotent_migration_on_legacy_table(tmp_path: Path) -> None:
    """A pre-existing scheduled_runs table missing ``database`` /
    ``catalog`` gets the columns added on next ``init()`` without
    losing rows.
    """
    db_path = tmp_path / "legacy.db"
    s1 = SQLiteHistoryStore(db_path)
    s1.init()
    # Simulate a legacy install: drop the new columns by recreating
    # the table without them, keeping a single legacy row.
    with s1._lock, s1._connect() as conn:  # type: ignore[attr-defined]
        conn.execute("ALTER TABLE scheduled_runs RENAME TO scheduled_runs_old")
        conn.execute(
            """
            CREATE TABLE scheduled_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                fire_at_utc REAL NOT NULL,
                fire_at_tz TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                db_profile TEXT NOT NULL,
                scope_json TEXT NOT NULL,
                llm_profile TEXT NOT NULL,
                review_strategy TEXT NOT NULL,
                extra_args_json TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                fired_at REAL,
                triggered_run_id INTEGER,
                last_error TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO scheduled_runs (
                name, fire_at_utc, fire_at_tz, db_profile,
                scope_json, llm_profile, review_strategy,
                created_at, updated_at
            ) VALUES ('legacy', 1, 'UTC', 'p', '{}', 'l', 'auto', 1, 1)
            """
        )
        conn.execute("DROP TABLE scheduled_runs_old")

    # Re-init: the migration helper must add the two columns without
    # erroring and the legacy row must survive.
    s2 = SQLiteHistoryStore(db_path)
    s2.init()
    rows = s2.list_scheduled_runs()
    assert len(rows) == 1
    assert rows[0]["name"] == "legacy"
    assert rows[0]["database"] is None
    assert rows[0]["catalog"] is None
