"""Local-SQLite → shared migration tests.

The migration helper walks the FK graph (``analysis_runs`` →
``run_results`` → ``app_events``), assigns UUID PKs to shared rows,
records the original local INT id + hostname so the dual-write
coordinator can later look up the right shared row.

These tests exercise the migration against an in-memory SQLite-backed
"shared" engine (no network, fast) and assert the row counts and FK
linkage round-trip.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine

from amx.storage.migration import migrate_local_to_shared
from amx.storage.shared_schema import build_metadata
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore


def _make_shared(tmp_path: Path) -> SQLAlchemyHistoryStore:
    db_path = tmp_path / "shared.db"
    engine = create_engine(f"sqlite:///{db_path}")
    md = build_metadata(schema="main")
    md.create_all(engine)
    s = SQLAlchemyHistoryStore.__new__(SQLAlchemyHistoryStore)
    s.engine = engine
    s.schema = "main"
    s._md = md
    s._t_runs = md.tables["main.analysis_runs"]
    s._t_results = md.tables["main.run_results"]
    s._t_events = md.tables["main.app_events"]
    s._t_session = md.tables["main.session_state"]
    s._t_meta = md.tables["main.schema_meta"]
    s._hostname = "test-host"
    s._username = "test-user"
    s._client_version = "0.12.0-test"
    return s


@pytest.fixture
def seeded_local(tmp_path: Path) -> SQLiteHistoryStore:
    db_path = tmp_path / "local.db"
    s = SQLiteHistoryStore(db_path)
    s.init()
    rid = s.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"public": ["users", "orders"]},
        selected_count=2,
        planned_count=2,
    )
    s.save_run_results(
        rid,
        [
            {
                "schema": "public",
                "table": "users",
                "column": "email",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "high",
                "alternatives": ["user email"],
            },
            {
                "schema": "public",
                "table": "orders",
                "column": "id",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "medium",
                "alternatives": ["order id"],
            },
        ],
    )
    s.finish_run(rid, status="success", metrics={}, tokens={}, results={"applied": 0})
    s.log_event(event_type="analyze.run", status="success", command="/run", details={})
    return s


def test_migration_copies_all_tables(seeded_local: SQLiteHistoryStore, tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    stats = migrate_local_to_shared(local=seeded_local, shared=shared)
    assert stats["analysis_runs"] == 1
    assert stats["run_results"] == 2
    assert stats["app_events"] == 1


def test_migration_is_idempotent(seeded_local: SQLiteHistoryStore, tmp_path: Path) -> None:
    """Running the migration twice copies once and skips the second run."""
    shared = _make_shared(tmp_path)
    first = migrate_local_to_shared(local=seeded_local, shared=shared)
    second = migrate_local_to_shared(local=seeded_local, shared=shared)
    assert first["analysis_runs"] == 1
    assert second["analysis_runs"] == 0  # nothing new to copy
    assert first["run_results"] == 2
    assert second["run_results"] == 0


def test_migration_preserves_fks(seeded_local: SQLiteHistoryStore, tmp_path: Path) -> None:
    """Each shared run_results row should resolve back to its parent run."""
    shared = _make_shared(tmp_path)
    migrate_local_to_shared(local=seeded_local, shared=shared)
    runs = shared.list_recent_runs(limit=10, command_filter="analyze.run")
    assert len(runs) == 1
    parent_uuid = runs[0]["id"]
    results = shared.get_run_results(parent_uuid)
    assert len(results) == 2
    assert all(r["run_id"] == parent_uuid for r in results)


def test_migration_records_local_id(seeded_local: SQLiteHistoryStore, tmp_path: Path) -> None:
    """``find_run_uuid_by_local_id`` finds the shared row by INT id."""
    shared = _make_shared(tmp_path)
    migrate_local_to_shared(local=seeded_local, shared=shared)
    # The seed created run id=1 in local — assert we can look it up.
    found = shared.find_run_uuid_by_local_id(1)
    assert found is not None
