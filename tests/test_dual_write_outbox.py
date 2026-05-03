"""Tests for the dual-write outbox semantics.

When the shared store fails on a write, ``DualWriteHistoryStore``
must:
1. Keep the local row (the user's CLI session never breaks).
2. Queue the failed op in ``pending_shared_writes``.
3. Retry it on ``flush_pending()``.

These tests fake the shared store with a controllable mock so we can
flip its behaviour mid-test (fail → succeed) and assert the outbox
drains as expected.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine

from amx.storage.dual_write import DualWriteHistoryStore
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
def dual(tmp_path: Path) -> DualWriteHistoryStore:
    local = SQLiteHistoryStore(tmp_path / "local.db")
    local.init()
    shared = _make_shared(tmp_path)
    return DualWriteHistoryStore(local=local, shared=shared)


def test_create_run_writes_to_both(dual: DualWriteHistoryStore) -> None:
    rid = dual.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"public": ["t"]},
    )
    assert isinstance(rid, int) and rid > 0
    # Local has the run
    assert dual.local.get_run(rid) is not None
    # Shared has a row keyed by hostname + local_id
    found = dual.shared.find_run_uuid_by_local_id(rid)
    assert found is not None
    # Outbox is empty
    assert dual.pending_count() == 0


def test_outbox_grows_when_shared_fails(dual: DualWriteHistoryStore) -> None:
    """When shared.create_run raises, the local row still exists and the
    op is queued for retry."""
    # Replace shared store's create_run with a raising mock so the
    # next dual write fails just on the shared side.
    original = dual.shared.create_run
    dual.shared.create_run = MagicMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("network down")
    )
    rid = dual.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"public": ["t"]},
    )
    assert isinstance(rid, int)
    assert dual.local.get_run(rid) is not None
    assert dual.pending_count() == 1

    # Restore the real shared.create_run and flush — outbox should drain.
    dual.shared.create_run = original  # type: ignore[method-assign]
    succeeded, remaining = dual.flush_pending()
    assert succeeded == 1
    assert remaining == 0


def test_flush_pending_does_not_drop_failed_after_max_attempts(
    dual: DualWriteHistoryStore,
) -> None:
    """A persistently-failing op stays in the outbox so the user can
    inspect it. We never silently drop work."""
    dual.shared.create_run = MagicMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("still down")
    )
    for _ in range(3):
        dual.create_run(
            command="analyze.run",
            mode="auto",
            db_backend="postgresql",
            db_profile="default",
            llm_provider="openai",
            llm_model="gpt-5",
            scope={"public": ["t"]},
        )
    assert dual.pending_count() == 3
    # Flush; everything still fails — succeeded=0, remaining=3.
    succeeded, remaining = dual.flush_pending()
    assert succeeded == 0
    assert remaining == 3


def test_log_event_dual_write(dual: DualWriteHistoryStore) -> None:
    dual.log_event(event_type="cli.start", status="ok", command="/", details={"v": 1})
    local_events = dual.local.list_recent_events(limit=10)
    shared_events = dual.shared.list_recent_events(limit=10)
    assert len(local_events) == 1
    assert len(shared_events) == 1
    assert local_events[0]["event_type"] == "cli.start"
    assert shared_events[0]["event_type"] == "cli.start"


def test_reads_come_from_local(dual: DualWriteHistoryStore) -> None:
    """Reads always go through local SQLite — even when shared has different data.

    This is intentional: shared mode v0.12 is dual-write but local-read
    so /history list is always fast and consistent with the user's
    machine. Team-wide reads are a follow-up minor.
    """
    rid = dual.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"public": ["t"]},
    )
    listed = dual.list_recent_runs(limit=5, command_filter="analyze.run")
    assert any(r["id"] == rid for r in listed)
