"""Phase 2b: recover_stale_runs + set_run_schedule_link tests."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _make_run(store: SQLiteHistoryStore, *, db_profile: str = "p") -> int:
    return store.create_run(
        command="run",
        mode="metadata",
        db_backend="snowflake",
        db_profile=db_profile,
        llm_provider="anthropic",
        llm_model="claude",
        scope={"public": ["t"]},
    )


# ── set_run_schedule_link ────────────────────────────────────────────


def test_set_run_schedule_link_attaches_schedule_id(
    store: SQLiteHistoryStore,
) -> None:
    rid = _make_run(store)
    store.set_run_schedule_link(rid, schedule_id=42)
    with sqlite3.connect(store.db_path) as conn:
        (val,) = conn.execute(
            "SELECT triggered_by_schedule_id FROM analysis_runs WHERE id=?",
            (rid,),
        ).fetchone()
    assert val == 42


def test_set_run_schedule_link_unknown_run_is_noop(
    store: SQLiteHistoryStore,
) -> None:
    store.set_run_schedule_link(99999, schedule_id=1)


# ── recover_stale_runs ───────────────────────────────────────────────


def test_recover_marks_running_without_heartbeat_failed(
    store: SQLiteHistoryStore,
) -> None:
    rid = _make_run(store)
    # No heartbeat ever -> treated as stale on next recovery sweep.
    recovered = store.recover_stale_runs(threshold_sec=60, now_utc=time.time())
    assert recovered == [rid]
    with sqlite3.connect(store.db_path) as conn:
        (status, error_text, ended_at) = conn.execute(
            "SELECT status, error_text, ended_at FROM analysis_runs WHERE id=?",
            (rid,),
        ).fetchone()
    assert status == "failed"
    assert "heartbeat threshold" in error_text
    assert ended_at is not None


def test_recover_leaves_fresh_heartbeat_alone(
    store: SQLiteHistoryStore,
) -> None:
    rid = _make_run(store)
    store.update_run_heartbeat(rid)
    recovered = store.recover_stale_runs(threshold_sec=60, now_utc=time.time())
    assert recovered == []
    with sqlite3.connect(store.db_path) as conn:
        (status,) = conn.execute("SELECT status FROM analysis_runs WHERE id=?", (rid,)).fetchone()
    assert status == "running"


def test_recover_marks_old_heartbeat_failed(
    store: SQLiteHistoryStore,
) -> None:
    rid = _make_run(store)
    long_ago = time.time() - 3600
    store.update_run_heartbeat(rid, now_utc=long_ago)
    recovered = store.recover_stale_runs(threshold_sec=60, now_utc=time.time())
    assert recovered == [rid]


def test_recover_skips_completed_runs(store: SQLiteHistoryStore) -> None:
    rid = _make_run(store)
    store.finish_run(rid, status="completed", metrics={}, tokens={}, results={})
    recovered = store.recover_stale_runs(threshold_sec=60, now_utc=time.time())
    assert recovered == []


def test_recover_is_idempotent(store: SQLiteHistoryStore) -> None:
    rid = _make_run(store)
    first = store.recover_stale_runs(threshold_sec=60, now_utc=time.time())
    second = store.recover_stale_runs(threshold_sec=60, now_utc=time.time())
    assert first == [rid]
    assert second == []  # already marked failed
