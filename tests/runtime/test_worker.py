"""Tests for the runtime worker scaffold (production spawn_worker).

The scaffold creates an analysis_runs row + links it to the schedule
and drives a pluggable executor. These tests exercise the contract;
swapping in the real Orchestrator executor is a follow-up.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

import pytest

from amx.runtime.worker import spawn_scheduled_worker
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _schedule(store: SQLiteHistoryStore, *, name: str = "s") -> dict[str, Any]:
    sid = store.create_scheduled_run(
        name=name,
        fire_at_utc=time.time() - 60,
        fire_at_tz="UTC",
        db_profile="prod_sf",
        scope_json=json.dumps({"mode": "schemas", "schemas": ["public"]}),
        llm_profile="claude",
        review_strategy="auto",
    )
    return store.get_scheduled_run(sid)


def test_worker_creates_run_links_and_completes(
    store: SQLiteHistoryStore,
) -> None:
    schedule = _schedule(store)
    sid = schedule["id"]

    run_id = spawn_scheduled_worker(
        schedule,
        store=store,
        background=False,  # sync so the test asserts on the final state
    )

    assert isinstance(run_id, int) and run_id > 0
    with sqlite3.connect(store.db_path) as conn:
        row = conn.execute(
            "SELECT status, triggered_by_schedule_id, last_heartbeat_at "
            "FROM analysis_runs WHERE id=?",
            (run_id,),
        ).fetchone()
    status, schedule_link, hb = row
    assert status == "completed"
    assert schedule_link == sid
    assert hb is not None  # initial heartbeat was emitted

    sched = store.get_scheduled_run(sid)
    assert sched["status"] == "completed"
    assert sched["triggered_run_id"] == run_id


def test_worker_marks_failed_when_executor_raises(
    store: SQLiteHistoryStore,
) -> None:
    schedule = _schedule(store, name="boom")
    sid = schedule["id"]

    def boom(_run_id: int, _payload: dict[str, Any]) -> None:
        raise RuntimeError("simulated executor failure")

    run_id = spawn_scheduled_worker(schedule, store=store, run_executor=boom, background=False)

    with sqlite3.connect(store.db_path) as conn:
        (status, err) = conn.execute(
            "SELECT status, error_text FROM analysis_runs WHERE id=?",
            (run_id,),
        ).fetchone()
    assert status == "failed"
    assert "simulated executor failure" in err

    sched = store.get_scheduled_run(sid)
    assert sched["status"] == "failed"
    assert "simulated executor failure" in sched["last_error"]
    assert sched["triggered_run_id"] == run_id


def test_worker_background_thread_does_not_block(
    store: SQLiteHistoryStore,
) -> None:
    schedule = _schedule(store, name="bg")

    started = time.monotonic()

    def slow(_run_id: int, _payload: dict[str, Any]) -> None:
        time.sleep(0.2)

    run_id = spawn_scheduled_worker(schedule, store=store, run_executor=slow, background=True)
    elapsed = time.monotonic() - started
    # The call returns before the slow executor finishes.
    assert elapsed < 0.15
    assert isinstance(run_id, int)
