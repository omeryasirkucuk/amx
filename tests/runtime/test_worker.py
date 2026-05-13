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
    # The default no-op executor writes no results and applies nothing,
    # so the run rolls up to "success" (no pending → no demotion).
    assert status == "success"
    assert schedule_link == sid
    assert hb is not None  # initial heartbeat was emitted

    sched = store.get_scheduled_run(sid)
    # Schedule lifecycle stays "completed" — that column tracks the
    # schedule, not the run outcome.
    assert sched["status"] == "completed"
    assert sched["triggered_run_id"] == run_id


def test_worker_demotes_to_ready_for_review_when_pending_results(
    store: SQLiteHistoryStore,
) -> None:
    """Manual review path: results saved, none applied → ready_for_review."""
    schedule = _schedule(store, name="manual")

    def manual_review(run_id: int, _payload: dict[str, Any]) -> None:
        # Mimic a manual-review scheduled run: the executor saves
        # alternatives into run_results but doesn't apply anything.
        store.save_run_results(
            run_id,
            [
                {
                    "schema": "public",
                    "table": "users",
                    "column": "id",
                    "asset_kind": "column",
                    "source": "llm",
                    "confidence": "high",
                    "reasoning": "primary key",
                    "alternatives": ["user_id"],
                }
            ],
        )

    run_id = spawn_scheduled_worker(
        schedule, store=store, run_executor=manual_review, background=False
    )

    with sqlite3.connect(store.db_path) as conn:
        (status,) = conn.execute(
            "SELECT status FROM analysis_runs WHERE id=?",
            (run_id,),
        ).fetchone()
    assert status == "ready_for_review"

    sched = store.get_scheduled_run(schedule["id"])
    assert sched["status"] == "completed"


def test_worker_stays_success_when_results_applied(
    store: SQLiteHistoryStore,
) -> None:
    """Auto-apply path: applied_count > 0 → status stays success."""
    schedule = _schedule(store, name="auto")

    def auto_apply(run_id: int, _payload: dict[str, Any]) -> None:
        # Mimic an auto-apply scheduled run: results were applied
        # straight to the catalog without queueing for review.
        store.increment_run_applied(run_id, by=3)

    run_id = spawn_scheduled_worker(
        schedule, store=store, run_executor=auto_apply, background=False
    )

    with sqlite3.connect(store.db_path) as conn:
        (status,) = conn.execute(
            "SELECT status FROM analysis_runs WHERE id=?",
            (run_id,),
        ).fetchone()
    assert status == "success"

    sched = store.get_scheduled_run(schedule["id"])
    assert sched["status"] == "completed"


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
