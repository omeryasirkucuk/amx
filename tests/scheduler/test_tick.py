"""Tests for the stateless ``tick`` engine.

These exercise the source-gated behaviour (bootstrap surfaces, daemon
fires, manual targets a specific id) against a real
:class:`SQLiteHistoryStore` populated via the phase-1a CRUD helpers.
The worker is mocked: tests record what ``spawn_worker`` saw and
assert on the report.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from amx.scheduler.tick import TickReport, tick
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _create_schedule(
    store: SQLiteHistoryStore,
    *,
    fire_at_utc: float,
    name: str = "s",
    db_profile: str = "prod_sf",
) -> int:
    return store.create_scheduled_run(
        name=name,
        fire_at_utc=fire_at_utc,
        fire_at_tz="UTC",
        db_profile=db_profile,
        scope_json=json.dumps({"mode": "schemas", "schemas": ["public"]}),
        llm_profile="claude",
        review_strategy="auto",
    )


def _recording_spawn(records: list[dict[str, Any]], next_id: int = 1):
    """Build a spawn_worker callable that records its arg and returns a fresh id."""
    counter = {"n": next_id}

    def spawn(payload: dict[str, Any]) -> int:
        records.append(payload)
        rid = counter["n"]
        counter["n"] += 1
        return rid

    return spawn


# ── Source gating ───────────────────────────────────────────────────


def test_bootstrap_surfaces_missed_without_firing(
    store: SQLiteHistoryStore,
) -> None:
    now = time.time()
    overdue = _create_schedule(store, fire_at_utc=now - 60, name="overdue")
    _create_schedule(store, fire_at_utc=now + 60, name="future")

    spawned: list[dict[str, Any]] = []
    report = tick(
        store=store,
        source="bootstrap",
        spawn_worker=_recording_spawn(spawned),
        now_utc=now,
    )

    assert report.missed_for_review == [overdue]
    assert report.fired == []
    assert spawned == []
    # The overdue schedule must stay pending — bootstrap never transitions.
    assert store.get_scheduled_run(overdue)["status"] == "pending"


def test_daemon_fires_due_schedules(store: SQLiteHistoryStore) -> None:
    now = time.time()
    s1 = _create_schedule(store, fire_at_utc=now - 120, name="oldest")
    s2 = _create_schedule(store, fire_at_utc=now - 60, name="newer")
    _create_schedule(store, fire_at_utc=now + 60, name="future")

    spawned: list[dict[str, Any]] = []
    report = tick(
        store=store,
        source="daemon",
        spawn_worker=_recording_spawn(spawned, next_id=100),
        now_utc=now,
    )

    # Both due rows fired; future stays pending.
    assert sorted(report.fired) == sorted([s1, s2])
    assert report.missed_for_review == []
    assert {p["id"] for p in spawned} == {s1, s2}
    for sid in (s1, s2):
        row = store.get_scheduled_run(sid)
        assert row["status"] == "running"
        assert row["fired_at"] is not None
        assert row["triggered_run_id"] in (100, 101)


def test_daemon_with_nothing_due_returns_empty_report(
    store: SQLiteHistoryStore,
) -> None:
    now = time.time()
    _create_schedule(store, fire_at_utc=now + 3600, name="future")
    report = tick(
        store=store,
        source="daemon",
        spawn_worker=_recording_spawn([]),
        now_utc=now,
    )
    assert report.fired == []
    assert report.missed_for_review == []


def test_manual_fires_target_id(store: SQLiteHistoryStore) -> None:
    now = time.time()
    sid = _create_schedule(store, fire_at_utc=now + 3600, name="future")
    spawned: list[dict[str, Any]] = []
    report = tick(
        store=store,
        source="manual",
        target_id=sid,
        spawn_worker=_recording_spawn(spawned, next_id=42),
        now_utc=now,
    )
    assert report.fired == [sid]
    row = store.get_scheduled_run(sid)
    assert row["status"] == "running"
    assert row["triggered_run_id"] == 42
    assert spawned == [pytest.approx(spawned[0], abs=0)] or spawned[0]["id"] == sid


def test_manual_without_target_id_raises(
    store: SQLiteHistoryStore,
) -> None:
    with pytest.raises(ValueError, match="requires target_id"):
        tick(store=store, source="manual", spawn_worker=lambda _: 1)


def test_manual_without_spawn_worker_raises(
    store: SQLiteHistoryStore,
) -> None:
    sid = _create_schedule(store, fire_at_utc=time.time() + 60)
    with pytest.raises(ValueError, match="spawn_worker"):
        tick(store=store, source="manual", target_id=sid)


def test_daemon_without_spawn_worker_raises(
    store: SQLiteHistoryStore,
) -> None:
    with pytest.raises(ValueError, match="spawn_worker"):
        tick(store=store, source="daemon")


def test_unknown_source_raises(store: SQLiteHistoryStore) -> None:
    with pytest.raises(ValueError, match="unknown tick source"):
        tick(store=store, source="frobnicate")  # type: ignore[arg-type]


# ── Worker failure handling ─────────────────────────────────────────


def test_worker_failure_marks_schedule_failed_and_records_error(
    store: SQLiteHistoryStore,
) -> None:
    now = time.time()
    sid = _create_schedule(store, fire_at_utc=now - 60, name="boom")

    def boom(_payload: dict[str, Any]) -> int:
        raise RuntimeError("simulated worker crash")

    report = tick(
        store=store,
        source="daemon",
        spawn_worker=boom,
        now_utc=now,
    )

    assert report.fired == []
    assert len(report.failed_resolution) == 1
    rid, msg = report.failed_resolution[0]
    assert rid == sid
    assert "simulated worker crash" in msg

    row = store.get_scheduled_run(sid)
    assert row["status"] == "failed"
    assert "simulated worker crash" in row["last_error"]


def test_manual_on_terminal_schedule_surfaces_state_machine_error(
    store: SQLiteHistoryStore,
) -> None:
    now = time.time()
    sid = _create_schedule(store, fire_at_utc=now - 60, name="done")
    # Manually drive the row to a terminal state.
    store.set_scheduled_run_status(sid, "running")
    store.set_scheduled_run_status(sid, "completed", triggered_run_id=1)

    spawned: list[dict[str, Any]] = []
    report = tick(
        store=store,
        source="manual",
        target_id=sid,
        spawn_worker=_recording_spawn(spawned),
        now_utc=now,
    )

    assert report.fired == []
    assert len(report.failed_resolution) == 1
    assert "illegal transition" in report.failed_resolution[0][1]
    assert spawned == []


# ── Concurrency ─────────────────────────────────────────────────────


def test_two_concurrent_daemon_ticks_dont_double_fire(
    store: SQLiteHistoryStore,
) -> None:
    """Two ticks racing on the same single due schedule -- one fires, one is empty."""
    now = time.time()
    sid = _create_schedule(store, fire_at_utc=now - 60, name="contested")

    reports: list[TickReport] = []
    rlock = threading.Lock()

    def run() -> None:
        spawned: list[dict[str, Any]] = []
        r = tick(
            store=store,
            source="daemon",
            spawn_worker=_recording_spawn(spawned, next_id=999),
            now_utc=now,
        )
        with rlock:
            reports.append(r)

    t1 = threading.Thread(target=run)
    t2 = threading.Thread(target=run)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    fired_lists = [r.fired for r in reports]
    fired_lists.sort(key=lambda lst: len(lst))
    assert fired_lists == [[], [sid]], (
        f"expected one tick to fire {sid} and the other to fire nothing; got {fired_lists}"
    )


# ── Report shape ────────────────────────────────────────────────────


def test_stale_recovered_field_exists_and_is_empty_until_wired(
    store: SQLiteHistoryStore,
) -> None:
    """The TickReport.stale_recovered list is part of the contract even
    before the heartbeat-based recovery wires in (deferred to a
    follow-up PR). Asserting on shape protects callers."""
    report = tick(
        store=store,
        source="bootstrap",
        spawn_worker=lambda _: 1,
        now_utc=time.time(),
    )
    assert report.stale_recovered == []
