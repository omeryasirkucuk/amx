"""Local-SQLite storage tests for the ``scheduled_runs`` table.

Phase 1a covers the local source-of-truth side only. The shared
SQLAlchemy mirror and the dual-write façade are added in Phase 1b.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _make(
    store: SQLiteHistoryStore,
    *,
    name: str = "sched",
    fire_at_utc: float | None = None,
    fire_at_tz: str = "Europe/Istanbul",
    db_profile: str = "prod_sf",
    scope: dict | None = None,
    llm_profile: str = "claude",
    review_strategy: str = "auto",
) -> int:
    return store.create_scheduled_run(
        name=name,
        fire_at_utc=fire_at_utc if fire_at_utc is not None else time.time() + 3600,
        fire_at_tz=fire_at_tz,
        db_profile=db_profile,
        scope_json=json.dumps(scope or {"mode": "schemas", "schemas": ["public"]}),
        llm_profile=llm_profile,
        review_strategy=review_strategy,
    )


# ── Schema ───────────────────────────────────────────────────────────


def test_schema_contains_scheduled_runs_table(store: SQLiteHistoryStore) -> None:
    with sqlite3.connect(store.db_path) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {row[0] for row in rows}
    assert "scheduled_runs" in names


def test_scheduled_runs_has_expected_columns(store: SQLiteHistoryStore) -> None:
    with sqlite3.connect(store.db_path) as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(scheduled_runs)").fetchall()}
    expected = {
        "id",
        "name",
        "fire_at_utc",
        "fire_at_tz",
        "status",
        "db_profile",
        "scope_json",
        "llm_profile",
        "review_strategy",
        "extra_args_json",
        "created_at",
        "updated_at",
        "fired_at",
        "triggered_run_id",
        "last_error",
    }
    missing = expected - cols
    assert not missing, f"missing columns: {missing}"


def test_analysis_runs_has_scheduler_columns(store: SQLiteHistoryStore) -> None:
    with sqlite3.connect(store.db_path) as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(analysis_runs)").fetchall()}
    assert "triggered_by_schedule_id" in cols
    assert "last_heartbeat_at" in cols


# ── Create + read ────────────────────────────────────────────────────


def test_create_then_get_round_trip(store: SQLiteHistoryStore) -> None:
    sid = store.create_scheduled_run(
        name="quarterly refresh",
        fire_at_utc=1_700_000_000.0,
        fire_at_tz="Europe/Istanbul",
        db_profile="prod_sf",
        scope_json='{"mode":"schemas","schemas":["public"]}',
        llm_profile="claude",
        review_strategy="auto",
    )
    assert isinstance(sid, int) and sid > 0
    row = store.get_scheduled_run(sid)
    assert row is not None
    assert row["name"] == "quarterly refresh"
    assert row["status"] == "pending"
    assert row["fire_at_utc"] == pytest.approx(1_700_000_000.0)
    assert row["fire_at_tz"] == "Europe/Istanbul"
    assert row["db_profile"] == "prod_sf"
    assert row["created_at"] > 0
    assert row["updated_at"] > 0
    assert row["fired_at"] is None
    assert row["triggered_run_id"] is None
    assert row["last_error"] is None


def test_get_nonexistent_returns_none(store: SQLiteHistoryStore) -> None:
    assert store.get_scheduled_run(99999) is None


# ── List + filters ──────────────────────────────────────────────────


def test_list_filters_by_status(store: SQLiteHistoryStore) -> None:
    pending = _make(store, name="p")
    paused = _make(store, name="x")
    store.set_scheduled_run_status(paused, "paused")

    pendings = store.list_scheduled_runs(statuses=["pending"])
    assert {r["id"] for r in pendings} == {pending}

    pauseds = store.list_scheduled_runs(statuses=["paused"])
    assert {r["id"] for r in pauseds} == {paused}


def test_list_filters_by_db_profile(store: SQLiteHistoryStore) -> None:
    a = _make(store, db_profile="prod_sf")
    _make(store, db_profile="staging")
    rows = store.list_scheduled_runs(db_profile="prod_sf")
    assert {r["id"] for r in rows} == {a}


def test_list_default_sorts_by_fire_at_utc_ascending(
    store: SQLiteHistoryStore,
) -> None:
    later = _make(store, name="later", fire_at_utc=2_000_000_000.0)
    earlier = _make(store, name="earlier", fire_at_utc=1_000_000_000.0)
    ids = [r["id"] for r in store.list_scheduled_runs()]
    assert ids == [earlier, later]


def test_list_honours_limit(store: SQLiteHistoryStore) -> None:
    for i in range(5):
        _make(store, name=f"s{i}", fire_at_utc=1_000_000_000.0 + i)
    rows = store.list_scheduled_runs(limit=2)
    assert len(rows) == 2


def test_list_due_pending_returns_only_due_pending(
    store: SQLiteHistoryStore,
) -> None:
    now = time.time()
    due_pending = _make(store, name="due", fire_at_utc=now - 60)
    _make(store, name="future", fire_at_utc=now + 60)  # future, pending
    paused_due = _make(store, name="paused-due", fire_at_utc=now - 30)
    store.set_scheduled_run_status(paused_due, "paused")

    rows = store.list_due_pending_schedules(now_utc=now)
    assert {r["id"] for r in rows} == {due_pending}


# ── update_scheduled_run ────────────────────────────────────────────


def test_update_changes_allowed_fields(store: SQLiteHistoryStore) -> None:
    sid = _make(store, name="old")
    store.update_scheduled_run(sid, patch={"name": "new", "fire_at_tz": "UTC"})
    row = store.get_scheduled_run(sid)
    assert row["name"] == "new"
    assert row["fire_at_tz"] == "UTC"
    assert row["updated_at"] >= row["created_at"]


def test_update_rejects_forbidden_field(store: SQLiteHistoryStore) -> None:
    sid = _make(store)
    with pytest.raises(ValueError, match="forbidden"):
        store.update_scheduled_run(sid, patch={"status": "running"})


def test_update_rejects_unknown_field(store: SQLiteHistoryStore) -> None:
    sid = _make(store)
    with pytest.raises(ValueError, match="unknown"):
        store.update_scheduled_run(sid, patch={"nonsense": 1})


# ── set_scheduled_run_status ────────────────────────────────────────


def test_status_transition_pending_to_paused(
    store: SQLiteHistoryStore,
) -> None:
    sid = _make(store)
    store.set_scheduled_run_status(sid, "paused")
    assert store.get_scheduled_run(sid)["status"] == "paused"


def test_status_transition_pending_to_running(
    store: SQLiteHistoryStore,
) -> None:
    sid = _make(store)
    store.set_scheduled_run_status(sid, "running")
    assert store.get_scheduled_run(sid)["status"] == "running"


def test_status_transition_running_to_completed_records_run_link(
    store: SQLiteHistoryStore,
) -> None:
    sid = _make(store)
    store.set_scheduled_run_status(sid, "running")
    store.set_scheduled_run_status(sid, "completed", triggered_run_id=42)
    row = store.get_scheduled_run(sid)
    assert row["status"] == "completed"
    assert row["triggered_run_id"] == 42


def test_status_transition_running_to_failed_records_error(
    store: SQLiteHistoryStore,
) -> None:
    sid = _make(store)
    store.set_scheduled_run_status(sid, "running")
    store.set_scheduled_run_status(
        sid, "failed", last_error="scope resolution: schema 'public' missing"
    )
    row = store.get_scheduled_run(sid)
    assert row["status"] == "failed"
    assert "public" in row["last_error"]


def test_illegal_transition_raises(store: SQLiteHistoryStore) -> None:
    sid = _make(store)
    store.set_scheduled_run_status(sid, "running")
    store.set_scheduled_run_status(sid, "completed")
    with pytest.raises(ValueError, match="illegal transition"):
        store.set_scheduled_run_status(sid, "running")


def test_transition_to_unknown_status_raises(
    store: SQLiteHistoryStore,
) -> None:
    sid = _make(store)
    with pytest.raises(ValueError, match="unknown status"):
        store.set_scheduled_run_status(sid, "frobnicated")


# ── delete_scheduled_run ────────────────────────────────────────────


def test_delete_removes_row_and_writes_audit_event(
    store: SQLiteHistoryStore,
) -> None:
    sid = _make(store, name="goodbye")
    store.delete_scheduled_run(sid)
    assert store.get_scheduled_run(sid) is None

    with sqlite3.connect(store.db_path) as conn:
        rows = conn.execute(
            "SELECT event_type, details_json FROM app_events WHERE event_type = 'schedule.deleted'"
        ).fetchall()
    assert len(rows) == 1
    payload = json.loads(rows[0][1])
    assert payload["schedule_id"] == sid
    assert payload["name"] == "goodbye"


def test_delete_unknown_id_is_a_noop(store: SQLiteHistoryStore) -> None:
    # No exception; nothing in audit.
    store.delete_scheduled_run(99999)


# ── claim_due_schedule ──────────────────────────────────────────────


def test_claim_returns_oldest_pending_due(store: SQLiteHistoryStore) -> None:
    now = time.time()
    older = _make(store, name="older", fire_at_utc=now - 60)
    newer = _make(store, name="newer", fire_at_utc=now - 1)
    claimed = store.claim_due_schedule(now_utc=now)
    assert claimed == older
    assert store.get_scheduled_run(older)["status"] == "running"
    assert store.get_scheduled_run(newer)["status"] == "pending"


def test_claim_returns_none_when_nothing_due(
    store: SQLiteHistoryStore,
) -> None:
    now = time.time()
    _make(store, name="future", fire_at_utc=now + 3600)
    assert store.claim_due_schedule(now_utc=now) is None


def test_claim_skips_paused_schedules(store: SQLiteHistoryStore) -> None:
    now = time.time()
    sid = _make(store, name="x", fire_at_utc=now - 60)
    store.set_scheduled_run_status(sid, "paused")
    assert store.claim_due_schedule(now_utc=now) is None


def test_two_concurrent_claims_dont_double_fire(
    store: SQLiteHistoryStore,
) -> None:
    """Race-safety: two threads racing claim on one due row → exactly one wins."""
    now = time.time()
    sid = _make(store, name="contested", fire_at_utc=now - 1)

    results: list[int | None] = []
    rlock = threading.Lock()

    def claim() -> None:
        r = store.claim_due_schedule(now_utc=now)
        with rlock:
            results.append(r)

    t1 = threading.Thread(target=claim)
    t2 = threading.Thread(target=claim)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    won = [r for r in results if r is not None]
    lost = [r for r in results if r is None]
    assert won == [sid]
    assert lost == [None]


# ── update_run_heartbeat ────────────────────────────────────────────


def test_update_run_heartbeat_writes_column(
    store: SQLiteHistoryStore,
) -> None:
    run_id = store.create_run(
        command="run",
        mode="metadata",
        db_backend="snowflake",
        db_profile="prod_sf",
        llm_provider="anthropic",
        llm_model="claude-sonnet",
        scope={"public": ["t"]},
    )
    before = time.time()
    store.update_run_heartbeat(run_id)
    with sqlite3.connect(store.db_path) as conn:
        (hb,) = conn.execute(
            "SELECT last_heartbeat_at FROM analysis_runs WHERE id=?", (run_id,)
        ).fetchone()
    assert hb is not None
    assert hb >= before - 1.0


def test_update_run_heartbeat_unknown_id_is_a_noop(
    store: SQLiteHistoryStore,
) -> None:
    store.update_run_heartbeat(99999)  # must not raise
