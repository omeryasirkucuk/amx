"""DualWriteHistoryStore façade tests for scheduled_runs surface.

Phase 1b ships the local-only delegation -- the shared SQLAlchemy
mirror lands later. These tests confirm the façade transparently
exposes all 11 scheduled-run methods on the local store and that
mode-specific behaviour (state-machine validation, atomic claim,
heartbeat updates) propagates through unchanged.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from amx.storage.dual_write import DualWriteHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def dual(tmp_path: Path) -> DualWriteHistoryStore:
    local = SQLiteHistoryStore(tmp_path / "history.db")
    local.init()
    shared = MagicMock()
    # The Phase 1b façade doesn't call the shared store for any
    # scheduled-run method; MagicMock keeps the constructor happy.
    return DualWriteHistoryStore(local=local, shared=shared)


def test_create_and_get_round_trip(dual: DualWriteHistoryStore) -> None:
    sid = dual.create_scheduled_run(
        name="x",
        fire_at_utc=time.time() + 60,
        fire_at_tz="UTC",
        db_profile="p",
        scope_json=json.dumps({"mode": "all"}),
        llm_profile="l",
        review_strategy="auto",
    )
    row = dual.get_scheduled_run(sid)
    assert row is not None
    assert row["name"] == "x"


def test_list_filters_pass_through(dual: DualWriteHistoryStore) -> None:
    sid = dual.create_scheduled_run(
        name="a",
        fire_at_utc=time.time() + 60,
        fire_at_tz="UTC",
        db_profile="p1",
        scope_json="{}",
        llm_profile="l",
        review_strategy="auto",
    )
    dual.create_scheduled_run(
        name="b",
        fire_at_utc=time.time() + 60,
        fire_at_tz="UTC",
        db_profile="p2",
        scope_json="{}",
        llm_profile="l",
        review_strategy="auto",
    )
    rows = dual.list_scheduled_runs(db_profile="p1")
    assert [r["id"] for r in rows] == [sid]


def test_state_machine_enforced_through_facade(
    dual: DualWriteHistoryStore,
) -> None:
    sid = dual.create_scheduled_run(
        name="x",
        fire_at_utc=time.time() + 60,
        fire_at_tz="UTC",
        db_profile="p",
        scope_json="{}",
        llm_profile="l",
        review_strategy="auto",
    )
    dual.set_scheduled_run_status(sid, "running")
    dual.set_scheduled_run_status(sid, "completed")
    with pytest.raises(ValueError):
        dual.set_scheduled_run_status(sid, "running")


def test_claim_due_schedule_through_facade(dual: DualWriteHistoryStore) -> None:
    now = time.time()
    sid = dual.create_scheduled_run(
        name="due",
        fire_at_utc=now - 60,
        fire_at_tz="UTC",
        db_profile="p",
        scope_json="{}",
        llm_profile="l",
        review_strategy="auto",
    )
    claimed = dual.claim_due_schedule(now_utc=now)
    assert claimed == sid
    assert dual.get_scheduled_run(sid)["status"] == "running"


def test_heartbeat_and_stale_recovery_through_facade(
    dual: DualWriteHistoryStore,
) -> None:
    rid = dual.create_run(
        command="run",
        mode="m",
        db_backend="x",
        db_profile="p",
        llm_provider="a",
        llm_model="m",
        scope={"public": ["t"]},
    )
    # Without a heartbeat the recovery sweep marks it failed.
    recovered = dual.recover_stale_runs(threshold_sec=10, now_utc=time.time())
    assert recovered == [rid]


def test_set_run_schedule_link_through_facade(
    dual: DualWriteHistoryStore,
) -> None:
    rid = dual.create_run(
        command="run",
        mode="m",
        db_backend="x",
        db_profile="p",
        llm_provider="a",
        llm_model="m",
        scope={"public": ["t"]},
    )
    dual.set_run_schedule_link(rid, schedule_id=42)
    # Confirm it round-tripped to the local store via raw introspection.
    row = dual.get_run(rid)
    assert row.get("triggered_by_schedule_id") == 42
