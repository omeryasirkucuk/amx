"""Change-triggered schedule dispatch + storage contracts.

These pin the behaviour the user asked for: a schedule with no fire time
that auto-runs when a new asset appears under its watched scope, fires
narrowly for just the new tables, advances a watermark so the same assets
never re-fire, and stays out of the time-based claim loop.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from amx.scheduler.change_trigger import _evaluate_one
from amx.search.catalog import SearchCatalog
from amx.storage import sqlite_store as ss
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def store_and_catalog(tmp_path: Path) -> tuple[SQLiteHistoryStore, SearchCatalog]:
    db_path = tmp_path / "history.db"
    store = SQLiteHistoryStore(db_path)
    store.init()
    ss._store = store  # noqa: SLF001
    cat = SearchCatalog(db_path)
    yield store, cat
    ss._store = None  # noqa: SLF001


def _insert_entity(
    cat: SearchCatalog,
    *,
    profile: str,
    schema: str,
    table: str,
    column: str | None,
    kind: str,
    first_synced_at: float,
) -> None:
    """Insert a catalog_entities row with an explicit first_synced_at so
    tests control what counts as "new since the watermark"."""
    with cat._connect() as conn:  # noqa: SLF001
        conn.execute(
            """
            INSERT INTO catalog_entities (
                db_profile, db_backend, database_name, schema_name, table_name,
                column_name, entity_kind, asset_kind, updated_at, last_synced_at,
                first_synced_at
            ) VALUES (?, '', '', ?, ?, ?, ?, 'table', ?, ?, ?)
            """,
            (profile, schema, table, column, kind, first_synced_at, first_synced_at, first_synced_at),
        )


def _make_change_schedule(store: SQLiteHistoryStore, *, profile: str, watermark: float) -> int:
    sid = store.create_scheduled_run(
        name="watch public",
        fire_at_utc=time.time(),  # placeholder; never used for change
        fire_at_tz="UTC",
        db_profile=profile,
        scope_json=json.dumps({"mode": "schemas", "schemas": ["public"], "deep_first": True}),
        llm_profile="default",
        review_strategy="manual",
        kind="analyze",
        trigger="change",
    )
    # Pin the watermark deterministically. create_scheduled_run seeds it to
    # "now"; we set it directly here (advance_change_watermark is
    # forward-only and would refuse to move it back for the test).
    with store._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE scheduled_runs SET last_checked_at = ? WHERE id = ?",
            (watermark, sid),
        )
    return sid


# ── time-claim exclusion ───────────────────────────────────────────────


def test_claim_due_schedule_skips_change_schedules(
    store_and_catalog: tuple[SQLiteHistoryStore, SearchCatalog],
) -> None:
    store, _ = store_and_catalog
    # A change schedule whose placeholder fire_at is in the past must NOT
    # be claimed by the time loop.
    store.create_scheduled_run(
        name="watcher",
        fire_at_utc=time.time() - 3600,
        fire_at_tz="UTC",
        db_profile="prof-a",
        scope_json=json.dumps({"mode": "all"}),
        llm_profile="default",
        review_strategy="manual",
        trigger="change",
    )
    assert store.claim_due_schedule(now_utc=time.time()) is None

    # A normal time schedule with an elapsed fire_at IS claimed.
    tid = store.create_scheduled_run(
        name="timed",
        fire_at_utc=time.time() - 60,
        fire_at_tz="UTC",
        db_profile="prof-a",
        scope_json=json.dumps({"mode": "all"}),
        llm_profile="default",
        review_strategy="auto",
        trigger="time",
    )
    assert store.claim_due_schedule(now_utc=time.time()) == tid


# ── rearm ──────────────────────────────────────────────────────────────


def test_rearm_change_schedule_returns_to_pending(
    store_and_catalog: tuple[SQLiteHistoryStore, SearchCatalog],
) -> None:
    store, _ = store_and_catalog
    sid = _make_change_schedule(store, profile="prof-a", watermark=time.time())
    store.set_scheduled_run_status(sid, "running")
    assert store.rearm_change_schedule(sid) is True
    assert store.get_scheduled_run(sid)["status"] == "pending"


def test_rearm_change_schedule_false_for_time_schedule(
    store_and_catalog: tuple[SQLiteHistoryStore, SearchCatalog],
) -> None:
    store, _ = store_and_catalog
    tid = store.create_scheduled_run(
        name="timed",
        fire_at_utc=time.time(),
        fire_at_tz="UTC",
        db_profile="prof-a",
        scope_json=json.dumps({"mode": "all"}),
        llm_profile="default",
        review_strategy="auto",
        trigger="time",
    )
    assert store.rearm_change_schedule(tid) is False


# ── new_entities_since ──────────────────────────────────────────────────


def test_new_entities_since_respects_watermark_and_scope(
    store_and_catalog: tuple[SQLiteHistoryStore, SearchCatalog],
) -> None:
    store, cat = store_and_catalog
    t0 = time.time()
    _insert_entity(cat, profile="p", schema="public", table="old", column=None, kind="table", first_synced_at=t0 - 100)
    _insert_entity(cat, profile="p", schema="public", table="new", column=None, kind="table", first_synced_at=t0 + 100)
    _insert_entity(cat, profile="p", schema="other", table="elsewhere", column=None, kind="table", first_synced_at=t0 + 100)

    rows = cat.new_entities_since("p", t0, schemas=["public"])
    names = {r["table_name"] for r in rows}
    assert names == {"new"}  # old is before watermark; elsewhere is out of scope


# ── dispatcher: fire, narrow, advance, no-refire ────────────────────────


def test_evaluate_one_fires_narrowed_run_and_advances_watermark(
    store_and_catalog: tuple[SQLiteHistoryStore, SearchCatalog],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, cat = store_and_catalog
    t0 = time.time()
    # Watermark in the recent past; assets are stamped at insert time so
    # their first_synced_at is always <= now (never in the future).
    sid = _make_change_schedule(store, profile="p", watermark=t0 - 10)
    # One pre-existing table (before watermark) + one that appeared after.
    _insert_entity(cat, profile="p", schema="public", table="users", column=None, kind="table", first_synced_at=t0 - 50)
    _insert_entity(cat, profile="p", schema="public", table="orders", column=None, kind="table", first_synced_at=t0 - 1)

    captured: dict[str, Any] = {}

    import amx.runtime.worker as worker

    def _fake_spawn(payload: dict[str, Any], **_kw: Any) -> int:
        captured["payload"] = payload
        return 999

    monkeypatch.setattr(worker, "spawn_scheduled_worker", _fake_spawn)

    sched = store.get_scheduled_run(sid)
    fired = _evaluate_one(store, cat, sched, databases=None)

    assert fired == 1
    scope = json.loads(captured["payload"]["scope_json"])
    assert scope["mode"] == "tables"
    assert scope["missing_only"] is True
    assert [t["table"] for t in scope["tables"]] == ["orders"]  # only the new table

    # Watermark advanced — a second evaluation with no further changes
    # must not fire again.
    captured.clear()
    sched2 = store.get_scheduled_run(sid)
    fired2 = _evaluate_one(store, cat, sched2, databases=None)
    assert fired2 == 0
    assert "payload" not in captured


def test_evaluate_one_no_new_assets_does_not_fire(
    store_and_catalog: tuple[SQLiteHistoryStore, SearchCatalog],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, cat = store_and_catalog
    t0 = time.time()
    sid = _make_change_schedule(store, profile="p", watermark=t0)
    _insert_entity(cat, profile="p", schema="public", table="users", column=None, kind="table", first_synced_at=t0 - 50)

    import amx.runtime.worker as worker

    def _boom(*_a: Any, **_k: Any) -> int:
        raise AssertionError("must not fire when nothing is new")

    monkeypatch.setattr(worker, "spawn_scheduled_worker", _boom)
    assert _evaluate_one(store, cat, store.get_scheduled_run(sid), databases=None) == 0


# ── catalog overlay (Databricks) + per-table deep-sync dispatch ─────────


def test_evaluate_one_matches_catalog_overlay(
    store_and_catalog: tuple[SQLiteHistoryStore, SearchCatalog],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On three-level backends the watcher stores its container in
    ``catalog`` (not ``database``); the dispatcher must match on it and
    not skip the schedule when the synced container list carries it."""
    store, cat = store_and_catalog
    t0 = time.time()
    sid = store.create_scheduled_run(
        name="watch catalog",
        fire_at_utc=t0,
        fire_at_tz="UTC",
        db_profile="dbr",
        catalog="amx_test",
        scope_json=json.dumps({"mode": "all", "deep_first": True}),
        llm_profile="default",
        review_strategy="manual",
        trigger="change",
    )
    with store._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE scheduled_runs SET last_checked_at = ? WHERE id = ?", (t0 - 10, sid)
        )
    _insert_entity(
        cat, profile="dbr", schema="amx_test_schema", table="adr_6",
        column="test2", kind="column", first_synced_at=t0 - 1,
    )
    # The new column's database_name must be the catalog for the filter to
    # match — mirror how deep_sync writes Databricks rows.
    with cat._connect() as conn:  # noqa: SLF001
        conn.execute("UPDATE catalog_entities SET database_name = 'amx_test'")

    import amx.runtime.worker as worker

    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        worker, "spawn_scheduled_worker", lambda payload, **_k: captured.update(payload=payload) or 1
    )
    sched = store.get_scheduled_run(sid)
    fired = _evaluate_one(store, cat, sched, databases=["amx_test"])
    assert fired == 1
    scope = json.loads(captured["payload"]["scope_json"])
    assert [t["table"] for t in scope["tables"]] == ["adr_6"]


def test_deep_sync_one_table_dispatches_change_schedules(
    store_and_catalog: tuple[SQLiteHistoryStore, SearchCatalog],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The per-table 'Deep sync' button (deep_sync_one_table) must invoke
    the change-trigger dispatcher — without it a Watching schedule never
    reacts to a newly-added column discovered by a single-table deep
    sync."""
    import amx.search.drift as drift
    from amx.db.connector import AssetKind, ColumnProfile, TableProfile

    class _StubConn:
        def profile_table(self, schema: str, table: str, sample_size: int = 0) -> TableProfile:
            return TableProfile(
                schema=schema,
                name=table,
                asset_kind=AssetKind.TABLE,
                columns=[ColumnProfile(name="test2", dtype="STRING", nullable=True)],
                row_count=5,
            )

    monkeypatch.setattr(drift, "_scoped_connector", lambda *a, **k: _StubConn())
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(
        drift,
        "_dispatch_change_schedules",
        lambda profile, cfg, *, databases=None: recorded.update(
            profile=profile, databases=databases
        ),
    )
    cfg = SimpleNamespace(db_profiles={"dbr": SimpleNamespace(backend="databricks")}, db=None)

    out = drift.deep_sync_one_table(cfg, "dbr", schema="amx_test_schema", table="adr_6", database="amx_test")
    assert out["ok"] is True
    assert recorded.get("profile") == "dbr"
    assert recorded.get("databases") == ["amx_test"]
