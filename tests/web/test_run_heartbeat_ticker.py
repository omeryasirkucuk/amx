"""Studio-driven web runs must beat ``analysis_runs.last_heartbeat_at``.

Background: ``_run_worker_body`` previously left ``last_heartbeat_at``
NULL for the lifetime of a Studio run. The scheduler loop's
``recover_stale_runs`` sweep treats a NULL heartbeat as immediately
stale (see ``amx/storage/sqlite_store.py``), so on the first tick (~60s
after Studio boot) any in-flight Studio run was flipped to ``failed``
while its worker thread was still alive — the run later flipped back
to ``ready_for_review`` once the worker called ``finish_run``. Users
saw a brief "failed" badge that disappeared after 5-10 seconds.

The helper exercised here keeps ``last_heartbeat_at`` fresh so the
sweep skips the row. Wiring into ``_run_worker_body`` is covered by
the existing integration suites.
"""

from __future__ import annotations

import time

from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.routers.runs import _start_heartbeat_ticker


def _fresh_run(store: SQLiteHistoryStore) -> int:
    return store.create_run(
        command="analyze.run",
        mode="chat",
        db_backend="postgresql",
        db_profile="p",
        llm_provider="openai",
        llm_model="gpt-test",
        scope={"public": ["orders"]},
    )


def test_start_heartbeat_ticker_writes_first_beat_immediately(tmp_path) -> None:
    """Closes the NULL window that lets ``recover_stale_runs`` sweep
    a brand-new Studio run before its worker has done anything."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    run_id = _fresh_run(store)

    stop, thread = _start_heartbeat_ticker(store, run_id, interval_sec=60.0)
    try:
        with store._connect() as conn:
            row = conn.execute(
                "SELECT last_heartbeat_at FROM analysis_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
        assert row is not None
        assert row[0] is not None, "first heartbeat must land before the ticker returns"
    finally:
        stop.set()
        thread.join(timeout=2.0)
    assert not thread.is_alive()


def test_start_heartbeat_ticker_keeps_beating_until_stop(tmp_path) -> None:
    """The looped beat must push ``last_heartbeat_at`` forward so a
    long Studio run doesn't get swept after its first 60s window."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    run_id = _fresh_run(store)

    stop, thread = _start_heartbeat_ticker(store, run_id, interval_sec=0.05)
    try:
        time.sleep(0.2)
        with store._connect() as conn:
            first = conn.execute(
                "SELECT last_heartbeat_at FROM analysis_runs WHERE id = ?",
                (run_id,),
            ).fetchone()[0]
        time.sleep(0.2)
        with store._connect() as conn:
            second = conn.execute(
                "SELECT last_heartbeat_at FROM analysis_runs WHERE id = ?",
                (run_id,),
            ).fetchone()[0]
    finally:
        stop.set()
        thread.join(timeout=2.0)

    assert second is not None
    assert first is not None
    assert second > first, "ticker must keep bumping last_heartbeat_at while alive"


def test_start_heartbeat_ticker_survives_store_failure(tmp_path) -> None:
    """A transient store error must not crash the ticker thread —
    otherwise one bad write would silently re-introduce the NULL
    heartbeat window the sweep punishes."""

    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    run_id = _fresh_run(store)

    calls: list[int] = []

    def flaky(self_run_id: int, *, now_utc: float | None = None) -> None:  # noqa: ARG001
        calls.append(self_run_id)
        if len(calls) == 1:
            raise RuntimeError("simulated transient")
        store.__class__.update_run_heartbeat(store, self_run_id, now_utc=now_utc)

    class _FlakyStore:
        def update_run_heartbeat(self, rid: int, *, now_utc: float | None = None) -> None:
            flaky(rid, now_utc=now_utc)

    stop, thread = _start_heartbeat_ticker(_FlakyStore(), run_id, interval_sec=0.05)
    try:
        time.sleep(0.25)
    finally:
        stop.set()
        thread.join(timeout=2.0)

    assert len(calls) >= 2, "ticker should have retried after the first beat raised"
    assert not thread.is_alive()
