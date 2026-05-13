"""Smoke test: SQLiteHistoryStore tolerates two concurrent run workers.

We don't try to exercise every write method; the goal is to verify the
hot path scheduled-run workers will hit (create_run + many
increment_run_processed + finish_run) does not deadlock or surface
``database is locked`` under the WAL+busy_timeout settings already in
place. If this ever regresses, that's a real signal — the scheduler
engine assumes parallel runs are honest.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

from amx.storage.sqlite_store import SQLiteHistoryStore


def test_two_workers_can_create_and_finish_runs_concurrently(
    tmp_path: Path,
) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.sqlite3")
    store.init()
    errors: list[BaseException] = []
    error_lock = threading.Lock()

    def worker(scope: dict, profile: str) -> None:
        try:
            run_id = store.create_run(
                command="run",
                mode="metadata",
                db_backend="snowflake",
                db_profile=profile,
                llm_provider="anthropic",
                llm_model="claude-sonnet",
                scope=scope,
            )
            for _ in range(20):
                store.increment_run_processed(run_id, by=1)
                time.sleep(0.005)
            store.finish_run(
                run_id,
                status="completed",
                metrics={},
                tokens={},
                results={},
            )
        except BaseException as exc:  # noqa: BLE001 - any failure is fatal here
            with error_lock:
                errors.append(exc)

    t1 = threading.Thread(target=worker, args=({"public": ["t1", "t2"]}, "prod_sf"))
    t2 = threading.Thread(target=worker, args=({"staging": ["t3", "t4"]}, "stg_sf"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert errors == [], f"concurrent writes failed: {errors!r}"
