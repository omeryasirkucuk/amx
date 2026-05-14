"""Regression test for the Overview ``success rate`` tile.

A run that finished the analysis pipeline without erroring should
count toward ``success_runs`` regardless of whether the user went on
to apply the suggestions. Earlier the SQL only matched literal
``status = 'success'`` rows, so a reviewer who never auto-applied
saw ``0%`` even on a perfectly healthy install.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


def _seed_run(store: SQLiteHistoryStore, *, status: str) -> int:
    run_id = store.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="sqlite",
        db_profile="test",
        llm_provider="openai",
        llm_model="gpt-4o-mini",
        scope={"public": ["t1"]},
        selected_count=1,
        planned_count=1,
    )
    store.update_run_status(run_id, status)
    return run_id


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(db_path=tmp_path / "history.db")
    s.init()
    return s


def test_success_rate_includes_review_and_partial_applied(
    store: SQLiteHistoryStore,
) -> None:
    _seed_run(store, status="success")
    _seed_run(store, status="ready_for_review")
    _seed_run(store, status="applied_partial")
    _seed_run(store, status="completed")
    _seed_run(store, status="failed")
    _seed_run(store, status="cancelled")

    stats = store.stats()

    assert stats["total_runs"] == 6
    assert stats["success_runs"] == 4
    assert stats["failed_runs"] == 1
    assert stats["ready_for_review_runs"] == 1
