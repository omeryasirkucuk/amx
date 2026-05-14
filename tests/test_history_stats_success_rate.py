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


def test_count_pending_review_runs_covers_ready_and_partial(
    store: SQLiteHistoryStore,
) -> None:
    # The Studio Landing chip should treat both "nothing applied yet"
    # and "some accepted, some still unreviewed" as work-awaiting-
    # review. ``success``, ``completed``, ``failed`` and ``cancelled``
    # carry no pending rows and must stay out of the count.
    _seed_run(store, status="ready_for_review")
    _seed_run(store, status="ready_for_review")
    _seed_run(store, status="applied_partial")
    _seed_run(store, status="success")
    _seed_run(store, status="completed")
    _seed_run(store, status="failed")
    _seed_run(store, status="cancelled")

    assert store.count_pending_review_runs() == 3
    # With the default ``analyze.run`` filter dropped the count is the
    # same here because every seeded row uses that command, but the
    # call must accept ``None`` without raising.
    assert store.count_pending_review_runs(command_filter=None) == 3
