"""Persisted ``analysis_runs.current_step_label`` for cold-load progress.

The Studio run-detail page used to render "Waiting for the worker to
begin…" on every refresh of an in-flight run because the SSE stream
has no replay and no other surface remembers which phase the worker
was in. The new column captures the most recent phase label so a
fresh page-load can render real progress instead of the bare
placeholder.

The lifecycle:
  * ``create_run`` inserts the row with ``current_step_label = NULL``.
  * ``update_run_current_step`` rewrites the column as the worker
    walks through connect / LLM init / RAG / orchestrator / per-table.
  * ``finish_run`` clears it back to NULL so terminal rows don't
    advertise a stale phase to the persisted-runs view.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _make_run(store: SQLiteHistoryStore) -> int:
    return store.create_run(
        command="analyze.run",
        mode="chat",
        db_backend="postgresql",
        db_profile="p",
        llm_provider="openai",
        llm_model="gpt-test",
        scope={"public": ["orders"]},
    )


def _read_label(store: SQLiteHistoryStore, run_id: int) -> str | None:
    with sqlite3.connect(store.db_path) as conn:
        row = conn.execute(
            "SELECT current_step_label FROM analysis_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
    assert row is not None
    return row[0]


def test_create_run_leaves_current_step_label_null(store: SQLiteHistoryStore) -> None:
    """Fresh rows must start blank so the SPA's fallback message
    governs the "before the worker has announced anything" window."""
    rid = _make_run(store)
    assert _read_label(store, rid) is None


def test_update_run_current_step_round_trips(store: SQLiteHistoryStore) -> None:
    rid = _make_run(store)
    store.update_run_current_step(rid, "Connecting to local-postgre @ bird_train")
    assert _read_label(store, rid) == "Connecting to local-postgre @ bird_train"

    store.update_run_current_step(rid, "Initializing LLM minimax/minimax-m2.7")
    assert _read_label(store, rid) == "Initializing LLM minimax/minimax-m2.7"


def test_update_run_current_step_unknown_run_is_noop(store: SQLiteHistoryStore) -> None:
    """Defensive: web bridges may call before the row exists; never raise."""
    store.update_run_current_step(99999, "any label")


def test_finish_run_clears_current_step_label(store: SQLiteHistoryStore) -> None:
    """Terminal rows must not keep advertising the last in-flight
    phase — the run is over, persisted views show ``ready_for_review``
    et al., and the phase chip would be misleading."""
    rid = _make_run(store)
    store.update_run_current_step(rid, "Building orchestrator")
    assert _read_label(store, rid) == "Building orchestrator"

    store.finish_run(
        rid,
        status="ready_for_review",
        metrics={},
        tokens={},
        results={"pending_count": 0},
    )
    assert _read_label(store, rid) is None
