"""``_announce_phase`` writes a Studio-visible progress signal on two
channels: the live SSE queue (so connected browser tabs see the label
within milliseconds) and ``analysis_runs.current_step_label`` (so a
page-refresh that arrives after the in-process replay buffer was lost
— typically because Studio restarted — still sees real progress
instead of "Waiting for the worker to begin…").

The web worker calls the helper at every meaningful boundary: scope
resolved, DB connector open, LLM provider built, history row created,
RAG store loaded, orchestrator constructed, per-table loop starting.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.jobs import Job
from amx.web.routers.runs import _announce_phase


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


def _step_updates_in_buffer(job: Job) -> list[str]:
    return [
        str(event.get("label", ""))
        for event in job.queue.buffer_snapshot()
        if event.get("type") == "step.update"
    ]


def test_announce_phase_emits_step_update_and_persists(store: SQLiteHistoryStore) -> None:
    """Both channels fire — SSE event for live tabs, column write for
    page refreshes that miss the live stream."""
    rid = _make_run(store)
    job = Job(id="t", kind="run")

    _announce_phase(job, store, rid, "Connecting to local-postgre @ bird_train")

    labels = _step_updates_in_buffer(job)
    assert labels == ["Connecting to local-postgre @ bird_train"]
    assert _read_label(store, rid) == "Connecting to local-postgre @ bird_train"


def test_announce_phase_last_writer_wins(store: SQLiteHistoryStore) -> None:
    """Later phases overwrite earlier ones; the column carries only the
    most recent phase. The SSE buffer accumulates the full sequence."""
    rid = _make_run(store)
    job = Job(id="t", kind="run")

    _announce_phase(job, store, rid, "Initializing LLM openai/gpt-test")
    _announce_phase(job, store, rid, "Loading docs RAG store")
    _announce_phase(job, store, rid, "Starting per-table processing")

    assert _step_updates_in_buffer(job) == [
        "Initializing LLM openai/gpt-test",
        "Loading docs RAG store",
        "Starting per-table processing",
    ]
    assert _read_label(store, rid) == "Starting per-table processing"


def test_announce_phase_without_history_store_still_emits(store: SQLiteHistoryStore) -> None:
    """Pre-history-store callers (history disabled, create_run failed)
    must still see SSE events — the persisted column is best-effort."""
    job = Job(id="t", kind="run")

    _announce_phase(job, None, None, "Connecting to local-postgre @ bird_train")

    assert _step_updates_in_buffer(job) == ["Connecting to local-postgre @ bird_train"]


def test_announce_phase_swallows_persistence_errors(store: SQLiteHistoryStore) -> None:
    """A history-store hiccup must not block the SSE channel — users
    seeing the live label is the load-bearing path."""

    class _ExplodingStore:
        def update_run_current_step(self, run_id: int, label: str) -> None:
            raise RuntimeError("simulated DB outage")

    job = Job(id="t", kind="run")
    _announce_phase(job, _ExplodingStore(), 1, "Recording run history")

    assert _step_updates_in_buffer(job) == ["Recording run history"]


def test_announce_phase_ignores_blank_labels(store: SQLiteHistoryStore) -> None:
    """Whitespace-only labels would clutter the buffer with empty
    chips; the helper short-circuits before either side effect."""
    rid = _make_run(store)
    job = Job(id="t", kind="run")

    _announce_phase(job, store, rid, "   ")

    assert _step_updates_in_buffer(job) == []
    assert _read_label(store, rid) is None
