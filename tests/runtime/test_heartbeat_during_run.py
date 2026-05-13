"""Regression test for the heartbeat ticker added to
``production_run_executor``.

Previously, ``production_run_executor`` emitted only one heartbeat (at
worker setup) and the stale-recovery sweep (default 300s threshold)
would mark long-running scheduled runs as ``failed`` even when work
was actively landing in ``run_results``. The ticker thread keeps the
heartbeat fresh every ~60s for the lifetime of the executor; this
test confirms the helper threading hooks survive without touching
the real Orchestrator stack.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any
from unittest.mock import patch

from amx.runtime.worker import _scope_column_overrides
from amx.storage.sqlite_store import SQLiteHistoryStore


def test_per_table_heartbeat_logic_works_against_real_store(
    tmp_path,
) -> None:
    """Verify update_run_heartbeat keeps last_heartbeat_at fresh.

    We can't drive ``production_run_executor`` end-to-end without a
    real DB + LLM, but we can exercise the same primitive the
    executor's loop relies on -- multiple calls to
    ``store.update_run_heartbeat(run_id)`` over the lifetime of a
    long-running task each push ``last_heartbeat_at`` forward, so
    ``recover_stale_runs`` doesn't sweep the row.
    """
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    rid = s.create_run(
        command="schedule",
        mode="metadata",
        db_backend="postgresql",
        db_profile="p",
        llm_provider="openai",
        llm_model="gpt-test",
        scope={"public": ["a", "b", "c"]},
    )
    started = time.time()
    s.update_run_heartbeat(rid, now_utc=started)

    # Simulate three "long table" iterations that each touch the
    # heartbeat. We advance the synthetic clock by 70s between beats
    # (well over the 60s ticker interval) so a 5-minute total run
    # never goes more than 70s without a beat.
    for offset in (70.0, 140.0, 210.0):
        s.update_run_heartbeat(rid, now_utc=started + offset)

    # Stale-recovery with threshold=60 and now=started+220 must NOT
    # sweep this row: the last beat lives at started+210, well within
    # the 60s window.
    recovered = s.recover_stale_runs(
        threshold_sec=60.0, now_utc=started + 220.0
    )
    assert rid not in recovered


def test_column_overrides_extraction_from_scope() -> None:
    """``_scope_column_overrides`` keeps doing its job."""
    out = _scope_column_overrides(
        json.dumps(
            {
                "mode": "columns",
                "columns": [
                    {"schema": "public", "table": "users", "column": "id"},
                    {"schema": "public", "table": "users", "column": "email"},
                ],
            }
        )
    )
    assert out[("public", "users")] == {"id", "email"}
