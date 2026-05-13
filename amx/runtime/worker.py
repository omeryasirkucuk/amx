"""Production worker entry for scheduled runs.

The scheduler engine (``amx.scheduler.tick``) accepts an injectable
``spawn_worker`` callable. This module provides the default
implementation that integrates with the local history store.

The full Orchestrator integration -- building the LLM client,
resolving the live DB profile, looping every table in the resolved
scope, persisting alternatives -- is a separate large concern that
benefits from per-table heartbeat plumbing inside the orchestrator
itself. For the initial cut shipped here, ``spawn_scheduled_worker``
performs the steps the rest of the pipeline depends on:

* Create an ``analysis_runs`` row with ``command='schedule'`` and
  attach the originating ``scheduled_runs`` id via
  ``set_run_schedule_link``. Status starts as ``running``;
  ``last_heartbeat_at`` is set to the current time so the
  stale-recovery sweep does not immediately reclaim it.
* Hand control to the (pluggable) per-run executor and update the
  history row + schedule status on completion.
* On exception, mark both rows as failed with the error message so
  the user sees a clean failure in ``amx schedule list`` / Studio.

The pluggable executor (``run_orchestrator_impl``) defaults to a
small no-op stub that simply marks the run as complete -- enough for
the CLI ``schedule run-now`` command, the daemon-cron path, and end-
to-end tests to confirm the full state-machine flow. Wiring the real
Orchestrator (with full LLM + DB resolution) is a follow-up that
benefits from an awake reviewer.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any, Protocol

log = logging.getLogger("amx.runtime.worker")


class _HistoryStore(Protocol):
    """The subset of the history store used by the worker."""

    def create_run(self, **kwargs: Any) -> int: ...

    def set_run_schedule_link(self, run_id: int, schedule_id: int) -> None: ...

    def update_run_heartbeat(self, run_id: int, *, now_utc: float | None = ...) -> None: ...

    def finish_run(self, run_id: int, **kwargs: Any) -> None: ...

    def set_scheduled_run_status(
        self,
        schedule_id: int,
        status: str,
        *,
        last_error: str | None = ...,
        fired_at: float | None = ...,
        triggered_run_id: int | None = ...,
    ) -> None: ...


# Pluggable per-run executor. Default is the small no-op stub below;
# callers can swap in a real Orchestrator runner in a follow-up PR.
RunExecutor = Callable[[int, dict[str, Any]], None]
"""Signature: (analysis_runs_id, schedule_payload) -> None.

Raises any exception to indicate failure; the worker translates this
into ``status='failed'`` on both rows.
"""


def default_run_executor(run_id: int, payload: dict[str, Any]) -> None:
    """No-op executor used until the real Orchestrator integration lands.

    Performs no real metadata-discovery work -- it exists so the
    scheduler's state-machine flow can be exercised end-to-end. A
    follow-up PR replaces this with the genuine Orchestrator drive
    (build LLM, resolve DB, loop tables, persist alternatives).

    A clear log line names the deferral so anyone tracing a run sees
    why this row finishes immediately.
    """
    log.warning(
        "amx.runtime.worker.default_run_executor: scheduled run %s "
        "marked complete WITHOUT running metadata discovery -- the "
        "full Orchestrator integration lands in a follow-up PR. "
        "Schedule payload: %s",
        run_id,
        json.dumps(
            {
                k: payload.get(k)
                for k in (
                    "id",
                    "name",
                    "db_profile",
                    "llm_profile",
                    "review_strategy",
                )
            }
        ),
    )


def spawn_scheduled_worker(
    payload: dict[str, Any],
    *,
    store: _HistoryStore,
    run_executor: RunExecutor | None = None,
    background: bool = True,
) -> int:
    """Create an ``analysis_runs`` row for the given schedule and run it.

    Returns the new ``analysis_runs.id`` immediately. When
    ``background=True`` (the production default for the daemon path)
    the executor runs in its own daemon thread so the tick loop is
    not blocked. ``background=False`` runs synchronously -- useful
    for the CLI ``schedule run-now`` flow where the user wants their
    invocation to block until the run finishes.

    *payload* is the full ``scheduled_runs`` row as returned by
    :meth:`SQLiteHistoryStore.get_scheduled_run`.
    """
    executor = run_executor or default_run_executor
    scope = _parse_scope(payload.get("scope_json"))

    run_id = store.create_run(
        command="schedule",
        mode="metadata",
        db_backend="(scheduled)",  # resolved by the real executor
        db_profile=str(payload.get("db_profile") or ""),
        llm_provider="(scheduled)",
        llm_model="(scheduled)",
        scope=scope,
        review_strategy=str(payload.get("review_strategy") or "auto"),
        llm_profile=str(payload.get("llm_profile") or ""),
    )
    schedule_id = int(payload["id"])
    store.set_run_schedule_link(run_id, schedule_id)
    # First heartbeat: keeps the stale-recovery sweep from immediately
    # reclaiming the row before the executor's first internal beat.
    store.update_run_heartbeat(run_id)
    # If the caller (tick's daemon path) has already transitioned the
    # schedule to ``running`` via claim_due_schedule, this is a no-op
    # (running -> running is accepted by the state machine). When the
    # worker is invoked standalone (CLI ``schedule run-now`` or tests),
    # we drive the transition here so the lifecycle is well-defined
    # regardless of entry point.
    try:
        store.set_scheduled_run_status(schedule_id, "running", fired_at=time.time())
    except ValueError:
        # Already in a terminal state -- nothing we can do; the
        # completion handler below will hit the same wall and skip.
        pass

    def _drive() -> None:
        try:
            executor(run_id, payload)
        except BaseException as exc:  # noqa: BLE001 - propagate to history
            log.exception("scheduled worker failed for schedule_id=%s", schedule_id)
            _mark_failed(store, run_id=run_id, schedule_id=schedule_id, error=str(exc))
            return
        _mark_completed(store, run_id=run_id, schedule_id=schedule_id)

    if background:
        threading.Thread(
            target=_drive,
            name=f"amx-scheduled-run-{run_id}",
            daemon=True,
        ).start()
    else:
        _drive()
    return run_id


def _parse_scope(scope_json: str | None) -> dict[str, list[str]]:
    """Turn the stored ``scope_json`` into the {schema: [table, ...]}
    shape ``create_run`` expects.

    The stored payload uses ``{"mode":"schemas|tables|all", ...}``;
    until the live resolver lands the worker passes through what it
    can extract and lets the (future) executor resolve the rest.
    """
    if not scope_json:
        return {}
    try:
        obj = json.loads(scope_json)
    except (TypeError, ValueError):
        return {}
    mode = obj.get("mode")
    if mode == "schemas":
        return {s: [] for s in obj.get("schemas", []) if isinstance(s, str)}
    if mode == "tables":
        out: dict[str, list[str]] = {}
        for item in obj.get("tables", []) or []:
            if not isinstance(item, dict):
                continue
            schema = str(item.get("schema") or "")
            table = str(item.get("table") or "")
            if not schema or not table:
                continue
            out.setdefault(schema, []).append(table)
        return out
    return {}


def _mark_completed(store: _HistoryStore, *, run_id: int, schedule_id: int) -> None:
    """Finalise analysis_runs + schedule rows on success."""
    now = time.time()
    try:
        store.finish_run(
            run_id,
            status="completed",
            metrics={},
            tokens={},
            results={},
        )
    except BaseException:  # noqa: BLE001
        log.exception("finish_run failed for run_id=%s", run_id)
    try:
        store.set_scheduled_run_status(
            schedule_id,
            "completed",
            triggered_run_id=run_id,
            fired_at=now,
        )
    except ValueError:
        # Already terminal -- the tick's manual path may have raced us.
        pass


def _mark_failed(
    store: _HistoryStore,
    *,
    run_id: int,
    schedule_id: int,
    error: str,
) -> None:
    """Finalise analysis_runs + schedule rows on failure."""
    truncated = error[:1000]
    try:
        store.finish_run(
            run_id,
            status="failed",
            metrics={},
            tokens={},
            results={},
            error_text=truncated,
        )
    except BaseException:  # noqa: BLE001
        log.exception("finish_run failed for run_id=%s", run_id)
    try:
        store.set_scheduled_run_status(
            schedule_id,
            "failed",
            last_error=truncated,
            triggered_run_id=run_id,
        )
    except ValueError:
        pass
