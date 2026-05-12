"""Stateless tick: one pass over the schedule store.

The tick performs three things per call:

1. Surface stale running runs so the UI can mark them recovered. The
   spawn-worker integration that updates ``analysis_runs.status`` for
   crashed runs is intentionally out of scope here — the heartbeat
   wiring lands together with the orchestrator update in a follow-up
   PR. For now the report's ``stale_recovered`` list is always empty
   so the call sites have the shape they need.
2. Find pending schedules whose ``fire_at_utc`` has elapsed. The
   *source* controls what happens next:

   * ``"bootstrap"`` — surface them in ``missed_for_review``. Do
     **not** fire. The CLI banner / Studio dashboard banner show the
     user this list; the user decides per-item what to do via
     ``amx schedule review`` or the Schedules page.
   * ``"daemon"`` — fire each in turn via the injected
     ``spawn_worker`` callable. Used by the launchd/systemd cron
     entry once the daemon ships in phase 4.
   * ``"manual"`` — fire exactly the schedule named by
     ``target_id``, transitioning it from ``pending`` (or ``missed``)
     to ``running`` first.

3. Return a :class:`TickReport` summarising what happened. Callers
   render it (CLI banner, Studio bootstrap-report endpoint) — the
   tick itself is purely transactional.

``spawn_worker`` is injectable so tests can supply a recorder and so
the production wiring (Studio's run worker) can land in a follow-up
without rewriting this module. The default — used in production once
phase 2b lands — will delegate to ``amx.runtime.worker.run_orchestrator``.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

Source = Literal["bootstrap", "daemon", "manual"]


class _ScheduleStore(Protocol):
    """The subset of ``SQLiteHistoryStore`` (or any IHistoryStore
    implementation) that the tick relies on.

    Defined as a structural Protocol so tests can pass a minimal fake
    without dragging in the full storage stack.
    """

    def list_due_pending_schedules(
        self, *, now_utc: float, limit: int = 200
    ) -> list[dict[str, Any]]: ...

    def claim_due_schedule(self, *, now_utc: float) -> int | None: ...

    def get_scheduled_run(self, schedule_id: int) -> dict[str, Any] | None: ...

    def set_scheduled_run_status(
        self,
        schedule_id: int,
        status: str,
        *,
        last_error: str | None = ...,
        fired_at: float | None = ...,
        triggered_run_id: int | None = ...,
    ) -> None: ...


SpawnWorker = Callable[[dict[str, Any]], int]
"""Callable that spawns a run worker from a schedule's payload.

Receives the full ``scheduled_runs`` row as a dict; returns the
``analysis_runs.id`` of the new run. The worker itself runs in
its own thread / process and updates the schedule status when
it completes.
"""


@dataclass
class TickReport:
    """Summary of a single tick. All lists may be empty."""

    fired: list[int] = field(default_factory=list)
    """Schedule ids that were transitioned to ``running`` and handed
    to ``spawn_worker``. Populated only for daemon / manual sources."""

    failed_resolution: list[tuple[int, str]] = field(default_factory=list)
    """Schedules that could not be fired because resolution failed
    (e.g., missing schema). Pairs of ``(schedule_id, error_message)``."""

    missed_for_review: list[int] = field(default_factory=list)
    """Schedules whose fire time has elapsed but which were NOT fired
    because ``source='bootstrap'``. The UI surfaces these to the user."""

    stale_recovered: list[int] = field(default_factory=list)
    """analysis_runs ids that were recovered as ``failed`` because
    their last heartbeat is older than the threshold. Always empty
    until the heartbeat wiring lands; the field is here so the
    report shape is stable across phases."""


def tick(
    *,
    store: _ScheduleStore,
    source: Source,
    target_id: int | None = None,
    spawn_worker: SpawnWorker | None = None,
    now_utc: float | None = None,
    max_per_tick: int = 50,
) -> TickReport:
    """Perform one scheduling pass against *store*.

    The tick never raises for per-schedule failures: a resolution
    error on one schedule is recorded in ``failed_resolution`` and
    the loop continues. Only programmer errors (wrong source value,
    manual without ``target_id``) surface as exceptions.
    """
    if source not in ("bootstrap", "daemon", "manual"):
        raise ValueError(f"unknown tick source: {source!r}")

    now = now_utc if now_utc is not None else time.time()
    report = TickReport()

    if source == "bootstrap":
        # Surface missed schedules. Do NOT fire — bootstrap respects
        # the user-warning contract: when AMX is closed, missed
        # schedules wait for the user's next interactive review.
        due = store.list_due_pending_schedules(now_utc=now)
        report.missed_for_review = [int(row["id"]) for row in due]
        return report

    if source == "manual":
        if target_id is None:
            raise ValueError("tick(source='manual') requires target_id")
        if spawn_worker is None:
            raise ValueError("tick(source='manual') requires a spawn_worker callable")
        _fire_one(
            store=store,
            schedule_id=target_id,
            now=now,
            spawn_worker=spawn_worker,
            report=report,
            force=True,
        )
        return report

    # source == "daemon"
    if spawn_worker is None:
        raise ValueError("tick(source='daemon') requires a spawn_worker callable")

    for _ in range(max_per_tick):
        sid = store.claim_due_schedule(now_utc=now)
        if sid is None:
            break
        _fire_one(
            store=store,
            schedule_id=sid,
            now=now,
            spawn_worker=spawn_worker,
            report=report,
            force=False,
        )
    return report


def _fire_one(
    *,
    store: _ScheduleStore,
    schedule_id: int,
    now: float,
    spawn_worker: SpawnWorker,
    report: TickReport,
    force: bool,
) -> None:
    """Move a single schedule from its current state to ``running`` (if
    not already moved by a prior claim) and hand it to ``spawn_worker``.

    For ``source='daemon'`` the caller has already transitioned the
    row to ``running`` via ``claim_due_schedule`` -- we just fetch the
    payload. For ``source='manual'`` (``force=True``) we drive the
    transition here so user-initiated re-runs work regardless of the
    schedule's prior state.
    """
    payload = store.get_scheduled_run(schedule_id)
    if payload is None:
        report.failed_resolution.append((schedule_id, f"schedule id={schedule_id} not found"))
        return

    if force and payload["status"] != "running":
        # Manual fire: transition via the state-machine path. If the
        # state machine rejects (e.g., schedule is already in a
        # terminal state), surface the error and stop.
        try:
            store.set_scheduled_run_status(schedule_id, "running", fired_at=now)
        except ValueError as exc:
            report.failed_resolution.append((schedule_id, str(exc)))
            return
        payload = store.get_scheduled_run(schedule_id) or payload

    try:
        run_id = spawn_worker(payload)
    except Exception as exc:  # noqa: BLE001 - worker failures must not crash the tick
        # Mark schedule as failed so the user sees it; ``last_error``
        # carries the worker's complaint verbatim.
        try:
            store.set_scheduled_run_status(
                schedule_id,
                "failed",
                last_error=f"spawn_worker failed: {exc}",
            )
        except ValueError:
            # Already in a terminal state; nothing useful to do but
            # record it in the report so the caller can log.
            pass
        report.failed_resolution.append((schedule_id, str(exc)))
        return

    # Worker has been spawned; record the linkage so the UI can deep-
    # link from the schedule row to the live run. The schedule
    # itself stays in ``running`` until the worker updates its
    # status on completion -- not the tick's job.
    try:
        # Re-read status first to avoid touching anything that's
        # already terminal (worker could in principle have finished
        # before this UPDATE lands, in which case ``running`` ->
        # ``running`` is a no-op and the state machine accepts it).
        current = store.get_scheduled_run(schedule_id)
        if current is not None and current["status"] == "running":
            # Use an in-place no-op transition to attach the run id;
            # the store's set_scheduled_run_status preserves status
            # when source==target.
            store.set_scheduled_run_status(schedule_id, "running", triggered_run_id=run_id)
    except ValueError:
        # Worker finished extremely fast; the schedule is already
        # terminal. The worker's own status update has the linkage.
        pass

    report.fired.append(schedule_id)
