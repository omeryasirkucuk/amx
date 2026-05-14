"""Re-Run endpoint: regenerate alternatives for one or many run_results rows.

The user clicks "Re-Run" on an item in the Studio results view (or
selects N items via the multi-select toolbar). The SPA calls
``POST /api/runs/rerun-item`` with the target ``result_ids`` and an
optional free-text addendum. The worker:

1. Spawns a thread (``kind="rerun"``) and registers it with the same
   :class:`JobRegistry` the regular run / apply paths use.
2. Calls :func:`amx.agents._orchestrator.rerun.rerun_items`, which
   freezes a snapshot per target, runs the agents, and writes new
   versioned ``run_results`` rows linked to the original via
   ``parent_result_id``.
3. Streams ``activity.added`` / ``activity.complete`` / ``activity.fail``
   events through the queue so the existing
   ``GET /api/runs/{job_id}/events`` endpoint keeps working without
   any SSE-side changes.

Snapshots are short-lived and cleaned up by the executor's ``finally``
block. The router itself never writes to ``rerun_context_snapshots``.
"""

from __future__ import annotations

import threading
import time
from queue import Queue
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from amx.agents._orchestrator.rerun import RerunOutcome, rerun_items
from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult
from amx.agents.rerun_context import RerunContextError
from amx.config import AMXConfig
from amx.pending_review import load_pending, save_pending
from amx.utils.logging import get_logger
from amx.web.deps import get_cfg, get_jobs
from amx.web.jobs import Job, JobRegistry
from amx.web.progress_bus import emit, emit_terminal
from amx.web.routers.runs import LLMOverrides

router = APIRouter(prefix="/api", tags=["rerun"])
log = get_logger("web.rerun")


class RerunRequest(BaseModel):
    """Body for ``POST /api/runs/rerun-item``."""

    result_ids: list[int] = Field(
        ...,
        min_length=1,
        description=(
            "One or more ``run_results.id`` values to re-run. Each is "
            "regenerated independently; results are linked back to the "
            "original via ``parent_result_id``."
        ),
    )
    user_instructions: str | None = Field(
        default=None,
        description=(
            "Optional free-text addendum the user typed in the re-run "
            "modal. Appended to the existing prompt; the original "
            "DB/docs/code context is preserved."
        ),
    )
    temperature_override: float | None = Field(
        default=None,
        description=(
            "Legacy single-knob temperature override. New Studio code "
            "sends ``llm_overrides.temperature`` instead; this field is "
            "kept for one release so in-flight bundles don't break. When "
            "both are set, this field wins so the wire contract stays "
            "stable for existing callers."
        ),
        ge=0.0,
        le=1.0,
    )
    llm_overrides: LLMOverrides | None = Field(
        default=None,
        description=(
            "Per-run override of the active LLM profile's tuning knobs — "
            "same shape and validators as ``LLMOverrides`` on "
            "``POST /api/runs``. Mirrors RunNew's Advanced LLM settings "
            "panel content. The executor applies these via immutable "
            "``dataclasses.replace`` on a derived ``LLMConfig``; the "
            "saved profile on disk is never mutated."
        ),
    )


class RerunOutcomeResponse(BaseModel):
    target_result_id: int
    new_result_id: int
    rerun_seq: int
    schema_name: str
    table_name: str
    column_name: str | None
    asset_kind: str
    alternatives: list[str]
    confidence: str
    logprob_score: float | None
    source: str
    error: str | None = None


class RerunJobResponse(BaseModel):
    job_id: str
    status: str
    new_run_id: int | None = None


def _outcome_to_dict(outcome: RerunOutcome) -> dict[str, Any]:
    return {
        "target_result_id": outcome.target_result_id,
        "new_result_id": outcome.new_result_id,
        "rerun_seq": outcome.rerun_seq,
        "schema_name": outcome.schema,
        "table_name": outcome.table,
        "column_name": outcome.column,
        "asset_kind": outcome.asset_kind,
        "alternatives": outcome.alternatives,
        "confidence": outcome.confidence,
        "logprob_score": outcome.logprob_score,
        "source": outcome.source,
        "error": outcome.error,
    }


def _make_event_emitter(queue: Queue):
    def _emit(event_type: str, payload: dict[str, Any]) -> None:
        emit(queue, event_type, payload)

    return _emit


def _queue_outcomes_for_review(outcomes: list[RerunOutcome]) -> int:
    """Add successful re-run outcomes to the on-disk pending queue.

    A Re-Run row is born without a chosen description -- it is a list
    of fresh alternatives waiting for a human pick. The Studio renders
    such rows on the run-detail page, but without a matching pending
    entry the alternative buttons have nothing to attach to and a
    click is a no-op. Auto-seeding the queue with the top alternative
    gives the user a clickable row immediately; they can swap to a
    different alternative on the SPA (which PATCHes the same entry)
    or skip it back out of the queue.

    The first alternative is chosen as a sensible default (it is the
    model's top pick under the active confidence signal). Pending
    entries are keyed by ``result_id``; a same-result_id idempotency
    check on the restore endpoint already protects against duplicate
    appends on retry.

    Re-Run / Variations are explicit "redo this asset" actions, so
    the new row supersedes any prior pending entry for the same
    ``(schema, table, column, asset_kind)`` -- without this, the
    pending file ends up carrying both the original v1 entry (seeded
    by ``/run``) AND the new v2/v3 entry. The Apply step would then
    issue two ``COMMENT ON`` statements for the same column with
    last-write-wins semantics that are invisible from the SPA. The
    pre-filter below drops the prior entry so the new row is the
    canonical queued pick for the asset.
    """
    # Asset-key set of the outcomes we are about to queue. Used to
    # supersede any prior pending entry on the same asset. Failed
    # outcomes (``outcome.error`` truthy or no alternatives) are
    # excluded so a model failure on v2 doesn't silently delete the
    # user's already-queued v1 pick.
    supersede_keys: set[tuple[str, str, str | None, str]] = set()
    for o in outcomes:
        if o.error or o.new_result_id <= 0:
            continue
        first_alt = (o.alternatives or [None])[0]
        if not first_alt or not str(first_alt).strip():
            continue
        supersede_keys.add((o.schema, o.table, o.column, o.asset_kind or "table"))

    existing = load_pending()
    if supersede_keys:
        rows = [
            r
            for r in existing
            if (r.schema, r.table, r.column, r.asset_kind or "table") not in supersede_keys
        ]
    else:
        rows = list(existing)
    superseded = len(existing) - len(rows)

    appended = 0
    for outcome in outcomes:
        if outcome.error:
            continue
        if outcome.new_result_id <= 0:
            continue
        # An asset can carry no alternatives if the agent failed to
        # produce anything -- skip those rather than queue a row with
        # an empty final_description (save_pending would drop it).
        alts = list(outcome.alternatives or [])
        if not alts or not (alts[0] or "").strip():
            continue
        try:
            confidence = Confidence[outcome.confidence.upper()]
        except (KeyError, AttributeError):
            confidence = Confidence.MEDIUM
        rows.append(
            ReviewResult(
                schema=outcome.schema,
                table=outcome.table,
                column=outcome.column,
                final_description=alts[0],
                confidence=confidence,
                source=outcome.source or "rerun",
                applied=True,
                asset_kind=outcome.asset_kind or "table",
                result_id=int(outcome.new_result_id),
                alternatives=alts,
                logprob_score=outcome.logprob_score,
            )
        )
        appended += 1
    if appended or superseded:
        save_pending(rows)
    return appended


def _rerun_worker(
    cfg: AMXConfig,
    job: Job,
    payload: RerunRequest,
) -> None:
    """Drive a single re-run job.

    Mirrors the structure of the run / apply workers in
    :mod:`amx.web.routers.runs`: flip status to ``running``, route
    progress events through the queue, and end with a terminal event.
    """
    job.status = "running"
    started_wall = time.time()
    try:
        # Build the LLM-overrides dict the executor applies via
        # ``dataclasses.replace`` (immutable, no profile mutation).
        # The legacy ``temperature_override`` shim wins when both
        # surfaces send a temperature so an in-flight Studio bundle
        # stays consistent.
        llm_overrides_dict: dict[str, Any] | None = None
        if payload.llm_overrides is not None:
            llm_overrides_dict = payload.llm_overrides.non_null()
            if not llm_overrides_dict:
                llm_overrides_dict = None

        def _bind_run_id(run_id: int) -> None:
            job.run_id = run_id
            emit(job.queue, "run.created", {"run_id": run_id})

        new_run_id, outcomes = rerun_items(
            cfg,
            target_result_ids=list(payload.result_ids),
            user_instructions=payload.user_instructions,
            temperature_override=payload.temperature_override,
            llm_overrides=llm_overrides_dict,
            job_id=job.id,
            cancel_token=job.cancel,
            on_event=_make_event_emitter(job.queue),
            on_run_created=_bind_run_id,
        )
    except RerunContextError as exc:
        job.status = "failed"
        job.error = str(exc)
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": str(exc)})
        emit_terminal(job.queue, "job.failed", {"error": str(exc)})
        return
    except Exception as exc:  # noqa: BLE001 — last-resort handler
        log.exception("rerun worker crashed")
        message = f"{exc.__class__.__name__}: {exc}"
        job.status = "failed"
        job.error = message
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": message})
        emit_terminal(job.queue, "job.failed", {"error": message})
        return

    successful = sum(1 for o in outcomes if not o.error)
    try:
        queued = _queue_outcomes_for_review(outcomes)
    except Exception as exc:  # noqa: BLE001 -- review queue is best-effort
        log.warning("Failed to seed pending queue after re-run: %s", exc)
        queued = 0
    job.run_id = int(new_run_id)
    job.status = "done"
    job.summary = {
        "new_run_id": int(new_run_id),
        "total": len(outcomes),
        "successful": successful,
        "failed": len(outcomes) - successful,
        "pending_queued": queued,
        "duration_sec": round(time.time() - started_wall, 3),
        "outcomes": [_outcome_to_dict(o) for o in outcomes],
    }
    job.ended_at = time.time()
    if queued:
        emit(job.queue, "pending.saved", {"count": queued})
    emit_terminal(job.queue, "job.done", {"summary": job.summary})


@router.post("/runs/rerun-item", response_model=RerunJobResponse)
def submit_rerun(
    body: RerunRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> RerunJobResponse:
    """Spawn a re-run worker. Returns the job id immediately so the SPA
    can subscribe to the existing SSE stream.

    Cancel via ``POST /api/runs/{job_id}/cancel`` — same machinery as a
    normal run. Results land in a new ``analysis_runs`` row whose id is
    in the terminal ``job.done`` summary.
    """
    if not body.result_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="result_ids must contain at least one target id.",
        )
    job = jobs.new_job("rerun")
    thread = threading.Thread(
        target=_rerun_worker,
        args=(cfg, job, body),
        name=f"amx-studio-rerun-{job.id}",
        daemon=True,
    )
    thread.start()
    return RerunJobResponse(job_id=job.id, status=job.status)


__all__ = ["router"]
