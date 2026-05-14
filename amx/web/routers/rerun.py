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
from amx.agents.rerun_context import RerunContextError
from amx.config import AMXConfig
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
    job.run_id = int(new_run_id)
    job.status = "done"
    job.summary = {
        "new_run_id": int(new_run_id),
        "total": len(outcomes),
        "successful": successful,
        "failed": len(outcomes) - successful,
        "duration_sec": round(time.time() - started_wall, 3),
        "outcomes": [_outcome_to_dict(o) for o in outcomes],
    }
    job.ended_at = time.time()
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
