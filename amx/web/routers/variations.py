"""Variations endpoint: seeded re-run from one chosen alternative.

The Studio's run-detail page renders a per-alternative ✨ trigger; the
modal collects ``(result_id, alternative_index, seed_text, mode)`` and
posts here. The worker spawns a thread, calls
:func:`amx.agents._orchestrator.variations.variations_one_item`, and
streams the same SSE event types
(``activity.added`` / ``activity.begin`` / ``activity.complete`` /
``activity.fail`` / terminal ``job.done``) the Re-Run flow uses — so
the SPA's existing ``GET /api/runs/{job_id}/events`` consumer works
unchanged.

Distinct from ``/api/runs/rerun-item`` because the wire shape differs
(``alternative_index`` + ``seed_text`` + top-level ``mode`` instead of a
list of ``result_ids``) and the audit columns persisted on the new
``run_results`` row differ too.
"""

from __future__ import annotations

import threading
import time
from queue import Queue
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from amx.agents._orchestrator.rerun import RerunContextError
from amx.agents._orchestrator.variations import variations_one_item
from amx.config import AMXConfig
from amx.utils.logging import get_logger
from amx.web.deps import get_cfg, get_jobs
from amx.web.jobs import Job, JobRegistry
from amx.web.progress_bus import emit, emit_terminal
from amx.web.routers.runs import LLMOverrides

router = APIRouter(prefix="/api", tags=["variations"])
log = get_logger("web.variations")


class VariationsRequest(BaseModel):
    """Body for ``POST /api/runs/variations``."""

    original_run_id: int = Field(
        ...,
        ge=1,
        description="``analysis_runs.id`` of the run that owns the seed alternative.",
    )
    result_id: int = Field(
        ...,
        ge=1,
        description=(
            "``run_results.id`` of the source row. The seed_text is "
            "expected to match one of its ``alternatives_json`` entries."
        ),
    )
    alternative_index: int = Field(
        ...,
        ge=0,
        le=25,
        description="Zero-based index into the source row's alternatives_json.",
    )
    seed_text: str = Field(
        ...,
        min_length=1,
        description="Verbatim text of the chosen alternative — the seed.",
    )
    mode: str = Field(
        default="semantic",
        description=(
            "Top-level diversity mode for this Variations run: "
            "``semantic`` (paraphrase the seed) or ``lexical`` (share "
            "vocabulary with the seed, allow meaning to drift). Wins "
            "over the saved profile's mode and over any nested "
            "``llm_overrides.alternatives_mode`` value."
        ),
    )
    user_instructions: str | None = Field(
        default=None,
        description=(
            "Optional free-text addendum the user typed in the modal. "
            "Layered on top of the seed directive."
        ),
    )
    llm_overrides: LLMOverrides | None = Field(
        default=None,
        description=(
            "Per-run override of the active LLM profile's tuning knobs "
            "— same shape and validators as on ``POST /api/runs``. The "
            "executor applies these via immutable ``dataclasses.replace`` "
            "on a derived ``LLMConfig``; the saved profile is never "
            "mutated. The top-level ``mode`` field above takes "
            "precedence over any ``alternatives_mode`` value in here."
        ),
    )


class VariationsJobResponse(BaseModel):
    job_id: str
    status: str
    new_run_id: int | None = None


def _make_event_emitter(queue: Queue):
    def _emit(event_type: str, payload: dict[str, Any]) -> None:
        emit(queue, event_type, payload)

    return _emit


def _variations_worker(
    cfg: AMXConfig,
    job: Job,
    payload: VariationsRequest,
) -> None:
    job.status = "running"
    started_wall = time.time()
    try:
        llm_overrides_dict: dict[str, Any] | None = None
        if payload.llm_overrides is not None:
            llm_overrides_dict = payload.llm_overrides.non_null()
            if not llm_overrides_dict:
                llm_overrides_dict = None

        def _bind_run_id(run_id: int) -> None:
            job.run_id = run_id
            emit(job.queue, "run.created", {"run_id": run_id})

        new_run_id, outcome = variations_one_item(
            cfg,
            original_run_id=int(payload.original_run_id),
            result_id=int(payload.result_id),
            alternative_index=int(payload.alternative_index),
            seed_text=payload.seed_text,
            mode=str(payload.mode or "semantic"),
            user_instructions=payload.user_instructions,
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
        log.exception("variations worker crashed")
        message = f"{exc.__class__.__name__}: {exc}"
        job.status = "failed"
        job.error = message
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": message})
        emit_terminal(job.queue, "job.failed", {"error": message})
        return

    job.run_id = int(new_run_id)
    job.status = "done"
    job.summary = {
        "new_run_id": int(new_run_id),
        "new_result_id": int(outcome.new_result_id),
        "alternatives": list(outcome.alternatives),
        "confidence": outcome.confidence,
        "logprob_score": outcome.logprob_score,
        "duration_sec": round(time.time() - started_wall, 3),
        "seed_alternative_id": f"{payload.result_id}:{payload.alternative_index}",
        "mode": payload.mode,
    }
    job.ended_at = time.time()
    emit_terminal(job.queue, "job.done", {"summary": job.summary})


@router.post("/runs/variations", response_model=VariationsJobResponse)
def submit_variations(
    body: VariationsRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> VariationsJobResponse:
    """Spawn a Variations worker.

    Returns the job id immediately so the SPA can subscribe to the
    existing SSE stream. Results land in a new ``analysis_runs`` row
    whose id is in the terminal ``job.done`` summary.
    """
    if not body.seed_text.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="seed_text must be non-empty.",
        )
    if body.mode not in {"semantic", "lexical"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="mode must be 'semantic' or 'lexical'.",
        )
    job = jobs.new_job("variations")
    thread = threading.Thread(
        target=_variations_worker,
        args=(cfg, job, body),
        name=f"amx-studio-variations-{job.id}",
        daemon=True,
    )
    thread.start()
    return VariationsJobResponse(job_id=job.id, status=job.status)


__all__ = ["router"]
