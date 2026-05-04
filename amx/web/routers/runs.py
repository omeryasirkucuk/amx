"""Run + apply trigger endpoints with SSE progress streams.

PR-C wires the /visualize UI's "Run on this table" / "Apply approved"
buttons to the existing :class:`amx.agents.orchestrator.Orchestrator`
+ :func:`apply_review_results_to_db`.

Job lifecycle:

1. ``POST /api/runs`` (or ``POST /api/apply``) → spawn a worker
   thread, register a :class:`Job` in the JobRegistry, return the
   job id.
2. Worker calls into the orchestrator with the job's
   ``cancel_token`` + a progress callback that pushes events onto
   the job's queue.
3. ``GET /api/runs/{id}/events`` (and ``/api/apply/{id}/events``)
   tails the queue as Server-Sent-Events.
4. ``POST /api/runs/{id}/cancel`` flips the cancel token; the worker
   bails between rows and the SSE stream emits ``job.cancelled``.

Per-job state is in-memory only — the visualizer is single-process
and per-CLI-session. PR-D adds the ``/ask`` job kind on the same
machinery.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from queue import Empty
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from amx.agents.orchestrator import (
    Orchestrator,
    ReviewResult,
    RunCancelled,
    apply_review_results_to_db,
)
from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector
from amx.llm.provider import LLMProvider
from amx.pending_review import load_pending, save_pending
from amx.storage.sqlite_store import history_store
from amx.utils.logging import get_logger
from amx.utils.token_tracker import tracker as token_tracker
from amx.web.deps import get_cfg, get_jobs
from amx.web.jobs import Job, JobRegistry
from amx.web.progress_bus import emit, emit_terminal

router = APIRouter(prefix="/api", tags=["runs"])
log = get_logger("web.runs")


class RunRequest(BaseModel):
    """Body for ``POST /api/runs``.

    PR-C only wires the apply path through this layer; the full run
    pipeline (Orchestrator.process_table[s_batch_mode]) lands once
    the headless run plumbing on AMXApplication is ready (see
    plan §3 Run section). For now ``/api/runs`` accepts the same
    payload shape so the SPA can stub-call it; the worker emits an
    explanatory ``job.failed`` event.
    """

    scope: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Schema → tables map. Empty {} means 'every reachable schema/table'.",
    )
    apply: bool = Field(default=False, description="Auto-apply after the run completes.")
    missing_only: bool = Field(default=False)
    batch_mode: bool = Field(default=False)


class ApplyRequest(BaseModel):
    """Body for ``POST /api/apply``.

    ``results`` accepts the on-disk ``ReviewResult`` shape. Omit the
    field to apply the user's pending queue
    (``~/.amx/pending_metadata.json``) end-to-end.
    """

    results: list[dict[str, Any]] | None = None


@router.post("/runs")
def submit_run(
    body: RunRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn a /run worker. Returns the job id immediately so the SPA
    can subscribe to the SSE event stream."""
    job = jobs.new_job("run")
    thread = threading.Thread(
        target=_run_worker,
        args=(cfg, job, body),
        name=f"amx-visualizer-run-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status}


@router.post("/apply")
def submit_apply(
    body: ApplyRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn an /apply worker. When ``body.results`` is omitted, the
    worker reads the pending review queue from disk."""
    job = jobs.new_job("apply")
    thread = threading.Thread(
        target=_apply_worker,
        args=(cfg, job, body),
        name=f"amx-visualizer-apply-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status}


@router.get("/runs/{job_id}")
def get_job(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    """Synchronous job snapshot — what the SPA polls when it can't
    keep an SSE connection open (e.g. user navigated away and back)."""
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No job {job_id}")
    return job.to_public_dict()


@router.get("/apply/{job_id}")
def get_apply_job(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    return get_job(job_id, jobs)


@router.get("/runs/{job_id}/events")
def stream_run_events(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> EventSourceResponse:
    return _events_endpoint(job_id, jobs)


@router.get("/apply/{job_id}/events")
def stream_apply_events(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> EventSourceResponse:
    return _events_endpoint(job_id, jobs)


@router.post("/runs/{job_id}/cancel")
def cancel_run(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    if not jobs.cancel(job_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active job {job_id} to cancel.",
        )
    return {"ok": True, "job_id": job_id}


@router.post("/apply/{job_id}/cancel")
def cancel_apply(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    return cancel_run(job_id, jobs)


# ── Internals ──────────────────────────────────────────────────────────


def _events_endpoint(job_id: str, jobs: JobRegistry) -> EventSourceResponse:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No job {job_id}")
    return EventSourceResponse(_event_generator(job))


def _event_generator(job: Job):
    """Drain ``job.queue`` until a terminal event arrives.

    Sends a periodic SSE comment line as a keepalive so corporate
    proxies don't reap the connection during long worker steps.
    """
    last_keepalive = time.monotonic()
    while True:
        try:
            event = job.queue.get(timeout=15)
        except Empty:
            now = time.monotonic()
            if now - last_keepalive > 14:
                yield {"event": "ping", "data": json.dumps({"t": now})}
                last_keepalive = now
            if job.status not in ("queued", "running"):
                # Worker terminated without a final event (shouldn't
                # happen, but ensures we don't tail an idle queue).
                break
            continue
        kind = str(event.get("type", ""))
        yield {"event": kind, "data": json.dumps(event)}
        if kind in {"job.done", "job.cancelled", "job.failed"}:
            break


def _build_progress_callback(job: Job) -> Callable[[ReviewResult, str, int, int, str], None]:
    def _on_progress(r: ReviewResult, status_word: str, idx: int, total: int, detail: str) -> None:
        emit(
            job.queue,
            "writeback.progress",
            {
                "schema": r.schema,
                "table": r.table,
                "column": r.column,
                "asset_kind": r.asset_kind,
                "status": status_word,
                "done": idx,
                "total": total,
                "detail": detail or "",
            },
        )

    return _on_progress


def _run_worker(cfg: AMXConfig, job: Job, body: RunRequest) -> None:
    """Drive a headless ``/run`` from the visualizer.

    Mirrors the non-interactive subset of ``cli_support/commands/
    analyze_flow.py``: create a history row, build Orchestrator, walk
    every (schema, table) in the requested scope, persist alternatives,
    and stash the deferred ReviewResults in ``~/.amx/pending_metadata.
    json`` for review on the SPA's Pending tab. Optional ``apply=True``
    chains an apply step on top.

    Compared to the CLI flow we skip:
      * Scope picker / coverage prompts (the SPA pre-selects).
      * Equivalence-class dedup pre-pass (still runs per-table normally
        once dedup lands as a per-run flag — out of scope for Stage 2).
      * Doc / code agents (RAG store + code report bindings — Stage 3
        wires those into the wizard).

    Cancellation: ``job.cancel`` is checked between tables so a long
    multi-schema run can be aborted cleanly mid-flight.
    """
    job.status = "running"
    run_started = time.monotonic()
    token_tracker.reset()

    if not cfg.llm.provider or not cfg.llm.model:
        _fail_job(job, "No active LLM profile is configured. Use Settings to add one.")
        return

    scope: dict[str, list[str]] = {
        str(s): [str(t) for t in (tables or [])] for s, tables in (body.scope or {}).items()
    }
    if not scope or not any(tbls for tbls in scope.values()):
        _fail_job(
            job,
            "Run scope is empty. Pick at least one table before submitting.",
        )
        return

    total_assets = sum(len(v) for v in scope.values())
    emit(
        job.queue,
        "run.scope.resolved",
        {
            "scope": scope,
            "total_assets": total_assets,
            "total_schemas": len(scope),
        },
    )

    try:
        db = DatabaseConnector(cfg.db)
    except Exception as exc:
        _fail_job(job, f"Could not open DB connector: {exc}")
        return

    try:
        llm = LLMProvider(cfg.llm)
    except Exception as exc:
        _fail_job(job, f"Could not initialize LLM: {exc}")
        try:
            db.close()
        except Exception:
            pass
        return

    run_id: int | None = None
    hs = history_store()
    if hs is not None:
        try:
            run_id = hs.create_run(
                command="analyze.run",
                mode="chat",
                db_backend=cfg.db.backend,
                db_profile=cfg.active_db_profile,
                llm_provider=cfg.llm.provider,
                llm_model=cfg.llm.model,
                scope=scope,
                selected_count=total_assets,
                planned_count=total_assets,
                review_strategy="individual",
                llm_profile=cfg.active_llm_profile,
                doc_profile=cfg.active_doc_profile or None,
                code_profile=cfg.active_code_profile or None,
                settings={
                    "missing_only": bool(body.missing_only),
                    "applied_flag": bool(body.apply),
                    "trigger": "visualizer",
                },
            )
            emit(job.queue, "run.created", {"run_id": int(run_id)})
        except Exception as exc:
            log.warning("Could not persist run history: %s", exc)

    orchestrator = Orchestrator(
        db=db,
        llm=llm,
        run_id=run_id,
        missing_only=bool(body.missing_only),
    )

    processed_assets: list[str] = []
    skipped_assets: list[str] = []
    failed_assets: list[tuple[str, str]] = []  # (asset_path, error)
    final_error_text = ""
    final_status = "success"

    try:
        idx_global = 0
        for schema, tables in scope.items():
            for table in tables:
                idx_global += 1
                if job.cancel.is_set():
                    raise RunCancelled(f"Cancelled before {schema}.{table}")
                asset_path = f"{schema}.{table}"
                emit(
                    job.queue,
                    "activity.added",
                    {
                        "idx": idx_global,
                        "label": asset_path,
                        "kind": "table",
                        "done": idx_global - 1,
                        "total": total_assets,
                    },
                )
                emit(job.queue, "activity.begin", {"idx": idx_global})
                try:
                    table_results = orchestrator.process_table(
                        schema,
                        table,
                        interactive_review=False,
                        auto_apply=False,
                    )
                except RunCancelled:
                    raise
                except Exception as exc:
                    failed_assets.append((asset_path, str(exc)))
                    log.warning("Table %s failed: %s", asset_path, exc)
                    emit(
                        job.queue,
                        "activity.fail",
                        {"idx": idx_global, "detail": f"{exc.__class__.__name__}: {exc}"},
                    )
                    continue
                processed_assets.append(asset_path)
                emit(
                    job.queue,
                    "activity.complete",
                    {
                        "idx": idx_global,
                        "detail": f"{len(table_results)} suggestion(s)",
                        "results": [_review_result_to_event(r) for r in table_results],
                    },
                )
    except RunCancelled:
        job.status = "cancelled"
        final_status = "cancelled"
        emit_terminal(job.queue, "job.cancelled", {})
    except Exception as exc:
        log.exception("Run worker crashed")
        final_status = "failed"
        final_error_text = f"{exc.__class__.__name__}: {exc}"
        job.status = "failed"
        job.error = final_error_text
        emit_terminal(job.queue, "job.failed", {"error": final_error_text})

    # Persist deferred review results into the pending queue regardless
    # of cancellation — the user may want to review what *did* finish.
    deferred: list[ReviewResult] = []
    for r in orchestrator.results:
        # _deferred_branch sets applied=False; mark as pending=True so
        # save_pending() picks them up (it skips applied=False rows by
        # default — see pending_review.save_pending).
        r.applied = True
        deferred.append(r)

    pending_count = 0
    if deferred:
        try:
            save_pending(deferred)
            pending_count = len(deferred)
            emit(job.queue, "pending.saved", {"count": pending_count})
        except Exception as exc:
            log.warning("Failed to save pending queue: %s", exc)

    try:
        db.close()
    except Exception:
        pass

    # Optional: chain an apply step automatically. We only do this when
    # the run finished successfully and there's something to apply.
    applied = 0
    if (
        final_status == "success"
        and bool(body.apply)
        and pending_count > 0
        and not job.cancel.is_set()
    ):
        try:
            db_for_apply = DatabaseConnector(cfg.db)
            applied = apply_review_results_to_db(
                db_for_apply,
                deferred,
                on_progress=_build_progress_callback(job),
                cancel_token=job.cancel,
            )
            try:
                db_for_apply.close()
            except Exception:
                pass
        except Exception as exc:
            log.warning("Auto-apply after run failed: %s", exc)
            final_error_text = f"Auto-apply failed: {exc}"

    if hs is not None and run_id is not None:
        try:
            hs.finish_run(
                run_id,
                status=final_status,
                metrics={
                    "duration_sec": round(time.monotonic() - run_started, 3),
                    "total_assets": total_assets,
                    "total_schemas": len(scope),
                    "processed_assets_count": len(processed_assets),
                    "processed_assets": processed_assets,
                    "skipped_assets_count": len(skipped_assets),
                    "skipped_assets": skipped_assets,
                    "failed_assets_count": len(failed_assets),
                    "applied_flag": bool(body.apply),
                    "applied_count": int(applied),
                },
                tokens={
                    "total_tokens": token_tracker.total_tokens,
                    "summary": token_tracker.summary(),
                },
                results={"pending_count": pending_count},
                error_text=final_error_text,
            )
        except Exception as exc:
            log.warning("finish_run failed: %s", exc)

    if final_status == "success":
        job.status = "done"
        job.summary = {
            "run_id": run_id,
            "processed": len(processed_assets),
            "failed": len(failed_assets),
            "pending": pending_count,
            "applied": int(applied),
        }
        job.ended_at = time.time()
        emit_terminal(job.queue, "job.done", {"summary": job.summary})


def _fail_job(job: Job, message: str) -> None:
    """Helper: stamp a job as failed and emit a terminal event."""
    job.status = "failed"
    job.error = message
    job.ended_at = time.time()
    emit(job.queue, "activity.fail", {"idx": 0, "detail": message})
    emit_terminal(job.queue, "job.failed", {"error": message})


def _review_result_to_event(r: ReviewResult) -> dict[str, Any]:
    """Compact dict representation for streaming on activity.complete.

    Keeps the SSE payload small — no full alternatives list, no
    logprob bars; the SPA fetches the rich shape via
    GET /api/history/runs/{id}/results when the user opens the run
    detail page.
    """
    return {
        "schema": r.schema,
        "table": r.table,
        "column": r.column,
        "asset_kind": getattr(r, "asset_kind", "table"),
        "confidence": r.confidence.value if r.confidence else "medium",
        "preview": (r.final_description or "")[:160],
        "result_id": r.result_id,
    }


def _apply_worker(cfg: AMXConfig, job: Job, body: ApplyRequest) -> None:
    job.status = "running"
    emit(
        job.queue,
        "activity.added",
        {"idx": 0, "label": "Writing approved descriptions"},
    )
    emit(job.queue, "activity.begin", {"idx": 0})

    try:
        results = _resolve_apply_results(body)
    except FileNotFoundError as exc:
        job.status = "failed"
        job.error = str(exc)
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(job.queue, "job.failed", {"error": job.error})
        return

    if not results:
        job.status = "done"
        job.summary = {"applied": 0, "total": 0}
        job.ended_at = time.time()
        emit(job.queue, "activity.complete", {"idx": 0, "detail": "No approved rows to apply."})
        emit_terminal(job.queue, "job.done", {"summary": job.summary})
        return

    db = DatabaseConnector(cfg.db)
    try:
        applied = apply_review_results_to_db(
            db,
            results,
            on_progress=_build_progress_callback(job),
            cancel_token=job.cancel,
        )
    except RunCancelled:
        # apply_review_results_to_db already commits-what-was-applied
        # before raising; this branch only triggers if a future
        # version starts raising explicitly. Treat both as cancelled.
        job.status = "cancelled"
        job.summary = {"applied": 0, "total": len(results)}
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": "Cancelled."})
        emit_terminal(job.queue, "job.cancelled", {"summary": job.summary})
        return
    except Exception as exc:
        job.status = "failed"
        job.error = str(exc)
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": str(exc)})
        emit_terminal(job.queue, "job.failed", {"error": job.error})
        return
    finally:
        try:
            db.close()
        except Exception:  # pragma: no cover - best-effort cleanup
            pass

    if job.cancel.is_set():
        # The loop returned cleanly, but the user's cancel signal
        # had fired. Surface as cancelled so the SPA shows the
        # "you cancelled mid-flight" banner.
        job.status = "cancelled"
        job.summary = {"applied": int(applied), "total": len(results)}
        job.ended_at = time.time()
        emit(
            job.queue,
            "activity.complete",
            {"idx": 0, "detail": f"Cancelled after {applied}/{len(results)}."},
        )
        emit_terminal(job.queue, "job.cancelled", {"summary": job.summary})
        return

    job.status = "done"
    job.summary = {"applied": int(applied), "total": len(results)}
    job.ended_at = time.time()
    emit(
        job.queue,
        "activity.complete",
        {"idx": 0, "detail": f"Applied {applied}/{len(results)}."},
    )
    emit_terminal(job.queue, "job.done", {"summary": job.summary})


def _resolve_apply_results(body: ApplyRequest) -> list[ReviewResult]:
    """Either coerce the body's results into ReviewResult objects or
    fall back to the on-disk pending queue."""
    if body.results is None:
        return list(load_pending())

    out: list[ReviewResult] = []
    for raw in body.results:
        try:
            out.append(_review_result_from_dict(raw))
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid result entry: {exc}",
            ) from exc
    return out


def _review_result_from_dict(raw: dict[str, Any]) -> ReviewResult:
    from amx.agents.base import Confidence

    confidence_value = raw.get("confidence", "medium")
    if isinstance(confidence_value, str):
        try:
            confidence = Confidence[confidence_value.upper()]
        except KeyError:
            confidence = Confidence.MEDIUM
    else:
        confidence = Confidence.MEDIUM

    return ReviewResult(
        schema=str(raw["schema"]),
        table=str(raw["table"]),
        column=raw.get("column"),
        final_description=str(raw["final_description"]),
        confidence=confidence,
        source=str(raw.get("source", "manual")),
        applied=bool(raw.get("applied", True)),
        asset_kind=str(raw.get("asset_kind", "table")),
        result_id=raw.get("result_id"),
        alternatives=list(raw.get("alternatives") or []),
        logprob_score=raw.get("logprob_score"),
    )
