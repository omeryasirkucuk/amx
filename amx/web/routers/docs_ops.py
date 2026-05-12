"""Document scan / ingest / search routes.

Wraps :mod:`amx.docs.scanner` and :mod:`amx.docs.rag` so
AMX Studio's Settings → Docs tab can drive the same flows the CLI's
``/scan`` / ``/ingest`` / ``/search-docs`` commands run, without
spawning a subprocess. Every long-running operation goes through the
existing :class:`Job` registry + SSE event bus so the SPA renders
the progress live.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from pydantic import BaseModel, Field

from amx.config import AMXConfig
from amx.docs.uploads import (
    MAX_BATCH_BYTES,
    UploadError,
    save_uploaded_batch,
)
from amx.utils.console import quiet_console
from amx.utils.logging import get_logger
from amx.web.deps import get_cfg, get_jobs
from amx.web.jobs import Job, JobRegistry
from amx.web.progress_bus import emit, emit_terminal

router = APIRouter(prefix="/api/docs", tags=["docs"])
log = get_logger("web.docs_ops")


class _DocPathsRequest(BaseModel):
    """Body for ``POST /api/docs/scan`` and ``/ingest``.

    Both endpoints accept either an explicit ``paths`` array or a
    ``profile`` name pointing at one of the user's saved doc profiles
    (``cfg.doc_profiles[name]``). When neither is given, the active
    doc profile is used — same fallback the CLI's ``/scan`` flow has.
    """

    paths: list[str] | None = None
    profile: str | None = None
    refresh: bool = Field(
        default=False,
        description="Ingest only — drop existing chunks for each source before re-uploading.",
    )


def _resolve_paths(body: _DocPathsRequest, cfg: AMXConfig) -> list[str]:
    if body.paths:
        return [p for p in body.paths if (p or "").strip()]
    profile = (body.profile or "").strip() or (cfg.active_doc_profile or "").strip()
    if not profile:
        return []
    return list(cfg.doc_profiles.get(profile, []))


@router.post("/scan")
def submit_scan(
    body: _DocPathsRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn a doc-scan worker. Streams ``activity.added`` /
    ``activity.complete`` SSE events with the file inventory."""
    paths = _resolve_paths(body, cfg)
    if not paths:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No paths to scan. Pass paths=[...], profile=<name>, or activate a doc profile.",
        )
    job = jobs.new_job("docs_scan")
    thread = threading.Thread(
        target=_scan_worker,
        args=(job, paths),
        name=f"amx-docs-scan-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status, "paths": paths}


@router.post("/ingest")
def submit_ingest(
    body: _DocPathsRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn a doc-ingest worker. Reuses the same SSE event shape as
    ``/scan`` plus a final ``ingest.summary`` event with chunk counts."""
    paths = _resolve_paths(body, cfg)
    if not paths:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No paths to ingest. Pass paths=[...], profile=<name>, or activate a doc profile.",
        )
    profile_name = (body.profile or "").strip() or (cfg.active_doc_profile or "").strip()
    job = jobs.new_job("docs_ingest")
    thread = threading.Thread(
        target=_ingest_worker,
        args=(job, paths, bool(body.refresh), cfg, profile_name),
        name=f"amx-docs-ingest-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status, "paths": paths, "refresh": bool(body.refresh)}


@router.post("/jobs/{job_id}/cancel")
def cancel_job(
    job_id: str,
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Signal an in-flight docs ingest/scan worker to stop.

    The worker polls ``job.cancel`` between documents (never mid-Chroma
    write) so cancellation latency is one document at most. Returns 200
    when the job exists and the flag is set, 404 when the job id is
    unknown.
    """
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found.",
        )
    job.cancel.set()
    return {"job_id": job_id, "cancelled": True, "status": job.status}


@router.post("/upload")
async def upload_docs(
    profile: str = Form(...),
    files: list[UploadFile] = File(...),
    ingest: bool = Form(default=True),
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Multipart drag-drop upload from Studio.

    Saves every file under ``~/.amx/uploads/<profile>/`` (content-
    addressed) and registers that directory on the doc profile so a
    follow-up scan/ingest picks it up. When ``ingest=true`` (the SPA's
    default) an ingest job is spawned right after the save and its
    ``job_id`` is returned so the SPA can subscribe to progress.

    Validation lives in :mod:`amx.docs.uploads` so the CLI's
    ``/doc-add`` shares the exact same accept-list and size caps.
    """
    profile_clean = (profile or "").strip()
    if not profile_clean:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile is required.",
        )
    # Auto-create the doc profile if the user uploads to a new name —
    # otherwise the wizard would force a separate "Add doc profile"
    # round-trip first, which defeats the point of drag-drop.
    if profile_clean not in cfg.doc_profiles:
        cfg.doc_profiles[profile_clean] = []

    payloads: list[tuple[str, bytes]] = []
    total = 0
    for upload in files:
        data = await upload.read()
        total += len(data)
        if total > MAX_BATCH_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=(f"Upload batch is over {MAX_BATCH_BYTES} bytes."),
            )
        payloads.append((upload.filename or "unnamed", data))

    try:
        results = save_uploaded_batch(cfg, profile_clean, payloads)
    except UploadError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    response: dict[str, Any] = {
        "profile": profile_clean,
        "saved": [
            {
                "name": r.original_name,
                "path": r.saved_path,
                "bytes": r.bytes_written,
                "duplicate": r.duplicate,
            }
            for r in results
        ],
        "count": len(results),
    }
    if ingest and results:
        # Re-use the existing ingest worker against the upload root
        # (which save_uploaded_batch already added to the profile's
        # paths). Passing the directory once is enough — scanner walks
        # children and picks up every dropped file.
        from pathlib import Path

        upload_root = str(Path(results[0].saved_path).parent)
        job = jobs.new_job("docs_ingest")
        thread = threading.Thread(
            target=_ingest_worker,
            args=(job, [upload_root], False, cfg, profile_clean),
            name=f"amx-docs-upload-ingest-{job.id}",
            daemon=True,
        )
        thread.start()
        response["job_id"] = job.id
    return response


@router.get("/search")
def search_docs(
    q: str,
    n: int = 5,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Synchronous embedding-only search (Chroma similarity, no LLM).
    The result set is small (default 5) and the call is cheap, so we
    don't bother with a job/SSE round trip — the SPA renders this
    directly when the user hits Enter in the search box."""
    query = (q or "").strip()
    if not query:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Query string is required.",
        )
    try:
        from amx.docs.rag import RAGStore
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"RAG dependencies not installed: {exc}",
        ) from exc
    # Apply the active doc profile's source filter so search results
    # respect the same scope as ``/run``. Without this, search would
    # leak chunks from other profiles' documents into the result list
    # regardless of which profile is active — confusing on its own and
    # arguably a small information leak in multi-profile setups.
    source_filters = cfg.effective_doc_paths() or None
    store = RAGStore(source_filters=source_filters)
    if store.doc_count == 0:
        return {"hits": [], "count": 0, "message": "RAG store is empty — run /ingest first."}
    hits = store.query(query, n_results=max(1, min(int(n), 25)))
    out = [
        {
            "source": str((hit.get("metadata") or {}).get("source", "unknown")),
            "distance": float(hit.get("distance", 0.0)),
            "preview": str(hit.get("text", ""))[:400],
        }
        for hit in hits
    ]
    return {"hits": out, "count": len(out)}


def _scan_worker(job: Job, paths: list[str]) -> None:
    with quiet_console():
        _scan_worker_body(job, paths)


def _scan_worker_body(job: Job, paths: list[str]) -> None:
    job.status = "running"
    emit(job.queue, "activity.added", {"idx": 0, "label": "Scanning sources"})
    emit(job.queue, "activity.begin", {"idx": 0})
    try:
        from amx.docs.scanner import scan_all_sources, total_size_mb

        scan_outcome = scan_all_sources(paths)
        # ``ScanResult`` exposes ``.documents`` and ``.failures``; if a
        # caller has stubbed the function (tests), it may still return
        # a bare list — ``getattr`` keeps both shapes working.
        documents = list(getattr(scan_outcome, "documents", scan_outcome) or [])
        failures = [
            {"path": src, "error": reason}
            for src, reason in (getattr(scan_outcome, "failures", None) or [])
        ]
        size = total_size_mb(documents)
        emit(
            job.queue,
            "scan.summary",
            {
                "total": len(documents),
                "size_mb": round(size, 2),
                "files": [
                    {
                        "path": d.path,
                        "size_kb": round(getattr(d, "size_bytes", 0) / 1024, 1),
                        "type": getattr(d, "source_type", ""),
                    }
                    for d in documents[:200]  # cap for SSE payload size
                ],
                "failures": failures,
            },
        )
        job.status = "done"
        job.summary = {
            "total": len(documents),
            "size_mb": round(size, 2),
            "failures": failures,
        }
        job.ended_at = time.time()
        emit(
            job.queue,
            "activity.complete",
            {"idx": 0, "detail": f"{len(documents)} docs · {size:.1f} MB"},
        )
        emit_terminal(job.queue, "job.done", {"summary": job.summary})
    except Exception as exc:
        log.exception("docs scan worker crashed")
        job.status = "failed"
        job.error = f"{exc.__class__.__name__}: {exc}"
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(job.queue, "job.failed", {"error": job.error})


def _ingest_worker(
    job: Job,
    paths: list[str],
    refresh: bool,
    cfg: AMXConfig | None = None,
    profile_name: str | None = None,
) -> None:
    with quiet_console():
        _ingest_worker_body(job, paths, refresh, cfg=cfg, profile_name=profile_name)


def _ingest_worker_body(
    job: Job,
    paths: list[str],
    refresh: bool,
    *,
    cfg: AMXConfig | None = None,
    profile_name: str | None = None,
) -> None:
    job.status = "running"
    emit(job.queue, "activity.added", {"idx": 0, "label": "Scanning"})
    emit(job.queue, "activity.begin", {"idx": 0})
    try:
        from amx.docs.rag import RAGStore
        from amx.docs.scanner import scan_all_sources, total_size_mb

        scan_outcome = scan_all_sources(paths)
        # ``ScanResult`` exposes ``.documents`` and ``.failures``; some
        # tests still stub a bare list, which ``getattr`` keeps working.
        documents = list(getattr(scan_outcome, "documents", scan_outcome) or [])
        scan_failures = [
            {"path": src, "error": reason}
            for src, reason in (getattr(scan_outcome, "failures", None) or [])
        ]
        size = total_size_mb(documents)
        emit(
            job.queue,
            "activity.complete",
            {"idx": 0, "detail": f"{len(documents)} docs · {size:.1f} MB"},
        )
        emit(job.queue, "activity.added", {"idx": 1, "label": "Ingesting into Chroma"})
        emit(job.queue, "activity.begin", {"idx": 1})

        store = RAGStore()
        # Iterate documents one at a time so we can poll ``job.cancel``
        # between docs and exit cleanly mid-batch. Mid-document
        # cancellation is intentionally NOT supported — interrupting a
        # Chroma upsert would leave the collection in a half-orphaned
        # state. One document of latency is the worst case.
        succeeded: list[str] = []
        failed_list: list[dict[str, str]] = []
        chunks_added = 0
        cancelled = False
        for idx, doc in enumerate(documents):
            if job.cancel.is_set():
                cancelled = True
                break
            single_summary = store.ingest([doc], refresh=bool(refresh))
            succeeded.extend(getattr(single_summary, "succeeded", None) or [])
            failed_list.extend(
                {"path": p, "error": r} for p, r in (getattr(single_summary, "failed", None) or [])
            )
            chunks_added += int(single_summary)
            if (idx + 1) % 5 == 0 or idx == len(documents) - 1:
                emit(
                    job.queue,
                    "ingest.progress",
                    {"done": idx + 1, "total": len(documents), "chunks": chunks_added},
                )
        succeeded_count = len(succeeded)
        status_label = "cancelled" if cancelled else "done"
        ingest_error: str | None = None
        if failed_list and not succeeded:
            ingest_error = "; ".join(f"{f['path']}: {f['error']}" for f in failed_list[:3])
        emit(
            job.queue,
            "ingest.summary",
            {
                "documents": len(documents),
                "chunks": chunks_added,
                "refresh": bool(refresh),
                "succeeded": succeeded_count,
                "failed": failed_list,
                "scan_failures": scan_failures,
                "cancelled": cancelled,
                "status": status_label,
            },
        )
        job.status = status_label  # type: ignore[assignment]
        job.summary = {
            "documents": len(documents),
            "chunks": chunks_added,
            "succeeded": succeeded_count,
            "failed": failed_list,
            "scan_failures": scan_failures,
            "cancelled": cancelled,
        }
        job.ended_at = time.time()
        if cfg is not None and profile_name:
            cfg.record_doc_profile_ingest(profile_name, error=ingest_error)
        detail = (
            f"{chunks_added} chunks ingested ({succeeded_count}/{len(documents)} files) — cancelled"
            if cancelled
            else f"{chunks_added} chunks ingested"
        )
        emit(job.queue, "activity.complete", {"idx": 1, "detail": detail})
        terminal_event = "job.cancelled" if cancelled else "job.done"
        emit_terminal(job.queue, terminal_event, {"summary": job.summary})
    except Exception as exc:
        log.exception("docs ingest worker crashed")
        job.status = "failed"
        job.error = f"{exc.__class__.__name__}: {exc}"
        job.ended_at = time.time()
        if cfg is not None and profile_name:
            cfg.record_doc_profile_ingest(profile_name, error=job.error)
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(job.queue, "job.failed", {"error": job.error})
