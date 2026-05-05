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

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from amx.config import AMXConfig
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
    job = jobs.new_job("docs_ingest")
    thread = threading.Thread(
        target=_ingest_worker,
        args=(job, paths, bool(body.refresh)),
        name=f"amx-docs-ingest-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status, "paths": paths, "refresh": bool(body.refresh)}


@router.get("/search")
def search_docs(q: str, n: int = 5) -> dict[str, Any]:
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
    store = RAGStore()
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

        documents = scan_all_sources(paths)
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
            },
        )
        job.status = "done"
        job.summary = {"total": len(documents), "size_mb": round(size, 2)}
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


def _ingest_worker(job: Job, paths: list[str], refresh: bool) -> None:
    with quiet_console():
        _ingest_worker_body(job, paths, refresh)


def _ingest_worker_body(job: Job, paths: list[str], refresh: bool) -> None:
    job.status = "running"
    emit(job.queue, "activity.added", {"idx": 0, "label": "Scanning"})
    emit(job.queue, "activity.begin", {"idx": 0})
    try:
        from amx.docs.rag import RAGStore
        from amx.docs.scanner import scan_all_sources, total_size_mb

        documents = scan_all_sources(paths)
        size = total_size_mb(documents)
        emit(
            job.queue,
            "activity.complete",
            {"idx": 0, "detail": f"{len(documents)} docs · {size:.1f} MB"},
        )
        emit(job.queue, "activity.added", {"idx": 1, "label": "Ingesting into Chroma"})
        emit(job.queue, "activity.begin", {"idx": 1})

        store = RAGStore()
        chunks_added = store.ingest(documents, refresh=bool(refresh))
        emit(
            job.queue,
            "ingest.summary",
            {
                "documents": len(documents),
                "chunks": int(chunks_added),
                "refresh": bool(refresh),
            },
        )
        job.status = "done"
        job.summary = {"documents": len(documents), "chunks": int(chunks_added)}
        job.ended_at = time.time()
        emit(
            job.queue,
            "activity.complete",
            {"idx": 1, "detail": f"{chunks_added} chunks ingested"},
        )
        emit_terminal(job.queue, "job.done", {"summary": job.summary})
    except Exception as exc:
        log.exception("docs ingest worker crashed")
        job.status = "failed"
        job.error = f"{exc.__class__.__name__}: {exc}"
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(job.queue, "job.failed", {"error": job.error})
