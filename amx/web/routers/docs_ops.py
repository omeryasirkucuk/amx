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


@router.post("/index")
def submit_index(
    body: _DocPathsRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Build / refresh the doc RAG index for a profile under the active
    embedding model. One smart, idempotent operation: it ingests new and
    changed files incrementally, and — when the embedding model has
    changed (a stale collection identity) — drops and rebuilds the
    collection so it is re-stamped with the active provider/model. This is
    the single replacement for the old scan / ingest / reindex verbs.
    Streams the same SSE shape, ending with an ``ingest.summary`` event.
    """
    paths = _resolve_paths(body, cfg)
    if not paths:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No paths to index. Pass paths=[...], profile=<name>, or activate a doc profile.",
        )
    profile_name = (body.profile or "").strip() or (cfg.active_doc_profile or "").strip()
    job = jobs.new_job("docs_ingest")
    thread = threading.Thread(
        target=_index_worker,
        args=(job, paths, cfg, profile_name),
        name=f"amx-docs-index-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status, "paths": paths}


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
        # Re-use the smart index worker against the upload root (which
        # save_uploaded_batch already added to the profile's paths).
        # Passing the directory once is enough — the scanner walks children
        # and picks up every dropped file.
        from pathlib import Path

        upload_root = str(Path(results[0].saved_path).parent)
        job = jobs.new_job("docs_ingest")
        thread = threading.Thread(
            target=_index_worker,
            args=(job, [upload_root], cfg, profile_clean),
            name=f"amx-docs-upload-index-{job.id}",
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


def _index_worker(
    job: Job,
    paths: list[str],
    cfg: AMXConfig | None = None,
    profile_name: str | None = None,
) -> None:
    with quiet_console():
        _index_worker_body(job, paths, cfg=cfg, profile_name=profile_name)


def _open_docs_store() -> tuple[Any, bool]:
    """Open the docs RAG store, recovering from a stale embedding identity.

    A model swap (e.g. minilm → gte-small) leaves the on-disk ``amx_docs``
    collection stamped with the old identity, so ``RAGStore()`` raises
    ``EmbeddingProviderMismatch`` on open. We force-drop the collection by
    hand and reconstruct so it is re-stamped with the active
    provider/model/dim.

    Returns ``(store, mismatch_recovered)``. ``mismatch_recovered`` is True
    when the collection had to be dropped — the caller then does a full
    rebuild; otherwise it ingests incrementally into the existing
    collection.
    """
    from pathlib import Path

    from amx.docs.rag import EmbeddingProviderMismatch, RAGStore

    try:
        return RAGStore(), False
    except EmbeddingProviderMismatch:
        import chromadb

        client = chromadb.PersistentClient(path=str(Path.home() / ".amx" / "chroma_db"))
        try:
            client.delete_collection(name="amx_docs")
        except Exception as exc:  # noqa: BLE001 - already absent is fine
            log.debug("index: delete_collection(amx_docs) skipped: %s", exc)
        return RAGStore(), True


def _index_worker_body(
    job: Job,
    paths: list[str],
    *,
    cfg: AMXConfig | None = None,
    profile_name: str | None = None,
) -> None:
    job.status = "running"
    emit(job.queue, "activity.added", {"idx": 0, "label": "Scanning"})
    emit(job.queue, "activity.begin", {"idx": 0})
    try:
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
        # Smart index: open the store, recovering from a stale embedding
        # identity. On a model change the collection is dropped + rebuilt
        # under the active model; otherwise we ingest incrementally into
        # the existing collection. One safe, idempotent operation.
        store, mismatch = _open_docs_store()
        emit(
            job.queue,
            "activity.added",
            {"idx": 1, "label": "Rebuilding index" if mismatch else "Indexing into Chroma"},
        )
        emit(job.queue, "activity.begin", {"idx": 1})
        if mismatch:
            # Embedding model changed — clear the collection + FTS5 sidecar
            # so the rebuild is stamped with the active provider/model.
            store.reset_collection()
        # refresh=False: incremental add when the identity matched, full
        # rebuild when we just reset to an empty collection.
        refresh = False
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
