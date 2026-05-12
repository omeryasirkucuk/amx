"""Code scan + results routes.

Wraps :func:`amx.codebase.analyzer.analyze_codebase` so AMX Studio
can drive a code scan with the same shape the CLI's ``/code-scan``
runs. Output is JSON: per-asset reference counts + a sample of file
hits. Heavier than docs scan because the analyzer walks every source
file under the codebase root, so the worker streams progress over the
existing SSE bus.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field

from amx.config import AMXConfig
from amx.utils.console import quiet_console
from amx.utils.logging import get_logger
from amx.web.deps import get_cfg, get_jobs
from amx.web.jobs import Job, JobRegistry
from amx.web.progress_bus import emit, emit_terminal

router = APIRouter(prefix="/api/code", tags=["code"])
log = get_logger("web.code_ops")


class _CodeScanRequest(BaseModel):
    """Body for ``POST /api/code/scan``.

    Path resolution precedence: ``path`` > ``profile`` > active code
    profile. Same fallback chain the CLI's ``/code-scan`` uses.

    DB enumeration scope (``db_profile`` / ``db_database`` /
    ``db_catalog``): the worker walks tables/columns to know which
    strings in source code are DB references. Set these to scope the
    enumeration to a specific DB profile without flipping the active
    one. Omitted → legacy single-active behaviour.
    """

    model_config = ConfigDict(populate_by_name=True)

    path: str | None = None
    profile: str | None = None
    # ``schema`` shadows BaseModel's ``schema`` method — use a private
    # field with an alias so request bodies still send {"schema": "x"}.
    schema_: str | None = Field(default=None, alias="schema")
    column_scan: bool = False  # include column names — slower but richer report
    db_profile: str | None = None
    db_database: str | None = None
    db_catalog: str | None = None


def _resolve_path(body: _CodeScanRequest, cfg: AMXConfig) -> str:
    if body.path:
        return body.path.strip()
    profile = (body.profile or "").strip() or (cfg.active_code_profile or "").strip()
    if not profile:
        return ""
    return str(cfg.code_profiles.get(profile, "") or "")


@router.post("/jobs/{job_id}/cancel")
def cancel_job(
    job_id: str,
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Signal an in-flight code scan worker to stop.

    The worker polls ``job.cancel`` between files (never mid-file —
    that would orphan a partial Chroma upsert until the idempotent
    delete lands in a follow-up PR). Returns 200 when the job exists
    and the flag is set, 404 when the job id is unknown.
    """
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found.",
        )
    job.cancel.set()
    return {"job_id": job_id, "cancelled": True, "status": job.status}


@router.post("/scan")
def submit_scan(
    body: _CodeScanRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn a code-scan job. Returns the job id immediately so the SPA
    can subscribe to the SSE event stream."""
    path = _resolve_path(body, cfg)
    if not path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No path to scan. Pass path=, profile=, or activate a code profile.",
        )
    # Resolve the profile name used for cache + catalog sync so the
    # subsequent ``/api/code/analyze`` and ``/api/code/search`` calls
    # can find the persisted report. Matches the CLI's fallback chain
    # in ``commands/code.py`` so the two surfaces share a cache key.
    profile_name = (
        (body.profile or "").strip() or (cfg.active_code_profile or "").strip() or "default"
    )
    job = jobs.new_job("code_scan")
    thread = threading.Thread(
        target=_scan_worker,
        args=(
            cfg,
            job,
            path,
            body.schema_,
            body.column_scan,
            body.db_profile,
            body.db_database,
            body.db_catalog,
            profile_name,
        ),
        name=f"amx-code-scan-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status, "path": path}


@router.get("/search")
def search_code(
    q: str,
    n: int = 5,
    profile: list[str] = Query(default_factory=list),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Synchronous embedding-only search over the ``amx_code`` Chroma
    index — the Studio counterpart to ``GET /api/docs/search``. No LLM
    call; the SPA renders results directly.

    ``profile`` is repeatable: ``?profile=foo&profile=bar`` searches
    the union of those code profiles' source paths. When the parameter
    is omitted, the search falls back to ``cfg.effective_code_paths()``
    — i.e. the active code profile — so search hits stay scoped to
    whatever the user has selected, matching how ``/ask`` resolves
    code context. Pass an explicit list to override.
    """
    query = (q or "").strip()
    if not query:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Query string is required.",
        )
    try:
        from amx.codebase.code_rag import code_collection_count, query_code_snippets
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Code RAG dependencies not installed: {exc}",
        ) from exc

    profile_names = [p.strip() for p in (profile or []) if p and p.strip()]
    source_filters: list[str] = []
    explicit_profiles = bool(profile_names)
    if explicit_profiles:
        for name in profile_names:
            if name not in cfg.code_profiles:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Unknown code profile {name!r}.",
                )
            path = (cfg.code_profiles.get(name) or "").strip()
            if path and path not in source_filters:
                source_filters.append(path)
    else:
        # Default to the active code profile's paths — same scope rule
        # as ``/api/docs/search`` uses via ``effective_doc_paths()``.
        for path in cfg.effective_code_paths() or []:
            cleaned = (path or "").strip()
            if cleaned and cleaned not in source_filters:
                source_filters.append(cleaned)

    if code_collection_count(source_filters=source_filters or None) == 0:
        if explicit_profiles:
            label = ", ".join(profile_names)
            message = f"No indexed code for profile {label!r} — run /code-scan first."
        else:
            message = "amx_code index is empty — run /code-scan first."
        return {"hits": [], "count": 0, "message": message}

    hits = query_code_snippets(
        query,
        n_results=max(1, min(int(n), 25)),
        source_filters=source_filters or None,
    )
    out: list[dict[str, Any]] = []
    for hit in hits:
        meta = hit.get("metadata") or {}
        out.append(
            {
                "source": str(meta.get("source") or meta.get("rel_path") or "unknown"),
                "rel_path": str(meta.get("rel_path") or ""),
                "symbol": str(meta.get("symbol") or meta.get("kind") or ""),
                "distance": float(hit.get("distance") or 0.0),
                "preview": str(hit.get("text") or "")[:400],
            }
        )
    return {"hits": out, "count": len(out)}


class _CodeAnalyzeRequest(BaseModel):
    """Body for ``POST /api/code/analyze``.

    Tables are listed explicitly (no schema-only mode) so the Studio
    user has to commit to a scope before paying the per-table LLM cost.
    ``code_profile`` and ``db_profile`` are optional — fall back to the
    active profiles when omitted, mirroring the CLI's ``/code-analyze``.
    """

    model_config = ConfigDict(populate_by_name=True)

    schema_: str = Field(alias="schema")
    tables: list[str] = Field(..., min_length=1, max_length=20)
    code_profile: str | None = None
    db_profile: str | None = None


@router.post("/analyze")
def submit_analyze(
    body: _CodeAnalyzeRequest,
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn a Code Agent worker and return the job id.

    Studio's Code → Analyze page subscribes to the SSE stream so the
    user sees per-table progress. The worker uses
    :func:`amx.codebase.agent_service.run_code_analysis` — the same
    loop the CLI's ``/code-analyze`` runs. Result is the union of every
    table's suggestions, persisted under ``~/.amx/code_agent_results.json``
    just like the CLI does.
    """
    if not cfg.llm.provider or not cfg.llm.model:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail={
                "message": (
                    "No active LLM profile. Open Settings → LLM and pick "
                    "a provider before running Code Analyze."
                ),
                "hint": "configure-llm",
            },
        )
    code_profile = (body.code_profile or "").strip() or (cfg.active_code_profile or "").strip()
    if not code_profile or code_profile not in cfg.code_profiles:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "No code profile selected. Pick one under Settings → Code, "
                "or pass `code_profile` in the request body."
            ),
        )
    job = jobs.new_job("code_analyze")
    thread = threading.Thread(
        target=_analyze_worker,
        args=(
            cfg,
            job,
            body.schema_,
            list(body.tables),
            code_profile,
            body.db_profile,
        ),
        name=f"amx-code-analyze-{job.id}",
        daemon=True,
    )
    thread.start()
    return {
        "job_id": job.id,
        "status": job.status,
        "schema": body.schema_,
        "tables": list(body.tables),
        "code_profile": code_profile,
    }


def _analyze_worker(
    cfg: AMXConfig,
    job: Job,
    schema: str,
    tables: list[str],
    code_profile: str,
    db_profile: str | None,
) -> None:
    """Run the analyze loop in a background thread, streaming progress."""
    job.status = "running"
    try:
        from amx.codebase.agent_service import (
            CodeAnalyzeRequest,
            run_code_analysis,
            serialize_suggestions,
        )
        from amx.codebase.cache import load_latest_cached_report
        from amx.db.connector import DatabaseConnector
        from amx.llm.provider import LLMProvider

        code_path = cfg.code_profiles.get(code_profile, "") or ""
        if not code_path:
            raise RuntimeError(f"Code profile {code_profile!r} has no path configured.")

        _, report = load_latest_cached_report(code_profile, code_path)
        if report is None:
            raise RuntimeError(f"No cached code-scan for {code_profile!r}. Run /code-scan first.")

        # Allow a body override for the DB the analyze runs against —
        # otherwise the active profile is used. The Studio request shape
        # mirrors the multi-profile pattern other endpoints adopted.
        if db_profile and db_profile in cfg.db_profiles:
            from amx.web.routers.live_db import _connector_for_scope

            db = _connector_for_scope(cfg, db_profile)
        else:
            db = DatabaseConnector(cfg.db)
        llm = LLMProvider(cfg.llm)

        for idx, table in enumerate(tables):
            emit(
                job.queue,
                "activity.added",
                {"idx": idx, "label": f"Analyzing {schema}.{table}"},
            )

        def _on_start(table_name: str, n_columns: int) -> None:
            emit(
                job.queue,
                "activity.begin",
                {
                    "idx": tables.index(table_name),
                    "detail": f"{n_columns} columns",
                },
            )

        def _on_done(table_name: str, n_suggestions: int) -> None:
            emit(
                job.queue,
                "activity.complete",
                {
                    "idx": tables.index(table_name),
                    "detail": f"{n_suggestions} suggestions",
                },
            )

        result = run_code_analysis(
            cfg,
            db,
            llm,
            CodeAnalyzeRequest(
                schema=schema,
                tables=tables,
                code_profile=code_profile,
                code_report=report,
            ),
            on_table_start=_on_start,
            on_table_done=_on_done,
        )

        # Persist to the same on-disk cache the CLI writes so /run picks
        # up the suggestions from either surface.
        import json as _json
        from pathlib import Path

        payload = serialize_suggestions(result.suggestions)
        cache_path = Path.home() / ".amx" / "code_agent_results.json"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(_json.dumps(payload, indent=2), encoding="utf-8")

        summary = {
            "schema": schema,
            "code_profile": code_profile,
            "by_table": result.by_table,
            "suggestions_total": len(payload),
            "suggestions": payload[:200],  # bound the SSE payload
        }
        job.status = "done"
        job.summary = summary
        job.ended_at = time.time()
        emit_terminal(job.queue, "job.done", {"summary": summary})
        try:
            db.close()
        except Exception:
            pass
    except Exception as exc:
        log.exception("code analyze worker crashed")
        job.status = "failed"
        job.error = f"{exc.__class__.__name__}: {exc}"
        job.ended_at = time.time()
        emit_terminal(job.queue, "job.failed", {"error": job.error})


@router.get("/results/{job_id}")
def get_results(job_id: str, jobs: JobRegistry = Depends(get_jobs)) -> dict[str, Any]:
    """Read the persisted scan result for a finished job. The SPA
    calls this once the SSE stream emits ``job.done`` so the page can
    render without rebuilding from event history.
    """
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No scan job {job_id}.",
        )
    if job.status != "done":
        return {"status": job.status, "ready": False}
    return {"status": job.status, "ready": True, **(job.summary or {})}


def _scan_worker(
    cfg: AMXConfig,
    job: Job,
    path: str,
    schema_filter: str | None,
    column_scan: bool,
    db_profile: str | None = None,
    db_database: str | None = None,
    db_catalog: str | None = None,
    profile_name: str = "default",
) -> None:
    with quiet_console():
        _scan_worker_body(
            cfg,
            job,
            path,
            schema_filter,
            column_scan,
            db_profile,
            db_database,
            db_catalog,
            profile_name,
        )


def _scan_worker_body(
    cfg: AMXConfig,
    job: Job,
    path: str,
    schema_filter: str | None,
    column_scan: bool,
    db_profile: str | None = None,
    db_database: str | None = None,
    db_catalog: str | None = None,
    profile_name: str = "default",
) -> None:
    job.status = "running"
    emit(job.queue, "activity.added", {"idx": 0, "label": "Collecting catalog assets"})
    emit(job.queue, "activity.begin", {"idx": 0})
    try:
        from amx.codebase.analyzer import analyze_codebase
        from amx.db.connector import DatabaseConnector

        # 1. Collect table (and optionally column) names from the active
        #    DB. analyze_codebase needs these to know which strings in
        #    source files count as references.
        table_names: list[str] = []
        column_names: list[str] = []
        try:
            scope = (db_profile or "").strip()
            if scope:
                from amx.web.routers.live_db import _connector_for_scope

                db = _connector_for_scope(cfg, scope, database=db_database, catalog=db_catalog)
            else:
                db = DatabaseConnector(cfg.db)
        except Exception as exc:
            raise RuntimeError(
                f"Cannot open DB profile to enumerate tables for scan: {exc}"
            ) from exc
        try:
            schemas = [schema_filter] if schema_filter else list(db.list_schemas())
            for sch in schemas:
                try:
                    for tbl in db.list_tables(sch):
                        table_names.append(str(tbl))
                        if column_scan:
                            try:
                                for col in db.list_column_profiles(sch, tbl):
                                    column_names.append(str(col.name))
                            except Exception:
                                # One missing column listing shouldn't kill the scan.
                                pass
                except Exception as exc:
                    log.debug("list_tables(%s) failed: %s", sch, exc)
        finally:
            try:
                db.close()
            except Exception:
                pass

        emit(
            job.queue,
            "activity.complete",
            {"idx": 0, "detail": f"{len(table_names)} table(s), {len(column_names)} column(s)"},
        )
        emit(job.queue, "activity.added", {"idx": 1, "label": f"Scanning {path}"})
        emit(job.queue, "activity.begin", {"idx": 1})

        # Per-file progress callback. ``analyze_codebase`` calls this
        # with ``("__total__", total_files)`` once, then
        # ``("__advance__", file_name)`` for each scanned file. Bridge
        # those events to the SSE bus so the SPA renders per-file
        # progress instead of a single "Scanning…" line — and use the
        # same hook to poll ``job.cancel`` between files (never
        # mid-file, to avoid orphaning a partial Chroma upsert).
        progress_state: dict[str, int] = {"total": 0, "processed": 0}
        cancelled = False

        class _Cancelled(RuntimeError):
            pass

        def _scan_progress(action: str, value: object) -> None:
            nonlocal cancelled
            if action == "__total__":
                progress_state["total"] = int(value or 0)
                emit(
                    job.queue,
                    "code.scan.progress",
                    {
                        "file_path": "",
                        "processed_count": 0,
                        "total_count": progress_state["total"],
                    },
                )
                return
            if action == "__advance__":
                # Poll cancellation BETWEEN files — raise out of the
                # analyzer so the in-progress walk stops cleanly. The
                # outer ``except _Cancelled`` finalizes a cancelled
                # summary; any partial work is left for the operator
                # to clear with ``/code-refresh`` (the same recovery
                # path the docs ingest cancellation uses today).
                if job.cancel.is_set():
                    cancelled = True
                    raise _Cancelled()
                progress_state["processed"] += 1
                emit(
                    job.queue,
                    "code.scan.progress",
                    {
                        "file_path": str(value or ""),
                        "processed_count": progress_state["processed"],
                        "total_count": progress_state["total"] or progress_state["processed"],
                    },
                )

        # 2. Run the analyzer with semantic indexing so the ``amx_code``
        # Chroma collection is populated — Studio /code-scan reached
        # functional parity with the CLI here. Without this, /ask's
        # ``search_code`` tool and /api/code/search return empty.
        try:
            report = analyze_codebase(
                path,
                table_names=table_names,
                column_names=column_names or None,
                index_semantic=True,
                progress_callback=_scan_progress,
            )
        except _Cancelled:
            # User cancelled mid-walk. Emit a cancelled summary and
            # exit cleanly — no exception trail in the SSE stream.
            scan_summary = {
                "path": path,
                "total_files": progress_state.get("total", 0),
                "scanned_files": progress_state.get("processed", 0),
                "catalog_assets": 0,
                "external_assets": 0,
                "catalog": [],
                "external": [],
                "cancelled": True,
                "status": "cancelled",
            }
            emit(
                job.queue,
                "code.scan.summary",
                {
                    "scanned_files": scan_summary["scanned_files"],
                    "catalog_assets": 0,
                    "external_assets": 0,
                    "cancelled": True,
                    "status": "cancelled",
                },
            )
            emit(
                job.queue,
                "activity.complete",
                {"idx": 1, "detail": "scan cancelled"},
            )
            job.status = "cancelled"
            job.summary = scan_summary
            job.ended_at = time.time()
            emit_terminal(job.queue, "job.cancelled", {"summary": scan_summary})
            return

        # 3. Compact the report into something the SPA can render.
        # analyze_codebase emits per-asset CodeReference objects; we
        # surface the count + first 5 hits so the JSON stays bounded
        # even on very large repos.
        def _ref_to_dict(r: Any) -> dict[str, Any]:
            # Tolerate both real CodeReference dataclass instances and
            # any duck-typed object so tests can pass MagicMocks.
            return {
                "file": str(getattr(r, "file", "") or ""),
                "line_no": int(getattr(r, "line_no", 0) or 0),
                "line_text": str(getattr(r, "line_text", "") or ""),
                "matched_asset": str(getattr(r, "matched_asset", "") or ""),
                "context": str(getattr(r, "context", "") or ""),
            }

        def _shape(refs_dict: dict[str, list[Any]]) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for asset, refs in sorted(refs_dict.items()):
                out.append(
                    {
                        "asset": asset,
                        "count": len(refs),
                        "samples": [_ref_to_dict(r) for r in refs[:5]],
                    }
                )
            return sorted(out, key=lambda r: -r["count"])

        catalog_refs = _shape(getattr(report, "references", {}))
        external_refs = _shape(getattr(report, "external_mentions", {}))
        scan_summary = {
            "path": path,
            "total_files": int(getattr(report, "total_files", 0) or 0),
            "scanned_files": int(getattr(report, "scanned_files", 0) or 0),
            "catalog_assets": len(catalog_refs),
            "external_assets": len(external_refs),
            "catalog": catalog_refs[:200],
            "external": external_refs[:200],
        }

        emit(
            job.queue,
            "code.summary",
            {
                "scanned_files": scan_summary["scanned_files"],
                "catalog_assets": scan_summary["catalog_assets"],
                "external_assets": scan_summary["external_assets"],
            },
        )

        # Persist the cached report so a follow-up ``/api/code/analyze``
        # (and the CLI's ``/code-results``) finds it under the same
        # slug the CLI writes. Without this step, Studio scans never
        # populate the cache and downstream code-analyze 500s with
        # "No cached code-scan".
        # Cache manifests need a single schema label. ``schema_filter``
        # wins when set; otherwise fall back to ``cfg.current_schema``
        # — and finally to the literal "all" so a multi-schema scan
        # still produces a stable, reload-able cache key.
        schema_for_cache = (
            (schema_filter or "").strip() or (cfg.current_schema or "").strip() or "all"
        )
        try:
            from amx.codebase.cache import save_cached_report

            save_cached_report(
                profile_name=profile_name,
                source_path=path,
                schema=schema_for_cache,
                tables=table_names,
                column_names=column_names,
                report=report,
            )
        except Exception as exc:
            log.warning("Could not save code-scan cache: %s", exc)

        # Sync the report into the search catalog so /search surfaces
        # the same code-evidence rows the CLI's ``/code-scan`` writes.
        try:
            from amx.search.catalog import SearchCatalog

            catalog_store = SearchCatalog.from_history_store()
            if catalog_store is not None:
                catalog_store.sync_code_report(
                    db_profile=cfg.active_db_profile or "default",
                    db_backend=cfg.db.backend,
                    database_name=(cfg.db.database or cfg.db.catalog or cfg.db.project or ""),
                    schema_name=schema_for_cache,
                    source_path=path,
                    report=report,
                )
        except Exception as exc:
            log.warning("Could not sync code report into /search catalog: %s", exc)

        emit(
            job.queue,
            "activity.complete",
            {
                "idx": 1,
                "detail": (
                    f"{scan_summary['scanned_files']} files · "
                    f"{scan_summary['catalog_assets']} catalog assets"
                ),
            },
        )

        job.status = "done"
        job.summary = scan_summary
        job.ended_at = time.time()
        emit_terminal(job.queue, "job.done", {"summary": scan_summary})
    except Exception as exc:
        log.exception("code scan worker crashed")
        job.status = "failed"
        job.error = f"{exc.__class__.__name__}: {exc}"
        job.ended_at = time.time()
        emit(job.queue, "activity.fail", {"idx": 0, "detail": job.error})
        emit_terminal(job.queue, "job.failed", {"error": job.error})


__all__ = ["router"]


# Suppress unused-import warning on Query — kept for forward-compat
# when we add filter knobs (limit, sort, etc.) without re-importing.
_ = Query
