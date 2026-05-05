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
        ),
        name=f"amx-code-scan-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status, "path": path}


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

        # 2. Run the analyzer.
        report = analyze_codebase(
            path,
            table_names=table_names,
            column_names=column_names or None,
        )

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
