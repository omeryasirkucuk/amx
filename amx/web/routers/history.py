"""Run history routes — what the SPA's ``/runs`` page reads.

Wraps :class:`amx.storage.sqlite_store.SQLiteHistoryStore` (or the
shared-mode ``DualWriteHistoryStore``). Every endpoint returns plain
dicts pre-serialized by the store; we deliberately don't impose a
pydantic model so adding a column to the underlying table doesn't
require a SPA-side migration.

PR-D adds a comparison endpoint backed by the existing
``/history compare`` Click command's payload assembler — that
refactor lives in ``amx/cli_support/commands/compare.py``.
"""

from __future__ import annotations

import io
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from amx.storage.sqlite_store import history_store
from amx.web.deps import get_jobs
from amx.web.jobs import JobRegistry

router = APIRouter(prefix="/api/history", tags=["history"])


def _store() -> Any:
    """Return the active history-store singleton or 503 if absent.

    The store is initialized at CLI startup; if it isn't ready yet
    (e.g. the user typed /studio before any DB profile activated)
    we surface a clean 503 with a hint instead of a 500 on
    ``store.list_recent_runs``.
    """
    store = history_store()
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "History store isn't initialized yet. Activate a DB profile "
                "(or run /history-store enable for shared mode) and reload."
            ),
        )
    return store


@router.get("/runs")
def list_recent_runs(
    limit: int = Query(default=20, ge=1, le=200),
    command: str | None = Query(default="analyze.run"),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Most-recent runs filtered by command. ``command=all`` includes
    every kind (analyze.run + search.ask + …) so the SPA's "All
    activity" view can render them together.

    Each row carries a ``live_job_id`` when its worker thread is still
    alive in the registry. The Studio uses that id to render an inline
    Cancel control on the running rows — without it, the only way to
    stop a stuck worker was to know the SSE job id off-hand and POST
    ``/api/runs/{job}/cancel`` by hand.
    """
    cmd_filter = None if (command or "").strip().lower() in {"", "all"} else command
    rows = _store().list_recent_runs(limit=limit, command_filter=cmd_filter)
    # Build a {run_id: job_id} index in O(active jobs) so the per-row
    # lookup below is O(1). Apply / rerun jobs are skipped — only the
    # primary "run" worker maps to an analyze.run / rerun row id.
    live_by_run_id: dict[int, str] = {}
    for job in jobs.list():
        if job.kind != "run":
            continue
        if job.status not in ("queued", "running"):
            continue
        if job.run_id is None:
            continue
        live_by_run_id[int(job.run_id)] = job.id
    if live_by_run_id:
        for row in rows:
            rid = row.get("id") if isinstance(row, dict) else None
            if rid is not None and int(rid) in live_by_run_id:
                row["live_job_id"] = live_by_run_id[int(rid)]
    return {"command_filter": cmd_filter, "runs": rows, "count": len(rows)}


@router.get("/runs/{run_id}")
def get_run(
    run_id: int,
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Full row + parsed JSON payloads (scope, metrics, tokens,
    results, settings) for one run.

    ``live_job_id`` is non-null when this run still has a worker
    thread alive in the job registry — the SPA uses it to subscribe
    to ``/api/runs/{job_id}/events`` and stream per-asset progress
    while the user is on the run-detail page (otherwise a
    long-running run looks frozen until the worker exits).
    """
    row = _store().get_run(run_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No run with id {run_id}.",
        )
    live_job_id: str | None = None
    for job in jobs.list():
        if job.kind == "run" and job.run_id == run_id and job.status in ("queued", "running"):
            live_job_id = job.id
            break
    row = dict(row)
    row["live_job_id"] = live_job_id
    # Surface the run's effective database / catalog at the top level so
    # the SPA can pin /api/pending/apply to the same scope without
    # parsing settings_json on the client. Falls back to the legacy
    # nested location for older runs that didn't capture it yet.
    settings = row.get("settings_json")
    if isinstance(settings, dict):
        if row.get("database") in (None, ""):
            row["database"] = settings.get("database") or None
        if row.get("catalog") in (None, ""):
            row["catalog"] = settings.get("catalog") or None
    return row


@router.get("/runs/{run_id}/results")
def get_run_results(
    run_id: int,
    unevaluated_only: bool = Query(default=False),
    include_history: bool = Query(
        default=False,
        description=(
            "When true, attach the full re-run chain (original + every "
            "child re-run row) to each result under ``history``. Used by "
            "the Studio history drawer to render v1/v2/v3 side-by-side."
        ),
    ),
) -> dict[str, Any]:
    """Per-column LLM suggestions saved during this run. Used by the
    run-detail page's results table + the column drill page's
    "alternatives" carousel."""
    store = _store()
    rows = store.get_run_results(run_id, unevaluated_only=unevaluated_only)
    if include_history:
        for row in rows:
            try:
                row["history"] = store.get_result_chain(int(row["id"]))
            except Exception:
                row["history"] = []
    return {"run_id": run_id, "results": rows, "count": len(rows)}


@router.get("/results/{result_id}/history")
def get_result_history(result_id: int) -> dict[str, Any]:
    """Return the full re-run chain (original + every child re-run row).

    Used by the Studio history drawer when the user clicks a "v2"
    badge on a specific row — cheaper than fetching every result for
    the whole run.
    """
    chain = _store().get_result_chain(int(result_id))
    return {"result_id": result_id, "chain": chain, "count": len(chain)}


@router.get("/runs/{run_id}/results-with-counts")
def list_runs_with_result_counts(
    limit: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    """Recent runs + their pending evaluation counts in one query —
    cheaper than asking for each run's results individually when the
    SPA is rendering the runs index."""
    rows = _store().list_runs_with_result_counts(limit=limit)
    return {"runs": rows, "count": len(rows)}


@router.get("/runs-by-scope")
def find_runs_for_scope(
    schema: str | None = Query(default=None),
    table: str | None = Query(default=None),
    command: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    """Runs that touched a given asset — what the table-detail page's
    "History" tab fetches."""
    rows = _store().find_runs_for_scope(
        schema=schema,
        table=table,
        command_filter=command,
        limit=limit,
    )
    return {
        "filter": {"schema": schema, "table": table, "command": command},
        "runs": rows,
        "count": len(rows),
    }


@router.get("/stats")
def stats(
    command: str | None = Query(default="analyze.run"),
) -> dict[str, Any]:
    """Aggregate counters the SPA renders as dashboard cards.

    ``command`` defaults to ``"analyze.run"`` so AMX Studio's
    "Total runs" / "Success rate" tiles match the Recent runs feed
    (which only shows /run invocations). Pass ``command=all`` to
    include every kind (analyze + ask + apply + …).
    """
    cmd_filter = None if (command or "").strip().lower() in {"", "all"} else command
    return _store().stats(command_filter=cmd_filter)


@router.get("/events")
def list_recent_events(
    limit: int = Query(default=30, ge=1, le=200),
) -> dict[str, Any]:
    """Audit-log events the SPA renders in a side strip (CLI calls,
    config edits, profile activations)."""
    rows = _store().list_recent_events(limit=limit)
    return {"events": rows, "count": len(rows)}


@router.get("/apply-events")
def list_apply_events(
    run_id: int | None = Query(default=None, description="Filter to a specific run."),
    profile_name: str | None = Query(default=None, description="Filter to a specific DB profile."),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    """Apply-events audit trail.

    One row per successful COMMENT write; populated by
    ``apply_review_results_to_db`` (CLI ``/analyze apply``, Studio
    ``/api/runs/{id}/apply``). Newest-first, optional filters by run
    or profile so the Recent Applies panel can pivot. Falls back to
    a 503 when the store isn't initialised yet — the SPA shows the
    same hint as the other /api/history/* endpoints.
    """
    rows = _store().list_apply_events(run_id=run_id, profile_name=profile_name, limit=limit)
    return {"events": rows, "count": len(rows)}


class CompareRequest(BaseModel):
    run_ids: list[int] = Field(..., min_length=1, description="Runs to compare side-by-side.")
    quality_tier: int = Field(
        default=0,
        ge=0,
        le=2,
        description=(
            "0 = Tier 0 only (offline metrics: chrF, ROUGE-L, schema "
            "grounding); 1 = + Tier 1 local embeddings; 2 = + Tier 2 "
            "LLM-as-judge (cost). Default 0 keeps the response cheap "
            "for the auto-fired modal load; the Studio 'Run deeper "
            "analysis' button posts to /compare/deep-analysis with 2."
        ),
    )
    ground_truth_run_id: int | None = Field(
        default=None,
        description=(
            "Pin one of the resolved runs as the ground-truth baseline "
            "for reference-based metrics (chrF / ROUGE-L / Levenshtein). "
            "Overrides the live DB COMMENT → catalog-applied → none "
            "waterfall."
        ),
    )


@router.post("/compare")
def compare(body: CompareRequest) -> dict[str, Any]:
    """Side-by-side run comparison payload. Mirrors what the CLI's
    ``/history compare`` command produces; the web UI uses the same
    helper so both surfaces stay in sync.

    ``quality_tier`` defaults to 0 (offline metrics only) so the modal
    auto-load stays cheap. Studio's "Run deeper analysis" button posts
    to ``/compare/deep-analysis`` with tier=2 to opt into the LLM
    judge (paid, slower).
    """
    # Force the store to be live before the helper queries it.
    _store()
    from amx.cli_support.commands.compare import compare_runs

    try:
        return compare_runs(
            list(body.run_ids),
            quality_tier=body.quality_tier,
            ground_truth_run_id=body.ground_truth_run_id,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc


@router.post("/compare/deep-analysis")
def compare_deep_analysis(body: CompareRequest) -> dict[str, Any]:
    """Tier 2 quality analysis — LLM judge tournament + Tier 1 embeddings.

    Triggered by the Studio Compare modal's "Run deeper analysis"
    button. Forces ``quality_tier=2`` regardless of the request body
    so a deep-analysis hit always runs the full G-Eval pairwise
    pipeline (Liu et al. 2023). Otherwise identical to the standard
    /compare endpoint — same payload shape, just enriched with the
    judge outcomes and embedding agreement.
    """
    _store()
    from amx.cli_support.commands.compare import compare_runs

    # Build an optional LLMProvider for the active scope. The judge
    # tournament needs a working chat() entry point; surface a 503
    # with a clean install hint when no LLM is configured so the UI
    # can render an actionable error instead of a deep traceback.
    llm_provider = None
    db_connector = None
    try:
        from amx.config import AMXConfig
        from amx.llm.provider import LLMProvider

        cfg = AMXConfig.load()
        if cfg.llm.provider and cfg.llm.model:
            llm_provider = LLMProvider(cfg.llm)
        else:
            raise HTTPException(
                status_code=status.HTTP_412_PRECONDITION_FAILED,
                detail={
                    "message": (
                        "Deep quality analysis needs an active LLM "
                        "profile. Open Settings → LLM and pick one."
                    ),
                    "hint": "configure-llm",
                },
            )
        try:
            from amx.db.connector import DatabaseConnector

            db_connector = DatabaseConnector(cfg.db)
        except Exception:
            db_connector = None
    except HTTPException:
        raise
    except Exception as exc:
        # Config / import failure should not 500 — return a clean 503.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not initialise LLM provider: {exc}",
        ) from exc

    try:
        return compare_runs(
            list(body.run_ids),
            quality_tier=2,
            ground_truth_run_id=body.ground_truth_run_id,
            db_connector=db_connector,
            llm_provider=llm_provider,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc


@router.post("/compare/pdf")
def compare_pdf(body: CompareRequest) -> StreamingResponse:
    """Render the comparison payload as a landscape A4 PDF report.

    The Studio "Download PDF" button on the Compare modal posts here
    with the same ``run_ids`` it just used for ``/compare``. We re-run
    ``compare_runs`` so the PDF reflects the latest stored values
    (cheap — same lookups the modal already triggered) and stream
    WeasyPrint's bytes back as a single ``application/pdf`` blob.
    """
    _store()
    from amx.cli_support.commands.compare import compare_runs, render_compare_pdf

    try:
        payload = compare_runs(list(body.run_ids))
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    if not payload.get("runs"):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="None of the requested run ids were found in history.",
        )
    try:
        pdf_bytes = render_compare_pdf(payload)
    except RuntimeError as exc:
        # ``optional_deps.ensure`` raises RuntimeError when pip install
        # fails (offline / read-only env / locked corp box). Surface
        # the hint verbatim so the UI can render the recommended
        # ``pip install`` command instead of a 500.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except OSError as exc:
        # cffi raises OSError when WeasyPrint imports cleanly but
        # ctypes can't dlopen the native Pango / Cairo libs — happens
        # on a vanilla macOS box without ``brew install pango cairo``
        # or a slim Linux container without ``libpango-1.0-0``. Map to
        # a 503 with a one-line install hint so the SPA can render an
        # actionable error instead of an opaque ASGI 500.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "PDF export needs the Pango / Cairo system libraries. "
                "Install them with `brew install pango cairo` (macOS) or "
                "`apt install libpango-1.0-0 libpangoft2-1.0-0` (Debian / "
                f"Ubuntu) and reload Studio. Original error: {exc}"
            ),
        ) from exc
    run_ids_label = "-".join(str(r["id"]) for r in payload["runs"])
    filename = f"compare-{run_ids_label}.pdf"
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
