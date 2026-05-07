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

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
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
) -> dict[str, Any]:
    """Most-recent runs filtered by command. ``command=all`` includes
    every kind (analyze.run + search.ask + …) so the SPA's "All
    activity" view can render them together."""
    cmd_filter = None if (command or "").strip().lower() in {"", "all"} else command
    rows = _store().list_recent_runs(limit=limit, command_filter=cmd_filter)
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
) -> dict[str, Any]:
    """Per-column LLM suggestions saved during this run. Used by the
    run-detail page's results table + the column drill page's
    "alternatives" carousel."""
    rows = _store().get_run_results(run_id, unevaluated_only=unevaluated_only)
    return {"run_id": run_id, "results": rows, "count": len(rows)}


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


@router.post("/compare")
def compare(body: CompareRequest) -> dict[str, Any]:
    """Side-by-side run comparison payload. Mirrors what the CLI's
    ``/history compare`` command produces; the web UI uses the same
    helper so both surfaces stay in sync.
    """
    # Force the store to be live before the helper queries it.
    _store()
    from amx.cli_support.commands.compare import compare_runs

    try:
        return compare_runs(list(body.run_ids))
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
