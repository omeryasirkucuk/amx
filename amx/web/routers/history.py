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

import fnmatch
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
    activity" view can render them together. ``command=comparable``
    restricts the feed to the description-producing runs the Compare
    picker can pivot (analyze / rerun / generate / schedule) — Ask and
    other non-description commands are dropped so the picker never lists
    a run the user can't actually compare.

    Each row carries a ``live_job_id`` when its worker thread is still
    alive in the registry. The Studio uses that id to render an inline
    Cancel control on the running rows — without it, the only way to
    stop a stuck worker was to know the SSE job id off-hand and POST
    ``/api/runs/{job}/cancel`` by hand.
    """
    normalized = (command or "").strip().lower()
    comparable_only = normalized == "comparable"
    cmd_filter = None if normalized in {"", "all", "comparable"} else command
    store = _store()
    rows = store.list_recent_runs(
        limit=limit, command_filter=cmd_filter, comparable_only=comparable_only
    )
    # Global "pending review" tally — runs still holding unreviewed
    # result rows (``ready_for_review`` + ``applied_partial``). The
    # Studio Landing chip reads this so its count reflects the whole
    # table, not the (necessarily small) recent-feed window.
    pending_review_total = store.count_pending_review_runs(
        command_filter=cmd_filter, comparable_only=comparable_only
    )
    # Build a {run_id: job_id} index in O(active jobs) so the per-row
    # lookup below is O(1). ``apply`` jobs don't carry a run_id and
    # are skipped; ``run``, ``rerun``, and ``variations`` jobs each
    # map to an analysis_runs row id and must surface live progress
    # so the Studio's per-row Cancel control + the run-detail
    # subscriber both work for in-flight Re-Run / Variations jobs.
    _LIVE_JOB_KINDS = {"run", "rerun", "variations"}
    live_by_run_id: dict[int, str] = {}
    for job in jobs.list():
        if job.kind not in _LIVE_JOB_KINDS:
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
    return {
        "command_filter": cmd_filter,
        "runs": rows,
        "count": len(rows),
        "pending_review_total": pending_review_total,
    }


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
        if (
            job.kind in ("run", "rerun", "variations")
            and job.run_id == run_id
            and job.status in ("queued", "running")
        ):
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

    # Lineage chip — surface the parent run id (for Variations + Re-Run
    # children) so the Studio header can render ``From run #N · seed:
    # …`` without an extra fetch. Variations capture the seed text on
    # the first run_results row; we read it lazily to avoid bloating
    # this hot endpoint on runs that have no descendants.
    lineage: dict[str, Any] = {}
    if isinstance(settings, dict):
        parent = settings.get("parent_run_id")
        if isinstance(parent, int) and parent > 0:
            lineage["parent_run_id"] = parent
            trigger = settings.get("trigger") or ""
            lineage["kind"] = "variations" if str(trigger) == "variations" else "rerun"
            # For Variations, surface the seed text + id from any
            # row in this run (they all share the same seed). For
            # Re-Run there is no per-alternative seed so both fields
            # stay null.
            if lineage["kind"] == "variations":
                first = _store().get_run_results(run_id)
                for r in first or []:
                    if r.get("seed_alternative_id"):
                        lineage["seed_alternative_id"] = r["seed_alternative_id"]
                        lineage["seed_alternative_text"] = r.get("seed_alternative_text")
                        break
    if lineage:
        row["lineage"] = lineage
    return row


@router.get("/runs/{run_id}/results")
def get_run_results(
    run_id: int,
    jobs: JobRegistry = Depends(get_jobs),
    unevaluated_only: bool = Query(default=False),
    include_history: bool = Query(
        default=False,
        description=(
            "When true, attach the full re-run chain (original + every "
            "child re-run row) to each result under ``history``. Used by "
            "the Studio history drawer to render v1/v2/v3 side-by-side."
        ),
    ),
    include_descendants: bool = Query(
        default=False,
        description=(
            "When true, fetch the descendant Variations + Re-Run runs "
            "and return them under ``descendants`` so the run-detail "
            "page can render them inline (Variations nested under the "
            "seed alternative; Re-Run as sibling groups). Variations "
            "recurse up to three levels deep; rows beyond carry "
            "``over_max_depth: true``. Re-Run descend one level only."
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
    payload: dict[str, Any] = {"run_id": run_id, "results": rows, "count": len(rows)}
    if include_descendants:
        try:
            descendants = store.get_descendant_runs(int(run_id))
        except Exception:
            descendants = []
        # ``version_label`` is computed server-side so the labels are
        # stable across page reloads and the frontend doesn't need to
        # re-sort. v1 = the direct run; v2..vN = descendants in
        # collection order. Each descendant carries its own label so
        # the inline rendering can group v1 + v2/v3 side-by-side.
        for idx, entry in enumerate(descendants, start=2):
            entry["version_label"] = f"v{idx}"
        # ``live_job_id`` per descendant — used by the Studio's
        # mount-time SSE hydration to re-subscribe after a page
        # refresh during execution. Without it, the
        # ``Generating variations…`` indicator only lives in
        # client memory and a reload silently wipes it. Builds the
        # same {run_id: job_id} index the recent-runs list uses.
        live_by_run_id: dict[int, str] = {}
        for job in jobs.list():
            if job.kind not in ("run", "rerun", "variations"):
                continue
            if job.status not in ("queued", "running"):
                continue
            if job.run_id is None:
                continue
            live_by_run_id[int(job.run_id)] = job.id
        for entry in descendants:
            entry["live_job_id"] = live_by_run_id.get(int(entry["run_id"]))
        payload["descendants"] = descendants
        # Convenience flag — frontend uses this to decide whether to
        # bother rendering the descendants section.
        payload["has_descendants"] = bool(descendants)
    return payload


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


@router.get("/status")
def history_status() -> dict[str, Any]:
    """Surface the shared history store status and any in-flight backfill.

    Used by the Studio backfill banner to poll for migration progress.
    Returns:
      - ``enabled``: whether shared mode is configured
      - ``backfill``: backfill state per scope (running / done / idle)
      - ``shared_profile``: the legacy single-profile field (kept for
        backward-compat with older Studio builds)
      - ``shared_profiles``: deduplicated union of the primary profile
        plus every entry in ``history_store_profiles`` — the full set
        of DB profiles participating in the shared store
    """
    store = _store()
    enabled = True
    shared_profile = ""
    shared_profiles: list[str] = []
    backfill: dict[str, Any] = {}
    try:
        from amx.config import AMXConfig, history_store_profile_set

        cfg = AMXConfig.load()
        shared_profile = cfg.history_store_profile or ""
        shared_profiles = history_store_profile_set(cfg)
        enabled = bool(cfg.history_store_enabled)
    except Exception:
        pass
    # Attempt to surface backfill state from the dual-write store
    try:
        if hasattr(store, "backfill_state"):
            backfill = store.backfill_state() or {}
        elif hasattr(store, "_local") and hasattr(store._local, "backfill_state"):
            backfill = store._local.backfill_state() or {}
    except Exception:
        backfill = {}
    return {
        "enabled": enabled,
        "shared_profile": shared_profile,
        "shared_profiles": shared_profiles,
        "backfill": backfill,
    }


class HistoryProfilesPatch(BaseModel):
    """Body for ``PATCH /api/history/profiles``."""

    profiles: list[str] = Field(default_factory=list)


@router.patch("/profiles")
def set_history_profiles(body: HistoryProfilesPatch) -> dict[str, Any]:
    """Replace the additional-profile list for the shared history store.

    The primary profile (singular ``history_store_profile``) is left
    untouched — it owns the schema and is only changed by
    ``/history-store enable``. This endpoint manages the *extra*
    profiles whose runs are also dual-written.

    Returns the new union after save so the Studio Settings page can
    re-render its chips immediately.
    """
    from amx.config import AMXConfig, history_store_profile_set

    try:
        cfg = AMXConfig.load()
    except Exception as exc:  # pragma: no cover - load is well-tested elsewhere
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to load config: {exc}",
        ) from exc

    primary = (cfg.history_store_profile or "").strip()
    cleaned: list[str] = []
    seen: set[str] = set()
    for name in body.profiles or []:
        text = str(name or "").strip()
        if not text:
            continue
        # Drop the primary from the extras list — it lives in the
        # singular field and shouldn't be duplicated on disk.
        if text == primary:
            continue
        if text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    cfg.history_store_profiles = cleaned
    try:
        cfg.save()
    except Exception as exc:  # pragma: no cover - save errors surface inline
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save config: {exc}",
        ) from exc

    return {
        "shared_profile": primary,
        "shared_profiles": history_store_profile_set(cfg),
    }


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
    judge (consumes tokens on the active LLM, slower).
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


def _row_matches_cell(
    row: dict[str, Any],
    schema: str,
    table: str,
    column: str | None,
) -> bool:
    """Return True if a run_results row matches the given cell key.

    A 3-part cell key (db.schema.table) matches table-level rows
    (``column_name`` is null/empty). A 4-part key requires an exact
    column match.
    """
    if str(row.get("schema_name") or "") != schema:
        return False
    if str(row.get("table_name") or "") != table:
        return False
    row_col = row.get("column_name") or ""
    if column is None:
        return not row_col
    return str(row_col) == column


def _row_glob_matches(
    row: dict[str, Any],
    schema_pat: str,
    table_pat: str,
    column_pat: str | None,
) -> bool:
    """Return True if a row matches all glob patterns. ``column_pat=None``
    means table-level only."""
    s = str(row.get("schema_name") or "")
    t = str(row.get("table_name") or "")
    c = row.get("column_name") or ""
    if not fnmatch.fnmatch(s, schema_pat):
        return False
    if not fnmatch.fnmatch(t, table_pat):
        return False
    if column_pat is None:
        return not c
    return bool(c) and fnmatch.fnmatch(str(c), column_pat)


def _row_to_per_run_entry(row: dict[str, Any], run_id: int) -> dict[str, Any]:
    """Project a run_results row into the per-run shape returned by
    ``/compare/cell``. Keeps the payload small — only fields the cell
    comparison view renders."""
    desc = row.get("chosen_description") or ""
    if not desc:
        alts = row.get("alternatives_json")
        if isinstance(alts, list) and alts:
            desc = str(alts[0]) if alts[0] else ""
    return {
        "run_id": run_id,
        "result_id": row.get("id"),
        "description": desc,
        "confidence": row.get("confidence"),
        "logprob_score": row.get("logprob_score"),
        "citations": row.get("citations_json") or [],
        "source": row.get("source"),
        "evaluation": row.get("evaluation"),
        "accepted": (row.get("evaluation") or "").lower() == "accepted",
        "applied_at": row.get("applied_at"),
    }


def _best_run_id(per_run: list[dict[str, Any] | None]) -> int | None:
    """Pick the best run for a cell by logprob_score (higher is better).

    Mirrors :func:`amx.cli_support.commands.compare._highlight_best`
    semantics — returns None when fewer than 2 non-null entries exist
    or all tied. Returns the ``run_id`` rather than the index so the
    SPA / CLI can render the highlight without re-deriving the order.
    """
    indexed: list[tuple[int, float]] = []
    for entry in per_run:
        if not entry:
            continue
        score = entry.get("logprob_score")
        try:
            indexed.append((int(entry["run_id"]), float(score)))
        except (TypeError, ValueError, KeyError):
            continue
    if len(indexed) < 2:
        return None
    distinct = {v for _, v in indexed}
    if len(distinct) == 1:
        return None
    rid, _ = max(indexed, key=lambda iv: iv[1])
    return rid


@router.get("/compare/cell")
def compare_cell(
    cell: str = Query(
        ...,
        description=(
            "Cell key: ``db.schema.table.column`` (column optional for "
            "table-level). May contain ``*`` glob wildcards in any "
            "segment to match multiple cells."
        ),
    ),
    runs: str = Query(..., description="Comma-separated run IDs."),
) -> dict[str, Any]:
    """Compare the same cell (or a glob of cells) across multiple runs.

    Single-cell mode (no ``*`` in the key)::

        { "cell": {database, schema, table, column},
          "per_run": [ {run_id, description, ...} | null, ... ],
          "best_run_id": int | null }

    Glob mode (``*`` anywhere in the key)::

        { "cells": [ {cell, per_run, best_run_id}, ... ],
          "count": int }

    A ``null`` entry under ``per_run`` means the cell didn't appear in
    that run.
    """
    store = _store()

    parts = cell.split(".")
    if len(parts) < 3 or len(parts) > 4:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="cell must be db.schema.table or db.schema.table.column",
        )
    db_part, schema_part, table_part = parts[0], parts[1], parts[2]
    column_part: str | None = parts[3] if len(parts) == 4 else None

    try:
        run_ids = [int(x) for x in runs.split(",") if x.strip()]
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"invalid run id list: {exc}",
        ) from exc
    if not run_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="at least one run id required",
        )

    rows_by_run: dict[int, list[dict[str, Any]]] = {}
    for rid in run_ids:
        try:
            rows_by_run[rid] = store.get_run_results(rid)
        except Exception:
            rows_by_run[rid] = []

    is_glob = "*" in cell

    if not is_glob:
        per_run: list[dict[str, Any] | None] = []
        for rid in run_ids:
            match = next(
                (
                    r
                    for r in rows_by_run[rid]
                    if _row_matches_cell(r, schema_part, table_part, column_part)
                ),
                None,
            )
            per_run.append(_row_to_per_run_entry(match, rid) if match else None)
        return {
            "cell": {
                "database": db_part,
                "schema": schema_part,
                "table": table_part,
                "column": column_part,
            },
            "per_run": per_run,
            "best_run_id": _best_run_id(per_run),
        }

    # Glob mode — collect distinct matching cells across all runs.
    distinct: dict[tuple[str, str, str | None], dict[str, Any]] = {}
    for rid in run_ids:
        for r in rows_by_run[rid]:
            if not _row_glob_matches(r, schema_part, table_part, column_part):
                continue
            key = (
                str(r.get("schema_name") or ""),
                str(r.get("table_name") or ""),
                (r.get("column_name") or None),
            )
            if key not in distinct:
                distinct[key] = {
                    "database": db_part,
                    "schema": key[0],
                    "table": key[1],
                    "column": key[2],
                }

    cells_out: list[dict[str, Any]] = []
    # Sort for deterministic output (schema → table → column).
    for cell_meta in sorted(
        distinct.values(),
        key=lambda c: (c["schema"], c["table"], c["column"] or ""),
    ):
        per_run = []
        for rid in run_ids:
            match = next(
                (
                    r
                    for r in rows_by_run[rid]
                    if _row_matches_cell(
                        r, cell_meta["schema"], cell_meta["table"], cell_meta["column"]
                    )
                ),
                None,
            )
            per_run.append(_row_to_per_run_entry(match, rid) if match else None)
        cells_out.append(
            {
                "cell": cell_meta,
                "per_run": per_run,
                "best_run_id": _best_run_id(per_run),
            }
        )

    return {"cells": cells_out, "count": len(cells_out)}


@router.post("/compare/deep-analysis")
def compare_deep_analysis(body: CompareRequest) -> dict[str, Any]:
    """Tier 2 quality analysis — LLM judge tournament + Tier 1 embeddings.

    Triggered by the Studio Compare modal's "Run deeper analysis"
    button. Forces ``quality_tier=2`` regardless of the request body
    so a deep-analysis hit always runs the full G-Eval pairwise
    pipeline (Liu et al. 2023). Otherwise identical to the standard
    /compare endpoint — same payload shape, just enriched with the
    judge outcomes and embedding agreement. Tier 2 consumes tokens
    on the active LLM provider; the call is opt-in (Studio's "Run
    deeper analysis" button shows a confirmation dialog first) and
    the token usage is audited via the ``app_events`` trail.
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
    with the same ``run_ids`` (and ``quality_tier`` / ``ground_truth_run_id``)
    it used for ``/compare``. We re-run ``compare_runs`` so the PDF
    reflects the latest stored values, opt into Tier 1+2 quality
    metrics when the user has already requested deep analysis, and
    stream WeasyPrint's bytes back as a single ``application/pdf`` blob.
    """
    _store()
    from amx.cli_support.commands.compare import compare_runs, render_compare_pdf

    # Tier 2 needs a working LLMProvider for the judge tournament; if
    # the request asks for Tier 2 PDF rendering and the box has no LLM
    # configured, gracefully demote to Tier 1 so the PDF still ships.
    llm_provider = None
    db_connector = None
    if body.quality_tier > 0:
        try:
            from amx.config import AMXConfig
            from amx.db.connector import DatabaseConnector

            cfg = AMXConfig.load()
            try:
                db_connector = DatabaseConnector(cfg.db)
            except Exception:
                db_connector = None
            if body.quality_tier >= 2 and cfg.llm.provider and cfg.llm.model:
                from amx.llm.provider import LLMProvider

                llm_provider = LLMProvider(cfg.llm)
        except Exception:
            db_connector = None
            llm_provider = None

    try:
        payload = compare_runs(
            list(body.run_ids),
            quality_tier=body.quality_tier,
            ground_truth_run_id=body.ground_truth_run_id,
            db_connector=db_connector,
            llm_provider=llm_provider,
        )
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
