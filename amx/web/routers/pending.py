"""Pending-review queue router.

Wraps the on-disk pending file at ``~/.amx/pending_metadata.json``
that :mod:`amx.pending_review` reads/writes. The visualizer's
``/pending`` page lists, edits, deletes, clears, and applies the
queue without the user touching the filesystem.

Apply spawns the same worker as :mod:`amx.web.routers.runs` so SSE
progress events look identical regardless of which page the user
triggered the apply from.
"""

from __future__ import annotations

import threading
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult
from amx.config import AMXConfig
from amx.pending_review import clear_pending, load_pending, save_pending
from amx.storage.sqlite_store import history_store
from amx.web.deps import get_cfg, get_jobs
from amx.web.jobs import JobRegistry
from amx.web.routers.runs import ApplyRequest, _apply_worker

router = APIRouter(prefix="/api/pending", tags=["pending"])


class PendingPatch(BaseModel):
    """Body for ``PATCH /api/pending/{idx}`` — every field optional so
    the SPA can update one column at a time."""

    model_config = ConfigDict(populate_by_name=True)

    schema_: str | None = Field(default=None, alias="schema")
    table: str | None = None
    column: str | None = None
    final_description: str | None = None
    confidence: str | None = None
    asset_kind: str | None = None


def _serialize(
    rr: ReviewResult,
    idx: int,
    *,
    enrichment: dict[int, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Serialize one ReviewResult for the SPA.

    ``enrichment`` is an optional ``{result_id: run_results_row}`` map
    that backfills fields ``load_pending()`` can't reconstruct from the
    flat JSON file (alternatives_json, logprob_score, source). The SPA
    needs the alternatives so the user can swap which one is the
    chosen description without leaving the page.
    """
    extra = enrichment.get(rr.result_id) if enrichment and rr.result_id is not None else None
    alternatives = list(rr.alternatives or [])
    logprob = rr.logprob_score
    if extra is not None:
        ext_alts = extra.get("alternatives_json")
        if isinstance(ext_alts, list) and ext_alts and not alternatives:
            alternatives = ext_alts
        if logprob is None and extra.get("logprob_score") is not None:
            logprob = extra.get("logprob_score")
    return {
        "idx": idx,
        "schema": rr.schema,
        "table": rr.table,
        "column": rr.column,
        "final_description": rr.final_description,
        "confidence": (
            rr.confidence.name.lower() if hasattr(rr.confidence, "name") else str(rr.confidence)
        ),
        "source": rr.source,
        "applied": bool(rr.applied),
        "asset_kind": rr.asset_kind,
        "result_id": rr.result_id,
        "alternatives": alternatives,
        "logprob_score": logprob,
    }


def _build_enrichment_map(rows: list[ReviewResult]) -> dict[int, dict[str, Any]]:
    """Fetch ``run_results`` rows for every pending entry's ``result_id``.

    The pending JSON file only stores ``final_description`` (the chosen
    one). Alternatives + logprob live in the run_results SQLite table,
    keyed by id. We do ONE iteration over the history to build a
    {result_id: row} index so /api/pending stays a single GET.
    """
    needed = {int(r.result_id) for r in rows if r.result_id is not None}
    if not needed:
        return {}
    hs = history_store()
    if hs is None:
        return {}
    out: dict[int, dict[str, Any]] = {}
    # Walk recent runs until we've covered every needed result_id.
    # Cheap bound: pending entries always come from a recent run, so
    # 50 runs is enough headroom; bump if real workloads outgrow it.
    try:
        recent = hs.list_recent_runs(limit=50)
    except Exception:
        return {}
    for run in recent:
        run_id = run.get("id") if isinstance(run, dict) else getattr(run, "id", None)
        if run_id is None:
            continue
        try:
            run_rows = hs.get_run_results(int(run_id))
        except Exception:
            continue
        for row in run_rows:
            rid = row.get("id")
            if isinstance(rid, int) and rid in needed:
                out[rid] = row
        if needed.issubset(out.keys()):
            break
    return out


@router.get("")
def list_pending() -> dict[str, Any]:
    """Return the on-disk pending queue with stable ``idx`` markers
    so PATCH/DELETE callers can target rows by position."""
    rows = list(load_pending())
    enrichment = _build_enrichment_map(rows)
    return {
        "pending": [_serialize(r, i, enrichment=enrichment) for i, r in enumerate(rows)],
        "count": len(rows),
    }


@router.patch("/{idx}")
def patch_pending(idx: int, body: PendingPatch) -> dict[str, Any]:
    """Update one row in place. The SPA's inline-edit modal posts
    only the changed fields."""
    rows = list(load_pending())
    if not 0 <= idx < len(rows):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No pending entry at index {idx}.",
        )
    target = rows[idx]
    if body.schema_ is not None:
        target.schema = body.schema_
    if body.table is not None:
        target.table = body.table
    if body.column is not None:
        target.column = body.column or None
    if body.final_description is not None:
        target.final_description = body.final_description
    if body.asset_kind is not None:
        target.asset_kind = body.asset_kind
    if body.confidence is not None:
        try:
            target.confidence = Confidence[body.confidence.upper()]
        except KeyError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid confidence '{body.confidence}'. Use high/medium/low.",
            ) from exc
    save_pending(rows)
    return _serialize(target, idx)


@router.delete("/{idx}")
def remove_pending(idx: int) -> dict[str, Any]:
    rows = list(load_pending())
    if not 0 <= idx < len(rows):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No pending entry at index {idx}.",
        )
    removed = rows.pop(idx)
    save_pending(rows)
    return {"ok": True, "removed": _serialize(removed, idx), "remaining": len(rows)}


@router.post("/clear")
def clear() -> dict[str, Any]:
    clear_pending()
    return {"ok": True}


@router.post("/apply")
def apply_pending(
    cfg: AMXConfig = Depends(get_cfg),
    jobs: JobRegistry = Depends(get_jobs),
) -> dict[str, Any]:
    """Spawn an apply job that writes every entry in the pending
    queue. Re-uses :func:`amx.web.routers.runs._apply_worker` so the
    SSE event stream is bit-identical to ``POST /api/apply``."""
    job = jobs.new_job("apply")
    thread = threading.Thread(
        target=_apply_worker,
        args=(cfg, job, ApplyRequest(results=None)),
        name=f"amx-visualizer-pending-apply-{job.id}",
        daemon=True,
    )
    thread.start()
    return {"job_id": job.id, "status": job.status}
