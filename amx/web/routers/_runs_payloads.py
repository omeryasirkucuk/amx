"""Pure payload + dict-shaping helpers for the runs router.

Extracted from :mod:`amx.web.routers.runs` so the per-job SSE event
payloads (token snapshots, review-result emitters, applied-result
hydrators) live in one focused module. The helpers are stateless and
operate on plain dicts / dataclasses — they have no FastAPI / SSE
machinery of their own.

``runs.py`` re-exports each name so callers that imported the
underscore form keep working unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import HTTPException, status

from amx.agents.orchestrator import ReviewResult
from amx.utils.token_tracker import tracker as token_tracker
from amx.web.progress_bus import emit

if TYPE_CHECKING:
    from amx.web.routers.runs import ApplyRequest


def _column_details_for_table(
    hs: Any, run_id: int | None, schema: str, table: str
) -> list[dict[str, Any]]:
    """Fetch all run_results rows for one table and shape them for SSE.

    Returns empty list when the history store isn't available (fresh
    CLI session) — caller falls back to a simpler preview-only shape.
    """
    if hs is None or run_id is None:
        return []
    try:
        rows = hs.get_run_results(int(run_id))
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if row.get("schema_name") != schema or row.get("table_name") != table:
            continue
        alt_raw = row.get("alternatives_json")
        alternatives: list[Any] = alt_raw if isinstance(alt_raw, list) else []
        out.append(
            {
                "result_id": row.get("id"),
                "schema": row.get("schema_name"),
                "table": row.get("table_name"),
                "column": row.get("column_name"),
                "asset_kind": row.get("asset_kind") or "table",
                "confidence": row.get("confidence") or "medium",
                "logprob_score": row.get("logprob_score"),
                "alternatives": alternatives,
                "chosen_description": row.get("chosen_description") or "",
                "source": row.get("source") or "",
                # Reasoning is the LLM's one-sentence justification --
                # the CLI shows it inline at review time. Without
                # forwarding it here, the SPA's live-progress card
                # could never display it for in-flight runs.
                "reasoning": row.get("reasoning") or "",
                # PR C: provenance trail for RAG-derived suggestions.
                # Empty list on non-RAG / legacy rows so the Studio
                # RunDetail component can render conditionally
                # without null guards in every consumer.
                "citations": row.get("citations_json") or [],
                # Diversity mode active when alternatives were generated.
                # NULL on legacy rows; the Pending / Review components
                # treat NULL as "mode not recorded" and skip the badge.
                "alternatives_mode": row.get("alternatives_mode"),
            }
        )
    return out


def _emit_tokens_snapshot(queue: Any) -> None:
    """Push a running tokens + USD cost total onto the SSE bus.

    Emitted after each per-table ``activity.complete`` so the SPA's
    LiveRunStream can render the same "tokens + cost" header that the
    CLI ``LiveDisplay`` shows mid-run. Reads directly from the
    module-level :class:`TokenTracker` singleton — every agent's
    ``record_for`` call has already accumulated the latest USD cost
    via :func:`amx.llm.pricing.compute_cost`.

    The ``input_tokens`` / ``output_tokens`` split is summed from the
    per-call records so the LiveRunStream can render an Input / Output
    / Total breakdown even before ``finish_run`` writes the persisted
    Metrics card.
    """
    records = token_tracker.records()
    input_total = sum(int(r.get("prompt_tokens") or 0) for r in records)
    output_total = sum(int(r.get("completion_tokens") or 0) for r in records)
    emit(
        queue,
        "tokens.snapshot",
        {
            "input_tokens": input_total,
            "output_tokens": output_total,
            "total_tokens": int(token_tracker.total_tokens or 0),
            "total_cost_usd": round(float(token_tracker.total_cost_usd or 0.0), 6),
            "model_processing_sec": round(
                float(token_tracker.total_model_processing_sec or 0.0), 3
            ),
        },
    )


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
        # Reasoning -- the LLM's one-sentence justification. The
        # ReviewResult dataclass does not currently carry it (the
        # value lives on the upstream MetadataSuggestion); this
        # fallback keeps the SSE shape forward-compatible if the
        # dataclass grows the field later. Today the primary
        # ``_column_details_for_table`` path reads reasoning straight
        # from SQLite via ``run_results.reasoning``, so the live
        # card already gets it for analyze runs that hit history.
        "reasoning": getattr(r, "reasoning", "") or "",
    }


def _resolve_apply_results(body: ApplyRequest) -> list[ReviewResult]:
    """Either coerce the body's results into ReviewResult objects or
    fall back to the on-disk pending queue."""
    if body.results is None:
        # Read ``load_pending`` off the runs module so tests that
        # monkeypatch ``runs.load_pending`` still affect this call site.
        from amx.web.routers import runs as runs_module

        return list(runs_module.load_pending())

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
