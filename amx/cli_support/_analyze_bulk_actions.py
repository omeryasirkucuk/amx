"""Bulk review-action helpers for ``amx analyze review``.

Extracted from :mod:`amx.cli_support.commands.analyze_flow` so the
three filter-driven bulk action functions (accept / skip / apply on a
filtered subset of pending review rows) live in their own focused
module. Each function operates on the pending-review queue + history
store; none requires per-row UI interaction.

``analyze_flow.py`` re-exports the three names so any caller importing
the underscore form keeps working unchanged.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

from amx.agents.orchestrator import apply_review_results_to_db
from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector
from amx.pending_review import load_pending, save_pending
from amx.utils.console import error, info

LogEvent = Callable[..., None]


def _bulk_accept_rows(
    rows: list[Any],
    *,
    log_event: LogEvent,
    run_id: int,
) -> None:
    """Queue every row for apply by re-saving the pending review file.

    Mirrors what the standard /review flow does on an Accept: stamp
    ``applied=True`` so :func:`save_pending` picks it up, then write the file.
    Idempotent — calling twice with the same rows produces the same queue.
    """

    by_rid: dict[int, Any] = {}
    for entry in load_pending():
        rid = getattr(entry, "result_id", None)
        if rid is not None:
            by_rid[int(rid)] = entry
    queued = 0
    for r in rows:
        # Skip rows without a description (nothing to accept) and rows
        # that are already applied to the live DB.
        if not (r.final_description or "").strip() or getattr(r, "applied", False):
            continue
        accepted = dataclasses.replace(r, applied=True)
        rid = getattr(r, "result_id", None)
        if rid is not None:
            by_rid[int(rid)] = accepted
        else:
            by_rid[id(r)] = accepted  # synthetic key for ID-less rows
        queued += 1
    save_pending(list(by_rid.values()))
    info(f"Queued {queued} row(s) for apply — see /apply to write them to the DB.")
    log_event(
        "analyze_review_bulk_accept",
        run_id=run_id,
        accepted_count=queued,
    )


def _bulk_skip_rows(
    rows: list[Any],
    *,
    log_event: LogEvent,
    run_id: int,
) -> None:
    """Drop every row from the pending queue (the CLI's analogue of Skip)."""

    drop_ids: set[int] = set()
    for r in rows:
        rid = getattr(r, "result_id", None)
        if rid is not None:
            drop_ids.add(int(rid))
    if not drop_ids:
        info("No rows had a stored result_id — nothing to skip.")
        log_event("analyze_review_bulk_skip", run_id=run_id, skipped_count=0)
        return
    survivors = [
        entry
        for entry in load_pending()
        if getattr(entry, "result_id", None) is None or int(entry.result_id) not in drop_ids
    ]
    save_pending(survivors)
    info(f"Skipped {len(drop_ids)} row(s) from the pending queue.")
    log_event(
        "analyze_review_bulk_skip",
        run_id=run_id,
        skipped_count=len(drop_ids),
    )


def _bulk_apply_rows(
    cfg: AMXConfig,
    rows: list[Any],
    *,
    log_event: LogEvent,
    run_id: int,
) -> None:
    """Accept rows into pending, then immediately apply to the live database."""

    if not cfg.db.backend:
        error("No database configured. Cannot apply.")
        return
    db = DatabaseConnector(cfg.db)
    if not db.test_connection():
        error("Cannot connect to database.")
        return

    accepted = [
        dataclasses.replace(r, applied=True) for r in rows if (r.final_description or "").strip()
    ]
    if not accepted:
        info("No rows with a non-empty description — nothing to apply.")
        return
    applied = apply_review_results_to_db(db, accepted)
    info(f"Applied {applied} metadata comment(s) to the database.")
    log_event(
        "analyze_review_bulk_apply",
        run_id=run_id,
        applied_count=applied,
    )
