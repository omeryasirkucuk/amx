"""Post-loop summary + apply branch extracted from ``execute_analyze_run``.

After the per-schema loop finishes we render the run summary, save
approved descriptions to the pending-review queue, optionally apply
to the live DB, and emit the equivalence-dedup recap. v0.9.4 lifts
that ~110-LOC block into its own function so the orchestrator's main
body stays focused on flow control.

The function mutates the run-id history counters when ``--apply`` is
on; it does NOT touch ``final_status`` (the caller owns that).
Returns ``(approved, skipped)`` so the caller's ``finally`` block can
pass them to ``_finalize_history_run``.
"""

from __future__ import annotations

from typing import Any

from amx.utils.console import (
    confirm,
    heading,
    info,
    render_table,
    render_token_summary,
    warn,
)
from amx.utils.logging import get_logger
from amx.utils.token_tracker import tracker as token_tracker

log = get_logger("cli.analyze_flow.run_summary")


def render_summary_and_apply(
    *,
    all_results: list[Any],
    orch: Any,
    review_strategy: str,
    apply: bool,
    rag_store: Any,
    dedup_outcome: Any,
    run_id: int | None,
    history_store_fn: Any,
) -> tuple[list[Any], list[Any]]:
    """Render the post-loop summary and execute the apply branch.

    Order of operations is intentional:

    1. Run the deferred ``batch_review`` if the strategy isn't
       auto-apply (auto-apply already wrote per-table inside
       ``process_table``; calling batch_review again would re-prompt
       the user for what they explicitly said "skip review" on).
    2. Drop RAG token-tracker steps when no RAG store was active.
    3. Print the summary heading, token usage, approved/skipped counts.
    4. Print the equivalence-dedup recap (separate counter — those
       columns went through the dedup LLM pass, NOT the per-table
       agents).
    5. Render the approved-metadata table (top 60 chars per row).
    6. Save approved descriptions to the pending-review queue.
    7. Execute the apply branch:
       * auto-apply: only the schema/database meta needs a final write
         (per-table writes happened inside ``process_table``).
       * other strategies: prompt the user once before writing.
    """
    # ── (1) Deferred batch review ──
    if review_strategy != "auto-apply":
        all_results = orch.batch_review(all_results)

    # ── (2) Drop RAG tracker steps if RAG was disabled ──
    if rag_store is None:
        token_tracker.drop_steps({"rag_agent", "rag_agent(batch)"})

    # ── (3) Summary heading + counts ──
    heading("Summary")
    render_token_summary(token_tracker)
    approved = [r for r in all_results if r.applied]
    skipped = [r for r in all_results if not r.applied]
    info(f"Approved: {len(approved)}  |  Skipped: {len(skipped)}")

    # ── (4) Equivalence dedup recap ──
    _emit_dedup_recap(dedup_outcome)

    # ── (5) Approved metadata table ──
    if approved:
        render_table(
            "Approved metadata",
            ["Asset", "Description", "Confidence", "Logprob", "Source"],
            [
                [
                    f"{r.schema}.{r.table}.{r.column}"
                    if r.column
                    else (f"{r.schema}.{r.table}" if r.table else r.schema),
                    (r.final_description or "")[:60],
                    r.confidence.value,
                    f"{r.logprob_score:.4f}" if r.logprob_score is not None else "N/A",
                    r.source,
                ]
                for r in approved
            ],
        )

    # ── (6) Save pending ──
    if approved:
        from amx.pending_review import save_pending

        save_pending(approved)
        if not apply:
            info(
                f"Saved {len(approved)} approved description(s) as pending. "
                "Run `/analyze` then `/apply` (or `/run-apply` next time) to write them to the database."
            )

    # ── (7) Apply branch ──
    _run_apply_branch(
        approved=approved,
        orch=orch,
        review_strategy=review_strategy,
        apply=apply,
        run_id=run_id,
        history_store_fn=history_store_fn,
    )

    return approved, skipped


def _emit_dedup_recap(dedup_outcome: Any) -> None:
    """Print the equivalence-dedup recap line, if dedup ran this run."""
    if dedup_outcome is None or not (
        dedup_outcome.classes_processed
        or dedup_outcome.classes_diverged
        or dedup_outcome.classes_failed
    ):
        return

    total_dedup_members = dedup_outcome.members_skipped
    classes_done = dedup_outcome.classes_processed
    saved_pct = 0.0
    if total_dedup_members:
        saved_calls = total_dedup_members - classes_done
        saved_pct = (saved_calls / total_dedup_members) * 100.0
    info(
        f"Equivalence dedup: {classes_done} class(es) applied → "
        f"{total_dedup_members} column(s) "
        f"(~{saved_pct:.1f}% fewer column-level LLM calls)."
    )
    if dedup_outcome.classes_diverged:
        info(
            f"  {dedup_outcome.classes_diverged} class(es) flagged DIVERGES — "
            "their members fell back to per-table profiling."
        )
    if dedup_outcome.classes_failed:
        warn(
            f"  {dedup_outcome.classes_failed} class(es) failed during the "
            "dedup LLM call; their members fell back to per-table profiling."
        )


def _run_apply_branch(
    *,
    approved: list[Any],
    orch: Any,
    review_strategy: str,
    apply: bool,
    run_id: int | None,
    history_store_fn: Any,
) -> None:
    """Execute the apply branch — either auto-apply meta or prompt-and-apply."""
    hs = history_store_fn()

    if review_strategy == "auto-apply":
        # Schema / database meta produced by the *_meta steps need a
        # final write since per-table apply didn't reach them.
        meta_to_apply = [
            r for r in approved
            if (r.column is None and r.table == "")
            or r.asset_kind in ("schema", "database")
        ]
        if apply and meta_to_apply:
            from amx.pending_review import clear_pending

            applied_n = orch.apply_results(meta_to_apply)
            clear_pending()
            if hs is not None and run_id is not None and applied_n:
                try:
                    hs.increment_run_applied(run_id, by=int(applied_n))
                except Exception as exc:
                    log.debug("Could not bump applied counter: %s", exc)
        return

    if apply and approved and confirm("Apply these metadata comments to the database?"):
        from amx.pending_review import clear_pending

        applied_n = orch.apply_results(approved)
        clear_pending()
        # Count distinct (schema, table) pairs as the "applied tables"
        # tally for /history. apply_results returns total rows written
        # (table-comments + column-comments), which we record under the
        # applied_count counter — surfaced in /history as the "Applied"
        # part of the X/Y display.
        if hs is not None and run_id is not None and applied_n:
            try:
                hs.increment_run_applied(run_id, by=int(applied_n))
            except Exception as exc:
                log.debug("Could not bump applied counter: %s", exc)


__all__ = ["render_summary_and_apply"]
