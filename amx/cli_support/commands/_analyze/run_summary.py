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

from amx.cli_support.review_filter import (
    STATUS_ACCEPTED,
    STATUS_APPLIED,
    STATUS_PENDING,
    STATUS_SKIPPED,
    apply_filters,
    apply_sort,
    derive_status,
    format_summary_footer,
    group_rows,
)
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


# Rich markup map for the STATUS column. The CLI summary calls the
# "accepted" state ``Accepted`` to match the Studio FilterBar chip;
# ``Pending`` is used for unreviewed rows so a reader scanning the
# table sees the same vocabulary the Studio surface does.
_STATUS_LABELS: dict[str, str] = {
    STATUS_PENDING: "[dim]· Pending[/dim]",
    STATUS_ACCEPTED: "[green]✓ Accepted[/green]",
    STATUS_SKIPPED: "[yellow]✗ Skipped[/yellow]",
    STATUS_APPLIED: "[bold green]✓ Applied[/bold green]",
}


def _format_status_cell(row: Any) -> str:
    """Return the Rich-markup STATUS cell for a ReviewResult-shaped row."""
    return _STATUS_LABELS.get(derive_status(row), _STATUS_LABELS[STATUS_PENDING])


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
    summary_filter: str | None = None,
    summary_sort: str | None = None,
    summary_group: str = "none",
    headless: bool = False,
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
    if headless:
        # Non-interactive run: there's no TTY to drive batch_review (the
        # prompt that otherwise aborts a headless run). Accept the top
        # suggestion for every generated result so it's captured as
        # pending below, mirroring the Studio worker
        # (runs.py flips applied=True before save_pending). Nothing is
        # written to the DB here — the apply branch stays gated on
        # ``apply`` (see below).
        for r in all_results:
            if r.final_description and not r.applied:
                r.applied = True
    elif review_strategy != "auto-apply":
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
        _render_results_table(
            all_rows=all_results,
            approved=approved,
            summary_filter=summary_filter,
            summary_sort=summary_sort,
            summary_group=summary_group,
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
        headless=headless,
    )

    return approved, skipped


def _render_results_table(
    *,
    all_rows: list[Any],
    approved: list[Any],
    summary_filter: str | None,
    summary_sort: str | None,
    summary_group: str,
) -> None:
    """Render the post-run summary as one or more Rich tables.

    The render path is shared by the inline post-``/run`` summary and
    the standalone ``/inspect-run`` slash command in PR A. Filter →
    sort → group ordering matches the Studio FilterBar so the two
    surfaces stay in lockstep.

    The STATUS column is rendered for the full row set (not just
    approved), but when the caller passes ``approved`` only — today's
    behaviour — those are all that's drawn. The ``all_rows`` total is
    used by the footer so a "Showing 12 of 187" line stays honest even
    when the filter narrows the visible set.
    """
    xs = approved
    total_before_filter = len(xs)
    if summary_filter:
        xs = apply_filters(xs, pattern=summary_filter)
    if summary_sort:
        xs = apply_sort(xs, sort_key=summary_sort)
    grouped = group_rows(xs, by=summary_group or "none")

    columns = [
        "Asset",
        "Status",
        "Description",
        "Confidence",
        "Logprob",
        "Source",
        "Sources",
    ]

    def _row_cells(r: Any) -> list[str]:
        return [
            f"{r.schema}.{r.table}.{r.column}"
            if r.column
            else (f"{r.schema}.{r.table}" if r.table else r.schema),
            _format_status_cell(r),
            (r.final_description or "")[:60],
            r.confidence.value,
            f"{r.logprob_score:.4f}" if r.logprob_score is not None else "N/A",
            r.source,
            _format_sources_cell(r),
        ]

    for group_label, group_rows_list in grouped:
        title = f"Approved metadata — {group_label}" if group_label else "Approved metadata"
        render_table(
            title,
            columns,
            [_row_cells(r) for r in group_rows_list],
        )

    visible = sum(len(g[1]) for g in grouped)
    info(
        format_summary_footer(
            total=total_before_filter,
            visible=visible,
            pattern=summary_filter,
            sort_key=summary_sort,
            group_by=summary_group or "none",
        )
    )


def _format_sources_cell(review_result: Any) -> str:
    """Render a compact ``path:chunk_idx, path:chunk_idx`` cell.

    Reads :attr:`ReviewResult.citations` populated by the orchestrator
    on every RAG-derived or merged-with-RAG suggestion. Returns the
    empty string when no citations are attached -- the spec is
    explicit that non-RAG rows should render as truly empty (no
    placeholder dash) so a 200-row run summary stays scannable.
    Truncates with an ellipsis past ~60 chars so a long citation list
    cannot push other columns off-screen.
    """
    citations = getattr(review_result, "citations", None) or []
    if not citations:
        return ""
    parts: list[str] = []
    for c in citations:
        source = getattr(c, "source", None)
        if source is None and isinstance(c, dict):
            source = c.get("source")
        if not source:
            continue
        # PR γ: line range wins over ``chunk_idx`` when present so a
        # Python-AST citation renders as ``src/foo.py:120-145`` and a
        # doc citation falls back to ``spec.pdf:5``. Citations with
        # neither field (legacy regex refs, manual rows) just show the
        # path so the cell never carries a misleading ``:0`` suffix.
        line_range = getattr(c, "line_range", None)
        if line_range is None and isinstance(c, dict):
            line_range = c.get("line_range")
        if line_range is not None:
            try:
                start = int(line_range[0])
                end = int(line_range[1])
            except (TypeError, ValueError, IndexError):
                start = end = 0
            if start > 0 and end > 0:
                rendered_loc = f"{start}-{end}" if start != end else str(start)
                parts.append(f"{source}:{rendered_loc}")
                continue
        chunk_idx = getattr(c, "chunk_idx", None)
        if chunk_idx is None and isinstance(c, dict):
            chunk_idx = c.get("chunk_idx", 0)
        try:
            chunk_idx_int = int(chunk_idx or 0)
        except (TypeError, ValueError):
            chunk_idx_int = 0
        if chunk_idx_int > 0:
            parts.append(f"{source}:{chunk_idx_int}")
        else:
            parts.append(str(source))
    if not parts:
        return ""
    rendered = ", ".join(parts)
    if len(rendered) > 60:
        # Trim full entries from the right and replace the dropped
        # tail with an ellipsis so the user can still tell which
        # documents the suggestion drew from at a glance.
        truncated_parts: list[str] = []
        running = 0
        for part in parts:
            extra = len(part) + (2 if truncated_parts else 0)
            if running + extra > 57:
                break
            truncated_parts.append(part)
            running += extra
        rendered = ", ".join(truncated_parts) + ("…" if truncated_parts else parts[0][:57] + "…")
    return rendered


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
    headless: bool = False,
) -> None:
    """Execute the apply branch — either auto-apply meta or prompt-and-apply.

    ``headless`` runs never prompt: when ``apply`` is set they write
    directly; when ``apply`` is False (``--no-apply``, the default) the
    branch is a no-op and nothing touches the DB.
    """
    hs = history_store_fn()

    if review_strategy == "auto-apply":
        # Schema / database meta produced by the *_meta steps need a
        # final write since per-table apply didn't reach them.
        meta_to_apply = [
            r
            for r in approved
            if (r.column is None and r.table == "") or r.asset_kind in ("schema", "database")
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

    # Headless runs can't answer the apply confirmation; when --apply was
    # passed we honour it directly, otherwise (--no-apply) we skip.
    if (
        apply
        and approved
        and (headless or confirm("Apply these metadata comments to the database?"))
    ):
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
