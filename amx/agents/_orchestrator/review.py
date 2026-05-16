"""Interactive human-in-the-loop review extracted from ``Orchestrator``.

The four review functions drive the CLI UI users see when AMX asks
them to accept, edit, or reject an auto-generated description. They
share ``orch.cfg``, ``orch.llm``, and ``orch._record_evaluation`` on
the host Orchestrator instance.

Public ``Orchestrator`` wrappers keep working as one-line delegators
so any external caller that patches them stays unaffected.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from amx.agents.base import Confidence, MetadataSuggestion
from amx.utils.console import (
    ask,
    ask_choice,
    console,
    heading,
    info,
    render_table,
)
from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.agents.orchestrator import Orchestrator, ReviewResult

log = get_logger("agents.orchestrator.review")


def human_review(
    orch: Orchestrator,
    suggestions: list[MetadataSuggestion],
    schema: str,
    table: str,
    asset_kind: str = "table",
    result_id_map: dict[str | None, int] | None = None,
) -> list[ReviewResult]:
    results: list[ReviewResult] = []
    result_id_map = result_id_map or {}

    table_suggestions = [s for s in suggestions if s.column is None]
    col_suggestions = [s for s in suggestions if s.column is not None]

    for s in table_suggestions:
        rid = result_id_map.get(s.column)  # column is None here
        result = orch._review_single(s, is_table=True, asset_kind=asset_kind, result_id=rid)
        results.append(result)

    if col_suggestions:
        col_count = len(col_suggestions)
        noun = "column" if col_count == 1 else "columns"
        heading(f"Column descriptions for {schema}.{table} ({col_count} {noun})")
        rows = []
        for s in col_suggestions:
            rows.append(
                [
                    s.column,
                    s.suggestions[0] if s.suggestions else "N/A",
                    s.confidence.value,
                    f"{s.logprob_score:.4f}" if s.logprob_score is not None else "N/A",
                    s.source,
                ]
            )
        render_table(
            "Suggested descriptions",
            ["Column", "Best Suggestion", "Confidence", "Logprob", "Source"],
            rows,
        )
        console.print()

        review_mode = ask_choice(
            "How would you like to review?",
            ["one-by-one", "accept-all-high", "accept-all", "reject-all"],
            default="one-by-one",
        )

        for s in col_suggestions:
            rid = result_id_map.get(s.column)
            if (
                review_mode == "accept-all"
                or review_mode == "accept-all-high"
                and s.confidence == Confidence.HIGH
            ):
                rr = ReviewResult(
                    schema=s.schema,
                    table=s.table,
                    column=s.column,
                    final_description=s.suggestions[0],
                    confidence=s.confidence,
                    source=s.source,
                    applied=True,
                    asset_kind=asset_kind,
                    result_id=rid,
                    logprob_score=s.logprob_score,
                    citations=list(getattr(s, "citations", None) or []),
                )
                orch._record_evaluation(
                    rid, chosen_description=s.suggestions[0], evaluation="accepted"
                )
                results.append(rr)
            elif (
                review_mode == "accept-all-high"
                and s.confidence != Confidence.HIGH
                or review_mode == "reject-all"
            ):
                rr = ReviewResult(
                    schema=s.schema,
                    table=s.table,
                    column=s.column,
                    final_description="",
                    confidence=s.confidence,
                    source=s.source,
                    applied=False,
                    asset_kind=asset_kind,
                    result_id=rid,
                    logprob_score=s.logprob_score,
                    citations=list(getattr(s, "citations", None) or []),
                )
                orch._record_evaluation(rid, chosen_description="", evaluation="skipped")
                results.append(rr)
            else:
                result = orch._review_single(
                    s, is_table=False, asset_kind=asset_kind, result_id=rid
                )
                results.append(result)

    return results


def review_single(
    orch: Orchestrator,
    s: MetadataSuggestion,
    is_table: bool,
    asset_kind: str = "table",
    result_id: int | None = None,
) -> ReviewResult:
    kind_label = asset_kind.replace("_", " ").title() if is_table else "Column"
    asset = f"{kind_label}: {s.schema}.{s.table}" if is_table else f"Column: {s.table}.{s.column}"
    console.print(f"\n  [heading]{asset}[/heading]")
    console.print(
        f"  Confidence: [{'success' if s.confidence == Confidence.HIGH else 'warning'}]{s.confidence.value}[/]"
    )
    console.print(
        f"  Logprob: {f'{s.logprob_score:.4f}' if s.logprob_score is not None else 'N/A'}"
    )
    console.print(f"  Source: {s.source}")
    console.print(f"  Reasoning: {s.reasoning}")
    console.print()

    options = list(s.suggestions) + ["Other (type your own)", "Skip"]
    choice = ask_choice("Select a description", options, default=options[0])

    if choice == "Skip":
        orch._record_evaluation(result_id, chosen_description="", evaluation="skipped")
        return ReviewResult(
            schema=s.schema,
            table=s.table,
            column=s.column,
            final_description="",
            confidence=s.confidence,
            source=s.source,
            applied=False,
            asset_kind=asset_kind,
            result_id=result_id,
            logprob_score=s.logprob_score,
            citations=list(getattr(s, "citations", None) or []),
        )
    elif choice == "Other (type your own)":
        custom = ask("Enter your description")
        orch._record_evaluation(result_id, chosen_description=custom, evaluation="custom")
        return ReviewResult(
            schema=s.schema,
            table=s.table,
            column=s.column,
            final_description=custom,
            confidence=Confidence.HIGH,
            source="human",
            applied=True,
            asset_kind=asset_kind,
            result_id=result_id,
            logprob_score=s.logprob_score,
            citations=list(getattr(s, "citations", None) or []),
        )
    else:
        orch._record_evaluation(result_id, chosen_description=choice, evaluation="accepted")
        return ReviewResult(
            schema=s.schema,
            table=s.table,
            column=s.column,
            final_description=choice,
            confidence=s.confidence,
            source=s.source,
            applied=True,
            asset_kind=asset_kind,
            result_id=result_id,
            logprob_score=s.logprob_score,
            citations=list(getattr(s, "citations", None) or []),
        )


def review_single_result(orch: Orchestrator, r: ReviewResult) -> ReviewResult:
    """Helper to review a single result by looking up its alternatives if needed."""
    suggestions = r.alternatives if r.alternatives else [r.final_description]

    # Create a dummy MetadataSuggestion for the UI
    s = MetadataSuggestion(
        schema=r.schema,
        table=r.table,
        column=r.column,
        suggestions=suggestions,
        confidence=r.confidence,
        reasoning="Deferred review",
        source=r.source,
        logprob_score=r.logprob_score,
    )
    return orch._review_single(
        s, is_table=(r.column is None), asset_kind=r.asset_kind, result_id=r.result_id
    )


def batch_review(orch: Orchestrator, results: list[ReviewResult]) -> list[ReviewResult]:
    """Perform interactive review for a list of un-applied results."""
    if not results:
        return []

    # Filter for unapplied results (including schema/database meta descriptions).
    to_review = [r for r in results if not r.applied]
    if not to_review:
        return results

    heading(f"Batch Review: {len(to_review)} items pending")

    # Group by table for better UX
    by_table = defaultdict(list)
    for r in to_review:
        by_table[(r.schema, r.table)].append(r)

    final_results = [r for r in results if r.applied]  # Keep already applied/meta

    for (sch, tbl), items in by_table.items():
        heading(f"Reviewing {sch}.{tbl}")

        # Separate table-level and column-level
        table_items = [r for r in items if r.column is None]
        col_items = [r for r in items if r.column is not None]

        for r in table_items:
            reviewed = orch._review_single_result(r)
            final_results.append(reviewed)

        if col_items:
            col_count = len(col_items)
            noun = "column" if col_count == 1 else "columns"
            info(f"Found {col_count} {noun} for {sch}.{tbl}")

            rows = [
                [
                    r.column,
                    r.final_description[:60],
                    r.confidence.value,
                    f"{r.logprob_score:.4f}" if r.logprob_score is not None else "N/A",
                    r.source,
                ]
                for r in col_items
            ]
            render_table(
                "Suggested descriptions",
                ["Column", "Best Suggestion", "Confidence", "Logprob", "Source"],
                rows,
            )

            review_mode = ask_choice(
                "How would you like to review these columns?",
                ["one-by-one", "accept-all-high", "accept-all", "reject-all"],
                default="one-by-one",
            )

            for r in col_items:
                if (
                    review_mode == "accept-all"
                    or review_mode == "accept-all-high"
                    and r.confidence == Confidence.HIGH
                ):
                    r.applied = True
                    orch._record_evaluation(
                        r.result_id,
                        chosen_description=r.final_description,
                        evaluation="accepted",
                    )
                    final_results.append(r)
                elif (
                    review_mode == "accept-all-high"
                    and r.confidence != Confidence.HIGH
                    or review_mode == "reject-all"
                ):
                    r.applied = False
                    orch._record_evaluation(
                        r.result_id, chosen_description="", evaluation="skipped"
                    )
                    final_results.append(r)
                else:
                    reviewed = orch._review_single_result(r)
                    final_results.append(reviewed)

    return final_results
