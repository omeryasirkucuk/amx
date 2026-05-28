"""Interactive human-in-the-loop review extracted from ``Orchestrator``.

The four review functions drive the CLI UI users see when AMX asks
them to accept, edit, or reject an auto-generated description. They
share ``orch.cfg``, ``orch.llm``, and ``orch._record_evaluation`` on
the host Orchestrator instance.

Public ``Orchestrator`` wrappers keep working as one-line delegators
so any external caller that patches them stays unaffected.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import TYPE_CHECKING

from amx.agents.base import Confidence, MetadataSuggestion
from amx.cli_support.review_keynav import format_help, parse_nav_command
from amx.utils.console import (
    ask,
    ask_choice,
    console,
    heading,
    info,
    render_table,
    warn,
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
    # Deferred import — see review_single for why this can't live at
    # module top (circular with ``orchestrator``). The accept-all /
    # reject-all branches below construct ReviewResult at runtime.
    from amx.agents.orchestrator import ReviewResult

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

        if review_mode == "one-by-one":
            # Position-aware, navigable review (back / goto / filter / help)
            # with a [i/N] counter. Replaces the old forward-only loop.
            results.extend(_navigable_review(orch, col_suggestions, asset_kind, result_id_map))
        else:
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
                else:
                    # accept-all-high + non-HIGH, or reject-all → skip.
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

    return results


def review_single(
    orch: Orchestrator,
    s: MetadataSuggestion,
    is_table: bool,
    asset_kind: str = "table",
    result_id: int | None = None,
) -> ReviewResult:
    _render_review_header(s, is_table=is_table, asset_kind=asset_kind)
    options = list(s.suggestions) + ["Other (type your own)", "Skip"]
    choice = ask_choice("Select a description", options, default=options[0])
    return _finalize_choice(orch, s, choice, asset_kind=asset_kind, result_id=result_id)


def _render_review_header(
    s: MetadataSuggestion,
    *,
    is_table: bool,
    asset_kind: str,
    position: tuple[int, int] | None = None,
) -> None:
    """Print the per-item review header. ``position`` adds a ``[i/N]``
    counter so the user always knows where they are in the queue."""
    kind_label = asset_kind.replace("_", " ").title() if is_table else "Column"
    asset = f"{kind_label}: {s.schema}.{s.table}" if is_table else f"Column: {s.table}.{s.column}"
    counter = f"[{position[0]}/{position[1]}] " if position else ""
    console.print(f"\n  [heading]{counter}{asset}[/heading]")
    console.print(
        f"  Confidence: [{'success' if s.confidence == Confidence.HIGH else 'warning'}]{s.confidence.value}[/]"
    )
    console.print(
        f"  Logprob: {f'{s.logprob_score:.4f}' if s.logprob_score is not None else 'N/A'}"
    )
    console.print(f"  Source: {s.source}")
    console.print(f"  Reasoning: {s.reasoning}")
    console.print()


def _finalize_choice(
    orch: Orchestrator,
    s: MetadataSuggestion,
    choice: str,
    *,
    asset_kind: str,
    result_id: int | None,
) -> ReviewResult:
    """Turn a picked option into a recorded :class:`ReviewResult`.

    Shared by ``review_single`` and the navigable loop. ``ReviewResult``
    is imported at call time — see ``review_single`` for why a module-top
    import would be circular.
    """
    from amx.agents.orchestrator import ReviewResult

    citations = list(getattr(s, "citations", None) or [])
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
            citations=citations,
        )
    if choice == "Other (type your own)":
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
            citations=citations,
        )
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
        citations=citations,
    )


def _interpret_selection(raw: str, options: list[str]) -> str | None:
    """Map raw input to a review option, or ``None`` if it isn't one.

    ``s``/``skip`` and ``o``/``other`` are shortcuts; a bare number picks
    by position; an exact label also matches. Nav keys (n/p/g/G///?) are
    NOT options — they're handled by ``parse_nav_command`` first.
    """
    low = raw.strip().lower()
    if low in ("s", "skip"):
        return "Skip"
    if low in ("o", "other"):
        return "Other (type your own)"
    if raw.strip().isdigit():
        n = int(raw.strip())
        return options[n - 1] if 1 <= n <= len(options) else None
    for opt in options:
        if raw.strip() == opt:
            return opt
    return None


def _filter_review_order(items: list[MetadataSuggestion], pattern: str) -> list[int]:
    """Indices of ``items`` whose column matches ``pattern`` (regex, case-
    insensitive). Falls back to the full list on an empty/invalid pattern
    or no match, with a warning, so a filter can never strand the user."""
    if not pattern:
        return list(range(len(items)))
    try:
        rx = re.compile(pattern, re.IGNORECASE)
    except re.error:
        warn(f"Invalid filter pattern: /{pattern} — showing all rows.")
        return list(range(len(items)))
    matched = [i for i, s in enumerate(items) if rx.search(s.column or "")]
    if not matched:
        warn(f"No columns match /{pattern} — showing all rows.")
        return list(range(len(items)))
    return matched


def _advance_to_undecided(order: list[int], decided: set[int], pos: int) -> int:
    """Next position in ``order`` (after ``pos``, wrapping) whose item is
    still undecided. Returns ``pos`` unchanged when everything is decided
    (the caller's ``while`` guard then exits)."""
    n = len(order)
    for step in range(1, n + 1):
        cand = (pos + step) % n
        if order[cand] not in decided:
            return cand
    return pos


def _navigable_review(
    orch: Orchestrator,
    items: list[MetadataSuggestion],
    asset_kind: str,
    result_id_map: dict[str | None, int],
) -> list[ReviewResult]:
    """One-by-one column review with a position counter + navigation.

    Replaces the forward-only loop. Every item still gets a decision —
    the loop exits only once all items are decided, matching the old
    semantics — but the user can now step back (``p``/``k``), jump
    (``g N``), jump to the last (``G``), filter the remaining queue
    (``/pattern``), or show help (``?``). ``Enter`` accepts the top
    suggestion and advances (the established convention), which also
    guarantees the loop terminates. A step budget bounds a
    non-interactive / EOF caller so it can never spin forever.
    """
    total = len(items)
    decisions: dict[int, ReviewResult] = {}
    order = list(range(total))
    pos = 0
    info(
        "Review one-by-one — Enter accepts the top suggestion; type a number "
        "to pick, 's' to skip, 'o' for your own text; n/p move, g N jump, "
        "G last, /text filter, ? help."
    )
    guard = 0
    max_iter = total * 50 + 100
    while len(decisions) < total:
        guard += 1
        if guard > max_iter:
            warn("Review exceeded its step budget — skipping the remaining undecided rows.")
            for i, s in enumerate(items):
                if i not in decisions:
                    decisions[i] = _finalize_choice(
                        orch,
                        s,
                        "Skip",
                        asset_kind=asset_kind,
                        result_id=result_id_map.get(s.column),
                    )
            break
        # If the current (possibly filtered) view is fully decided but the
        # run isn't, expand back to all rows so a filter can't strand the
        # user on the rows it hid — then land on the first undecided one.
        if not order or all(i in decisions for i in order):
            order = list(range(total))
            pos = next((p for p, i in enumerate(order) if i not in decisions), 0)
        pos = max(0, min(pos, len(order) - 1))
        idx = order[pos]
        s = items[idx]
        _render_review_header(
            s, is_table=False, asset_kind=asset_kind, position=(pos + 1, len(order))
        )
        if idx in decisions:
            console.print("  [dim](already decided — pick again to change, or navigate on)[/dim]")
        options = list(s.suggestions) + ["Other (type your own)", "Skip"]
        for i, opt in enumerate(options, 1):
            console.print(f"    {i}. {opt}")
        raw = ask("> ")
        if raw == "":
            decisions[idx] = _finalize_choice(
                orch, s, options[0], asset_kind=asset_kind, result_id=result_id_map.get(s.column)
            )
            pos = _advance_to_undecided(order, set(decisions), pos)
            continue
        nav = parse_nav_command(raw, position=pos, queue_len=len(order))
        if nav.action == "help":
            console.print(format_help())
            continue
        if nav.action == "filter":
            order = _filter_review_order(items, nav.payload)
            pos = 0
            continue
        if nav.action == "goto" and nav.payload == "":
            target = ask("Go to row #").strip()
            if target.isdigit():
                pos = max(0, min(len(order) - 1, int(target) - 1))
            continue
        if nav.action in ("next", "prev", "last", "goto"):
            pos = nav.position
            continue
        choice = _interpret_selection(raw, options)
        if choice is None:
            warn(
                f"'{raw}' isn't a row choice or nav key — type a number "
                f"1-{len(options)}, 's', 'o', or '?' for help."
            )
            continue
        decisions[idx] = _finalize_choice(
            orch, s, choice, asset_kind=asset_kind, result_id=result_id_map.get(s.column)
        )
        pos = _advance_to_undecided(order, set(decisions), pos)
    return [decisions[i] for i in range(total)]


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
