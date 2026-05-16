"""Merge sub-agent suggestions into final descriptions.

Extracted from ``Orchestrator`` so the prompt templates, the LLM-driven
merge step, the optional fill-up second pass, and the response parser
live in one place. The mutating merge functions take
``orch: Orchestrator`` as their first argument and read runtime state
(``orch.llm``, ``orch.cfg``, ``orch.run_id`` etc.) off it; the parser
is pure.

The historical ``Orchestrator._merge_suggestions`` /
``Orchestrator._merge_fill_up`` / ``Orchestrator._parse_merge_response``
methods are kept as one-line delegators so any external caller that
patches them still works.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import TYPE_CHECKING

from amx.agents._mode_guardrail import check_mode_consistency
from amx.agents._prompt_helpers import alternatives_mode_merge_note
from amx.agents.base import (
    AgentContext,
    Confidence,
    MetadataSuggestion,
    apply_confidence_signals,
    apply_logprob_confidence,
)
from amx.config import DEFAULT_ALTERNATIVES_MODE
from amx.llm.prompts import length_rule
from amx.utils.console import step_spinner
from amx.utils.logging import get_logger
from amx.utils.token_tracker import estimate_tokens, tracker

if TYPE_CHECKING:
    from amx.agents.orchestrator import Orchestrator

log = get_logger("agents.orchestrator.merge")


MERGE_PROMPT = """\
You are merging metadata suggestions from multiple sources for database columns.
Produce up to {n_alternatives} ranked descriptions per column using evidence discipline, not averaging.

Length rule (CRITICAL — honour the user's verbosity preset):
{description_length_rule}

{alternatives_mode_note}

Alternative descriptions:
- DESCRIPTION_1 is the single best, most defensible description.
- DESCRIPTION_2 .. DESCRIPTION_{n_alternatives} are ranked alternative
  candidates (see the diversity directive above for what makes a valid
  alternative).
- The Length rule above applies EQUALLY to DESCRIPTION_1, DESCRIPTION_2,
  ..., DESCRIPTION_{n_alternatives} — an alternative is never a shorter
  version. Do not collapse alternates into one-sentence summaries when
  the verbosity preset is comprehensive or exhaustive.
- If the evidence does not support another candidate for a slot, write
  a single em-dash "—" on that line. Do NOT pad with rephrasings.

Output rules:
- Write every description and reasoning string in **clear, business-friendly American English**.
- Use complete sentences. End every description with a period.
- No hedging language ("might", "possibly", "could be") unless the evidence really is ambiguous —
  in which case lower CONFIDENCE rather than soften the description.
- Never start a description with the column name or "This column" — describe the *meaning*, not the row.
- Keep the response labels (`COLUMN`, `DESCRIPTION_1`, `CONFIDENCE`, `REASONING`) verbatim.

Source precedence:
- Prefer descriptions supported by explicit code behavior or strong database/profile evidence.
- Use documentation when it clearly matches the asset, but do not let generic docs override stronger direct evidence.
- If sources disagree, choose the narrower description that is directly supported.
- If no source proves a specific business meaning, prefer a broader neutral description rather than hallucinating a precise one.

Confidence rules:
- HIGH: multiple strong sources agree or one source is highly explicit.
- MEDIUM: one reasonable interpretation dominates but some ambiguity remains.
- LOW: evidence is sparse, conflicting, or generic.

Reasoning must mention which source types won and why.

{columns_text}

Respond in this exact format for EACH column (one block per column):

COLUMN: <column_name>
DESCRIPTION_1: <best merged description>
{description_lines}
CONFIDENCE: <HIGH|MEDIUM|LOW>
REASONING: <why>
"""

MERGE_SYSTEM_PROMPT = """\
You merge metadata suggestions conservatively.
Do not invent meaning not present in the source proposals or their reasoning.
Honour the user-supplied verbosity preset in the user message — do not
silently shorten an "exhaustive" or "comprehensive" answer to a single
sentence.
"""

MERGE_FILLUP_PROMPT = """\
You previously produced merged descriptions for these columns, but some
columns still need additional distinct alternative descriptions to reach
the user's requested count of {n_alternatives}.

Length rule (CRITICAL — honour the user's verbosity preset):
{description_length_rule}

{alternatives_mode_note}

For EACH column below:
- Existing descriptions are listed under "Existing".
- Produce additional ranked alternative descriptions labelled
  DESCRIPTION_2 .. DESCRIPTION_{n_alternatives}, filling only the slots
  that are missing from the existing list.
- Each new alternative MUST follow the diversity directive above.
- The Length rule above applies EQUALLY to every DESCRIPTION_N slot you
  fill — an alternative is never a shorter version. Do not collapse
  alternates into one-sentence summaries when the verbosity preset is
  comprehensive or exhaustive.
- If the evidence truly does not support another distinct alternative for
  a slot, write a single em-dash "—" on that line. Do NOT pad with
  rephrasings of an existing description.

Output rules:
- Write every description in **clear, business-friendly American English**.
- Use complete sentences. End every description with a period.
- Keep the response labels (`COLUMN`, `DESCRIPTION_<i>`) verbatim.

{columns_text}

Respond in this exact format for EACH column (one block per column):

COLUMN: <column_name>
{fillup_response_lines}
"""


def _strip_code_fences(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def merge_suggestions(
    orch: Orchestrator, suggestions: list[MetadataSuggestion], ctx: AgentContext
) -> list[MetadataSuggestion]:
    by_column: dict[str | None, list[MetadataSuggestion]] = defaultdict(list)
    for s in suggestions:
        by_column[s.column].append(s)

    merged: list[MetadataSuggestion] = []
    needs_merge: dict[str | None, list[MetadataSuggestion]] = {}

    for col_name, col_suggestions in by_column.items():
        if len(col_suggestions) == 1:
            merged.append(col_suggestions[0])
        else:
            needs_merge[col_name] = col_suggestions

    if not needs_merge:
        return merged

    columns_blocks: list[str] = []
    for col_name, col_suggestions in needs_merge.items():
        label = col_name or "(table-level)"
        source_text = "\n".join(
            f"  [{s.source}] (confidence={s.confidence.value}): "
            f"{', '.join(s.suggestions)}\n    reasoning: {s.reasoning}"
            for s in col_suggestions
        )
        columns_blocks.append(f"### {label}\n{source_text}")

    columns_text = "\n\n".join(columns_blocks)
    # Inject the active LLM profile's verbosity directive so the
    # merge step preserves an "exhaustive" / "comprehensive"
    # answer instead of collapsing every column to one tight
    # sentence. Without this the per-agent agents already write
    # the long form, but this LLM call summarises it back down.
    verbosity = getattr(orch.llm.cfg, "description_verbosity", "brief")
    cap = max(1, min(5, getattr(orch.llm.cfg, "n_alternatives", 3)))
    alternatives_mode = getattr(orch.llm.cfg, "alternatives_mode", DEFAULT_ALTERNATIVES_MODE)
    description_lines = (
        "\n".join(
            f"DESCRIPTION_{i}: <alternative description — apply the SAME length rule as DESCRIPTION_1>"
            for i in range(2, cap + 1)
        )
        if cap > 1
        else ""
    )
    messages = [
        {"role": "system", "content": MERGE_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": MERGE_PROMPT.format(
                columns_text=columns_text,
                description_length_rule=length_rule(verbosity),
                n_alternatives=cap,
                description_lines=description_lines,
                alternatives_mode_note=alternatives_mode_merge_note(alternatives_mode, cap),
            ),
        },
    ]
    est = estimate_tokens(messages)
    # Scale the merge call's output budget the same way ProfileAgent
    # does: ~1600 tokens per column at the exhaustive preset, less
    # for shorter presets. Without this the merge call inherits
    # cfg.max_tokens (16k by default) and truncates mid-column on a
    # multi-column comprehensive/exhaustive batch. Multiply by
    # ``cap`` because we are now asking for up to N alternative
    # descriptions per column instead of a single best.
    from amx.llm.prompts import per_col_token_budget

    merge_max_tokens = max(
        orch.llm.cfg.max_tokens,
        len(needs_merge) * per_col_token_budget(verbosity) * cap,
    )
    with step_spinner(f"Merging suggestions: {len(needs_merge)} columns", token_estimate=est):
        result = orch.llm.chat(messages, max_tokens=merge_max_tokens)
    tracker.record_for("merge", est, orch.llm, result.usage)

    parsed = orch._parse_merge_response(result.content)

    merge_results: list[MetadataSuggestion] = []
    per_column_state: dict[str | None, tuple[str, list[str]]] = {}
    underfilled: dict[str | None, list[str]] = {}
    for col_name, col_suggestions in needs_merge.items():
        key = col_name or "(table-level)"
        merge_alts, conf, reasoning = parsed.get(key, ([], Confidence.MEDIUM, ""))

        all_descs: list[str] = []
        for d in merge_alts:
            if d and d not in all_descs:
                all_descs.append(d)
        for s in col_suggestions:
            for d in s.suggestions:
                if d and d not in all_descs:
                    all_descs.append(d)

        per_column_state[col_name] = (key, all_descs)
        if len(all_descs) < cap:
            underfilled[col_name] = all_descs

        # Union citations from every input suggestion (RAG + DB +
        # codebase) onto the merged output, deduped by
        # ``(source, chunk_idx)``. Without this the merge step
        # would silently drop the provenance trail RAGAgent
        # attached to its per-agent suggestion.
        merged_citations: list = []
        seen_citations: set = set()
        for s in col_suggestions:
            for c in getattr(s, "citations", None) or []:
                # PR γ: include ``line_range`` in the dedup key so
                # two code citations from the same file at
                # different line spans both survive the merge.
                # Doc citations have ``line_range=None`` so the
                # tuple collapses to the pre-PR-γ shape for them.
                key_ = (c.source, c.chunk_idx, getattr(c, "line_range", None))
                if key_ in seen_citations:
                    continue
                seen_citations.add(key_)
                merged_citations.append(c)

        merge_results.append(
            MetadataSuggestion(
                schema=ctx.schema,
                table=ctx.table,
                column=col_name,
                suggestions=all_descs[:cap],
                confidence=conf,
                reasoning=reasoning,
                source="combined",
                citations=merged_citations,
            )
        )

    # ── Fill-up retry ────────────────────────────────────────────────
    # Some columns came back with fewer than ``cap`` distinct
    # alternatives — either the merge LLM emitted "—" abstain
    # markers, or sub-agent suggestions de-duplicated against the
    # merge "best". Make a single follow-up call asking only for
    # the still-missing slots so the user-facing alt count is
    # honoured. We cap the retry at one pass: if the LLM still
    # cannot ground a distinct alternative after seeing the
    # existing list, it has been asked twice and we accept the
    # shorter answer.
    if underfilled and cap > 1:
        orch._merge_fill_up(
            ctx=ctx,
            merge_results=merge_results,
            per_column_state=per_column_state,
            underfilled=underfilled,
            verbosity=verbosity,
            cap=cap,
        )

    merged_with_logprob = apply_logprob_confidence(
        merge_results,
        result.logprobs,
        high_threshold=orch.llm.cfg.logprob_high,
        medium_threshold=orch.llm.cfg.logprob_medium,
        response_text=result.content,
    )
    # Re-score per-alternative confidence on the merged candidates.
    # The merge step builds fresh ``MetadataSuggestion(source="combined")``
    # objects whose ``.suggestions`` list may differ from any single
    # sub-agent's output, so per-alternative scores attached upstream
    # would no longer align. Running ``apply_confidence_signals``
    # here re-embeds (self_consistency), reparses (self_decl), or
    # re-ranks (judge) the merged candidate set. Best-effort: any
    # failure is swallowed and the row falls back to the legacy
    # ``list[str]`` payload.
    try:
        apply_confidence_signals(
            suggestions=merged_with_logprob,
            logprobs_content=result.logprobs,
            response_text=result.content,
            cfg=orch.llm.cfg,
            llm=orch.llm,
        )
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("Post-merge confidence scoring failed: %s", exc)
    # Per-asset guardrail: flag suspected alternatives_mode inversions.
    # Reads cfg.llm.alternatives_mode + cfg.llm.confidence_signal and
    # compares the mean SC against the mode's expected band. Diagnostic
    # is logged via ``check_mode_consistency`` so CI / log aggregation
    # surfaces it without aborting the run. No-ops when the active
    # signal is not self_consistency or when fewer than 2 scores
    # are available for an asset.
    mode = getattr(orch.llm.cfg, "alternatives_mode", DEFAULT_ALTERNATIVES_MODE)
    active_signal = getattr(orch.llm.cfg, "confidence_signal", None)
    for s in merged_with_logprob:
        scores_attr = getattr(s, "suggestion_scores", None) or []
        sc_scores = [getattr(sc, "score", None) for sc in scores_attr]
        check_mode_consistency(
            asset_label=f"{s.schema}.{s.table}.{s.column or '(table)'}",
            mode=mode,
            confidence_signal=active_signal,
            sc_scores=sc_scores,
        )
    merged.extend(merged_with_logprob)
    return merged


def _merge_fill_up(
    orch: Orchestrator,
    *,
    ctx: AgentContext,
    merge_results: list[MetadataSuggestion],
    per_column_state: dict[str | None, tuple[str, list[str]]],
    underfilled: dict[str | None, list[str]],
    verbosity: str,
    cap: int,
) -> None:
    """Make one follow-up LLM call to top up under-filled columns.

    Mutates ``merge_results`` in place: each entry whose column
    appears in ``underfilled`` has its ``suggestions`` list
    extended with newly-grounded alternatives (deduped) up to
    ``cap``. Logs ``agent.merge.fill_short`` for any column that
    is still short after the retry — those are model-quality
    signals worth surfacing to operators, not failures.
    """
    from amx.llm.prompts import per_col_token_budget

    # Build a per-column block listing existing descriptions and
    # the slot indices still to fill.
    blocks: list[str] = []
    for col_name, existing in underfilled.items():
        label = col_name or "(table-level)"
        existing_text = "\n".join(f"  - {d}" for d in existing) if existing else "  (none yet)"
        missing_slots = ", ".join(f"DESCRIPTION_{i}" for i in range(len(existing) + 1, cap + 1))
        blocks.append(f"### {label}\nExisting:\n{existing_text}\nStill to fill: {missing_slots}")

    columns_text = "\n\n".join(blocks)
    fillup_response_lines = "\n".join(
        f"DESCRIPTION_{i}: <alternative — apply the SAME length rule as DESCRIPTION_1, or — if none is supported>"
        for i in range(2, cap + 1)
    )

    messages = [
        {"role": "system", "content": MERGE_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": MERGE_FILLUP_PROMPT.format(
                n_alternatives=cap,
                description_length_rule=length_rule(verbosity),
                columns_text=columns_text,
                fillup_response_lines=fillup_response_lines,
                alternatives_mode_note=alternatives_mode_merge_note(
                    getattr(
                        orch.llm.cfg,
                        "alternatives_mode",
                        DEFAULT_ALTERNATIVES_MODE,
                    ),
                    cap,
                ),
            ),
        },
    ]
    est = estimate_tokens(messages)
    fillup_max_tokens = max(
        orch.llm.cfg.max_tokens,
        len(underfilled) * per_col_token_budget(verbosity) * cap,
    )
    with step_spinner(
        f"Filling alternatives: {len(underfilled)} columns",
        token_estimate=est,
    ):
        result = orch.llm.chat(messages, max_tokens=fillup_max_tokens)
    tracker.record_for("merge_fillup", est, orch.llm, result.usage)

    fill_parsed = orch._parse_merge_response(result.content)

    # Index merge_results by column so we can patch suggestions in place.
    by_column: dict[str | None, MetadataSuggestion] = {ms.column: ms for ms in merge_results}
    for col_name, existing in underfilled.items():
        key, _ = per_column_state[col_name]
        new_alts, _new_conf, _new_reasoning = fill_parsed.get(key, ([], Confidence.MEDIUM, ""))
        combined = list(existing)
        for d in new_alts:
            if d and d not in combined:
                combined.append(d)
        target = by_column.get(col_name)
        if target is not None:
            target.suggestions = combined[:cap]
        if len(combined) < cap:
            log.info(
                "agent.merge.fill_short column=%s have=%d want=%d",
                key,
                len(combined),
                cap,
            )


def parse_merge_response(
    text: str,
) -> dict[str, tuple[list[str], Confidence, str]]:
    """Parse batched merge response into ``{column: (descriptions, confidence, reasoning)}``.

    Each ``DESCRIPTION_<i>`` slot may span multiple lines when the
    user picks a verbose preset (``comprehensive`` / ``exhaustive``);
    continuation lines accumulate into the most recently opened
    slot until another known label appears. ``REASONING`` is
    likewise multi-line capable. A standalone ``—`` (em-dash) on
    a description line is treated as "abstained — no distinct
    alternative supported by the evidence" and dropped from the
    returned list, so callers see only descriptions actually
    backed by source proposals.

    ``BEST_DESCRIPTION:`` is accepted as a legacy synonym for
    ``DESCRIPTION_1:`` so older fixtures still parse.
    """
    text = _strip_code_fences(text)
    results: dict[str, tuple[list[str], Confidence, str]] = {}
    current_col = ""
    # Map of slot index -> list of lines (preserves rank order via sorted keys at flush).
    desc_slots: dict[int, list[str]] = {}
    reasoning_lines: list[str] = []
    conf = Confidence.MEDIUM
    # ``active_slot`` is the description slot currently absorbing
    # continuation lines; ``active_field`` flags whether we're in
    # a description slot or in REASONING.
    active_slot: int | None = None
    active_field: str | None = None  # "description" | "reasoning" | None

    ABSTAIN_MARKERS = {"—", "-", "n/a", "none", ""}

    def _flush() -> None:
        if not current_col:
            return
        ordered: list[str] = []
        for idx in sorted(desc_slots.keys()):
            joined = "\n".join(line.rstrip() for line in desc_slots[idx]).strip()
            if joined.lower() in ABSTAIN_MARKERS:
                continue
            if not joined:
                continue
            ordered.append(joined)
        reasoning_text = "\n".join(line.rstrip() for line in reasoning_lines).strip()
        if ordered:
            results[current_col] = (ordered, conf, reasoning_text)

    desc_label = re.compile(r"^DESCRIPTION_(\d+)\s*:\s*(.*)$")

    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("COLUMN:"):
            _flush()
            current_col = stripped.split(":", 1)[1].strip()
            desc_slots = {}
            reasoning_lines = []
            conf = Confidence.MEDIUM
            active_slot = None
            active_field = None
            continue

        m = desc_label.match(stripped)
        if m:
            idx = int(m.group(1))
            first = m.group(2).strip()
            desc_slots[idx] = [first] if first else []
            active_slot = idx
            active_field = "description"
            continue

        if stripped.startswith("BEST_DESCRIPTION:"):
            # Legacy label; treat as DESCRIPTION_1.
            first = stripped.split(":", 1)[1].strip()
            desc_slots[1] = [first] if first else []
            active_slot = 1
            active_field = "description"
            continue

        if stripped.startswith("CONFIDENCE:"):
            raw = stripped.split(":", 1)[1].strip().upper()
            conf = Confidence[raw] if raw in Confidence.__members__ else Confidence.MEDIUM
            active_slot = None
            active_field = None
            continue

        if stripped.startswith("REASONING:"):
            first = stripped.split(":", 1)[1].strip()
            reasoning_lines = [first] if first else []
            active_slot = None
            active_field = "reasoning"
            continue

        # Continuation line for the most recently opened multi-line
        # field. Preserves blank lines so multi-paragraph answers
        # keep their paragraph breaks.
        if active_field == "description" and active_slot is not None:
            desc_slots.setdefault(active_slot, []).append(raw_line)
        elif active_field == "reasoning":
            reasoning_lines.append(raw_line)

    _flush()
    return results
