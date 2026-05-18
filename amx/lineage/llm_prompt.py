"""Prompt builder + JSON response parser for :mod:`amx.lineage.extractors.llm`.

Pure logic: no LLM calls, no DB I/O. Tests feed canned inputs and
assert the prompt shape and the parsed output independently of the
expensive on-demand LLM round-trip.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from amx.lineage.types import ColumnRef

_SYSTEM_PROMPT = (
    "You are a data lineage assistant. The user has chosen ONE focal "
    "table (the anchor) and wants to know which OTHER tables relate "
    "to it as upstream sources or downstream consumers.\n\n"
    "Hard rules — violating any of these makes the suggestion useless:\n"
    "  1. NEVER suggest an edge where ``from_table == to_table``. The "
    "anchor is the focal node, not a node that links to itself.\n"
    "  2. Every edge MUST have the anchor on exactly ONE side. Edges "
    "between two non-anchor candidates are out of scope.\n"
    "  3. Every edge MUST name a concrete column pair. ``from_column`` "
    "and ``to_column`` are required, not optional — table-level edges "
    "without column granularity are rejected.\n"
    "  4. Only use the table names that appear in the candidate list. "
    "Do not invent tables, columns, or relationships you have no "
    "evidence for.\n"
    "  5. Prefer suggestions backed by deterministic evidence "
    "(``evidence`` field: ``FK``, ``view``, ``co-query``). Use "
    "``name`` or ``inferred`` sparingly and only with sub-0.7 "
    "confidence.\n"
    '  6. Calibrate ``confidence`` honestly — 0.9+ means "I would '
    "stake the day's work on this edge\". A guess based purely on "
    "column-name similarity is 0.5-0.7, not 0.9.\n\n"
    "The anchor section may include foreign-key partners, views that "
    "join the anchor with other tables, and tables co-queried with "
    "the anchor. Use that grounding aggressively — when a candidate "
    "is already named in those sections, the edge almost certainly "
    "exists and the only question is the column pair.\n\n"
    "Respond with a single JSON object:\n"
    "{\n"
    '  "edges": [\n'
    "    {\n"
    '      "from_table":  "schema.table",\n'
    '      "from_column": "col_name",\n'
    '      "to_table":    "schema.table",\n'
    '      "to_column":   "col_name",\n'
    '      "direction":   "upstream | downstream",\n'
    '      "evidence":    "FK | view | co-query | name | inferred",\n'
    '      "reasoning":   "one short sentence",\n'
    '      "confidence":  0.0\n'
    "    }\n"
    "  ]\n"
    "}\n\n"
    "Output nothing else — no markdown, no commentary. The JSON must "
    "be directly parseable."
)


@dataclass(frozen=True)
class AnchorContext:
    """Everything the prompt builder needs about the anchor table.

    Backward compatible: callers that only set ``fqn``/``columns``/
    ``description`` get the old behaviour; the new optional fields
    surface in the prompt's anchor block when populated, giving the
    LLM grounding it never had before.
    """

    fqn: str  # 'schema.table'
    columns: list[dict[str, str]]  # [{name, dtype, description}, ...]
    description: str = ""
    fk_partners: list[dict[str, str]] = field(default_factory=list)
    # [{direction, other_fqn, from_column, to_column}, ...]
    view_references: list[dict[str, Any]] = field(default_factory=list)
    # [{view_fqn, other_tables: [str, ...]}, ...]
    co_occurrence_partners: list[dict[str, Any]] = field(default_factory=list)
    # [{other_fqn, count}, ...]


@dataclass(frozen=True)
class CandidateTable:
    """One table the LLM may suggest as an edge endpoint.

    ``score`` + ``reasons`` come from the candidate ranker — the prompt
    surfaces them so the LLM sees WHY each candidate was picked
    (FK partner, view co-mention, query co-occurrence, etc.) instead
    of just a flat list.
    """

    fqn: str  # 'schema.table'
    columns: list[dict[str, str]]
    description: str = ""
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SuggestedEdge:
    """One parsed edge from an LLM response."""

    from_fqn: str  # 'schema.table'
    to_fqn: str
    column_pairs: list[tuple[str, str]]
    reasoning: str
    confidence: float


@dataclass(frozen=True)
class FeedbackExample:
    """One previously-verdicted edge to fold into the next prompt.

    Column granularity matters for feedback. "User approved
    ``customers → orders``" is weak signal; "User approved
    ``customers.id → orders.customer_id``" tells the LLM exactly
    which column pair the user wants to see again, and the same
    rule reads as a negative example when verdict is rejected.
    """

    from_fqn: str
    to_fqn: str
    from_column: str = ""
    to_column: str = ""
    note: str = ""  # e.g. "FK pattern", "spurious join" — short reason


def build_messages(
    anchor: AnchorContext,
    candidates: list[CandidateTable],
    *,
    max_candidates: int = 30,
    approved_examples: list[FeedbackExample] | None = None,
    rejected_examples: list[FeedbackExample] | None = None,
    max_examples_each: int = 5,
) -> list[dict[str, str]]:
    """Return a ``[{role, content}]`` chat messages list for LLM client.

    Candidates are truncated to ``max_candidates`` to keep prompt size
    bounded — the caller picks the best subset (e.g. by name-prefix
    match or co-occurrence frequency).

    ``approved_examples`` and ``rejected_examples`` (v3 S5 feedback
    loop) fold previously-verdicted edges into the system prompt as
    positive / negative few-shot examples so the LLM converges on the
    user's taste. Each list is capped at ``max_examples_each`` to keep
    token spend predictable.
    """
    candidates = candidates[:max_candidates]
    approved = list(approved_examples or [])[:max_examples_each]
    rejected = list(rejected_examples or [])[:max_examples_each]
    anchor_block: dict[str, Any] = {
        "table": anchor.fqn,
        "description": anchor.description,
        "columns": anchor.columns,
    }
    # Surface the rich anchor signals only when they have content; an
    # empty list would just inflate the prompt without grounding.
    if anchor.fk_partners:
        anchor_block["known_foreign_keys"] = anchor.fk_partners
    if anchor.view_references:
        anchor_block["views_joining_this_table"] = [
            {"view": v["view_fqn"], "joins_with": v["other_tables"]} for v in anchor.view_references
        ]
    if anchor.co_occurrence_partners:
        anchor_block["tables_queried_alongside"] = anchor.co_occurrence_partners
    user_payload: dict[str, Any] = {
        "anchor": anchor_block,
        "candidates": [
            {
                "table": c.fqn,
                "description": c.description,
                "columns": c.columns,
                # Per-candidate score + reasons make the prompt
                # self-explanatory: the LLM sees which evidence
                # surfaced each candidate (FK, view, co-query, …)
                # instead of treating them as an opaque list.
                "score": c.score,
                "reasons": c.reasons,
            }
            for c in candidates
        ],
    }
    feedback_blocks: list[str] = []
    if approved:
        feedback_blocks.append(
            "User has previously **approved** these edges in this catalogue. "
            "Treat them as ground truth — mirror the column-pair style:\n"
            + "\n".join(f"  - {_format_feedback(e)}" for e in approved)
        )
    if rejected:
        feedback_blocks.append(
            "User has previously **rejected** these edges in this catalogue. "
            "Do not propose anything analogous:\n"
            + "\n".join(f"  - {_format_feedback(e)}" for e in rejected)
        )
    feedback_section = "\n\n" + "\n\n".join(feedback_blocks) if feedback_blocks else ""
    return [
        {
            "role": "system",
            "content": _SYSTEM_PROMPT + feedback_section,
        },
        {
            "role": "user",
            "content": (
                "Identify likely lineage edges between the anchor and the "
                "candidate tables below. Respond with the JSON shape from "
                "the system prompt.\n\n" + json.dumps(user_payload, ensure_ascii=False, indent=2)
            ),
        },
    ]


def parse_response(
    raw: str,
    *,
    anchor_fqn: str,
    valid_candidate_fqns: set[str],
    min_confidence: float = 0.6,
) -> list[SuggestedEdge]:
    """Parse + validate the LLM's JSON reply.

    Drops any edge whose endpoint is not in ``valid_candidate_fqns``
    (defends against hallucinated table names) or whose confidence is
    below ``min_confidence``. One of the two endpoints MUST be the
    anchor — edges that ignore the anchor entirely are also dropped.

    ``from_fqn == to_fqn`` self-loops are rejected too. The system
    prompt already states the anchor is the focal node and forbids
    anchor-to-anchor suggestions, but a defense-in-depth filter here
    catches stray cases (model glitches, off-distribution prompts)
    before they reach the canvas as duplicate-anchor visual artifacts.
    """
    payload = _coerce_json(raw)
    if not isinstance(payload, dict):
        return []
    raw_edges = payload.get("edges") if isinstance(payload, dict) else []
    if not isinstance(raw_edges, list):
        return []
    out: list[SuggestedEdge] = []
    valid = set(valid_candidate_fqns)
    valid.add(anchor_fqn)
    for entry in raw_edges:
        if not isinstance(entry, dict):
            continue
        from_fqn = str(entry.get("from_table") or "").strip()
        to_fqn = str(entry.get("to_table") or "").strip()
        if from_fqn == to_fqn:
            # Self-loop — reject unconditionally. The streaming hook
            # would otherwise synthesise a second copy of the anchor
            # node on the canvas with no incident edge to it.
            continue
        if from_fqn not in valid or to_fqn not in valid:
            continue
        if anchor_fqn not in (from_fqn, to_fqn):
            continue
        try:
            confidence = float(entry.get("confidence") or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence < min_confidence:
            continue
        # The new prompt asks for ``from_column``/``to_column`` as
        # scalar strings (one pair per edge). Older prompts and
        # cached responses still use ``column_pairs`` (list of
        # ``[src, dst]``). Accept both shapes so a mid-flight
        # rollout doesn't reject perfectly-good cached output.
        column_pairs: list[tuple[str, str]] = []
        single_from = str(entry.get("from_column") or "").strip()
        single_to = str(entry.get("to_column") or "").strip()
        if single_from or single_to:
            column_pairs = [(single_from, single_to)]
        else:
            column_pairs = _extract_column_pairs(entry.get("column_pairs"))
        reasoning = str(entry.get("reasoning") or "").strip()
        out.append(
            SuggestedEdge(
                from_fqn=from_fqn,
                to_fqn=to_fqn,
                column_pairs=column_pairs,
                reasoning=reasoning,
                confidence=max(0.0, min(1.0, confidence)),
            )
        )
    return out


def _format_feedback(ex: FeedbackExample) -> str:
    """Render one feedback example as a compact one-liner.

    Shape: ``schema.t1.col_a → schema.t2.col_b  (note)``. Falls back
    to table-level when the column pair is missing (legacy rows
    written before column-grain edges were standard).
    """
    if ex.from_column and ex.to_column:
        head = f"{ex.from_fqn}.{ex.from_column} → {ex.to_fqn}.{ex.to_column}"
    else:
        head = f"{ex.from_fqn} → {ex.to_fqn}"
    return head + (f"  ({ex.note})" if ex.note else "")


def _coerce_json(raw: str) -> Any:
    """Best-effort JSON parse — strips Markdown code fences if the model
    wrapped its reply despite the system prompt saying not to.
    """
    if not raw:
        return None
    text = raw.strip()
    if text.startswith("```"):
        # Strip fenced block: ```json\n...\n``` or ```\n...\n```
        text = re.sub(r"^```[a-zA-Z]*\n", "", text)
        text = re.sub(r"\n```\s*$", "", text)
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return None


def _extract_column_pairs(raw_pairs: Any) -> list[tuple[str, str]]:
    if not isinstance(raw_pairs, list):
        return []
    out: list[tuple[str, str]] = []
    for entry in raw_pairs:
        if isinstance(entry, list) and len(entry) == 2:
            src = str(entry[0] or "").strip()
            tgt = str(entry[1] or "").strip()
            if src and tgt:
                out.append((src, tgt))
        elif isinstance(entry, dict):
            src = str(entry.get("source") or entry.get("from") or "").strip()
            tgt = str(entry.get("target") or entry.get("to") or "").strip()
            if src and tgt:
                out.append((src, tgt))
    return out


def to_column_ref(anchor: ColumnRef, fqn: str) -> ColumnRef:
    """Resolve ``'schema.table'`` against the anchor's database scope."""
    schema, _, table = fqn.partition(".")
    return ColumnRef(database=anchor.database, schema=schema, table=table, column="")


__all__ = [
    "AnchorContext",
    "CandidateTable",
    "FeedbackExample",
    "SuggestedEdge",
    "build_messages",
    "parse_response",
    "to_column_ref",
]
