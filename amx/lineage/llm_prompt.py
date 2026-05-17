"""Prompt builder + JSON response parser for :mod:`amx.lineage.extractors.llm`.

Pure logic: no LLM calls, no DB I/O. Tests feed canned inputs and
assert the prompt shape and the parsed output independently of the
expensive on-demand LLM round-trip.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from amx.lineage.types import ColumnRef

_SYSTEM_PROMPT = (
    "You are a data lineage assistant. Given an anchor table, its columns, "
    "and a list of other tables in the same database, identify which other "
    "tables LIKELY FEED INTO (upstream) or CONSUME FROM (downstream) the "
    "anchor table.\n\n"
    "Use only the schema/column information provided. Do not invent table or "
    "column names that are not in the candidate list. Do not output "
    "speculative edges with low confidence — when in doubt, omit the edge.\n\n"
    "Respond with a single JSON object of the form:\n"
    "{\n"
    '  "edges": [\n'
    "    {\n"
    '      "from_table": "schema.table",\n'
    '      "to_table":   "schema.table",\n'
    '      "column_pairs": [["source_col", "target_col"], ...],\n'
    '      "reasoning": "one-sentence why this edge is likely",\n'
    '      "confidence": 0.0  // 0..1; 0.8+ only when the schema + column '
    "names give clear, unambiguous evidence\n"
    "    }\n"
    "  ]\n"
    "}\n\n"
    "Output nothing else — no markdown, no commentary. The JSON must be "
    "directly parseable."
)


@dataclass(frozen=True)
class AnchorContext:
    """Everything the prompt builder needs about the anchor table."""

    fqn: str  # 'schema.table'
    columns: list[dict[str, str]]  # [{name, dtype, description}, ...]
    description: str = ""


@dataclass(frozen=True)
class CandidateTable:
    """One table the LLM may suggest as an edge endpoint."""

    fqn: str  # 'schema.table'
    columns: list[dict[str, str]]
    description: str = ""


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
    """One previously-verdicted edge to fold into the next prompt."""

    from_fqn: str
    to_fqn: str
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
    user_payload = {
        "anchor": {
            "table": anchor.fqn,
            "description": anchor.description,
            "columns": anchor.columns,
        },
        "candidates": [
            {
                "table": c.fqn,
                "description": c.description,
                "columns": c.columns,
            }
            for c in candidates
        ],
    }
    feedback_blocks: list[str] = []
    if approved:
        feedback_blocks.append(
            "User has previously **approved** these edges in this catalogue. "
            "Treat them as ground truth — mirror their reasoning style:\n"
            + "\n".join(
                f"  - {e.from_fqn} → {e.to_fqn}" + (f"  ({e.note})" if e.note else "")
                for e in approved
            )
        )
    if rejected:
        feedback_blocks.append(
            "User has previously **rejected** these edges in this catalogue. "
            "Do not propose anything analogous:\n"
            + "\n".join(
                f"  - {e.from_fqn} → {e.to_fqn}" + (f"  ({e.note})" if e.note else "")
                for e in rejected
            )
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
    min_confidence: float = 0.4,
) -> list[SuggestedEdge]:
    """Parse + validate the LLM's JSON reply.

    Drops any edge whose endpoint is not in ``valid_candidate_fqns``
    (defends against hallucinated table names) or whose confidence is
    below ``min_confidence``. One of the two endpoints MUST be the
    anchor — edges that ignore the anchor entirely are also dropped.
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
