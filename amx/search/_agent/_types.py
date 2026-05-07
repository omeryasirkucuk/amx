"""Shared dataclasses + helpers for the ``SearchAgent`` mixin modules.

The original ``amx/search/agent.py`` carried these as module-level
definitions; mixin method bodies referenced them via Python's module
scope. After the v0.9.0 mixin split each mixin became its own
module — but the moved methods kept calling ``SearchPlan(...)`` /
``_input_token_budget_for(...)`` / etc. by bare name, causing
``NameError`` at runtime once those code paths fired (most notably
``SearchAgent.ask()`` → ``_synthesize_answer`` → ``_input_token_budget_for``).

Living here makes the dataclasses + small helpers available to every
mixin without a circular import: ``_types.py`` imports only stdlib +
``json`` / ``re``, so importing it from any mixin (or from
``agent.py``) is always safe.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

# ──────────────────────────────────────────────────────────────────────
# Dataclasses
# ──────────────────────────────────────────────────────────────────────


@dataclass
class SearchPlan:
    """Structured output of the question-interpretation step.

    Carries the LLM's plan for how to answer a /search question:
    intent, search-mode, target entity, and the row-shape hints used
    by the deterministic / LLM synthesizer pair.
    """

    intent: str
    out_of_domain: bool
    normalized_question: str
    search_mode: str
    question_class: str
    target_entity: str
    entity_hints: list[str]
    search_queries: list[str]
    needs_typo_recovery: bool
    answer_language: str
    ambiguity_flags: list[str]
    reason: str
    decision_confidence: str = "high"
    needs_clarification: bool = False
    clarification_question: str = ""
    review_notes: str = ""
    # Answer-shape hints. Empty/zero defaults mean "no signal from
    # interpretation" — the policy/derivation step picks a shape based
    # on question_class.
    aggregation_op: str = ""  # "" | "max" | "min" | "top_k" | "bottom_k" | "count"
    aggregation_field: str = ""  # "" | "row_count" | "column_count" | "table_count"
    aggregation_limit: int = 0  # 0 = no aggregation; 1 for superlatives; N for top-K
    answer_shape: str = ""  # See _ANSWER_SHAPES below; "" = derive from policy.


@dataclass
class SearchPolicy:
    """Retrieval policy derived from the plan + active settings."""

    question_class: str
    retrieval_policy: str
    requires_catalog: bool
    deterministic_answer: bool
    verify_live: bool
    allow_vector: bool
    allow_code: bool
    answer_format: str
    fallback_behavior: str
    answer_shape: str = ""  # Derived from plan.answer_shape or question_class.


@dataclass
class SearchActionSuggestion:
    """A next-step suggestion the agent surfaces in the UI ("/search sync …")."""

    action: str
    reason: str


@dataclass
class LiveProbePlan:
    """Output of the live-probe planner — operations + reason."""

    needs_live_probe: bool
    reason: str
    operations: list[dict[str, str]]


@dataclass
class ResolvedTarget:
    """One resolved (or partially-resolved) table the question explicitly named."""

    requested: str
    resolved_path: str
    source: str
    is_exact: bool
    confidence: str
    warnings: list[str]
    candidates: list[str]


# ──────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────


# Closed set of presentation shapes the agent + renderer dispatch on.
# Tests and the renderer share this vocabulary.
_ANSWER_SHAPES: set[str] = {
    "single_fact",  # one-sentence headline, no list, no rich table
    "short_table",  # headline + 2-5 row markdown table inline in summary
    "full_table",  # broad inventory dump (existing behaviour)
    "ranked_list",  # headline + rich Search matches table (filtered to non-zero scores)
    "table_summary",  # headline + key-columns rich table for table_explain
    "join_candidates",  # existing join Rich table dispatch
    "prose",  # 2-4 sentence explanation, no table
}


# Conservative default input-token budget for ``_synthesize_answer``.
# Family-specific overrides live in ``_input_token_budget_for``.
_DEFAULT_INPUT_TOKEN_BUDGET = 60_000


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _question_language_hint(text: str) -> str:
    """Light-weight language detector for short user questions.

    Returns one of ``arabic`` / ``japanese`` / ``korean`` / ``russian``
    / ``english`` based on Unicode block presence. AMX answers in
    English regardless; the detector exists only so the planner can
    note the input language for telemetry / heuristics.
    """
    sample = (text or "").strip()
    if not sample:
        return "english"
    if re.search(r"[؀-ۿ]", sample):
        return "arabic"
    if re.search(r"[぀-ヿ一-鿿]", sample):
        return "japanese"
    if re.search(r"[가-힯]", sample):
        return "korean"
    if re.search(r"[Ѐ-ӿ]", sample):
        return "russian"
    return "english"


def _json_block(text: str) -> dict[str, Any]:
    """Parse a JSON object from a (possibly fenced) LLM response."""
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]
    return json.loads(raw)


def _merge_usage(*payloads: dict[str, Any] | None) -> dict[str, Any]:
    """Sum ``usage`` payloads from multiple LLM calls in one /ask turn."""
    merged = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "model_processing_sec": 0.0,
    }
    for payload in payloads:
        if not payload:
            continue
        merged["prompt_tokens"] += int(payload.get("prompt_tokens") or 0)
        merged["completion_tokens"] += int(payload.get("completion_tokens") or 0)
        merged["total_tokens"] += int(payload.get("total_tokens") or 0)
        merged["model_processing_sec"] += float(payload.get("model_processing_sec") or 0.0)
    return merged


def _input_token_budget_for(model: str | None) -> int:
    """Conservative input-token budget for the active LLM model.

    Frontier models with very large context windows (Claude 3.5/4,
    Gemini 1.5/2.0 pro) get a higher budget; everything else uses the
    default ``_DEFAULT_INPUT_TOKEN_BUDGET`` (60K) which fits OpenAI
    ``gpt-4o``, ``gpt-4o-mini``, DeepSeek, and most local servers.
    """
    if not model:
        return _DEFAULT_INPUT_TOKEN_BUDGET
    name = model.lower()
    if any(
        token in name
        for token in (
            "claude-3-5",
            "claude-sonnet-4",
            "claude-opus-4",
            "claude-3-opus",
            "claude-haiku-4",
        )
    ):
        return 150_000  # Claude family: 200K context window.
    if any(
        token in name
        for token in (
            "gemini-1.5-pro",
            "gemini-2.0-pro",
            "gemini-2.0-flash",
            "gemini-1.5-flash",
        )
    ):
        return 250_000  # Gemini family: 1M-2M context.
    return _DEFAULT_INPUT_TOKEN_BUDGET


def _trim_rows_to_token_budget(
    rows: list[dict[str, Any]],
    *,
    system_text: str,
    base_payload: dict[str, Any],
    budget: int,
) -> tuple[list[dict[str, Any]], int]:
    """Drop lowest-scored rows until the prompt fits ``budget`` tokens.

    Computes the per-row cost from a single full-payload encoding plus
    a no-rows encoding (O(n) total) rather than re-encoding inside a
    loop, so large row sets do not pay quadratic cost.

    Returns ``(kept_rows, dropped_count)``. The result is sorted by
    descending ``match_score`` so the highest-confidence rows survive.
    """
    from amx.utils.token_tracker import estimate_tokens

    if not rows:
        return rows, 0

    sorted_rows = sorted(rows, key=lambda row: float(row.get("match_score") or 0.0), reverse=True)

    full_payload = dict(base_payload, rows=sorted_rows, result_count=len(sorted_rows))
    full_msgs = [
        {"role": "system", "content": system_text},
        {"role": "user", "content": json.dumps(full_payload, ensure_ascii=True)},
    ]
    full_tokens = estimate_tokens(full_msgs)
    if full_tokens <= budget:
        return sorted_rows, 0

    empty_payload = dict(base_payload, rows=[], result_count=0)
    empty_msgs = [
        {"role": "system", "content": system_text},
        {"role": "user", "content": json.dumps(empty_payload, ensure_ascii=True)},
    ]
    base_tokens = estimate_tokens(empty_msgs)

    rows_token_cost = max(1, full_tokens - base_tokens)
    avg_per_row = max(1, rows_token_cost // len(sorted_rows))
    available_for_rows = max(0, budget - base_tokens)
    keep_count = max(0, available_for_rows // avg_per_row)
    keep_count = min(keep_count, len(sorted_rows))

    if keep_count >= len(sorted_rows):
        return sorted_rows, 0
    return sorted_rows[:keep_count], len(sorted_rows) - keep_count


__all__ = [
    "LiveProbePlan",
    "ResolvedTarget",
    "SearchActionSuggestion",
    "SearchPlan",
    "SearchPolicy",
    "_ANSWER_SHAPES",
    "_DEFAULT_INPUT_TOKEN_BUDGET",
    "_input_token_budget_for",
    "_json_block",
    "_merge_usage",
    "_question_language_hint",
    "_trim_rows_to_token_budget",
]
