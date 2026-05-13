"""Orchestrator: turn a single LLM response + N alternatives into a
list of ``AlternativeScore`` instances.

This is the single integration point that agents call after parsing a
completion. The orchestrator dispatches to each enabled signal,
collects raw per-alternative scores, and feeds them through the
ensemble to produce final ``AlternativeScore`` rows.
"""

from __future__ import annotations

from amx.config import ConfidenceConfig
from amx.llm.confidence import AlternativeScore
from amx.llm.confidence.ensemble import build_alternative_scores


def score_alternatives(
    alternatives: list[str],
    logprobs_content: list | None,
    response_text: str | None,
    cfg: ConfidenceConfig,
) -> list[AlternativeScore]:
    """Compute Phase 1 confidence signals for one suggestion block.

    Both signals are best-effort: a failure inside one signal yields
    a column of ``None`` values for that signal, never an exception
    propagated to the caller. The orchestrator is wrapped in
    ``apply_confidence_signals`` (Task 7) which itself swallows
    unexpected exceptions so a regression in this module cannot
    abort an analysis run.
    """
    if not cfg.enabled or not alternatives:
        return build_alternative_scores(
            alternatives=alternatives,
            signals={},
            thresholds=(cfg.high, cfg.med),
        )

    signals: dict[str, list[float | None]] = {}

    if cfg.use_logprob:
        from amx.llm.confidence.logprob_span import score_per_alternative as logprob_score

        signals["logprob"] = logprob_score(
            logprobs_content=logprobs_content,
            response_text=response_text,
            alternatives=alternatives,
        )

    if cfg.use_self_consistency:
        from amx.llm.confidence.self_consistency import (
            score_per_alternative as self_consistency_score,
        )

        signals["self_consistency"] = self_consistency_score(alternatives)

    if cfg.use_self_decl:
        from amx.llm.confidence.self_declaration import (
            score_per_alternative as self_decl_score,
        )

        signals["self_decl"] = self_decl_score(response_text, n=len(alternatives))

    # Phase 3 (judge) deliberately not wired here yet.

    return build_alternative_scores(
        alternatives=alternatives,
        signals=signals,
        thresholds=(cfg.high, cfg.med),
    )


__all__ = ["score_alternatives"]
