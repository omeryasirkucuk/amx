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
    llm: object | None = None,
) -> list[AlternativeScore]:
    """Compute per-alternative confidence signals for one suggestion block.

    Every signal is best-effort: a failure inside one signal yields a
    column of ``None`` values for that signal, never an exception
    propagated to the caller. The orchestrator is wrapped in
    ``apply_confidence_signals`` which itself swallows unexpected
    exceptions so a regression in this module cannot abort an
    analysis run.

    ``llm`` is required only when ``cfg.use_judge`` is on (Signal D
    issues a second LLM call). When ``llm`` is ``None`` and the judge
    is enabled, the judge signal is skipped silently and the ensemble
    falls back to the other active signals.
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

    if cfg.use_judge and llm is not None:
        from amx.llm.confidence.judge import score_per_alternative as judge_score

        signals["judge"] = judge_score(alternatives, llm=llm)

    return build_alternative_scores(
        alternatives=alternatives,
        signals=signals,
        thresholds=(cfg.high, cfg.med),
    )


__all__ = ["score_alternatives"]
