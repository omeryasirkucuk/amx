"""Single-signal dispatcher.

After the per-alternative pipeline pivoted from a four-signal ensemble
to a user-selected single signal, this module simply reads
``cfg.confidence_signal``, calls exactly one scorer, and folds the raw
score plus band label into an :class:`AlternativeScore` per alternative.

The scorer is best-effort by design: if the per-signal call fails for
any reason (provider returned no logprobs, sentence-transformers
missing in the env, the second-pass judge call timed out) the offending
alternative gets ``score=None`` and ``band='—'`` — the run never
aborts.
"""

from __future__ import annotations

from typing import Any

from amx.llm.confidence import AlternativeScore
from amx.llm.confidence.band import band_for


def _empty_for(alternatives: list[str]) -> list[AlternativeScore]:
    """Return ``[]`` so the caller leaves ``suggestion_scores`` untouched.

    Used for the disabled paths (``confidence_signal == 'none'`` or the
    master ``enabled`` switch flipped off). Returning an empty list (not
    a list of ``None``-scored entries) is important: the agent contract
    is "no signal ran → no structured payload → legacy serialisation".
    """
    return []


def score_alternatives(
    alternatives: list[str],
    logprobs_content: list | None,
    response_text: str | None,
    cfg: Any,
    llm: Any | None = None,
) -> list[AlternativeScore]:
    """Score ``alternatives`` with the single active signal on ``cfg``.

    ``cfg`` is an :class:`amx.config.LLMConfig`. We read
    ``cfg.confidence_signal`` (the active signal name) and the band
    cut-offs / master enable switch off ``cfg.confidence``.
    """
    if not alternatives:
        return []

    confidence_cfg = getattr(cfg, "confidence", None)
    if confidence_cfg is None or not getattr(confidence_cfg, "enabled", True):
        return _empty_for(alternatives)

    signal = getattr(cfg, "confidence_signal", "none")
    if signal == "none":
        return _empty_for(alternatives)

    high = float(getattr(confidence_cfg, "high", 0.75))
    med = float(getattr(confidence_cfg, "med", 0.50))

    try:
        raw_scores = _run_signal(
            signal=signal,
            alternatives=alternatives,
            logprobs_content=logprobs_content,
            response_text=response_text,
            llm=llm,
        )
    except Exception:
        raw_scores = [None] * len(alternatives)

    # Align / pad in case a scorer returned the wrong number of entries.
    raw_scores = (list(raw_scores) + [None] * len(alternatives))[: len(alternatives)]

    return [
        AlternativeScore(
            text=text,
            signal=signal,
            score=score,
            band=band_for(score, high, med),
        )
        for text, score in zip(alternatives, raw_scores, strict=False)
    ]


def _run_signal(
    *,
    signal: str,
    alternatives: list[str],
    logprobs_content: list | None,
    response_text: str | None,
    llm: Any | None,
) -> list[float | None]:
    """Dispatch to exactly one per-signal scorer module."""
    if signal == "logprob":
        from amx.llm.confidence import logprob_span

        return logprob_span.score_per_alternative(
            logprobs_content=logprobs_content,
            response_text=response_text,
            alternatives=alternatives,
        )
    if signal == "self_consistency":
        from amx.llm.confidence import self_consistency

        return self_consistency.score_per_alternative(alternatives)
    if signal == "self_decl":
        from amx.llm.confidence import self_declaration

        return self_declaration.score_per_alternative(response_text, n=len(alternatives))
    if signal == "judge":
        if llm is None:
            return [None] * len(alternatives)
        from amx.llm.confidence import judge

        return judge.score_per_alternative(alternatives, llm=llm)
    # Unknown signal — defensive default rather than crashing.
    return [None] * len(alternatives)


__all__ = ["score_alternatives"]
