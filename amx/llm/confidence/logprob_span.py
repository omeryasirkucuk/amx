"""Signal A: per-alternative logprob span scoring.

The Profile Agent emits ``n_alternatives`` description blocks inside a
single completion (``DESCRIPTION_1`` … ``DESCRIPTION_N``). For each
alternative we locate its text inside the raw completion and score the
overlapping tokens using ``logprob_confidence_score_for_text``.

Known limitation: tokens of ``DESCRIPTION_2`` are conditioned on
``DESCRIPTION_1`` (same completion), so later alternatives carry a
position bias. Phase 4's evaluation harness measures it against
``run_results.accepted`` and decides whether positional correction
or true independent completions (``n`` parameter where supported) is
worth introducing.
"""

from __future__ import annotations

from amx.llm.provider import logprob_confidence_score_for_text


def score_per_alternative(
    logprobs_content: list | None,
    response_text: str | None,
    alternatives: list[str],
) -> list[float | None]:
    """Return one logprob span score per alternative, aligned by index.

    A ``None`` entry means the score could not be computed for that
    alternative (either logprobs were not returned by the provider, the
    response text is missing, or the alternative could not be located in
    the response). Callers downstream treat ``None`` as "signal A
    unavailable for this row" and fall back to the remaining signals.
    """
    if not logprobs_content or not response_text:
        return [None] * len(alternatives)

    return [
        logprob_confidence_score_for_text(logprobs_content, response_text, alt)
        for alt in alternatives
    ]


__all__ = ["score_per_alternative"]
