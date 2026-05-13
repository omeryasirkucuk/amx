"""Apply Phase 1 confidence signals to a list of MetadataSuggestions."""

from __future__ import annotations


def _suggestion(alts: list[str]):
    from amx.agents.base import Confidence, MetadataSuggestion

    return MetadataSuggestion(
        schema="s",
        table="t",
        column="c",
        suggestions=alts,
        confidence=Confidence.MEDIUM,
        reasoning="",
        source="db_profile",
    )


def test_suggestion_scores_populated_with_no_logprobs():
    """When logprobs are missing, self-consistency alone fills the ensemble
    and ``suggestion_scores`` is set to a list of length N."""
    from amx.agents.base import apply_confidence_signals
    from amx.config import ConfidenceConfig

    cfg = ConfidenceConfig(use_self_consistency=False, use_logprob=False)
    s = _suggestion(["alpha", "beta"])
    out = apply_confidence_signals(
        suggestions=[s],
        logprobs_content=None,
        response_text=None,
        cfg=cfg,
    )
    assert out is not None
    assert s.suggestion_scores is not None
    assert len(s.suggestion_scores) == 2
    assert [score.text for score in s.suggestion_scores] == ["alpha", "beta"]


def test_apply_is_idempotent_and_safe_on_empty_suggestions():
    from amx.agents.base import apply_confidence_signals
    from amx.config import ConfidenceConfig

    out = apply_confidence_signals(
        suggestions=[],
        logprobs_content=None,
        response_text=None,
        cfg=ConfidenceConfig(),
    )
    assert out == []
