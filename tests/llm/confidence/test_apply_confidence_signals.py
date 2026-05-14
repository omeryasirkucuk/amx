"""Apply confidence signals to a list of MetadataSuggestions."""

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


def test_suggestion_scores_populated_when_signal_disabled():
    """When ``confidence_signal == 'none'`` the scorer skips work and
    ``suggestion_scores`` stays at the empty-list sentinel; storage
    serialisation then falls back to legacy ``list[str]``."""
    from amx.agents.base import apply_confidence_signals
    from amx.config import LLMConfig

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.confidence_signal = "none"
    s = _suggestion(["alpha", "beta"])
    out = apply_confidence_signals(
        suggestions=[s],
        logprobs_content=None,
        response_text=None,
        cfg=cfg,
    )
    assert out is not None
    # Scorer returns [] for disabled paths; that empty list is the
    # signal to the storage layer that no structured payload exists.
    assert s.suggestion_scores == []


def test_apply_is_idempotent_and_safe_on_empty_suggestions():
    from amx.agents.base import apply_confidence_signals
    from amx.config import LLMConfig

    out = apply_confidence_signals(
        suggestions=[],
        logprobs_content=None,
        response_text=None,
        cfg=LLMConfig(provider="openai", model="gpt-4o-mini"),
    )
    assert out == []


def test_apply_with_active_signal_populates_alternative_scores():
    """When the active signal can be patched to return deterministic
    scores, ``apply_confidence_signals`` writes one AlternativeScore per
    alternative carrying the chosen ``signal`` name."""
    from unittest.mock import patch

    from amx.agents.base import apply_confidence_signals
    from amx.config import LLMConfig

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.confidence_signal = "logprob"

    s = _suggestion(["alpha", "beta"])
    with patch(
        "amx.llm.confidence.logprob_span.score_per_alternative",
        return_value=[0.82, 0.40],
    ):
        apply_confidence_signals(
            suggestions=[s],
            logprobs_content=["dummy"],
            response_text="alpha beta",
            cfg=cfg,
        )

    assert s.suggestion_scores is not None
    assert [score.signal for score in s.suggestion_scores] == ["logprob", "logprob"]
    assert [score.score for score in s.suggestion_scores] == [0.82, 0.40]
    assert [score.band for score in s.suggestion_scores] == ["HIGH", "LOW"]
