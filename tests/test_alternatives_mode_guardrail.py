"""Mode-consistency guardrail (``amx.agents._mode_guardrail``)."""

from __future__ import annotations

from amx.agents._mode_guardrail import (
    LEXICAL_MAX_MEAN_SC,
    SEMANTIC_MIN_MEAN_SC,
    check_mode_consistency,
)


def _diag(**kwargs):
    return check_mode_consistency(
        asset_label="schema.table.col",
        confidence_signal="self_consistency",
        **kwargs,
    )


def test_semantic_high_sc_passes():
    """Realistic semantic mode (paraphrases) — high mean SC, no warning."""
    assert _diag(mode="semantic", sc_scores=[0.95, 0.93, 0.96]) is None


def test_semantic_low_sc_flags_inversion():
    """Semantic mode producing low mean SC suggests the LLM is
    generating meaning-shifted alts despite the paraphrase directive
    — possible inversion or weak steering."""
    diag = _diag(mode="semantic", sc_scores=[0.5, 0.4, 0.55])
    assert diag is not None
    assert "alternatives_mode_inversion_suspect" in diag
    assert "mode=semantic" in diag
    assert "per_alt_sc=" in diag


def test_semantic_floor_is_at_inclusive_boundary():
    """At exactly the floor, no warning (mean must be strictly < floor)."""
    assert _diag(mode="semantic", sc_scores=[SEMANTIC_MIN_MEAN_SC] * 3) is None


def test_lexical_moderate_sc_passes():
    """Realistic lexical mode (shifted-nuance alts) — moderate mean SC
    around 0.80–0.85, no warning."""
    assert _diag(mode="lexical", sc_scores=[0.82, 0.84, 0.80]) is None


def test_lexical_high_sc_flags_inversion():
    """Lexical mode producing near-paraphrase SC suggests the LLM
    collapsed into semantic behaviour despite the lexical directive."""
    diag = _diag(mode="lexical", sc_scores=[0.95, 0.93, 0.92])
    assert diag is not None
    assert "alternatives_mode_inversion_suspect" in diag
    assert "mode=lexical" in diag


def test_lexical_ceiling_is_at_inclusive_boundary():
    """At exactly the ceiling, no warning (mean must be strictly > ceiling)."""
    assert _diag(mode="lexical", sc_scores=[LEXICAL_MAX_MEAN_SC] * 3) is None


def test_no_warning_for_non_self_consistency_signal():
    """The guardrail's mean-SC interpretation only holds when the
    active signal IS self-consistency (cosine = semantic similarity).
    Other signals measure different things; do not flag."""
    diag = check_mode_consistency(
        asset_label="t",
        mode="semantic",
        confidence_signal="logprob",
        sc_scores=[0.4, 0.3, 0.5],
    )
    assert diag is None


def test_no_warning_when_mode_missing():
    """Legacy rows or n=1 runs have no mode contract to check."""
    diag = check_mode_consistency(
        asset_label="t",
        mode=None,
        confidence_signal="self_consistency",
        sc_scores=[0.4, 0.3, 0.5],
    )
    assert diag is None


def test_no_warning_when_too_few_scores():
    """A single score has no mean-to-spread information."""
    diag = check_mode_consistency(
        asset_label="t",
        mode="semantic",
        confidence_signal="self_consistency",
        sc_scores=[0.4],
    )
    assert diag is None


def test_handles_none_scores_in_mix():
    """Some alternatives may carry ``None`` (scorer unavailable for
    that row). The guardrail averages the numeric ones and ignores
    the gaps."""
    diag = _diag(mode="semantic", sc_scores=[0.95, None, 0.93])
    assert diag is None  # mean of 0.95 and 0.93 = 0.94, above floor
