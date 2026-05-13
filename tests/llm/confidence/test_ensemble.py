"""Ensemble normalisation and band-cutoff tests."""

from __future__ import annotations


def test_single_signal_all_none_returns_zero_low():
    from amx.llm.confidence.ensemble import build_alternative_scores

    out = build_alternative_scores(
        alternatives=["a", "b"],
        signals={
            "logprob": [None, None],
            "self_consistency": [None, None],
        },
        thresholds=(0.75, 0.50),
    )
    assert [s.ensemble_score for s in out] == [0.0, 0.0]
    assert [s.band for s in out] == ["LOW", "LOW"]


def test_min_max_normalises_within_signal():
    from amx.llm.confidence.ensemble import build_alternative_scores

    out = build_alternative_scores(
        alternatives=["high", "low"],
        signals={
            "logprob": [0.9, 0.5],
            "self_consistency": [None, None],
        },
        thresholds=(0.75, 0.50),
    )
    assert out[0].ensemble_score == 1.0
    assert out[1].ensemble_score == 0.0
    assert out[0].band == "HIGH"
    assert out[1].band == "LOW"


def test_two_signals_average_after_normalisation():
    from amx.llm.confidence.ensemble import build_alternative_scores

    out = build_alternative_scores(
        alternatives=["a", "b"],
        signals={
            "logprob": [0.8, 0.4],  # → 1.0, 0.0
            "self_consistency": [0.6, 0.8],  # → 0.0, 1.0
        },
        thresholds=(0.75, 0.50),
    )
    assert out[0].ensemble_score == 0.5
    assert out[1].ensemble_score == 0.5
    assert out[0].band == "MED"
    assert out[1].band == "MED"


def test_raw_signal_values_preserved_on_dataclass():
    from amx.llm.confidence.ensemble import build_alternative_scores

    out = build_alternative_scores(
        alternatives=["a", "b"],
        signals={
            "logprob": [0.82, None],
            "self_consistency": [0.71, 0.30],
        },
        thresholds=(0.75, 0.50),
    )
    assert out[0].logprob_score == 0.82
    assert out[0].self_consistency_score == 0.71
    assert out[1].logprob_score is None
    assert out[1].self_consistency_score == 0.30
    assert out[0].self_decl_score is None
    assert out[0].judge_score is None


def test_band_cutoffs_are_inclusive_at_thresholds():
    from amx.llm.confidence.ensemble import _band

    assert _band(0.751, 0.75, 0.50) == "HIGH"
    assert _band(0.75, 0.75, 0.50) == "HIGH"
    assert _band(0.7499, 0.75, 0.50) == "MED"
    assert _band(0.50, 0.75, 0.50) == "MED"
    assert _band(0.4999, 0.75, 0.50) == "LOW"
