"""Absolute-threshold band cut-offs (replaces ensemble + normalisation tests)."""

from __future__ import annotations


def test_band_for_high_is_inclusive_at_threshold():
    from amx.llm.confidence.band import band_for

    assert band_for(0.751, 0.75, 0.50) == "HIGH"
    assert band_for(0.75, 0.75, 0.50) == "HIGH"


def test_band_for_med_is_inclusive_at_threshold():
    from amx.llm.confidence.band import band_for

    assert band_for(0.7499, 0.75, 0.50) == "MED"
    assert band_for(0.50, 0.75, 0.50) == "MED"


def test_band_for_low_below_med_cutoff():
    from amx.llm.confidence.band import band_for

    assert band_for(0.4999, 0.75, 0.50) == "LOW"
    assert band_for(0.0, 0.75, 0.50) == "LOW"


def test_band_for_none_returns_em_dash():
    from amx.llm.confidence.band import BAND_UNAVAILABLE, band_for

    assert band_for(None, 0.75, 0.50) == BAND_UNAVAILABLE
    assert BAND_UNAVAILABLE == "—"


def test_band_for_custom_thresholds():
    """Profiles can override band cut-offs via ConfidenceConfig.high / .med."""
    from amx.llm.confidence.band import band_for

    assert band_for(0.81, 0.80, 0.60) == "HIGH"
    assert band_for(0.79, 0.80, 0.60) == "MED"
    assert band_for(0.59, 0.80, 0.60) == "LOW"
