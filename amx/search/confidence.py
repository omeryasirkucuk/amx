"""Map composite ``rank_score`` values to a 3-band label users can read.

The numbers here come from the scoring formula in :mod:`amx.search.catalog`:
exact column-name equality contributes ``+12.0`` on its own; prefix/contains
hits land in the 7–11 band; vector-only matches and weak keyword smear
contribute under 4. The thresholds below split that distribution into a
shape end-users can interpret without reading the code.
"""

from __future__ import annotations

BAND_HIGH = "High"
BAND_MEDIUM = "Medium"
BAND_LOW = "Low"

# (high_threshold, medium_threshold). Tuned to the catalog scoring formula.
THRESHOLDS = (12.0, 6.0)


def band(score: float | int | None) -> str:
    try:
        value = float(score or 0.0)
    except (TypeError, ValueError):
        return BAND_LOW
    if value >= THRESHOLDS[0]:
        return BAND_HIGH
    if value >= THRESHOLDS[1]:
        return BAND_MEDIUM
    return BAND_LOW


def band_style(b: str) -> str:
    return {BAND_HIGH: "bold green", BAND_MEDIUM: "yellow", BAND_LOW: "dim"}.get(b, "white")
