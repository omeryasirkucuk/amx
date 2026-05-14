"""Map a raw confidence score to a HIGH / MED / LOW band.

The single-signal pivot dropped the min-max-normalise + average step.
The active signal returns a raw 0–1 score per alternative; this module
turns that score into one of three band labels using absolute cut-offs
from ``ConfidenceConfig`` (defaults: ``high=0.75``, ``med=0.50``).
"""

from __future__ import annotations

#: Returned in place of a band when the score is ``None`` (signal
#: unavailable on this row, e.g. logprob on a provider that does not
#: surface token logprobs). Renders as an em-dash in Studio and CLI.
BAND_UNAVAILABLE = "—"


def band_for(score: float | None, high: float, med: float) -> str:
    """Return ``"HIGH"`` / ``"MED"`` / ``"LOW"`` / ``"—"`` for a score.

    Cut-offs are inclusive at the boundary: ``score >= high`` is HIGH,
    ``score >= med`` is MED, anything below ``med`` is LOW. ``None``
    short-circuits to :data:`BAND_UNAVAILABLE` so callers do not need to
    null-check before calling.
    """
    if score is None:
        return BAND_UNAVAILABLE
    if score >= high:
        return "HIGH"
    if score >= med:
        return "MED"
    return "LOW"


__all__ = ["band_for", "BAND_UNAVAILABLE"]
