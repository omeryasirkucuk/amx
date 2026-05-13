"""Signal B: LLM self-declared confidence per alternative.

When ``cfg.confidence.use_self_decl`` is on, the system prompt emits
a ``CONFIDENCE_i: HIGH|MED|LOW`` marker right after each
``DESCRIPTION_i: …`` block. This module parses those markers out of
the raw response text and maps the bands to a numeric score:

* ``HIGH``   → 0.9
* ``MED`` (or ``MEDIUM``) → 0.6
* ``LOW``    → 0.3

Missing or unrecognised values become ``None`` so the ensemble falls
back to the remaining signals for that alternative. This signal is
notoriously poorly-calibrated on its own (models are overconfident),
but as part of the ensemble alongside logprob + self-consistency it
adds useful disagreement signal for the thesis evaluation in Phase 4.
"""

from __future__ import annotations

import re

_BAND_TO_SCORE = {
    "HIGH": 0.9,
    "MED": 0.6,
    "MEDIUM": 0.6,
    "LOW": 0.3,
}

#: Matches ``CONFIDENCE_<n>: <BAND>`` where ``<n>`` is a 1-based integer
#: and ``<BAND>`` is one of HIGH / MED / MEDIUM / LOW (case-insensitive).
#: The trailing ``\b`` keeps us from accidentally matching adjacent
#: words like ``HIGHLY``.
_CONFIDENCE_LINE = re.compile(
    r"^CONFIDENCE_(\d+)\s*:\s*([A-Za-z_]+)\b",
    re.IGNORECASE | re.MULTILINE,
)


def score_per_alternative(response_text: str | None, n: int) -> list[float | None]:
    """Return one self-declared confidence score per alternative.

    ``n`` is the number of alternatives the agent asked the model to
    produce. The returned list always has length ``n``; missing /
    unrecognised entries are ``None``.
    """
    out: list[float | None] = [None] * n
    if not response_text or n <= 0:
        return out
    for match in _CONFIDENCE_LINE.finditer(response_text):
        idx_raw, band_raw = match.group(1), match.group(2)
        try:
            idx = int(idx_raw)
        except ValueError:
            continue
        if idx < 1 or idx > n:
            continue
        score = _BAND_TO_SCORE.get(band_raw.upper())
        if score is None:
            continue
        out[idx - 1] = score
    return out


__all__ = ["score_per_alternative"]
