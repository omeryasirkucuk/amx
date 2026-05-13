"""Combine per-alternative confidence signals into an ensemble score + band."""

from __future__ import annotations

from amx.llm.confidence import AlternativeScore

# Mapping from public signal name (used in JSON) → AlternativeScore attribute.
_SIGNAL_TO_FIELD = {
    "logprob": "logprob_score",
    "self_consistency": "self_consistency_score",
    "self_decl": "self_decl_score",
    "judge": "judge_score",
}


def _band(ensemble: float, high: float, med: float) -> str:
    if ensemble >= high:
        return "HIGH"
    if ensemble >= med:
        return "MED"
    return "LOW"


def _min_max_normalise(values: list[float | None]) -> list[float | None]:
    """Normalise a single signal's raw values across alternatives to [0, 1].

    All-None: returns the same list unchanged. Single non-None value:
    that alternative maps to 1.0 (it is the only signal-bearing one).
    Identical non-None values across the list: every present entry
    maps to 1.0 (the signal carries no discriminating information).
    """
    present = [v for v in values if v is not None]
    if not present:
        return values
    lo = min(present)
    hi = max(present)
    if hi <= lo:
        return [None if v is None else 1.0 for v in values]
    return [None if v is None else (v - lo) / (hi - lo) for v in values]


def _aligned_raw(raw: list[float | None] | None, n: int) -> list[float | None]:
    """Pad / truncate a raw-signal list to exactly N entries."""
    if raw is None:
        return [None] * n
    return (list(raw) + [None] * n)[:n]


def build_alternative_scores(
    alternatives: list[str],
    signals: dict[str, list[float | None]],
    thresholds: tuple[float, float] = (0.75, 0.50),
) -> list[AlternativeScore]:
    """Build per-alternative ``AlternativeScore`` instances.

    Parameters
    ----------
    alternatives:
        Ordered list of description strings.
    signals:
        Mapping from signal name to a list of raw scores aligned with
        ``alternatives``. Allowed keys: ``logprob``, ``self_consistency``,
        ``self_decl``, ``judge``. Missing keys are treated as
        all-None. Phase 1 always passes only ``logprob`` and
        ``self_consistency``.
    thresholds:
        ``(high, med)`` cutoffs for the band. Defaults follow the spec.
    """
    n = len(alternatives)
    high, med = thresholds

    raw_by_name: dict[str, list[float | None]] = {
        name: _aligned_raw(signals.get(name), n) for name in _SIGNAL_TO_FIELD
    }
    normalised: dict[str, list[float | None]] = {
        name: _min_max_normalise(raw_by_name[name]) for name in _SIGNAL_TO_FIELD
    }

    results: list[AlternativeScore] = []
    for i, text in enumerate(alternatives):
        active = [normalised[name][i] for name in _SIGNAL_TO_FIELD if normalised[name][i] is not None]
        ensemble = sum(active) / len(active) if active else 0.0
        results.append(
            AlternativeScore(
                text=text,
                logprob_score=raw_by_name["logprob"][i],
                self_consistency_score=raw_by_name["self_consistency"][i],
                self_decl_score=raw_by_name["self_decl"][i],
                judge_score=raw_by_name["judge"][i],
                ensemble_score=ensemble,
                band=_band(ensemble, high, med),
            )
        )
    return results


__all__ = ["build_alternative_scores", "_band"]
