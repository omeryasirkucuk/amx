"""Per-alternative confidence scoring for ``n_alternatives`` runs.

Each LLM-emitted suggestion block now carries one
:class:`AlternativeScore` per alternative description. The user picks a
single active signal (``confidence_signal`` on the LLM profile);
:func:`amx.llm.confidence.scorer.score_alternatives` runs that signal,
maps the raw 0–1 score to a HIGH / MED / LOW band by
:func:`amx.llm.confidence.band.band_for`, and stores the result here.

When the active signal is ``"none"`` no scoring runs and the
``suggestion_scores`` field on ``MetadataSuggestion`` stays ``None`` so
the existing storage / UI back-compat path (legacy ``list[str]``
``alternatives_json``) takes over.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AlternativeScore:
    """One alternative's active-signal score plus its HIGH/MED/LOW band.

    ``signal`` records which scorer produced the value so the UI can show
    "SC: HIGH 0.78" style labels and the Phase 4 eval harness can group
    rows by the signal that was active when each row was scored. When
    the signal is unavailable for a row (e.g. logprob on a provider that
    does not return token logprobs) ``score`` is ``None`` and ``band``
    is ``"—"``.
    """

    text: str
    signal: str
    score: float | None
    band: str

    def to_json(self) -> dict[str, object]:
        return {
            "text": self.text,
            "signal": self.signal,
            "score": self.score,
            "band": self.band,
        }


__all__ = ["AlternativeScore"]
