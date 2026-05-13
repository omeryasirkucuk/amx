"""Per-alternative confidence scoring for ``n_alternatives`` runs.

Each LLM-emitted suggestion block now carries one
:class:`AlternativeScore` per alternative description, holding both the
raw per-signal scores (logprob span, self-consistency, …) and the
combined ensemble + band that Studio and the CLI render.

Phase 1 enables Signals A (logprob span) and C (self-consistency); the
Signal B / D fields stay ``None`` until later phases wire them in.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AlternativeScore:
    """Per-alternative confidence signals + ensemble.

    Stored inside ``run_results.alternatives_json`` as the new
    structured shape:

        [{"text": ..., "scores": {...}, "ensemble": float, "band": str}, ...]
    """

    text: str
    logprob_score: float | None
    self_consistency_score: float | None
    self_decl_score: float | None
    judge_score: float | None
    ensemble_score: float
    band: str

    def to_json(self) -> dict[str, object]:
        return {
            "text": self.text,
            "scores": {
                "logprob": self.logprob_score,
                "self_consistency": self.self_consistency_score,
                "self_decl": self.self_decl_score,
                "judge": self.judge_score,
            },
            "ensemble": self.ensemble_score,
            "band": self.band,
        }


__all__ = ["AlternativeScore"]
