"""Score-fusion helpers for hybrid retrieval (PR-E).

Reciprocal Rank Fusion (RRF) combines several rankings of the same
candidate pool into one scored dict that gracefully handles
heterogeneous score scales — BM25 scores (negative, magnitude-
specific) and Chroma cosine distances (0..2) live in different
ranges, so naive linear combination would require per-source
calibration. RRF instead consumes only the *position* of each
candidate in each input ranking, which is dimensionless by
construction.

Reference: Cormack, Clarke, Büttcher (2009), \"Reciprocal Rank Fusion
outperforms Condorcet and individual Rank Learning Methods.\"

The recommended constant ``k = 60`` is from the original paper;
larger values flatten the score curve (each rank contributes more
similar weight), smaller values sharpen it (top ranks dominate
more). The default is what every production hybrid-retrieval
implementation we surveyed uses.
"""

from __future__ import annotations

from collections.abc import Sequence


def reciprocal_rank_fusion(
    rankings: Sequence[Sequence[str]],
    *,
    k: int = 60,
) -> dict[str, float]:
    """Fuse multiple input rankings into one score-per-id dict.

    Each input ranking is a sequence of candidate ids in best-to-
    worst order (rank 1 = first element). The output dict maps each
    candidate id to the sum of ``1 / (k + rank)`` across every input
    ranking that contains it. Candidates absent from an input
    contribute zero from that source — they're penalised relative
    to candidates that appear in multiple rankings, which is the
    whole point.

    The dict is NOT sorted; callers should ``sorted(scores.items(),
    key=lambda kv: kv[1], reverse=True)`` themselves so they choose
    the tie-breaking rule (and ``dict`` preserves insertion order
    which the caller almost certainly does not want here).

    Empty input → empty output (no rankings, no scores).
    """
    if k <= 0:
        raise ValueError(f"k must be positive, got {k}")
    scores: dict[str, float] = {}
    for ranking in rankings:
        for rank, candidate in enumerate(ranking, start=1):
            scores[candidate] = scores.get(candidate, 0.0) + 1.0 / float(k + rank)
    return scores


__all__ = ["reciprocal_rank_fusion"]
