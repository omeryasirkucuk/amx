"""Score-fusion and diversity helpers for hybrid retrieval.

Two reranking strategies live here:

1. :func:`reciprocal_rank_fusion` (PR-E) — fuses several ranked
   candidate lists into one score-per-id dict. Rank-based;
   immune to score-scale drift between BM25 (negative magnitudes)
   and Chroma cosine distances (0..2). Reference: Cormack, Clarke,
   Büttcher (2009).

2. :func:`maximal_marginal_relevance` (PR-I) — picks a diverse
   top-k from a candidate pool by trading off relevance to the
   query against similarity to already-picked candidates. Reduces
   the rate at which the LLM sees three near-duplicate chunks
   from the same paragraph of one document instead of three
   chunks from three different documents.

Both functions are pure: no IO, no globals, deterministic.
"""

from __future__ import annotations

import math
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


def _cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    """Plain cosine similarity, ``-1 <= result <= 1``.

    Returns ``0.0`` when either vector is empty or has zero norm —
    treats degenerate inputs as \"neutral, neither similar nor
    dissimilar\" rather than raising. The MMR caller treats higher
    as more similar; returning ``0`` for a degenerate vector keeps
    it from being spuriously promoted or demoted.
    """
    if not vec_a or not vec_b:
        return 0.0
    n = min(len(vec_a), len(vec_b))
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for i in range(n):
        a = float(vec_a[i])
        b = float(vec_b[i])
        dot += a * b
        norm_a += a * a
        norm_b += b * b
    if norm_a <= 0.0 or norm_b <= 0.0:
        return 0.0
    return dot / (math.sqrt(norm_a) * math.sqrt(norm_b))


def maximal_marginal_relevance(
    *,
    candidates: Sequence[tuple[str, float, Sequence[float]]],
    k: int,
    lambda_: float = 0.7,
) -> list[str]:
    """Pick a diverse top-``k`` from ``candidates`` via MMR.

    Decouples *relevance* (already computed upstream, e.g. by the
    rerank step) from *similarity-to-selected* (cosine on cached
    chunk embeddings). MMR scores each unpicked candidate ``c``
    against the running selection ``S`` as:

        score(c) = lambda * relevance(c)
                 - (1 - lambda) * max_{s in S} sim(c, s)

    and greedily picks the highest-scoring candidate until ``k``
    are chosen or candidates run out.

    Parameters:
        candidates — list of ``(id, relevance, embedding)`` tuples
            in descending relevance order. The relevance score is
            whatever upstream signal you want to preserve: rerank
            heuristic, RRF fusion total, or raw cosine to the query.
            The embedding is the dense vector for diversity math.
            Decoupling relevance from the embedding lets MMR sit
            cleanly after any reranker without re-embedding the
            query.
        k — how many to pick.
        lambda_ — trade-off between relevance (``1.0`` → \"pure
            relevance, no diversity\") and diversity (``0.0`` →
            \"max-different, ignore relevance\"). ``0.7`` is a
            reasonable default — relevance-leaning but actively
            avoids near-duplicates.

    Returns a list of selected ids in pick order. Always a subset
    of (and order-preserving within) the input ids.

    Edge cases:
    - ``k <= 0`` → ``[]``.
    - empty ``candidates`` → ``[]``.
    - missing / empty embedding on a candidate → that candidate
      gets a similarity of ``0`` against everything; effectively
      treated as \"never near a duplicate\" and ranked by
      relevance only.
    - ``len(candidates) <= k`` → return every candidate (still in
      MMR-pick order).
    """
    if k <= 0 or not candidates:
        return []
    if not 0.0 <= lambda_ <= 1.0:
        raise ValueError(f"lambda_ must be in [0, 1], got {lambda_}")
    relevance: dict[str, float] = {cid: float(rel) for cid, rel, _emb in candidates}
    embeddings: dict[str, Sequence[float]] = {cid: emb for cid, _rel, emb in candidates}
    remaining_order: list[str] = [cid for cid, _, _ in candidates]

    selected: list[str] = []
    while remaining_order and len(selected) < k:
        if not selected:
            # First pick: pure relevance, since there's nothing to
            # diversify away from yet.
            best_id = max(remaining_order, key=lambda cid: relevance.get(cid, 0.0))
        else:
            best_id = None
            best_score = -math.inf
            for cid in remaining_order:
                rel = relevance.get(cid, 0.0)
                max_sim = max(_cosine_similarity(embeddings[cid], embeddings[s]) for s in selected)
                score = lambda_ * rel - (1.0 - lambda_) * max_sim
                if score > best_score:
                    best_score = score
                    best_id = cid
        selected.append(best_id)
        remaining_order.remove(best_id)
    return selected


__all__ = ["maximal_marginal_relevance", "reciprocal_rank_fusion"]
