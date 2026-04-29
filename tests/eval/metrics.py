"""Scoring metrics for retrieval evaluation.

The functions here take a *ranked list of result identifiers* (model
output) and a set of *relevant identifiers* (ground truth) and return
a single scalar so retrieval changes (a new embedding model, a
different distance threshold, a new chunking scheme) can be compared
on apples-to-apples terms.

All functions are pure: no global state, no IO, deterministic.

Identifiers are compared with ``==``; lowercase + strip is the caller's
responsibility so the metrics never silently rewrite case-sensitive
ids.

Example::

    >>> hit_at_k(["users", "orders", "events"], {"orders"}, k=2)
    1.0
    >>> hit_at_k(["users", "orders", "events"], {"orders"}, k=1)
    0.0
    >>> mean_reciprocal_rank(
    ...     [
    ...         (["users", "orders"], {"orders"}),
    ...         (["events", "audit"], {"audit"}),
    ...     ]
    ... )
    0.5
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from math import log2


def hit_at_k(
    ranked: Sequence[str],
    relevant: Iterable[str],
    *,
    k: int,
) -> float:
    """Return ``1.0`` if any relevant id appears in the first *k* results,
    else ``0.0``. This is the simplest possible signal — does retrieval
    surface *any* correct answer in the top-k window?
    """
    if k <= 0:
        raise ValueError("k must be positive")
    relevant_set = set(relevant)
    if not relevant_set:
        return 0.0
    for item in ranked[:k]:
        if item in relevant_set:
            return 1.0
    return 0.0


def reciprocal_rank(
    ranked: Sequence[str],
    relevant: Iterable[str],
) -> float:
    """Return ``1 / position`` of the first relevant id (1-indexed), or
    ``0.0`` if no relevant id appears anywhere in *ranked*. RR is the
    per-query value you average to get MRR.
    """
    relevant_set = set(relevant)
    if not relevant_set:
        return 0.0
    for index, item in enumerate(ranked, start=1):
        if item in relevant_set:
            return 1.0 / float(index)
    return 0.0


def mean_reciprocal_rank(
    queries: Iterable[tuple[Sequence[str], Iterable[str]]],
) -> float:
    """Average :func:`reciprocal_rank` across many ``(ranked, relevant)``
    pairs. Empty input returns ``0.0`` rather than raising — eval scripts
    typically iterate over fixture files and may legitimately encounter
    a zero-length set.
    """
    rrs: list[float] = []
    for ranked, relevant in queries:
        rrs.append(reciprocal_rank(ranked, relevant))
    if not rrs:
        return 0.0
    return sum(rrs) / float(len(rrs))


def ndcg_at_k(
    ranked: Sequence[str],
    relevant: Iterable[str],
    *,
    k: int,
) -> float:
    """Normalised Discounted Cumulative Gain at *k* with binary relevance.

    Each relevant hit at position *i* (1-indexed) contributes
    ``1 / log2(i + 1)``. The result is normalised by the ideal DCG —
    the score you would get if every relevant id ranked above every
    irrelevant id — so the value lies in ``[0.0, 1.0]``.

    Returns ``0.0`` when there are no relevant ids or when *k* admits
    no candidates.
    """
    if k <= 0:
        raise ValueError("k must be positive")
    relevant_set = set(relevant)
    if not relevant_set:
        return 0.0
    truncated = list(ranked[:k])
    if not truncated:
        return 0.0

    dcg = 0.0
    for index, item in enumerate(truncated, start=1):
        if item in relevant_set:
            dcg += 1.0 / log2(index + 1)

    ideal_hits = min(len(relevant_set), k)
    if ideal_hits == 0:
        return 0.0
    idcg = sum(1.0 / log2(i + 1) for i in range(1, ideal_hits + 1))
    if idcg == 0.0:
        return 0.0
    return dcg / idcg


def precision_at_k(
    ranked: Sequence[str],
    relevant: Iterable[str],
    *,
    k: int,
) -> float:
    """Fraction of the top-*k* that are relevant. Useful when callers
    care about the *purity* of the candidate window, not just whether
    one correct hit slipped in.
    """
    if k <= 0:
        raise ValueError("k must be positive")
    relevant_set = set(relevant)
    if not relevant_set:
        return 0.0
    window = list(ranked[:k])
    if not window:
        return 0.0
    hits = sum(1 for item in window if item in relevant_set)
    return float(hits) / float(len(window))
