"""Signal C: self-consistency across alternatives.

For ``N >= 2`` alternatives, embed each one and score every alternative
as its **mean cosine similarity to the other N-1**. An outlier
description (semantically distant from the rest) receives a low score;
the "centroid" cluster receives high scores.

For ``N == 1`` the signal is degenerate; we return ``1.0`` so the
ensemble math stays well-defined.

The embedding model is ``sentence-transformers/all-MiniLM-L6-v2``
(~80 MB, CPU-friendly, deterministic offline). The first invocation in
a fresh environment triggers a one-time download via
``sentence-transformers``; ``amx.utils.optional_deps.ensure`` handles
the pip install when the package itself is missing.
"""

from __future__ import annotations

import math
import threading
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("confidence.self_consistency")

_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
_model_lock = threading.Lock()
_model: Any | None = None


def _load_model() -> Any | None:
    """Return a cached sentence-transformers model, or ``None`` on failure.

    Returning ``None`` lets the orchestrator treat self-consistency as
    "signal unavailable" and degrade gracefully to the remaining signals
    rather than aborting the entire run.
    """
    global _model
    if _model is not None:
        return _model
    with _model_lock:
        if _model is not None:
            return _model
        try:
            from amx.utils.optional_deps import ensure

            ensure("local_embeddings", feature="self-consistency confidence")
            from sentence_transformers import SentenceTransformer

            _model = SentenceTransformer(_MODEL_ID)
        except Exception as exc:  # pragma: no cover — environmental
            log.warning("Self-consistency model load failed: %s", exc)
            _model = None
        return _model


def _cosine(vec_a: list[float], vec_b: list[float]) -> float:
    dot = 0.0
    na = 0.0
    nb = 0.0
    for a, b in zip(vec_a, vec_b, strict=False):
        dot += a * b
        na += a * a
        nb += b * b
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


def score_per_alternative(alternatives: list[str]) -> list[float | None]:
    """Return one self-consistency score per alternative.

    Conventions:
      * Empty input → empty list.
      * ``N == 1`` → ``[1.0]`` (degenerate, no comparison possible).
      * Model unavailable / load fails → ``[None, …]`` of length N.
      * Score is the mean cosine similarity to the other N-1
        alternatives, clamped to ``[0.0, 1.0]``.
    """
    n = len(alternatives)
    if n == 0:
        return []
    if n == 1:
        return [1.0]

    model = _load_model()
    if model is None:
        return [None] * n

    try:
        embeddings_raw = model.encode(alternatives, normalize_embeddings=False)
    except Exception as exc:  # pragma: no cover — model-level failure
        log.warning("Self-consistency encode failed: %s", exc)
        return [None] * n

    embeddings = [list(map(float, row)) for row in embeddings_raw]

    scores: list[float | None] = []
    for i, vec_i in enumerate(embeddings):
        sims = [_cosine(vec_i, embeddings[j]) for j in range(n) if j != i]
        if not sims:
            scores.append(1.0)
            continue
        mean_sim = sum(sims) / len(sims)
        scores.append(max(0.0, min(1.0, mean_sim)))
    return scores


__all__ = ["score_per_alternative"]
