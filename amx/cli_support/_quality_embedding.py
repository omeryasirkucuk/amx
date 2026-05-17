"""Embedding-based quality signals for ``amx compare``.

Extracted from :mod:`amx.cli_support.quality`. The four functions
form the Tier-1 (local-embedding) layer of the quality framework:

- ``_load_sentence_embedder`` lazy-loads
  :class:`sentence_transformers.SentenceTransformer` (default
  ``all-MiniLM-L6-v2``).
- ``_cosine`` — stdlib cosine similarity over two python lists.
- ``embedding_agreement_for_asset`` — pairwise cosine matrix across
  multiple candidate descriptions for the same asset.
- ``semantic_grounding_score`` — cosine between the description text
  and the asset's structural signal tokens.

``quality.py`` re-exports each public name so the existing call site
(``compute_quality_metrics`` lower in the same module) and any
future direct importers keep working.
"""

from __future__ import annotations

import math
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("cli_support.quality.embedding")


def _load_sentence_embedder(
    model_name: str = "all-MiniLM-L6-v2",
) -> Any | None:
    """Lazy-load a sentence-transformers model. Returns ``None`` when
    the package isn't installed (caller falls through gracefully).
    """
    try:
        from amx.utils.optional_deps import ensure

        ensure(
            [("sentence_transformers", "sentence-transformers")],
            feature="Compare quality embeddings",
        )
    except RuntimeError:
        # ``ensure`` raises when pip install fails; treat as missing dep.
        return None
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return None
    return SentenceTransformer(model_name)


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def embedding_agreement_for_asset(
    descriptions_by_run: dict[int, str],
    embedder: Any,
) -> dict[int, float]:
    """For each run, mean cosine similarity to every other run on the
    same asset. High = the run agrees with the consensus; low = outlier.
    Empty descriptions are skipped from the matrix.
    """
    valid = {rid: t for rid, t in descriptions_by_run.items() if t}
    if len(valid) < 2:
        return dict.fromkeys(descriptions_by_run, 0.0)
    rids = sorted(valid.keys())
    texts = [valid[r] for r in rids]
    vectors = embedder.encode(texts, show_progress_bar=False).tolist()
    by_run: dict[int, list[float]] = dict(zip(rids, vectors, strict=False))
    agreement: dict[int, float] = {}
    for rid in rids:
        sims = [_cosine(by_run[rid], by_run[other]) for other in rids if other != rid]
        agreement[rid] = sum(sims) / float(len(sims)) if sims else 0.0
    # Runs whose description was empty get 0 agreement so the UI can
    # differentiate "missing" from "outlier".
    for rid in descriptions_by_run:
        agreement.setdefault(rid, 0.0)
    return agreement


def semantic_grounding_score(
    description: str,
    *,
    schema: str | None,
    table: str | None,
    column: str | None,
    dtype: str | None,
    embedder: Any,
) -> float:
    """Embedding-based version of schema grounding: how close is the
    description to a synthetic schema-anchor sentence?
    """
    if not description:
        return 0.0
    parts = [p for p in (schema, table, column) if p]
    anchor = ".".join(parts) if parts else ""
    if dtype:
        anchor = f"{anchor} ({dtype})"
    if not anchor:
        return 0.0
    vec_anchor, vec_desc = embedder.encode([anchor, description], show_progress_bar=False).tolist()
    return max(0.0, _cosine(vec_anchor, vec_desc))
