"""Cross-encoder rerank for Document RAG (PR-F, opt-in).

The heuristic reranker in ``amx.docs.rag.RAGStore.rerank`` does a
reasonable job for prose-heavy corpora (distance + token overlap +
explanatory-term bonus) but is structurally blind: it can't tell
that a chunk talking about "pricing rules" matches a query about
"how is the price computed" more closely than one literally
containing "price" three times.

A cross-encoder rerank fixes that. Cross-encoders take both the
query and a candidate chunk as a *joint* input and produce a single
relevance score — a much stronger signal than bi-encoder cosine.

This module is opt-in via ``cfg.docs.rerank.kind``:

| kind | model | size | notes |
| --- | --- | --- | --- |
| ``heuristic`` | n/a | 0 | Default. The existing in-process scorer. |
| ``cross_encoder`` | ``cross-encoder/ms-marco-MiniLM-L-6-v2`` | ~80 MB | English. Recommended default for amx[local-embeddings] users. |
| ``cross_encoder_multilingual`` | ``BAAI/bge-reranker-v2-m3`` | ~568 MB | Multilingual fallback. Heavy; only useful for non-English corpora. |

The heuristic remains the fallback for every failure mode:
- ``sentence-transformers`` not installed → heuristic.
- Model download blocked / disk full → heuristic.
- Cross-encoder construction raises for any reason → heuristic.

We do not enable cross-encoder by default because (a) it requires
the ``local-embeddings`` extra, and (b) it adds 30-200ms of latency
per query depending on candidate pool size. Power users opt in;
the default stays fast and offline.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("rag_core.rerank")

# Registry of supported model identifiers. Keeping these in one
# place makes the ``cfg.docs.rerank.kind`` switch trivial and
# survives future tuning (e.g. swapping to a newer MiniLM variant).
MODEL_FOR_KIND: dict[str, str] = {
    "cross_encoder": "cross-encoder/ms-marco-MiniLM-L-6-v2",
    "cross_encoder_multilingual": "BAAI/bge-reranker-v2-m3",
}


class CrossEncoderReranker:
    """Lazy wrapper around ``sentence-transformers``' ``CrossEncoder``.

    Construction is cheap — the model isn't loaded until the first
    ``rerank`` call. That keeps ``RAGStore`` bootstrap fast even
    when cross-encoder rerank is wired in but not exercised yet
    (e.g. process imports the agent but never calls /docs search).

    The fallback is **silent at the retrieval-quality layer**: if
    the model fails to load (no ``sentence-transformers``, no
    network, no disk), the reranker returns the input list
    unchanged so the caller's existing heuristic rerank stays the
    load-bearing path.
    """

    def __init__(self, model_id: str) -> None:
        if not model_id:
            raise ValueError("CrossEncoderReranker requires a non-empty model id")
        self._model_id = model_id
        self._model: Any | None = None  # lazy
        self._load_failed = False

    @property
    def model_id(self) -> str:
        return self._model_id

    def _ensure_model(self) -> Any | None:
        """Return the CrossEncoder instance, loading on first call.
        Returns ``None`` on load failure so callers fall back to
        their existing rerank path."""
        if self._load_failed:
            return None
        if self._model is not None:
            return self._model
        try:
            from sentence_transformers import CrossEncoder
        except ImportError as exc:
            log.warning(
                "CrossEncoderReranker(%s): sentence-transformers not installed (%s); "
                "falling back to heuristic rerank. Install `amx-cli[local-embeddings]`.",
                self._model_id,
                exc,
            )
            self._load_failed = True
            return None
        try:
            self._model = CrossEncoder(self._model_id)
        except Exception as exc:  # noqa: BLE001 — broad on purpose
            log.warning(
                "CrossEncoderReranker(%s): could not load model (%s: %s); "
                "falling back to heuristic rerank.",
                self._model_id,
                exc.__class__.__name__,
                exc,
            )
            self._load_failed = True
            self._model = None
        return self._model

    def rerank(
        self,
        question: str,
        hits: Sequence[dict],
    ) -> list[dict]:
        """Re-score ``hits`` with the cross-encoder, sort descending.

        Each input hit is expected to have a ``text`` field. The
        function returns a new list of the same hits, sorted by the
        cross-encoder score (also stamped onto each hit under
        ``score`` — replaces the heuristic score if present).

        Empty ``hits`` returns ``[]`` without invoking the model.
        Model load failure returns the input as-is (preserving the
        caller's existing ordering, typically the heuristic
        rerank's).
        """
        if not hits:
            return []
        model = self._ensure_model()
        if model is None:
            return list(hits)
        pairs = [(str(question or ""), str(hit.get("text") or "")) for hit in hits]
        try:
            scores = model.predict(pairs, convert_to_numpy=True)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "CrossEncoderReranker(%s).predict failed: %s; returning input ordering unchanged.",
                self._model_id,
                exc,
            )
            return list(hits)
        # Update each hit's ``score`` to the cross-encoder output
        # so downstream code (citation header, MMR relevance
        # weighting, snapshot writers) sees the better signal.
        scored: list[dict] = []
        for hit, raw_score in zip(hits, scores, strict=False):
            new_hit = dict(hit)
            try:
                new_hit["score"] = float(raw_score)
            except (TypeError, ValueError):
                new_hit["score"] = 0.0
            scored.append(new_hit)
        scored.sort(key=lambda h: h["score"], reverse=True)
        return scored


def reranker_from_kind(kind: str) -> CrossEncoderReranker | None:
    """Factory: build a reranker for the given ``cfg.docs.rerank.kind``.

    Returns ``None`` for ``heuristic`` (the caller's existing path)
    or any unrecognised kind. Construction never raises — model
    loading is lazy.
    """
    key = (kind or "").strip().lower()
    if not key or key == "heuristic":
        return None
    model_id = MODEL_FOR_KIND.get(key)
    if not model_id:
        log.warning(
            "Unknown rerank kind %r; falling back to heuristic. Valid kinds: %s",
            kind,
            ", ".join(sorted(["heuristic", *MODEL_FOR_KIND.keys()])),
        )
        return None
    return CrossEncoderReranker(model_id=model_id)


__all__ = [
    "MODEL_FOR_KIND",
    "CrossEncoderReranker",
    "reranker_from_kind",
]
