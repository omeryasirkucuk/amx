"""Shared helpers for converting RAG retrieval hits into :class:`Citation`.

PR γ extracts the docs-RAG citation helpers so the :class:`CodeAgent`
can produce citations in exactly the same shape as :class:`RAGAgent`.
The contract is: every hit dict produced by Chroma-backed retrieval
(``{text|document, metadata, score|distance}``) becomes one
:class:`Citation` whose ``line_range`` is populated from the metadata's
``start_line`` / ``end_line`` keys when they exist (code chunks from
:mod:`amx.codebase.code_rag`) and left ``None`` otherwise (doc chunks
from :mod:`amx.docs.rag`, which only carry ``chunk_idx``).
"""

from __future__ import annotations

from amx.agents.base import Citation, MetadataSuggestion


def _meta_line_range(meta: dict) -> tuple[int, int] | None:
    """Pull ``(start_line, end_line)`` from chunk metadata, ``None`` when absent.

    Both keys must be present and parseable as ``int`` -- a partial
    metadata row (e.g. legacy chunks indexed before PR γ) falls back to
    ``None`` so the renderer can choose ``path:chunk_idx`` instead.
    """
    start_raw = meta.get("start_line")
    end_raw = meta.get("end_line")
    if start_raw is None and end_raw is None:
        return None
    try:
        start = int(start_raw) if start_raw is not None else 0
        end = int(end_raw) if end_raw is not None else start
    except (TypeError, ValueError):
        return None
    if start <= 0 and end <= 0:
        return None
    return (start, end)


def hits_to_citations(prompt_hits: list[dict]) -> list[Citation]:
    """Convert prompt hits into deduplicated :class:`Citation` records.

    Hits are deduped by ``(source, chunk_idx, line_range)`` so a chunk
    surfaced twice by the table-level + per-column queries renders as a
    single citation. ``source`` falls back to the metadata's
    ``rel_path`` when the absolute ``source`` key is missing, mirroring
    how the code-RAG tool returns hits.
    """
    citations: list[Citation] = []
    seen: set[tuple[str, int, tuple[int, int] | None]] = set()
    for h in prompt_hits or []:
        meta = h.get("metadata") or {}
        # Prefer ``rel_path`` for code-RAG hits so citations render as
        # ``src/foo.py:120-145`` instead of the noisier absolute
        # ``/Users/.../src/foo.py:120-145``. Fall back to ``source``
        # (the absolute path stamped at ingest) for docs-RAG and any
        # other producer that did not set ``rel_path``.
        source = str(meta.get("rel_path") or meta.get("source") or "").strip()
        if not source:
            continue
        chunk_id_raw = meta.get("chunk_idx") or meta.get("chunk_id") or 0
        try:
            chunk_idx = int(chunk_id_raw)
        except (TypeError, ValueError):
            # Code-RAG stores ``chunk_id`` as a string (e.g.
            # ``"my_func_42"``); fall back to 0 because the
            # ``line_range`` already carries the real provenance.
            chunk_idx = 0
        line_range = _meta_line_range(meta)
        key = (source, chunk_idx, line_range)
        if key in seen:
            continue
        seen.add(key)
        # Hits returned by RAGStore.query carry both ``score`` (rerank)
        # and ``distance`` (raw Chroma); code-RAG only carries
        # ``distance``. Normalise distance into a 0..1 similarity so
        # the UI sees comparable numbers.
        score_val = h.get("score")
        if score_val is None:
            distance = h.get("distance")
            if isinstance(distance, (int, float)):
                score_val = max(0.0, 1.0 - float(distance))
        try:
            score = float(score_val) if score_val is not None else 0.0
        except (TypeError, ValueError):
            score = 0.0
        text = h.get("text") or h.get("document") or ""
        snippet = str(text)[:200].strip()
        citations.append(
            Citation(
                source=source,
                chunk_idx=chunk_idx,
                score=score,
                snippet=snippet,
                line_range=line_range,
            )
        )
    return citations


def attach_citations(
    suggestions: list[MetadataSuggestion],
    citations: list[Citation],
) -> list[MetadataSuggestion]:
    """Attach ``citations`` to every suggestion in-place.

    Existing citations on the suggestion are unioned with the new ones,
    deduped by ``(source, chunk_idx, line_range)`` so the orchestrator's
    merge step (which already dedupes by ``(source, chunk_idx)``) never
    sees duplicates from the agent layer either.
    """
    if not suggestions or not citations:
        return suggestions
    for s in suggestions:
        existing = list(getattr(s, "citations", None) or [])
        seen: set[tuple[str, int, tuple[int, int] | None]] = {
            (c.source, c.chunk_idx, c.line_range) for c in existing
        }
        for c in citations:
            key = (c.source, c.chunk_idx, c.line_range)
            if key in seen:
                continue
            seen.add(key)
            existing.append(c)
        s.citations = existing
    return suggestions


def regex_refs_to_citations(refs_by_asset: dict[str, list]) -> list[Citation]:
    """Convert ``CodebaseReport.references`` into single-line citations.

    Each :class:`amx.codebase.analyzer.CodeReference` already pinpoints
    a ``(file, line_no, context)`` triple, so the resulting citation
    spans ``(line_no, line_no)``. Deduped across assets so a snippet
    referencing both a table and one of its columns only renders once.
    """
    out: list[Citation] = []
    seen: set[tuple[str, int]] = set()
    for refs in (refs_by_asset or {}).values():
        for ref in refs or []:
            file = getattr(ref, "file", "") or ""
            line_no = int(getattr(ref, "line_no", 0) or 0)
            if not file or line_no <= 0:
                continue
            key = (file, line_no)
            if key in seen:
                continue
            seen.add(key)
            ctx_text = getattr(ref, "context", "") or getattr(ref, "line_text", "") or ""
            out.append(
                Citation(
                    source=file,
                    chunk_idx=0,
                    score=1.0,
                    snippet=str(ctx_text)[:200].strip(),
                    line_range=(line_no, line_no),
                )
            )
    return out


__all__ = [
    "attach_citations",
    "hits_to_citations",
    "regex_refs_to_citations",
]
