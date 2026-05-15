"""Storage correctness for the docs RAG (PR B).

Pins three behaviours that PR B added to ``amx.docs.rag.RAGStore``:

1. The Chroma collection records ``embedding_provider`` and
   ``embedding_model`` on first create so a later reopen can detect a
   provider switch.
2. Reopening with a different provider raises
   :class:`EmbeddingProviderMismatch`; a pre-existing (PR A and older)
   collection without those metadata keys is grandfathered in and gets
   the metadata back-filled silently.
3. ``RAGStore.ingest`` deletes orphan chunks for a source path before
   upserting, so editing a 10-chunk file down to 2 no longer leaves
   chunks 2..9 in the collection forever.
"""

from __future__ import annotations

from pathlib import Path

import chromadb
import pytest

from amx.docs.rag import EmbeddingProviderMismatch, RAGStore
from amx.docs.scanner import DocInfo

# ── Helpers ───────────────────────────────────────────────────────────


def _make_store(
    persist_dir: Path,
    *,
    provider: str = "minilm",
    model: str = "minilm-l6-v2",
) -> RAGStore:
    """Build a ``RAGStore`` with an explicit provider/model and no real
    embedding function (MiniLM bundled default). Keeps tests offline
    and deterministic."""
    return RAGStore(
        persist_dir=str(persist_dir),
        embedding_function=None,
        embedding_provider=provider,
        embedding_model=model,
    )


def _make_doc(tmp_path: Path, body: str, name: str = "fixture.txt") -> DocInfo:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return DocInfo(
        path=str(p),
        size_bytes=p.stat().st_size,
        extension=".txt",
        source_type="local",
        source_root=str(tmp_path),
    )


# ── Fix 2: metadata recorded on first create ──────────────────────────


def test_collection_records_embedding_metadata_on_create(tmp_path: Path) -> None:
    store = _make_store(tmp_path / "chroma", provider="minilm", model="minilm-l6-v2")
    meta = dict(store.collection.metadata or {})
    assert meta.get("embedding_provider") == "minilm"
    assert meta.get("embedding_model") == "minilm-l6-v2"
    # PR-B: schema bumped to v2, ``embedding_dim`` added for MiniLM
    # (well-known = 384). Older v1 metadata gets upgraded silently
    # on reopen — see ``test_pre_existing_collection_*``.
    assert meta.get("amx_schema_version") == 2
    assert meta.get("embedding_dim") == 384


# ── Fix 3: mismatch raises ────────────────────────────────────────────


def test_mismatch_raises_embedding_provider_mismatch(tmp_path: Path) -> None:
    persist = tmp_path / "chroma"
    # Seal the collection with provider A.
    _make_store(persist, provider="openai_compatible", model="text-embedding-3-small")
    # Reopen with provider B; expect the structured mismatch error.
    with pytest.raises(EmbeddingProviderMismatch) as excinfo:
        _make_store(persist, provider="minilm", model="minilm-l6-v2")
    msg = str(excinfo.value)
    assert "openai_compatible" in msg
    assert "text-embedding-3-small" in msg
    assert "minilm" in msg
    assert "/docs reindex" in msg


# ── Fix 3 (grandfather): pre-PR-B collection has no metadata ──────────


def test_pre_existing_collection_without_metadata_is_grandfathered(
    tmp_path: Path,
) -> None:
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    # Simulate a pre-PR-B collection: only ``hnsw:space`` in metadata.
    client = chromadb.PersistentClient(path=str(persist))
    client.get_or_create_collection(
        name="amx_docs",
        metadata={"hnsw:space": "cosine"},
    )
    # No assertion on the absence-of-keys here — different chromadb
    # versions backfill ``hnsw:*`` defaults differently. The real
    # contract under test is "reopen does not raise even when our
    # embedding_* keys are missing".

    store = _make_store(persist, provider="minilm", model="minilm-l6-v2")
    backfilled = dict(store.collection.metadata or {})
    assert backfilled.get("embedding_provider") == "minilm"
    assert backfilled.get("embedding_model") == "minilm-l6-v2"


# ── Fix 5: idempotency on shrink ──────────────────────────────────────


def _ids_for_source(store: RAGStore, source: str) -> list[str]:
    res = store.collection.get(where={"source": source}, include=[])
    return list(res.get("ids") or [])


def test_ingest_deletes_orphan_chunks_on_shrink(tmp_path: Path) -> None:
    store = _make_store(tmp_path / "chroma")
    # Long enough body (with paragraph breaks) to produce multiple
    # chunks under the default RecursiveCharacterTextSplitter
    # (chunk_size=1000, overlap=200).
    long_body = "\n\n".join(["paragraph " + ("x" * 600)] * 8)
    doc = _make_doc(tmp_path, long_body, name="big.txt")
    store.ingest([doc])
    big_count = len(_ids_for_source(store, doc.path))
    assert big_count >= 2, "fixture should produce at least 2 chunks"

    # Re-ingest the SAME path with a one-chunk body and verify the
    # orphans got cleaned up rather than lingering forever.
    Path(doc.path).write_text("tiny single chunk content", encoding="utf-8")
    store.ingest([doc])
    final = _ids_for_source(store, doc.path)
    assert len(final) == 1
    assert final[0] == f"{doc.path}::0"


def test_ingest_idempotent_on_unchanged_file(tmp_path: Path) -> None:
    store = _make_store(tmp_path / "chroma")
    doc = _make_doc(tmp_path, "stable content body", name="stable.txt")
    store.ingest([doc])
    first = sorted(_ids_for_source(store, doc.path))
    store.ingest([doc])
    second = sorted(_ids_for_source(store, doc.path))
    assert first == second


# ── PR-B: dim recorded + dim-mismatch detection ──────────────────────


def test_collection_records_embedding_dim_for_minilm(tmp_path: Path) -> None:
    """MiniLM has a well-known 384-dim output; PR-B records it on
    collection metadata so a later provider swap that happens to
    preserve provider/model strings but change dim is caught."""
    store = _make_store(tmp_path / "chroma", provider="minilm", model="minilm-l6-v2")
    meta = dict(store.collection.metadata or {})
    assert meta.get("embedding_dim") == 384


def test_legacy_v1_collection_dim_backfilled_on_reopen(tmp_path: Path) -> None:
    """A v1 metadata layout (no embedding_dim) reopens cleanly and
    the dim is back-filled silently on next open."""
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    # Simulate a v1 collection: provider+model recorded, no dim.
    client = chromadb.PersistentClient(path=str(persist))
    client.get_or_create_collection(
        name="amx_docs",
        metadata={
            "hnsw:space": "cosine",
            "embedding_provider": "minilm",
            "embedding_model": "minilm-l6-v2",
            "amx_schema_version": 1,
        },
    )
    store = _make_store(persist, provider="minilm", model="minilm-l6-v2")
    meta = dict(store.collection.metadata or {})
    assert meta.get("embedding_dim") == 384
    assert meta.get("amx_schema_version") == 2


def test_dim_mismatch_raises_even_when_provider_model_match(tmp_path: Path) -> None:
    """If somehow a collection is recorded with a dim that disagrees
    with the active embedder's dim, raise the structured mismatch —
    same provider/model string is no longer sufficient to call it a
    match."""
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    # Seed metadata with a deliberately-wrong dim (768 instead of 384)
    # to simulate the silent-corruption case (e.g. provider rotated
    # model internals while keeping the same id).
    client = chromadb.PersistentClient(path=str(persist))
    client.get_or_create_collection(
        name="amx_docs",
        metadata={
            "hnsw:space": "cosine",
            "embedding_provider": "minilm",
            "embedding_model": "minilm-l6-v2",
            "embedding_dim": 768,
            "amx_schema_version": 2,
        },
    )
    with pytest.raises(EmbeddingProviderMismatch) as excinfo:
        _make_store(persist, provider="minilm", model="minilm-l6-v2")
    msg = str(excinfo.value)
    assert "dim 768 -> 384" in msg
    assert excinfo.value.recorded_dim == 768
    assert excinfo.value.active_dim == 384


def test_dim_zero_on_either_side_disables_dim_check(tmp_path: Path) -> None:
    """If the recorded collection has a non-zero dim but the active
    side reports 0 (unknown), the dim check is bypassed — never raise
    on an inferred-zero. Same logic the other way round."""
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(persist))
    client.get_or_create_collection(
        name="amx_docs",
        metadata={
            "hnsw:space": "cosine",
            "embedding_provider": "openai_compatible",
            "embedding_model": "text-embedding-3-small",
            "embedding_dim": 1536,
            "amx_schema_version": 2,
        },
    )
    # Reopen with provider/model matching but active dim unknown
    # (OpenAI-compatible reports 0 today). Must NOT raise.
    store = _make_store(
        persist,
        provider="openai_compatible",
        model="text-embedding-3-small",
    )
    meta = dict(store.collection.metadata or {})
    # Recorded dim is preserved (not overwritten to 0).
    assert meta.get("embedding_dim") == 1536


# ── Fix 4: callers surface the mismatch as a run error ────────────────


def test_record_rag_unavailable_reason_tags_mismatch(monkeypatch) -> None:
    """The analyze-flow helper tags the mismatch case with a
    ``embedding_mismatch:`` prefix so /history and Studio can
    distinguish it from a generic init crash."""
    from amx.cli_support.commands.analyze_flow import _record_rag_unavailable_reason

    exc = EmbeddingProviderMismatch(
        recorded_provider="openai_compatible",
        recorded_model="text-embedding-3-small",
        active_provider="minilm",
        active_model="minilm-l6-v2",
    )
    sink: dict[str, str] = {}
    _record_rag_unavailable_reason(sink, exc)
    assert sink["rag_unavailable_reason"].startswith("embedding_mismatch:")
    assert "openai_compatible" in sink["rag_unavailable_reason"]


def test_query_drops_chunks_above_distance_ceiling(monkeypatch, tmp_path: Path) -> None:
    """Regression: a doc profile whose chunks return high cosine
    distances (cosine_sim < ``rag_min_similarity``) used to flow into
    the LLM prompt as if they were authoritative. A real-world case
    was a CV uploaded as the doc profile — every column-level query
    returned the resume with distance 0.66–1.02 and the agent
    synthesised absurd "address alias / exclusion" descriptions from
    it. ``min_similarity`` filters those out before they ever reach
    the rerank step or the prompt.

    Test plan:
      * Stub Chroma's raw ``collection.query`` so we control the
        distances directly (no embedding model needed).
      * Confirm a strong match (distance 0.30) survives.
      * Confirm a weak match (distance 0.85) is dropped when
        ``min_similarity=0.40`` is passed.
      * Confirm ``min_similarity=0.0`` (legacy default) keeps the
        weak chunk in the result so existing callers don't change
        behaviour silently.
    """
    store = _make_store(tmp_path / "chroma")

    def _fake_query(query_texts: list[str], n_results: int, **_: object) -> dict:
        return {
            "documents": [["RELEVANT chunk about zip codes.", "RESUME irrelevant block."]],
            "metadatas": [
                [{"source": "/abs/zips.md", "source_root": "/abs"}],
            ][:0]
            + [
                [
                    {"source": "/abs/zips.md", "source_root": "/abs"},
                    {"source": "/abs/resume.pdf", "source_root": "/abs"},
                ]
            ],
            "distances": [[0.30, 0.85]],
        }

    monkeypatch.setattr(store.collection, "query", _fake_query)
    # Legacy default keeps every hit.
    hits_default = store.query("zip code column", n_results=5)
    assert len(hits_default) == 2
    sources_default = [h["metadata"].get("source") for h in hits_default]
    assert "/abs/resume.pdf" in sources_default

    # Threshold drops the noise chunk.
    hits_filtered = store.query("zip code column", n_results=5, min_similarity=0.40)
    assert len(hits_filtered) == 1
    assert hits_filtered[0]["metadata"].get("source") == "/abs/zips.md"


def test_format_rag_unavailable_reason_tags_mismatch() -> None:
    """The library-side helper used by ``infer_table_metadata`` mirrors
    the analyze-flow tagging so the two paths never drift."""
    from amx.core.inference import _format_rag_unavailable_reason

    exc = EmbeddingProviderMismatch(
        recorded_provider="sentence_transformers",
        recorded_model="BAAI/bge-large-en-v1.5",
        active_provider="minilm",
        active_model="minilm-l6-v2",
    )
    msg = _format_rag_unavailable_reason(exc)
    assert msg.startswith("embedding_mismatch:")
    assert "bge-large-en-v1.5" in msg
