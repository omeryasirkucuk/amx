"""Catalog Search (``amx.search.index.SearchIndex``) identity tests — PR-B.

Catalog Search historically recorded no embedding identity on its
Chroma collections; ``/search rebuild`` after an ``/embeddings`` swap
silently re-embedded with whatever provider was active, with no
warning that the existing vectors lived in a different semantic space.
PR-B closes that gap by:

1. Stamping ``embedding_provider`` / ``embedding_model`` /
   ``embedding_dim`` / ``amx_schema_version`` on every per-profile
   collection at create time.
2. Reconciling on reopen via
   :func:`amx.rag_core.collection_identity.reconcile_identity` — raises
   :class:`CollectionIdentityMismatch` when the recorded triple
   disagrees with the active config.
3. Grandfather rule: legacy collections that lack identity metadata
   are silently back-filled on first reopen (no forced rebuild).

These tests pin those behaviours so a future refactor cannot
re-introduce silent re-embed.
"""

from __future__ import annotations

from pathlib import Path

import chromadb
import pytest

from amx.rag_core.collection_identity import CollectionIdentityMismatch
from amx.search.index import SearchIndex, _collection_name_for


def test_default_index_records_identity_on_legacy_collection(tmp_path: Path) -> None:
    """A fresh SearchIndex creates the legacy (empty-profile)
    collection and stamps the active embedding identity onto it."""
    index = SearchIndex(persist_dir=str(tmp_path / "chroma"))
    meta = dict(index.collection.metadata or {})
    assert meta.get("embedding_provider") == "minilm"
    assert meta.get("embedding_model") == "minilm-l6-v2"
    assert meta.get("embedding_dim") == 384
    assert meta.get("amx_schema_version") == 1


def test_per_profile_collection_carries_identity(tmp_path: Path) -> None:
    """Every per-profile collection (not just the legacy one) records
    the identity. Two profiles produce two collections, both stamped."""
    index = SearchIndex(persist_dir=str(tmp_path / "chroma"))
    col_a = index._collection_for("profile_a")
    col_b = index._collection_for("profile_b")
    for col in (col_a, col_b):
        meta = dict(col.metadata or {})
        assert meta.get("embedding_provider") == "minilm"
        assert meta.get("embedding_dim") == 384


def test_legacy_collection_without_identity_is_grandfathered(tmp_path: Path) -> None:
    """A pre-PR-B Catalog Search collection (no identity metadata)
    reopens cleanly. The identity is back-filled silently — never
    forced into a rebuild."""
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    # Seed a legacy v0 collection directly via Chroma — only
    # ``hnsw:space`` and the AMX profile key, no embedding identity.
    client = chromadb.PersistentClient(path=str(persist))
    legacy_name = _collection_name_for("")
    client.get_or_create_collection(
        name=legacy_name,
        metadata={"hnsw:space": "cosine", "amx_db_profile": ""},
    )
    # Reopening through SearchIndex must NOT raise — and must record
    # the identity so future opens have something to compare against.
    index = SearchIndex(persist_dir=str(persist))
    meta = dict(index.collection.metadata or {})
    assert meta.get("embedding_provider") == "minilm"
    assert meta.get("embedding_model") == "minilm-l6-v2"
    assert meta.get("embedding_dim") == 384


def test_provider_swap_raises_collection_identity_mismatch(tmp_path: Path) -> None:
    """A collection stamped with one provider, re-opened by an index
    configured for another, raises the structured mismatch with a
    ``/search rebuild`` recovery hint. This is the failure the previous
    silent-re-embed behaviour hid.

    ``SearchIndex`` opens the legacy collection lazily, so the mismatch
    fires on first access to ``index.collection`` (or any tool path
    that reaches ``_collection_for``), not at construction.
    """
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(persist))
    legacy_name = _collection_name_for("")
    # Seed with an explicitly different provider (openai_compatible /
    # text-embedding-3-small / dim 1536).
    client.get_or_create_collection(
        name=legacy_name,
        metadata={
            "hnsw:space": "cosine",
            "amx_db_profile": "",
            "embedding_provider": "openai_compatible",
            "embedding_model": "text-embedding-3-small",
            "embedding_dim": 1536,
            "amx_schema_version": 1,
        },
    )
    index = SearchIndex(persist_dir=str(persist))
    with pytest.raises(CollectionIdentityMismatch) as excinfo:
        _ = index.collection
    msg = str(excinfo.value)
    assert "/search rebuild" in msg
    # provider changed AND model changed AND dim changed.
    assert excinfo.value.recorded.embedding_provider == "openai_compatible"
    assert excinfo.value.active.embedding_provider == "minilm"


def test_dim_mismatch_alone_raises(tmp_path: Path) -> None:
    """Same provider/model strings but a recorded dim that disagrees
    with the active dim still raises. Catches the rotation-with-same-
    name corruption case."""
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(persist))
    legacy_name = _collection_name_for("")
    # Provider/model match MiniLM but record the wrong dim.
    client.get_or_create_collection(
        name=legacy_name,
        metadata={
            "hnsw:space": "cosine",
            "amx_db_profile": "",
            "embedding_provider": "minilm",
            "embedding_model": "minilm-l6-v2",
            "embedding_dim": 768,
            "amx_schema_version": 1,
        },
    )
    index = SearchIndex(persist_dir=str(persist))
    with pytest.raises(CollectionIdentityMismatch) as excinfo:
        _ = index.collection
    msg = str(excinfo.value)
    assert "dim: 768" in msg or "dim 768 -> 384" in msg


def test_dim_zero_on_either_side_is_a_match(tmp_path: Path) -> None:
    """Recorded dim ``0`` (legacy v1 metadata) plus active dim that
    can be inferred (MiniLM = 384) is treated as a match. The dim is
    upgraded silently on this open so subsequent reopens get the
    stronger check."""
    persist = tmp_path / "chroma"
    persist.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(persist))
    legacy_name = _collection_name_for("")
    client.get_or_create_collection(
        name=legacy_name,
        metadata={
            "hnsw:space": "cosine",
            "amx_db_profile": "",
            "embedding_provider": "minilm",
            "embedding_model": "minilm-l6-v2",
            "embedding_dim": 0,
            "amx_schema_version": 1,
        },
    )
    index = SearchIndex(persist_dir=str(persist))
    meta = dict(index.collection.metadata or {})
    assert meta.get("embedding_dim") == 384
