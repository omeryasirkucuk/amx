"""PR D — doctor adds a ``RAG store`` check.

Three branches matter to the user:

* fresh persist_dir (collection opens, 0 chunks) → ``ok=True`` with a
  "0 chunks indexed" detail and a "run /ingest" hint;
* mismatched embedding (``EmbeddingProviderMismatch``) → ``ok=False``
  with the mismatch message;
* arbitrary open failure → ``ok=False`` with the exception name.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from amx.cli_support.commands.doctor import _check_rag_store


def test_rag_store_check_passes_on_fresh_collection(monkeypatch, tmp_path):
    fake_store = MagicMock()
    fake_store.collection = MagicMock()
    fake_store.collection.get = MagicMock(return_value={"ids": []})
    fake_store.collection.count = MagicMock(return_value=0)
    fake_store.embedding_provider = "minilm"
    fake_store.embedding_model = "minilm-l6-v2"

    monkeypatch.setattr("amx.docs.rag.RAGStore", lambda *a, **kw: fake_store)

    cfg = MagicMock()
    result = _check_rag_store(cfg)
    assert result.ok is True
    assert "0 chunks" in result.detail
    assert "minilm" in result.detail
    assert result.hint  # should suggest running /ingest


def test_rag_store_check_fails_on_embedding_mismatch(monkeypatch):
    from amx.docs.rag import EmbeddingProviderMismatch

    def _raise_mismatch(*a, **kw):
        raise EmbeddingProviderMismatch(
            recorded_provider="openai_compatible",
            recorded_model="text-embedding-3-small",
            active_provider="minilm",
            active_model="minilm-l6-v2",
        )

    monkeypatch.setattr("amx.docs.rag.RAGStore", _raise_mismatch)

    cfg = MagicMock()
    result = _check_rag_store(cfg)
    assert result.ok is False
    assert "text-embedding-3-small" in result.detail
    assert "reindex" in (result.hint or "").lower()


def test_rag_store_check_fails_on_open_error(monkeypatch):
    def _boom(*a, **kw):
        raise RuntimeError("chroma_db permission denied")

    monkeypatch.setattr("amx.docs.rag.RAGStore", _boom)

    cfg = MagicMock()
    result = _check_rag_store(cfg)
    assert result.ok is False
    assert "permission denied" in result.detail


def test_rag_store_check_passes_with_existing_chunks(monkeypatch):
    fake_store = MagicMock()
    fake_store.collection = MagicMock()
    fake_store.collection.get = MagicMock(return_value={"ids": ["a", "b"]})
    fake_store.collection.count = MagicMock(return_value=42)
    fake_store.embedding_provider = "openai_compatible"
    fake_store.embedding_model = "text-embedding-3-small"

    monkeypatch.setattr("amx.docs.rag.RAGStore", lambda *a, **kw: fake_store)

    cfg = MagicMock()
    result = _check_rag_store(cfg)
    assert result.ok is True
    assert "42 chunks" in result.detail
