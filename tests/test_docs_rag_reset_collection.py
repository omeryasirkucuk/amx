"""``RAGStore.reset_collection`` regression.

After an ``/embeddings`` swap the docs Chroma collection's stamped
identity (``embedding_provider`` / ``embedding_model`` /
``embedding_dim``) no longer matches the active config, and every
subsequent ``RAGStore()`` open raises
:class:`EmbeddingProviderMismatch`. ``ingest(refresh=True)`` did not
help — it only deleted documents for the same source path and left
the identity stamp alone.

This test pins the new contract: ``reset_collection()`` drops the
Chroma collection outright AND clears the FTS5 sidecar, then
re-opens with the current identity so the next ingest is consistent.
"""

from __future__ import annotations

import sqlite3
import tempfile

from amx.docs import rag as docs_rag


def test_reset_collection_drops_chroma_and_clears_fts() -> None:
    deleted: list[str] = []
    creates: list[dict[str, object]] = []

    class FakeCollection:
        def __init__(self, name: str = "amx_docs") -> None:
            self.name = name
            self.metadata: dict[str, object] = {}

        # Surface enough no-ops for the constructor's backfill /
        # FTS-bootstrap path to complete.
        def modify(self, *, metadata: dict[str, object]) -> None:
            self.metadata = dict(metadata)

        def get(self, **_: object) -> dict[str, list]:
            return {"ids": [], "documents": [], "metadatas": []}

    class FakeClient:
        def __init__(self, *, path: str) -> None:
            self._path = path
            self._collections: dict[str, FakeCollection] = {}

        def get_or_create_collection(self, **kwargs: object) -> FakeCollection:
            creates.append(kwargs)
            name = str(kwargs.get("name") or "amx_docs")
            col = self._collections.get(name) or FakeCollection(name)
            self._collections[name] = col
            return col

        def delete_collection(self, *, name: str) -> None:
            deleted.append(name)
            self._collections.pop(name, None)

    with tempfile.TemporaryDirectory() as td:
        # Use the real RAGStore against a fake Chroma client. The
        # FTS5 sidecar is real (SQLite); we will inspect it directly.
        import chromadb

        original_pc = chromadb.PersistentClient
        chromadb.PersistentClient = FakeClient  # type: ignore[assignment]
        try:
            store = docs_rag.RAGStore(persist_dir=td)
            # Seed the FTS sidecar with a row so we can prove ``clear``
            # actually runs.
            assert store._fts.upsert([("chunk_x", "doc.md", "hello world")]) == 1
            assert store._fts.count() == 1

            store.reset_collection()
        finally:
            chromadb.PersistentClient = original_pc  # type: ignore[assignment]

        # Chroma collection was dropped (not just emptied).
        assert "amx_docs" in deleted, f"expected amx_docs drop, got {deleted}"

        # And re-opened with active identity metadata.
        assert len(creates) >= 2, "reset_collection should re-open the collection"
        reopen_kwargs = creates[-1]
        metadata = dict(reopen_kwargs.get("metadata") or {})
        assert metadata.get("embedding_provider") == store.embedding_provider
        assert metadata.get("embedding_model") == store.embedding_model

        # FTS sidecar was cleared so BM25 cannot resurrect dropped chunks.
        conn = sqlite3.connect(str(store._fts.db_path))
        count = conn.execute("SELECT COUNT(*) FROM chunks_fts").fetchone()[0]
        conn.close()
        assert count == 0, f"FTS sidecar should be empty after reset, got {count} rows"
