"""Vector index for AMX search catalog entities.

Each DB profile is mapped to its own Chroma collection
(``amx_search_<sha256-prefix>``) so vectors from one profile cannot
leak into another. The previous design used a single ``amx_search``
collection filtered by a ``db_profile`` metadata field on every query;
forgetting the filter anywhere in the codebase risked cross-profile
pollution. The empty profile name still maps to the legacy
``amx_search`` collection for back-compat with tests and callers that
have not adopted the per-profile API yet.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import chromadb
from chromadb.api.types import EmbeddingFunction


_LEGACY_COLLECTION_NAME = "amx_search"


def _collection_name_for(db_profile: str) -> str:
    """Stable, Chroma-valid collection name for *db_profile*.

    Profile names can contain anything (spaces, slashes, unicode),
    so we hash to keep within Chroma's allowed character set
    (``[a-zA-Z0-9._-]``, 3–63 chars). Empty profile maps to the
    legacy ``amx_search`` collection so older data is still readable
    without forcing an immediate rebuild.
    """
    if not db_profile:
        return _LEGACY_COLLECTION_NAME
    digest = hashlib.sha256(db_profile.encode("utf-8")).hexdigest()[:16]
    return f"amx_search_{digest}"


class SearchIndex:
    """Thin wrapper around per-profile Chroma collections.

    The ``embedding_function`` argument lets callers swap in a different
    embedding provider (see :mod:`amx.search.embeddings`); ``None``
    falls back to the process-wide default factory installed by the CLI
    at startup, and finally to Chroma's bundled MiniLM if no factory is
    registered (preserving the original behaviour for tests and direct
    constructors).
    """

    def __init__(
        self,
        persist_dir: str | None = None,
        *,
        embedding_function: EmbeddingFunction | None = None,
    ) -> None:
        self.persist_dir = persist_dir or str(Path.home() / ".amx" / "chroma_db")
        Path(self.persist_dir).mkdir(parents=True, exist_ok=True)
        self.client = chromadb.PersistentClient(path=self.persist_dir)
        if embedding_function is None:
            # No explicit override — fall back to the process-wide default
            # the CLI installed at startup based on ``cfg.embedding``.
            from amx.search.embeddings import get_default_embedding_function

            embedding_function = get_default_embedding_function()
        self.embedding_function = embedding_function
        self._collections: dict[str, Any] = {}
        # Eagerly construct the legacy collection (empty profile key) so the
        # historical ``self.collection`` attribute keeps working for callers
        # and tests that have not adopted the per-profile API.
        self.collection = self._collection_for("")

    def _collection_for(self, db_profile: str) -> Any:
        """Get or create the Chroma collection for *db_profile*.

        Cached after first access so we do not pay Chroma's
        ``get_or_create_collection`` cost on every upsert / query.
        """
        cache_key = db_profile or ""
        cached = self._collections.get(cache_key)
        if cached is not None:
            return cached
        name = _collection_name_for(db_profile)
        kwargs: dict[str, Any] = {
            "name": name,
            "metadata": {"hnsw:space": "cosine", "amx_db_profile": db_profile},
        }
        if self.embedding_function is not None:
            kwargs["embedding_function"] = self.embedding_function
        col = self.client.get_or_create_collection(**kwargs)
        self._collections[cache_key] = col
        return col

    def upsert_entities(self, entities: list[dict[str, Any]]) -> int:
        # Group rows by their db_profile so each collection only ever sees
        # its own data; cross-profile leaks are physically impossible.
        by_profile: dict[str, list[dict[str, Any]]] = {}
        for entity in entities:
            profile = str(entity.get("db_profile") or "")
            by_profile.setdefault(profile, []).append(entity)

        total = 0
        for profile, rows in by_profile.items():
            docs: list[str] = []
            ids: list[str] = []
            metas: list[dict[str, Any]] = []
            for entity in rows:
                doc = str(entity.get("search_text") or "").strip()
                entity_id = entity.get("id")
                if not doc or entity_id is None:
                    continue
                ids.append(f"entity:{int(entity_id)}")
                docs.append(doc)
                metas.append(
                    {
                        "entity_id": int(entity_id),
                        "db_profile": str(entity.get("db_profile") or ""),
                        "schema_name": str(entity.get("schema_name") or ""),
                        "table_name": str(entity.get("table_name") or ""),
                        "column_name": str(entity.get("column_name") or ""),
                        "entity_kind": str(entity.get("entity_kind") or ""),
                        "effective_source_kind": str(entity.get("effective_source_kind") or ""),
                    }
                )
            if ids:
                self._collection_for(profile).upsert(ids=ids, documents=docs, metadatas=metas)
                total += len(ids)
        return total

    def delete_entity_ids(self, entity_ids: list[int]) -> None:
        """Delete by entity id without knowing the originating db_profile.

        Without the profile we cannot single out the right collection, so
        we attempt the delete on every collection we have opened in this
        process. The operation is idempotent on Chroma's side, so
        deleting an id that does not exist in a given collection is a
        no-op.
        """
        ids = [f"entity:{int(entity_id)}" for entity_id in entity_ids if entity_id is not None]
        if not ids:
            return
        for col in list(self._collections.values()):
            try:
                col.delete(ids=ids)
            except Exception:
                # One collection's failure must not block the others —
                # callers expect a best-effort delete, and the row is
                # almost certainly only in one collection anyway.
                continue

    def reset_profile(self, db_profile: str) -> None:
        col = self._collection_for(db_profile)
        rows = col.get(include=[])
        ids = rows.get("ids") or []
        if ids:
            col.delete(ids=ids)

    def query(self, question: str, *, db_profile: str, n_results: int = 8) -> list[dict[str, Any]]:
        col = self._collection_for(db_profile)
        res = col.query(
            query_texts=[question],
            n_results=max(1, int(n_results)),
            # No ``where`` clause needed — the collection is profile-scoped,
            # so cross-profile pollution is impossible regardless of caller
            # discipline.
        )
        docs = (res.get("documents") or [[]])[0]
        metas = (res.get("metadatas") or [[]])[0]
        distances = (res.get("distances") or [[]])[0]
        hits: list[dict[str, Any]] = []
        for idx, doc in enumerate(docs):
            meta = metas[idx] if idx < len(metas) else {}
            dist = distances[idx] if idx < len(distances) else None
            hits.append({"text": doc, "metadata": meta or {}, "distance": dist})
        return hits
