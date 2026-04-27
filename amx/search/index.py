"""Vector index for AMX search catalog entities."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import chromadb


class SearchIndex:
    """Thin wrapper around a Chroma collection for effective catalog rows."""

    def __init__(self, persist_dir: str | None = None):
        self.persist_dir = persist_dir or str(Path.home() / ".amx" / "chroma_db")
        Path(self.persist_dir).mkdir(parents=True, exist_ok=True)
        self.client = chromadb.PersistentClient(path=self.persist_dir)
        self.collection = self.client.get_or_create_collection(
            name="amx_search",
            metadata={"hnsw:space": "cosine"},
        )

    def upsert_entities(self, entities: list[dict[str, Any]]) -> int:
        docs: list[str] = []
        ids: list[str] = []
        metas: list[dict[str, Any]] = []
        for entity in entities:
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
            self.collection.upsert(ids=ids, documents=docs, metadatas=metas)
        return len(ids)

    def delete_entity_ids(self, entity_ids: list[int]) -> None:
        ids = [f"entity:{int(entity_id)}" for entity_id in entity_ids if entity_id is not None]
        if ids:
            self.collection.delete(ids=ids)

    def reset_profile(self, db_profile: str) -> None:
        rows = self.collection.get(where={"db_profile": db_profile}, include=[])
        ids = rows.get("ids") or []
        if ids:
            self.collection.delete(ids=ids)

    def query(self, question: str, *, db_profile: str, n_results: int = 8) -> list[dict[str, Any]]:
        res = self.collection.query(
            query_texts=[question],
            n_results=max(1, int(n_results)),
            where={"db_profile": db_profile},
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
