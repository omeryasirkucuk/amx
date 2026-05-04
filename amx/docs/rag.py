"""RAG pipeline — chunk documents and store in ChromaDB for retrieval."""

from __future__ import annotations

import re
from pathlib import Path

import chromadb
from langchain_community.document_loaders import (
    CSVLoader,
    Docx2txtLoader,
    PyPDFLoader,
    TextLoader,
    UnstructuredExcelLoader,
    UnstructuredHTMLLoader,
    UnstructuredMarkdownLoader,
    UnstructuredPowerPointLoader,
)
from langchain_text_splitters import RecursiveCharacterTextSplitter

from amx.docs.scanner import DocInfo
from amx.utils.logging import get_logger

log = get_logger("docs.rag")

EXPLANATORY_TERMS = frozenset(
    {
        "because",
        "therefore",
        "used",
        "means",
        "represents",
        "identifies",
        "calculated",
        "derived",
        "when",
        "where",
        "join",
        "maps",
        "foreign",
        "primary",
        "constraint",
        "business",
        "process",
        "why",
        "how",
    }
)

LOADER_MAP = {
    ".pdf": PyPDFLoader,
    ".docx": Docx2txtLoader,
    ".doc": Docx2txtLoader,
    ".txt": TextLoader,
    ".md": UnstructuredMarkdownLoader,
    ".csv": CSVLoader,
    ".xlsx": UnstructuredExcelLoader,
    ".xls": UnstructuredExcelLoader,
    ".html": UnstructuredHTMLLoader,
    ".htm": UnstructuredHTMLLoader,
    ".pptx": UnstructuredPowerPointLoader,
    ".json": TextLoader,
    ".yaml": TextLoader,
    ".yml": TextLoader,
    ".rst": TextLoader,
    ".rtf": TextLoader,
}


class RAGStore:
    def __init__(
        self,
        persist_dir: str | None = None,
        source_filters: list[str] | None = None,
    ):
        self.persist_dir = persist_dir or str(Path.home() / ".amx" / "chroma_db")
        Path(self.persist_dir).mkdir(parents=True, exist_ok=True)
        self.client = chromadb.PersistentClient(path=self.persist_dir)
        self.collection = self.client.get_or_create_collection(
            name="amx_docs",
            metadata={"hnsw:space": "cosine"},
        )
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            separators=["\n\n", "\n", ". ", " ", ""],
        )
        self.source_filters = [
            self._normalize_source_filter(s) for s in (source_filters or []) if s
        ]

    def delete_chunks_for_sources(self, sources: list[str]) -> int:
        """Remove chunks by resolved file path or original configured source path."""
        removed = 0
        for src in sources:
            if not src:
                continue
            ids: list[str] = []
            for key, value in (
                ("source", src),
                ("source_root", self._normalize_source_filter(src)),
            ):
                try:
                    res = self.collection.get(where={key: value}, include=[])
                except Exception as exc:
                    log.warning("Chroma get for delete failed %s=%s: %s", key, value, exc)
                    continue
                ids.extend(res.get("ids") or [])
            ids = sorted(set(ids))
            if ids:
                self.collection.delete(ids=ids)
                removed += len(ids)
                log.info("Deleted %d chunks for source %s", len(ids), src)
        return removed

    def ingest(
        self,
        docs: list[DocInfo],
        *,
        refresh: bool = False,
    ) -> int:
        if refresh and docs:
            self.delete_chunks_for_sources([x for d in docs for x in (d.path, d.source_root) if x])
        total_chunks = 0
        for doc in docs:
            loader_cls = LOADER_MAP.get(doc.extension)
            if loader_cls is None:
                log.warning("No loader for %s, skipping %s", doc.extension, doc.path)
                continue
            try:
                loader = loader_cls(doc.path)
                pages = loader.load()
                chunks = self.splitter.split_documents(pages)
                if not chunks:
                    continue

                ids = [f"{doc.path}::{i}" for i in range(len(chunks))]
                texts = [c.page_content for c in chunks]
                metadatas = [
                    {
                        "source": doc.path,
                        "source_root": self._normalize_source_filter(doc.source_root or doc.path),
                        "source_type": doc.source_type,
                        "chunk_idx": i,
                    }
                    for i in range(len(chunks))
                ]
                self.collection.upsert(ids=ids, documents=texts, metadatas=metadatas)
                total_chunks += len(chunks)
                log.info("Ingested %s -> %d chunks", doc.path, len(chunks))
            except Exception as exc:
                log.error("Error ingesting %s: %s", doc.path, exc)
        return total_chunks

    def query(self, question: str, n_results: int = 5) -> list[dict]:
        raw_n = max(int(n_results), min(int(n_results) * 4, 40))
        results = self.collection.query(query_texts=[question], n_results=raw_n)
        hits: list[dict] = []
        for i in range(len(results["documents"][0])):
            meta = results["metadatas"][0][i]
            if not self._source_allowed(meta):
                continue
            hits.append(
                {
                    "text": results["documents"][0][i],
                    "metadata": meta,
                    "distance": results["distances"][0][i] if results.get("distances") else None,
                }
            )
        return self.rerank(question, hits)[:n_results]

    def rerank(self, question: str, hits: list[dict]) -> list[dict]:
        """Prioritize explanatory chunks over repetitive technical headers."""
        q_tokens = {
            token for token in re.findall(r"\w+", (question or "").lower()) if len(token) > 2
        }

        def _score(hit: dict) -> float:
            text = str(hit.get("text") or "")
            lower = text.lower()
            tokens = {token for token in re.findall(r"\w+", lower) if len(token) > 2}
            overlap = len(q_tokens.intersection(tokens))
            explanatory = sum(1 for term in EXPLANATORY_TERMS if term in tokens)
            header_penalty = 0.0
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            if lines:
                short_lines = sum(1 for line in lines if len(line) <= 40)
                header_penalty = min(2.0, short_lines / max(1, len(lines)))
            distance = hit.get("distance")
            distance_score = max(0.0, 2.0 - float(distance)) if distance is not None else 0.0
            return distance_score + overlap * 0.4 + explanatory * 0.35 - header_penalty

        return sorted(hits, key=_score, reverse=True)

    @property
    def doc_count(self) -> int:
        if self.source_filters:
            return self.filtered_doc_count()
        return self.collection.count()

    def filtered_doc_count(self) -> int:
        """Count chunks visible under source filters."""
        if not self.source_filters:
            return self.collection.count()
        try:
            rows = self.collection.get(include=["metadatas"])
            metas = rows.get("metadatas") or []
            return sum(1 for m in metas if self._source_allowed(m))
        except Exception:
            return 0

    def _source_allowed(self, metadata: dict | None) -> bool:
        if not self.source_filters:
            return True
        if not metadata:
            return False
        src = str(metadata.get("source") or "")
        root_src = str(metadata.get("source_root") or "")
        if not src and not root_src:
            return False
        for root in self.source_filters:
            if root_src and (root_src == root or root_src.startswith(root)):
                return True
            if src and (src == root or src.startswith(root)):
                return True
        return False

    @staticmethod
    def _normalize_source_filter(source: str) -> str:
        src = str(source or "").strip()
        if not src:
            return ""
        if src.startswith(("http://", "https://", "s3://", "git@")):
            return src
        try:
            return str(Path(src).expanduser().resolve())
        except Exception:
            return src
