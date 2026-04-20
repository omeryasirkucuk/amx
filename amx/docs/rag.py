"""RAG pipeline — chunk documents and store in ChromaDB for retrieval."""

from __future__ import annotations

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
    def __init__(self, persist_dir: str | None = None):
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

    def delete_chunks_for_sources(self, sources: list[str]) -> int:
        """Remove all chunks whose metadata ``source`` equals one of the given paths (exact match)."""
        removed = 0
        for src in sources:
            if not src:
                continue
            try:
                res = self.collection.get(where={"source": src}, include=[])
            except Exception as exc:
                log.warning("Chroma get for delete failed %s: %s", src, exc)
                continue
            ids = res.get("ids") or []
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
            self.delete_chunks_for_sources([d.path for d in docs])
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
                    {"source": doc.path, "source_type": doc.source_type, "chunk_idx": i}
                    for i in range(len(chunks))
                ]
                self.collection.upsert(ids=ids, documents=texts, metadatas=metadatas)
                total_chunks += len(chunks)
                log.info("Ingested %s → %d chunks", doc.path, len(chunks))
            except Exception as exc:
                log.error("Error ingesting %s: %s", doc.path, exc)
        return total_chunks

    def query(self, question: str, n_results: int = 5) -> list[dict]:
        results = self.collection.query(query_texts=[question], n_results=n_results)
        hits: list[dict] = []
        for i in range(len(results["documents"][0])):
            hits.append(
                {
                    "text": results["documents"][0][i],
                    "metadata": results["metadatas"][0][i],
                    "distance": results["distances"][0][i] if results.get("distances") else None,
                }
            )
        return hits

    @property
    def doc_count(self) -> int:
        return self.collection.count()
