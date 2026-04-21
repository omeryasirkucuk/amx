"""Semantic index over repository source (Chroma collection ``amx_code``)."""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import chromadb
from langchain_text_splitters import RecursiveCharacterTextSplitter

from amx.codebase.analyzer import CODE_EXTENSIONS, CodebaseReport
from amx.utils.logging import get_logger

log = get_logger("codebase.code_rag")

COLLECTION = "amx_code"


def _iter_python_chunks(rel_path: str, content: str) -> list[tuple[str, str]]:
    """Return (chunk_id_suffix, text) for RAG indexing."""
    chunks: list[tuple[str, str]] = []
    try:
        tree = ast.parse(content, filename=rel_path)
    except SyntaxError:
        seg = content[:14000]
        return [("module", seg)] if seg.strip() else []

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            try:
                segment = ast.get_source_segment(content, node)
            except Exception:
                segment = None
            if segment and len(segment.strip()) > 40:
                nid = f"{node.name}_{getattr(node, 'lineno', 0)}"
                chunks.append((nid, segment[:12000]))
    if not chunks and content.strip():
        chunks.append(("module", content[:14000]))
    return chunks


def _split_fallback(text: str, max_chars: int = 4000) -> list[str]:
    sp = RecursiveCharacterTextSplitter(chunk_size=max_chars, chunk_overlap=200)
    return sp.split_text(text)


def index_codebase_tree(
    root: Path,
    *,
    report: CodebaseReport | None = None,
    persist_dir: str | None = None,
) -> int:
    """Chunk Python (AST) and other code files; upsert into ``amx_code`` collection."""
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    Path(persist).mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=persist)
    coll = client.get_or_create_collection(
        name=COLLECTION,
        metadata={"hnsw:space": "cosine"},
    )

    code_files = [
        f for f in root.rglob("*")
        if f.is_file() and f.suffix.lower() in CODE_EXTENSIONS
    ]
    total = 0
    root_s = str(root.resolve())
    for fpath in code_files:
        rel = str(fpath.relative_to(root))
        try:
            text = fpath.read_text(errors="replace")
        except Exception:
            continue
        suffix = fpath.suffix.lower()
        pieces: list[tuple[str, str]] = []
        if suffix == ".py":
            pieces = _iter_python_chunks(rel, text)
        else:
            for i, part in enumerate(_split_fallback(text)):
                pieces.append((f"part{i}", part))

        for cid, chunk in pieces:
            if not chunk.strip():
                continue
            h = hashlib.sha256(f"{root_s}:{rel}:{cid}".encode()).hexdigest()[:24]
            doc_id = f"code::{h}"
            meta = {
                "source": f"{root_s}/{rel}",
                "rel_path": rel,
                "chunk_id": cid,
                "kind": "python_ast" if suffix == ".py" else "text_split",
            }
            coll.upsert(ids=[doc_id], documents=[chunk], metadatas=[meta])
            total += 1

    if report:
        log.info("Indexed %d code chunks under %s (report had %d ref keys)", total, root, len(report.references))
    return total


def query_code_snippets(question: str, n_results: int = 5, persist_dir: str | None = None) -> list[dict]:
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    client = chromadb.PersistentClient(path=persist)
    try:
        coll = client.get_collection(COLLECTION)
    except Exception:
        return []
    res = coll.query(query_texts=[question], n_results=n_results)
    hits: list[dict] = []
    for i in range(len(res["documents"][0])):
        hits.append(
            {
                "text": res["documents"][0][i],
                "metadata": res["metadatas"][0][i],
                "distance": res["distances"][0][i] if res.get("distances") else None,
            }
        )
    return hits


def code_collection_count(persist_dir: str | None = None) -> int:
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    try:
        client = chromadb.PersistentClient(path=persist)
        coll = client.get_collection(COLLECTION)
        return int(coll.count())
    except Exception:
        return 0


def delete_code_collection(persist_dir: str | None = None) -> bool:
    """Remove the entire ``amx_code`` collection (e.g. before full re-index).

    Returns ``True`` if a collection was deleted, ``False`` if it didn't exist.
    """
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    try:
        client = chromadb.PersistentClient(path=persist)
        client.delete_collection(COLLECTION)
        log.info("Deleted Chroma collection %s", COLLECTION)
        return True
    except Exception:
        return False
