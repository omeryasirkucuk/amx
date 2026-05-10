"""Semantic index over repository source (Chroma collection ``amx_code``)."""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

from amx.utils.optional_deps import ensure as _ensure

# Codebase RAG shares the ``rag`` bundle (chromadb + splitter +
# tiktoken) with /docs and /search; whichever feature the user
# touches first pays the install once.
_ensure("rag")

import chromadb  # noqa: E402
from langchain_text_splitters import RecursiveCharacterTextSplitter  # noqa: E402

from amx.codebase.analyzer import CODE_EXTENSIONS, CodebaseReport  # noqa: E402
from amx.utils.logging import get_logger  # noqa: E402

log = get_logger("codebase.code_rag")

COLLECTION = "amx_code"


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
    source_root: str | None = None,
) -> int:
    """Chunk Python (AST) and other code files; upsert into ``amx_code`` collection."""
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    Path(persist).mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=persist)
    coll = client.get_or_create_collection(
        name=COLLECTION,
        metadata={"hnsw:space": "cosine"},
    )

    code_files = [f for f in root.rglob("*") if f.is_file() and f.suffix.lower() in CODE_EXTENSIONS]
    total = 0
    root_s = str(root.resolve())
    source_root_s = _normalize_source_filter(source_root or root_s)
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
                "source_root": source_root_s,
                "rel_path": rel,
                "chunk_id": cid,
                "kind": "python_ast" if suffix == ".py" else "text_split",
            }
            coll.upsert(ids=[doc_id], documents=[chunk], metadatas=[meta])
            total += 1

    if report:
        log.info(
            "Indexed %d code chunks under %s (report had %d ref keys)",
            total,
            root,
            len(report.references),
        )
    return total


def query_code_snippets(
    question: str,
    n_results: int = 5,
    persist_dir: str | None = None,
    source_filters: list[str] | None = None,
) -> list[dict]:
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    client = chromadb.PersistentClient(path=persist)
    try:
        coll = client.get_collection(COLLECTION)
    except Exception:
        return []
    filters = [_normalize_source_filter(s) for s in (source_filters or []) if s]
    query_n = n_results if not filters else max(n_results * 4, n_results)
    res = coll.query(query_texts=[question], n_results=query_n)
    hits: list[dict] = []
    for i in range(len(res["documents"][0])):
        meta = res["metadatas"][0][i]
        if filters and not _source_allowed(meta, filters):
            continue
        hits.append(
            {
                "text": res["documents"][0][i],
                "metadata": meta,
                "distance": res["distances"][0][i] if res.get("distances") else None,
            }
        )
        if len(hits) >= n_results:
            break
    return hits


def code_collection_count(
    persist_dir: str | None = None,
    source_filters: list[str] | None = None,
) -> int:
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    try:
        client = chromadb.PersistentClient(path=persist)
        coll = client.get_collection(COLLECTION)
        filters = [_normalize_source_filter(s) for s in (source_filters or []) if s]
        if filters:
            rows = coll.get(include=["metadatas"])
            metas = rows.get("metadatas") or []
            return sum(1 for m in metas if _source_allowed(m, filters))
        return int(coll.count())
    except Exception:
        return 0


def delete_code_collection(
    persist_dir: str | None = None,
    source_filters: list[str] | None = None,
) -> bool:
    """Remove the entire ``amx_code`` collection (e.g. before full re-index).

    Returns ``True`` if a collection was deleted, ``False`` if it didn't exist.
    """
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    try:
        client = chromadb.PersistentClient(path=persist)
        if not source_filters:
            client.delete_collection(COLLECTION)
            log.info("Deleted Chroma collection %s", COLLECTION)
            return True
        coll = client.get_collection(COLLECTION)
        filters = [_normalize_source_filter(s) for s in source_filters if s]
        rows = coll.get(include=["metadatas"])
        ids = [
            row_id
            for row_id, meta in zip(
                rows.get("ids") or [], rows.get("metadatas") or [], strict=False
            )
            if _source_allowed(meta, filters)
        ]
        if not ids:
            return False
        coll.delete(ids=ids)
        log.info("Deleted %d code chunks from %s", len(ids), COLLECTION)
        return True
    except Exception:
        return False


def _source_allowed(metadata: dict | None, filters: list[str]) -> bool:
    if not metadata:
        return False
    src = str(metadata.get("source") or "")
    root = str(metadata.get("source_root") or "")
    for flt in filters:
        if root and (root == flt or root.startswith(flt)):
            return True
        if src and (src == flt or src.startswith(flt)):
            return True
    return False
