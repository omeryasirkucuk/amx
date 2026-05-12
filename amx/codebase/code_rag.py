"""Semantic index over repository source (Chroma collection ``amx_code``)."""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from amx.utils.optional_deps import ensure as _ensure

if TYPE_CHECKING:
    from chromadb.api.types import EmbeddingFunction

# Codebase RAG shares the ``rag`` bundle (chromadb + splitter +
# tiktoken) with /docs and /search; whichever feature the user
# touches first pays the install once.
_ensure("rag")

import chromadb  # noqa: E402
from langchain_text_splitters import RecursiveCharacterTextSplitter  # noqa: E402

from amx.codebase.analyzer import CODE_EXTENSIONS, CodebaseReport  # noqa: E402
from amx.codebase.walker import walk_code_files  # noqa: E402
from amx.utils.logging import get_logger  # noqa: E402

log = get_logger("codebase.code_rag")

COLLECTION = "amx_code"

# Schema version for the code RAG collection metadata. Bumped when the
# metadata shape changes in a way old AMX cannot read. Mirrors the
# value used by the docs RAG store so the two indexes evolve together.
_AMX_CODE_SCHEMA_VERSION = 1


class CodeEmbeddingMismatch(RuntimeError):
    """Raised when the active embedding provider does not match the
    one used to populate the existing ``amx_code`` collection.

    The collection metadata records the provider/model used at first
    create. If the user later switches embedding providers (via
    ``/embeddings``) the existing vectors are in a different semantic
    space, so retrieval would silently degrade. Raising here forces an
    explicit reindex or revert decision.
    """

    def __init__(
        self,
        *,
        recorded_provider: str,
        recorded_model: str,
        active_provider: str,
        active_model: str,
    ) -> None:
        self.recorded_provider = recorded_provider
        self.recorded_model = recorded_model
        self.active_provider = active_provider
        self.active_model = active_model
        super().__init__(
            f"Code RAG collection was indexed with provider={recorded_provider} "
            f"model={recorded_model}. Current config says provider={active_provider} "
            f"model={active_model}. "
            "Run `/code-refresh` to rebuild the collection with the active provider, "
            "or update the embedding profile to match the indexed model."
        )


def _resolve_active_embedding(
    cfg: Any | None = None,
) -> tuple[str, str, EmbeddingFunction | None]:
    """Resolve ``(provider, model, embedding_function)`` from config.

    Mirrors :func:`amx.docs.rag._resolve_active_embedding` so the code
    path honours the same embedding provider as the docs path. Imports
    are deferred to avoid a circular import (``amx.search.embeddings``
    pulls Chroma which pulls this module on some platforms).
    """
    from amx.search.embeddings import make_embedding_function

    if cfg is None:
        try:
            from amx.config import AMXConfig

            cfg = AMXConfig.load()
        except Exception:
            cfg = None

    embedding = getattr(cfg, "embedding", None) if cfg is not None else None
    if embedding is None:
        return ("minilm", "minilm-l6-v2", None)

    kind = (getattr(embedding, "kind", "") or "minilm").lower().strip()
    model = getattr(embedding, "model", "") or ""
    api_key = getattr(embedding, "api_key", "") or ""
    base_url = getattr(embedding, "base_url", "") or ""

    if kind in {"", "minilm", "default", "minilm-l6-v2"}:
        return ("minilm", "minilm-l6-v2", None)

    if not model:
        return ("minilm", "minilm-l6-v2", None)

    try:
        ef = make_embedding_function(kind, model=model, api_key=api_key, base_url=base_url)
    except Exception:
        return ("minilm", "minilm-l6-v2", None)
    return (kind, model, ef)


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


def _iter_ipynb_chunks(rel_path: str, content: str) -> list[tuple[str, str, str]]:
    """Cell-aware ``.ipynb`` chunker.

    Returns a list of ``(chunk_id_suffix, text, kind)`` tuples where
    ``kind`` is one of ``"ipynb_code"`` / ``"ipynb_md"``. Cell outputs
    are deliberately dropped — they're noisy, often huge (base64
    images), and rarely useful for code retrieval.

    On malformed JSON the caller falls back to the generic splitter
    (returning ``None`` signals that to the loop without raising).
    """
    try:
        nb = json.loads(content)
    except json.JSONDecodeError:
        log.warning("Failed to parse .ipynb at %s, falling back to text split", rel_path)
        return []

    out: list[tuple[str, str, str]] = []
    cells = nb.get("cells") or []
    for idx, cell in enumerate(cells):
        cell_type = str(cell.get("cell_type") or "").lower()
        if cell_type not in {"code", "markdown"}:
            continue
        source = cell.get("source") or ""
        text = "".join(str(piece) for piece in source) if isinstance(source, list) else str(source)
        if not text.strip():
            continue
        kind = "ipynb_code" if cell_type == "code" else "ipynb_md"
        out.append((f"cell{idx}", text[:12000], kind))
    return out


def _split_fallback(text: str, max_chars: int = 4000) -> list[str]:
    sp = RecursiveCharacterTextSplitter(chunk_size=max_chars, chunk_overlap=200)
    return sp.split_text(text)


def _manifest_path(persist_dir: str, source_root: str) -> Path:
    """Resolve the sidecar manifest path for ``source_root``.

    Stored under ``<persist_dir>/../code_cache/<slug>/index_manifest.json``
    where the slug is a stable hash of the source root. Used to detect
    files that disappeared between scans so their chunks can be
    deleted (a pure ``upsert`` cannot infer deletion).
    """
    slug = hashlib.sha256(source_root.encode()).hexdigest()[:16]
    base = Path(persist_dir).expanduser().resolve().parent / "code_cache" / slug
    base.mkdir(parents=True, exist_ok=True)
    return base / "index_manifest.json"


def _load_manifest(path: Path) -> list[str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    files = data.get("files") if isinstance(data, dict) else None
    return [str(p) for p in files] if isinstance(files, list) else []


def _save_manifest(path: Path, files: list[str]) -> None:
    try:
        path.write_text(
            json.dumps({"files": sorted(set(files))}, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        log.warning("Could not write code index manifest %s: %s", path, exc)


def _open_collection(
    client: chromadb.PersistentClient,
    *,
    embedding_provider: str,
    embedding_model: str,
    embedding_function: EmbeddingFunction | None,
):
    """Open / create the ``amx_code`` collection and reconcile metadata.

    See :class:`amx.docs.rag.RAGStore.__init__` for the parallel
    docs-path implementation. Raises :class:`CodeEmbeddingMismatch`
    when the recorded provider/model disagree with the active config.
    """
    kwargs: dict[str, Any] = {
        "name": COLLECTION,
        "metadata": {
            "hnsw:space": "cosine",
            "embedding_provider": embedding_provider,
            "embedding_model": embedding_model,
            "amx_schema_version": _AMX_CODE_SCHEMA_VERSION,
        },
    }
    if embedding_function is not None:
        kwargs["embedding_function"] = embedding_function
    coll = client.get_or_create_collection(**kwargs)

    existing_meta = dict(coll.metadata or {})
    recorded_provider = existing_meta.get("embedding_provider")
    recorded_model = existing_meta.get("embedding_model")
    if recorded_provider and recorded_model:
        if recorded_provider != embedding_provider or recorded_model != embedding_model:
            raise CodeEmbeddingMismatch(
                recorded_provider=str(recorded_provider),
                recorded_model=str(recorded_model),
                active_provider=str(embedding_provider),
                active_model=str(embedding_model),
            )
    else:
        # Pre-PR-beta collection — backfill metadata silently. Strip
        # ``hnsw:*`` keys before modify(): Chroma rejects construction-
        # time parameters even when the value is unchanged.
        merged = {k: v for k, v in existing_meta.items() if not str(k).startswith("hnsw:")}
        merged["embedding_provider"] = embedding_provider
        merged["embedding_model"] = embedding_model
        merged["amx_schema_version"] = _AMX_CODE_SCHEMA_VERSION
        try:
            coll.modify(metadata=merged)
        except Exception as exc:
            log.warning("Could not backfill code RAG collection metadata: %s", exc)
    return coll


def index_codebase_tree(
    root: Path,
    *,
    report: CodebaseReport | None = None,
    persist_dir: str | None = None,
    source_root: str | None = None,
    cfg: Any | None = None,
) -> int:
    """Chunk Python (AST) and other code files; upsert into ``amx_code`` collection."""
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    Path(persist).mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=persist)

    provider, model, ef = _resolve_active_embedding(cfg)
    coll = _open_collection(
        client,
        embedding_provider=provider,
        embedding_model=model,
        embedding_function=ef,
    )

    code_files = list(walk_code_files(root))
    total = 0
    root_s = str(root.resolve())
    source_root_s = _normalize_source_filter(source_root or root_s)
    indexed_rels: list[str] = []
    for fpath in code_files:
        rel = str(fpath.relative_to(root))
        try:
            text = fpath.read_text(errors="replace")
        except Exception:
            continue
        suffix = fpath.suffix.lower()
        pieces: list[tuple[str, str]] = []
        # ``per_chunk_kind`` lets the ipynb path stamp distinct chunk
        # kinds per cell type without forcing a third tuple position
        # on the AST / generic splits.
        per_chunk_kind: dict[str, str] = {}
        if suffix == ".py":
            pieces = _iter_python_chunks(rel, text)
        elif suffix == ".ipynb":
            cell_chunks = _iter_ipynb_chunks(rel, text)
            if cell_chunks:
                for cid, chunk, kind in cell_chunks:
                    pieces.append((cid, chunk))
                    per_chunk_kind[cid] = kind
            elif text.strip():
                # Malformed notebook — fall back to the generic
                # splitter so we still produce something rather than
                # leaving the file unindexed.
                for i, part in enumerate(_split_fallback(text)):
                    pieces.append((f"part{i}", part))
        else:
            for i, part in enumerate(_split_fallback(text)):
                pieces.append((f"part{i}", part))

        # Pre-compute the chunk IDs the file will produce so we can
        # find orphans from a previous larger version of the same file
        # and delete them BEFORE upsert. Closes the
        # function-rename / shrink / delete bug.
        new_ids: list[str] = []
        new_payload: list[tuple[str, str, dict[str, Any]]] = []
        for cid, chunk in pieces:
            if not chunk.strip():
                continue
            h = hashlib.sha256(f"{root_s}:{rel}:{cid}".encode()).hexdigest()[:24]
            doc_id = f"code::{h}"
            if suffix == ".py":
                kind = "python_ast"
            elif suffix == ".ipynb":
                kind = per_chunk_kind.get(cid, "text_split")
            else:
                kind = "text_split"
            meta = {
                "source": f"{root_s}/{rel}",
                "source_root": source_root_s,
                "rel_path": rel,
                "chunk_id": cid,
                "kind": kind,
            }
            new_ids.append(doc_id)
            new_payload.append((doc_id, chunk, meta))

        try:
            existing = coll.get(where={"rel_path": rel}, include=[])
            existing_ids = set(existing.get("ids") or [])
        except Exception as exc:
            log.warning("Could not enumerate existing code chunks for %s: %s", rel, exc)
            existing_ids = set()
        orphans = sorted(existing_ids - set(new_ids))
        if orphans:
            try:
                coll.delete(ids=orphans)
                log.info(
                    "Deleted %d orphan code chunk(s) for %s before re-ingest",
                    len(orphans),
                    rel,
                )
            except Exception as exc:
                log.warning("Could not delete orphan code chunks for %s: %s", rel, exc)

        if new_payload:
            ids = [p[0] for p in new_payload]
            docs = [p[1] for p in new_payload]
            metas = [p[2] for p in new_payload]
            coll.upsert(ids=ids, documents=docs, metadatas=metas)
            total += len(new_payload)
            indexed_rels.append(rel)

    # File-deletion detection via sidecar manifest. Compare the
    # previous walk to this one; anything missing is gone from the
    # tree and its chunks must be evicted.
    manifest = _manifest_path(persist, source_root_s or root_s)
    previous = set(_load_manifest(manifest))
    current = set(indexed_rels)
    removed_files = sorted(previous - current)
    for dead_rel in removed_files:
        try:
            res = coll.get(where={"rel_path": dead_rel}, include=[])
            dead_ids = list(res.get("ids") or [])
            if dead_ids:
                coll.delete(ids=dead_ids)
                log.info("Deleted %d chunk(s) for removed file %s", len(dead_ids), dead_rel)
        except Exception as exc:
            log.warning("Could not evict chunks for removed file %s: %s", dead_rel, exc)
    _save_manifest(manifest, indexed_rels)

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
    *,
    cfg: Any | None = None,
) -> list[dict]:
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    client = chromadb.PersistentClient(path=persist)
    try:
        # Honour cfg.embedding here too so a query running with a
        # custom provider can hit the collection it indexed earlier.
        provider, model, ef = _resolve_active_embedding(cfg)
        coll = _open_collection(
            client,
            embedding_provider=provider,
            embedding_model=model,
            embedding_function=ef,
        )
    except CodeEmbeddingMismatch:
        # Propagate the structured error so callers (agents, CLI,
        # Studio) can surface a remediation hint rather than silently
        # returning no hits.
        raise
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


__all__ = [
    "CodeEmbeddingMismatch",
    "COLLECTION",
    "code_collection_count",
    "delete_code_collection",
    "index_codebase_tree",
    "query_code_snippets",
]


# Keep the extension list importable from here for any external
# tooling that already depends on it.
_ = CODE_EXTENSIONS
