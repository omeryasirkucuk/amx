"""Semantic index over repository source (Chroma collection ``amx_code``)."""

from __future__ import annotations

import ast
import concurrent.futures
import hashlib
import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from amx.utils.optional_deps import ensure as _ensure

if TYPE_CHECKING:
    import chromadb
    from chromadb.api.types import EmbeddingFunction

# Codebase RAG shares the ``rag`` bundle (chromadb + splitter +
# tiktoken) with /docs and /search. The bundle is fetched on first
# Chroma client construction, NOT at module import — keeping import
# lightweight matters for Studio cold start (Studio's import chain
# does not transitively pull this module today, but the same lazy-
# install policy now applies uniformly across all four RAG modules).

from amx.codebase.analyzer import CODE_EXTENSIONS, CodebaseReport
from amx.codebase.walker import walk_code_files
from amx.utils.logging import get_logger


def _get_chroma():
    """Ensure the ``rag`` bundle is installed and return the
    :mod:`chromadb` module.

    Centralising the lazy import here means every Chroma entry point
    in this module can write ``client = _get_chroma().PersistentClient
    (path=...)`` without repeating the install + import boilerplate.
    The :func:`amx.utils.optional_deps.ensure` call is idempotent.
    """
    _ensure("rag")
    import chromadb

    return chromadb


log = get_logger("codebase.code_rag")

COLLECTION = "amx_code"

# Schema version for the code RAG collection metadata. Bumped when the
# metadata shape changes in a way old AMX cannot read. Mirrors the
# value used by the docs RAG store so the two indexes evolve together.
# v2 added ``embedding_dim`` — older collections still work; the field
# is backfilled silently on first reopen.
_AMX_CODE_SCHEMA_VERSION = 2


class CodeEmbeddingMismatch(RuntimeError):
    """Raised when the active embedding identity does not match the
    one used to populate the existing ``amx_code`` collection.

    PR-B (this commit) added ``recorded_dim`` / ``active_dim`` —
    optional, default ``0``. ``0`` on either side disables the dim
    half of the check (keeps backward-compat for pre-PR-B collections
    whose metadata has no ``embedding_dim`` key).
    """

    def __init__(
        self,
        *,
        recorded_provider: str,
        recorded_model: str,
        active_provider: str,
        active_model: str,
        recorded_dim: int = 0,
        active_dim: int = 0,
    ) -> None:
        self.recorded_provider = recorded_provider
        self.recorded_model = recorded_model
        self.active_provider = active_provider
        self.active_model = active_model
        self.recorded_dim = int(recorded_dim or 0)
        self.active_dim = int(active_dim or 0)
        dim_suffix = ""
        if self.recorded_dim and self.active_dim and self.recorded_dim != self.active_dim:
            dim_suffix = f" (dim {self.recorded_dim} -> {self.active_dim})"
        super().__init__(
            f"Code RAG collection was indexed with provider={recorded_provider} "
            f"model={recorded_model}. Current config says provider={active_provider} "
            f"model={active_model}{dim_suffix}. "
            "Run `/code-refresh` to rebuild the collection with the active provider, "
            "or update the embedding profile to match the indexed model."
        )


# PR-C: Code RAG default upgrade path.
#
# When the user has not picked an explicit embedding provider, code
# retrieval prefers a code-specialised model (``jinaai/jina-embeddings-v2-base-code``,
# ~161 MB, 768-dim) over the generic English prose model MiniLM ships
# with. Identifier-heavy and snake_case / CamelCase queries are
# measurably better on a code-trained encoder.
#
# The upgrade is **opportunistic**: jina only kicks in when
# ``sentence-transformers`` is installed (``pip install amx[local-embeddings]``)
# AND the model is loadable (either cached from a prior call or
# downloadable now). Anything that fails — missing dep, offline
# install, model-load error — degrades gracefully back to bundled
# MiniLM with a one-time WARNING that names the install command. The
# fallback is **silent** for retrieval quality (MiniLM still works) but
# **logged** so operators see what's happening.
#
# Document RAG keeps MiniLM as its default — prose retrieval doesn't
# benefit from a code-trained encoder. Switching there belongs to a
# separate decision tied to E2 of the audit (``bge-small-en-v1.5``).
JINA_CODE_MODEL = "jinaai/jina-embeddings-v2-base-code"

# Cache so the WARNING fires once per process even when many
# CodeIndex instances are constructed during a long-running CLI
# session. Per-process, not per-config — the message is about the
# user's install, not their config.
_jina_fallback_warned = False


def _try_jina_code_embedder() -> tuple[EmbeddingFunction | None, str | None]:
    """Try to build the jina code embedder. Returns ``(ef, error)``.

    Returns ``(ef, None)`` on success.
    Returns ``(None, reason)`` when sentence-transformers isn't
    installed, the model can't be downloaded (offline / network
    failure), or any other construction error — caller falls back to
    MiniLM. ``reason`` is short and quotable in the one-time WARNING.
    """
    try:
        from amx.search.embeddings import SentenceTransformerEmbedding
    except ImportError as exc:  # pragma: no cover — amx package always available
        return None, f"amx.search.embeddings unavailable: {exc}"
    try:
        ef = SentenceTransformerEmbedding(model=JINA_CODE_MODEL)
    except RuntimeError as exc:
        # SentenceTransformerEmbedding.__init__ raises RuntimeError
        # when ``sentence-transformers`` itself isn't importable.
        return None, str(exc)
    except Exception as exc:  # noqa: BLE001 — broad on purpose
        # Model load failure: network down, HF Hub unreachable,
        # disk-cache permission issue, etc. Don't crash the whole
        # CodeIndex on a recoverable degradation.
        return None, f"{exc.__class__.__name__}: {exc}"
    return ef, None


def _resolve_active_embedding(
    cfg: Any | None = None,
) -> tuple[str, str, EmbeddingFunction | None]:
    """Resolve ``(provider, model, embedding_function)`` from config.

    Mirrors :func:`amx.docs.rag._resolve_active_embedding` for the
    explicit-provider path. The DEFAULT path differs: code retrieval
    prefers ``jinaai/jina-embeddings-v2-base-code`` over MiniLM when
    ``sentence-transformers`` is installed (PR-C); falls back to
    MiniLM with a one-time WARNING when it isn't.

    Imports are deferred to avoid a circular import
    (``amx.search.embeddings`` pulls Chroma which pulls this module on
    some platforms).
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
        return _default_code_embedding()

    kind = (getattr(embedding, "kind", "") or "minilm").lower().strip()
    model = getattr(embedding, "model", "") or ""
    api_key = getattr(embedding, "api_key", "") or ""
    base_url = getattr(embedding, "base_url", "") or ""

    if kind in {"", "minilm", "default", "minilm-l6-v2"}:
        # User is on the default — opportunistically upgrade to the
        # code-specialised embedder. An explicit ``/embeddings minilm``
        # choice falls into this branch too (the cfg layer normalises
        # both to ``minilm``), which is intentional: a code-trained
        # encoder is the right floor for ``/code search`` regardless
        # of what the user picked for prose RAG.
        return _default_code_embedding()

    if not model:
        return _default_code_embedding()

    try:
        ef = make_embedding_function(kind, model=model, api_key=api_key, base_url=base_url)
    except Exception:
        return _default_code_embedding()
    return (kind, model, ef)


def _default_code_embedding() -> tuple[str, str, EmbeddingFunction | None]:
    """The opportunistic-jina-with-MiniLM-fallback default. Extracted
    so all the early-return branches in :func:`_resolve_active_embedding`
    share one code path and one place to fire the WARNING."""
    global _jina_fallback_warned
    ef, reason = _try_jina_code_embedder()
    if ef is not None:
        return ("sentence_transformers", JINA_CODE_MODEL, ef)
    if not _jina_fallback_warned:
        _jina_fallback_warned = True
        log.warning(
            "Code RAG falling back to MiniLM (%s). Install "
            '`pip install "amx-cli[local-embeddings]"` to enable the '
            "code-specialised %s embedder (~161 MB).",
            reason or "sentence-transformers unavailable",
            JINA_CODE_MODEL,
        )
    return ("minilm", "minilm-l6-v2", None)


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


def _iter_python_chunks(rel_path: str, content: str) -> list[tuple[str, str, int, int]]:
    """Return ``(chunk_id_suffix, text, start_line, end_line)`` for RAG indexing.

    Line bounds are 1-based, inclusive, and captured from the AST node
    BEFORE :func:`ast.get_source_segment` discards them. Module-level
    fallback chunks (no AST hits, or :exc:`SyntaxError`) span the whole
    file so the citation always points somewhere real.
    """
    chunks: list[tuple[str, str, int, int]] = []
    total_lines = max(1, content.count("\n") + 1)
    try:
        tree = ast.parse(content, filename=rel_path)
    except SyntaxError:
        seg = content[:14000]
        if not seg.strip():
            return []
        return [("module", seg, 1, total_lines)]

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            start_line = int(getattr(node, "lineno", 1) or 1)
            end_line = int(getattr(node, "end_lineno", None) or start_line)
            try:
                segment = ast.get_source_segment(content, node)
            except Exception:
                segment = None
            if segment and len(segment.strip()) > 40:
                nid = f"{node.name}_{start_line}"
                chunks.append((nid, segment[:12000], start_line, end_line))
    if not chunks and content.strip():
        chunks.append(("module", content[:14000], 1, total_lines))
    return chunks


def _iter_ipynb_chunks(rel_path: str, content: str) -> list[tuple[str, str, str, int]]:
    """Cell-aware ``.ipynb`` chunker.

    Returns a list of ``(chunk_id_suffix, text, kind, cell_idx_1based)``
    tuples where ``kind`` is one of ``"ipynb_code"`` / ``"ipynb_md"``.
    Cell outputs are deliberately dropped — they're noisy, often huge
    (base64 images), and rarely useful for code retrieval.

    ``cell_idx_1based`` is the 1-based cell number (skipping the
    raw/output cell types that are filtered out) used downstream as the
    citation's ``start_line`` / ``end_line`` so notebooks render as
    ``demo.ipynb:3`` for the third cell.

    On malformed JSON the caller falls back to the generic splitter
    (returning ``[]`` signals that to the loop without raising).
    """
    try:
        nb = json.loads(content)
    except json.JSONDecodeError:
        log.warning("Failed to parse .ipynb at %s, falling back to text split", rel_path)
        return []

    out: list[tuple[str, str, str, int]] = []
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
        # The 1-based cell index from the raw notebook stream (not the
        # filtered index) so users can map back to the file directly.
        out.append((f"cell{idx}", text[:12000], kind, idx + 1))
    return out


def _split_fallback(text: str, max_chars: int = 4000) -> list[str]:
    _ensure("rag")
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    sp = RecursiveCharacterTextSplitter(chunk_size=max_chars, chunk_overlap=200)
    return sp.split_text(text)


def _iter_split_chunks(text: str, max_chars: int = 4000) -> list[tuple[str, str, int, int]]:
    """Generic-splitter path that also records 1-based line ranges.

    The :class:`RecursiveCharacterTextSplitter` does not return offsets,
    so we walk the original text linearly: for each produced chunk,
    find its first occurrence starting at our running cursor, then
    advance the cursor past that chunk. Line numbers are then derived
    by counting newlines up to those character offsets. The fallback
    when a chunk cannot be located (e.g. splitter collapsed whitespace)
    is to inherit the previous chunk's bounds — better than emitting
    ``0`` and confusing the renderer.
    """
    parts = _split_fallback(text, max_chars=max_chars)
    if not parts:
        return []

    out: list[tuple[str, str, int, int]] = []
    cursor = 0
    last_bounds = (1, 1)
    for i, part in enumerate(parts):
        if not part:
            continue
        idx = text.find(part, cursor)
        if idx == -1:
            # Splitter normalised characters in a way that broke direct
            # substring lookup. Fall back gracefully to the previous
            # chunk's bounds rather than emitting line 0.
            start_line, end_line = last_bounds
        else:
            start_line = text.count("\n", 0, idx) + 1
            end_idx = idx + len(part)
            end_line = text.count("\n", 0, max(end_idx - 1, 0)) + 1
            cursor = end_idx
            last_bounds = (start_line, end_line)
        out.append((f"part{i}", part, start_line, end_line))
    return out


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
    # PR-B: resolve and record the embedding dim alongside provider /
    # model so silent-corruption switches (same model id, different
    # vector size) get caught at reopen.
    from amx.rag_core.collection_identity import infer_dimension

    embedding_dim = infer_dimension(embedding_provider, embedding_model, embedding_function)

    kwargs: dict[str, Any] = {
        "name": COLLECTION,
        "metadata": {
            "hnsw:space": "cosine",
            "embedding_provider": embedding_provider,
            "embedding_model": embedding_model,
            "embedding_dim": int(embedding_dim),
            "amx_schema_version": _AMX_CODE_SCHEMA_VERSION,
        },
    }
    if embedding_function is not None:
        kwargs["embedding_function"] = embedding_function
    coll = client.get_or_create_collection(**kwargs)

    existing_meta = dict(coll.metadata or {})
    recorded_provider = existing_meta.get("embedding_provider")
    recorded_model = existing_meta.get("embedding_model")
    try:
        recorded_dim = int(existing_meta.get("embedding_dim", 0) or 0)
    except (TypeError, ValueError):
        recorded_dim = 0
    if recorded_provider and recorded_model:
        dim_mismatch = recorded_dim > 0 and embedding_dim > 0 and recorded_dim != embedding_dim
        if (
            recorded_provider != embedding_provider
            or recorded_model != embedding_model
            or dim_mismatch
        ):
            raise CodeEmbeddingMismatch(
                recorded_provider=str(recorded_provider),
                recorded_model=str(recorded_model),
                active_provider=str(embedding_provider),
                active_model=str(embedding_model),
                recorded_dim=recorded_dim,
                active_dim=embedding_dim,
            )
        if recorded_dim == 0 and embedding_dim > 0:
            # Upgrade legacy v1 metadata so the next reopen has the dim
            # for comparison.
            merged = {k: v for k, v in existing_meta.items() if not str(k).startswith("hnsw:")}
            merged["embedding_dim"] = int(embedding_dim)
            merged["amx_schema_version"] = _AMX_CODE_SCHEMA_VERSION
            try:
                coll.modify(metadata=merged)
            except Exception as exc:
                log.warning("Could not upgrade code RAG collection metadata dim: %s", exc)
    else:
        # Pre-PR-B collection — backfill metadata silently. Strip
        # ``hnsw:*`` keys before modify(): Chroma rejects construction-
        # time parameters even when the value is unchanged.
        merged = {k: v for k, v in existing_meta.items() if not str(k).startswith("hnsw:")}
        merged["embedding_provider"] = embedding_provider
        merged["embedding_model"] = embedding_model
        merged["embedding_dim"] = int(embedding_dim)
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
    client = _get_chroma().PersistentClient(path=persist)

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
        # Each piece carries ``(chunk_id, text, start_line, end_line)``
        # where the line bounds are 1-based and inclusive. PR γ added
        # the line bounds so every citation produced by the agents can
        # point at a real range inside the source file. Splitter and
        # ipynb paths compute their own bounds (see below).
        pieces: list[tuple[str, str, int, int]] = []
        # ``per_chunk_kind`` lets the ipynb path stamp distinct chunk
        # kinds per cell type without forcing a fifth tuple position
        # on the AST / generic splits.
        per_chunk_kind: dict[str, str] = {}
        if suffix == ".py":
            pieces = _iter_python_chunks(rel, text)
        elif suffix == ".ipynb":
            cell_chunks = _iter_ipynb_chunks(rel, text)
            if cell_chunks:
                for cid, chunk, kind, cell_idx in cell_chunks:
                    # 1-based cell index is more useful than a raw
                    # source-line number for notebooks; the renderer
                    # shows ``nb.ipynb:3`` for cell 3 regardless of how
                    # many code lines lived inside the cell.
                    pieces.append((cid, chunk, cell_idx, cell_idx))
                    per_chunk_kind[cid] = kind
            elif text.strip():
                # Malformed notebook — fall back to the generic
                # splitter so we still produce something rather than
                # leaving the file unindexed.
                pieces.extend(_iter_split_chunks(text))
        else:
            pieces.extend(_iter_split_chunks(text))

        # Pre-compute the chunk IDs the file will produce so we can
        # find orphans from a previous larger version of the same file
        # and delete them BEFORE upsert. Closes the
        # function-rename / shrink / delete bug.
        new_ids: list[str] = []
        new_payload: list[tuple[str, str, dict[str, Any]]] = []
        for cid, chunk, start_line, end_line in pieces:
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
                # PR γ: 1-based, inclusive bounds. Python AST chunks
                # use real source lines; ``.ipynb`` cells reuse the
                # 1-based cell index for both bounds (so the renderer
                # shows ``nb.ipynb:3``); generic-splitter chunks derive
                # bounds from the chunk's offset inside the file.
                "start_line": int(start_line),
                "end_line": int(end_line),
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


# Tokens used by ``_hybrid_score`` to extract query keywords. Code
# identifiers routinely carry underscores (``sap_s6p``), so we keep
# them as boundary characters AND emit the surrounding subtokens
# (``sap``, ``s6p``) for substring matching.
_CODE_TOKEN_RX = re.compile(r"[A-Za-z][A-Za-z0-9]*")


def _code_query_tokens(query: str) -> list[str]:
    """Lower-case tokens with length >= 2. Short queries like ``sap``
    survive the length floor; single-letter noise is dropped."""
    out: list[str] = []
    seen: set[str] = set()
    for tok in _CODE_TOKEN_RX.findall(query or ""):
        low = tok.lower()
        if len(low) < 2 or low in seen:
            continue
        seen.add(low)
        out.append(low)
    return out


def _hybrid_score(query_tokens: list[str], hit: dict) -> float:
    """Combine embedding distance with literal keyword overlap.

    Pure cosine similarity from a 384-d MiniLM embedding is noisy for
    short keyword queries — a 3-letter token like ``sap`` against the
    embedding produces a distance near 1.0 even when the chunk
    contains ``SAP`` literally. We boost any chunk whose text carries
    the query's keywords so literal matches always outrank pure-
    embedding-noise hits. The two signals are additive on different
    scales:

      embedding_score = max(0, 2 - distance)   ->  [0, 2]
      keyword_overlap = matched / total        ->  [0, 1]
      hybrid          = embedding_score + 2.5 * keyword_overlap

    The 2.5× factor makes a single literal token sufficient to
    overcome the worst-case embedding noise (~0.5 below the second-
    place chunk). The exact weight isn't load-bearing — it just has
    to be large enough to flip a wrong-but-close pair like
    ``[1.073 SAP-match, 1.682 unrelated]`` so the matching chunk
    lands on top.
    """
    text = str(hit.get("text") or "").lower()
    distance = hit.get("distance")
    distance_score = max(0.0, 2.0 - float(distance)) if distance is not None else 0.0
    if not query_tokens:
        return distance_score
    matched = sum(1 for token in query_tokens if token in text)
    keyword_overlap = matched / max(1, len(query_tokens))
    return distance_score + 2.5 * keyword_overlap


def query_code_snippets(
    question: str,
    n_results: int = 5,
    persist_dir: str | None = None,
    source_filters: list[str] | None = None,
    *,
    cfg: Any | None = None,
    timeout: float | None = None,
) -> list[dict]:
    """Semantic retrieval over the ``amx_code`` collection.

    PR δ (I4): when ``timeout`` is a positive float, wrap the underlying
    ``coll.query`` call in a single-thread executor so the wall-clock
    cap is honoured even when Chroma stalls. On timeout we raise
    :class:`amx.docs.rag.RAGQueryTimeout` (the shared exception class
    docs RAG uses since PR D) so callers — :class:`CodeAgent`, the
    ``/code-search`` CLI command, and Studio's ``/api/code-search``
    endpoint — can branch on the timeout case and surface a user-facing
    diagnostic instead of returning an ambiguous empty hit list.
    """
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    client = _get_chroma().PersistentClient(path=persist)
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
    # Over-fetch so the hybrid reranker has enough candidates to
    # actually pull literal-match chunks above embedding-noise ones.
    # The previous behaviour over-fetched only when source filters
    # were set; we always over-fetch now (capped) so a short keyword
    # query like ``sap`` has room to find its 3rd-place real match in
    # a 50-chunk haystack rather than being limited to the top 5 by
    # raw distance.
    query_n = min(40, max(n_results * 4, n_results))

    def _do_query() -> Any:
        return coll.query(query_texts=[question], n_results=query_n)

    if timeout is not None and float(timeout) > 0:
        # Re-using docs-RAG's :class:`RAGQueryTimeout` keeps the
        # diagnostic shape identical across the two retrieval surfaces
        # — the agents catch one exception class regardless of whether
        # the stall happened in the code or docs collection.
        from amx.docs.rag import RAGQueryTimeout

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_do_query)
            try:
                res = future.result(timeout=float(timeout))
            except concurrent.futures.TimeoutError as e:
                log.warning(
                    "Code RAG retrieval exceeded %.2fs timeout, proceeding without context",
                    float(timeout),
                )
                future.cancel()
                raise RAGQueryTimeout(
                    f"Code retrieval exceeded {float(timeout):.2f}s timeout"
                ) from e
    else:
        res = _do_query()
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

    # Hybrid rerank: combine embedding distance with literal keyword
    # overlap so chunks carrying the user's query terms always outrank
    # pure-embedding-noise hits. Stable sort preserves the original
    # Chroma order when scores tie. The ``score`` field is exposed on
    # each hit so the UI can show a meaningful "match quality" number
    # instead of (or alongside) the raw cosine distance — short
    # keyword queries against MiniLM-l6-v2 produce noisy distance
    # numbers that mislead even when the result is correct.
    query_tokens = _code_query_tokens(question)
    for hit in hits:
        hit["score"] = float(_hybrid_score(query_tokens, hit))
    hits.sort(key=lambda h: h["score"], reverse=True)
    return hits[:n_results]


def code_collection_count(
    persist_dir: str | None = None,
    source_filters: list[str] | None = None,
) -> int:
    """Return the number of indexed code chunks under the source filters.

    PR δ (I11): when ``source_filters`` is set, push the filter down to
    Chroma via its ``where={...}`` operator instead of pulling every
    metadata row into Python and filtering client-side. On large code
    indexes (10k+ chunks) the python-side scan walked the whole
    collection on every call site that asked "is there anything for
    this profile?" — the orchestrator hits this on every ``/run``,
    every doctor check, and every Studio profile-health refresh.

    Falls back to a per-filter aggregate when the installed Chroma
    rejects ``$or`` (older versions do); a final fallback to the
    historical full-scan path keeps the function shape backwards-
    compatible if every server-side route errors.
    """
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    try:
        client = _get_chroma().PersistentClient(path=persist)
        coll = client.get_collection(COLLECTION)
        filters = [_normalize_source_filter(s) for s in (source_filters or []) if s]
        if not filters:
            return int(coll.count())

        # Preferred path: single server-side ``$or`` filter so Chroma
        # walks its own index instead of round-tripping every metadata
        # blob through Python.
        try:
            or_clauses = [{"source_root": {"$eq": p}} for p in filters]
            where = {"$or": or_clauses} if len(or_clauses) > 1 else or_clauses[0]
            result = coll.get(where=where, include=[])
            return len(result.get("ids") or [])
        except Exception as exc:
            log.debug("code_collection_count: $or where failed (%s); per-filter fallback", exc)

        # Fallback 1: aggregate per-filter so each request stays small.
        # We still avoid the historical full-scan because every clause
        # ships an indexed equality match.
        try:
            seen: set[str] = set()
            for p in filters:
                result = coll.get(where={"source_root": {"$eq": p}}, include=[])
                for rid in result.get("ids") or []:
                    seen.add(rid)
            return len(seen)
        except Exception as exc:
            log.debug(
                "code_collection_count: per-filter where failed (%s); full-scan fallback",
                exc,
            )

        # Fallback 2: the historical python-side scan. Slow on large
        # collections but guaranteed to work on any Chroma build.
        rows = coll.get(include=["metadatas"])
        metas = rows.get("metadatas") or []
        return sum(1 for m in metas if _source_allowed(m, filters))
    except Exception:
        return 0


def code_collection_metadata(persist_dir: str | None = None) -> dict[str, Any]:
    """Best-effort read of the ``amx_code`` collection's recorded metadata.

    Returns ``{"embedding_provider": ..., "embedding_model": ...,
    "amx_schema_version": ...}`` when the collection exists, or an
    empty dict on any failure (no collection yet, persist dir missing,
    chroma init blew up). Used by Studio's
    ``GET /api/profiles/code/{name}/health`` so the SPA can render the
    embedding the user's chunks are indexed with.
    """
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    try:
        client = _get_chroma().PersistentClient(path=persist)
        coll = client.get_collection(COLLECTION)
        return dict(coll.metadata or {})
    except Exception:
        return {}


def delete_code_collection(
    persist_dir: str | None = None,
    source_filters: list[str] | None = None,
) -> bool:
    """Remove the entire ``amx_code`` collection (e.g. before full re-index).

    Returns ``True`` if a collection was deleted, ``False`` if it didn't exist.
    """
    persist = persist_dir or str(Path.home() / ".amx" / "chroma_db")
    try:
        client = _get_chroma().PersistentClient(path=persist)
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
    "code_collection_metadata",
    "delete_code_collection",
    "index_codebase_tree",
    "query_code_snippets",
]


# Keep the extension list importable from here for any external
# tooling that already depends on it.
_ = CODE_EXTENSIONS
