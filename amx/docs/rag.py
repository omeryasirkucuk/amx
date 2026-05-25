"""RAG pipeline — chunk documents and store in ChromaDB for retrieval."""

from __future__ import annotations

import concurrent.futures
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from amx.utils.optional_deps import ensure as _ensure

if TYPE_CHECKING:
    from chromadb.api.types import EmbeddingFunction

# Document-RAG pulls a heavy cluster (~150 MB across chromadb + the
# langchain ecosystem + unstructured's parser fleet). The cluster is
# fetched on first :class:`RAGStore` construction, NOT at module
# import — Studio's transitive import path runs through this module
# unconditionally (web.routers.ask -> agents.orchestrator ->
# agents.rag_agent -> here), and a module-level install call would
# block every Studio cold start on a fresh ``pip install amx-cli``.
# The bundle name is shared with /search and /code, so a user who
# has already touched any RAG path skips the install.

from amx.docs._fts5_sidecar import FTS5Sidecar
from amx.docs.scanner import DocInfo
from amx.rag_core.fusion import (
    maximal_marginal_relevance,
    reciprocal_rank_fusion,
)
from amx.rag_core.rerank import (
    CrossEncoderReranker,
    reranker_from_kind,
)
from amx.utils.logging import get_logger

log = get_logger("docs.rag")


class RAGQueryTimeout(TimeoutError):
    """Raised by :meth:`RAGStore.query` when the per-call wall-clock cap fires.

    The :class:`RAGAgent` catches this to record a diagnostic and fall
    back to running the table without RAG context; other call sites can
    treat it as a soft failure and return an empty hit list.
    """


class EmbeddingProviderMismatch(RuntimeError):
    """Raised when the active embedding identity does not match the
    one used to populate the existing Chroma collection.

    The collection metadata records the provider/model/dim used at
    first create. If the user later switches embedding providers (via
    ``/embeddings``) the existing vectors are in a different semantic
    space, so retrieval would silently degrade. Raising here forces
    an explicit reindex or revert decision.

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
            f"Docs RAG collection was indexed with provider={recorded_provider} "
            f"model={recorded_model}. Current config says provider={active_provider} "
            f"model={active_model}{dim_suffix}. "
            "Run `/docs reindex` to rebuild the collection with the active provider, "
            "or update the embedding profile to match the indexed model."
        )


# Schema version for the docs RAG collection metadata. Bumped when the
# metadata shape changes in a way old AMX cannot read. v2 added
# ``embedding_dim`` — older collections still work; the field is
# backfilled silently on first reopen.
_AMX_RAG_SCHEMA_VERSION = 2


def _resolve_docs_embedding(cfg: Any | None = None) -> tuple[str, str, EmbeddingFunction | None]:
    """Resolve ``(provider, model, embedding_function)`` for docs RAG.

    Reads ``cfg.embedding_docs``. Falls back to the bundled MiniLM
    (``embedding_function=None``) when no config is available or the
    user has the default kind selected. The provider/model strings are
    what get persisted as collection metadata so a later reopen can
    detect mismatches.
    """
    from amx.rag_core.embedding_resolver import resolve_embedding

    return resolve_embedding(
        "docs", cfg, default_resolver=lambda: ("minilm", "minilm-l6-v2", None)
    ).as_tuple()


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


class _StringFnLoader:
    """Adapter wrapping a ``(path) -> str`` loader as the langchain loader
    protocol (``__init__(path)`` + ``.load() -> list[Document]``).

    The pages module ships pure-Python loaders for ``.xlsx`` and ``.eml``
    that return a single markdown string; this adapter feeds them into
    the same ingest path the langchain loaders use without the heavy
    ``unstructured`` dependency.
    """

    def __init__(self, fn: Any, path: str | Path) -> None:
        self._fn = fn
        self._path = str(path)

    def load(self) -> list[Any]:
        from langchain_core.documents import Document

        text = self._fn(self._path)
        return [Document(page_content=text, metadata={"source": self._path})]


def _make_string_fn_loader(fn: Any) -> Any:
    """Return a class-shaped loader factory that closes over ``fn``."""

    def _factory(path: str | Path) -> _StringFnLoader:
        return _StringFnLoader(fn, path)

    return _factory


def _build_loader_map() -> dict[str, Any]:
    """Build the extension -> langchain loader class map on first use.

    The langchain document loaders ship in the ``docs-extended``
    bundle, which is fetched lazily. Building the map at runtime
    keeps :mod:`amx.docs.rag`'s import cost limited to the standard
    library (Studio's cold-start path imports this module without
    needing the loaders).
    """
    _ensure("docs-extended")
    from langchain_community.document_loaders import (
        CSVLoader,
        Docx2txtLoader,
        PyPDFLoader,
        TextLoader,
        UnstructuredExcelLoader,
        UnstructuredHTMLLoader,
        UnstructuredPowerPointLoader,
    )

    from amx.docs.loaders.eml_loader import load_eml
    from amx.docs.loaders.xlsx_loader import load_xlsx

    return {
        ".pdf": PyPDFLoader,
        ".docx": Docx2txtLoader,
        ".doc": Docx2txtLoader,
        ".txt": TextLoader,
        # PR-D: Markdown loads as plain text (not via
        # ``UnstructuredMarkdownLoader``) because the Markdown-aware
        # splitter in ``amx.docs.splitters`` needs the raw ``#`` /
        # ``##`` markers to extract heading metadata. The Unstructured
        # backend silently strips formatting, making every Markdown
        # chunk look identical to plain prose to the splitter.
        ".md": TextLoader,
        ".markdown": TextLoader,
        ".csv": CSVLoader,
        # TSV is tab-separated values; ``CSVLoader`` is delimiter-
        # agnostic at the langchain level and treats the file as one
        # logical record per row.
        ".tsv": CSVLoader,
        # Pure-Python xlsx loader from amx.docs.loaders: produces a
        # markdown table per sheet, no Unstructured runtime needed.
        ".xlsx": _make_string_fn_loader(load_xlsx),
        ".xls": UnstructuredExcelLoader,
        ".html": UnstructuredHTMLLoader,
        ".htm": UnstructuredHTMLLoader,
        ".pptx": UnstructuredPowerPointLoader,
        ".json": TextLoader,
        ".yaml": TextLoader,
        ".yml": TextLoader,
        ".rst": TextLoader,
        ".py": TextLoader,
        # Email loader: headers as a frontmatter block plus plain-text
        # body (falling back to markdownify-converted HTML).
        ".eml": _make_string_fn_loader(load_eml),
    }


@dataclass(frozen=True)
class IngestSummary:
    """Per-file outcome of a single :meth:`RAGStore.ingest` call.

    Replaces the historic bare-``int`` return so CLI and Studio can
    actually tell the user *which* files failed and why. The class
    still answers ``int(...)`` with the total chunk count so existing
    callers that wrote ``f"{chunks} chunks"`` keep working without a
    lock-step rewrite.
    """

    #: Source paths that produced at least one chunk in the collection.
    succeeded: list[str] = field(default_factory=list)
    #: ``(source_path, short error message)`` for every file that the
    #: loader, splitter, or upsert step failed on. Each tuple is
    #: render-ready: the CLI emits one line per entry, Studio emits
    #: ``{path, error}`` objects in the ``ingest.summary`` SSE event.
    failed: list[tuple[str, str]] = field(default_factory=list)
    #: Total number of chunks upserted into the collection. Preserved
    #: as a separate counter (instead of derived from ``succeeded``)
    #: because one file produces many chunks.
    chunk_count: int = 0

    def __int__(self) -> int:
        return int(self.chunk_count)


class RAGStore:
    def __init__(
        self,
        persist_dir: str | None = None,
        source_filters: list[str] | None = None,
        *,
        embedding_function: EmbeddingFunction | None = None,
        embedding_provider: str | None = None,
        embedding_model: str | None = None,
        reranker_kind: str | None = None,
        cfg: Any | None = None,
    ):
        # First runtime touchpoint for the heavy RAG cluster — install
        # the bundle and bind chromadb locally. Module-level imports
        # are intentionally absent so Studio cold start can import
        # this module without paying the ~150 MB install cost.
        _ensure("docs-extended")
        import chromadb

        self.persist_dir = persist_dir or str(Path.home() / ".amx" / "chroma_db")
        Path(self.persist_dir).mkdir(parents=True, exist_ok=True)
        self.client = chromadb.PersistentClient(path=self.persist_dir)

        # Resolve the active embedding provider so we can (a) wire it
        # into Chroma's ``embedding_function=`` slot — historically
        # omitted, which silently forced bundled MiniLM regardless of
        # the user's ``cfg.embedding_docs`` choice — and (b) record it on
        # the collection metadata for the cross-version mismatch
        # check below.
        if embedding_provider is None or embedding_model is None or embedding_function is None:
            resolved_provider, resolved_model, resolved_ef = _resolve_docs_embedding(cfg)
            if embedding_provider is None:
                embedding_provider = resolved_provider
            if embedding_model is None:
                embedding_model = resolved_model
            if embedding_function is None:
                embedding_function = resolved_ef

        # PR-B: resolve the dim alongside provider/model so the
        # collection records the full identity triple. ``0`` means
        # "unknown" — disables the dim half of the mismatch check
        # without breaking anything.
        from amx.rag_core.collection_identity import infer_dimension

        embedding_dim = infer_dimension(embedding_provider, embedding_model, embedding_function)

        self.embedding_provider = embedding_provider
        self.embedding_model = embedding_model
        self.embedding_dim = embedding_dim
        self.embedding_function = embedding_function

        kwargs: dict[str, Any] = {
            "name": "amx_docs",
            "metadata": {
                "hnsw:space": "cosine",
                "embedding_provider": embedding_provider,
                "embedding_model": embedding_model,
                "embedding_dim": int(embedding_dim),
                "amx_schema_version": _AMX_RAG_SCHEMA_VERSION,
            },
        }
        if embedding_function is not None:
            kwargs["embedding_function"] = embedding_function
        self.collection = self.client.get_or_create_collection(**kwargs)

        # ``get_or_create_collection`` only honours ``metadata=`` on
        # FIRST create; for an existing collection Chroma drops the
        # passed-in dict on the floor. So after open we explicitly
        # reconcile:
        #
        # * If the existing collection has recorded provider/model
        #   that DON'T match the active config → raise mismatch.
        # * If recorded provider+model match but recorded dim differs
        #   from a non-zero active dim → raise. (PR-B addition; catches
        #   the silent-corruption case where two providers expose the
        #   same model name string with different vector dimensions.)
        # * If recorded dim is 0 (legacy v1 metadata) but active dim is
        #   non-zero → backfill the dim onto the collection so future
        #   reopens get the tighter check.
        # * If it has no recorded provider/model (pre-PR-B
        #   collection) → backfill the full metadata now; do NOT force
        #   a reindex (grandfather rule from the design spec).
        existing_meta = dict(self.collection.metadata or {})
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
                raise EmbeddingProviderMismatch(
                    recorded_provider=str(recorded_provider),
                    recorded_model=str(recorded_model),
                    active_provider=str(embedding_provider),
                    active_model=str(embedding_model),
                    recorded_dim=recorded_dim,
                    active_dim=embedding_dim,
                )
            if recorded_dim == 0 and embedding_dim > 0:
                # Upgrade legacy v1 metadata to record the dim so the
                # next reopen has it for comparison. Same modify()
                # pattern as the full backfill below.
                merged = {k: v for k, v in existing_meta.items() if not str(k).startswith("hnsw:")}
                merged["embedding_dim"] = int(embedding_dim)
                merged["amx_schema_version"] = _AMX_RAG_SCHEMA_VERSION
                try:
                    self.collection.modify(metadata=merged)
                except Exception as exc:
                    log.warning("Could not upgrade RAG collection metadata dim: %s", exc)
        else:
            # Pre-existing collection without metadata — write it now
            # so future opens have something to compare against.
            # Strip ``hnsw:*`` keys: Chroma treats those as construction-
            # time parameters and rejects ``modify(metadata=)`` calls
            # that try to "change" them (even to the same value).
            merged = {k: v for k, v in existing_meta.items() if not str(k).startswith("hnsw:")}
            merged["embedding_provider"] = embedding_provider
            merged["embedding_model"] = embedding_model
            merged["embedding_dim"] = int(embedding_dim)
            merged["amx_schema_version"] = _AMX_RAG_SCHEMA_VERSION
            try:
                self.collection.modify(metadata=merged)
            except Exception as exc:
                # Non-fatal: the collection still works, we just lose
                # the mismatch check on the next reopen.
                log.warning("Could not backfill RAG collection metadata: %s", exc)

        # PR-D removed the single-splitter ``self.splitter`` attribute.
        # Per-document chunking now goes through
        # :func:`amx.docs.splitters.get_splitter` which picks the
        # right splitter based on the file extension.
        self.source_filters = [
            self._normalize_source_filter(s) for s in (source_filters or []) if s
        ]

        # PR-E: SQLite FTS5 sidecar for the BM25 half of hybrid
        # retrieval. Lives in the same persist dir as Chroma
        # (``<persist_dir>/docs_fts.sqlite``) so backup/restore lifts
        # both together. Every upsert into Chroma also lands in the
        # sidecar; ``query()`` fuses dense + lexical via RRF.
        self._fts = FTS5Sidecar(self.persist_dir)

        # PR-F: optional cross-encoder reranker. When ``reranker_kind``
        # is None / "heuristic", retrieval keeps using the in-process
        # heuristic ``rerank``. When a real model id is passed
        # (e.g. ``"cross_encoder"``), the cross-encoder replaces the
        # heuristic on the candidate pool — opt-in because it requires
        # ``sentence-transformers`` (~500 MB) and adds 30-200 ms per
        # query. The factory returns ``None`` on unknown / heuristic
        # kinds; the factory and the wrapper both degrade silently to
        # the heuristic on load failure (no install, no network).
        resolved_kind = reranker_kind
        if resolved_kind is None:
            docs_cfg = getattr(cfg, "docs", None) if cfg is not None else None
            rerank_cfg = getattr(docs_cfg, "rerank", None) if docs_cfg is not None else None
            resolved_kind = getattr(rerank_cfg, "kind", None) if rerank_cfg is not None else None
        self._cross_encoder: CrossEncoderReranker | None = reranker_from_kind(resolved_kind or "")
        # First-time-after-PR-E backfill: if the sidecar is empty but
        # the Chroma collection has chunks (returning user upgrading
        # AMX), seed the FTS table from existing chunks so hybrid
        # retrieval works immediately rather than requiring a manual
        # ``/docs ingest --refresh``. Best-effort; failures degrade to
        # vector-only.
        self._maybe_backfill_fts_from_chroma()

    def _maybe_backfill_fts_from_chroma(self) -> None:
        """Populate the FTS5 sidecar from Chroma if Chroma has data
        and the sidecar is empty. One-time migration for collections
        created before PR-E.
        """
        try:
            if self._fts.count() > 0:
                return  # sidecar already populated; nothing to do
            existing = self.collection.get(include=["documents", "metadatas"])
        except Exception as exc:
            log.warning("Could not inspect Chroma for FTS backfill: %s", exc)
            return
        ids = list(existing.get("ids") or [])
        documents = list(existing.get("documents") or [])
        metadatas = list(existing.get("metadatas") or [])
        if not ids:
            return
        rows: list[tuple[str, str, str]] = []
        for cid, content, meta in zip(ids, documents, metadatas, strict=False):
            if not isinstance(content, str):
                continue
            source = ""
            if isinstance(meta, dict):
                source = str(meta.get("source") or "")
            rows.append((str(cid), source, content))
        if rows:
            inserted = self._fts.upsert(rows)
            log.info("Backfilled FTS5 sidecar from %d existing Chroma chunks", inserted)

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
                # PR-E: drop the same chunks from the FTS5 sidecar so
                # hybrid retrieval doesn't surface stale lexical hits.
                self._fts.delete_by_ids(ids)
                removed += len(ids)
                log.info("Deleted %d chunks for source %s", len(ids), src)
        return removed

    def reset_collection(self) -> None:
        """Drop the docs vector store and rebuild it with the active
        identity.

        ``ingest(refresh=True)`` removes documents one source at a
        time but leaves the Chroma collection's identity metadata
        (``embedding_provider`` / ``embedding_model`` /
        ``embedding_dim``) intact. After an ``/embeddings`` swap that
        leaves the user stuck — the next ``RAGStore()`` open reads
        the stale identity and raises
        :class:`EmbeddingProviderMismatch`. ``reset_collection``
        drops the Chroma collection outright AND clears the FTS5
        sidecar so the next ingest stamps fresh identity metadata
        and the BM25 channel cannot resurface chunks from the dropped
        collection.

        The instance keeps working after the reset: ``self.collection``
        is rebound to a freshly opened Chroma collection with the
        active triple.
        """
        name = self.collection.name if hasattr(self.collection, "name") else "amx_docs"
        try:
            self.client.delete_collection(name=name)
        except Exception as exc:
            log.debug("reset_collection: delete_collection(%s) skipped: %s", name, exc)
        try:
            self._fts.clear()
        except Exception as exc:
            log.debug("reset_collection: fts clear failed: %s", exc)
        kwargs: dict[str, Any] = {
            "name": name,
            "metadata": {
                "hnsw:space": "cosine",
                "embedding_provider": self.embedding_provider,
                "embedding_model": self.embedding_model,
                "embedding_dim": int(self.embedding_dim),
                "amx_schema_version": _AMX_RAG_SCHEMA_VERSION,
            },
        }
        if self.embedding_function is not None:
            kwargs["embedding_function"] = self.embedding_function
        self.collection = self.client.get_or_create_collection(**kwargs)

    def ingest(
        self,
        docs: list[DocInfo],
        *,
        refresh: bool = False,
    ) -> IngestSummary:
        """Load each document, split it into chunks, upsert into Chroma.

        Returns an :class:`IngestSummary` whose ``failed`` list carries
        a one-line reason per file that the loader or splitter couldn't
        process. Files with no extracted chunks (e.g. a truly empty
        ``.md``) land in ``failed`` with an ``"empty document"`` reason
        rather than being silently dropped — silent drops were the
        whole reason this contract changed in PR A.
        """
        if refresh and docs:
            self.delete_chunks_for_sources([x for d in docs for x in (d.path, d.source_root) if x])
        # Lazy imports: splitters and the loader-class map both rely
        # on the ``docs-extended`` bundle. Building them here (not at
        # module top) keeps :mod:`amx.docs.rag` import light enough
        # for Studio's transitive cold-start path.
        from amx.docs.splitters import get_splitter

        loader_map = _build_loader_map()
        succeeded: list[str] = []
        failed: list[tuple[str, str]] = []
        total_chunks = 0
        for doc in docs:
            loader_cls = loader_map.get(doc.extension)
            if loader_cls is None:
                reason = f"no loader for extension {doc.extension!r}"
                log.warning("No loader for %s, skipping %s", doc.extension, doc.path)
                failed.append((doc.path, reason))
                continue
            try:
                loader = loader_cls(doc.path)
                pages = loader.load()
                # PR-D: per-extension dispatcher routes Markdown
                # documents through a header-aware splitter that
                # preserves section metadata; non-Markdown extensions
                # use the same RecursiveCharacterTextSplitter as
                # before so retrieval quality is unchanged on the
                # docs RAG eval baseline.
                splitter = get_splitter(doc.extension)
                chunks = splitter.split_documents(pages)
                if not chunks:
                    failed.append((doc.path, "empty document (no chunks produced)"))
                    continue

                ids = [f"{doc.path}::{i}" for i in range(len(chunks))]
                texts = [c.page_content for c in chunks]
                metadatas = []
                for i, chunk in enumerate(chunks):
                    meta = {
                        "source": doc.path,
                        "source_root": self._normalize_source_filter(doc.source_root or doc.path),
                        "source_type": doc.source_type,
                        "chunk_idx": i,
                    }
                    # PR-D: propagate header metadata produced by the
                    # Markdown-aware splitter (h1/h2/h3 keys). Only
                    # set values are recorded — Chroma rejects None.
                    chunk_meta = getattr(chunk, "metadata", None) or {}
                    for header_key in ("h1", "h2", "h3"):
                        value = chunk_meta.get(header_key)
                        if value:
                            meta[header_key] = str(value)
                    metadatas.append(meta)

                # Idempotency on file shrink: if the previous ingest of
                # this exact path produced N chunks and the new content
                # only produces M<N, chunks ``::M`` … ``::N-1`` would
                # otherwise live on in the collection forever. Look up
                # the existing ID set for this source and delete any
                # that the new upsert won't cover. Done BEFORE the
                # upsert so a partial failure leaves the file in a
                # half-deleted state at worst, never a half-orphaned
                # one.
                try:
                    existing = self.collection.get(where={"source": doc.path}, include=[])
                    existing_ids = set(existing.get("ids") or [])
                except Exception as exc:
                    log.warning("Could not enumerate existing chunks for %s: %s", doc.path, exc)
                    existing_ids = set()
                new_id_set = set(ids)
                orphans = sorted(existing_ids - new_id_set)
                if orphans:
                    try:
                        self.collection.delete(ids=orphans)
                        # PR-E: keep FTS5 sidecar in sync with Chroma
                        # — orphans must vanish from the lexical index
                        # too, otherwise BM25 will keep surfacing them.
                        self._fts.delete_by_ids(orphans)
                        log.info(
                            "Deleted %d orphan chunk(s) for %s before re-ingest",
                            len(orphans),
                            doc.path,
                        )
                    except Exception as exc:
                        log.warning("Could not delete orphan chunks for %s: %s", doc.path, exc)

                self.collection.upsert(ids=ids, documents=texts, metadatas=metadatas)
                # PR-E: mirror the same chunks into the FTS5 sidecar
                # so BM25 retrieval sees identical content. The
                # sidecar upsert is best-effort — a failure logs and
                # degrades to vector-only retrieval for this corpus,
                # not a hard ingest failure.
                self._fts.upsert(zip(ids, [doc.path] * len(ids), texts, strict=False))
                total_chunks += len(chunks)
                succeeded.append(doc.path)
                log.info("Ingested %s -> %d chunks", doc.path, len(chunks))
            except Exception as exc:
                # One-line reason — full traceback already in the log.
                # Keep ``exc.__class__.__name__`` so the user sees the
                # category at a glance (PdfReadError, UnicodeDecodeError,
                # PermissionError, ...) without needing the log file.
                reason = f"{exc.__class__.__name__}: {exc}"
                log.error("Error ingesting %s: %s", doc.path, exc)
                failed.append((doc.path, reason))
        return IngestSummary(succeeded=succeeded, failed=failed, chunk_count=total_chunks)

    def query(
        self,
        question: str,
        n_results: int = 5,
        *,
        timeout: float | None = None,
        min_similarity: float = 0.0,
        use_mmr: bool = True,
        mmr_lambda: float = 0.7,
    ) -> list[dict]:
        raw_n = max(int(n_results), min(int(n_results) * 4, 40))

        def _do_query() -> Any:
            return self.collection.query(
                query_texts=[question],
                n_results=raw_n,
                include=["documents", "metadatas", "distances"],
            )

        # Honour the optional per-query wall-clock cap. We submit the
        # Chroma call to a fresh single-thread executor scoped to a
        # context manager so the worker thread is reaped even on
        # timeout — a leaked daemon thread per query would pile up
        # under a slow vector store. ``timeout=None`` or ``timeout<=0``
        # skips the executor and runs synchronously (matches the pre-
        # PR-D fast path with zero overhead for callers that opted
        # out).
        if timeout is not None and float(timeout) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(_do_query)
                try:
                    results = future.result(timeout=float(timeout))
                except concurrent.futures.TimeoutError as exc:
                    log.warning(
                        "RAG retrieval exceeded %.2fs timeout, proceeding without context",
                        float(timeout),
                    )
                    # Best-effort cancel; the underlying Chroma call
                    # may still finish in the background thread but
                    # the executor's context manager joins it before
                    # we leave this block. Re-raise as a typed
                    # exception so callers can branch on the timeout
                    # case (and surface a user-facing diagnostic)
                    # without inspecting an empty hit list.
                    future.cancel()
                    raise RAGQueryTimeout(
                        f"RAG retrieval exceeded {float(timeout):.2f}s timeout"
                    ) from exc
        else:
            results = _do_query()

        # Drop chunks below the caller's relevance floor before anything
        # else looks at them. Chroma uses cosine *distance* (1 -
        # cosine_similarity) on minilm-l6-v2 collections — so a higher
        # distance is a worse match. The threshold compares against the
        # equivalent ``1 - min_similarity`` ceiling. ``min_similarity=0``
        # disables the filter (legacy behaviour). A real-world case the
        # filter catches: a resume PDF chunked alongside a database
        # generated distances 0.66–1.02 against a ``zip_code`` column
        # (cosine_sim ≈ 0.34 down to -0.02 — basically orthogonal); the
        # LLM consumed them and synthesised absurd descriptions.
        threshold = float(min_similarity or 0.0)
        max_distance = (1.0 - threshold) if threshold > 0.0 else None

        # Build a dict keyed by chunk_id so we can fuse with the FTS5
        # ranking without duplicating entries. Vector candidates are
        # appended in retrieval order so the chunk_id list below
        # preserves Chroma's ranking.
        hits_by_id: dict[str, dict] = {}
        vector_ranking: list[str] = []
        ids_row = (results.get("ids") or [[]])[0]
        documents_row = (results.get("documents") or [[]])[0]
        metadatas_row = (results.get("metadatas") or [[]])[0]
        distances_row = (results.get("distances") or [[]])[0] if results.get("distances") else []
        for i in range(len(documents_row)):
            # Use the Chroma id when available; otherwise synthesise a
            # per-position id so test fakes that don't surface ``ids``
            # still produce distinct dict entries. Synthetic ids never
            # collide with real ids (real ones contain ``::`` as the
            # ``"{path}::{idx}"`` separator).
            raw_id = ids_row[i] if i < len(ids_row) and ids_row[i] is not None else None
            chunk_id = str(raw_id) if raw_id is not None and str(raw_id) else f"__synth__::{i}"
            meta = metadatas_row[i] if i < len(metadatas_row) else {}
            if not self._source_allowed(meta):
                continue
            raw_distance = distances_row[i] if i < len(distances_row) else None
            if (
                max_distance is not None
                and raw_distance is not None
                and float(raw_distance) > max_distance
            ):
                continue
            hits_by_id[chunk_id] = {
                "id": chunk_id,
                "text": documents_row[i],
                "metadata": meta,
                "distance": raw_distance,
            }
            vector_ranking.append(chunk_id)

        # PR-E: BM25 channel via the FTS5 sidecar. Top-N candidates
        # scored by BM25 join the candidate pool; chunks present
        # only in the lexical channel get their text+metadata
        # back-filled from Chroma so downstream rerank sees a
        # uniform shape. Sidecar errors degrade to vector-only.
        # ``getattr`` default handles tests that bypass __init__
        # (e.g. ``object.__new__(RAGStore)`` with hand-set
        # ``collection`` only) — no sidecar means vector-only,
        # exactly the pre-PR-E behaviour.
        lexical_ranking: list[str] = []
        fts = getattr(self, "_fts", None)
        lexical_hits = fts.query(question, k=raw_n) if fts is not None else []
        if lexical_hits:
            missing_ids = [cid for cid, _ in lexical_hits if cid and cid not in hits_by_id]
            if missing_ids:
                try:
                    enrich = self.collection.get(
                        ids=missing_ids, include=["documents", "metadatas"]
                    )
                except Exception as exc:
                    log.warning("Could not enrich BM25-only hits from Chroma: %s", exc)
                    enrich = {"ids": [], "documents": [], "metadatas": []}
                got_ids = list(enrich.get("ids") or [])
                got_docs = list(enrich.get("documents") or [])
                got_metas = list(enrich.get("metadatas") or [])
                for cid, content, meta in zip(got_ids, got_docs, got_metas, strict=False):
                    if not self._source_allowed(meta):
                        continue
                    hits_by_id[str(cid)] = {
                        "id": str(cid),
                        "text": content,
                        "metadata": meta,
                        "distance": None,  # not measured for BM25-only hits
                    }
            for chunk_id, _score in lexical_hits:
                if chunk_id in hits_by_id:
                    lexical_ranking.append(chunk_id)

        # Fuse the two rankings via RRF. When only one channel
        # produced results (no BM25 sidecar yet, or query had no
        # alphanumeric tokens), RRF collapses to that channel's
        # original order — exact backward-compat for vector-only
        # corpora.
        if vector_ranking or lexical_ranking:
            rankings = [r for r in (vector_ranking, lexical_ranking) if r]
            rrf_scores = reciprocal_rank_fusion(rankings)
            fused_order = sorted(
                rrf_scores.items(), key=lambda kv: (-kv[1], kv[0])
            )  # tiebreak by id for determinism
            hits = [hits_by_id[cid] for cid, _ in fused_order if cid in hits_by_id]
        else:
            hits = list(hits_by_id.values())

        # PR-F: cross-encoder rerank replaces the heuristic when
        # configured. The cross-encoder is opt-in and may silently
        # fall back to the heuristic on model-load failure. The
        # ``getattr`` default keeps tests that bypass __init__
        # (e.g. ``object.__new__(RAGStore)``) on the heuristic path.
        cross_encoder = getattr(self, "_cross_encoder", None)
        if cross_encoder is not None:
            reranked = cross_encoder.rerank(question, hits)
        else:
            reranked = self.rerank(question, hits)

        # PR-I: MMR for diversity. After rerank we have a relevance
        # ordering; MMR re-orders it to demote near-duplicate
        # chunks (e.g. three consecutive paragraphs of the same
        # section that all match the query). ``mmr_lambda=0.7``
        # leans toward relevance but actively avoids duplicates.
        # MMR runs over the full reranked pool then we take the
        # first ``n_results`` — that way the diversity-aware
        # selection has the most material to work with rather than
        # being asked to diversify an already-tiny list.
        if use_mmr and len(reranked) > 1:
            mmr_candidates = self._build_mmr_candidates(reranked)
            if mmr_candidates:
                picked_ids = maximal_marginal_relevance(
                    candidates=mmr_candidates,
                    k=len(reranked),
                    lambda_=mmr_lambda,
                )
                by_id = {h.get("id"): h for h in reranked if h.get("id")}
                # Append in MMR order, then any hits without an id (in
                # case of legacy sidecar-only entries) at the end so
                # callers don't lose hits.
                ordered = [by_id[cid] for cid in picked_ids if cid in by_id]
                seen_ids = {cid for cid in picked_ids if cid in by_id}
                for h in reranked:
                    if h.get("id") not in seen_ids:
                        ordered.append(h)
                reranked = ordered
        return reranked[:n_results]

    def _build_mmr_candidates(self, hits: list[dict]) -> list[tuple[str, float, list[float]]]:
        """Fetch embeddings for the reranked hits and build the MMR
        candidate triples ``(chunk_id, relevance, embedding)``.

        Uses the rerank ``score`` as the relevance signal (already
        computed; no query re-embedding needed). Embeddings come
        from Chroma's per-chunk store in one batched call. Hits
        without an embedding (e.g. BM25-only enrichment edges, or
        chunks the embedding fetch couldn't find) are dropped from
        the MMR set — the caller falls back to the rerank order
        for those.
        """
        ids = [str(h.get("id") or "") for h in hits if h.get("id")]
        if not ids:
            return []
        try:
            fetched = self.collection.get(ids=ids, include=["embeddings"])
        except Exception as exc:
            log.warning("MMR: could not fetch embeddings: %s; falling back to rerank order", exc)
            return []
        # Chroma may return numpy arrays here, not lists — ``or []``
        # raises \"truth value of an empty array is ambiguous\" on
        # those. Pull the fields with explicit None-check and let
        # zip terminate naturally when either side runs out.
        fetched_ids = fetched.get("ids")
        fetched_embeddings = fetched.get("embeddings")
        if fetched_ids is None or fetched_embeddings is None:
            return []
        emb_by_id: dict[str, list[float]] = {}
        for cid, emb in zip(fetched_ids, fetched_embeddings, strict=False):
            if emb is None:
                continue
            try:
                emb_by_id[str(cid)] = [float(x) for x in emb]
            except (TypeError, ValueError):
                continue
        if not emb_by_id:
            return []
        triples: list[tuple[str, float, list[float]]] = []
        for h in hits:
            cid = str(h.get("id") or "")
            if not cid or cid not in emb_by_id:
                continue
            try:
                rel = float(h.get("score") or 0.0)
            except (TypeError, ValueError):
                rel = 0.0
            triples.append((cid, rel, emb_by_id[cid]))
        return triples

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

        # Persist the rerank score onto each hit so callers can carry
        # it through to user-facing citations (PR C) without having to
        # recompute the same heuristic at the agent layer.
        for hit in hits:
            hit["score"] = float(_score(hit))
        return sorted(hits, key=lambda h: h["score"], reverse=True)

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
