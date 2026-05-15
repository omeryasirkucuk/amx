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

# Document-RAG is a heavy cluster (~150 MB across chromadb + the
# langchain ecosystem + unstructured's parser fleet). It only loads
# on first ``/docs ingest`` / ``/run`` with docs / RAG-backed answer
# — not on every CLI launch — so the install cost is amortised across
# the whole tool's lifetime, incurred once, by the user who actually
# uses the feature. The bundle name is shared with /search and /code
# so a user who has already touched any RAG path skips the install.
_ensure("docs-extended")

import chromadb  # noqa: E402
from langchain_community.document_loaders import (  # noqa: E402
    CSVLoader,
    Docx2txtLoader,
    PyPDFLoader,
    TextLoader,
    UnstructuredExcelLoader,
    UnstructuredHTMLLoader,
    UnstructuredMarkdownLoader,
    UnstructuredPowerPointLoader,
)
from langchain_text_splitters import RecursiveCharacterTextSplitter  # noqa: E402

from amx.docs.scanner import DocInfo  # noqa: E402
from amx.utils.logging import get_logger  # noqa: E402

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


def _resolve_active_embedding(cfg: Any | None = None) -> tuple[str, str, EmbeddingFunction | None]:
    """Resolve ``(provider, model, embedding_function)`` from config.

    Falls back to the bundled MiniLM (``embedding_function=None``) when
    no config is available or the user has the default kind selected.
    The provider/model strings are what get persisted as collection
    metadata so a later reopen can detect mismatches.
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

    # For non-default kinds the model id IS the unique identifier; if
    # the user hasn't picked one yet fall back to MiniLM rather than
    # error here (the embeddings module emits a themed warning at
    # startup for that case).
    if not model:
        return ("minilm", "minilm-l6-v2", None)

    try:
        ef = make_embedding_function(kind, model=model, api_key=api_key, base_url=base_url)
    except Exception:
        # Builder failure (missing optional dep, bad model id) — fall
        # back to MiniLM so retrieval still works; the mismatch check
        # later will catch the wrong-provider case explicitly.
        return ("minilm", "minilm-l6-v2", None)
    return (kind, model, ef)


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
    # ``.markdown`` is just the long-form extension of ``.md`` — same
    # syntax, same loader. Listing it explicitly keeps the LOADER_MAP /
    # SUPPORTED_EXTENSIONS contract honest (every supported extension
    # has its own loader entry).
    ".markdown": UnstructuredMarkdownLoader,
    ".csv": CSVLoader,
    # TSV is tab-separated values; ``CSVLoader`` is delimiter-agnostic
    # at the langchain level and treats the file as one logical record
    # per row, which is all the RAG pipeline needs (the splitter then
    # decides chunking).
    ".tsv": CSVLoader,
    ".xlsx": UnstructuredExcelLoader,
    ".xls": UnstructuredExcelLoader,
    ".html": UnstructuredHTMLLoader,
    ".htm": UnstructuredHTMLLoader,
    ".pptx": UnstructuredPowerPointLoader,
    ".json": TextLoader,
    ".yaml": TextLoader,
    ".yml": TextLoader,
    ".rst": TextLoader,
    # Python source files: index as plain text so the chunker can pull
    # out docstrings / comments alongside code identifiers.
    ".py": TextLoader,
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
        cfg: Any | None = None,
    ):
        self.persist_dir = persist_dir or str(Path.home() / ".amx" / "chroma_db")
        Path(self.persist_dir).mkdir(parents=True, exist_ok=True)
        self.client = chromadb.PersistentClient(path=self.persist_dir)

        # Resolve the active embedding provider so we can (a) wire it
        # into Chroma's ``embedding_function=`` slot — historically
        # omitted, which silently forced bundled MiniLM regardless of
        # the user's ``cfg.embedding`` choice — and (b) record it on
        # the collection metadata for the cross-version mismatch
        # check below.
        if embedding_provider is None or embedding_model is None or embedding_function is None:
            resolved_provider, resolved_model, resolved_ef = _resolve_active_embedding(cfg)
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
        succeeded: list[str] = []
        failed: list[tuple[str, str]] = []
        total_chunks = 0
        for doc in docs:
            loader_cls = LOADER_MAP.get(doc.extension)
            if loader_cls is None:
                reason = f"no loader for extension {doc.extension!r}"
                log.warning("No loader for %s, skipping %s", doc.extension, doc.path)
                failed.append((doc.path, reason))
                continue
            try:
                loader = loader_cls(doc.path)
                pages = loader.load()
                chunks = self.splitter.split_documents(pages)
                if not chunks:
                    failed.append((doc.path, "empty document (no chunks produced)"))
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
                        log.info(
                            "Deleted %d orphan chunk(s) for %s before re-ingest",
                            len(orphans),
                            doc.path,
                        )
                    except Exception as exc:
                        log.warning("Could not delete orphan chunks for %s: %s", doc.path, exc)

                self.collection.upsert(ids=ids, documents=texts, metadatas=metadatas)
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
    ) -> list[dict]:
        raw_n = max(int(n_results), min(int(n_results) * 4, 40))

        def _do_query() -> Any:
            return self.collection.query(query_texts=[question], n_results=raw_n)

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
        hits: list[dict] = []
        for i in range(len(results["documents"][0])):
            meta = results["metadatas"][0][i]
            if not self._source_allowed(meta):
                continue
            raw_distance = results["distances"][0][i] if results.get("distances") else None
            if (
                max_distance is not None
                and raw_distance is not None
                and float(raw_distance) > max_distance
            ):
                continue
            hits.append(
                {
                    "text": results["documents"][0][i],
                    "metadata": meta,
                    "distance": raw_distance,
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
