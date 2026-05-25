"""Asset-RAG store: chunked + embedded semantic retrieval over ingested
remote assets.

Mirrors :mod:`amx.docs.rag` for the asset surface (notebooks,
queries, pipelines, streams, streamlit apps, jobs). Each consumer
(Pages composer, Ask retrieval, Run worker, Studio /assets) hits
this single store; chunking + embedding logic lives here.

v1 ships dense-only retrieval (Chroma + cosine). The BM25 sidecar
that docs RAG uses is a follow-up — dense embeddings already
deliver a step-change over the BM25-lite scorer the Ask path used
before.

Collection identity (provider/model/dim) is stamped at first create
via :mod:`amx.rag_core.collection_identity`. A subsequent embedding
swap (``cfg set embedding_assets.kind ...``) surfaces
:class:`EmbeddingProviderMismatch` so retrieval cannot silently
degrade across vector spaces; ``reset_collection`` (used by
``/db assets reindex``) recovers.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, Any

from amx.assets.chunking_config import AssetChunkingConfig
from amx.assets.loaders import load_asset_documents
from amx.assets.types import AssetDocument, AssetQueryHit
from amx.utils.logging import get_logger
from amx.utils.optional_deps import ensure as _ensure

if TYPE_CHECKING:
    from chromadb.api.types import EmbeddingFunction

log = get_logger("assets.rag")

_ASSET_COLLECTION_NAME = "amx_assets"
_AMX_ASSET_RAG_SCHEMA_VERSION = 1


class EmbeddingProviderMismatch(RuntimeError):
    """Raised when the active embedding identity does not match the
    one used to populate the existing Chroma collection.

    Recovery: ``/db assets reindex`` (drops the collection and
    re-ingests under the active embedding triple).
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
            f"Assets RAG collection was indexed with provider={recorded_provider} "
            f"model={recorded_model}. Current config says provider={active_provider} "
            f"model={active_model}{dim_suffix}. "
            "Run `/db assets reindex` to rebuild under the active provider."
        )


def _resolve_assets_embedding(cfg: Any | None = None) -> tuple[str, str, Any | None]:
    """Resolve ``(provider, model, embedding_function)`` for asset RAG.

    Reads ``cfg.embedding_assets``. Falls back to the bundled MiniLM
    (``embedding_function=None``) when no config is available or the
    user has the default kind selected.
    """
    from amx.rag_core.embedding_resolver import resolve_embedding

    return resolve_embedding(
        "assets", cfg, default_resolver=lambda: ("minilm", "minilm-l6-v2", None)
    ).as_tuple()


class AssetRAGStore:
    """Chunked + embedded semantic retrieval over ingested remote assets."""

    def __init__(
        self,
        persist_dir: str | None = None,
        *,
        embedding_function: EmbeddingFunction | None = None,
        embedding_provider: str | None = None,
        embedding_model: str | None = None,
        cfg: Any | None = None,
        chunking_cfg: AssetChunkingConfig | None = None,
    ):
        # Lazy install of the docs-extended bundle (chromadb +
        # sentence-transformers). Same pattern as docs RAG so module
        # import stays cheap.
        _ensure("docs-extended")
        import chromadb

        self.persist_dir = persist_dir or str(Path.home() / ".amx" / "chroma_db")
        Path(self.persist_dir).mkdir(parents=True, exist_ok=True)
        self.client = chromadb.PersistentClient(path=self.persist_dir)

        if embedding_provider is None or embedding_model is None or embedding_function is None:
            resolved_provider, resolved_model, resolved_ef = _resolve_assets_embedding(cfg)
            if embedding_provider is None:
                embedding_provider = resolved_provider
            if embedding_model is None:
                embedding_model = resolved_model
            if embedding_function is None:
                embedding_function = resolved_ef

        from amx.rag_core.collection_identity import infer_dimension

        embedding_dim = infer_dimension(embedding_provider, embedding_model, embedding_function)

        self.embedding_provider = embedding_provider
        self.embedding_model = embedding_model
        self.embedding_dim = embedding_dim
        self.embedding_function = embedding_function

        kwargs: dict[str, Any] = {
            "name": _ASSET_COLLECTION_NAME,
            "metadata": {
                "hnsw:space": "cosine",
                "embedding_provider": embedding_provider,
                "embedding_model": embedding_model,
                "embedding_dim": int(embedding_dim),
                "amx_schema_version": _AMX_ASSET_RAG_SCHEMA_VERSION,
            },
        }
        if embedding_function is not None:
            kwargs["embedding_function"] = embedding_function
        self.collection = self.client.get_or_create_collection(**kwargs)
        self._reconcile_collection_identity()

        # Resolve the asset chunking strategy. Explicit arg wins over
        # ``cfg.assets_chunking`` so tests can inject a deterministic
        # config without touching the global cfg.
        if chunking_cfg is not None:
            self.chunking_cfg = chunking_cfg
        else:
            self.chunking_cfg = (
                getattr(cfg, "assets_chunking", None) if cfg is not None else None
            ) or AssetChunkingConfig()

    # ── identity reconciliation ───────────────────────────────────

    def _reconcile_collection_identity(self) -> None:
        """Verify the recorded embedding identity matches the active one.

        On mismatch, raise :class:`EmbeddingProviderMismatch` so the
        user runs ``/db assets reindex``. On legacy collections without
        recorded metadata, backfill silently.
        """
        existing_meta = dict(self.collection.metadata or {})
        recorded_provider = existing_meta.get("embedding_provider")
        recorded_model = existing_meta.get("embedding_model")
        try:
            recorded_dim = int(existing_meta.get("embedding_dim", 0) or 0)
        except (TypeError, ValueError):
            recorded_dim = 0
        if recorded_provider and recorded_model:
            dim_mismatch = (
                recorded_dim > 0 and self.embedding_dim > 0 and recorded_dim != self.embedding_dim
            )
            if (
                recorded_provider != self.embedding_provider
                or recorded_model != self.embedding_model
                or dim_mismatch
            ):
                raise EmbeddingProviderMismatch(
                    recorded_provider=str(recorded_provider),
                    recorded_model=str(recorded_model),
                    active_provider=str(self.embedding_provider),
                    active_model=str(self.embedding_model),
                    recorded_dim=recorded_dim,
                    active_dim=self.embedding_dim,
                )
            return
        # Legacy or freshly-created collection: backfill metadata.
        merged = {k: v for k, v in existing_meta.items() if not str(k).startswith("hnsw:")}
        merged["embedding_provider"] = self.embedding_provider
        merged["embedding_model"] = self.embedding_model
        merged["embedding_dim"] = int(self.embedding_dim)
        merged["amx_schema_version"] = _AMX_ASSET_RAG_SCHEMA_VERSION
        try:
            self.collection.modify(metadata=merged)
        except Exception as exc:  # noqa: BLE001 — non-fatal
            log.warning("Could not backfill asset RAG collection metadata: %s", exc)

    # ── writes ────────────────────────────────────────────────────

    def ingest_documents(self, docs: list[AssetDocument]) -> int:
        """Upsert pre-built :class:`AssetDocument` chunks into Chroma.

        Returns the number of chunks indexed. ``upsert`` (Chroma) replaces
        any existing chunk with the same ``chunk_id``, so re-ingesting an
        asset whose source changed is idempotent.
        """
        if not docs:
            return 0
        ids = [d.chunk_id for d in docs]
        texts = [d.text for d in docs]
        metadatas: list[dict[str, Any]] = []
        for d in docs:
            meta: dict[str, Any] = {
                "kind": d.kind,
                "profile": d.profile,
                "remote_id": int(d.remote_id),
                "chunk_index": int(d.chunk_index),
            }
            for k, v in (d.metadata or {}).items():
                if v is None:
                    continue
                if isinstance(v, (str, int, float, bool)):
                    meta[k] = v
                else:
                    meta[k] = str(v)
            metadatas.append(meta)
        try:
            self.collection.upsert(ids=ids, documents=texts, metadatas=metadatas)
        except Exception as exc:  # noqa: BLE001 — Chroma errors are observable
            log.warning("Asset RAG upsert failed: %s", exc)
            return 0
        return len(ids)

    def ingest_profile(
        self,
        *,
        conn: Any,
        profile_name: str,
        kinds: list[str] | None = None,
        only_ids: dict[str, list[int]] | None = None,
        chunking: Any | None = None,
        only_changed: bool = True,
    ) -> int:
        """Re-chunk + upsert every (or scoped) asset for ``profile_name``.

        ``only_ids`` lets the auto-index hook pass the exact remote ids
        that the ingest just refreshed so a 5,000-notebook workspace
        does not re-embed unchanged assets every time.

        ``chunking`` (defaults to ``self.chunking_cfg`` resolved at
        construction) controls strategy + chunk_chars + chunk_overlap
        per kind. Tests inject a custom config; production callers
        leave it ``None`` and the cfg.yml value flows through.

        ``only_changed`` (PR-D): when true (the default), notebooks /
        queries / pipelines whose current content hash already matches
        ``last_embedded_hash`` are skipped before chunking. The hash
        update happens after a successful embed so a crashed run
        leaves the row eligible for retry. Set to ``False`` for the
        ``/db assets reindex --force`` path that drops and rebuilds
        unconditionally.
        """
        cfg = chunking if chunking is not None else self.chunking_cfg
        docs = load_asset_documents(
            conn=conn,
            profile_name=profile_name,
            kinds=kinds,
            only_ids=only_ids,
            chunking=cfg,
        )
        if only_changed:
            # PR-D: drop documents whose underlying row's current
            # content hash already matches what we last embedded.
            # Other kinds (jobs / streams / streamlit_apps) fall
            # through unchanged — their content is metadata-only and
            # cheap to re-embed.
            current = _current_hashes_for_kinds(conn, profile_name, docs)
            last = _last_embedded_hashes_for_kinds(conn, profile_name, docs)
            skipped: set[tuple[str, int]] = set()
            for (kind, rid), curr_hash in current.items():
                if curr_hash and last.get((kind, rid)) == curr_hash:
                    skipped.add((kind, rid))
            if skipped:
                docs = [d for d in docs if (d.kind, d.remote_id) not in skipped]
        # Clear any stale chunks for the asset rows we are about to
        # rewrite so a shrunk notebook (fewer cells) does not leave
        # orphaned chunks at the tail.
        self.delete_chunks_for_assets([(d.kind, d.profile, d.remote_id) for d in docs])
        written = self.ingest_documents(docs)
        if only_changed and written:
            _update_last_embedded_hashes(conn, profile_name, docs)
        return written

    def delete_chunks_for_assets(self, refs: list[tuple[str, str, int]]) -> int:
        """Delete every chunk for a list of ``(kind, profile, remote_id)``.

        Used at re-ingest time (drop stale tail) and by
        :meth:`delete_asset` when a remote asset is removed from the
        catalog.
        """
        if not refs:
            return 0
        removed = 0
        for kind, profile, remote_id in refs:
            try:
                where = {
                    "$and": [
                        {"kind": {"$eq": kind}},
                        {"profile": {"$eq": profile}},
                        {"remote_id": {"$eq": int(remote_id)}},
                    ]
                }
                res = self.collection.get(where=where, include=[])
                ids = list(res.get("ids") or [])
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "Asset RAG lookup-for-delete failed (%s:%s:%s): %s",
                    kind,
                    profile,
                    remote_id,
                    exc,
                )
                continue
            if ids:
                try:
                    self.collection.delete(ids=ids)
                    removed += len(ids)
                except Exception as exc:  # noqa: BLE001
                    log.warning("Asset RAG delete failed: %s", exc)
        return removed

    def delete_asset(self, *, kind: str, profile: str, remote_id: int) -> int:
        return self.delete_chunks_for_assets([(kind, profile, int(remote_id))])

    def reset_collection(self) -> None:
        """Drop the asset collection and rebuild with the active identity.

        ``ingest_profile`` removes stale chunks per asset but leaves
        the collection's identity metadata intact. After an embedding
        provider swap the next ``AssetRAGStore()`` open reads the
        stale identity and raises ``EmbeddingProviderMismatch``;
        ``reset_collection`` recovers.
        """
        try:
            self.client.delete_collection(name=_ASSET_COLLECTION_NAME)
        except Exception as exc:  # noqa: BLE001
            log.debug("reset_collection: delete skipped: %s", exc)
        kwargs: dict[str, Any] = {
            "name": _ASSET_COLLECTION_NAME,
            "metadata": {
                "hnsw:space": "cosine",
                "embedding_provider": self.embedding_provider,
                "embedding_model": self.embedding_model,
                "embedding_dim": int(self.embedding_dim),
                "amx_schema_version": _AMX_ASSET_RAG_SCHEMA_VERSION,
            },
        }
        if self.embedding_function is not None:
            kwargs["embedding_function"] = self.embedding_function
        self.collection = self.client.get_or_create_collection(**kwargs)

    def reindex_profile(
        self,
        *,
        conn: Any,
        profile_name: str,
        kinds: list[str] | None = None,
        force: bool = True,
    ) -> int:
        """Drop and rebuild the index for ``profile_name`` from scratch.

        PR-D: ``force=True`` (the default for this entry point —
        the user invoked ``/db assets reindex`` for a reason)
        wipes every chunk for the profile, NULLs out the per-row
        ``last_embedded_hash``, and re-embeds unconditionally.
        ``force=False`` is the cheap path for the ingest auto-hook
        — it skips assets whose hash matches the last embed.
        """
        # Delete every existing chunk for this profile first so an
        # asset that was removed since the last ingest does not linger.
        try:
            existing = self.collection.get(where={"profile": {"$eq": profile_name}}, include=[])
            stale_ids = list(existing.get("ids") or [])
            if stale_ids:
                self.collection.delete(ids=stale_ids)
        except Exception as exc:  # noqa: BLE001
            log.warning("reindex_profile: profile sweep failed: %s", exc)
        if force:
            # PR-D: clear the last_embedded_hash so the follow-up
            # ingest_profile re-embeds every row regardless of
            # current content hash.
            _clear_last_embedded_hashes(conn, profile_name)
        written = self.ingest_profile(
            conn=conn,
            profile_name=profile_name,
            kinds=kinds,
            only_changed=not force,
        )
        # Sweep any Chroma entries whose row was removed from SQLite
        # between ingest passes. Stale vectors are the documented
        # source of the "Asset not found" 404 the Studio drawer
        # used to display after a search hit. Best-effort: never
        # blocks the reindex on a sweep failure.
        try:
            self.prune_stale_vectors(conn, profile_name)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "reindex_profile: prune_stale_vectors failed for %s: %s",
                profile_name,
                exc,
            )
        return written

    # ── reads ─────────────────────────────────────────────────────

    def query(
        self,
        text: str,
        *,
        top_k: int = 5,
        profile: str | None = None,
        kind: str | None = None,
        remote_ids: list[int] | None = None,
    ) -> list[AssetQueryHit]:
        """Dense retrieval across the asset collection.

        Filters by ``profile`` / ``kind`` / ``remote_ids`` via Chroma's
        metadata where-clause. Empty query returns ``[]``.
        """
        if not text or not text.strip() or top_k <= 0:
            return []
        where = self._where_clause(profile=profile, kind=kind, remote_ids=remote_ids)
        try:
            results = self.collection.query(
                query_texts=[text],
                n_results=int(top_k),
                where=where if where else None,
                include=["documents", "metadatas", "distances"],
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Asset RAG query failed: %s", exc)
            return []
        out: list[AssetQueryHit] = []
        ids_row = (results.get("ids") or [[]])[0]
        docs_row = (results.get("documents") or [[]])[0]
        metas_row = (results.get("metadatas") or [[]])[0]
        dists_row = (results.get("distances") or [[]])[0] if results.get("distances") else []
        for i, chunk_id in enumerate(ids_row):
            meta = metas_row[i] if i < len(metas_row) else {}
            if not isinstance(meta, dict):
                meta = {}
            text_val = docs_row[i] if i < len(docs_row) else ""
            distance = dists_row[i] if i < len(dists_row) else None
            score = 1.0 - float(distance) if distance is not None else 0.0
            out.append(
                AssetQueryHit(
                    chunk_id=str(chunk_id),
                    kind=str(meta.get("kind") or ""),
                    profile=str(meta.get("profile") or ""),
                    remote_id=int(meta.get("remote_id") or 0),
                    name=str(meta.get("asset_name") or ""),
                    text=str(text_val or ""),
                    score=score,
                    metadata={
                        k: v
                        for k, v in meta.items()
                        if k not in {"kind", "profile", "remote_id", "asset_name"}
                    },
                )
            )
        return out

    @staticmethod
    def _where_clause(
        *, profile: str | None, kind: str | None, remote_ids: list[int] | None
    ) -> dict[str, Any]:
        clauses: list[dict[str, Any]] = []
        if profile:
            clauses.append({"profile": {"$eq": profile}})
        if kind:
            clauses.append({"kind": {"$eq": kind}})
        if remote_ids:
            clauses.append({"remote_id": {"$in": [int(r) for r in remote_ids]}})
        if not clauses:
            return {}
        if len(clauses) == 1:
            return clauses[0]
        return {"$and": clauses}

    def count(self) -> int:
        """Total chunks indexed (for diagnostics / tests)."""
        try:
            return int(self.collection.count())
        except Exception:  # noqa: BLE001
            return 0

    def rerank(
        self,
        candidate_remote_ids: list[int],
        text: str,
        *,
        profile: str,
        kind: str,
        top_k: int = 20,
    ) -> list[AssetQueryHit]:
        """Order ``candidate_remote_ids`` by semantic similarity to ``text``.

        Companion of :meth:`query` used by :class:`HybridAssetSearch`:
        FTS5 produces the candidate set (keyword presence guaranteed),
        then Chroma reranks those candidates by cosine. Empty candidate
        list returns ``[]`` immediately so the caller never pays the
        Chroma round-trip.
        """
        if not candidate_remote_ids or not text or not text.strip() or top_k <= 0:
            return []
        unique_ids = list({int(r) for r in candidate_remote_ids})
        return self.query(
            text,
            top_k=int(top_k),
            profile=profile,
            kind=kind,
            remote_ids=unique_ids,
        )

    def prune_stale_vectors(self, conn: Any, profile_name: str) -> int:
        """Delete Chroma entries whose (kind, remote_id) is gone from SQLite.

        Runs at the end of every reindex pass. For each asset kind,
        gather the set of ``remote_id`` values currently alive in
        SQLite and the set indexed in Chroma; remove the difference.

        Returns the number of stale chunks deleted. Best-effort:
        Chroma / SQLite errors are logged and the method keeps going
        with the kinds it can reach.
        """
        kinds_to_tables = {
            "notebook": "remote_notebooks",
            "query": "remote_queries",
            "pipeline": "remote_pipelines",
            "job": "remote_jobs",
            "stream": "remote_streams",
            "streamlit": "remote_streamlit_apps",
        }
        removed = 0
        for kind, table in kinds_to_tables.items():
            try:
                rows = conn.execute(
                    f"SELECT id FROM {table} WHERE profile_name = ?",  # noqa: S608
                    (profile_name,),
                ).fetchall()
                live_ids = {int(r[0]) for r in rows}
            except Exception as exc:  # noqa: BLE001
                log.warning("prune_stale_vectors: SQLite read failed for %s: %s", kind, exc)
                continue
            try:
                where = {
                    "$and": [
                        {"profile": {"$eq": profile_name}},
                        {"kind": {"$eq": kind}},
                    ]
                }
                got = self.collection.get(where=where, include=["metadatas"])
            except Exception as exc:  # noqa: BLE001
                log.warning("prune_stale_vectors: Chroma get failed for %s: %s", kind, exc)
                continue
            ids = list(got.get("ids") or [])
            metas = list(got.get("metadatas") or [])
            stale_chunk_ids: list[str] = []
            for chunk_id, meta in zip(ids, metas, strict=False):
                if not isinstance(meta, dict):
                    continue
                try:
                    rid = int(meta.get("remote_id") or 0)
                except (TypeError, ValueError):
                    continue
                if rid and rid not in live_ids:
                    stale_chunk_ids.append(str(chunk_id))
            if not stale_chunk_ids:
                continue
            try:
                self.collection.delete(ids=stale_chunk_ids)
                removed += len(stale_chunk_ids)
                log.info(
                    "prune_stale_vectors: removed %d stale chunks for %s/%s",
                    len(stale_chunk_ids),
                    profile_name,
                    kind,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("prune_stale_vectors: delete failed for %s: %s", kind, exc)
        return removed


# ── PR-D: incremental-embed helpers ───────────────────────────────────────

# Per-kind config for the incremental-embed hash check. Each entry
# is ``(table, hash_expr)``: a SQL expression that yields the current
# canonical content hash for a row. notebooks + queries already store
# a hash in a dedicated column; pipelines have no native hash so we
# derive one from the content envelope at compare time. Other kinds
# (jobs / streams / streamlit_apps) are absent on purpose — their
# content is metadata-only and re-embedding them on every ingest is
# cheap.
_HASHABLE_KIND_SQL: dict[str, tuple[str, str]] = {
    "notebook": ("remote_notebooks", "source_hash"),
    "query": ("remote_queries", "sql_hash"),
    "pipeline": (
        "remote_pipelines",
        "COALESCE(libraries_json, '') || '::' || COALESCE(latest_update_state, '')",
    ),
}


def _hashable_for_pipeline(envelope: str) -> str:
    """Return a stable sha256 over the pipeline content envelope.

    Pipelines lack a per-row hash column; we compute one at compare
    time from the same envelope the loader uses. Notebook + query
    callers reuse their existing column values directly (already a
    sha256 hex), so this helper is only used for pipelines.
    """
    return hashlib.sha256(envelope.encode("utf-8")).hexdigest()


def _current_hashes_for_kinds(
    conn: Any, profile_name: str, docs: list[AssetDocument]
) -> dict[tuple[str, int], str]:
    """Resolve current content hashes for the rows behind ``docs``.

    Returns ``(kind, remote_id) -> hash`` for hashable kinds; other
    kinds are absent so the caller treats them as "always re-embed".
    """
    out: dict[tuple[str, int], str] = {}
    by_kind: dict[str, set[int]] = {}
    for d in docs:
        if d.kind not in _HASHABLE_KIND_SQL:
            continue
        by_kind.setdefault(d.kind, set()).add(int(d.remote_id))
    for kind, ids in by_kind.items():
        table, hash_expr = _HASHABLE_KIND_SQL[kind]
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"SELECT id, {hash_expr} FROM {table} "  # noqa: S608 — identifiers controlled above
            f"WHERE profile_name = ? AND id IN ({placeholders})",
            (profile_name, *ids),
        ).fetchall()
        for rid, raw in rows:
            if raw is None:
                continue
            value = str(raw)
            if kind == "pipeline":
                value = _hashable_for_pipeline(value)
            out[(kind, int(rid))] = value
    return out


def _last_embedded_hashes_for_kinds(
    conn: Any, profile_name: str, docs: list[AssetDocument]
) -> dict[tuple[str, int], str | None]:
    """Read the most-recently-stamped embed hash per hashable row."""
    out: dict[tuple[str, int], str | None] = {}
    by_kind: dict[str, set[int]] = {}
    for d in docs:
        if d.kind not in _HASHABLE_KIND_SQL:
            continue
        by_kind.setdefault(d.kind, set()).add(int(d.remote_id))
    for kind, ids in by_kind.items():
        table = _HASHABLE_KIND_SQL[kind][0]
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"SELECT id, last_embedded_hash FROM {table} "  # noqa: S608 — identifiers controlled
            f"WHERE profile_name = ? AND id IN ({placeholders})",
            (profile_name, *ids),
        ).fetchall()
        for rid, value in rows:
            out[(kind, int(rid))] = str(value) if value is not None else None
    return out


def _update_last_embedded_hashes(conn: Any, profile_name: str, docs: list[AssetDocument]) -> None:
    """Stamp ``last_embedded_hash`` for every hashable row in ``docs``.

    Called only after :meth:`AssetRAGStore.ingest_documents` succeeds,
    so a crashed embed leaves the row eligible for retry on the next
    ingest. The same set may be re-stamped on a re-run with identical
    content — that's a no-op and cheap.
    """
    current = _current_hashes_for_kinds(conn, profile_name, docs)
    if not current:
        return
    by_kind: dict[str, list[tuple[str, int]]] = {}
    for (kind, rid), value in current.items():
        by_kind.setdefault(kind, []).append((value, rid))
    for kind, pairs in by_kind.items():
        table = _HASHABLE_KIND_SQL[kind][0]
        conn.executemany(
            f"UPDATE {table} SET last_embedded_hash = ? "  # noqa: S608 — identifiers controlled
            "WHERE profile_name = ? AND id = ?",
            [(value, profile_name, rid) for value, rid in pairs],
        )
    if hasattr(conn, "commit"):
        try:
            conn.commit()
        except Exception:  # noqa: BLE001 — best-effort autocommit
            pass


def _clear_last_embedded_hashes(conn: Any, profile_name: str) -> None:
    """NULL out ``last_embedded_hash`` for every hashable row in a profile.

    Used by ``reindex_profile(force=True)`` and by the per-asset
    chunking-override PUT/DELETE so the next ingest re-embeds that
    row even if its source_hash has not changed.
    """
    for kind in _HASHABLE_KIND_SQL:
        table = _HASHABLE_KIND_SQL[kind][0]
        conn.execute(
            f"UPDATE {table} SET last_embedded_hash = NULL WHERE profile_name = ?",  # noqa: S608
            (profile_name,),
        )
    if hasattr(conn, "commit"):
        try:
            conn.commit()
        except Exception:  # noqa: BLE001
            pass


def _clear_last_embedded_hash_for_row(
    conn: Any, profile_name: str, kind: str, remote_id: int
) -> None:
    """NULL out a single row's ``last_embedded_hash``.

    Used by the per-asset chunking-override PUT / DELETE: the user
    asked for a different chunking strategy on this one asset, so
    the next ingest must re-embed it under the new strategy even
    when the source content is byte-identical.
    """
    spec = _HASHABLE_KIND_SQL.get(kind)
    if spec is None:
        return
    table = spec[0]
    conn.execute(
        f"UPDATE {table} SET last_embedded_hash = NULL "  # noqa: S608 — identifiers controlled
        "WHERE profile_name = ? AND id = ?",
        (profile_name, int(remote_id)),
    )
    if hasattr(conn, "commit"):
        try:
            conn.commit()
        except Exception:  # noqa: BLE001
            pass


__all__ = ["AssetRAGStore", "EmbeddingProviderMismatch"]
