"""Vector index for AMX search catalog entities.

Each DB profile is mapped to its own Chroma collection
(``amx_search_<sha256-prefix>``) so vectors from one profile cannot
leak into another. The previous design used a single ``amx_search``
collection filtered by a ``db_profile`` metadata field on every query;
forgetting the filter anywhere in the codebase risked cross-profile
pollution. The empty profile name still maps to the legacy
``amx_search`` collection for back-compat with tests and callers that
have not adopted the per-profile API yet.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from amx.utils.optional_deps import ensure as _ensure

if TYPE_CHECKING:
    from chromadb.api.types import EmbeddingFunction

# Catalog Search shares the ``rag`` bundle (chromadb + splitter +
# tiktoken) with /docs and /code. The bundle is fetched on first
# :class:`SearchIndex` construction, NOT at module import — Studio's
# transitive cold-start path runs through this module
# (web.routers.ask -> search.catalog -> search.index), so a module-
# level install call would block every Studio first launch on a
# fresh wheel.

from amx.rag_core.collection_identity import (
    CollectionIdentity,
    reconcile_identity,
)
from amx.utils.logging import get_logger

log = get_logger("search.index")

_LEGACY_COLLECTION_NAME = "amx_search"

# PR-B: schema version pinned at 1 because Catalog Search has never
# recorded an embedding identity before — this is the first version
# that does. v1 here is independent of docs / code RAG's v2.
_AMX_SEARCH_SCHEMA_VERSION = 1


def _resolve_search_identity(
    embedding_function: EmbeddingFunction | None,
    cfg: Any | None = None,
) -> CollectionIdentity:
    """Return the (provider, model, dim) triple to stamp on this
    process's Catalog Search collections.

    Reads from ``cfg.embedding_docs`` (search powers docs RAG). Falls
    back to MiniLM defaults so a missing config never
    blocks rebuild. The embedding function is consulted for its
    ``dim`` attribute when the static dispatch can't resolve it.
    """
    if cfg is None:
        try:
            from amx.config import AMXConfig

            cfg = AMXConfig.load()
        except Exception:
            cfg = None

    embedding = getattr(cfg, "embedding_docs", None) if cfg is not None else None
    kind = "minilm"
    model = "minilm-l6-v2"
    if embedding is not None:
        candidate_kind = (getattr(embedding, "kind", "") or "minilm").lower().strip()
        candidate_model = getattr(embedding, "model", "") or ""
        if candidate_kind not in {"", "minilm", "default", "minilm-l6-v2"}:
            if candidate_model:
                kind = candidate_kind
                model = candidate_model
            # else fall through to MiniLM (matches the embeddings
            # module's behaviour when a non-default kind has no model).
    return CollectionIdentity.from_active(kind, model, embedding_function)


def _collection_name_for(db_profile: str) -> str:
    """Stable, Chroma-valid collection name for *db_profile*.

    Profile names can contain anything (spaces, slashes, unicode),
    so we hash to keep within Chroma's allowed character set
    (``[a-zA-Z0-9._-]``, 3–63 chars). Empty profile maps to the
    legacy ``amx_search`` collection so older data is still readable
    without forcing an immediate rebuild.
    """
    if not db_profile:
        return _LEGACY_COLLECTION_NAME
    digest = hashlib.sha256(db_profile.encode("utf-8")).hexdigest()[:16]
    return f"amx_search_{digest}"


class SearchIndex:
    """Thin wrapper around per-profile Chroma collections.

    The ``embedding_function`` argument lets callers swap in a different
    embedding provider (see :mod:`amx.search.embeddings`); ``None``
    falls back to the process-wide default factory installed by the CLI
    at startup, and finally to Chroma's bundled MiniLM if no factory is
    registered (preserving the original behaviour for tests and direct
    constructors).
    """

    def __init__(
        self,
        persist_dir: str | None = None,
        *,
        embedding_function: EmbeddingFunction | None = None,
        cfg: Any | None = None,
    ) -> None:
        # First runtime touchpoint for the search RAG cluster — install
        # the ``rag`` bundle (~80 MB) and bind chromadb locally. Module
        # top is intentionally clean so Studio cold start can import
        # this module without paying the install cost.
        _ensure("rag")
        import chromadb

        self.persist_dir = persist_dir or str(Path.home() / ".amx" / "chroma_db")
        Path(self.persist_dir).mkdir(parents=True, exist_ok=True)
        self.client = chromadb.PersistentClient(path=self.persist_dir)
        if embedding_function is None:
            # No explicit override — fall back to the docs-side factory the
            # CLI installed at startup from ``cfg.embedding_docs``. The
            # search index powers docs RAG; the code RAG path resolves its
            # own factory in ``amx.codebase.code_rag``.
            from amx.search.embeddings import get_embedding_function

            embedding_function = get_embedding_function("docs")
        self.embedding_function = embedding_function
        # PR-B: resolve and record the embedding identity so /search
        # rebuild after an /embeddings swap no longer silently
        # re-embeds with a mismatched provider. Identity is per
        # process — every per-profile collection records the same
        # provider/model/dim. Mismatch raises
        # :class:`CollectionIdentityMismatch` on the next /search
        # rebuild attempt; recovery is the same `/search rebuild`
        # flow, which now matches docs RAG's /docs reindex and code
        # RAG's /code-refresh in semantics.
        self._identity = _resolve_search_identity(embedding_function, cfg=cfg)
        self._collections: dict[str, Any] = {}

    @property
    def collection(self) -> Any:
        """Legacy (empty-profile) collection, opened lazily.

        Earlier versions opened this in ``__init__``. The eager open
        meant every ``/ask`` request absorbed the chroma init cost and
        — worse — surfaced a ``CollectionIdentityMismatch`` at worker
        startup even when the user had explicitly turned both Docs
        and Code RAG off and the legacy collection was never going to
        be queried. Opening lazily defers ``reconcile_identity`` to
        the first call site that actually needs the collection, so
        non-RAG questions never touch the on-disk vector store.
        """
        return self._collection_for("")

    def _collection_for(self, db_profile: str) -> Any:
        """Get or create the Chroma collection for *db_profile*.

        Cached after first access so we do not pay Chroma's
        ``get_or_create_collection`` cost on every upsert / query.

        PR-B: every collection is stamped with the active embedding
        identity at create time and reconciled with the recorded
        identity on reopen via
        :func:`amx.rag_core.collection_identity.reconcile_identity`.
        Mismatch raises with the user-facing recovery hint
        ``/search rebuild``.
        """
        cache_key = db_profile or ""
        cached = self._collections.get(cache_key)
        if cached is not None:
            return cached
        name = _collection_name_for(db_profile)
        identity_meta = self._identity.to_metadata()
        kwargs: dict[str, Any] = {
            "name": name,
            "metadata": {
                "hnsw:space": "cosine",
                "amx_db_profile": db_profile,
                **identity_meta,
                "amx_schema_version": _AMX_SEARCH_SCHEMA_VERSION,
            },
        }
        if self.embedding_function is not None:
            kwargs["embedding_function"] = self.embedding_function
        col = self.client.get_or_create_collection(**kwargs)
        reconcile_identity(
            col,
            self._identity,
            schema_version=_AMX_SEARCH_SCHEMA_VERSION,
            recovery_hint=(
                "Run `/search rebuild` to repopulate the catalog with the active "
                "embedding provider, or revert the embedding profile to match the "
                "indexed identity."
            ),
        )
        self._collections[cache_key] = col
        return col

    def upsert_entities(self, entities: list[dict[str, Any]]) -> int:
        # Group rows by their db_profile so each collection only ever sees
        # its own data; cross-profile leaks are physically impossible.
        by_profile: dict[str, list[dict[str, Any]]] = {}
        for entity in entities:
            profile = str(entity.get("db_profile") or "")
            by_profile.setdefault(profile, []).append(entity)

        total = 0
        for profile, rows in by_profile.items():
            docs: list[str] = []
            ids: list[str] = []
            metas: list[dict[str, Any]] = []
            for entity in rows:
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
                self._collection_for(profile).upsert(ids=ids, documents=docs, metadatas=metas)
                total += len(ids)
        return total

    def delete_entity_ids(self, entity_ids: list[int]) -> None:
        """Delete by entity id without knowing the originating db_profile.

        Without the profile we cannot single out the right collection, so
        we attempt the delete on every collection we have opened in this
        process. The operation is idempotent on Chroma's side, so
        deleting an id that does not exist in a given collection is a
        no-op.
        """
        ids = [f"entity:{int(entity_id)}" for entity_id in entity_ids if entity_id is not None]
        if not ids:
            return
        for col in list(self._collections.values()):
            try:
                col.delete(ids=ids)
            except Exception:
                # One collection's failure must not block the others —
                # callers expect a best-effort delete, and the row is
                # almost certainly only in one collection anyway.
                continue

    def reset_profile(self, db_profile: str) -> None:
        """Drop the per-profile collection so the next open reseeds
        the identity metadata.

        The previous body deleted documents one id at a time but left
        the collection's identity metadata (``embedding_provider`` /
        ``embedding_model`` / ``embedding_dim``) in place. After an
        ``/embeddings`` swap that meant the user could run
        ``/search rebuild`` and STILL hit
        :class:`CollectionIdentityMismatch` on the next ``/ask`` — the
        rebuild deleted vectors but did not invalidate the stamped
        identity, so the new MiniLM/MiniLM-L6-v2 active config kept
        clashing with the old (e.g. ``sentence_transformers`` /
        ``gte-small``) recorded triple. Dropping the collection
        outright forces the next ``_collection_for`` call to
        ``get_or_create_collection`` with the active identity.
        """
        name = _collection_name_for(db_profile)
        # Drop the in-process cache first so the next access creates
        # a fresh collection handle bound to the new on-disk state.
        self._collections.pop(db_profile or "", None)
        try:
            self.client.delete_collection(name=name)
        except Exception as exc:
            # The collection may not exist yet (first ``rebuild`` on a
            # fresh install) — that is fine. We log at debug instead
            # of swallowing silently so a genuine Chroma error during
            # rebuild is still investigable from the log.
            log.debug("reset_profile: delete_collection(%s) skipped: %s", name, exc)

    def query(
        self,
        question: str,
        *,
        db_profile: str | Sequence[str],
        n_results: int = 8,
    ) -> list[dict[str, Any]]:
        """Vector-similarity query.

        0.11.0 ``db_profile`` accepts either a single profile name (the
        original signature) or a sequence of profile names — used by
        ``/ask`` retrieval when the active scope spans multiple DB
        profiles. Multi-profile queries hit each collection in turn
        and the union of hits is returned, sorted by distance ascending
        so the consumer's existing "smaller distance = better" ranking
        still works. Each hit carries the originating profile in
        ``metadata['db_profile']`` so callers can render a Profile
        column or build cross-profile join candidates.
        """
        # Normalise to a list so we can treat single + multi uniformly.
        if isinstance(db_profile, str):
            profiles = [db_profile]
        else:
            seen: set[str] = set()
            profiles = []
            for raw in db_profile:
                name = (raw or "").strip()
                if name in seen:
                    continue
                seen.add(name)
                profiles.append(name)
            if not profiles:
                # Empty filter → no hits. The legacy "" fallback to the
                # legacy collection still works because the empty list
                # is distinct from "single empty string".
                return []

        n_per = max(1, int(n_results))
        all_hits: list[dict[str, Any]] = []
        for profile in profiles:
            col = self._collection_for(profile)
            res = col.query(
                query_texts=[question],
                n_results=n_per,
                # No ``where`` clause needed — the collection is
                # profile-scoped, so cross-profile pollution is
                # impossible regardless of caller discipline.
            )
            docs = (res.get("documents") or [[]])[0]
            metas = (res.get("metadatas") or [[]])[0]
            distances = (res.get("distances") or [[]])[0]
            for idx, doc in enumerate(docs):
                meta = dict(metas[idx]) if idx < len(metas) and metas[idx] else {}
                # Stamp the originating profile so the agent can group /
                # filter rows by db_profile downstream.
                meta.setdefault("db_profile", profile)
                dist = distances[idx] if idx < len(distances) else None
                all_hits.append({"text": doc, "metadata": meta, "distance": dist})

        if len(profiles) == 1:
            return all_hits

        # Multi-profile: rank by distance ascending then trim to the
        # requested top-N. Hits with no distance sink to the end.
        all_hits.sort(
            key=lambda h: h.get("distance") if h.get("distance") is not None else float("inf")
        )
        return all_hits[:n_per]
