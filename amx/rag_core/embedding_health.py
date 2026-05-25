"""Shared embedding health + rebuild logic for every surface.

AMX embeds text in three subsystems — docs (which also powers catalog
search), code, and ingested assets — each with its own Chroma collection
and ``embedding_{side}`` config. This module is the single, transport-free
authority for two questions the Studio panel, the cross-page CTAs, and the
``/embeddings`` CLI all need to answer the same way:

* **What is each store's health?** ``collection_status`` reports the
  configured-vs-running model, whether a silent fallback happened, the
  persisted vector identity, and the single ``needs_rebuild`` verdict.
* **How do I rebuild a store?** ``rebuild_side`` / ``rebuild_all`` clear
  the collections so the next ingest/query re-embeds under the active
  provider.

Kept free of FastAPI so the CLI can import it without dragging the web
stack (and chromadb) onto the boot path — callers lazy-import the heavy
backends inside the functions. The web router wraps
:class:`EmbeddingBackendUnavailable` into an HTTP error; the CLI prints it.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

#: Every embeddable side, in display order. docs first because it powers
#: catalog search and is the one users notice degrading first.
EMBEDDING_SIDES: tuple[str, ...] = ("docs", "code", "assets")

#: Chroma collection-name prefix per side.
_COLLECTION_PREFIX = {"docs": "amx_search", "assets": "amx_assets", "code": "amx_code"}


class EmbeddingBackendUnavailable(Exception):
    """A side's RAG backend could not be reached to rebuild it.

    ``status_code`` mirrors the HTTP status the web router should map to
    (500 when the backend import/op failed, 503 when a prerequisite like
    ``/sync`` has not run yet) so the API contract is preserved after the
    logic moved out of the router.
    """

    def __init__(self, message: str, *, status_code: int = 500) -> None:
        super().__init__(message)
        self.status_code = status_code


def _chroma_persist_path() -> str:
    return str(Path.home() / ".amx" / "chroma_db")


def collection_status(side: str, cfg: Any) -> dict[str, Any]:
    """Inspect a side's persisted collections and report its health.

    Returns the per-collection ``provider``/``model``/``count`` plus the
    configured-vs-running identity, the ``fell_back`` signal, and a single
    ``needs_rebuild`` verdict (stale vectors OR a silent fallback). An
    empty collection (count 0) is never "stale" — flagging it produces a
    false alarm from leftover/legacy shells.
    """
    try:
        import chromadb
    except Exception:
        return {"collections": [], "stale": False, "error": "chromadb not installed"}

    try:
        client = chromadb.PersistentClient(path=_chroma_persist_path())
    except Exception as exc:
        return {"collections": [], "stale": False, "error": f"{exc.__class__.__name__}: {exc}"}

    from amx.rag_core.embedding_resolver import resolve_side

    prefix = _COLLECTION_PREFIX.get(side, "amx_search")
    resolved = resolve_side(side, cfg)
    active_provider = resolved.active_provider
    active_model = resolved.active_model

    collections: list[dict[str, Any]] = []
    stale = False
    for coll in client.list_collections():
        name = getattr(coll, "name", "")
        if not name.startswith(prefix):
            continue
        try:
            meta = dict(coll.metadata or {})
        except Exception:
            meta = {}
        recorded_provider = str(meta.get("embedding_provider") or "")
        recorded_model = str(meta.get("embedding_model") or "")
        try:
            count = int(coll.count())
        except Exception:
            count = 0
        is_stale = bool(
            recorded_provider
            and recorded_model
            and count > 0
            and (recorded_provider != active_provider or recorded_model != active_model)
        )
        if is_stale:
            stale = True
        collections.append(
            {
                "name": name,
                "count": count,
                "embedding_provider": recorded_provider,
                "embedding_model": recorded_model,
                "stale": is_stale,
            }
        )
    return {
        "collections": collections,
        "stale": stale,
        "current_provider": active_provider,
        "current_model": active_model,
        "configured_provider": resolved.configured_provider,
        "configured_model": resolved.configured_model,
        "fell_back": resolved.fell_back,
        "fallback_reason": resolved.fallback_reason,
        "dependency_available": resolved.dependency_available,
        "needs_rebuild": bool(stale or resolved.fell_back),
    }


def all_status(cfg: Any) -> dict[str, dict[str, Any]]:
    """Health for every side, keyed by side name."""
    return {side: collection_status(side, cfg) for side in EMBEDDING_SIDES}


def rebuild_side(side: str, cfg: Any, profile_filter: str | None = None) -> dict[str, Any]:
    """Clear one side's vector collections so the next query/ingest
    re-embeds under the active provider.

    Raises :class:`EmbeddingBackendUnavailable` when the side's RAG
    backend is missing or not yet initialised. A rebuild re-embeds under
    the *active* provider; if the side has silently fallen back it
    re-embeds under the fallback — it does not restore the configured
    model. Fix the dependency/config first (the ``fell_back`` flag from
    :func:`collection_status` is the signal).
    """
    if side == "docs":
        try:
            from amx.search.catalog import SearchCatalog
        except Exception as exc:
            raise EmbeddingBackendUnavailable(
                f"Docs catalog unavailable: {exc.__class__.__name__}: {exc}"
            ) from exc
        catalog = SearchCatalog.from_history_store()
        if catalog is None:
            raise EmbeddingBackendUnavailable(
                "Docs catalog is not initialised; run /sync first.", status_code=503
            )
        rebuilt: list[str] = []
        db_profiles = getattr(cfg, "db_profiles", {}) or {}
        targets = [profile_filter] if profile_filter else sorted(db_profiles.keys()) or [""]
        for db_profile in targets:
            try:
                catalog.rebuild_profile(db_profile or "")
                rebuilt.append(db_profile or "(default)")
            except Exception as exc:
                log.warning("rebuild_profile failed for %r: %s", db_profile, exc)
        return {
            "ok": True,
            "side": "docs",
            "rebuilt": rebuilt,
            "message": f"Rebuilt {len(rebuilt)} docs collection(s).",
        }

    if side == "assets":
        try:
            from amx.assets.rag import AssetRAGStore
        except Exception as exc:
            raise EmbeddingBackendUnavailable(
                f"Asset RAG unavailable: {exc.__class__.__name__}: {exc}"
            ) from exc
        try:
            store = AssetRAGStore(cfg=cfg)
            store.reset_collection()
        except Exception as exc:
            raise EmbeddingBackendUnavailable(
                f"Asset collection reset failed: {exc.__class__.__name__}: {exc}"
            ) from exc
        return {
            "ok": True,
            "side": "assets",
            "cleared": True,
            "message": (
                "Cleared the assets collection; run /db assets reindex to "
                "re-embed under the active provider."
            ),
        }

    # side == "code"
    try:
        from amx.codebase.code_rag import delete_code_collection
    except Exception as exc:
        raise EmbeddingBackendUnavailable(
            f"Code RAG unavailable: {exc.__class__.__name__}: {exc}"
        ) from exc
    cleared = bool(
        delete_code_collection(source_filters=[profile_filter] if profile_filter else None)
    )
    return {
        "ok": True,
        "side": "code",
        "cleared": cleared,
        "message": (
            "Cleared the code collection; the next /code query will re-embed "
            "with the active provider."
            if cleared
            else "Code collection did not exist; nothing to clear."
        ),
    }


def rebuild_all(cfg: Any, profile_filter: str | None = None) -> dict[str, Any]:
    """Rebuild every side, recording per-side failures without aborting.

    ``ok`` is True only when every side cleared. A side whose backend is
    unavailable lands in ``failed`` with its error captured in
    ``results`` so the caller can report a partial outcome.
    """
    results: list[dict[str, Any]] = []
    failed: list[str] = []
    for side in EMBEDDING_SIDES:
        try:
            results.append(rebuild_side(side, cfg, profile_filter))
        except EmbeddingBackendUnavailable as exc:
            failed.append(side)
            results.append({"ok": False, "side": side, "error": str(exc)})
    return {
        "ok": not failed,
        "results": results,
        "failed": failed,
        "message": (
            f"Rebuilt {len(EMBEDDING_SIDES) - len(failed)}/{len(EMBEDDING_SIDES)} sides."
            if failed
            else "Rebuilt every RAG store; re-ingest/query to re-embed under the active provider."
        ),
    }
