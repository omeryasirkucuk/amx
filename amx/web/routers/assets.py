"""FastAPI router for /api/assets — list, detail, ingest (with SSE progress)."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from amx.config import AMXConfig
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/assets", tags=["assets"])

ASSET_KINDS = {
    "notebook": ("remote_notebooks", "name"),
    "job": ("remote_jobs", "name"),
    "pipeline": ("remote_pipelines", "name"),
    "streamlit": ("remote_streamlit_apps", "qualified_name"),
    "stream": ("remote_streams", "qualified_name"),
    "query": ("remote_queries", "name"),
}

# PR-C (scale): the additional column the substring search hits beyond
# the primary name column. None means "no second search axis for this
# kind" — query rows have no natural path, jobs are name-only.
_PATH_SEARCH_COL = {
    "notebook": "COALESCE(workspace_path, qualified_name, '')",
    "job": None,
    "pipeline": "COALESCE(target_schema, '')",
    "streamlit": None,  # qualified_name IS the name
    "stream": None,
    "query": None,
}


class _JobChannel:
    """Per-job event log + waiter for SSE consumers.

    Why a log (not just a queue): corporate proxies sometimes kill an
    idle HTTP connection mid-job. The browser reconnects with
    ``Last-Event-ID``; we replay anything the client missed straight
    from ``events`` instead of dropping the user back to "unknown
    state". ``_eof`` is the in-band marker that the producer is done;
    once observed, no further publishes are expected.

    Studio is single-tenant per process so concurrent consumers for the
    same job are rare; the design still handles them — each consumer
    reads from its own cursor.
    """

    __slots__ = ("events", "waiter", "closed")

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        # A fresh Event per publish so consumers can hold a reference to
        # the *old* event across the await boundary without missing the
        # next signal. ``asyncio.Event`` must be created inside a
        # running loop — callers instantiate ``_JobChannel`` from the
        # async request handler, which guarantees that.
        self.waiter: asyncio.Event = asyncio.Event()
        self.closed = False

    def publish(self, evt: dict[str, Any]) -> None:
        self.events.append(evt)
        if evt.get("_eof"):
            self.closed = True
        old = self.waiter
        self.waiter = asyncio.Event()
        old.set()


# In-process registry of ingest job_id → channel. Studio is
# single-tenant per process, so a module-level dict is fine.
_INGEST_JOBS: dict[str, _JobChannel] = {}


def _history_db_path(cfg: AMXConfig) -> Path:
    """Resolve the local history SQLite path, matching the CLI helper."""
    config_dir = getattr(cfg, "CONFIG_DIR", None) or str(Path.home() / ".amx")
    return Path(config_dir) / "history.db"


@router.get("")
def list_assets(
    profile: str = Query(...),
    type: str = Query(..., alias="type"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    q: str = Query(default="", description="Case-insensitive substring filter on name + path."),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return a paginated slice of ingested assets for a DB profile.

    PR-C (scale): the endpoint used to ``SELECT *`` without bounds —
    a 5,000-notebook profile sent 5,000 rows on first paint. Studio
    now requests pages (default 100, max 500) plus an optional
    substring filter ``q`` against both the name column and the
    kind's natural path column. The response carries
    ``{items, count, total, has_more, offset, limit}`` so the table
    can render a "Showing 100 of 5,000" footer + Next/Prev controls.
    Legacy callers (no limit/offset/q) get the first 100 rows; the
    ``count`` field still reflects ``len(items)`` for backwards
    compatibility.
    """
    if type not in ASSET_KINDS:
        raise HTTPException(400, f"Unknown asset type: {type!r}. Valid: {', '.join(ASSET_KINDS)}")
    table, name_col = ASSET_KINDS[type]
    path_expr = _PATH_SEARCH_COL.get(type)
    needle = q.strip()
    params: list[Any] = [profile]
    where = "profile_name = ?"
    if needle:
        like = f"%{needle.lower()}%"
        if path_expr:
            where += f" AND (LOWER({name_col}) LIKE ? OR LOWER({path_expr}) LIKE ?)"
            params.extend([like, like])
        else:
            where += f" AND LOWER({name_col}) LIKE ?"
            params.append(like)
    with sqlite3.connect(_history_db_path(cfg)) as conn:
        conn.row_factory = sqlite3.Row
        total = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {where}",  # noqa: S608 — identifiers controlled above
            params,
        ).fetchone()[0]
        page_params = [*params, int(limit), int(offset)]
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE {where} ORDER BY {name_col}, id LIMIT ? OFFSET ?",  # noqa: S608 — identifiers controlled above
            page_params,
        ).fetchall()
        items = [dict(r) for r in rows]
    return {
        "items": items,
        "count": len(items),
        "total": int(total),
        "has_more": (int(offset) + len(items)) < int(total),
        "offset": int(offset),
        "limit": int(limit),
    }


@router.get("/search")
def search_assets(
    q: str = Query(..., min_length=1, description="Free-form natural-language query."),
    profile: str = Query(..., description="DB profile to search within."),
    kind: str = Query(..., description="Asset kind to search within (tab-scoped)."),
    limit: int = Query(default=20, ge=1, le=100),
    mode: str = Query(
        default="keyword_strict",
        description=(
            "Search mode: keyword_strict (default, FTS5 + semantic "
            "rerank), semantic_only (pure embedding), or auto "
            "(keyword first, fall back to semantic when zero hits)."
        ),
    ),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Hybrid keyword-first search over ingested remote assets.

    The Studio Assets page always passes the currently-selected tab
    as ``kind`` so search is tab-scoped end to end. The default
    ``keyword_strict`` mode runs an FTS5 candidate query on
    ``fts_<kind>`` and reranks the surviving ``remote_id`` set by
    cosine similarity through :class:`AssetRAGStore`. Hits are
    guaranteed to contain the search tokens — no more "result text
    does not contain the search term" surprises.

    Set ``mode=semantic_only`` to bypass FTS5 (synonym recall), or
    ``mode=auto`` to fall back to semantic when keyword returns zero.

    When ``AssetRAGStore`` is unavailable (no ingest yet, or a
    one-time :class:`EmbeddingProviderMismatch` after switching
    embedding models), the endpoint falls back to a no-embed
    keyword-only result built straight from FTS5 so the UI never
    shows an empty list for an obviously-matching query.
    """
    if kind not in ASSET_KINDS:
        raise HTTPException(400, f"Unknown asset kind: {kind!r}. Valid: {', '.join(ASSET_KINDS)}")
    if mode not in ("keyword_strict", "semantic_only", "auto"):
        raise HTTPException(
            400, f"Unknown search mode: {mode!r}. Valid: keyword_strict, semantic_only, auto"
        )

    db_path = _history_db_path(cfg)
    rag_available = True
    rag_reason = ""
    store = None
    try:
        from amx.assets.rag import AssetRAGStore

        store = AssetRAGStore(cfg=cfg)
    except Exception as exc:  # noqa: BLE001
        rag_available = False
        rag_reason = str(exc)

    try:
        from amx.assets.search import HybridAssetSearch
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, f"Hybrid search unavailable: {exc}") from exc

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if store is None or not rag_available:
            # Fall back to keyword-only: FTS5 candidates with no
            # rerank. Builds minimal hits for the UI so the user
            # still sees keyword matches before any embed pass.
            search = HybridAssetSearch(conn, _NullRAGStore(conn))  # type: ignore[arg-type]
            hits = search.search(
                q,
                kind=kind,
                profile=profile,
                limit=int(limit),
                mode="keyword_strict",
            )
        else:
            search = HybridAssetSearch(conn, store)
            hits = search.search(
                q,
                kind=kind,
                profile=profile,
                limit=int(limit),
                mode=mode,  # type: ignore[arg-type]
            )

    items = [
        {
            "chunk_id": hit.chunk_id,
            "kind": hit.kind,
            "profile": hit.profile,
            "remote_id": hit.remote_id,
            "name": hit.name,
            "path": _hit_path(hit),
            "score": hit.score,
            "matched_text": hit.text,
            "match_type": hit.metadata.get("match_type", "keyword_strict"),
            "metadata": hit.metadata,
        }
        for hit in hits
    ]
    payload: dict[str, Any] = {
        "items": items,
        "rag_available": rag_available,
        "count": len(items),
        "mode": mode,
        "kind": kind,
    }
    if rag_reason:
        payload["reason"] = rag_reason
    return payload


class _NullRAGStore:
    """Stand-in for :class:`AssetRAGStore` when embedding is unavailable.

    Only :meth:`rerank` and :meth:`query` are wired — both return ``[]``
    so :class:`HybridAssetSearch` falls back to its built-in FTS-only
    hit builder.
    """

    def __init__(self, conn: Any) -> None:  # noqa: D401 — narrow shim
        self.conn = conn

    def rerank(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    def query(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []


def _hit_path(hit: Any) -> str:
    """Return the disambiguating path for a semantic-search hit.

    Different kinds expose the path under different metadata keys, so
    normalize them into a single ``path`` surface for the Studio rows:

    * notebook → ``workspace_path`` (Databricks) or
      ``qualified_name`` (Snowflake)
    * stream / streamlit → ``qualified_name`` IS the identity, but we
      expose it under ``path`` for rendering uniformity
    * pipeline → ``target_schema`` (logical, not a path, but the
      natural same-name disambiguator)
    * query / job → no natural path; empty string

    Returns ``""`` when no candidate metadata key is set so the UI can
    render ``name`` alone for kinds that don't carry a path.
    """
    md = getattr(hit, "metadata", None) or {}
    for key in ("workspace_path", "qualified_name", "target_schema"):
        value = md.get(key)
        if value:
            return str(value)
    return ""


_SSE_HEARTBEAT_SECONDS = 15.0


@router.get("/ingest/{job_id}/events")
async def ingest_events(request: Request, job_id: str) -> StreamingResponse:
    """Stream SSE progress events for a running ingest job.

    Emits one heartbeat comment per ~15s of silence so corporate
    proxies (which kill idle HTTP connections after 30–60s) keep the
    pipe open. Honors ``Last-Event-ID`` for resume after a network
    drop — the client picks up exactly where it left off.
    """
    channel = _INGEST_JOBS.get(job_id)
    if not channel:
        raise HTTPException(404, "Unknown job_id")

    last_event_id = request.headers.get("last-event-id", "")
    try:
        start_idx = max(int(last_event_id), 0)
    except ValueError:
        start_idx = 0

    async def gen():
        idx = start_idx
        while True:
            # Drain everything the client has not yet seen.
            while idx < len(channel.events):
                evt = channel.events[idx]
                event_id = idx + 1  # 1-based id sent on the wire
                idx += 1
                if evt.get("_eof"):
                    yield f"id: {event_id}\nevent: end\ndata: {{}}\n\n"
                    _INGEST_JOBS.pop(job_id, None)
                    return
                yield f"id: {event_id}\ndata: {json.dumps(evt)}\n\n"
            if channel.closed:
                _INGEST_JOBS.pop(job_id, None)
                return
            waiter = channel.waiter
            try:
                await asyncio.wait_for(waiter.wait(), timeout=_SSE_HEARTBEAT_SECONDS)
            except asyncio.TimeoutError:
                # Comment frame: valid SSE no-op, browsers ignore the
                # payload, but the bytes travelling across the wire
                # reset corporate-proxy idle timers.
                yield ": keepalive\n\n"

    headers = {
        # ``no-transform`` blocks chunk-reassembling intermediaries
        # from buffering the stream; ``X-Accel-Buffering: no`` does the
        # same for nginx specifically.
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)


_CHUNKING_KIND_NORM = {
    "notebook": "notebook",
    "query": "query",
    "pipeline": "pipeline",
    # ``streamlit`` and ``stream`` / ``job`` are metadata-only — no
    # override is meaningful, so the endpoints return 400 for them.
}


class ChunkingOverrideIn(BaseModel):
    strategy: str
    chunk_chars: int | None = None
    chunk_overlap: int | None = None


# ── PR-E: Lazy discover tree endpoints ──────────────────────────────────────
# Registered BEFORE ``/{kind}/{asset_id}`` so the literal path segments
# ``discover/tree`` aren't mis-matched as ``kind=discover, asset_id=tree``
# (which would 422 on the int parse of ``"tree"``).


@router.get("/discover/tree")
def discover_tree(
    profile: str = Query(...),
    kind: str = Query(default="notebook"),
    parent: str = Query(default=""),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return immediate children of ``parent`` for the Studio tree picker."""
    from amx.assets.discover_cache import read_children, refresh_parent

    db_path = _history_db_path(cfg)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cached, parent_fetched_at = read_children(
            conn, profile=profile, kind=kind, parent_path=parent
        )
        if parent_fetched_at is not None:
            return {
                "items": cached,
                "parent_path": parent,
                "parent_fetched_at": parent_fetched_at,
                "cache_empty": False,
            }
        from amx.cli_support.commands.db_assets_impl import _open_connector

        try:
            connector = _open_connector(cfg, profile)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(502, f"Could not open connector: {exc}") from exc
        try:
            entries = list(connector.list_workspace_children(parent_path=parent, kind=kind))
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                502, f"Adapter listing failed for parent={parent!r}: {exc}"
            ) from exc
        refresh_parent(conn, profile=profile, kind=kind, parent_path=parent, entries=entries)
        rows, parent_fetched_at = read_children(
            conn, profile=profile, kind=kind, parent_path=parent
        )
    return {
        "items": rows,
        "parent_path": parent,
        "parent_fetched_at": parent_fetched_at,
        "cache_empty": False,
    }


@router.post("/discover/tree/refresh")
def refresh_tree(
    profile: str = Query(...),
    kind: str = Query(default="notebook"),
    parent: str = Query(default=""),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Force-refresh ``parent``'s immediate children (atomic replace)."""
    from amx.assets.discover_cache import read_children, refresh_parent
    from amx.cli_support.commands.db_assets_impl import _open_connector

    db_path = _history_db_path(cfg)
    try:
        connector = _open_connector(cfg, profile)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"Could not open connector: {exc}") from exc
    try:
        entries = list(connector.list_workspace_children(parent_path=parent, kind=kind))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"Adapter listing failed for parent={parent!r}: {exc}") from exc
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        written = refresh_parent(
            conn, profile=profile, kind=kind, parent_path=parent, entries=entries
        )
        rows, parent_fetched_at = read_children(
            conn, profile=profile, kind=kind, parent_path=parent
        )
    return {
        "items": rows,
        "parent_path": parent,
        "parent_fetched_at": parent_fetched_at,
        "cache_empty": False,
        "written": written,
    }


@router.post("/discover/tree/walk")
def walk_tree(
    profile: str = Query(...),
    kind: str = Query(default="notebook"),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Full recursive walk that seeds the entire cache."""
    from amx.assets.discover_cache import walk_full
    from amx.cli_support.commands.db_assets_impl import _open_connector

    db_path = _history_db_path(cfg)
    try:
        connector = _open_connector(cfg, profile)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"Could not open connector: {exc}") from exc
    fetch_method_name = {
        "notebook": "list_remote_notebooks_metadata",
        "job": "list_remote_jobs_metadata",
        "pipeline": "list_remote_pipelines_metadata",
    }.get(kind)
    if fetch_method_name is None:
        raise HTTPException(400, f"Walk not supported for kind={kind!r}.")
    fetcher = getattr(connector, fetch_method_name, None)
    if fetcher is None:
        raise HTTPException(501, f"Adapter has no {fetch_method_name} for profile={profile!r}.")
    try:
        leaves = list(fetcher())
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"Walk failed: {exc}") from exc
    with sqlite3.connect(db_path) as conn:
        counts = walk_full(conn, profile=profile, kind=kind, leaves=leaves)
    return counts


@router.get("/{kind}/{asset_id}/chunking")
def get_asset_chunking(
    kind: str,
    asset_id: int,
    profile: str = Query(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return the effective chunking config for a single asset.

    Merges the global ``cfg.assets_chunking`` default with the
    per-asset override (if any) so the Studio modal can render the
    current values without two round-trips.
    """
    normalized = _CHUNKING_KIND_NORM.get(kind)
    if normalized is None:
        raise HTTPException(
            400,
            f"Chunking is configurable only for {sorted(_CHUNKING_KIND_NORM)}. Got {kind!r}.",
        )
    from amx.assets.chunking_overrides import get_override
    from amx.storage.sqlite_store import history_store

    hs = history_store()
    if hs is None:
        raise HTTPException(503, "History store unavailable.")
    override = get_override(
        history=hs, profile_name=profile, kind=normalized, remote_id=int(asset_id)
    )
    defaults = _kind_defaults(cfg, normalized)
    if override is None:
        return {
            "kind": normalized,
            "profile": profile,
            "remote_id": int(asset_id),
            "has_override": False,
            "effective": defaults,
            "default": defaults,
        }
    effective = {**defaults}
    effective["strategy"] = override.strategy
    if override.chunk_chars is not None:
        effective["chunk_chars"] = override.chunk_chars
    if override.chunk_overlap is not None:
        effective["chunk_overlap"] = override.chunk_overlap
    return {
        "kind": normalized,
        "profile": profile,
        "remote_id": int(asset_id),
        "has_override": True,
        "effective": effective,
        "default": defaults,
        "override": {
            "strategy": override.strategy,
            "chunk_chars": override.chunk_chars,
            "chunk_overlap": override.chunk_overlap,
            "updated_at": override.updated_at,
        },
    }


@router.put("/{kind}/{asset_id}/chunking")
def set_asset_chunking(
    kind: str,
    asset_id: int,
    body: ChunkingOverrideIn,
    profile: str = Query(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Set the per-asset chunking override and re-embed just this asset.

    Studio's "Chunk" button calls this; the endpoint validates the
    strategy against the asset kind, writes the row, drops the
    asset's existing chunks from Chroma, and re-ingests under the new
    strategy. Best-effort: a missing chromadb / collection identity
    mismatch still persists the row so the next ingest picks it up.
    """
    normalized = _CHUNKING_KIND_NORM.get(kind)
    if normalized is None:
        raise HTTPException(
            400,
            f"Chunking is configurable only for {sorted(_CHUNKING_KIND_NORM)}. Got {kind!r}.",
        )
    from amx.assets.chunking_overrides import (
        ChunkingOverrideValidationError,
        set_override,
    )
    from amx.storage.sqlite_store import history_store

    hs = history_store()
    if hs is None:
        raise HTTPException(503, "History store unavailable.")
    try:
        override = set_override(
            history=hs,
            profile_name=profile,
            kind=normalized,
            remote_id=int(asset_id),
            strategy=body.strategy,
            chunk_chars=body.chunk_chars,
            chunk_overlap=body.chunk_overlap,
        )
    except ChunkingOverrideValidationError as exc:
        raise HTTPException(400, str(exc)) from exc

    reindexed = _reindex_single_asset(cfg, hs, normalized, profile, int(asset_id))
    return {
        "ok": True,
        "kind": normalized,
        "profile": profile,
        "remote_id": int(asset_id),
        "override": {
            "strategy": override.strategy,
            "chunk_chars": override.chunk_chars,
            "chunk_overlap": override.chunk_overlap,
            "updated_at": override.updated_at,
        },
        "reindexed": reindexed,
    }


@router.delete("/{kind}/{asset_id}/chunking")
def clear_asset_chunking(
    kind: str,
    asset_id: int,
    profile: str = Query(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Clear the per-asset override and re-embed under the global default."""
    normalized = _CHUNKING_KIND_NORM.get(kind)
    if normalized is None:
        raise HTTPException(
            400,
            f"Chunking is configurable only for {sorted(_CHUNKING_KIND_NORM)}. Got {kind!r}.",
        )
    from amx.assets.chunking_overrides import clear_override
    from amx.storage.sqlite_store import history_store

    hs = history_store()
    if hs is None:
        raise HTTPException(503, "History store unavailable.")
    cleared = clear_override(
        history=hs, profile_name=profile, kind=normalized, remote_id=int(asset_id)
    )
    reindexed = _reindex_single_asset(cfg, hs, normalized, profile, int(asset_id))
    return {
        "ok": True,
        "kind": normalized,
        "profile": profile,
        "remote_id": int(asset_id),
        "cleared": cleared,
        "reindexed": reindexed,
    }


def _kind_defaults(cfg: AMXConfig, kind: str) -> dict[str, Any]:
    """Return the global chunking defaults for a kind."""
    ac = getattr(cfg, "assets_chunking", None)
    if ac is None:
        from amx.assets.chunking_config import AssetChunkingConfig

        ac = AssetChunkingConfig()
    if kind == "notebook":
        return {
            "strategy": ac.notebook.strategy,
            "chunk_chars": ac.notebook.chunk_chars,
            "chunk_overlap": ac.notebook.chunk_overlap,
        }
    if kind == "query":
        return {
            "strategy": ac.query.strategy,
            "chunk_chars": ac.query.chunk_chars,
            "chunk_overlap": ac.query.chunk_overlap,
        }
    if kind == "pipeline":
        return {"strategy": ac.pipeline.strategy}
    return {}


def _reindex_single_asset(cfg: AMXConfig, hs: Any, kind: str, profile: str, remote_id: int) -> bool:
    """Drop + re-embed just one asset's chunks. Best-effort.

    PR-D: the chunking-override PUT/DELETE that calls this needs the
    next ingest to re-embed the row even when ``source_hash`` is
    unchanged (the strategy changed, not the content). Clear
    ``last_embedded_hash`` for the row before re-embedding so the
    incremental gate inside ``ingest_profile`` doesn't skip it.
    """
    try:
        from amx.assets.rag import AssetRAGStore, _clear_last_embedded_hash_for_row
    except Exception:  # noqa: BLE001
        return False
    try:
        store = AssetRAGStore(cfg=cfg)
    except Exception:  # noqa: BLE001
        return False
    try:
        store.delete_asset(kind=kind, profile=profile, remote_id=remote_id)
        with hs._connect() as conn:  # noqa: SLF001
            _clear_last_embedded_hash_for_row(conn, profile, kind, remote_id)
            store.ingest_profile(
                conn=conn,
                profile_name=profile,
                kinds=[kind],
                only_ids={kind: [remote_id]},
            )
    except Exception:  # noqa: BLE001
        return False
    return True


@router.get("/{kind}/{asset_id}/lineage")
def get_asset_lineage(
    kind: str,
    asset_id: int,
    profile: str = Query(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return asset-to-asset lineage edges anchored at one asset.

    Materialised by ``amx.assets.lineage.LineageExtractor`` and stored
    in :data:`asset_lineage_edges`. The response carries three lists:

    * ``outgoing`` — edges this asset sources (job → notebook /
      pipeline / query, pipeline → notebook / target table)
    * ``incoming`` — edges that target this asset (notebook used by
      job X, table written by pipeline Y)
    * ``task_dag`` — task-to-task ``depends_on`` edges within the
      same job, always empty for non-job kinds

    Each edge entry includes ``to_kind``, ``to_id``, ``to_name``
    (resolved against the appropriate ``remote_*`` / ``catalog_entities``
    row), ``edge_type`` and the raw platform reference. The Studio
    Lineage panel renders ``outgoing`` as clickable asset chips and
    ``task_dag`` as an adjacency list.
    """
    if kind not in ASSET_KINDS:
        raise HTTPException(400, f"Unknown asset kind: {kind!r}. Valid: {', '.join(ASSET_KINDS)}")
    table, _name_col = ASSET_KINDS[kind]
    db_path = _history_db_path(cfg)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        anchor = conn.execute(
            f"SELECT id FROM {table} WHERE id = ? AND profile_name = ?",  # noqa: S608
            (int(asset_id), profile),
        ).fetchone()
        if anchor is None:
            raise HTTPException(404, "Asset not found")

        outgoing_rows = conn.execute(
            """
            SELECT id, to_kind, to_id, edge_type, raw_ref, discovered_at
            FROM asset_lineage_edges
            WHERE profile_name = ?
              AND from_kind = ?
              AND from_id = ?
              AND edge_type != 'task_depends_on'
            ORDER BY edge_type, to_kind, to_id
            """,
            (profile, kind, int(asset_id)),
        ).fetchall()
        incoming_rows = conn.execute(
            """
            SELECT id, from_kind, from_id, edge_type, raw_ref, discovered_at
            FROM asset_lineage_edges
            WHERE profile_name = ?
              AND to_kind = ?
              AND to_id = ?
              AND edge_type != 'task_depends_on'
            ORDER BY edge_type, from_kind, from_id
            """,
            (profile, kind, int(asset_id)),
        ).fetchall()
        dag_rows: list[sqlite3.Row] = []
        if kind == "job":
            dag_rows = conn.execute(
                """
                SELECT raw_ref
                FROM asset_lineage_edges
                WHERE profile_name = ?
                  AND from_kind = 'job'
                  AND from_id = ?
                  AND edge_type = 'task_depends_on'
                """,
                (profile, int(asset_id)),
            ).fetchall()

        outgoing = [
            {
                **_describe_edge_endpoint(conn, r["to_kind"], int(r["to_id"]), profile),
                "to_kind": r["to_kind"],
                "to_id": int(r["to_id"]),
                "edge_type": r["edge_type"],
                "raw_ref": _decode_raw_ref(r["raw_ref"]),
            }
            for r in outgoing_rows
        ]
        incoming = [
            {
                **_describe_edge_endpoint(conn, r["from_kind"], int(r["from_id"]), profile),
                "from_kind": r["from_kind"],
                "from_id": int(r["from_id"]),
                "edge_type": r["edge_type"],
                "raw_ref": _decode_raw_ref(r["raw_ref"]),
            }
            for r in incoming_rows
        ]
        task_dag = [
            entry
            for r in dag_rows
            if (entry := _decode_raw_ref(r["raw_ref"])) is not None
            and isinstance(entry, dict)
            and "from_task" in entry
            and "to_task" in entry
        ]
    return {
        "kind": kind,
        "id": int(asset_id),
        "profile": profile,
        "outgoing": outgoing,
        "incoming": incoming,
        "task_dag": task_dag,
    }


def _describe_edge_endpoint(
    conn: sqlite3.Connection, endpoint_kind: str, endpoint_id: int, profile: str
) -> dict[str, Any]:
    """Resolve a display name + optional path for a lineage endpoint.

    Falls back to a placeholder when the endpoint row is gone (e.g.
    asset was deleted after the edge was materialised). Lineage is
    rewritten on every refresh, so stale endpoints are rare but
    possible during a partial refresh.
    """
    if endpoint_kind == "table":
        row = conn.execute(
            """
            SELECT database_name, schema_name, table_name
            FROM catalog_entities WHERE id = ?
            """,
            (endpoint_id,),
        ).fetchone()
        if row is None:
            return {"to_name": "(table removed)", "to_path": ""}
        fqn = ".".join(filter(None, (row[0], row[1], row[2])))
        return {"to_name": str(row[2] or ""), "to_path": fqn}

    spec = ASSET_KINDS.get(endpoint_kind)
    if spec is None:
        return {"to_name": "(unknown)", "to_path": ""}
    table, name_col = spec
    path_expr = _LINEAGE_PATH_EXPR.get(endpoint_kind, "''")
    row = conn.execute(
        f"SELECT {name_col} AS display_name, "  # noqa: S608 — identifiers controlled
        f"{path_expr} AS display_path "
        f"FROM {table} WHERE id = ? AND profile_name = ?",
        (endpoint_id, profile),
    ).fetchone()
    if row is None:
        return {"to_name": "(asset removed)", "to_path": ""}
    return {
        "to_name": str(row["display_name"] or ""),
        "to_path": str(row["display_path"] or ""),
    }


# Per-kind SQL expression for the lineage endpoint's "display path"
# field. Only kinds whose table actually carries the relevant column
# get a non-empty expression; the rest collapse to ``''`` so the
# COALESCE in the query never references a missing column.
_LINEAGE_PATH_EXPR: dict[str, str] = {
    "notebook": "COALESCE(workspace_path, qualified_name, '')",
    "pipeline": "COALESCE(target_schema, '')",
    "stream": "COALESCE(qualified_name, '')",
    "streamlit": "COALESCE(qualified_name, '')",
    "query": "''",
    "job": "''",
}


# Edge types that this endpoint treats as the source asset
# *reading* the target table. Anything else is treated as a write.
# NULL direction (legacy edges from extractors that don't carry the
# column yet) is grouped under both sides — the UI labels those rows
# as "direction unknown".
_READ_EDGE_TYPES = frozenset(
    {
        "task_runs_notebook",
        "task_runs_query",
        "query_reads_table",
        "notebook_reads_table",
    }
)
_WRITE_EDGE_TYPES = frozenset(
    {
        "pipeline_writes_table",
        "task_runs_pipeline",
        "query_writes_table",
        "notebook_writes_table",
    }
)


@router.get("/by-table")
def list_assets_for_table(
    profile: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    database: str = Query(default=""),
    since_days: int = Query(default=90, ge=0, le=3650),
    direction: str = Query(default="all", pattern="^(all|read|write)$"),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return every ingested asset that reads or writes a given table.

    Powers the inline "Linked assets" surface on the Studio table
    page (the "what touched this table?" view that Unity Catalog
    leads with on every table). Rows come from ``asset_lineage_edges``
    joined to the matching ``remote_*`` table for display names; the
    read versus write split is derived from the edge's ``direction``
    column with ``edge_type`` as a fallback for legacy rows that
    pre-date the column.

    The endpoint accepts the same ``(profile, database, schema,
    table)`` shape the rest of the table routes use so callers don't
    need to know the catalog_entities row id. The matching row is
    resolved internally; the ``database`` param is optional because
    single-database profiles often leave it empty in catalog_entities.

    Parameters
    ----------
    profile
        Required. Scopes the lookup to one ingested DB profile.
    schema, table
        Required. Identify the catalog row.
    database
        Optional. When empty, the lookup falls back to a row whose
        ``database_name`` is empty so single-database profiles work
        out of the box.
    since_days
        Recent-activity window. ``0`` returns the full history;
        any positive value filters by ``last_used_at`` (when
        present) or ``discovered_at`` (fallback for edges that
        platform usage signals have not yet refreshed).
    direction
        ``all`` (default), ``read``, or ``write``. Filters the
        returned lists without changing the per-kind counts.
    """
    db_path = _history_db_path(cfg)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # A provided ``database`` prefers the exact row but also
        # accepts rows whose database_name is empty/NULL: catalog
        # entries synced before multi-database walks (and
        # single-database profiles generally) leave the column blank
        # while deep links carry the live database name — a strict
        # match would 404 the very table the caller is looking at.
        table_row = conn.execute(
            """
            SELECT id, database_name, schema_name, table_name
            FROM catalog_entities
            WHERE db_profile = ?
              AND entity_kind = 'table'
              AND LOWER(schema_name) = LOWER(?)
              AND LOWER(table_name) = LOWER(?)
              AND (
                    ? = ''
                    OR LOWER(database_name) = LOWER(?)
                    OR COALESCE(database_name, '') = ''
                  )
            ORDER BY CASE WHEN LOWER(database_name) = LOWER(?) THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (profile, schema, table, database, database, database),
        ).fetchone()
        if table_row is None:
            raise HTTPException(404, "Table not found in this profile")
        table_id = int(table_row["id"])
        fqn = ".".join(
            filter(
                None,
                (
                    table_row["database_name"],
                    table_row["schema_name"],
                    table_row["table_name"],
                ),
            )
        )

        window_clause = ""
        params: list[Any] = [profile, int(table_id)]
        if since_days > 0:
            window_clause = " AND COALESCE(last_used_at, discovered_at) >= ?"
            cutoff = _epoch_now() - float(since_days) * 86400.0
            params.append(cutoff)

        rows = conn.execute(
            f"""
            SELECT id, from_kind, from_id, edge_type, raw_ref,
                   discovered_at, direction, last_used_at, last_user
            FROM asset_lineage_edges
            WHERE profile_name = ?
              AND to_kind = 'table'
              AND to_id = ?
              {window_clause}
            ORDER BY COALESCE(last_used_at, discovered_at) DESC,
                     edge_type, from_kind, from_id
            """,  # noqa: S608 — window_clause is fixed literal above
            params,
        ).fetchall()

        reads: list[dict[str, Any]] = []
        writes: list[dict[str, Any]] = []
        counts: dict[str, int] = dict.fromkeys(ASSET_KINDS, 0)
        for r in rows:
            endpoint = _describe_edge_endpoint(
                conn, str(r["from_kind"]), int(r["from_id"]), profile
            )
            row_dir = _resolve_direction(str(r["direction"] or ""), str(r["edge_type"] or ""))
            payload = {
                "kind": str(r["from_kind"]),
                "id": int(r["from_id"]),
                "name": endpoint.get("to_name", ""),
                "path": endpoint.get("to_path", ""),
                "edge_type": str(r["edge_type"] or ""),
                "direction": row_dir,
                "last_used_at": (
                    float(r["last_used_at"]) if r["last_used_at"] is not None else None
                ),
                "last_user": str(r["last_user"] or "") or None,
                "discovered_at": (
                    float(r["discovered_at"]) if r["discovered_at"] is not None else None
                ),
                "raw_ref": _decode_raw_ref(r["raw_ref"]),
            }
            if r["from_kind"] in counts:
                counts[str(r["from_kind"])] += 1
            if row_dir in ("read", "unknown"):
                reads.append(payload)
            if row_dir in ("write", "unknown"):
                writes.append(payload)

        if direction == "read":
            writes = []
        elif direction == "write":
            reads = []

    return {
        "table": {
            "id": int(table_id),
            "fqn": fqn,
            "database": table_row["database_name"] or "",
            "schema": table_row["schema_name"] or "",
            "name": table_row["table_name"] or "",
        },
        "profile": profile,
        "since_days": since_days,
        "direction": direction,
        "reads": reads,
        "writes": writes,
        "counts": counts,
    }


def _resolve_direction(stored: str, edge_type: str) -> str:
    """Return ``'read'`` / ``'write'`` / ``'unknown'`` for a row.

    Prefers the explicit ``direction`` column. Falls back to the
    legacy ``edge_type`` mapping so rows written before the column
    existed still classify correctly. Anything unrecognised lands
    in ``'unknown'`` so the UI can surface it under both sides.
    """
    stored = (stored or "").strip().lower()
    if stored in {"read", "write"}:
        return stored
    if stored == "both":
        return "unknown"
    if edge_type in _READ_EDGE_TYPES:
        return "read"
    if edge_type in _WRITE_EDGE_TYPES:
        return "write"
    return "unknown"


def _epoch_now() -> float:
    """Indirection seam so tests can monkey-patch the clock."""
    import time as _time

    return _time.time()


def _decode_raw_ref(raw: Any) -> Any:
    """Decode the ``raw_ref`` payload as JSON, return raw string on failure."""
    if not raw:
        return None
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


@router.get("/{kind}/{asset_id}")
def get_asset(
    kind: str,
    asset_id: int,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return full detail for a single asset, including downstream table references."""
    if kind not in ASSET_KINDS:
        raise HTTPException(400, f"Unknown asset kind: {kind!r}. Valid: {', '.join(ASSET_KINDS)}")
    table, _name_col = ASSET_KINDS[kind]
    with sqlite3.connect(_history_db_path(cfg)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            f"SELECT * FROM {table} WHERE id = ?",
            (asset_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(404, "Asset not found")
        detail = dict(row)
        # Kind-specific enrichment. Joined data lives in sibling tables; the
        # generic row above only carries the top-level record. Adding the
        # children here keeps the detail page to one round-trip.
        if kind == "job":
            detail["tasks"] = _job_tasks(conn, asset_id)
            detail["recent_runs"] = _job_runs(conn, asset_id)
        elif kind == "pipeline":
            detail["libraries"] = _decode_json_array(detail.pop("libraries_json", None))
            detail["latest_update"] = {
                "state": detail.get("latest_update_state"),
                "created_at": detail.get("latest_update_creation_time"),
            }
        elif kind == "streamlit":
            detail["launch_info"] = {
                "main_file": detail.get("main_file"),
                "root_location": detail.get("root_location"),
                "query_warehouse": detail.get("query_warehouse"),
            }
        # Downstream tables joined via catalog_relationships.
        downstream_rows = conn.execute(
            """
            SELECT ce.database_name, ce.schema_name, ce.table_name, ce.id
            FROM catalog_relationships cr
            JOIN catalog_entities ce ON ce.id = cr.to_entity_id
            WHERE cr.relationship_type = 'asset_references_table'
              AND cr.from_entity_kind = ?
              AND cr.from_entity_id = ?
            ORDER BY ce.database_name, ce.schema_name, ce.table_name
            """,
            (kind, asset_id),
        ).fetchall()
    detail["downstream_tables"] = [
        {
            "fqn": ".".join(filter(None, (r["database_name"], r["schema_name"], r["table_name"]))),
            "entity_id": r["id"],
        }
        for r in downstream_rows
    ]
    return detail


def _job_tasks(conn: sqlite3.Connection, job_id: int) -> list[dict[str, Any]]:
    """Return tasks for a job with depends_on decoded and notebook_name resolved."""
    rows = conn.execute(
        """
        SELECT t.task_key, t.task_type, t.notebook_path, t.notebook_id_fk,
               t.sql_query_id, t.sql_warehouse_id, t.pipeline_id_fk,
               t.depends_on_json, t.raw_definition_json,
               n.name AS notebook_name
        FROM remote_job_tasks t
        LEFT JOIN remote_notebooks n ON n.id = t.notebook_id_fk
        WHERE t.job_id_fk = ?
        ORDER BY t.task_key
        """,
        (job_id,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "task_key": r["task_key"],
                "task_type": r["task_type"],
                "notebook_path": r["notebook_path"],
                "notebook_id_fk": r["notebook_id_fk"],
                "notebook_name": r["notebook_name"],
                "sql_query_id": r["sql_query_id"],
                "sql_warehouse_id": r["sql_warehouse_id"],
                "pipeline_id_fk": r["pipeline_id_fk"],
                "depends_on": _decode_json_array(r["depends_on_json"]),
            }
        )
    return out


def _job_runs(conn: sqlite3.Connection, job_id: int) -> list[dict[str, Any]]:
    """Return the last 20 runs ordered most-recent first."""
    rows = conn.execute(
        """
        SELECT run_id, state_result, start_time, end_time,
               setup_duration_ms, execution_duration_ms
        FROM remote_job_runs
        WHERE job_id_fk = ?
        ORDER BY start_time DESC
        LIMIT 20
        """,
        (job_id,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        duration_ms = (r["setup_duration_ms"] or 0) + (r["execution_duration_ms"] or 0)
        out.append(
            {
                "run_id": r["run_id"],
                "state_result": r["state_result"],
                "start_time": r["start_time"],
                "end_time": r["end_time"],
                "setup_duration_ms": r["setup_duration_ms"],
                "execution_duration_ms": r["execution_duration_ms"],
                "duration_ms": duration_ms,
            }
        )
    return out


def _decode_json_array(raw: Any) -> list[Any]:
    """Decode a JSON-array column; return [] on null/empty/parse error."""
    if not raw:
        return []
    try:
        decoded = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return []
    return decoded if isinstance(decoded, list) else []


@router.delete("/{kind}/{asset_id}", status_code=200)
def delete_asset(
    kind: str,
    asset_id: int,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Delete a single asset and its dependent rows.

    Returns a counts dict so the caller can confirm what was removed.
    404 when the asset doesn't exist in the active history DB.
    """
    if kind not in ASSET_KINDS:
        raise HTTPException(400, f"Unknown asset kind: {kind!r}. Valid: {', '.join(ASSET_KINDS)}")
    table, _ = ASSET_KINDS[kind]
    deleted = {"primary": 0, "children": 0, "lineage_edges": 0}
    with sqlite3.connect(_history_db_path(cfg)) as conn:
        existing = conn.execute(f"SELECT id FROM {table} WHERE id = ?", (asset_id,)).fetchone()
        if existing is None:
            raise HTTPException(404, "Asset not found")
        # Cascade child rows for jobs; other kinds have no children.
        if kind == "job":
            cur = conn.execute("DELETE FROM remote_job_tasks WHERE job_id_fk = ?", (asset_id,))
            deleted["children"] += cur.rowcount or 0
            cur = conn.execute("DELETE FROM remote_job_runs WHERE job_id_fk = ?", (asset_id,))
            deleted["children"] += cur.rowcount or 0
        # Clear lineage edges where this asset is the FROM side.
        cur = conn.execute(
            "DELETE FROM catalog_relationships "
            "WHERE relationship_type = 'asset_references_table' "
            "AND from_entity_kind = ? AND from_entity_id = ?",
            (kind, asset_id),
        )
        deleted["lineage_edges"] = cur.rowcount or 0
        # Finally the primary row.
        cur = conn.execute(f"DELETE FROM {table} WHERE id = ?", (asset_id,))
        deleted["primary"] = cur.rowcount or 0
        conn.commit()
    return {"deleted": True, "kind": kind, "asset_id": asset_id, "counts": deleted}


class IngestBody(BaseModel):
    profile: str
    types: list[str]
    history_days: int = 7
    runs_per_job: int = 20
    query_history_limit: int = 1000
    # PR-A: optional per-kind subset of platform-native external_ids
    # from the Studio / CLI browse-and-pick wizard. When absent, every
    # asset of the requested types is ingested (pre-PR-A behaviour).
    selection: dict[str, list[str]] | None = None


# PR-A: browse-and-pick wizard data source. Adapters yield
# ``AssetMetadata`` rows (id + name + path + owner +
# last_modified, no content) so the Studio table populates in one
# burst without dragging the heavy per-asset content fetch into
# the initial paint. The wizard then posts the selected ids back
# through ``POST /ingest`` with ``selection={kind: [...]}``.

_DISCOVER_METHODS = {
    "notebooks": "list_remote_notebooks_metadata",
    "jobs": "list_remote_jobs_metadata",
    "pipelines": "list_remote_pipelines_metadata",
    "streamlit_apps": "list_remote_streamlit_apps_metadata",
    "streams": "list_remote_streams_metadata",
}


@router.get("/discover")
def discover_assets(
    profile: str = Query(...),
    kind: str = Query(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return cheap identity rows for every asset of ``kind`` in ``profile``.

    Powers the Studio IngestDialog "Browse" step and the CLI wizard's
    ``Browse and pick?`` flow. ``queries`` and ``task_dependencies``
    are intentionally absent — they're time-windowed aggregates, not
    per-asset rows the user picks individually.
    """
    method_name = _DISCOVER_METHODS.get(kind)
    if method_name is None:
        raise HTTPException(
            400,
            f"Unknown asset kind: {kind!r}. Valid: {', '.join(_DISCOVER_METHODS)}",
        )
    from amx.cli_support.commands.db_assets_impl import _open_connector

    connector = _open_connector(cfg, profile)
    fn = getattr(connector, method_name, None)
    if fn is None:
        raise HTTPException(
            501,
            f"Profile {profile!r} adapter does not support {kind} discovery.",
        )
    items: list[dict[str, Any]] = []
    try:
        for meta in fn():
            items.append(
                {
                    "kind": meta.kind,
                    "external_id": meta.external_id,
                    "name": meta.name,
                    "path": meta.path,
                    "owner": meta.owner,
                    "last_modified": (
                        meta.last_modified.isoformat() if meta.last_modified else None
                    ),
                }
            )
    except AttributeError as exc:
        raise HTTPException(
            501,
            f"Profile {profile!r} adapter does not support {kind} discovery.",
        ) from exc
    return {"items": items}


@router.post("/ingest", status_code=202)
async def start_ingest(
    body: IngestBody,
    background: BackgroundTasks,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    """Kick off an asset ingest job and return a job_id for SSE polling."""
    job_id = str(uuid.uuid4())
    channel = _JobChannel()
    _INGEST_JOBS[job_id] = channel
    background.add_task(_run_ingest_job, job_id=job_id, body=body, cfg=cfg, channel=channel)
    return {"job_id": job_id}


async def _run_ingest_job(
    *, job_id: str, body: IngestBody, cfg: AMXConfig, channel: _JobChannel
) -> None:
    """Run IngestAssetsService in a thread-executor and forward progress to the channel."""
    from amx.cli_support.commands.db_assets_impl import _open_catalog, _open_connector
    from amx.services.ingest_assets import IngestAssetsService, IngestRequest

    loop = asyncio.get_running_loop()

    def on_progress(evt) -> None:
        loop.call_soon_threadsafe(
            channel.publish,
            {
                "state": evt.state,
                "asset_type": evt.asset_type,
                "count": evt.count,
                "message": evt.message,
            },
        )

    try:
        connector = _open_connector(cfg, body.profile)
        catalog = _open_catalog(cfg)
        svc = IngestAssetsService(connector=connector, catalog=catalog)
        req = IngestRequest(
            profile_name=body.profile,
            # body.types is validated by Pydantic to the same literal
            # set IngestRequest accepts, but mypy can't see the
            # cross-class narrowing — silence the false positive.
            types=body.types,  # type: ignore[arg-type]
            history_days=body.history_days,
            runs_per_job=body.runs_per_job,
            query_history_limit=body.query_history_limit,
            selection=body.selection,
        )
        result = await loop.run_in_executor(None, lambda: svc.run(req, progress=on_progress))
        channel.publish(
            {"state": "completed", "counts": result.counts, "failures": result.failures}
        )
    except Exception as exc:  # noqa: BLE001
        channel.publish({"state": "error", "message": str(exc)})
    finally:
        channel.publish({"_eof": True})


@router.post("/refresh", status_code=202)
async def refresh_assets(
    body: IngestBody,
    background: BackgroundTasks,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, str]:
    """Drop and re-ingest all of a profile's assets.

    Deletes all existing remote asset rows for the given profile, then
    kicks off a fresh ingest job via the same SSE channel as /ingest.
    """
    db_path = _history_db_path(cfg)
    with sqlite3.connect(db_path) as conn:
        for tbl in (
            "remote_notebooks",
            "remote_jobs",
            "remote_pipelines",
            "remote_streamlit_apps",
            "remote_streams",
            "remote_queries",
            "remote_task_dependencies",
        ):
            conn.execute(f"DELETE FROM {tbl} WHERE profile_name = ?", (body.profile,))
        conn.commit()
    return await start_ingest(body=body, background=background, cfg=cfg)
