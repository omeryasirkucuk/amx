"""FastAPI router for /api/assets — list, detail, ingest (with SSE progress)."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
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

# In-process registry of ingest jobs to asyncio.Queue. Studio is single-tenant
# per process, so a module-level dict is fine.
_INGEST_JOBS: dict[str, asyncio.Queue] = {}


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
    profile: str | None = Query(default=None),
    kind: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Semantic search over ingested remote assets.

    Routes the query through :class:`AssetRAGStore` (Chroma + dense
    embedding). When the store is unavailable (no ingest yet, or a
    one-time :class:`EmbeddingProviderMismatch` after switching
    embedding models) the endpoint returns ``items=[]`` and
    ``rag_available=false`` so the Studio UI can surface a hint.
    """
    try:
        from amx.assets.rag import AssetRAGStore
    except Exception as exc:  # noqa: BLE001
        return {"items": [], "rag_available": False, "reason": f"AssetRAGStore unavailable: {exc}"}
    try:
        store = AssetRAGStore(cfg=cfg)
    except Exception as exc:  # noqa: BLE001
        return {"items": [], "rag_available": False, "reason": str(exc)}
    try:
        results = store.query(
            q,
            top_k=int(limit),
            profile=profile or None,
            kind=kind or None,
        )
    except Exception as exc:  # noqa: BLE001
        return {"items": [], "rag_available": False, "reason": str(exc)}
    items = [
        {
            "chunk_id": hit.chunk_id,
            "kind": hit.kind,
            "profile": hit.profile,
            "remote_id": hit.remote_id,
            "name": hit.name,
            # PR-B: surface the disambiguating path as a top-level
            # field so Studio doesn't have to dig through ``metadata``
            # to render "name (path)" for same-name assets. The metadata
            # dict still carries the original ``workspace_path`` /
            # ``qualified_name`` (Snowflake) / ``target_schema``
            # (pipelines) for callers that need the raw value.
            "path": _hit_path(hit),
            "score": hit.score,
            "matched_text": hit.text,
            "metadata": hit.metadata,
        }
        for hit in results
    ]
    return {"items": items, "rag_available": True, "count": len(items)}


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


@router.get("/ingest/{job_id}/events")
async def ingest_events(job_id: str) -> StreamingResponse:
    """Stream SSE progress events for a running ingest job."""
    queue = _INGEST_JOBS.get(job_id)
    if not queue:
        raise HTTPException(404, "Unknown job_id")

    async def gen():
        while True:
            evt = await queue.get()
            if evt.get("_eof"):
                _INGEST_JOBS.pop(job_id, None)
                return
            yield f"data: {json.dumps(evt)}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


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
    """Drop + re-embed just one asset's chunks. Best-effort."""
    try:
        from amx.assets.rag import AssetRAGStore
    except Exception:  # noqa: BLE001
        return False
    try:
        store = AssetRAGStore(cfg=cfg)
    except Exception:  # noqa: BLE001
        return False
    try:
        store.delete_asset(kind=kind, profile=profile, remote_id=remote_id)
        with hs._connect() as conn:  # noqa: SLF001
            store.ingest_profile(
                conn=conn,
                profile_name=profile,
                kinds=[kind],
                only_ids={kind: [remote_id]},
            )
    except Exception:  # noqa: BLE001
        return False
    return True


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
    queue: asyncio.Queue = asyncio.Queue()
    _INGEST_JOBS[job_id] = queue
    background.add_task(_run_ingest_job, job_id=job_id, body=body, cfg=cfg, queue=queue)
    return {"job_id": job_id}


async def _run_ingest_job(
    *, job_id: str, body: IngestBody, cfg: AMXConfig, queue: asyncio.Queue
) -> None:
    """Run IngestAssetsService in a thread-executor and forward progress to the queue."""
    from amx.cli_support.commands.db_assets_impl import _open_catalog, _open_connector
    from amx.services.ingest_assets import IngestAssetsService, IngestRequest

    loop = asyncio.get_running_loop()

    def on_progress(evt) -> None:
        loop.call_soon_threadsafe(
            queue.put_nowait,
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
            types=body.types,
            history_days=body.history_days,
            runs_per_job=body.runs_per_job,
            query_history_limit=body.query_history_limit,
            selection=body.selection,
        )
        result = await loop.run_in_executor(None, lambda: svc.run(req, progress=on_progress))
        await queue.put(
            {"state": "completed", "counts": result.counts, "failures": result.failures}
        )
    except Exception as exc:  # noqa: BLE001
        await queue.put({"state": "error", "message": str(exc)})
    finally:
        await queue.put({"_eof": True})


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
