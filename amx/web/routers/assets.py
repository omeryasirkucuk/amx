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
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return all ingested assets of the given type for a DB profile."""
    if type not in ASSET_KINDS:
        raise HTTPException(400, f"Unknown asset type: {type!r}. Valid: {', '.join(ASSET_KINDS)}")
    table, _name_col = ASSET_KINDS[type]
    with sqlite3.connect(_history_db_path(cfg)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE profile_name = ? ORDER BY id",
            (profile,),
        ).fetchall()
        items = [dict(r) for r in rows]
    return {"items": items, "count": len(items)}


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
