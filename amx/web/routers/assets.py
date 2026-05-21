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
            "fqn": ".".join(
                filter(None, (r["database_name"], r["schema_name"], r["table_name"]))
            ),
            "entity_id": r["id"],
        }
        for r in downstream_rows
    ]
    return detail


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


async def _run_ingest_job(*, job_id: str, body: IngestBody, cfg: AMXConfig, queue: asyncio.Queue) -> None:
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
