"""Lineage routes — Studio's read + refresh + LLM-suggest surface.

All endpoints are scoped to one DB profile (resolved the same way as
:mod:`amx.web.routers.catalog`). The bulk read (``GET /api/lineage/...``)
is cache-only: it never opens a wire connection. Refresh +/ suggest
post to the corresponding service methods, blocking until done.
Response shape matches what
:func:`amx.lineage.service.lineage_for_studio` returns.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from amx.config import AMXConfig
from amx.lineage import service as lineage_service
from amx.lineage import store as lineage_store
from amx.lineage.discover import discover_profile_lineage
from amx.lineage.types import ColumnRef, Scope
from amx.storage.sqlite_store import history_store
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/lineage", tags=["lineage"])


def _resolve_profile(cfg: AMXConfig, profile: str | None) -> str:
    name = (profile or getattr(cfg, "active_db_profile", "") or "").strip()
    if not name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "No active DB profile — pass ?profile=<name> to scope this request, "
                "or activate a profile via /api/profiles/db/<name>/activate."
            ),
        )
    return name


def _default_database(cfg: AMXConfig, profile_name: str) -> str:
    profiles = getattr(cfg, "db_profiles", {}) or {}
    profile_cfg = profiles.get(profile_name)
    if profile_cfg is None:
        return ""
    return getattr(profile_cfg, "database", "") or ""


def _parse_anchor_path(path: str) -> tuple[str, str, str, str]:
    """Parse ``schema.table`` or ``database.schema.table[.column]``.

    Returns ``(database, schema, table, column)`` with empty strings for
    components the path didn't carry.
    """
    parts = [p for p in re.split(r"[./]", path) if p]
    if len(parts) == 1:
        return "", "", parts[0], ""
    if len(parts) == 2:
        return "", parts[0], parts[1], ""
    if len(parts) == 3:
        return "", parts[0], parts[1], parts[2]
    if len(parts) >= 4:
        return parts[-4], parts[-3], parts[-2], parts[-1]
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Could not parse anchor path {path!r}.",
    )


def _scope(
    cfg: AMXConfig,
    *,
    profile: str | None,
    anchor_path: str,
    depth_up: int = 1,
    depth_down: int = 1,
) -> Scope:
    name = _resolve_profile(cfg, profile)
    database, schema, table, column = _parse_anchor_path(anchor_path)
    if not database:
        database = _default_database(cfg, name)
    anchor = ColumnRef(database=database, schema=schema, table=table, column=column)
    return Scope(
        profile=name,
        anchor=anchor,
        depth_up=depth_up,
        depth_down=depth_down,
        database=database,
        schema=schema,
    )


@router.get("")
def list_artifacts(
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """List rendered lineage artifacts. Optional ``?profile=`` filter."""
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    rows = lineage_store.list_lineage_artifacts(hs, db_profile=profile or "")
    return {"artifacts": rows, "count": len(rows)}


@router.post("/discover")
def post_discover(
    profile: str | None = Query(default=None),
    max_tables: int = Query(default=500, ge=1, le=2000),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Walk cached tables for the profile and return anchors with edges.

    Cache-only — never opens the wire. Returns the ranked list so the
    Studio browse page can surface the most lineage-rich anchors as
    one-click open targets.
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    name = _resolve_profile(cfg, profile)
    result = discover_profile_lineage(hs=hs, profile=name, max_tables=max_tables)
    return {
        "profile": result.profile,
        "anchors": [
            {
                "database": a.database,
                "schema": a.schema,
                "table": a.table,
                "fqn": a.fqn(),
                "edge_count": a.edge_count,
                "extractors_used": a.extractors_used,
                "partial": a.partial,
            }
            for a in result.anchors
        ],
        "tables_examined": result.tables_examined,
        "tables_with_edges": result.tables_with_edges,
        "total_edges": result.total_edges,
        "truncated": result.truncated,
        "duration_sec": result.duration_sec,
    }


@router.get("/{anchor_path:path}")
def get_lineage(
    anchor_path: str,
    profile: str | None = Query(default=None),
    depth_up: int = Query(default=1, ge=0, le=5),
    depth_down: int = Query(default=1, ge=0, le=5),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Cache-only lineage read — JSON nodes + edges for ``anchor_path``."""
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    scope = _scope(
        cfg,
        profile=profile,
        anchor_path=anchor_path,
        depth_up=depth_up,
        depth_down=depth_down,
    )
    payload = lineage_service.lineage_for_studio(hs=hs, scope=scope)
    return payload


@router.post("/{anchor_path:path}/refresh")
def post_refresh(
    anchor_path: str,
    profile: str | None = Query(default=None),
    no_cache: bool = Query(default=False),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Re-extract all default extractors for ``anchor_path``.

    Blocking: returns once extraction completes. If you pass
    ``?no_cache=true`` the view-DDL cache for this scope is invalidated
    and the extractor is allowed to fill from the DB (otherwise the
    decision is ``skip`` — cache-only).
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    scope = _scope(cfg, profile=profile, anchor_path=anchor_path)
    # Try to find an existing artifact for this anchor + run refresh
    # against it. If no artifact yet, fall back to a fresh create.
    anchor_id = lineage_service.resolve_anchor_entity_id(
        hs, profile=scope.profile, anchor=scope.anchor
    )
    if anchor_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anchor {scope.anchor.fqn()!r} not found in catalog.",
        )
    artifacts = lineage_store.list_lineage_artifacts(hs, db_profile=scope.profile)
    matching = [a for a in artifacts if a["anchor_entity_id"] == anchor_id]
    fill = "fill" if no_cache else "skip"
    if matching:
        result = lineage_service.refresh_lineage(
            hs=hs,
            artifact=matching[0],
            fill_decision=fill,
            no_cache=no_cache,
        )
    else:
        # Synthesize a name + path for the new artifact.
        from pathlib import Path as _P

        from amx.config import _resolve_config_dir

        slug = re.sub(
            r"[^A-Za-z0-9_-]+",
            "_",
            "-".join(p for p in (scope.anchor.schema, scope.anchor.table) if p) or "lineage",
        )
        out = _P(_resolve_config_dir()) / "lineage" / f"{slug}.svg"
        result = lineage_service.create_lineage(
            hs=hs,
            scope=scope,
            name=slug,
            output_path=out,
            fmt="svg",
            fill_decision=fill,
        )
    return {
        "ok": not result.aborted,
        "artifact_id": result.artifact_id,
        "node_count": result.node_count,
        "edge_count": result.edge_count,
        "extractors_used": result.extractors_used,
        "extractors_partial": result.extractors_partial,
        "aborted": result.aborted,
        "abort_reason": result.abort_reason,
    }


@router.post("/suggest-bulk")
def post_suggest_bulk(
    profile: str | None = Query(default=None),
    schema: str = Query(..., min_length=1),
    database: str = Query(default=""),
    budget_tokens: int = Query(default=50_000, ge=100, le=2_000_000),
    budget_tables: int = Query(default=25, ge=1, le=500),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Run AI suggest across every table in ``schema``.

    Hard-stops as soon as either ``budget_tokens`` or ``budget_tables``
    is reached. Synchronous in v3: returns the full rollup once
    finished. SSE-streamed progress is on the S5 roadmap.
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    name = _resolve_profile(cfg, profile)
    db = database or _default_database(cfg, name)
    rollup = lineage_service.suggest_lineage_llm_bulk(
        hs=hs,
        profile=name,
        schema=schema,
        database=db,
        cfg=cfg,
        budget_tokens=budget_tokens,
        budget_tables=budget_tables,
    )
    if rollup.aborted:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=rollup.abort_reason,
        )
    return {
        "profile": rollup.profile,
        "schema": rollup.schema,
        "model": rollup.model,
        "tables_examined": rollup.tables_examined,
        "tables_with_edges": rollup.tables_with_edges,
        "total_edges_persisted": rollup.total_edges_persisted,
        "total_tokens_used": rollup.total_tokens_used,
        "halted_by": rollup.halted_by,
        "per_table": rollup.per_table,
    }


@router.post("/{anchor_path:path}/suggest")
def post_suggest(
    anchor_path: str,
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Run a single on-demand LLM call for ``anchor_path``.

    Persists any returned edges into ``catalog_relationships`` so the
    next ``GET /api/lineage/<path>`` surfaces them. Spends LLM tokens
    on the user's active profile — the Studio UI must wrap this in a
    confirmation prompt.
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    scope = _scope(cfg, profile=profile, anchor_path=anchor_path)
    result = lineage_service.suggest_lineage_llm(hs=hs, scope=scope, cfg=cfg)
    if result.aborted:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result.abort_reason,
        )
    return {
        "edges": result.edges,
        "persisted": result.persisted_count,
        "model": result.model,
    }
