"""Lineage routes — Studio's read + refresh + LLM-suggest surface.

All endpoints are scoped to one DB profile (resolved the same way as
:mod:`amx.web.routers.catalog`). The bulk read (``GET /api/lineage/...``)
is cache-only: it never opens a wire connection. Refresh +/ suggest
post to the corresponding service methods, blocking until done.
Response shape matches what
:func:`amx.lineage.service.lineage_for_studio` returns.
"""

from __future__ import annotations

import getpass
import json
import re
import time
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

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
    explicit_database: str = "",
) -> Scope:
    name = _resolve_profile(cfg, profile)
    database, schema, table, column = _parse_anchor_path(anchor_path)
    # Explicit ``?database=…`` overrides everything — the Studio wizard
    # picks a database without baking it into the URL slug so the path
    # stays human-readable.
    if explicit_database:
        database = explicit_database
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
    """List rendered lineage artifacts. Optional ``?profile=`` filter.

    Each row is enriched with the anchor's ``(database, schema, table,
    column)`` so the Studio canvas can construct subsequent API calls
    without re-fetching the catalog row.
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    rows = lineage_store.list_lineage_artifacts(hs, db_profile=profile or "")
    if rows:
        anchor_ids = {int(r["anchor_entity_id"]) for r in rows}
        anchors = _bulk_anchor_fqns(hs, anchor_ids)
        for row in rows:
            anchor = anchors.get(int(row["anchor_entity_id"]))
            if anchor:
                row["anchor_database"] = anchor["database"]
                row["anchor_schema"] = anchor["schema"]
                row["anchor_table"] = anchor["table"]
                row["anchor_column"] = anchor["column"]
    return {"artifacts": rows, "count": len(rows)}


def _bulk_anchor_fqns(hs: Any, anchor_ids: set[int]) -> dict[int, dict[str, str]]:
    if not anchor_ids:
        return {}
    placeholders = ",".join("?" for _ in anchor_ids)
    with hs._connect() as conn:
        rows = conn.execute(
            f"""
            SELECT id, database_name, schema_name, table_name, column_name
            FROM catalog_entities
            WHERE id IN ({placeholders})
            """,
            tuple(anchor_ids),
        ).fetchall()
    return {
        int(r[0]): {
            "database": str(r[1] or ""),
            "schema": str(r[2] or ""),
            "table": str(r[3] or ""),
            "column": str(r[4] or ""),
        }
        for r in rows
    }


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
    database: str = Query(default=""),
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
        explicit_database=database,
    )
    payload = lineage_service.lineage_for_studio(hs=hs, scope=scope)
    return payload


@router.post("/{anchor_path:path}/refresh")
def post_refresh(
    anchor_path: str,
    profile: str | None = Query(default=None),
    database: str = Query(default=""),
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
    scope = _scope(cfg, profile=profile, anchor_path=anchor_path, explicit_database=database)
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
    database: str = Query(default=""),
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
    scope = _scope(cfg, profile=profile, anchor_path=anchor_path, explicit_database=database)
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


# ── v3 S4 — manual authoring ─────────────────────────────────────────────


def _actor_name() -> str:
    """Best-effort actor identifier for the audit columns."""
    try:
        return getpass.getuser() or "studio"
    except Exception:
        return "studio"


def _resolve_entity_id_strict(hs: Any, profile: str, fqn: str) -> int:
    """Look up the catalog_entities.id for ``database.schema.table`` (or
    ``schema.table``). Raises 404 with a clear message when missing.
    """
    parts = [p for p in re.split(r"[./]", fqn) if p]
    if len(parts) == 2:
        database, schema, table = "", parts[0], parts[1]
    elif len(parts) == 3:
        database, schema, table = parts[0], parts[1], parts[2]
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not parse FQN {fqn!r} (expected schema.table or database.schema.table).",
        )
    with hs._connect() as conn:
        # Try with the explicit database first; if empty, fall back to
        # any database-scope so paths from the JSON payload (which
        # always carry a database) and paths from the canvas (which
        # may not) both work.
        if database:
            row = conn.execute(
                """
                SELECT id FROM catalog_entities
                WHERE db_profile = ? AND database_name = ?
                  AND schema_name = ? AND table_name = ?
                  AND entity_kind = 'table'
                LIMIT 1
                """,
                (profile, database, schema, table),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT id FROM catalog_entities
                WHERE db_profile = ?
                  AND schema_name = ? AND table_name = ?
                  AND entity_kind = 'table'
                LIMIT 1
                """,
                (profile, schema, table),
            ).fetchone()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"FQN {fqn!r} not found in catalog under profile {profile!r}.",
        )
    return int(row[0])


@router.post("/edges", status_code=status.HTTP_201_CREATED)
def post_edge(
    payload: dict[str, Any] = Body(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Persist a user-authored lineage edge.

    Body shape::

        {
          "profile": "local-postgre",
          "source_fqn": "public.customers"   # or "db.public.customers"
          "target_fqn": "public.orders",
          "notes": "optional human note"
        }
    """
    profile = str(payload.get("profile") or "").strip()
    source_fqn = str(payload.get("source_fqn") or "").strip()
    target_fqn = str(payload.get("target_fqn") or "").strip()
    notes = str(payload.get("notes") or "").strip()
    if not profile or not source_fqn or not target_fqn:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile, source_fqn, target_fqn are required.",
        )
    if source_fqn == target_fqn:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A self-edge (source == target) is not allowed.",
        )
    profile = _resolve_profile(cfg, profile)

    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    src_id = _resolve_entity_id_strict(hs, profile, source_fqn)
    tgt_id = _resolve_entity_id_strict(hs, profile, target_fqn)
    actor = _actor_name()
    now = time.time()
    details = {"notes": notes, "actor": actor, "ts": now}

    with hs._lock, hs._connect() as conn:
        # Upsert by (from, to, manual): re-drawing the same edge updates
        # the verdict + audit fields rather than stacking duplicates.
        conn.execute(
            """
            DELETE FROM catalog_relationships
            WHERE from_entity_id = ? AND to_entity_id = ?
              AND relationship_type = 'lineage_manual'
            """,
            (src_id, tgt_id),
        )
        cur = conn.execute(
            """
            INSERT INTO catalog_relationships
                (from_entity_id, to_entity_id, relationship_type, score, source,
                 details_json, last_seen, verdict, audit_actor, audit_at)
            VALUES (?, ?, 'lineage_manual', 1.0, 'manual',
                    ?, ?, 'approved', ?, ?)
            """,
            (src_id, tgt_id, json.dumps(details, ensure_ascii=False), now, actor, now),
        )
        edge_id = int(cur.lastrowid or 0)
    return {
        "id": edge_id,
        "from": source_fqn,
        "to": target_fqn,
        "verdict": "approved",
        "audit_actor": actor,
        "audit_at": now,
    }


@router.patch("/edges/{edge_id}/verdict")
def patch_edge_verdict(
    edge_id: int,
    payload: dict[str, Any] = Body(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Mark an inferred edge as approved / rejected / pending.

    ``edge_id`` is the ``catalog_relationships.id`` exposed via the
    enriched edge payload (see :func:`get_lineage`).
    """
    verdict = str(payload.get("verdict") or "").strip().lower()
    if verdict not in {"approved", "rejected", "pending", ""}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="verdict must be one of: approved, rejected, pending, '' (clear).",
        )
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    actor = _actor_name()
    now = time.time()
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            """
            UPDATE catalog_relationships
            SET verdict = ?, audit_actor = ?, audit_at = ?
            WHERE id = ?
            """,
            (verdict, actor, now, int(edge_id)),
        )
    if cur.rowcount == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Edge {edge_id} not found.",
        )
    return {"id": int(edge_id), "verdict": verdict, "audit_actor": actor, "audit_at": now}


@router.delete("/edges/{edge_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_edge(
    edge_id: int,
    cfg: AMXConfig = Depends(get_cfg),
) -> None:
    """Hard-delete an edge. Use for manual edges the user no longer wants.

    Inferred edges deleted this way will re-appear on the next
    extractor run; for those, ``PATCH /edges/{id}/verdict`` with
    ``rejected`` is the durable choice.
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    with hs._lock, hs._connect() as conn:
        cur = conn.execute("DELETE FROM catalog_relationships WHERE id = ?", (int(edge_id),))
    if cur.rowcount == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Edge {edge_id} not found.",
        )
    return None


@router.post("/manual", status_code=status.HTTP_201_CREATED)
def post_manual_artifact(
    payload: dict[str, Any] = Body(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Persist a hand-drawn canvas as a fresh lineage artifact.

    Body::

        {
          "profile": "local-postgre",
          "name": "my-custom-flow",
          "anchor_fqn": "public.orders",        # canvas's centre
          "edges": [
            {"source_fqn": "public.customers", "target_fqn": "public.orders"},
            ...
          ]
        }
    """
    profile = str(payload.get("profile") or "").strip()
    name = str(payload.get("name") or "").strip()
    anchor_fqn = str(payload.get("anchor_fqn") or "").strip()
    edges_in = payload.get("edges") or []
    if not profile or not name or not anchor_fqn:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile, name, anchor_fqn are required.",
        )
    profile = _resolve_profile(cfg, profile)
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    anchor_id = _resolve_entity_id_strict(hs, profile, anchor_fqn)
    actor = _actor_name()
    now = time.time()
    persisted = 0
    with hs._lock, hs._connect() as conn:
        for edge in edges_in:
            if not isinstance(edge, dict):
                continue
            src = str(edge.get("source_fqn") or "").strip()
            tgt = str(edge.get("target_fqn") or "").strip()
            if not src or not tgt or src == tgt:
                continue
            try:
                src_id = _resolve_entity_id_strict(hs, profile, src)
                tgt_id = _resolve_entity_id_strict(hs, profile, tgt)
            except HTTPException:
                continue
            details = {"actor": actor, "ts": now, "via": "manual_canvas"}
            conn.execute(
                """
                DELETE FROM catalog_relationships
                WHERE from_entity_id = ? AND to_entity_id = ?
                  AND relationship_type = 'lineage_manual'
                """,
                (src_id, tgt_id),
            )
            conn.execute(
                """
                INSERT INTO catalog_relationships
                    (from_entity_id, to_entity_id, relationship_type, score, source,
                     details_json, last_seen, verdict, audit_actor, audit_at)
                VALUES (?, ?, 'lineage_manual', 1.0, 'manual',
                        ?, ?, 'approved', ?, ?)
                """,
                (
                    src_id,
                    tgt_id,
                    json.dumps(details, ensure_ascii=False),
                    now,
                    actor,
                    now,
                ),
            )
            persisted += 1

    # Compute scope and render the artifact so it shows up on the
    # browse list immediately. We re-use create_lineage so the matplotlib
    # PNG/SVG generation, hashing, and lineage_artifacts row insert all
    # happen through the canonical path.
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT database_name, schema_name, table_name FROM catalog_entities WHERE id = ?",
            (anchor_id,),
        ).fetchone()
    database = str(row[0] or "")
    schema = str(row[1] or "")
    table = str(row[2] or "")
    scope = Scope(
        profile=profile,
        anchor=ColumnRef(database=database, schema=schema, table=table, column=""),
        depth_up=1,
        depth_down=1,
        database=database,
        schema=schema,
    )
    from pathlib import Path as _P

    from amx.config import _resolve_config_dir

    slug = re.sub(r"[^A-Za-z0-9_-]+", "_", name) or "lineage"
    out = _P(_resolve_config_dir()) / "lineage" / f"{slug}.svg"
    try:
        result = lineage_service.create_lineage(
            hs=hs,
            scope=scope,
            name=slug,
            output_path=out,
            fmt="svg",
            fill_decision="skip",
        )
    except Exception as exc:  # render failure should not lose the edges
        return {
            "ok": True,
            "persisted_edges": persisted,
            "artifact_id": 0,
            "render_error": str(exc),
        }
    return {
        "ok": not result.aborted,
        "persisted_edges": persisted,
        "artifact_id": result.artifact_id,
        "node_count": result.node_count,
        "edge_count": result.edge_count,
        "extractors_used": result.extractors_used,
    }
