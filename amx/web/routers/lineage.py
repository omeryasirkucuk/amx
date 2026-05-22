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
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

from amx.config import AMXConfig
from amx.lineage import service as lineage_service
from amx.lineage import store as lineage_store
from amx.lineage.discover import discover_profile_lineage
from amx.lineage.types import ColumnRef, Scope
from amx.storage.sqlite_store import history_store
from amx.web.deps import get_cfg
from amx.web.permissions import require_writer_role

router = APIRouter(prefix="/api/lineage", tags=["lineage"])


def _artifact_record_to_dict(record: Any) -> dict[str, Any]:
    """Convert a SQLAlchemy LineageArtifactRecord dataclass to a plain dict.

    The local SQLite path already returns plain dicts; the shared-store
    path returns dataclass instances that need converting so both code
    paths emit the same shape to the frontend.
    """
    if isinstance(record, dict):
        return record
    return dict(vars(record)) if hasattr(record, "__dict__") else dict(record)


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
    # When the caller passes the database explicitly AND the path
    # starts with that database segment, strip it before parsing.
    # Otherwise ``_parse_anchor_path`` treats the 3-part FQN
    # ``db.schema.table`` as ``schema.table.column`` and AI Generate
    # blows up with "anchor not found in catalog_entities". The strip
    # is purely defensive — a path that doesn't carry the leading
    # database segment is left alone.
    if explicit_database and anchor_path.startswith(f"{explicit_database}."):
        anchor_path = anchor_path[len(explicit_database) + 1 :]
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
    db_profiles: list[str] | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """List rendered lineage artifacts.

    ``?profile=`` filters to a single profile (legacy parameter, kept for
    back-compat). ``?db_profiles=a&db_profiles=b`` filters to multiple
    profiles. When neither is provided all artifacts are returned.

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
    # Resolve the effective profile filter. ``db_profiles`` (multi-value)
    # takes precedence; fall back to the legacy single ``profile`` param.
    effective_profiles: list[str] | None = None
    if db_profiles:
        effective_profiles = [p for p in db_profiles if p]
    elif profile:
        effective_profiles = [profile]

    # SQLAlchemyHistoryStore supports db_profiles= natively; the local
    # SQLite store only accepts a single db_profile string. Detect which
    # store is active and route accordingly.
    if hasattr(hs, "list_lineage_artifacts") and hasattr(hs, "engine"):
        # Shared (SQLAlchemy) path
        rows = hs.list_lineage_artifacts(db_profiles=effective_profiles)
        rows = [_artifact_record_to_dict(r) for r in rows]
    else:
        # Local SQLite path — list all then filter in Python
        rows = lineage_store.list_lineage_artifacts(hs, db_profile="")
        if effective_profiles:
            rows = [r for r in rows if r.get("db_profile") in effective_profiles]
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


@router.get("/artifacts-with-table")
def artifacts_with_table(
    profile: str = Query(...),
    database: str = Query(default=""),
    schema: str = Query(default=""),
    table: str = Query(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """List every saved lineage artifact that contains the given
    table. Drives the table page's ``Open lineage`` button:

    * 0 results → frontend opens a fresh canvas seeded with the table
    * 1 result  → frontend navigates straight to ``?artifact=<id>``
    * 2+        → frontend offers a picker

    Match is by ``(profile, database, schema, table)`` against
    ``catalog_entities`` joined with ``lineage_artifact_nodes`` — the
    same identity used by every other lineage flow.
    """
    if not profile or not table:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile and table are required.",
        )
    profile = _resolve_profile(cfg, profile)
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    with hs._connect() as conn:
        # ``database_name`` is matched leniently: an AI-stream table
        # often lands in ``catalog_entities`` with the database
        # column empty (the streamed 2-part FQN didn't carry one),
        # while the same table added via the picker lands with the
        # database populated. A strict equality match would split
        # those two saves into "different" entities and the
        # button's multi-match picker would only ever surface one
        # of them. We treat empty-on-either-side as a wildcard so
        # both representations of the same logical table fold into
        # the same artifact list.
        rows = conn.execute(
            """
            SELECT DISTINCT la.id, la.name, la.db_profile,
                   la.anchor_entity_id, la.depth_up, la.depth_down,
                   la.format, la.output_path, la.edge_set_hash,
                   la.node_count, la.edge_count, la.generated_at,
                   la.extractors_used, la.extractors_partial
            FROM lineage_artifacts la
            JOIN lineage_artifact_nodes lan ON lan.artifact_id = la.id
            JOIN catalog_entities ce       ON ce.id = lan.entity_id
            WHERE ce.db_profile    = ?
              AND ce.schema_name   = ?
              AND ce.table_name    = ?
              AND (
                ce.database_name = ?
                OR ce.database_name = ''
                OR ? = ''
              )
            ORDER BY la.generated_at DESC
            """,
            (profile, schema, table, database, database),
        ).fetchall()
    artifacts: list[dict[str, Any]] = []
    for r in rows:
        try:
            extractors = json.loads(r[12] or "[]")
        except (TypeError, ValueError):
            extractors = []
        artifacts.append(
            {
                "id": int(r[0]),
                "name": str(r[1] or ""),
                "db_profile": str(r[2] or ""),
                "anchor_entity_id": int(r[3] or 0),
                "depth_up": int(r[4] or 0),
                "depth_down": int(r[5] or 0),
                "format": str(r[6] or ""),
                "output_path": str(r[7] or ""),
                "edge_set_hash": str(r[8] or ""),
                "node_count": int(r[9] or 0),
                "edge_count": int(r[10] or 0),
                "generated_at": float(r[11] or 0.0),
                "extractors_used": extractors if isinstance(extractors, list) else [],
                "extractors_partial": bool(r[13] or 0),
            }
        )
    return {"artifacts": artifacts, "count": len(artifacts)}


@router.get("/audit")
def get_audit_trail(
    profile: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Recent verdict + manual-edge actions for the profile.

    Returns rows sorted by ``audit_at`` desc — consumed by the audit
    trail card on the Lineage browse page.
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    name = _resolve_profile(cfg, profile)
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT cr.id, cr.relationship_type, cr.verdict, cr.audit_actor, cr.audit_at,
                   cr.source, cr.details_json,
                   src.schema_name, src.table_name,
                   tgt.schema_name, tgt.table_name
            FROM catalog_relationships cr
            JOIN catalog_entities src ON src.id = cr.from_entity_id
            JOIN catalog_entities tgt ON tgt.id = cr.to_entity_id
            WHERE src.db_profile = ? AND cr.audit_at IS NOT NULL
            ORDER BY cr.audit_at DESC
            LIMIT ?
            """,
            (name, limit),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            details = json.loads(row[6] or "{}")
        except (TypeError, ValueError):
            details = {}
        note = ""
        if isinstance(details, dict):
            note = str(details.get("notes") or details.get("reasoning") or "")[:200]
        out.append(
            {
                "edge_id": int(row[0]),
                "relationship_type": str(row[1]),
                "verdict": str(row[2] or ""),
                "actor": str(row[3] or ""),
                "at": float(row[4] or 0.0),
                "source": str(row[5] or ""),
                "from": f"{row[7]}.{row[8]}",
                "to": f"{row[9]}.{row[10]}",
                "note": note,
            }
        )
    return {"profile": name, "entries": out, "count": len(out)}


# ── v4 — trace panel (server-side column BFS) ────────────────────────────
#
# Declared BEFORE the catch-all ``/{anchor_path:path}`` route so the
# concrete prefix wins the FastAPI match order.


@router.get("/column-trace/{anchor_path:path}")
def get_column_trace(
    anchor_path: str,
    profile: str | None = Query(default=None),
    column: str = Query(..., min_length=1),
    direction: str = Query(default="upstream"),
    max_depth: int = Query(default=50, ge=1, le=200),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Server-side BFS over column-level edges.

    Returns an ordered list of steps from ORIGIN → HERE (upstream) or
    HERE → DOWNSTREAM (downstream), capped at ``max_depth`` so an
    accidentally deep chain cannot stall the page.

    Each step carries ``{step, fqn, table, column, kind,
    relationship_type, operator?}`` — operator entries surface the
    op_kind + expression of the synthetic node so the panel can render
    a transformation step rather than just a column-to-column hop.
    """
    name = _resolve_profile(cfg, profile)
    database, schema, table, _anchor_col = _parse_anchor_path(anchor_path)
    if not table:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not parse anchor table from {anchor_path!r}.",
        )
    if direction not in {"upstream", "downstream"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="direction must be 'upstream' or 'downstream'.",
        )
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    anchor_id = _lookup_table_id(hs, name, database, schema, table)
    if anchor_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table {schema}.{table} not in catalog for profile {name!r}.",
        )

    steps = _walk_column_chain(
        hs,
        anchor_id=anchor_id,
        anchor_column=column,
        direction=direction,
        max_depth=max_depth,
    )
    truncated = len(steps) >= max_depth
    return {
        "profile": name,
        "anchor": {
            "database": database,
            "schema": schema,
            "table": table,
            "column": column,
        },
        "direction": direction,
        "steps": steps,
        "count": len(steps),
        "truncated": truncated,
    }


def _lookup_table_id(hs: Any, profile: str, database: str, schema: str, table: str) -> int | None:
    with hs._connect() as conn:
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
    return int(row[0]) if row else None


def _walk_column_chain(
    hs: Any,
    *,
    anchor_id: int,
    anchor_column: str,
    direction: str,
    max_depth: int,
) -> list[dict[str, Any]]:
    """BFS over column-grain catalog_relationships rows.

    Stops when the depth limit is reached, when a leaf has no further
    edges, or when a cycle re-enters a previously-visited
    ``(entity_id, column)`` node.
    """
    visited: set[tuple[int, str]] = {(anchor_id, anchor_column)}
    frontier: list[tuple[int, str, int]] = [(anchor_id, anchor_column, 0)]
    steps: list[dict[str, Any]] = []
    upstream = direction == "upstream"
    with hs._connect() as conn:
        while frontier and len(steps) < max_depth:
            node_id, col, depth = frontier.pop(0)
            if upstream:
                rows = conn.execute(
                    """
                    SELECT cr.from_entity_id, cr.from_column, cr.relationship_type,
                           ce.entity_kind, ce.search_text, ce.schema_name, ce.table_name
                    FROM catalog_relationships cr
                    JOIN catalog_entities ce ON ce.id = cr.from_entity_id
                    WHERE cr.to_entity_id = ? AND cr.to_column = ?
                    """,
                    (node_id, col),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT cr.to_entity_id, cr.to_column, cr.relationship_type,
                           ce.entity_kind, ce.search_text, ce.schema_name, ce.table_name
                    FROM catalog_relationships cr
                    JOIN catalog_entities ce ON ce.id = cr.to_entity_id
                    WHERE cr.from_entity_id = ? AND cr.from_column = ?
                    """,
                    (node_id, col),
                ).fetchall()
            for r in rows:
                next_id = int(r[0])
                next_col = str(r[1] or "")
                key = (next_id, next_col)
                if key in visited:
                    continue
                visited.add(key)
                kind = str(r[3] or "")
                step: dict[str, Any] = {
                    "step": len(steps) + 1,
                    "depth": depth + 1,
                    "entity_id": next_id,
                    "fqn": f"{r[5]}.{r[6]}" + (f".{next_col}" if next_col else ""),
                    "table": str(r[6] or ""),
                    "schema": str(r[5] or ""),
                    "column": next_col,
                    "kind": kind,
                    "relationship_type": str(r[2] or ""),
                }
                if kind == "operator":
                    try:
                        details = json.loads(r[4] or "{}")
                    except (TypeError, ValueError):
                        details = {}
                    if isinstance(details, dict):
                        step["operator"] = {
                            "op_kind": str(details.get("op_kind") or ""),
                            "expression": str(details.get("expression") or ""),
                        }
                steps.append(step)
                if len(steps) >= max_depth:
                    break
                frontier.append((next_id, next_col, depth + 1))
    return steps


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


# ── Routes MUST come before the catch-all GET below ─────────────────────
#
# FastAPI matches routes in registration order; the bare
# ``/{anchor_path:path}`` GET below would otherwise gobble ``/by-id/<id>``
# and treat the artifact id as an anchor table FQN. We register the
# by-id read + the SSE stream up here so they win the routing match.


@router.get("/by-id/{artifact_id}")
def get_artifact_by_id(
    artifact_id: int,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Load a saved canvas by its artifact_id (cross-profile aware).

    Returns nodes (with per-node profile + position), edges,
    and comments. This is the canonical re-open path the frontend uses
    after a save — it never resolves the canvas by the artifact's
    user-visible name, which avoids the save-canvas name-as-table
    misresolve bug.
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    with hs._connect() as conn:
        art = conn.execute(
            "SELECT id, name, db_profile, anchor_entity_id, generated_at, "
            "       node_count, edge_count "
            "FROM lineage_artifacts WHERE id = ?",
            (int(artifact_id),),
        ).fetchone()
    if not art:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Lineage artifact {artifact_id} not found.",
        )
    primary_profile = str(art[2] or "")
    anchor_id = int(art[3])

    with hs._connect() as conn:
        node_rows = conn.execute(
            "SELECT entity_id, db_profile, x, y, width, height, z_index, "
            "       COALESCE(logo_key, '') "
            "FROM lineage_artifact_nodes WHERE artifact_id = ? "
            "ORDER BY z_index, id",
            (int(artifact_id),),
        ).fetchall()
        comment_rows = conn.execute(
            "SELECT id, x, y, width, height, color, text, created_at, updated_at, "
            "       COALESCE(style, 'note') "
            "FROM lineage_comments WHERE artifact_id = ? ORDER BY id",
            (int(artifact_id),),
        ).fetchall()
    from amx.lineage.logo_store import list_logo_nodes

    logo_nodes_out = list_logo_nodes(hs, int(artifact_id))

    nodes_out: list[dict[str, Any]] = []
    by_profile: dict[str, list[int]] = {}
    for row in node_rows:
        by_profile.setdefault(str(row[1] or ""), []).append(int(row[0]))

    from amx.lineage.operator_ops import decode_operator_details

    entity_meta: dict[int, dict[str, Any]] = {}
    for prof, ids in by_profile.items():
        if not ids:
            continue
        placeholders = ",".join("?" for _ in ids)
        with hs._connect() as conn:
            rows = conn.execute(
                f"SELECT id, database_name, schema_name, table_name, "
                f"       column_name, entity_kind, search_text "
                f"FROM catalog_entities WHERE id IN ({placeholders})",
                tuple(ids),
            ).fetchall()
        for r in rows:
            kind = str(r[5] or "table")
            meta: dict[str, Any] = {
                "profile": prof,
                "database": str(r[1] or ""),
                "schema": str(r[2] or ""),
                "table": str(r[3] or ""),
                "column": str(r[4] or ""),
                "kind": kind,
            }
            # Operator entities stash their op_kind + expression
            # inside ``search_text`` JSON. Surface them as first-class
            # fields so the frontend's ``loadedNodeToCanvasNode`` can
            # rebuild the OperatorNode without re-parsing.
            if kind == "operator":
                details = decode_operator_details(str(r[6] or ""))
                meta["op_kind"] = str(details.get("op_kind") or "")
                meta["expression"] = str(details.get("expression") or "")
            elif kind in ("notebook", "query", "stream", "pipeline", "streamlit_app", "job"):
                # Bridge rows for ingested remote assets store the
                # asset's display name in ``search_text`` (set by
                # ``SyncMixin._upsert_asset_entity``). Surface it as
                # ``label`` so the canvas AssetNode can render it
                # without a second lookup.
                meta["label"] = str(r[6] or "")
            entity_meta[int(r[0])] = meta

    # Bulk-fetch per-table column lists from the column-comments
    # cache so the canvas re-renders with the real column rail
    # instead of an empty "(no columns cached)" placeholder.
    table_columns: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    table_keys = {
        (
            entity_meta[int(r[0])].get("profile", str(r[1] or "")),
            entity_meta[int(r[0])].get("database", ""),
            entity_meta[int(r[0])].get("schema", ""),
            entity_meta[int(r[0])].get("table", ""),
        )
        for r in node_rows
        if entity_meta.get(int(r[0]), {}).get("kind") == "table"
    }
    if table_keys:
        with hs._connect() as conn:
            for prof, db, sch, tbl in table_keys:
                if not tbl:
                    continue
                row = conn.execute(
                    """
                    SELECT columns_json FROM column_comments_cache
                    WHERE db_profile = ? AND database_name = ?
                      AND schema_name = ? AND table_name = ?
                    LIMIT 1
                    """,
                    (prof, db, sch, tbl),
                ).fetchone()
                if not row:
                    continue
                try:
                    raw = json.loads(row[0] or "[]")
                except (ValueError, TypeError):
                    continue
                if not isinstance(raw, list):
                    continue
                cols: list[dict[str, Any]] = []
                for c in raw:
                    if not isinstance(c, dict):
                        continue
                    name = str(c.get("name") or "").strip()
                    if not name:
                        continue
                    cols.append(
                        {
                            "name": name,
                            "dtype": str(c.get("dtype") or c.get("type") or ""),
                            "isPrimary": bool(c.get("pk_flag") or c.get("is_primary")),
                            "isForeign": bool(c.get("fk_flag") or c.get("is_foreign")),
                        }
                    )
                table_columns[(prof, db, sch, tbl)] = cols

    for row in node_rows:
        entity_id = int(row[0])
        meta = entity_meta.get(entity_id, {})
        node_entry: dict[str, Any] = {
            "entity_id": entity_id,
            "profile": str(row[1] or ""),
            "x": float(row[2] or 0.0),
            "y": float(row[3] or 0.0),
            "width": float(row[4] or 240.0),
            "height": float(row[5] or 120.0),
            "z_index": int(row[6] or 0),
            "logo_key": str(row[7] or ""),
            "database": meta.get("database", ""),
            "schema": meta.get("schema", ""),
            "table": meta.get("table", ""),
            "column": meta.get("column", ""),
            "kind": meta.get("kind", "table"),
            "fqn": ".".join(
                p
                for p in (
                    meta.get("database", ""),
                    meta.get("schema", ""),
                    meta.get("table", ""),
                )
                if p
            ),
        }
        if meta.get("kind") == "operator":
            node_entry["op_kind"] = meta.get("op_kind", "")
            node_entry["expression"] = meta.get("expression", "")
        elif meta.get("kind") in (
            "notebook",
            "query",
            "stream",
            "pipeline",
            "streamlit_app",
            "job",
        ):
            node_entry["label"] = meta.get("label", "")
        elif meta.get("kind") == "table":
            cols = table_columns.get(
                (
                    str(row[1] or ""),
                    meta.get("database", ""),
                    meta.get("schema", ""),
                    meta.get("table", ""),
                ),
                [],
            )
            if cols:
                node_entry["columns"] = cols
        nodes_out.append(node_entry)

    edges_out: list[dict[str, Any]] = []
    if node_rows:
        node_ids = [int(r[0]) for r in node_rows]
        placeholders = ",".join("?" for _ in node_ids)
        with hs._connect() as conn:
            rels = conn.execute(
                f"""
                SELECT id, from_entity_id, to_entity_id, from_column, to_column,
                       relationship_type, source, score, verdict,
                       style_color, style_dashed, cardinality
                FROM catalog_relationships
                WHERE from_entity_id IN ({placeholders})
                  AND to_entity_id IN ({placeholders})
                """,
                tuple(node_ids) + tuple(node_ids),
            ).fetchall()
        for r in rels:
            # ``style_dashed`` is a nullable INTEGER (0/1); preserve
            # ``None`` so the frontend can tell "user left it
            # untouched" apart from "user explicitly picked solid".
            dashed_raw = r[10]
            dashed_out: bool | None = bool(int(dashed_raw)) if dashed_raw is not None else None
            edges_out.append(
                {
                    "id": int(r[0]),
                    "from_entity_id": int(r[1]),
                    "to_entity_id": int(r[2]),
                    "from_column": str(r[3] or ""),
                    "to_column": str(r[4] or ""),
                    "relationship_type": str(r[5] or ""),
                    "source": str(r[6] or ""),
                    "score": float(r[7] or 0.0),
                    "verdict": str(r[8] or ""),
                    "style_color": (str(r[9]) if r[9] else None),
                    "style_dashed": dashed_out,
                    "cardinality": (str(r[11]) if r[11] else None),
                }
            )

    comments_out = [
        {
            "id": int(c[0]),
            "x": float(c[1] or 0.0),
            "y": float(c[2] or 0.0),
            "width": float(c[3] or 240.0),
            "height": float(c[4] or 140.0),
            "color": str(c[5] or "amber"),
            "text": str(c[6] or ""),
            "created_at": float(c[7] or 0.0),
            "updated_at": float(c[8] or 0.0),
            "style": str(c[9] or "note"),
        }
        for c in comment_rows
    ]

    return {
        "artifact_id": int(art[0]),
        "name": str(art[1] or ""),
        "primary_profile": primary_profile,
        "anchor_entity_id": anchor_id,
        "generated_at": float(art[4] or 0.0),
        "nodes": nodes_out,
        "edges": edges_out,
        "comments": comments_out,
        "logo_nodes": logo_nodes_out,
    }


@router.delete("/by-id/{artifact_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_artifact_by_id(
    artifact_id: int,
    cfg: AMXConfig = Depends(get_cfg),
    _: None = Depends(require_writer_role),
) -> None:
    """Hard-delete a saved canvas. Cascade-removes its nodes, logo
    nodes and comments; does NOT touch ``catalog_relationships``
    because those edges are shared across artifacts (deleting them
    here would yank the same edge out of every other canvas that
    surfaces it).

    Idempotent on the artifact row itself: returns 404 if the
    artifact does not exist so callers can detect the difference,
    but children that were already orphaned by a half-applied
    delete are tolerated silently.
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    aid = int(artifact_id)
    with hs._lock, hs._connect() as conn:
        exists = conn.execute(
            "SELECT 1 FROM lineage_artifacts WHERE id = ?",
            (aid,),
        ).fetchone()
        if not exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Lineage artifact {artifact_id} not found.",
            )
        # Explicit cascade — SQLite respects ``ON DELETE CASCADE``
        # only when ``PRAGMA foreign_keys`` is on, and we don't rely
        # on that being set everywhere the store is opened. Run the
        # deletes in a single connection so an interruption can't
        # leave half-purged children behind.
        conn.execute(
            "DELETE FROM lineage_artifact_nodes WHERE artifact_id = ?",
            (aid,),
        )
        conn.execute(
            "DELETE FROM lineage_logo_nodes WHERE artifact_id = ?",
            (aid,),
        )
        conn.execute(
            "DELETE FROM lineage_comments WHERE artifact_id = ?",
            (aid,),
        )
        conn.execute(
            "DELETE FROM lineage_artifacts WHERE id = ?",
            (aid,),
        )
    return None


@router.get("/{anchor_path:path}/suggest/stream")
def stream_suggest_lineage(
    anchor_path: str,
    profile: str | None = Query(default=None),
    database: str = Query(default=""),
    cfg: AMXConfig = Depends(get_cfg),
):
    """Server-Sent Events stream that emits one event per extractor batch.

    Wraps :func:`amx.lineage.service.suggest_lineage_llm` and the cache-only
    pass of the deterministic extractors. Events are emitted as they
    complete (FK first, then view-DDL, then LLM) so the frontend canvas
    can animate each batch in instead of waiting for the full pipeline.

    Registered BEFORE the catch-all GET below so the routing match wins.

    Event format::

        event: edges-batch
        data: {"extractor": "fk", "edges": [...], "partial": false}

        event: done
        data: {"total_edges": N}

        event: error
        data: {"message": "..."}
    """
    from fastapi.responses import StreamingResponse

    name = _resolve_profile(cfg, profile)
    scope = _scope(
        cfg,
        profile=name,
        anchor_path=anchor_path,
        depth_up=2,
        depth_down=2,
        explicit_database=database,
    )

    def _stream():
        hs = history_store()
        if hs is None:
            yield 'event: error\ndata: {"message": "History store unavailable"}\n\n'
            return
        try:
            yield from lineage_service.stream_suggest_lineage(hs, scope, cfg)
        except Exception as exc:  # pragma: no cover - defensive
            payload = json.dumps({"message": str(exc)})
            yield f"event: error\ndata: {payload}\n\n"

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/by-id/{artifact_id}/comments")
def list_comments(artifact_id: int) -> dict[str, Any]:
    """List sticky-note comments on a saved canvas."""
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT id, x, y, width, height, color, text, created_at, updated_at, "
            "       COALESCE(style, 'note') "
            "FROM lineage_comments WHERE artifact_id = ? ORDER BY id",
            (int(artifact_id),),
        ).fetchall()
    return {
        "artifact_id": int(artifact_id),
        "comments": [
            {
                "id": int(r[0]),
                "x": float(r[1] or 0.0),
                "y": float(r[2] or 0.0),
                "width": float(r[3] or 240.0),
                "height": float(r[4] or 140.0),
                "color": str(r[5] or "amber"),
                "text": str(r[6] or ""),
                "created_at": float(r[7] or 0.0),
                "updated_at": float(r[8] or 0.0),
                "style": str(r[9] or "note"),
            }
            for r in rows
        ],
    }


# ── Logo registry + logo-node CRUD ───────────────────────────────────────
#
# All GET routes here are registered BEFORE the catch-all
# ``/{anchor_path:path}`` GET below for the same routing-precedence reason
# the by-id reads use. POST / PATCH / DELETE don't share a catch-all so
# they can sit anywhere, but we keep them grouped here for readability.


@router.get("/logos")
def get_logos() -> dict[str, Any]:
    """Return every logo in the registry (defaults + customs)."""
    from amx.lineage.logo_store import list_logos

    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    return {"logos": list_logos(hs)}


@router.post("/logos", status_code=status.HTTP_201_CREATED)
def post_logo(
    payload: dict[str, Any] = Body(...),
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Upload a custom logo (data URL or external URL)."""
    from amx.lineage.logo_store import LogoStoreError, create_custom_logo

    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    try:
        return create_custom_logo(
            hs,
            key=str(payload.get("key") or ""),
            label=str(payload.get("label") or ""),
            category=str(payload.get("category") or "custom"),
            data_url=str(payload.get("data_url") or ""),
            url=str(payload.get("url") or ""),
        )
    except LogoStoreError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.delete("/logos/{logo_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_logo(
    logo_id: int,
    _: None = Depends(require_writer_role),
) -> None:
    """Delete a custom logo. Defaults are protected (403)."""
    from amx.lineage.logo_store import LogoStoreError, delete_custom_logo

    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    try:
        delete_custom_logo(hs, int(logo_id))
    except LogoStoreError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.get("/by-id/{artifact_id}/logo-nodes")
def get_logo_nodes(artifact_id: int) -> dict[str, Any]:
    """List standalone logo nodes on a saved canvas."""
    from amx.lineage.logo_store import list_logo_nodes

    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    return {
        "artifact_id": int(artifact_id),
        "logo_nodes": list_logo_nodes(hs, int(artifact_id)),
    }


@router.post("/by-id/{artifact_id}/logo-nodes", status_code=status.HTTP_201_CREATED)
def post_logo_node(
    artifact_id: int,
    payload: dict[str, Any] = Body(...),
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Drop a logo (by id or key) on a saved canvas."""
    from amx.lineage.logo_store import LogoStoreError, create_logo_node

    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    try:
        return create_logo_node(
            hs,
            int(artifact_id),
            logo_id=int(payload["logo_id"]) if payload.get("logo_id") else None,
            logo_key=str(payload.get("logo_key") or "") or None,
            label=str(payload.get("label") or ""),
            x=float(payload.get("x") or 0.0),
            y=float(payload.get("y") or 0.0),
            width=float(payload.get("width") or 120.0),
            height=float(payload.get("height") or 120.0),
        )
    except LogoStoreError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.patch("/by-id/{artifact_id}/logo-nodes/{node_id}")
def patch_logo_node(
    artifact_id: int,
    node_id: int,
    payload: dict[str, Any] = Body(...),
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Move / resize / relabel a logo node."""
    from amx.lineage.logo_store import update_logo_node

    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    updated = update_logo_node(hs, int(artifact_id), int(node_id), payload=payload)
    return {"ok": True, "updated": int(updated)}


@router.delete(
    "/by-id/{artifact_id}/logo-nodes/{node_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_logo_node_route(
    artifact_id: int,
    node_id: int,
    _: None = Depends(require_writer_role),
) -> None:
    from amx.lineage.logo_store import delete_logo_node

    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    delete_logo_node(hs, int(artifact_id), int(node_id))


@router.get("/{anchor_path:path}")
def get_lineage(
    anchor_path: str,
    profile: str | None = Query(default=None),
    database: str = Query(default=""),
    depth_up: int = Query(default=1, ge=0, le=5),
    depth_down: int = Query(default=1, ge=0, le=5),
    include_heuristics: bool = Query(default=False),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Cache-only lineage read — JSON nodes + edges for ``anchor_path``.

    v4 S5 — ``include_heuristics`` toggles :class:`NameMatchExtractor`.
    Off by default to keep the default canvas free of name-based
    false positives; users opt in for noisy-but-broader discovery.
    """
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
    payload = lineage_service.lineage_for_studio(
        hs=hs, scope=scope, include_heuristics=include_heuristics
    )
    return payload


@router.post("/{anchor_path:path}/refresh")
def post_refresh(
    anchor_path: str,
    profile: str | None = Query(default=None),
    database: str = Query(default=""),
    no_cache: bool = Query(default=False),
    cfg: AMXConfig = Depends(get_cfg),
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Re-extract all default extractors for ``anchor_path``.

    Blocking: returns once extraction completes. If you pass
    ``?no_cache=true`` the view-DDL cache for this scope is invalidated
    and the extractor is allowed to fill from the DB (otherwise the
    decision is ``skip`` — cache-only).

    v4 hotfix — any unexpected exception inside the extractor or
    render pipeline now returns ``aborted=True`` with the reason
    surfaced to the toast, never a bare 500.
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    scope = _scope(cfg, profile=profile, anchor_path=anchor_path, explicit_database=database)
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
    try:
        if matching:
            result = lineage_service.refresh_lineage(
                hs=hs,
                artifact=matching[0],
                fill_decision=fill,
                no_cache=no_cache,
            )
        else:
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
    except Exception as exc:
        return {
            "ok": False,
            "artifact_id": 0,
            "node_count": 0,
            "edge_count": 0,
            "extractors_used": [],
            "extractors_partial": False,
            "aborted": True,
            "abort_reason": f"{type(exc).__name__}: {exc}",
        }
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
    _: None = Depends(require_writer_role),
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
    _: None = Depends(require_writer_role),
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
    try:
        result = lineage_service.suggest_lineage_llm(hs=hs, scope=scope, cfg=cfg)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{type(exc).__name__}: {exc}",
        ) from exc
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


def _split_fqn_with_column(fqn: str) -> tuple[str, str, str, str]:
    """Parse a 2/3/4-part FQN. Returns ``(database, schema, table, column)``.

    Two parts: ``schema.table``. Three: ``database.schema.table`` —
    column stays empty; callers that need a 3-part column FQN
    (``schema.table.column``) should use
    :func:`_split_fqn_resolve_column`, which falls back to the
    column interpretation when the table lookup misses. Four parts:
    ``database.schema.table.column``.
    """
    parts = [p for p in re.split(r"[./]", fqn) if p]
    if len(parts) == 2:
        return "", parts[0], parts[1], ""
    if len(parts) == 4:
        return parts[0], parts[1], parts[2], parts[3]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2], ""
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
            f"Could not parse FQN {fqn!r} "
            "(expected schema.table, database.schema.table, "
            "or database.schema.table.column)."
        ),
    )


def _split_fqn_resolve_column(hs: Any, profile: str, fqn: str) -> tuple[str, str, str, str]:
    """Column-aware parser. Disambiguates 3-part FQNs by table lookup.

    For 3-part inputs, first treats parts as ``database.schema.table``;
    if no catalog row matches, retries as ``schema.table.column``.
    """
    parts = [p for p in re.split(r"[./]", fqn) if p]
    if len(parts) != 3:
        return _split_fqn_with_column(fqn)
    db_cand, schema_cand, table_cand = parts
    with hs._connect() as conn:
        hit = conn.execute(
            """
            SELECT 1 FROM catalog_entities
            WHERE db_profile = ? AND database_name = ?
              AND schema_name = ? AND table_name = ?
              AND entity_kind = 'table'
            LIMIT 1
            """,
            (profile, db_cand, schema_cand, table_cand),
        ).fetchone()
    if hit:
        return db_cand, schema_cand, table_cand, ""
    return "", db_cand, schema_cand, table_cand


def _resolve_entity_id_strict(hs: Any, profile: str, fqn: str) -> int:
    """Look up the catalog_entities.id for the parent table of ``fqn``.

    Accepts 2-, 3-, and 4-part FQNs. 3-part is disambiguated via
    :func:`_split_fqn_resolve_column` so ``schema.table.column``
    paths resolve to the table id. 4-part FQNs resolve to the
    parent table id (the column part is intentionally dropped by
    this helper; callers that need both the id and the column
    should use :func:`_split_fqn_resolve_column` directly).
    Raises 404 with a clear message when missing.
    """
    database, schema, table, _column = _split_fqn_resolve_column(hs, profile, fqn)
    with hs._connect() as conn:
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
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Persist a user-authored lineage edge.

    Body shape::

        {
          "profile": "local-postgre",
          "source_fqn": "public.customers.id"     # or 2/3-part FQN
          "target_fqn": "public.orders.customer_id",
          "notes": "optional human note",
          "source_column": "id",                  # optional override
          "target_column": "customer_id"          # optional override
        }

    When the FQN carries 4 parts (``database.schema.table.column``),
    the rightmost segment is treated as the column. ``source_column``
    / ``target_column`` in the body override the parsed value when
    both are present — useful for hand-authored edges where the
    canvas already knows the column independently from the FQN.
    """
    from amx.lineage.operator_ops import write_column_edge

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
    _src_db, _src_schema, _src_table, src_column_from_fqn = _split_fqn_resolve_column(
        hs, profile, source_fqn
    )
    _tgt_db, _tgt_schema, _tgt_table, tgt_column_from_fqn = _split_fqn_resolve_column(
        hs, profile, target_fqn
    )
    src_id = _resolve_entity_id_strict(hs, profile, source_fqn)
    tgt_id = _resolve_entity_id_strict(hs, profile, target_fqn)
    src_column = str(payload.get("source_column") or src_column_from_fqn or "").strip()
    tgt_column = str(payload.get("target_column") or tgt_column_from_fqn or "").strip()
    actor = _actor_name()
    now = time.time()
    details = {"notes": notes, "actor": actor, "ts": now}

    edge_id = write_column_edge(
        hs,
        from_entity_id=src_id,
        from_column=src_column,
        to_entity_id=tgt_id,
        to_column=tgt_column,
        relationship_type="lineage_manual",
        score=1.0,
        source="manual",
        details=details,
        verdict="approved",
        audit_actor=actor,
        audit_at=now,
    )
    return {
        "id": edge_id,
        "from": source_fqn,
        "to": target_fqn,
        "from_column": src_column,
        "to_column": tgt_column,
        "verdict": "approved",
        "audit_actor": actor,
        "audit_at": now,
    }


@router.patch("/edges/{edge_id}/verdict")
def patch_edge_verdict(
    edge_id: int,
    payload: dict[str, Any] = Body(...),
    cfg: AMXConfig = Depends(get_cfg),
    _: None = Depends(require_writer_role),
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


_VALID_CARDINALITIES = {"1:1", "1:N", "N:M"}


@router.patch("/edges/{edge_id}/style")
def patch_edge_style(
    edge_id: int,
    payload: dict[str, Any] = Body(...),
    cfg: AMXConfig = Depends(get_cfg),
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Update Studio-canvas style overrides for an edge.

    Body keys are all optional; absent keys leave the column
    untouched, ``null`` clears the override back to the default.

    * ``style_color``: hex string (e.g. ``"#60a5fa"``) or ``null``.
    * ``style_dashed``: bool or ``null``.
    * ``cardinality``: one of ``"1:1"``, ``"1:N"``, ``"N:M"``, or
      ``null``.
    """
    sets: list[str] = []
    args: list[Any] = []
    if "style_color" in payload:
        v = payload["style_color"]
        if v is not None and not isinstance(v, str):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="style_color must be a string or null.",
            )
        sets.append("style_color = ?")
        args.append(v)
    if "style_dashed" in payload:
        v = payload["style_dashed"]
        if v is None:
            sets.append("style_dashed = NULL")
        else:
            if not isinstance(v, bool):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="style_dashed must be a bool or null.",
                )
            sets.append("style_dashed = ?")
            args.append(1 if v else 0)
    if "cardinality" in payload:
        v = payload["cardinality"]
        if v is not None and v not in _VALID_CARDINALITIES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="cardinality must be one of: '1:1', '1:N', 'N:M', or null.",
            )
        sets.append("cardinality = ?")
        args.append(v)
    if not sets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No style fields provided.",
        )
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    args.append(int(edge_id))
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            f"UPDATE catalog_relationships SET {', '.join(sets)} WHERE id = ?",
            tuple(args),
        )
    if cur.rowcount == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Edge {edge_id} not found.",
        )
    return {"id": int(edge_id), "ok": True}


@router.post("/edges/among")
def edges_among(
    payload: dict[str, Any] = Body(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return every ``catalog_relationships`` row whose endpoints are
    both in the supplied set of entities.

    Studio calls this after AI Generate (and on demand via the
    "Discover related" toolbar button) to surface neighbour-to-
    neighbour edges. The LLM extractor is anchor-centric and never
    asks for non-anchor pairs, so without this call deterministic
    edges (FK / view DDL / query log) between newly-spawned tables
    would stay invisible on the canvas.

    Body — either form is accepted, and they may be mixed:

        ``{"entity_ids": [int, ...]}``
            Used by canvases loaded from an artifact (every node
            carries its backend ``entity_id``).
        ``{"tables": [{"profile": str, "fqn": str}, ...]}``
            Used by canvases assembled via AI Generate where the
            new rows have not been persisted yet and only carry an
            FQN. The handler resolves each pair to an entity_id
            via the same strict lookup the save flow uses;
            unresolved entries are silently dropped.

    Returns the same edge shape used by ``GET /by-id/{artifact_id}``
    (the canvas's load path) so the frontend can dedupe by ``id`` and
    drop merged rows straight into ReactFlow state.
    """
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    raw_ids = payload.get("entity_ids") or []
    raw_tables = payload.get("tables") or []
    ids: list[int] = []
    seen: set[int] = set()
    for v in raw_ids:
        try:
            iv = int(v)
        except (TypeError, ValueError):
            continue
        if iv in seen:
            continue
        seen.add(iv)
        ids.append(iv)
    for entry in raw_tables:
        if not isinstance(entry, dict):
            continue
        profile = str(entry.get("profile") or "").strip()
        fqn = str(entry.get("fqn") or "").strip()
        if not profile or not fqn:
            continue
        try:
            iv = _resolve_entity_id_strict(hs, profile, fqn)
        except HTTPException:
            # Missing catalog row — skip silently rather than failing
            # the whole call. Studio's canvas may carry stale FQNs
            # that no longer resolve, and we'd rather surface the
            # other edges than 404 the discovery pass.
            continue
        if iv in seen:
            continue
        seen.add(iv)
        ids.append(iv)
    if len(ids) < 2:
        # Less than two endpoints means no edge can sit between two
        # distinct nodes — short-circuit with an empty payload rather
        # than hitting SQLite with a degenerate WHERE.
        return {"edges": [], "count": 0}
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    placeholders = ",".join("?" for _ in ids)
    with hs._connect() as conn:
        rows = conn.execute(
            f"""
            SELECT id, from_entity_id, to_entity_id, from_column, to_column,
                   relationship_type, source, score, verdict,
                   style_color, style_dashed, cardinality
            FROM catalog_relationships
            WHERE from_entity_id IN ({placeholders})
              AND to_entity_id IN ({placeholders})
              AND from_entity_id != to_entity_id
            """,
            tuple(ids) + tuple(ids),
        ).fetchall()
    edges_out: list[dict[str, Any]] = []
    for r in rows:
        dashed_raw = r[10]
        dashed_out: bool | None = bool(int(dashed_raw)) if dashed_raw is not None else None
        edges_out.append(
            {
                "id": int(r[0]),
                "from_entity_id": int(r[1]),
                "to_entity_id": int(r[2]),
                "from_column": str(r[3] or ""),
                "to_column": str(r[4] or ""),
                "relationship_type": str(r[5] or ""),
                "source": str(r[6] or ""),
                "score": float(r[7] or 0.0),
                "verdict": str(r[8] or ""),
                "style_color": (str(r[9]) if r[9] else None),
                "style_dashed": dashed_out,
                "cardinality": (str(r[11]) if r[11] else None),
            }
        )
    return {"edges": edges_out, "count": len(edges_out)}


@router.delete("/edges/{edge_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_edge(
    edge_id: int,
    cfg: AMXConfig = Depends(get_cfg),
    _: None = Depends(require_writer_role),
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


# ── v4 — column-level operator nodes ─────────────────────────────────────


@router.post("/operators", status_code=status.HTTP_201_CREATED)
def post_operator(
    payload: dict[str, Any] = Body(...),
    cfg: AMXConfig = Depends(get_cfg),
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Create a transformation operator and chain it between two columns.

    Writes one ``catalog_entities`` row (the operator) plus two
    ``catalog_relationships`` rows (in/out edges) atomically — see
    :func:`amx.lineage.operator_ops.create_operator_with_edges`.

    Body::

        {
          "profile": "local-postgre",
          "source_fqn": "public.orders.amount",
          "target_fqn": "public.daily_totals.gross",
          "op_kind": "aggregate",
          "expression": "SUM(amount)",
          "relationship_type": "lineage_manual"   # optional, defaults to manual
        }
    """
    from amx.lineage.operator_ops import create_operator_with_edges

    profile = str(payload.get("profile") or "").strip()
    source_fqn = str(payload.get("source_fqn") or "").strip()
    target_fqn = str(payload.get("target_fqn") or "").strip()
    op_kind = str(payload.get("op_kind") or "").strip().lower()
    expression = str(payload.get("expression") or "").strip()
    relationship_type = str(payload.get("relationship_type") or "lineage_manual").strip()
    if not profile or not source_fqn or not target_fqn or not op_kind:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile, source_fqn, target_fqn, op_kind are required.",
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
    _src_db, _src_sch, _src_tbl, src_col_from_fqn = _split_fqn_resolve_column(
        hs, profile, source_fqn
    )
    tgt_db, tgt_schema, tgt_table, tgt_col_from_fqn = _split_fqn_resolve_column(
        hs, profile, target_fqn
    )
    src_col = str(payload.get("source_column") or src_col_from_fqn or "").strip()
    tgt_col = str(payload.get("target_column") or tgt_col_from_fqn or "").strip()
    db_backend = _profile_backend(cfg, profile)
    actor = _actor_name()
    try:
        result = create_operator_with_edges(
            hs,
            profile=profile,
            db_backend=db_backend,
            source_entity_id=src_id,
            source_column=src_col,
            target_entity_id=tgt_id,
            target_column=tgt_col,
            target_database=tgt_db,
            target_schema=tgt_schema,
            target_table=tgt_table,
            op_kind=op_kind,
            expression=expression,
            relationship_type=relationship_type,
            source="manual" if relationship_type == "lineage_manual" else "view_ddl",
            verdict="approved" if relationship_type == "lineage_manual" else "",
            audit_actor=actor,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    return {
        "operator_id": result["operator_id"],
        "operator_path": result["operator_path"],
        "edge_ids": result["edge_ids"],
        "op_kind": op_kind,
        "expression": expression,
    }


@router.patch("/operators/{operator_id}")
def patch_operator(
    operator_id: int,
    payload: dict[str, Any] = Body(...),
    cfg: AMXConfig = Depends(get_cfg),
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Update an operator entity's expression."""
    from amx.lineage.operator_ops import lookup_operator, update_operator_expression

    expression = str(payload.get("expression") or "").strip()
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    if not update_operator_expression(hs, operator_id=int(operator_id), expression=expression):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Operator {operator_id} not found.",
        )
    after = lookup_operator(hs, operator_id=int(operator_id))
    return {
        "operator_id": int(operator_id),
        "expression": expression,
        "operator_path": after["operator_path"] if after else "",
    }


def _profile_backend(cfg: AMXConfig, profile: str) -> str:
    """Look up the active backend for a profile (best effort)."""
    profiles = getattr(cfg, "db_profiles", {}) or {}
    entry = profiles.get(profile)
    if entry is None:
        return ""
    return str(getattr(entry, "backend", "") or "")


@router.post("/manual", status_code=status.HTTP_201_CREATED)
def post_manual_artifact(
    payload: dict[str, Any] = Body(...),
    cfg: AMXConfig = Depends(get_cfg),
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Persist a hand-drawn canvas as a fresh lineage artifact.

    Cross-profile aware: each node may carry its own ``profile`` so a
    single canvas can host tables from multiple DB profiles. The
    request's top-level ``profile`` is the artifact's "primary" profile
    (used for AI generate / refresh on this canvas) and the default for
    nodes that omit their own profile.

    The ``name`` field is a pure display string; the response carries
    ``artifact_id`` for navigation. Clients must navigate to the canvas
    by id (``?artifact=<id>``), never by name — using name as an
    anchor-table FQN was the root cause of the save-canvas mis-resolve
    bug.

    Body::

        {
          "profile": "local-postgre",
          "name": "my-custom-flow",
          "anchor_fqn": "public.orders",        # canvas's centre
          "nodes": [
            {
              "profile": "local-postgre",       # optional override
              "fqn": "public.orders",
              "x": 120, "y": 80,
              "width": 240, "height": 120
            },
            ...
          ],
          "edges": [
            {
              "source_fqn": "public.customers", "source_profile": "...",
              "target_fqn": "public.orders",   "target_profile": "...",
              "source_column": "id",           "target_column": "customer_id"
            },
            ...
          ],
          "comments": [
            {"x": 40, "y": 40, "width": 240, "height": 140,
             "color": "amber", "text": "Note body"}
          ]
        }
    """
    primary_profile = str(payload.get("profile") or "").strip()
    name = str(payload.get("name") or "").strip()
    anchor_fqn = str(payload.get("anchor_fqn") or "").strip()
    nodes_in = payload.get("nodes") or []
    edges_in = payload.get("edges") or []
    operators_in = payload.get("operators") or []
    comments_in = payload.get("comments") or []
    logo_nodes_in = payload.get("logo_nodes") or []
    # Optional artifact_id signals an update of an already-loaded
    # canvas. When present and the row exists, the handler purges
    # the old children + re-inserts under the same id (the URL
    # ``?artifact=<id>`` stays stable). When absent, a name clash
    # bounces the request with a 409 so the frontend can surface a
    # rename hint instead of a 500.
    supplied_artifact_id = payload.get("artifact_id")
    update_id: int | None = None
    if supplied_artifact_id is not None:
        try:
            update_id = int(supplied_artifact_id)
        except (TypeError, ValueError):
            update_id = None
    if not primary_profile or not name or not anchor_fqn:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile, name, anchor_fqn are required.",
        )
    primary_profile = _resolve_profile(cfg, primary_profile)
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    from amx.lineage.operator_ops import upsert_operator_entity, write_column_edge

    # Resolve whether the requested name clashes with a different
    # already-saved artifact. The frontend treats the 409 as a
    # signal to surface an inline "rename or open existing" hint.
    with hs._connect() as conn:
        existing_row = conn.execute(
            "SELECT id, name FROM lineage_artifacts WHERE name = ? LIMIT 1",
            (name,),
        ).fetchone()
    existing_id = int(existing_row[0]) if existing_row else None
    if existing_id is not None and existing_id != update_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "name_in_use",
                "message": f"Lineage name '{name}' is already used by another saved canvas.",
                "existing_id": existing_id,
                "existing_name": name,
            },
        )

    # Guard against the case where the client thought it was
    # updating an artifact that has since been deleted: drop the
    # update intent so the request creates a fresh row instead of
    # failing the children-update DELETE+INSERT on a missing parent.
    if update_id is not None:
        with hs._connect() as conn:
            still_there = conn.execute(
                "SELECT 1 FROM lineage_artifacts WHERE id = ?",
                (update_id,),
            ).fetchone()
        if not still_there:
            update_id = None

    anchor_id = _resolve_entity_id_strict(hs, primary_profile, anchor_fqn)
    actor = _actor_name()
    now = time.time()
    persisted = 0

    # Resolve every operator on the canvas to a backend entity_id up
    # front so the edge loop below can stitch table↔operator edges by
    # the operator's React Flow node id.
    db_backend = _profile_backend(cfg, primary_profile)
    op_node_to_entity: dict[str, int] = {}
    op_node_meta: dict[str, dict[str, Any]] = {}
    for op in operators_in:
        if not isinstance(op, dict):
            continue
        node_id = str(op.get("node_id") or "").strip()
        op_kind = str(op.get("op_kind") or "").strip()
        if not node_id or not op_kind:
            continue
        expression = str(op.get("expression") or "")
        # The operator entity needs a host (database, schema, table)
        # so two filters on the same target table don't collide on
        # the unique key — anchor table is the natural host for the
        # canvas-floating operators users draw by hand.
        anchor_database, anchor_schema, anchor_table, _anchor_col = _split_fqn_resolve_column(
            hs, primary_profile, anchor_fqn
        )
        try:
            entity_id, _path = upsert_operator_entity(
                hs,
                profile=primary_profile,
                db_backend=db_backend or "",
                database=anchor_database or "",
                schema=anchor_schema or "",
                table=anchor_table or "",
                op_kind=op_kind,
                expression=expression,
            )
        except ValueError:
            # Unknown op_kind — skip rather than fail the whole save.
            continue
        op_node_to_entity[node_id] = entity_id
        op_node_meta[node_id] = {
            "entity_id": entity_id,
            "x": float(op.get("x") or 0.0),
            "y": float(op.get("y") or 0.0),
            "width": float(op.get("width") or 240.0),
            "height": float(op.get("height") or 120.0),
            "z_index": int(op.get("z_index") or 0),
        }
    for edge in edges_in:
        if not isinstance(edge, dict):
            continue
        # Each endpoint can be either an operator on the canvas
        # (identified by ``source_node_id`` / ``target_node_id``
        # mapped via ``op_node_to_entity``) or a real table
        # (identified by ``source_fqn`` / ``target_fqn``). An edge
        # is dropped only when neither form resolves on a side,
        # so a freshly-drawn table↔operator edge round-trips.
        src_node_id = str(edge.get("source_node_id") or "").strip()
        tgt_node_id = str(edge.get("target_node_id") or "").strip()
        src_op_id = op_node_to_entity.get(src_node_id) if src_node_id else None
        tgt_op_id = op_node_to_entity.get(tgt_node_id) if tgt_node_id else None
        src = str(edge.get("source_fqn") or "").strip()
        tgt = str(edge.get("target_fqn") or "").strip()
        src_profile = str(edge.get("source_profile") or primary_profile).strip() or primary_profile
        tgt_profile = str(edge.get("target_profile") or primary_profile).strip() or primary_profile
        try:
            if src_op_id is not None:
                src_id = src_op_id
                src_col_from_fqn = ""
            else:
                if not src:
                    continue
                src_id = _resolve_entity_id_strict(hs, src_profile, src)
                _sdb, _ssch, _stbl, src_col_from_fqn = _split_fqn_resolve_column(
                    hs, src_profile, src
                )
            if tgt_op_id is not None:
                tgt_id = tgt_op_id
                tgt_col_from_fqn = ""
            else:
                if not tgt:
                    continue
                tgt_id = _resolve_entity_id_strict(hs, tgt_profile, tgt)
                _tdb, _tsch, _ttbl, tgt_col_from_fqn = _split_fqn_resolve_column(
                    hs, tgt_profile, tgt
                )
        except HTTPException:
            continue
        if src_id == tgt_id:
            continue
        src_col = str(edge.get("source_column") or src_col_from_fqn or "").strip()
        tgt_col = str(edge.get("target_column") or tgt_col_from_fqn or "").strip()
        details = {
            "actor": actor,
            "ts": now,
            "via": "manual_canvas",
            "source_profile": src_profile,
            "target_profile": tgt_profile,
        }
        # Optional Studio-canvas overrides: only forwarded when the
        # payload sets them so a re-save of an old canvas does not
        # accidentally null out fields it never knew about.
        style_color = edge.get("style_color")
        style_dashed_raw = edge.get("style_dashed")
        style_dashed: bool | None = bool(style_dashed_raw) if style_dashed_raw is not None else None
        cardinality = edge.get("cardinality")
        if cardinality is not None and cardinality not in _VALID_CARDINALITIES:
            cardinality = None
        write_column_edge(
            hs,
            from_entity_id=src_id,
            from_column=src_col,
            to_entity_id=tgt_id,
            to_column=tgt_col,
            relationship_type="lineage_manual",
            score=1.0,
            source="manual",
            details=details,
            verdict="approved",
            audit_actor=actor,
            audit_at=now,
            style_color=str(style_color) if style_color else None,
            style_dashed=style_dashed,
            cardinality=str(cardinality) if cardinality else None,
        )
        persisted += 1

    # Compute scope and render the artifact so it shows up on the
    # browse list immediately. We re-use create_lineage so the matplotlib
    # PNG/SVG generation, hashing, and lineage_artifacts row insert all
    # happen through the canonical path. Scope is the primary profile;
    # nodes from other profiles are layered in via lineage_artifact_nodes
    # below and surface through the cross-profile read path in
    # ``GET /api/lineage/by-id/{id}``.
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT database_name, schema_name, table_name FROM catalog_entities WHERE id = ?",
            (anchor_id,),
        ).fetchone()
    database = str(row[0] or "")
    schema = str(row[1] or "")
    table = str(row[2] or "")
    scope = Scope(
        profile=primary_profile,
        anchor=ColumnRef(database=database, schema=schema, table=table, column=""),
        depth_up=1,
        depth_down=1,
        database=database,
        schema=schema,
    )
    from pathlib import Path as _P

    from amx.config import _resolve_config_dir

    # Slug is used for the on-disk image path only. It never participates
    # in re-open routing; the artifact_id is the only identifier the
    # frontend uses to load this canvas back.
    slug_base = re.sub(r"[^A-Za-z0-9_-]+", "_", name) or "lineage"
    slug = f"{slug_base}_{int(now)}"
    out = _P(_resolve_config_dir()) / "lineage" / f"{slug}.svg"

    @dataclass
    class _ArtifactStub:
        """Mini-LineageRunResult for the in-place update path so the
        downstream children-insertion blocks (which all read
        ``result.artifact_id``) stay untouched."""

        artifact_id: int

    if update_id is not None:
        # Update-in-place: drop every child row of the existing
        # artifact, then rename + refresh the parent row. The
        # downstream blocks re-INSERT the new children with the
        # same artifact_id so the URL ``?artifact=<id>`` stays
        # stable across saves.
        node_count = sum(1 for n in nodes_in if isinstance(n, dict))
        edge_count = persisted
        with hs._lock, hs._connect() as conn:
            conn.execute(
                "DELETE FROM lineage_artifact_nodes WHERE artifact_id = ?",
                (update_id,),
            )
            conn.execute(
                "DELETE FROM lineage_logo_nodes WHERE artifact_id = ?",
                (update_id,),
            )
            conn.execute(
                "DELETE FROM lineage_comments WHERE artifact_id = ?",
                (update_id,),
            )
            conn.execute(
                """
                UPDATE lineage_artifacts
                SET name = ?, db_profile = ?, anchor_entity_id = ?,
                    node_count = ?, edge_count = ?, generated_at = ?
                WHERE id = ?
                """,
                (
                    name,
                    primary_profile,
                    int(anchor_id),
                    int(node_count),
                    int(edge_count),
                    float(now),
                    update_id,
                ),
            )
        result = _ArtifactStub(artifact_id=update_id)
    else:
        try:
            result = lineage_service.create_lineage(
                hs=hs,
                scope=scope,
                name=name,
                output_path=out,
                fmt="svg",
                fill_decision="skip",
            )
        except sqlite3.IntegrityError as exc:
            # Race: another save grabbed the name between our
            # pre-flight check and the insert. Bounce with the
            # same 409 shape so the frontend's name-conflict UI
            # behaves identically.
            with hs._connect() as conn:
                row = conn.execute(
                    "SELECT id FROM lineage_artifacts WHERE name = ? LIMIT 1",
                    (name,),
                ).fetchone()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "name_in_use",
                    "message": f"Lineage name '{name}' is already used by another saved canvas.",
                    "existing_id": int(row[0]) if row else None,
                    "existing_name": name,
                },
            ) from exc
        except Exception as exc:  # render failure should not lose the edges
            return {
                "ok": True,
                "persisted_edges": persisted,
                "artifact_id": 0,
                "render_error": str(exc),
            }

    # Persist operator-node placements alongside table nodes so the
    # load endpoint can re-render the canvas exactly as it was —
    # without this row, the operator entity exists in the catalog but
    # the artifact has no idea where to draw it.
    if result.artifact_id and op_node_meta:
        with hs._connect() as conn:
            for _node_id, meta in op_node_meta.items():
                conn.execute(
                    """
                    INSERT INTO lineage_artifact_nodes
                        (artifact_id, entity_id, db_profile, x, y, width, height,
                         z_index, logo_key)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(result.artifact_id),
                        int(meta["entity_id"]),
                        primary_profile,
                        meta["x"],
                        meta["y"],
                        meta["width"],
                        meta["height"],
                        meta["z_index"],
                        "",
                    ),
                )

    # Persist per-node placements + cross-profile mapping.
    if result.artifact_id and nodes_in:
        with hs._connect() as conn:
            for node in nodes_in:
                if not isinstance(node, dict):
                    continue
                node_fqn = str(node.get("fqn") or "").strip()
                if not node_fqn:
                    continue
                node_profile = (
                    str(node.get("profile") or primary_profile).strip() or primary_profile
                )
                try:
                    entity_id = _resolve_entity_id_strict(hs, node_profile, node_fqn)
                except HTTPException:
                    continue
                conn.execute(
                    """
                    INSERT INTO lineage_artifact_nodes
                        (artifact_id, entity_id, db_profile, x, y, width, height,
                         z_index, logo_key)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(result.artifact_id),
                        entity_id,
                        node_profile,
                        float(node.get("x") or 0.0),
                        float(node.get("y") or 0.0),
                        float(node.get("width") or 240.0),
                        float(node.get("height") or 120.0),
                        int(node.get("z_index") or 0),
                        str(node.get("logo_key") or ""),
                    ),
                )

    # Persist canvas annotations — both styles share this table:
    # ``style='note'`` (default) is the colored sticky; ``style='text'``
    # is the minimal plain-text label.
    if result.artifact_id and comments_in:
        with hs._connect() as conn:
            for comment in comments_in:
                if not isinstance(comment, dict):
                    continue
                conn.execute(
                    """
                    INSERT INTO lineage_comments
                        (artifact_id, x, y, width, height, color, text, style,
                         created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(result.artifact_id),
                        float(comment.get("x") or 0.0),
                        float(comment.get("y") or 0.0),
                        float(comment.get("width") or 240.0),
                        float(comment.get("height") or 140.0),
                        str(comment.get("color") or "amber"),
                        str(comment.get("text") or ""),
                        str(comment.get("style") or "note"),
                        now,
                        now,
                    ),
                )

    # Persist standalone logo nodes (Power BI / Tableau / etc.) — these
    # represent external systems on the canvas. Each entry references a
    # registry row by ``logo_key`` (preferred, stable across history
    # store rebuilds) or by ``logo_id``. Unknown keys are silently
    # skipped so a stale frontend cache can't 500 the save.
    if result.artifact_id and logo_nodes_in:
        from amx.lineage.logo_store import LogoStoreError, create_logo_node

        for entry in logo_nodes_in:
            if not isinstance(entry, dict):
                continue
            try:
                create_logo_node(
                    hs,
                    int(result.artifact_id),
                    logo_id=int(entry["logo_id"]) if entry.get("logo_id") else None,
                    logo_key=str(entry.get("logo_key") or "") or None,
                    label=str(entry.get("label") or ""),
                    x=float(entry.get("x") or 0.0),
                    y=float(entry.get("y") or 0.0),
                    width=float(entry.get("width") or 120.0),
                    height=float(entry.get("height") or 120.0),
                )
            except LogoStoreError:
                continue

    return {
        "ok": not result.aborted,
        "persisted_edges": persisted,
        "artifact_id": result.artifact_id,
        "node_count": result.node_count,
        "edge_count": result.edge_count,
        "extractors_used": result.extractors_used,
    }


# ── Comments CRUD + SSE stream + SQL bridge ──────────────────────────────


@router.post("/by-id/{artifact_id}/comments", status_code=status.HTTP_201_CREATED)
def post_comment(
    artifact_id: int,
    payload: dict[str, Any] = Body(...),
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Create a sticky-note comment on a saved canvas."""
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    now = time.time()
    with hs._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO lineage_comments
                (artifact_id, x, y, width, height, color, text, style,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(artifact_id),
                float(payload.get("x") or 0.0),
                float(payload.get("y") or 0.0),
                float(payload.get("width") or 240.0),
                float(payload.get("height") or 140.0),
                str(payload.get("color") or "amber"),
                str(payload.get("text") or ""),
                str(payload.get("style") or "note"),
                now,
                now,
            ),
        )
        new_id = int(cur.lastrowid)
    return {"id": new_id, "artifact_id": int(artifact_id), "created_at": now}


@router.patch("/by-id/{artifact_id}/comments/{comment_id}")
def patch_comment(
    artifact_id: int,
    comment_id: int,
    payload: dict[str, Any] = Body(...),
    _: None = Depends(require_writer_role),
) -> dict[str, Any]:
    """Update a sticky-note comment in place."""
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    sets: list[str] = []
    args: list[Any] = []
    for key in ("x", "y", "width", "height"):
        if key in payload:
            sets.append(f"{key} = ?")
            args.append(float(payload[key]))
    for key in ("color", "text", "style"):
        if key in payload:
            sets.append(f"{key} = ?")
            args.append(str(payload[key]))
    if not sets:
        return {"ok": True, "updated": 0}
    now = time.time()
    sets.append("updated_at = ?")
    args.append(now)
    args.extend([int(artifact_id), int(comment_id)])
    with hs._connect() as conn:
        cur = conn.execute(
            f"UPDATE lineage_comments SET {', '.join(sets)} WHERE artifact_id = ? AND id = ?",
            tuple(args),
        )
        updated = cur.rowcount
    return {"ok": True, "updated": int(updated or 0), "updated_at": now}


@router.delete(
    "/by-id/{artifact_id}/comments/{comment_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_comment(
    artifact_id: int,
    comment_id: int,
    _: None = Depends(require_writer_role),
) -> None:
    hs = history_store()
    if hs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised.",
        )
    with hs._connect() as conn:
        conn.execute(
            "DELETE FROM lineage_comments WHERE artifact_id = ? AND id = ?",
            (int(artifact_id), int(comment_id)),
        )


@router.post("/sql/parse")
def sql_parse(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Parse a SELECT statement into canvas-ready node/edge JSON.

    Backed by :mod:`amx.lineage.sql_bridge` which reuses the same sqlglot
    parse path the ``view_ddl`` extractor uses, so parsing behavior stays
    consistent between view inference and SQL import.
    """
    from amx.lineage import sql_bridge

    sql = str(payload.get("sql") or "").strip()
    dialect = str(payload.get("dialect") or "").strip() or None
    if not sql:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="`sql` is required.",
        )
    try:
        return sql_bridge.parse_select_to_canvas(sql, dialect=dialect)
    except sql_bridge.SqlBridgeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post("/sql/render")
def sql_render(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Render a canvas back into a SELECT statement.

    Walks the canvas's operator chain (filter/join/aggregate/projection)
    and emits a single composed SELECT. Round-trip-friendly with
    ``/api/lineage/sql/parse``.
    """
    from amx.lineage import sql_bridge

    canvas = payload.get("canvas") or {}
    dialect = str(payload.get("dialect") or "").strip() or None
    if not isinstance(canvas, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="`canvas` must be an object.",
        )
    try:
        sql = sql_bridge.render_canvas_to_sql(canvas, dialect=dialect)
    except sql_bridge.SqlBridgeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    return {"sql": sql, "dialect": dialect or "ansi"}
