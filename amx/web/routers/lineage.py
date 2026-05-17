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


# ── v4 — column-level operator nodes ─────────────────────────────────────


@router.post("/operators", status_code=status.HTTP_201_CREATED)
def post_operator(
    payload: dict[str, Any] = Body(...),
    cfg: AMXConfig = Depends(get_cfg),
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
    from amx.lineage.operator_ops import write_column_edge

    anchor_id = _resolve_entity_id_strict(hs, profile, anchor_fqn)
    actor = _actor_name()
    now = time.time()
    persisted = 0
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
        _src_db, _src_schema, _src_table, src_col_from_fqn = _split_fqn_resolve_column(
            hs, profile, src
        )
        _tgt_db, _tgt_schema, _tgt_table, tgt_col_from_fqn = _split_fqn_resolve_column(
            hs, profile, tgt
        )
        src_col = str(edge.get("source_column") or src_col_from_fqn or "").strip()
        tgt_col = str(edge.get("target_column") or tgt_col_from_fqn or "").strip()
        details = {"actor": actor, "ts": now, "via": "manual_canvas"}
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
