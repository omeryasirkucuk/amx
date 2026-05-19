"""SQLite-backed cache for view DDLs + lineage artifact registry.

Follows the pattern of :mod:`amx.storage._history_caches` — each function
takes the ``SQLiteHistoryStore`` as ``hs`` and reuses its lock + connect
plumbing. No DDL touched here; tables are created by
``SQLiteHistoryStore.init()``.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.storage.sqlite_store import SQLiteHistoryStore

DEFAULT_VIEW_CACHE_TTL_SECONDS = 1800.0  # 30 min, matches column_comments_cache default


def _view_cache_key(*, db_profile: str, database: str, schema: str, view: str) -> str:
    return f"{db_profile}|{database or ''}|{schema}|{view}"


def lookup_view_definitions(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    schema: str,
) -> list[dict[str, Any]]:
    """Return fresh cached view rows for the given ``(profile, database, schema)``.

    Rows with ``expires_at < now`` are skipped — callers treat their absence
    as a cache miss for that schema. The returned dicts have keys
    ``view_name``, ``ddl_text``, ``dialect``, ``parsed_lineage`` (decoded
    JSON or ``None``), ``parse_status``, ``parse_error``, ``fetched_at``,
    ``expires_at``.
    """
    now = time.time()
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT view_name, ddl_text, dialect, parsed_lineage_json,
                   parse_status, parse_error, fetched_at, expires_at
            FROM view_definitions_cache
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND expires_at >= ?
            ORDER BY view_name
            """,
            (str(db_profile or ""), str(database or ""), str(schema or ""), now),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        parsed: Any | None
        try:
            parsed = json.loads(r[3]) if r[3] else None
        except (TypeError, ValueError):
            parsed = None
        out.append(
            {
                "view_name": str(r[0]),
                "ddl_text": str(r[1]),
                "dialect": str(r[2]),
                "parsed_lineage": parsed,
                "parse_status": str(r[4]),
                "parse_error": str(r[5] or ""),
                "fetched_at": float(r[6]),
                "expires_at": float(r[7]),
            }
        )
    return out


def upsert_view_definitions(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str,
    schema: str,
    entries: Iterable[dict[str, Any]],
    ttl_seconds: float = DEFAULT_VIEW_CACHE_TTL_SECONDS,
) -> int:
    """Upsert per-view rows after a ``db_fill`` round-trip.

    Each entry should carry: ``view_name`` (str), ``ddl_text`` (str),
    ``dialect`` (str), ``parsed_lineage`` (any JSON-serialisable | None),
    ``parse_status`` (str), ``parse_error`` (str, may be empty).
    """
    rows: list[tuple[Any, ...]] = []
    now = time.time()
    # ``0`` is the "use default" sentinel; negative TTLs are honoured so
    # tests can stamp rows as already-expired.
    if ttl_seconds == 0:
        ttl_seconds = DEFAULT_VIEW_CACHE_TTL_SECONDS
    expires_at = now + float(ttl_seconds)
    for e in entries:
        view = str(e.get("view_name") or "")
        if not view:
            continue
        parsed_payload = e.get("parsed_lineage")
        parsed_json = (
            json.dumps(parsed_payload, ensure_ascii=True) if parsed_payload is not None else None
        )
        rows.append(
            (
                _view_cache_key(
                    db_profile=str(db_profile or ""),
                    database=str(database or ""),
                    schema=str(schema or ""),
                    view=view,
                ),
                str(db_profile or ""),
                str(database or ""),
                str(schema or ""),
                view,
                str(e.get("ddl_text") or ""),
                str(e.get("dialect") or ""),
                parsed_json,
                str(e.get("parse_status") or "ok"),
                str(e.get("parse_error") or ""),
                now,
                expires_at,
            )
        )
    if not rows:
        return 0
    with hs._lock, hs._connect() as conn:
        conn.executemany(
            """
            INSERT INTO view_definitions_cache
                (cache_key, db_profile, database_name, schema_name, view_name,
                 ddl_text, dialect, parsed_lineage_json,
                 parse_status, parse_error, fetched_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                ddl_text            = excluded.ddl_text,
                dialect             = excluded.dialect,
                parsed_lineage_json = excluded.parsed_lineage_json,
                parse_status        = excluded.parse_status,
                parse_error         = excluded.parse_error,
                fetched_at          = excluded.fetched_at,
                expires_at          = excluded.expires_at
            """,
            rows,
        )
    return len(rows)


def invalidate_view_definitions(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str,
    database: str = "",
    schema: str = "",
) -> int:
    """Delete cached view rows. Empty ``schema`` clears the whole database;
    empty ``database`` clears the whole profile.
    """
    with hs._lock, hs._connect() as conn:
        if schema:
            cur = conn.execute(
                "DELETE FROM view_definitions_cache "
                "WHERE db_profile = ? AND database_name = ? AND schema_name = ?",
                (str(db_profile or ""), str(database or ""), str(schema or "")),
            )
        elif database:
            cur = conn.execute(
                "DELETE FROM view_definitions_cache WHERE db_profile = ? AND database_name = ?",
                (str(db_profile or ""), str(database or "")),
            )
        else:
            cur = conn.execute(
                "DELETE FROM view_definitions_cache WHERE db_profile = ?",
                (str(db_profile or ""),),
            )
    return cur.rowcount or 0


def gc_view_definitions(hs: SQLiteHistoryStore) -> int:
    """Drop expired view-cache rows. Returns the deleted count."""
    now = time.time()
    with hs._lock, hs._connect() as conn:
        cur = conn.execute("DELETE FROM view_definitions_cache WHERE expires_at < ?", (now,))
    return cur.rowcount or 0


# ── lineage_artifacts ────────────────────────────────────────────────────


def compute_edge_set_hash(edges: Iterable[tuple[int, int, str, float]]) -> str:
    """Stable hash of the edge set that backs a rendered artifact.

    Each tuple is ``(from_entity_id, to_entity_id, relationship_type, score)``.
    Inputs are sorted before hashing so two callers with identical edge
    contents always agree.
    """
    materialised = sorted((int(a), int(b), str(c), round(float(d), 6)) for a, b, c, d in edges)
    payload = json.dumps(materialised, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def insert_lineage_artifact(
    hs: SQLiteHistoryStore,
    *,
    name: str,
    db_profile: str,
    anchor_entity_id: int,
    depth_up: int,
    depth_down: int,
    fmt: str,
    output_path: str,
    edge_set_hash: str,
    node_count: int,
    edge_count: int,
    extractors_used: list[str],
    extractors_partial: bool,
) -> int:
    """Insert one ``lineage_artifacts`` row. Returns the new row id."""
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO lineage_artifacts
                (name, db_profile, anchor_entity_id, depth_up, depth_down,
                 format, output_path, edge_set_hash, node_count, edge_count,
                 generated_at, extractors_used, extractors_partial)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(name),
                str(db_profile or ""),
                int(anchor_entity_id),
                int(depth_up),
                int(depth_down),
                str(fmt),
                str(output_path),
                str(edge_set_hash),
                int(node_count),
                int(edge_count),
                time.time(),
                json.dumps(sorted(set(extractors_used)), ensure_ascii=True),
                1 if extractors_partial else 0,
            ),
        )
    return int(cur.lastrowid or 0)


def update_lineage_artifact(
    hs: SQLiteHistoryStore,
    *,
    artifact_id: int,
    edge_set_hash: str,
    node_count: int,
    edge_count: int,
    extractors_used: list[str],
    extractors_partial: bool,
    output_path: str | None = None,
    fmt: str | None = None,
) -> None:
    """Refresh artifact metadata after a successful re-render."""
    sets = [
        "edge_set_hash = ?",
        "node_count = ?",
        "edge_count = ?",
        "generated_at = ?",
        "extractors_used = ?",
        "extractors_partial = ?",
    ]
    args: list[Any] = [
        str(edge_set_hash),
        int(node_count),
        int(edge_count),
        time.time(),
        json.dumps(sorted(set(extractors_used)), ensure_ascii=True),
        1 if extractors_partial else 0,
    ]
    if output_path is not None:
        sets.append("output_path = ?")
        args.append(str(output_path))
    if fmt is not None:
        sets.append("format = ?")
        args.append(str(fmt))
    args.append(int(artifact_id))
    with hs._lock, hs._connect() as conn:
        conn.execute(
            f"UPDATE lineage_artifacts SET {', '.join(sets)} WHERE id = ?",
            args,
        )


def delete_lineage_artifact(hs: SQLiteHistoryStore, *, artifact_id: int) -> None:
    with hs._lock, hs._connect() as conn:
        conn.execute("DELETE FROM lineage_artifacts WHERE id = ?", (int(artifact_id),))


def lookup_lineage_artifact(
    hs: SQLiteHistoryStore,
    *,
    name_or_id: str,
) -> dict[str, Any] | None:
    """Resolve an artifact by numeric id or by ``name``. Returns ``None`` if absent."""
    with hs._connect() as conn:
        row = None
        if name_or_id.isdigit():
            row = conn.execute(_ARTIFACT_SELECT_BY_ID, (int(name_or_id),)).fetchone()
        if row is None:
            row = conn.execute(_ARTIFACT_SELECT_BY_NAME, (str(name_or_id),)).fetchone()
    return _row_to_artifact_dict(row) if row else None


def list_lineage_artifacts(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str = "",
) -> list[dict[str, Any]]:
    """List artifacts. When ``db_profile`` is empty, returns every row."""
    with hs._connect() as conn:
        if db_profile:
            rows = conn.execute(
                _ARTIFACT_SELECT_LIST + " WHERE db_profile = ? ORDER BY generated_at DESC",
                (str(db_profile),),
            ).fetchall()
        else:
            rows = conn.execute(_ARTIFACT_SELECT_LIST + " ORDER BY generated_at DESC").fetchall()
    return [_row_to_artifact_dict(r) for r in rows]


_ARTIFACT_COLUMNS = (
    "id, name, db_profile, anchor_entity_id, depth_up, depth_down, "
    "format, output_path, edge_set_hash, node_count, edge_count, "
    "generated_at, extractors_used, extractors_partial"
)
_ARTIFACT_SELECT_BY_ID = f"SELECT {_ARTIFACT_COLUMNS} FROM lineage_artifacts WHERE id = ?"
_ARTIFACT_SELECT_BY_NAME = f"SELECT {_ARTIFACT_COLUMNS} FROM lineage_artifacts WHERE name = ?"
_ARTIFACT_SELECT_LIST = f"SELECT {_ARTIFACT_COLUMNS} FROM lineage_artifacts"


def _row_to_artifact_dict(row: Any) -> dict[str, Any]:
    extractors_raw = row[12]
    try:
        extractors = list(json.loads(extractors_raw)) if extractors_raw else []
    except (TypeError, ValueError):
        extractors = []
    return {
        "id": int(row[0]),
        "name": str(row[1]),
        "db_profile": str(row[2] or ""),
        "anchor_entity_id": int(row[3]),
        "depth_up": int(row[4]),
        "depth_down": int(row[5]),
        "format": str(row[6]),
        "output_path": str(row[7]),
        "edge_set_hash": str(row[8]),
        "node_count": int(row[9]),
        "edge_count": int(row[10]),
        "generated_at": float(row[11]),
        "extractors_used": extractors,
        "extractors_partial": bool(int(row[13] or 0)),
    }


def list_artifact_edges(
    hs: SQLiteHistoryStore,
    *,
    artifact: dict[str, Any],
    limit: int = 200,
) -> dict[str, Any]:
    """Materialise the edges + nodes covered by a lineage artifact.

    ``lineage_artifacts`` rows store the anchor + depth + extractor
    set used to render the diagram, but the actual edges live in
    ``catalog_relationships``. This helper walks the relationship
    graph from the anchor with the artifact's depth limits, scopes
    to the artifact's extractors, and joins ``catalog_entities`` for
    human-readable paths and ``entity_kind`` labels (so non-table
    nodes — reports, dashboards, files, operator nodes — round-trip
    too).

    Returns ``{"edges": [...], "nodes": [...], "truncated": bool}``
    where each edge carries ``from_path``, ``from_kind``,
    ``from_column``, ``to_path``, ``to_kind``, ``to_column``,
    ``relationship_type``, ``source``, ``score``; each node carries
    ``path`` and ``kind``. The list is capped at ``limit`` rows
    (default 200) so a sprawling graph cannot blow the composer
    context budget.
    """
    anchor_id = int(artifact.get("anchor_entity_id", 0) or 0)
    depth_up = int(artifact.get("depth_up", 0) or 0)
    depth_down = int(artifact.get("depth_down", 0) or 0)
    extractors = [str(e) for e in (artifact.get("extractors_used") or [])]

    if anchor_id <= 0:
        return {"edges": [], "nodes": [], "truncated": False}

    # Marker that means "no extractor filter" — when the artifact has
    # an empty extractor list we still want to surface the graph.
    if extractors:
        placeholder = ",".join(["?"] * len(extractors))
        source_clause_up = f"AND r.source IN ({placeholder})"
        source_clause_down = f"AND r.source IN ({placeholder})"
        source_clause_edges = f"AND r.source IN ({placeholder})"
        extractor_args_up: tuple[Any, ...] = tuple(extractors)
        extractor_args_down: tuple[Any, ...] = tuple(extractors)
        extractor_args_edges: tuple[Any, ...] = tuple(extractors)
    else:
        source_clause_up = source_clause_down = source_clause_edges = ""
        extractor_args_up = extractor_args_down = extractor_args_edges = ()

    sql = f"""
    WITH RECURSIVE
    upstream(eid, depth) AS (
        SELECT ?, 0
        UNION ALL
        SELECT r.from_entity_id, u.depth + 1
        FROM catalog_relationships r
        JOIN upstream u ON r.to_entity_id = u.eid
        WHERE u.depth < ?
          {source_clause_up}
    ),
    downstream(eid, depth) AS (
        SELECT ?, 0
        UNION ALL
        SELECT r.to_entity_id, d.depth + 1
        FROM catalog_relationships r
        JOIN downstream d ON r.from_entity_id = d.eid
        WHERE d.depth < ?
          {source_clause_down}
    ),
    visited AS (
        SELECT eid FROM upstream
        UNION
        SELECT eid FROM downstream
    )
    SELECT
        r.from_entity_id, r.to_entity_id, r.relationship_type,
        r.source, r.from_column, r.to_column, r.score,
        fe.db_profile, fe.database_name, fe.schema_name,
        fe.table_name, fe.column_name, fe.entity_kind,
        te.db_profile, te.database_name, te.schema_name,
        te.table_name, te.column_name, te.entity_kind
    FROM catalog_relationships r
    JOIN catalog_entities fe ON fe.id = r.from_entity_id
    JOIN catalog_entities te ON te.id = r.to_entity_id
    WHERE r.from_entity_id IN (SELECT eid FROM visited)
      AND r.to_entity_id   IN (SELECT eid FROM visited)
      {source_clause_edges}
    ORDER BY r.from_entity_id, r.to_entity_id
    LIMIT ?
    """

    params: list[Any] = []
    params.append(anchor_id)
    params.append(depth_up)
    params.extend(extractor_args_up)
    params.append(anchor_id)
    params.append(depth_down)
    params.extend(extractor_args_down)
    params.extend(extractor_args_edges)
    params.append(int(limit) + 1)  # fetch one extra to detect truncation

    edges: list[dict[str, Any]] = []
    seen_nodes: dict[int, dict[str, Any]] = {}
    with hs._connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    truncated = len(rows) > limit
    for row in rows[:limit]:
        (
            from_id,
            to_id,
            rel_type,
            source,
            from_column,
            to_column,
            score,
            f_prof,
            f_db,
            f_sch,
            f_tab,
            f_col,
            f_kind,
            t_prof,
            t_db,
            t_sch,
            t_tab,
            t_col,
            t_kind,
        ) = row

        from_path = _format_entity_path(f_prof, f_db, f_sch, f_tab, f_col or from_column)
        to_path = _format_entity_path(t_prof, t_db, t_sch, t_tab, t_col or to_column)
        edges.append(
            {
                "from_id": int(from_id),
                "to_id": int(to_id),
                "from_path": from_path,
                "from_kind": str(f_kind or "table"),
                "from_column": str(from_column or ""),
                "to_path": to_path,
                "to_kind": str(t_kind or "table"),
                "to_column": str(to_column or ""),
                "relationship_type": str(rel_type or ""),
                "source": str(source or ""),
                "score": float(score or 0.0),
            }
        )
        seen_nodes.setdefault(
            int(from_id),
            {"path": from_path, "kind": str(f_kind or "table")},
        )
        seen_nodes.setdefault(
            int(to_id),
            {"path": to_path, "kind": str(t_kind or "table")},
        )

    return {
        "edges": edges,
        "nodes": list(seen_nodes.values()),
        "truncated": truncated,
    }


def _format_entity_path(
    db_profile: Any,
    database: Any,
    schema: Any,
    table: Any,
    column: Any,
) -> str:
    """Build a compact display path for an entity row.

    Drops empty segments. Column suffix is dot-joined when present.
    Examples:
      ('prod', '', 'sales', 'orders', '')        -> 'prod/sales/orders'
      ('prod', 'main', 'sales', 'orders', 'id')  -> 'prod/main/sales/orders.id'
      ('pbi', '', '', 'Revenue Dashboard', '')   -> 'pbi/Revenue Dashboard'
    """
    parts = [str(p) for p in (db_profile, database, schema, table) if str(p or "")]
    base = "/".join(parts)
    col = str(column or "")
    return f"{base}.{col}" if col else base


__all__ = [
    "DEFAULT_VIEW_CACHE_TTL_SECONDS",
    "lookup_view_definitions",
    "upsert_view_definitions",
    "invalidate_view_definitions",
    "gc_view_definitions",
    "compute_edge_set_hash",
    "insert_lineage_artifact",
    "update_lineage_artifact",
    "delete_lineage_artifact",
    "lookup_lineage_artifact",
    "list_lineage_artifacts",
    "list_artifact_edges",
]
