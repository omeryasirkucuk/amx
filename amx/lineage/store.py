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
]
