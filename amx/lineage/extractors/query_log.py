"""Query-log extractor.

Mines AMX's local query history for tables that get touched together. The
plan originally called for sqlglot parsing of stored SQL strings, but a
look at the schema showed AMX does not persist raw SQL — only the
``scope_json`` describing which tables a run intentionally targeted, and
the ``chat_turns.tables_json`` listing tables an ``/ask`` answer
mentioned. Those two together are still real lineage signal: tables that
co-occur in a single run are likely related, even when FK + ViewDDL find
nothing.

This extractor stays cache-only by construction — every read is against
the local SQLite ``history.db``. No DB round-trip ever, so the
``mode="db_fill"`` branch is a no-op (kept for protocol compliance).
"""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from amx.lineage.types import (
    ColumnRef,
    Edge,
    ExtractMode,
    ExtractResult,
    Scope,
)

# How many recent runs / chat turns to scan. Capped so the extractor stays
# cheap even on a heavily-used history.db. Anything older than this is
# unlikely to reflect current lineage practice anyway.
_MAX_RUNS = 500
_MAX_TURNS = 500

# A co-occurrence needs to repeat this many times before it becomes an
# edge. One-off joins are noise; recurring joins are signal.
_MIN_CO_OCCURRENCE = 2

# Edge confidence floor + slope: 0.3 at the minimum count, +0.05 per
# extra occurrence, capped at 0.7. Keeps these edges visibly weaker
# than deterministic ones (FK / view DDL = 1.0).
_CONFIDENCE_FLOOR = 0.3
_CONFIDENCE_STEP = 0.05
_CONFIDENCE_CEILING = 0.7


class QueryLogExtractor:
    name = "query_log"

    def extract(
        self,
        *,
        hs: Any,
        scope: Scope,
        mode: ExtractMode = "cache_only",
    ) -> ExtractResult:
        anchor = scope.anchor
        if not anchor.schema or not anchor.table:
            return ExtractResult()

        co_counts = Counter()
        with hs._connect() as conn:
            co_counts.update(_co_occurring_from_runs(conn, scope))
            co_counts.update(_co_occurring_from_chat_turns(conn, scope))

        edges = list(_edges_from_counter(co_counts, scope))
        return ExtractResult(edges=edges, cache_status="hit")


def _co_occurring_from_runs(conn: Any, scope: Scope) -> Counter:
    """Mine analysis_runs.scope_json for tables that appeared with the anchor."""
    counts: Counter = Counter()
    rows = conn.execute(
        """
        SELECT scope_json
        FROM analysis_runs
        WHERE db_profile = ?
          AND scope_json IS NOT NULL AND scope_json <> ''
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (scope.profile, _MAX_RUNS),
    ).fetchall()
    anchor_key = _scope_key(scope.anchor)
    for (raw,) in rows:
        for other_key in _other_table_keys(raw, anchor_key):
            counts[other_key] += 1
    return counts


def _co_occurring_from_chat_turns(conn: Any, scope: Scope) -> Counter:
    """Mine chat_turns.tables_json for the same signal in /ask flows."""
    counts: Counter = Counter()
    try:
        rows = conn.execute(
            """
            SELECT t.tables_json
            FROM chat_turns t
            JOIN chat_sessions s ON s.id = t.session_id
            WHERE s.db_profile = ?
              AND t.tables_json IS NOT NULL AND t.tables_json <> '[]'
            ORDER BY t.created_at DESC
            LIMIT ?
            """,
            (scope.profile, _MAX_TURNS),
        ).fetchall()
    except Exception:
        # chat_sessions may carry a different column on older installs;
        # fail gracefully so an upgrade doesn't break /lineage.
        return counts
    anchor_key = _scope_key(scope.anchor)
    for (raw,) in rows:
        for other_key in _other_table_keys(raw, anchor_key):
            counts[other_key] += 1
    return counts


def _scope_key(ref: ColumnRef) -> str:
    """Canonical (schema, table) key. Database is dropped because most
    AMX history rows don't carry it; co-occurrence within a profile is
    the useful unit."""
    return f"{ref.schema}.{ref.table}"


def _other_table_keys(raw_json: str, anchor_key: str) -> list[str]:
    """Pull table keys out of a scope_json / tables_json blob,
    excluding the anchor itself.

    Tolerates several shapes seen in the wild:
    * ``{"schemas": {"public": ["orders", "customers"]}, ...}``
    * ``{"tables": [{"schema": "public", "name": "orders"}, ...]}``
    * ``[{"schema": "public", "table": "orders"}, ...]``  (chat_turns)
    * ``["public.orders", "public.customers"]``
    """
    try:
        payload = json.loads(raw_json)
    except (TypeError, ValueError):
        return []
    candidates: list[str] = []
    candidates.extend(_extract_from_schemas_dict(payload))
    candidates.extend(_extract_from_tables_list(payload))
    candidates.extend(_extract_from_string_list(payload))
    # Dedupe and drop the anchor.
    seen: set[str] = set()
    out: list[str] = []
    for key in candidates:
        if not key or key == anchor_key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    # If the anchor itself never appears in this blob, the run/turn is
    # not relevant — return nothing so we don't count unrelated runs.
    if anchor_key not in candidates:
        return []
    return out


def _extract_from_schemas_dict(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return []
    schemas = payload.get("schemas")
    if not isinstance(schemas, dict):
        return []
    out: list[str] = []
    for schema, tables in schemas.items():
        if not isinstance(tables, list):
            continue
        for tbl in tables:
            if isinstance(tbl, str):
                out.append(f"{schema}.{tbl}")
    return out


def _extract_from_tables_list(payload: Any) -> list[str]:
    container = payload
    if isinstance(payload, dict):
        container = payload.get("tables")
    if not isinstance(container, list):
        return []
    out: list[str] = []
    for entry in container:
        if not isinstance(entry, dict):
            continue
        schema = str(entry.get("schema") or entry.get("schema_name") or "")
        table = str(entry.get("name") or entry.get("table") or entry.get("table_name") or "")
        if schema and table:
            out.append(f"{schema}.{table}")
    return out


def _extract_from_string_list(payload: Any) -> list[str]:
    if not isinstance(payload, list):
        return []
    out: list[str] = []
    for entry in payload:
        if isinstance(entry, str) and "." in entry:
            out.append(entry)
    return out


def _edges_from_counter(counts: Counter, scope: Scope) -> list[Edge]:
    edges: list[Edge] = []
    for key, count in counts.items():
        if count < _MIN_CO_OCCURRENCE:
            continue
        schema, _, table = key.partition(".")
        if not schema or not table:
            continue
        confidence = min(
            _CONFIDENCE_CEILING,
            _CONFIDENCE_FLOOR + (count - _MIN_CO_OCCURRENCE) * _CONFIDENCE_STEP,
        )
        other = ColumnRef(
            database=scope.anchor.database,
            schema=schema,
            table=table,
            column="",
        )
        # Always emit anchor → other so the render layer has a stable
        # endpoint mapping. Co-occurrence is conceptually undirected; the
        # renderer styles these as dashed so the visual reads "related"
        # rather than "feeds".
        edges.append(
            Edge(
                source=scope.anchor,
                target=other,
                relationship_type="lineage_co_occurs",
                extractor="query_log",
                confidence=confidence,
                evidence=f"co-occurred {count}× in recent history",
            )
        )
    return edges


__all__ = ["QueryLogExtractor"]
