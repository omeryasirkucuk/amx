"""Auto-discover lineage edges across every cached table in a profile.

Cache-only by construction — never opens a wire connection. For each
known table entity, calls :func:`amx.lineage.service.lineage_for_studio`
with ``include_llm_cached=True`` so previously persisted LLM edges are
surfaced too. The aggregate result is returned as a list of
``DiscoveredAnchor`` rows ranked by edge count so the browse page can
surface the most lineage-rich anchors first.

Discover is intentionally synchronous in v3: cache-only extractors are
fast enough that 500 tables complete in single-digit seconds. If the
profile turns out to be 5000+ tables a future slice can move this onto
the in-process :class:`amx.web.jobs.JobRegistry` with SSE progress.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from amx.lineage.service import lineage_for_studio
from amx.lineage.types import ColumnRef, Scope

# Hard cap on how many tables we walk in one request. Anything past
# this slows the page far enough that the user perceives a hang; for
# bigger profiles surface a follow-up "scan more" affordance later.
MAX_TABLES = 500

# We only surface anchors that actually carry edges — empty results
# are noise that hides the interesting ones at the bottom of the list.
MIN_EDGES_TO_REPORT = 1


@dataclass(frozen=True)
class DiscoveredAnchor:
    """One table that has lineage signal in the cache."""

    database: str
    schema: str
    table: str
    edge_count: int
    extractors_used: list[str]
    partial: bool

    def fqn(self) -> str:
        parts = [p for p in (self.database, self.schema, self.table) if p]
        return ".".join(parts)


@dataclass
class DiscoverResult:
    """Aggregate response for the discover endpoint."""

    profile: str
    anchors: list[DiscoveredAnchor] = field(default_factory=list)
    tables_examined: int = 0
    tables_with_edges: int = 0
    total_edges: int = 0
    truncated: bool = False
    duration_sec: float = 0.0


def discover_profile_lineage(
    hs: Any,
    *,
    profile: str,
    max_tables: int = MAX_TABLES,
) -> DiscoverResult:
    """Walk cached tables for ``profile``, return ranked DiscoveredAnchor rows.

    Reads only from local SQLite — `catalog_entities` for the table
    set, then per-table `lineage_for_studio` (which fans out the
    cache-only extractors). The function does **not** persist any
    artifacts; that stays the user's choice via the Create / Open flow.
    """
    started = time.perf_counter()
    tables = _list_tables(hs, profile, max_tables)
    truncated = len(tables) == max_tables and _table_count(hs, profile) > max_tables

    anchors: list[DiscoveredAnchor] = []
    tables_with_edges = 0
    total_edges = 0
    for database, schema, table in tables:
        scope = Scope(
            profile=profile,
            anchor=ColumnRef(database=database, schema=schema, table=table, column=""),
            depth_up=1,
            depth_down=1,
            database=database,
            schema=schema,
        )
        try:
            payload = lineage_for_studio(hs=hs, scope=scope)
        except Exception:
            # Skip individual failures rather than aborting the whole
            # scan; a single broken anchor shouldn't take the page down.
            continue
        edges = payload.get("edges") or []
        edge_count = len(edges)
        if edge_count >= MIN_EDGES_TO_REPORT:
            anchors.append(
                DiscoveredAnchor(
                    database=database,
                    schema=schema,
                    table=table,
                    edge_count=edge_count,
                    extractors_used=list(payload.get("extractors_used") or []),
                    partial=bool(payload.get("partial")),
                )
            )
            tables_with_edges += 1
            total_edges += edge_count

    anchors.sort(key=lambda a: (-a.edge_count, a.schema, a.table))
    return DiscoverResult(
        profile=profile,
        anchors=anchors,
        tables_examined=len(tables),
        tables_with_edges=tables_with_edges,
        total_edges=total_edges,
        truncated=truncated,
        duration_sec=round(time.perf_counter() - started, 3),
    )


def _list_tables(hs: Any, profile: str, limit: int) -> list[tuple[str, str, str]]:
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT database_name, schema_name, table_name
            FROM catalog_entities
            WHERE db_profile = ?
              AND entity_kind = 'table'
              AND table_name <> ''
            ORDER BY schema_name, table_name
            LIMIT ?
            """,
            (profile, limit),
        ).fetchall()
    return [(str(d or ""), str(s or ""), str(t or "")) for d, s, t in rows]


def _table_count(hs: Any, profile: str) -> int:
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM catalog_entities
            WHERE db_profile = ? AND entity_kind = 'table' AND table_name <> ''
            """,
            (profile,),
        ).fetchone()
    return int(row[0]) if row else 0


__all__ = [
    "DiscoveredAnchor",
    "DiscoverResult",
    "MAX_TABLES",
    "discover_profile_lineage",
]
