"""Surface user-authored manual edges into the studio payload.

Persisted via :func:`amx.web.routers.lineage.post_edge` (drag-to-
connect) or :func:`post_manual_artifact` (save canvas). The extractor
reads them back from ``catalog_relationships`` with their stable
``id`` so the canvas can DELETE / PATCH per-row.

Cache-only by construction: only reads, never writes.
"""

from __future__ import annotations

from typing import Any

from amx.lineage.types import ColumnRef, Edge, ExtractMode, ExtractResult, Scope


class ManualEdgeExtractor:
    name = "manual"

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
        anchor_id = _resolve_anchor_id(hs, scope)
        if anchor_id is None:
            return ExtractResult()
        with hs._connect() as conn:
            rows = conn.execute(
                """
                SELECT cr.id, cr.from_entity_id, cr.to_entity_id, cr.score, cr.verdict,
                       src.schema_name, src.table_name,
                       tgt.schema_name, tgt.table_name
                FROM catalog_relationships cr
                JOIN catalog_entities src ON src.id = cr.from_entity_id
                JOIN catalog_entities tgt ON tgt.id = cr.to_entity_id
                WHERE cr.relationship_type = 'lineage_manual'
                  AND (cr.from_entity_id = ? OR cr.to_entity_id = ?)
                """,
                (anchor_id, anchor_id),
            ).fetchall()
        edges: list[Edge] = []
        for row in rows:
            edges.append(
                Edge(
                    source=ColumnRef(
                        database=scope.anchor.database,
                        schema=str(row[5]),
                        table=str(row[6]),
                        column="",
                    ),
                    target=ColumnRef(
                        database=scope.anchor.database,
                        schema=str(row[7]),
                        table=str(row[8]),
                        column="",
                    ),
                    relationship_type="lineage_manual",
                    extractor="manual",
                    confidence=float(row[3] or 1.0),
                    evidence="user-authored",
                    db_id=int(row[0]),
                    verdict=str(row[4] or "approved"),
                )
            )
        return ExtractResult(edges=edges, cache_status="hit")


def _resolve_anchor_id(hs: Any, scope: Scope) -> int | None:
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT id FROM catalog_entities
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND table_name = ? AND entity_kind = 'table'
            LIMIT 1
            """,
            (
                scope.profile,
                scope.anchor.database,
                scope.anchor.schema,
                scope.anchor.table,
            ),
        ).fetchone()
    return int(row[0]) if row else None


__all__ = ["ManualEdgeExtractor"]
