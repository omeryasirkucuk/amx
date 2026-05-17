"""FK-derived lineage edges, read straight out of ``catalog_relationships``.

This extractor never touches the wire — :class:`SQLiteHistoryStore` *is*
the cache. Empty results when the target platform doesn't expose FK
metadata are treated as normal, not as errors. Edges are emitted at table
granularity; column refinements are added per-FK from ``details_json``
when ``constrained_columns`` and ``referred_columns`` are present.
"""

from __future__ import annotations

import json
from typing import Any

from amx.lineage.types import ColumnRef, Edge, ExtractMode, ExtractResult, Scope

_RELATIONSHIP_TYPE = "lineage_fk"


class FKExtractor:
    name = "fk"

    def extract(
        self,
        *,
        hs: Any,
        scope: Scope,
        mode: ExtractMode = "cache_only",
    ) -> ExtractResult:
        anchor_table_id = _resolve_anchor_table_id(hs, scope)
        if anchor_table_id is None:
            return ExtractResult()

        edges: list[Edge] = []
        with hs._connect() as conn:
            edges.extend(_outgoing_fk_edges(conn, anchor_table_id, scope))
            edges.extend(_incoming_fk_edges(conn, anchor_table_id, scope))
        return ExtractResult(edges=edges, cache_status="hit")


def _resolve_anchor_table_id(hs: Any, scope: Scope) -> int | None:
    """Return ``catalog_entities.id`` of the anchor's table row."""
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


def _outgoing_fk_edges(conn: Any, anchor_table_id: int, scope: Scope) -> list[Edge]:
    """anchor.fk_col -> referenced_table.referenced_col (anchor is upstream-consumer)."""
    rows = conn.execute(
        """
        SELECT cr.details_json, ce.database_name, ce.schema_name, ce.table_name
        FROM catalog_relationships cr
        JOIN catalog_entities ce ON ce.id = cr.to_entity_id
        WHERE cr.from_entity_id = ? AND cr.relationship_type = 'foreign_key'
        """,
        (anchor_table_id,),
    ).fetchall()
    edges: list[Edge] = []
    for raw, db, sch, tbl in rows:
        fk = _safe_json(raw)
        constrained = list(fk.get("constrained_columns") or []) if isinstance(fk, dict) else []
        referred = list(fk.get("referred_columns") or []) if isinstance(fk, dict) else []
        if constrained and referred and len(constrained) == len(referred):
            for src_col, anchor_col in zip(referred, constrained, strict=False):
                edges.append(
                    Edge(
                        source=ColumnRef(
                            database=str(db or ""),
                            schema=str(sch or ""),
                            table=str(tbl or ""),
                            column=str(src_col),
                        ),
                        target=ColumnRef(
                            database=scope.anchor.database,
                            schema=scope.anchor.schema,
                            table=scope.anchor.table,
                            column=str(anchor_col),
                        ),
                        relationship_type=_RELATIONSHIP_TYPE,
                        extractor="fk",
                        confidence=1.0,
                        evidence=f"FK {scope.anchor.table}.({','.join(constrained)}) -> {tbl}.({','.join(referred)})",
                    )
                )
        else:
            edges.append(
                Edge(
                    source=ColumnRef(
                        database=str(db or ""),
                        schema=str(sch or ""),
                        table=str(tbl or ""),
                        column="",
                    ),
                    target=ColumnRef(
                        database=scope.anchor.database,
                        schema=scope.anchor.schema,
                        table=scope.anchor.table,
                        column="",
                    ),
                    relationship_type=_RELATIONSHIP_TYPE,
                    extractor="fk",
                    confidence=1.0,
                    evidence=f"FK to {tbl}",
                )
            )
    return edges


def _incoming_fk_edges(conn: Any, anchor_table_id: int, scope: Scope) -> list[Edge]:
    """source_table.col -> anchor.col (other tables consume anchor's columns)."""
    rows = conn.execute(
        """
        SELECT cr.details_json, ce.database_name, ce.schema_name, ce.table_name
        FROM catalog_relationships cr
        JOIN catalog_entities ce ON ce.id = cr.from_entity_id
        WHERE cr.to_entity_id = ? AND cr.relationship_type = 'incoming_foreign_key'
        """,
        (anchor_table_id,),
    ).fetchall()
    edges: list[Edge] = []
    for raw, db, sch, tbl in rows:
        fk = _safe_json(raw)
        if not isinstance(fk, dict):
            fk = {}
        # incoming FK payload uses 'source_columns' / 'target_columns' in some
        # writers, 'constrained_columns' / 'referred_columns' in the standard
        # SQLAlchemy shape. Accept either.
        source_cols = list(fk.get("source_columns") or fk.get("constrained_columns") or [])
        target_cols = list(fk.get("target_columns") or fk.get("referred_columns") or [])
        if source_cols and target_cols and len(source_cols) == len(target_cols):
            for src_col, anchor_col in zip(source_cols, target_cols, strict=False):
                edges.append(
                    Edge(
                        source=ColumnRef(
                            database=scope.anchor.database,
                            schema=scope.anchor.schema,
                            table=scope.anchor.table,
                            column=str(anchor_col),
                        ),
                        target=ColumnRef(
                            database=str(db or ""),
                            schema=str(sch or ""),
                            table=str(tbl or ""),
                            column=str(src_col),
                        ),
                        relationship_type=_RELATIONSHIP_TYPE,
                        extractor="fk",
                        confidence=1.0,
                        evidence=f"FK from {tbl}.({','.join(source_cols)})",
                    )
                )
        else:
            edges.append(
                Edge(
                    source=ColumnRef(
                        database=scope.anchor.database,
                        schema=scope.anchor.schema,
                        table=scope.anchor.table,
                        column="",
                    ),
                    target=ColumnRef(
                        database=str(db or ""),
                        schema=str(sch or ""),
                        table=str(tbl or ""),
                        column="",
                    ),
                    relationship_type=_RELATIONSHIP_TYPE,
                    extractor="fk",
                    confidence=1.0,
                    evidence=f"FK from {tbl}",
                )
            )
    return edges


def _safe_json(raw: Any) -> Any:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return {}
