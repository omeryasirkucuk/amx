"""Name + type heuristic fallback.

100% cache-driven: reads ``column_comments_cache.columns_json`` for the
anchor's database and proposes edges to columns elsewhere whose
``(name, type)`` match. Edges are tagged as proposed (``confidence < 1``)
and rendered with a distinct style so they never look authoritative.
"""

from __future__ import annotations

import json
from typing import Any

from amx.lineage.types import (
    ColumnRef,
    Edge,
    ExtractMode,
    ExtractResult,
    Scope,
    ScopeFragment,
)

_MAX_PROPOSED_PER_ANCHOR = 10


class NameMatchExtractor:
    name = "name_match"

    def extract(
        self,
        *,
        hs: Any,
        scope: Scope,
        mode: ExtractMode = "cache_only",
    ) -> ExtractResult:
        anchor = scope.anchor
        if not anchor.column:
            return ExtractResult()  # heuristic only fires for column anchors

        with hs._connect() as conn:
            rows = conn.execute(
                """
                SELECT database_name, schema_name, table_name, columns_json
                FROM column_comments_cache
                WHERE db_profile = ?
                  AND database_name = ?
                """,
                (scope.profile, anchor.database),
            ).fetchall()

        if not rows:
            return ExtractResult(
                edges=[],
                cache_status="miss",
                missing_scope=[ScopeFragment(database=anchor.database, schema=anchor.schema)],
            )

        anchor_type = _resolve_anchor_type(rows, anchor)
        candidates: list[tuple[float, Edge]] = []
        for db_name, sch_name, tbl_name, cols_json in rows:
            if str(sch_name) == anchor.schema and str(tbl_name) == anchor.table:
                continue
            try:
                cols = json.loads(cols_json) if cols_json else {}
            except (TypeError, ValueError):
                cols = {}
            if not isinstance(cols, dict):
                continue
            for col_name, payload in cols.items():
                col_type = _extract_type(payload)
                conf = _score(anchor.column, col_name, anchor_type, col_type)
                if conf == 0.0:
                    continue
                candidates.append(
                    (
                        conf,
                        Edge(
                            source=ColumnRef(
                                database=str(db_name or ""),
                                schema=str(sch_name or ""),
                                table=str(tbl_name or ""),
                                column=str(col_name),
                            ),
                            target=anchor,
                            relationship_type="lineage_name_match",
                            extractor="name_match",
                            confidence=conf,
                            evidence=f"name match {col_name} ({col_type or '?'})",
                        ),
                    )
                )
        candidates.sort(key=lambda t: t[0], reverse=True)
        edges = [edge for _, edge in candidates[:_MAX_PROPOSED_PER_ANCHOR]]
        return ExtractResult(edges=edges, cache_status="hit")


def _resolve_anchor_type(rows: list[tuple], anchor: ColumnRef) -> str:
    for _, sch, tbl, cols_json in rows:
        if str(sch) != anchor.schema or str(tbl) != anchor.table:
            continue
        try:
            cols = json.loads(cols_json) if cols_json else {}
        except (TypeError, ValueError):
            return ""
        payload = cols.get(anchor.column) if isinstance(cols, dict) else None
        return _extract_type(payload)
    return ""


def _extract_type(payload: Any) -> str:
    """``columns_json`` stores either a string comment or a richer dict."""
    if isinstance(payload, dict):
        for key in ("type", "data_type", "dtype"):
            value = payload.get(key)
            if value:
                return str(value).lower()
    return ""


def _score(
    anchor_col: str,
    other_col: str,
    anchor_type: str,
    other_type: str,
) -> float:
    """Heuristic: exact name+type > name+type-family > name only > nothing."""
    if not anchor_col or not other_col:
        return 0.0
    a = anchor_col.lower()
    o = other_col.lower()
    types_match_exact = bool(anchor_type and other_type and anchor_type == other_type)
    types_match_family = _types_family_match(anchor_type, other_type)
    if a == o:
        if types_match_exact:
            return 0.6
        if types_match_family:
            return 0.5
        return 0.3
    # Suffix match: 'order_id' on `orders.id` is the canonical signal.
    if _suffix_match(a, o, types_match_family):
        if types_match_exact:
            return 0.5
        return 0.4 if types_match_family else 0.3
    return 0.0


def _suffix_match(a: str, o: str, type_family_match: bool) -> bool:
    """``order_id`` ↔ ``id`` only when types agree. Avoid spurious unrelated joins."""
    if not type_family_match:
        return False
    return a.endswith("_" + o) or o.endswith("_" + a)


_INTEGER_TYPES = {"int", "integer", "bigint", "smallint", "tinyint", "long", "int4", "int8"}
_TEXT_TYPES = {"text", "varchar", "char", "string", "nvarchar", "clob"}
_FLOAT_TYPES = {"float", "double", "real", "numeric", "decimal"}
_DATE_TYPES = {"date", "datetime", "timestamp", "timestamptz", "time"}
_BOOL_TYPES = {"bool", "boolean", "bit"}


def _types_family_match(a: str, b: str) -> bool:
    if not a or not b:
        return False
    a = a.split("(")[0].strip()
    b = b.split("(")[0].strip()
    if a == b:
        return True
    for family in (_INTEGER_TYPES, _TEXT_TYPES, _FLOAT_TYPES, _DATE_TYPES, _BOOL_TYPES):
        if a in family and b in family:
            return True
    return False


__all__ = ["NameMatchExtractor"]
