"""Lineage tools for :class:`ToolBox`.

``lineage_for_table`` and ``lineage_for_column`` walk the
``catalog_relationships`` graph so the /ask agent can answer
"what is upstream of X" / "which assets reference Y" without
needing the user to open the lineage canvas.

Both tools read from the local SQLite history store and are
``freshness="cache_ok"``. Walk depth is capped to keep result size
within token budget.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.config import AMXConfig


# Relationship types that show meaningful lineage edges. ``join_inference``
# is excluded by default because it carries thousands of speculative
# edges; the LLM can pass ``include_inferred=true`` to widen the query
# when the user explicitly asks about join paths.
_LINEAGE_RELATIONSHIP_TYPES: tuple[str, ...] = (
    "asset_references_table",
    "foreign_key",
    "view_depends_on",
    "column_lineage",
)


def _resolve_entity_id(
    conn: Any,
    *,
    profile: str,
    schema: str,
    table: str,
    column: str | None = None,
) -> int | None:
    """Resolve a (profile, schema, table[, column]) tuple to an entity id.

    Returns the catalog_entities.id of the matching table row when
    ``column`` is ``None``, or the column row when ``column`` is set.
    Returns ``None`` when the lookup misses — the caller surfaces the
    miss back to the LLM so it can suggest a name fix.
    """
    if column is None:
        row = conn.execute(
            """
            SELECT id FROM catalog_entities
            WHERE db_profile = ? AND schema_name = ? AND table_name = ?
              AND entity_kind = 'table'
            LIMIT 1
            """,
            (profile, schema, table),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT id FROM catalog_entities
            WHERE db_profile = ? AND schema_name = ? AND table_name = ?
              AND column_name = ? AND entity_kind = 'column'
            LIMIT 1
            """,
            (profile, schema, table, column),
        ).fetchone()
    return int(row[0]) if row else None


def _format_edge(
    *,
    other_kind: str,
    other_name: str,
    other_schema: str,
    other_profile: str,
    other_column: str,
    relationship_type: str,
    direction: str,
    source_remote_id: int | None,
) -> dict[str, Any]:
    """Build the per-edge dict the LLM consumes.

    ``direction`` is ``"upstream"`` or ``"downstream"`` from the
    perspective of the resolved entity (not the edge itself).
    """
    return {
        "direction": direction,
        "relationship_type": relationship_type,
        "other_kind": other_kind,
        "other_profile": other_profile,
        "other_schema": other_schema,
        "other_name": other_name,
        "other_column": other_column,
        "other_source_remote_id": source_remote_id,
    }


def _fetch_edges(
    conn: Any,
    *,
    entity_id: int,
    direction: str,
    rel_types: tuple[str, ...],
    column_grain: bool,
    limit: int,
) -> list[dict[str, Any]]:
    """Read one direction's edges and join the other side's metadata."""
    types_placeholder = ",".join("?" for _ in rel_types)
    if direction == "upstream":
        side_filter = "r.to_entity_id = ?"
        other_id_col = "r.from_entity_id"
        own_column_col = "r.to_column"
        other_column_col = "r.from_column"
        other_kind_col = "r.from_entity_kind"
    else:
        side_filter = "r.from_entity_id = ?"
        other_id_col = "r.to_entity_id"
        own_column_col = "r.from_column"
        other_column_col = "r.to_column"
        other_kind_col = "r.to_entity_kind"

    # Column-grain filter narrows to edges where at least one side
    # carries a non-empty column name (v4 schema feature). The
    # caller resolves the entity_id at column granularity, which is
    # already enough to scope down — but we still ask for the column
    # field so the response carries it.
    column_clause = ""
    if column_grain:
        column_clause = f" AND {own_column_col} != ''"

    rows = conn.execute(
        f"""
        SELECT
            {other_kind_col} AS other_kind,
            {other_column_col} AS other_column,
            ce.db_profile AS other_profile,
            ce.schema_name AS other_schema,
            COALESCE(ce.table_name, ce.name, '') AS other_name,
            ce.source_remote_id AS source_remote_id,
            r.relationship_type AS rel_type
        FROM catalog_relationships r
        JOIN catalog_entities ce ON ce.id = {other_id_col}
        WHERE {side_filter}
          AND r.relationship_type IN ({types_placeholder})
          {column_clause}
        LIMIT ?
        """,
        (entity_id, *rel_types, limit),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        other_kind = str(r[0] or "table")
        out.append(
            _format_edge(
                other_kind=other_kind,
                other_name=str(r[4] or ""),
                other_schema=str(r[3] or ""),
                other_profile=str(r[2] or ""),
                other_column=str(r[1] or ""),
                relationship_type=str(r[6] or ""),
                direction=direction,
                source_remote_id=int(r[5]) if r[5] is not None else None,
            )
        )
    return out


class _LineageToolsMixin:
    """Lineage-graph tools for the /ask tool-calling agent."""

    # Provided by the host ``ToolBox`` instance.
    cfg: AMXConfig
    db_profiles: list[str]

    def _tool_lineage_for_table(
        self,
        schema: str,
        table: str,
        profile: str | None = None,
        direction: str = "both",
        include_inferred: bool = False,
        limit: int = 30,
    ) -> dict[str, Any]:
        """Return lineage edges anchored on a single table.

        ``direction`` is ``"upstream"``, ``"downstream"``, or
        ``"both"``. ``profile`` is required when more than one
        profile is in scope; otherwise the single in-scope profile
        is used by default.
        """
        return self._lineage_for(
            profile=profile,
            schema=schema,
            table=table,
            column=None,
            direction=direction,
            include_inferred=include_inferred,
            limit=limit,
        )

    def _tool_lineage_for_column(
        self,
        schema: str,
        table: str,
        column: str,
        profile: str | None = None,
        direction: str = "both",
        include_inferred: bool = False,
        limit: int = 30,
    ) -> dict[str, Any]:
        """Return column-grain lineage edges anchored on a single column.

        Uses the v4 ``from_column`` / ``to_column`` schema columns
        plus any operator-node intermediaries
        (``entity_kind='operator'`` rows) to surface the transform
        logic between source and target columns.
        """
        return self._lineage_for(
            profile=profile,
            schema=schema,
            table=table,
            column=column,
            direction=direction,
            include_inferred=include_inferred,
            limit=limit,
        )

    def _lineage_for(
        self,
        *,
        profile: str | None,
        schema: str,
        table: str,
        column: str | None,
        direction: str,
        include_inferred: bool,
        limit: int,
    ) -> dict[str, Any]:
        sch = (schema or "").strip()
        tbl = (table or "").strip()
        if not sch or not tbl:
            return {"error": "schema and table are required"}
        col = (column or "").strip() or None

        resolved_profile = (profile or "").strip()
        if not resolved_profile:
            if len(self.db_profiles) == 1:
                resolved_profile = self.db_profiles[0]
            else:
                return {
                    "error": "profile is required when more than one profile is in scope",
                    "scope_dbs": list(self.db_profiles),
                }
        if resolved_profile not in self.db_profiles:
            return {
                "error": f"Profile {resolved_profile!r} is not in scope",
                "scope_dbs": list(self.db_profiles),
            }

        normalized_direction = direction.lower().strip()
        if normalized_direction not in {"upstream", "downstream", "both"}:
            return {
                "error": f"direction must be upstream|downstream|both, got {direction!r}",
            }

        rel_types: tuple[str, ...] = _LINEAGE_RELATIONSHIP_TYPES
        if include_inferred:
            rel_types = rel_types + ("join_inference",)

        capped_limit = max(1, min(int(limit or 30), 100))

        from amx.storage.sqlite_store import history_store

        store = history_store()
        if store is None:
            return {"error": "no_history_store"}

        with store._connect() as conn:  # noqa: SLF001
            entity_id = _resolve_entity_id(
                conn,
                profile=resolved_profile,
                schema=sch,
                table=tbl,
                column=col,
            )
            if entity_id is None:
                missing_label = f"column {col!r} on {sch}.{tbl}" if col else f"table {sch}.{tbl}"
                return {
                    "error": f"No catalog entity found for {missing_label} in profile {resolved_profile!r}",
                    "profile": resolved_profile,
                    "hint": (
                        "Confirm the schema and table names with list_tables_in_schema "
                        "or describe_table before retrying."
                    ),
                }
            edges: list[dict[str, Any]] = []
            if normalized_direction in {"upstream", "both"}:
                edges.extend(
                    _fetch_edges(
                        conn,
                        entity_id=entity_id,
                        direction="upstream",
                        rel_types=rel_types,
                        column_grain=col is not None,
                        limit=capped_limit,
                    )
                )
            if normalized_direction in {"downstream", "both"}:
                edges.extend(
                    _fetch_edges(
                        conn,
                        entity_id=entity_id,
                        direction="downstream",
                        rel_types=rel_types,
                        column_grain=col is not None,
                        limit=capped_limit,
                    )
                )

        # Stable order: upstream first, then downstream; inside each
        # direction sort by other_profile, other_schema, other_name
        # so the LLM gets a predictable rendering shape.
        edges.sort(
            key=lambda e: (
                0 if e["direction"] == "upstream" else 1,
                e.get("other_profile", ""),
                e.get("other_schema", ""),
                e.get("other_name", ""),
            )
        )

        # Surface asset-side edges separately so the LLM knows when
        # to chain a describe_asset call to read the body text.
        asset_edges = [
            {
                "kind": e["other_kind"],
                "name": e["other_name"],
                "profile": e["other_profile"],
                "remote_id": e["other_source_remote_id"],
                "direction": e["direction"],
                "relationship_type": e["relationship_type"],
            }
            for e in edges
            if e["other_kind"]
            in {"notebook", "query", "job", "pipeline", "stream", "streamlit_app"}
            and e["other_source_remote_id"] is not None
        ]

        return {
            "entity_id": entity_id,
            "profile": resolved_profile,
            "schema": sch,
            "table": tbl,
            "column": col,
            "direction": normalized_direction,
            "edges": edges[: capped_limit * 2],
            "edge_count": len(edges),
            "asset_edges": asset_edges,
            "scope_dbs": list(self.db_profiles),
        }


__all__ = ["_LineageToolsMixin"]
