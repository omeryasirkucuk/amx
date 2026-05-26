"""Resolve per-table lineage context blocks for ``/analyze run``.

The ProfileAgent describes a table better when it knows what feeds and
consumes it. This module reads the table's immediate lineage neighbours
straight from ``catalog_relationships`` — foreign keys, view
dependencies, ingested-asset references, and the
``/lineage fetch``-sourced native edges (``lineage_native_*``) — and
returns compact ``dict[(schema, table) -> list[block]]`` the
orchestrator attaches to :class:`AgentContext.lineage_context`.

Unlike :func:`amx.lineage.evidence.build_lineage_evidence` (which is
saved-artifact-scoped and returns entity ids for the ASK pipeline),
this returns human-readable neighbour names + directions for the
prompt, and needs no saved canvas to exist.
"""

from __future__ import annotations

from typing import Any

from amx.utils.logging import get_logger

log = get_logger("analyze.lineage_context")

# Relationship types that carry lineage meaning for a table. Keeps the
# prompt focused — join_inference / name_match heuristics are excluded.
_LINEAGE_REL_TYPES = (
    "foreign_key",
    "view_depends_on",
    "asset_references_table",
    "lineage_native_table",
    "lineage_native_column",
    "lineage_native_asset",
)

# Bound the work so a whole-schema run can't fan out unboundedly.
_MAX_ANCHOR_TABLES = 300
_MAX_BLOCKS_PER_TABLE = 12


def resolve_lineage_context_for_run(
    *,
    store: Any,
    profile: str,
    scope: dict[str, list[str]] | None = None,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Return ``{(schema, table) -> [lineage block]}`` for a run.

    ``scope`` is the run's schema → tables map (``{}`` / ``None`` means
    every reachable table). Each block is
    ``{"direction": "upstream"|"downstream", "kind", "name",
    "relationship"}`` — the neighbour as seen from the anchor table.
    """
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    if store is None or not profile:
        return out
    with store._connect() as conn:  # noqa: SLF001
        anchors = _anchor_tables(conn, profile, scope)
        if not anchors:
            return out
        for entity_id, schema, table in anchors:
            blocks = _neighbours_for(conn, entity_id)
            if blocks:
                out[(schema.lower(), table.lower())] = blocks
    return out


def _anchor_tables(
    conn: Any, profile: str, scope: dict[str, list[str]] | None
) -> list[tuple[int, str, str]]:
    """Resolve the run's table entities, honouring the schema/table scope."""
    rows = conn.execute(
        """
        SELECT id, schema_name, table_name FROM catalog_entities
        WHERE db_profile = ? AND entity_kind = 'table'
        """,
        (profile,),
    ).fetchall()
    scoped: list[tuple[int, str, str]] = []
    for entity_id, schema, table in rows:
        if not table:
            continue
        if scope:
            wanted = scope.get(str(schema))
            if wanted is None:
                continue
            # Empty list for a schema means "all tables in this schema".
            if wanted and str(table) not in wanted:
                continue
        scoped.append((int(entity_id), str(schema or ""), str(table)))
        if len(scoped) >= _MAX_ANCHOR_TABLES:
            break
    return scoped


def _neighbours_for(conn: Any, anchor_id: int) -> list[dict[str, Any]]:
    """One-hop lineage neighbours of ``anchor_id`` as prompt blocks."""
    placeholders = ",".join("?" for _ in _LINEAGE_REL_TYPES)
    rows = conn.execute(
        f"""
        SELECT cr.from_entity_id, cr.to_entity_id, cr.relationship_type,
               nf.entity_kind, nf.schema_name, nf.table_name, nf.search_text,
               nt.entity_kind, nt.schema_name, nt.table_name, nt.search_text
        FROM catalog_relationships cr
        JOIN catalog_entities nf ON nf.id = cr.from_entity_id
        JOIN catalog_entities nt ON nt.id = cr.to_entity_id
        WHERE cr.relationship_type IN ({placeholders})
          AND (cr.from_entity_id = ? OR cr.to_entity_id = ?)
        """,  # noqa: S608 — relationship types are fixed literals
        (*_LINEAGE_REL_TYPES, anchor_id, anchor_id),
    ).fetchall()
    blocks: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        from_id = int(row[0])
        if from_id == anchor_id:
            direction = "downstream"  # anchor feeds the `to` neighbour
            kind = str(row[7] or "table")
            name = _entity_name(row[8], row[9], row[10], kind)
        else:
            direction = "upstream"  # the `from` neighbour feeds anchor
            kind = str(row[3] or "table")
            name = _entity_name(row[4], row[5], row[6], kind)
        rel = str(row[2])
        key = (direction, kind, name)
        if key in seen:
            continue
        seen.add(key)
        blocks.append({"direction": direction, "kind": kind, "name": name, "relationship": rel})
        if len(blocks) >= _MAX_BLOCKS_PER_TABLE:
            break
    return blocks


def _entity_name(schema: Any, table: Any, search_text: Any, kind: str) -> str:
    if kind != "table" and search_text:
        return str(search_text)
    parts = [str(p) for p in (schema, table) if p]
    return ".".join(parts) or str(table or kind)


__all__ = ["resolve_lineage_context_for_run"]
