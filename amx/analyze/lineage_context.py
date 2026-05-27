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

from amx.lineage.neighbors import Neighbor, lineage_neighbors
from amx.utils.logging import get_logger

log = get_logger("analyze.lineage_context")

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

    ``scope`` is the run's schema -> tables map (``{}`` / ``None`` means
    every reachable table). Each block is
    ``{"direction", "kind", "name", "relationship"}`` -- the neighbour
    as seen from the anchor table. Built on the shared
    :func:`amx.lineage.neighbors.lineage_neighbors` core so RUN and ASK
    share one graph walk.
    """
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    if store is None or not profile:
        return out
    with store._connect() as conn:  # noqa: SLF001
        anchors = _anchor_tables(conn, profile, scope)
        if not anchors:
            return out
        id_to_loc = {eid: (s.lower(), t.lower()) for eid, s, t in anchors}
        neighbours = lineage_neighbors(
            conn, anchor_entity_ids=list(id_to_loc), fanout=_MAX_BLOCKS_PER_TABLE
        )
        for anchor_id, nbs in neighbours.items():
            loc = id_to_loc.get(anchor_id)
            if loc and nbs:
                out[loc] = [_block(nb) for nb in nbs]
    return out


def _block(nb: Neighbor) -> dict[str, Any]:
    return {
        "direction": nb.direction,
        "kind": nb.kind,
        "name": nb.name,
        "relationship": nb.relationship,
    }


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


__all__ = ["resolve_lineage_context_for_run"]
