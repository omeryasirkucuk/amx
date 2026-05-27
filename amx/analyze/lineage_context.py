"""Resolve per-table lineage context blocks for ``/analyze run``.

The ProfileAgent describes a table better when it knows what feeds and
consumes it. This module resolves per-table lineage context blocks by
delegating the one-hop graph walk to
:func:`amx.lineage.neighbors.lineage_neighbors` (foreign keys, view
dependencies, ingested-asset references, and ``/lineage fetch``-sourced
native edges) and returns a compact
``dict[(schema, table) -> list[block]]`` the orchestrator attaches to
:class:`AgentContext.lineage_context`.

Unlike :func:`amx.lineage.evidence.build_lineage_evidence` (which is
saved-artifact-scoped and returns entity ids for the ASK pipeline),
this returns human-readable neighbour names + directions for the
prompt, and needs no saved canvas to exist.
"""

from __future__ import annotations

from typing import Any

from amx.lineage.neighbors import Neighbor, lineage_neighbors

# Bound the work so a whole-schema run can't fan out unboundedly.
_MAX_ANCHOR_TABLES = 300
_MAX_BLOCKS_PER_TABLE = 12
# Truncate a neighbour's description so a fanned-out block list stays
# within the ProfileAgent prompt budget.
_MAX_DETAIL_CHARS = 200


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
    share one graph walk. Returns empty when ``AMX_LINEAGE_CONTEXT_DISABLED``
    is set (see :func:`amx.lineage.neighbors.enrichment_disabled`).
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
        neighbour_ids = {nb.entity_id for nbs in neighbours.values() for nb in nbs}
        descriptions = _descriptions_for(conn, neighbour_ids)
    for anchor_id, nbs in neighbours.items():
        loc = id_to_loc.get(anchor_id)
        if loc and nbs:
            out[loc] = [_block(nb, descriptions) for nb in nbs]
    return out


def _block(nb: Neighbor, descriptions: dict[int, str]) -> dict[str, Any]:
    block: dict[str, Any] = {
        "direction": nb.direction,
        "kind": nb.kind,
        "name": nb.name,
        "relationship": nb.relationship,
    }
    desc = descriptions.get(nb.entity_id)
    if desc:
        block["detail"] = desc[:_MAX_DETAIL_CHARS].rstrip()
    return block


def _descriptions_for(conn: Any, entity_ids: set[int]) -> dict[int, str]:
    """Map ``entity_id -> effective description text`` for the ids given.

    Reads the catalog's chosen description via
    ``catalog_entities.effective_description_id``; entities without one
    are simply absent from the result. Tables and assets are treated
    the same -- any entity with a generated description contributes one.
    """
    ids = sorted(int(e) for e in entity_ids)
    if not ids:
        return {}
    ph = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT ce.id, cd.description_text
        FROM catalog_entities ce
        JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
        WHERE ce.id IN ({ph})
        """,  # noqa: S608 — ids are integer placeholders
        tuple(ids),
    ).fetchall()
    return {int(r[0]): str(r[1]) for r in rows if r[1]}


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
