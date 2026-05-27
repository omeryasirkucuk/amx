"""Shared one-hop native-lineage neighbour query for RUN and ASK.

Given a set of anchor catalog-entity ids and an open SQLite connection,
return each anchor's immediate upstream producers and downstream
consumers as name-resolved, bounded neighbour records read straight
from ``catalog_relationships`` — no saved lineage canvas required.

This is the single place the native-lineage graph walk lives: the
``/analyze run`` lineage-context resolver and the /ask retrieval
enrichment both call it instead of carrying near-duplicate queries.
The shape is one-hop on purpose (the performance-safe default); the
design doc records multi-hop as a measured follow-up.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Literal

# Relationship types that carry lineage meaning. Native edges
# (``lineage_native_*``, from ``/lineage fetch``) plus the structural
# edges RUN/ASK already treated as lineage. ``join_inference`` is
# excluded — it carries thousands of speculative edges.
LINEAGE_REL_TYPES: tuple[str, ...] = (
    "foreign_key",
    "view_depends_on",
    "asset_references_table",
    "lineage_native_table",
    "lineage_native_column",
    "lineage_native_asset",
)

# Per-anchor cap on returned neighbours (token budget guard).
DEFAULT_FANOUT = 12

_DISABLE_ENV = "AMX_LINEAGE_CONTEXT_DISABLED"


@dataclass(frozen=True)
class Neighbor:
    """One name-resolved lineage neighbour of an anchor entity."""

    direction: str  # "upstream" | "downstream"
    kind: str  # neighbour entity_kind (table/notebook/job/...)
    name: str  # human name: "schema.table" or asset search_text
    relationship: str  # relationship_type of the edge
    entity_id: int  # neighbour catalog_entities.id
    metadata_state: str  # "full" | "name_only"


def enrichment_disabled() -> bool:
    """True when the field kill-switch env var is set to a truthy value."""
    return os.environ.get(_DISABLE_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def lineage_neighbors(
    conn: Any,
    *,
    anchor_entity_ids: list[int],
    rel_types: tuple[str, ...] = LINEAGE_REL_TYPES,
    fanout: int = DEFAULT_FANOUT,
) -> dict[int, list[Neighbor]]:
    """Return ``{anchor_id -> [Neighbor]}`` for the given anchors.

    One hop in each direction, name-resolved against
    ``catalog_entities``, deduped per anchor, capped at ``fanout``
    neighbours per anchor. Reads only local rows; never touches the
    network. Returns ``{}`` when the kill-switch is set, no anchors
    are given, or ``rel_types`` is empty. When multiple edge types
    connect the same neighbour entity, the entity is deduplicated to
    one ``Neighbor`` and only the first-encountered ``relationship``
    value is kept.
    """
    if enrichment_disabled() or not anchor_entity_ids or not rel_types:
        return {}
    ids = sorted({int(a) for a in anchor_entity_ids})
    anchor_ph = ",".join("?" for _ in ids)
    rel_ph = ",".join("?" for _ in rel_types)
    rows = conn.execute(
        f"""
        SELECT cr.from_entity_id, cr.to_entity_id, cr.relationship_type,
               nf.entity_kind, nf.schema_name, nf.table_name, nf.search_text,
               nf.metadata_state,
               nt.entity_kind, nt.schema_name, nt.table_name, nt.search_text,
               nt.metadata_state
        FROM catalog_relationships cr
        JOIN catalog_entities nf ON nf.id = cr.from_entity_id
        JOIN catalog_entities nt ON nt.id = cr.to_entity_id
        WHERE cr.relationship_type IN ({rel_ph})
          AND (cr.from_entity_id IN ({anchor_ph})
               OR cr.to_entity_id IN ({anchor_ph}))
        """,  # noqa: S608 — all interpolated fragments are placeholder lists
        (*rel_types, *ids, *ids),
    ).fetchall()

    anchor_set = set(ids)
    out: dict[int, list[Neighbor]] = {a: [] for a in ids}
    seen: dict[int, set[tuple[str, str, int]]] = {a: set() for a in ids}
    for row in rows:
        from_id = int(row[0])
        to_id = int(row[1])
        rel = str(row[2])
        # An edge can touch two anchors at once; record the neighbour
        # from each in-scope endpoint's viewpoint.
        if from_id in anchor_set:
            _add(out, seen, from_id, _neighbor(row, side="to", direction="downstream", rel=rel), fanout)
        if to_id in anchor_set:
            _add(out, seen, to_id, _neighbor(row, side="from", direction="upstream", rel=rel), fanout)
    return out


def _neighbor(row: Any, *, side: Literal["to", "from"], direction: str, rel: str) -> Neighbor:
    if side == "to":
        kind = str(row[8] or "table")
        name = _entity_name(row[9], row[10], row[11], kind)
        ent_id = int(row[1])
        state = str(row[12] or "full")
    elif side == "from":
        kind = str(row[3] or "table")
        name = _entity_name(row[4], row[5], row[6], kind)
        ent_id = int(row[0])
        state = str(row[7] or "full")
    else:
        raise ValueError(f"side must be 'to' or 'from', got {side!r}")
    return Neighbor(
        direction=direction,
        kind=kind,
        name=name,
        relationship=rel,
        entity_id=ent_id,
        metadata_state=state,
    )


def _add(
    out: dict[int, list[Neighbor]],
    seen: dict[int, set[tuple[str, str, int]]],
    anchor_id: int,
    nb: Neighbor,
    fanout: int,
) -> None:
    if len(out[anchor_id]) >= fanout:
        return
    key = (nb.direction, nb.kind, nb.entity_id)
    if key in seen[anchor_id]:
        return
    seen[anchor_id].add(key)
    out[anchor_id].append(nb)


def _entity_name(schema: Any, table: Any, search_text: Any, kind: str) -> str:
    if kind != "table" and search_text:
        return str(search_text)
    parts = [str(p) for p in (schema, table) if p]
    return ".".join(parts) or str(table or kind)


__all__ = [
    "Neighbor",
    "lineage_neighbors",
    "enrichment_disabled",
    "LINEAGE_REL_TYPES",
    "DEFAULT_FANOUT",
]
