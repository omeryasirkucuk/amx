"""Anchor-based lineage retrieval for the ASK pipeline.

Given a set of anchor entity ids, this module surfaces the immediate
upstream / downstream neighbours plus any sticky-note comments and
logo keys associated with the lineage canvases that include the
anchors. The output is consumed by ``amx/search/_agent/retrieval.py``
to ground ASK answers in saved lineage knowledge without re-running
extractors or parsing the rendered image on disk.

The directed graph is read from ``catalog_relationships`` via
``amx.lineage.store.list_artifact_edges`` so the answer respects the
same depth / extractor scoping the user originally chose when they
saved the canvas. When that edge set comes back empty (catalog rows
pruned after the artifact was rendered), the builder falls back to
the lossy co-resident view from ``lineage_artifact_nodes`` so the
caller still sees neighbour ids, just without direction.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from amx.lineage.store import list_artifact_edges, lookup_lineage_artifact
from amx.storage.sqlite_store import SQLiteHistoryStore

# Cap on the number of catalog_relationships rows pulled per artifact.
# ``list_artifact_edges`` enforces this internally; keeping it at 200
# matches the default the existing /lineage UI tools use.
_EDGE_LOAD_LIMIT = 200


@dataclass(slots=True)
class LineageEvidence:
    """Anchor-relative lineage payload returned to the ASK retrieval layer."""

    upstream_entity_ids: list[int] = field(default_factory=list)
    downstream_entity_ids: list[int] = field(default_factory=list)
    artifact_names: list[str] = field(default_factory=list)
    logo_keys: list[str] = field(default_factory=list)
    comments: list[str] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.upstream_entity_ids and not self.downstream_entity_ids and not self.comments


def build_lineage_evidence(
    *,
    store: SQLiteHistoryStore,
    entity_ids: Iterable[int],
    artifact_filter: list[str] | None,
    max_upstream: int = 5,
    max_downstream: int = 5,
    max_comments: int = 3,
) -> LineageEvidence:
    """Return 1-hop neighbours + comments + logo keys for ``entity_ids``.

    ``artifact_filter`` semantics:
      * ``None`` — include every lineage artifact that mentions any
        anchor entity.
      * non-empty list — restrict to artifacts whose ``name`` is in
        the list.
      * empty list — lineage retrieval is turned off; return an empty
        ``LineageEvidence`` immediately.
    """
    if artifact_filter == []:
        return LineageEvidence()
    ent_set = {int(e) for e in entity_ids}
    if not ent_set:
        return LineageEvidence()

    out = LineageEvidence()

    placeholders = ",".join("?" for _ in ent_set)
    with store._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            f"""
            SELECT DISTINCT la.id, la.name
            FROM lineage_artifacts la
            JOIN lineage_artifact_nodes lan ON lan.artifact_id = la.id
            WHERE lan.entity_id IN ({placeholders})
            """,
            tuple(ent_set),
        ).fetchall()

    artifacts: list[tuple[int, str]] = [
        (int(aid), str(name))
        for aid, name in rows
        if artifact_filter is None or name in artifact_filter
    ]

    for aid, name in artifacts:
        out.artifact_names.append(name)

        artifact_dict = lookup_lineage_artifact(store, name_or_id=str(aid))
        edges: list[dict[str, Any]] = []
        if artifact_dict is not None:
            payload = list_artifact_edges(store, artifact=artifact_dict, limit=_EDGE_LOAD_LIMIT)
            edges = list(payload.get("edges") or [])

        if not edges:
            # Catalog rows pruned after rendering — fall back to the
            # co-resident view so the anchor still surfaces neighbour
            # ids, just without an upstream / downstream split.
            with store._connect() as conn:  # noqa: SLF001
                co_resident = [
                    int(r[0])
                    for r in conn.execute(
                        "SELECT DISTINCT entity_id FROM lineage_artifact_nodes "
                        f"WHERE artifact_id = ? AND entity_id NOT IN ({placeholders})",
                        (aid, *ent_set),
                    ).fetchall()
                ]
            half = max(1, len(co_resident) // 2) if co_resident else 0
            out.upstream_entity_ids.extend(co_resident[:half])
            out.downstream_entity_ids.extend(co_resident[half:])
        else:
            for edge in edges:
                try:
                    src = int(edge["from_id"])
                    tgt = int(edge["to_id"])
                except (KeyError, TypeError, ValueError):
                    continue
                if tgt in ent_set and src not in ent_set:
                    out.upstream_entity_ids.append(src)
                if src in ent_set and tgt not in ent_set:
                    out.downstream_entity_ids.append(tgt)

        with store._connect() as conn:  # noqa: SLF001
            for (text,) in conn.execute(
                "SELECT text FROM lineage_comments WHERE artifact_id = ? "
                "ORDER BY updated_at DESC LIMIT ?",
                (aid, max_comments),
            ):
                if text and len(out.comments) < max_comments:
                    out.comments.append(str(text))
            for (key,) in conn.execute(
                "SELECT DISTINCT logo_key FROM lineage_artifact_nodes "
                "WHERE artifact_id = ? AND logo_key != ''",
                (aid,),
            ):
                if key and key not in out.logo_keys:
                    out.logo_keys.append(str(key))

    out.upstream_entity_ids = _dedup(out.upstream_entity_ids)[:max_upstream]
    out.downstream_entity_ids = _dedup(out.downstream_entity_ids)[:max_downstream]
    out.artifact_names = _dedup(out.artifact_names)
    return out


def _dedup(xs: list[Any]) -> list[Any]:
    seen: set[Any] = set()
    out: list[Any] = []
    for x in xs:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out
