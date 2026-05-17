"""Lineage orchestration. Imported by the CLI and (in a later slice) Studio.

This module owns the *flow*:

1. Resolve anchor entity from the catalog.
2. Fan extractors out in ``cache_only`` mode.
3. If anything reported a cache miss, build a cost report and let the
   caller decide whether to fill (the actual prompt lives in the CLI so
   the service stays headless-safe for Studio).
4. Render once everyone is satisfied.
5. Persist a ``lineage_artifacts`` row.

The CLI calls :func:`create_lineage` and :func:`refresh_lineage`. The
non-interactive surface (Studio, tests, automation) calls the same
functions with ``fill_decision`` set explicitly.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from amx.lineage import store as lineage_store
from amx.lineage.extractors import (
    FKExtractor,
    NameMatchExtractor,
    ViewDDLExtractor,
)
from amx.lineage.extractors.view_ddl import ConnectorFactory, ConnectorHandle
from amx.lineage.render import (
    HARD_EDGE_LIMIT,
    HARD_NODE_LIMIT,
    SOFT_NODE_LIMIT,
    SUPPORTED_FORMATS,
    RenderInput,
    count_nodes,
    render_lineage_image,
)
from amx.lineage.types import (
    ColumnRef,
    Edge,
    ExtractResult,
    Scope,
    ScopeFragment,
)

FillDecision = Literal["fill", "skip", "abort"]
FillPrompt = Callable[["CacheMissReport"], FillDecision]


@dataclass
class CacheMissReport:
    """Cost summary handed to the CLI to drive the ``confirm()`` prompt."""

    missing_scopes: list[ScopeFragment] = field(default_factory=list)
    estimated_views: int = 0
    estimated_seconds: float = 0.0
    extractors_with_misses: list[str] = field(default_factory=list)

    def has_misses(self) -> bool:
        return bool(self.missing_scopes) or bool(self.extractors_with_misses)


@dataclass
class ScaleVerdict:
    """Pre-render check. Mirrors render.py guardrails."""

    node_count: int
    edge_count: int
    needs_soft_confirm: bool
    blocked: bool
    blocked_reason: str = ""


@dataclass
class LineageRunResult:
    """Outcome of :func:`create_lineage` / :func:`refresh_lineage`."""

    artifact_id: int
    output_path: Path
    node_count: int
    edge_count: int
    extractors_used: list[str]
    extractors_partial: bool
    aborted: bool = False
    abort_reason: str = ""


def build_default_extractors(
    connector_factory: ConnectorFactory | None = None,
) -> list[Any]:
    """The three slice-1 extractors in dispatch order."""
    return [
        FKExtractor(),
        ViewDDLExtractor(connector_factory=connector_factory),
        NameMatchExtractor(),
    ]


def resolve_anchor_entity_id(
    hs: Any,
    *,
    profile: str,
    anchor: ColumnRef,
) -> int | None:
    """Return ``catalog_entities.id`` matching the anchor's kind+identity."""
    entity_kind = "column" if anchor.column else "table"
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT id FROM catalog_entities
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND table_name = ?
              AND COALESCE(column_name, '') = COALESCE(?, '')
              AND entity_kind = ?
            LIMIT 1
            """,
            (
                profile,
                anchor.database,
                anchor.schema,
                anchor.table,
                anchor.column or None,
                entity_kind,
            ),
        ).fetchone()
    return int(row[0]) if row else None


def gather_edges(
    hs: Any,
    scope: Scope,
    extractors: list[Any],
) -> tuple[list[Edge], list[ExtractResult], CacheMissReport]:
    """Run every extractor in ``cache_only`` mode and aggregate."""
    edges: list[Edge] = []
    results: list[ExtractResult] = []
    miss_report = CacheMissReport()
    for ext in extractors:
        result = ext.extract(hs=hs, scope=scope, mode="cache_only")
        results.append(result)
        edges.extend(result.edges)
        if result.cache_status != "hit":
            miss_report.extractors_with_misses.append(ext.name)
            miss_report.missing_scopes.extend(result.missing_scope)
            miss_report.estimated_views += result.estimated_db_cost.estimated_views
            miss_report.estimated_seconds += result.estimated_db_cost.estimated_seconds
    return edges, results, miss_report


def fill_cache_misses(
    hs: Any,
    scope: Scope,
    extractors: list[Any],
    miss_report: CacheMissReport,
) -> tuple[list[Edge], list[ExtractResult]]:
    """Re-run only the extractors that reported misses in ``db_fill`` mode."""
    edges: list[Edge] = []
    results: list[ExtractResult] = []
    miss_set = set(miss_report.extractors_with_misses)
    for ext in extractors:
        if ext.name in miss_set:
            r = ext.extract(hs=hs, scope=scope, mode="db_fill")
        else:
            r = ext.extract(hs=hs, scope=scope, mode="cache_only")
        results.append(r)
        edges.extend(r.edges)
    return edges, results


def assess_scale(edges: list[Edge], anchor: ColumnRef) -> ScaleVerdict:
    node_count = count_nodes(edges, anchor)
    edge_count = len(edges)
    blocked = False
    reason = ""
    if node_count > HARD_NODE_LIMIT:
        blocked = True
        reason = f"node count {node_count} exceeds hard limit {HARD_NODE_LIMIT}"
    elif edge_count > HARD_EDGE_LIMIT:
        blocked = True
        reason = f"edge count {edge_count} exceeds hard limit {HARD_EDGE_LIMIT}"
    return ScaleVerdict(
        node_count=node_count,
        edge_count=edge_count,
        needs_soft_confirm=(node_count > SOFT_NODE_LIMIT and not blocked),
        blocked=blocked,
        blocked_reason=reason,
    )


def described_entities(
    hs: Any,
    *,
    profile: str,
    refs: list[ColumnRef],
) -> set[str]:
    """Return the FQNs (using same convention as render._node_id) that have a description."""
    if not refs:
        return set()
    with hs._connect() as conn:
        described: set[str] = set()
        for ref in refs:
            entity_kind = "column" if ref.column else "table"
            row = conn.execute(
                """
                SELECT effective_description_id FROM catalog_entities
                WHERE db_profile = ? AND database_name = ? AND schema_name = ?
                  AND table_name = ?
                  AND COALESCE(column_name, '') = COALESCE(?, '')
                  AND entity_kind = ?
                LIMIT 1
                """,
                (
                    profile,
                    ref.database,
                    ref.schema,
                    ref.table,
                    ref.column or None,
                    entity_kind,
                ),
            ).fetchone()
            if row and row[0]:
                described.add(_fqn(ref))
    return described


def create_lineage(
    hs: Any,
    *,
    scope: Scope,
    name: str,
    output_path: Path,
    fmt: str,
    fill_prompt: FillPrompt | None = None,
    fill_decision: FillDecision | None = None,
    force_scale: bool = False,
    soft_confirm: Callable[[ScaleVerdict], bool] | None = None,
    connector_factory: ConnectorFactory | None = None,
    extractors: list[Any] | None = None,
) -> LineageRunResult:
    """End-to-end: extract, render, persist."""
    fmt = fmt.lower()
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"unsupported format {fmt!r}")

    anchor_id = resolve_anchor_entity_id(hs, profile=scope.profile, anchor=scope.anchor)
    if anchor_id is None:
        raise LookupError(
            f"anchor {_fqn(scope.anchor)!r} not found in catalog_entities for profile {scope.profile!r}"
        )

    extractors = extractors or build_default_extractors(connector_factory=connector_factory)

    edges, _results, miss_report = gather_edges(hs, scope, extractors)
    extractors_partial = False
    if miss_report.has_misses():
        decision = fill_decision
        if decision is None and fill_prompt is not None:
            decision = fill_prompt(miss_report)
        if decision is None:
            decision = "skip"
        if decision == "abort":
            return LineageRunResult(
                artifact_id=0,
                output_path=Path(""),
                node_count=0,
                edge_count=0,
                extractors_used=[],
                extractors_partial=False,
                aborted=True,
                abort_reason="user aborted before DB fill",
            )
        if decision == "fill":
            edges, _results = fill_cache_misses(hs, scope, extractors, miss_report)
        else:
            extractors_partial = True

    verdict = assess_scale(edges, scope.anchor)
    if verdict.blocked and not force_scale:
        return LineageRunResult(
            artifact_id=0,
            output_path=Path(""),
            node_count=verdict.node_count,
            edge_count=verdict.edge_count,
            extractors_used=[],
            extractors_partial=extractors_partial,
            aborted=True,
            abort_reason=verdict.blocked_reason + " (use --force to override)",
        )
    if verdict.needs_soft_confirm and soft_confirm is not None:
        if not soft_confirm(verdict):
            return LineageRunResult(
                artifact_id=0,
                output_path=Path(""),
                node_count=verdict.node_count,
                edge_count=verdict.edge_count,
                extractors_used=[],
                extractors_partial=extractors_partial,
                aborted=True,
                abort_reason="user declined large-graph render",
            )

    # Resolve "has description" badges before rendering.
    all_refs = list(
        {
            _node_id(scope.anchor): scope.anchor,
            **{_node_id(r): r for e in edges for r in (e.source, e.target)},
        }.values()
    )
    described = described_entities(hs, profile=scope.profile, refs=all_refs)

    warning = ""
    if extractors_partial:
        scopes = ", ".join(
            f"{s.database}.{s.schema}" if s.database else s.schema
            for s in miss_report.missing_scopes
        )
        warning = (
            f"Partial render — view DDL cache stale for: {scopes}. "
            f"Run /lineage refresh --no-cache for full coverage."
        )

    payload = RenderInput(
        edges=list(edges),
        anchor=scope.anchor,
        described_entities=described,
        title=f"lineage: {_fqn(scope.anchor)} (↑{scope.depth_up} / ↓{scope.depth_down})",
        partial_warning=warning,
    )
    final_path = render_lineage_image(payload=payload, fmt=fmt, output_path=output_path)

    edge_set_hash = lineage_store.compute_edge_set_hash(_edges_for_hash(hs, scope, edges))
    extractors_used = sorted({e.extractor for e in edges})
    artifact_id = lineage_store.insert_lineage_artifact(
        hs,
        name=name,
        db_profile=scope.profile,
        anchor_entity_id=anchor_id,
        depth_up=scope.depth_up,
        depth_down=scope.depth_down,
        fmt=fmt,
        output_path=str(final_path),
        edge_set_hash=edge_set_hash,
        node_count=verdict.node_count,
        edge_count=verdict.edge_count,
        extractors_used=extractors_used,
        extractors_partial=extractors_partial,
    )
    return LineageRunResult(
        artifact_id=artifact_id,
        output_path=final_path,
        node_count=verdict.node_count,
        edge_count=verdict.edge_count,
        extractors_used=extractors_used,
        extractors_partial=extractors_partial,
    )


def refresh_lineage(
    hs: Any,
    *,
    artifact: dict[str, Any],
    fill_prompt: FillPrompt | None = None,
    fill_decision: FillDecision | None = None,
    no_cache: bool = False,
    force_scale: bool = False,
    soft_confirm: Callable[[ScaleVerdict], bool] | None = None,
    connector_factory: ConnectorFactory | None = None,
) -> LineageRunResult:
    """Re-extract + re-render an existing artifact, preserving its scope."""
    anchor_ref = _anchor_from_db(hs, artifact["anchor_entity_id"])
    if anchor_ref is None:
        raise LookupError(f"anchor entity id {artifact['anchor_entity_id']} no longer in catalog")
    scope = Scope(
        profile=artifact["db_profile"],
        anchor=anchor_ref,
        depth_up=artifact["depth_up"],
        depth_down=artifact["depth_down"],
    )
    if no_cache:
        # Invalidate the view-cache for the scope so db_fill picks fresh data.
        lineage_store.invalidate_view_definitions(
            hs,
            db_profile=scope.profile,
            database=scope.anchor.database,
            schema=scope.anchor.schema,
        )
        # And force fill if user already authorised refresh.
        if fill_decision is None:
            fill_decision = "fill"

    extractors = build_default_extractors(connector_factory=connector_factory)
    edges, _results, miss_report = gather_edges(hs, scope, extractors)
    extractors_partial = False
    if miss_report.has_misses():
        decision = fill_decision
        if decision is None and fill_prompt is not None:
            decision = fill_prompt(miss_report)
        if decision is None:
            decision = "skip"
        if decision == "abort":
            return LineageRunResult(
                artifact_id=int(artifact["id"]),
                output_path=Path(artifact["output_path"]),
                node_count=int(artifact["node_count"]),
                edge_count=int(artifact["edge_count"]),
                extractors_used=list(artifact["extractors_used"]),
                extractors_partial=bool(artifact["extractors_partial"]),
                aborted=True,
                abort_reason="user aborted before DB fill",
            )
        if decision == "fill":
            edges, _results = fill_cache_misses(hs, scope, extractors, miss_report)
        else:
            extractors_partial = True

    verdict = assess_scale(edges, scope.anchor)
    if verdict.blocked and not force_scale:
        return LineageRunResult(
            artifact_id=int(artifact["id"]),
            output_path=Path(artifact["output_path"]),
            node_count=verdict.node_count,
            edge_count=verdict.edge_count,
            extractors_used=list(artifact["extractors_used"]),
            extractors_partial=extractors_partial,
            aborted=True,
            abort_reason=verdict.blocked_reason + " (use --force to override)",
        )

    all_refs = list(
        {
            _node_id(scope.anchor): scope.anchor,
            **{_node_id(r): r for e in edges for r in (e.source, e.target)},
        }.values()
    )
    described = described_entities(hs, profile=scope.profile, refs=all_refs)

    warning = ""
    if extractors_partial:
        scopes = ", ".join(
            f"{s.database}.{s.schema}" if s.database else s.schema
            for s in miss_report.missing_scopes
        )
        warning = (
            f"Partial render — view DDL cache stale for: {scopes}. "
            f"Run /lineage refresh --no-cache for full coverage."
        )

    payload = RenderInput(
        edges=list(edges),
        anchor=scope.anchor,
        described_entities=described,
        title=f"lineage: {_fqn(scope.anchor)} (↑{scope.depth_up} / ↓{scope.depth_down})",
        partial_warning=warning,
    )
    final_path = render_lineage_image(
        payload=payload,
        fmt=str(artifact["format"]),
        output_path=Path(artifact["output_path"]),
    )

    edge_set_hash = lineage_store.compute_edge_set_hash(_edges_for_hash(hs, scope, edges))
    extractors_used = sorted({e.extractor for e in edges})
    lineage_store.update_lineage_artifact(
        hs,
        artifact_id=int(artifact["id"]),
        edge_set_hash=edge_set_hash,
        node_count=verdict.node_count,
        edge_count=verdict.edge_count,
        extractors_used=extractors_used,
        extractors_partial=extractors_partial,
        output_path=str(final_path),
    )
    return LineageRunResult(
        artifact_id=int(artifact["id"]),
        output_path=final_path,
        node_count=verdict.node_count,
        edge_count=verdict.edge_count,
        extractors_used=extractors_used,
        extractors_partial=extractors_partial,
    )


def text_tree(
    hs: Any,
    *,
    scope: Scope,
    connector_factory: ConnectorFactory | None = None,
) -> list[str]:
    """Return a text-mode lineage tree for ``/lineage show``.

    Cache-only by design — never calls the wire. Each line is prefixed
    with ``[✓]`` / ``[ ]`` reflecting description presence.
    """
    extractors = build_default_extractors(connector_factory=connector_factory)
    edges, _, _ = gather_edges(hs, scope, extractors)
    refs = list(
        {
            _node_id(scope.anchor): scope.anchor,
            **{_node_id(r): r for e in edges for r in (e.source, e.target)},
        }.values()
    )
    described = described_entities(hs, profile=scope.profile, refs=refs)

    def _matches_anchor(ref: ColumnRef) -> bool:
        # Column-anchor: exact column match. Table-anchor: any column under the same table.
        if scope.anchor.column:
            return _node_id(ref) == _node_id(scope.anchor)
        return (
            ref.database == scope.anchor.database
            and ref.schema == scope.anchor.schema
            and ref.table == scope.anchor.table
        )

    upstream = [e for e in edges if _matches_anchor(e.target)]
    downstream = [e for e in edges if _matches_anchor(e.source)]

    lines: list[str] = []
    lines.append(_tree_line(scope.anchor, described, anchor=True))
    if upstream:
        lines.append("upstream:")
        for e in upstream:
            lines.append(
                "  " + _tree_line(e.source, described, anchor=False) + f"   [{e.extractor}]"
            )
    if downstream:
        lines.append("downstream:")
        for e in downstream:
            lines.append(
                "  " + _tree_line(e.target, described, anchor=False) + f"   [{e.extractor}]"
            )
    if not upstream and not downstream:
        lines.append("(no lineage found in cache)")
    return lines


# ── internal helpers ─────────────────────────────────────────────────────


def _fqn(ref: ColumnRef) -> str:
    return ".".join(p for p in (ref.database, ref.schema, ref.table, ref.column) if p)


def _node_id(ref: ColumnRef) -> str:
    # Mirrors amx.lineage.render._node_id so described_entities keys match.
    return _fqn(ref)


def _tree_line(ref: ColumnRef, described: set[str], *, anchor: bool) -> str:
    badge = "[✓]" if _fqn(ref) in described else "[ ]"
    marker = "★" if anchor else "·"
    return f"{marker} {badge} {_fqn(ref)}"


def _anchor_from_db(hs: Any, anchor_entity_id: int) -> ColumnRef | None:
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT database_name, schema_name, table_name, column_name "
            "FROM catalog_entities WHERE id = ?",
            (int(anchor_entity_id),),
        ).fetchone()
    if not row:
        return None
    return ColumnRef(
        database=str(row[0] or ""),
        schema=str(row[1] or ""),
        table=str(row[2] or ""),
        column=str(row[3] or ""),
    )


def _edges_for_hash(hs: Any, scope: Scope, edges: list[Edge]) -> list[tuple[int, int, str, float]]:
    """Resolve each edge's endpoints to ``catalog_entities.id`` for the hash."""
    ids: list[tuple[int, int, str, float]] = []
    cache: dict[tuple[str, str, str, str, str], int] = {}
    with hs._connect() as conn:

        def _resolve(ref: ColumnRef) -> int:
            entity_kind = "column" if ref.column else "table"
            key = (
                scope.profile,
                ref.database,
                ref.schema,
                ref.table,
                ref.column or "",
            )
            if key in cache:
                return cache[key]
            row = conn.execute(
                """
                SELECT id FROM catalog_entities
                WHERE db_profile = ? AND database_name = ? AND schema_name = ?
                  AND table_name = ?
                  AND COALESCE(column_name, '') = COALESCE(?, '')
                  AND entity_kind = ?
                LIMIT 1
                """,
                (
                    scope.profile,
                    ref.database,
                    ref.schema,
                    ref.table,
                    ref.column or None,
                    entity_kind,
                ),
            ).fetchone()
            entity_id = int(row[0]) if row else 0
            cache[key] = entity_id
            return entity_id

        for edge in edges:
            ids.append(
                (
                    _resolve(edge.source),
                    _resolve(edge.target),
                    edge.relationship_type,
                    edge.confidence,
                )
            )
    return ids


__all__ = [
    "CacheMissReport",
    "ScaleVerdict",
    "LineageRunResult",
    "FillDecision",
    "FillPrompt",
    "ConnectorHandle",
    "build_default_extractors",
    "resolve_anchor_entity_id",
    "gather_edges",
    "fill_cache_misses",
    "assess_scale",
    "described_entities",
    "create_lineage",
    "refresh_lineage",
    "text_tree",
]
