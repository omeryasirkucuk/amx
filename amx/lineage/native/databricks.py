"""Databricks native lineage provider (Unity Catalog REST).

Maps the ``/api/2.0/lineage-tracking/{table,column}-lineage`` REST
responses onto the backend-agnostic value objects in
:mod:`amx.lineage.native.provider`. All coupling to Databricks' JSON
field names lives in this module — the response mixes camelCase
wrappers (``tableInfo``, ``notebookInfos``) with snake_case inner keys
(``catalog_name``, ``schema_name``), and is read defensively so a
renamed or missing field degrades a single node to name-only rather
than failing the fetch.

The endpoint authorizes on ordinary table visibility, so this is the
source AMX uses for the per-table "fetch lineage" flow — in contrast
to the bulk ``system.access.*`` extractor, which most users cannot
read.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from amx.db.adapters._databricks_workspace import (
    DatabricksAuthError,
    DatabricksWorkspaceClient,
)
from amx.lineage.native import provider as P
from amx.utils.logging import get_logger

log = get_logger("lineage.native.databricks")


class DatabricksLineageProvider:
    """Read Unity Catalog lineage for one table via the Workspace REST API."""

    backend = "databricks"

    def __init__(self, client: DatabricksWorkspaceClient) -> None:
        self._client = client

    # ── public API ───────────────────────────────────────────────────

    def fetch_table_lineage(
        self,
        fqn: str,
        *,
        with_columns: bool,
        anchor_columns: tuple[str, ...] = (),
    ) -> P.NativeLineageResult:
        anchor = _table_node_from_fqn(fqn)
        result = P.NativeLineageResult(anchor=anchor)

        # Table + entity (producer/consumer) lineage. A failure to reach
        # the anchor's lineage at all is allowed to propagate so the
        # service can tell the user; per-item parsing never raises.
        raw = self._client.table_lineage(table_name=fqn, include_entity_lineage=True)
        for item in _as_list(raw.get("upstreams")):
            result.edges.extend(_edges_for_item(item, anchor, P.UPSTREAM))
        for item in _as_list(raw.get("downstreams")):
            result.edges.extend(_edges_for_item(item, anchor, P.DOWNSTREAM))

        if with_columns and anchor_columns:
            result.edges.extend(self._column_edges(fqn, anchor, anchor_columns))

        self._resolve_entity_names(result)
        return result

    def _resolve_entity_names(self, result: P.NativeLineageResult) -> None:
        """Replace ``<kind> <id>`` placeholders with real asset names.

        The lineage response only carries entity ids; one REST get per
        distinct (kind, id) turns them into human names. Cached so a
        notebook referenced by several edges costs one call.
        """
        cache: dict[tuple[str, str], str | None] = {}

        def named(node: P.NativeLineageNode) -> P.NativeLineageNode:
            if node.kind not in P.ASSET_KINDS or not node.external_id:
                return node
            key = (node.kind, node.external_id)
            if key not in cache:
                cache[key] = self._client.resolve_entity_name(
                    kind=node.kind, external_id=node.external_id
                )
            name = cache[key]
            return replace(node, name=name) if name else node

        result.edges = [
            replace(e, source=named(e.source), target=named(e.target)) for e in result.edges
        ]

    # ── column lineage ───────────────────────────────────────────────

    def _column_edges(
        self, fqn: str, anchor: P.NativeLineageNode, anchor_columns: tuple[str, ...]
    ) -> list[P.NativeLineageEdge]:
        edges: list[P.NativeLineageEdge] = []
        for col in anchor_columns:
            try:
                raw = self._client.column_lineage(table_name=fqn, column_name=col)
            except DatabricksAuthError:
                # No column-lineage access for this column — skip it; the
                # table-grain edges already convey the relationship.
                continue
            except Exception as exc:  # noqa: BLE001
                log.info("column lineage failed for %s.%s: %s", fqn, col, exc)
                continue
            anchor_col_node = anchor
            for up in _as_list(raw.get("upstream_cols") or raw.get("upstreams")):
                node, col_name = _column_endpoint(up)
                if node is None:
                    continue
                edges.append(
                    P.NativeLineageEdge(
                        source=node,
                        target=anchor_col_node,
                        direction=P.UPSTREAM,
                        from_column=col_name,
                        to_column=col,
                    )
                )
            for down in _as_list(raw.get("downstream_cols") or raw.get("downstreams")):
                node, col_name = _column_endpoint(down)
                if node is None:
                    continue
                edges.append(
                    P.NativeLineageEdge(
                        source=anchor_col_node,
                        target=node,
                        direction=P.DOWNSTREAM,
                        from_column=col,
                        to_column=col_name,
                    )
                )
        return edges


# ── factory + registration ───────────────────────────────────────────


def build_provider_for_profile(profile_name: str) -> DatabricksLineageProvider | None:
    """Resolve a profile to a Databricks lineage provider, or ``None``.

    Returns ``None`` when the profile is missing, is not a Databricks
    backend, or its credentials cannot build a workspace client — the
    registry treats that as "native lineage unavailable for this
    profile".
    """
    try:
        from amx.config import AMXConfig
    except Exception:  # noqa: BLE001
        return None
    try:
        cfg = AMXConfig.load()
    except Exception as exc:  # noqa: BLE001
        log.debug("native lineage: cfg load failed: %s", exc)
        return None
    db_cfg = cfg.db_profiles.get(profile_name)
    if db_cfg is None or (getattr(db_cfg, "backend", "") or "").lower() != "databricks":
        return None
    host = getattr(db_cfg, "host", None)
    token = getattr(db_cfg, "workspace_token", None) or getattr(db_cfg, "access_token", None)
    if not host or not token:
        return None
    client = DatabricksWorkspaceClient(host=host, token=token)
    return DatabricksLineageProvider(client)


def register() -> None:
    """Register the Databricks provider with the native lineage registry."""
    P.register_provider("databricks", build_provider_for_profile)


# ── response mapping helpers (all field coupling lives below) ──────────


def _as_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    return []


def _get_ci(d: dict[str, Any], *keys: str) -> Any:
    """First present value among ``keys`` (camel/snake tolerant)."""
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _table_node_from_fqn(fqn: str) -> P.NativeLineageNode:
    parts = [p for p in (fqn or "").split(".") if p]
    name = parts[-1] if parts else (fqn or "")
    return P.NativeLineageNode(kind=P.TABLE, name=name, fqn=fqn or None)


def _table_node_from_info(info: dict[str, Any]) -> P.NativeLineageNode | None:
    catalog = _get_ci(info, "catalog_name", "catalogName")
    schema = _get_ci(info, "schema_name", "schemaName")
    name = _get_ci(info, "name", "table_name", "tableName")
    if not name:
        return None
    fqn = ".".join(str(p) for p in (catalog, schema, name) if p)
    return P.NativeLineageNode(kind=P.TABLE, name=str(name), fqn=fqn or None)


# Maps each entity-info wrapper key in the response to its node kind and
# the inner field that holds the platform external id. ``external``
# absorbs anything not listed so unknown entity types are still named.
_ENTITY_INFO_KEYS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("notebookInfos", P.NOTEBOOK, ("notebook_id", "notebookId")),
    ("jobInfos", P.JOB, ("job_id", "jobId")),
    ("pipelineInfos", P.PIPELINE, ("pipeline_id", "pipelineId")),
    ("dashboardInfos", P.DASHBOARD, ("dashboard_id", "dashboardId")),
    ("queryInfos", P.QUERY, ("query_id", "queryId")),
)


def _asset_nodes_from_item(item: dict[str, Any]) -> list[P.NativeLineageNode]:
    nodes: list[P.NativeLineageNode] = []
    for wrapper_key, kind, id_keys in _ENTITY_INFO_KEYS:
        for info in _as_list(item.get(wrapper_key)):
            ext = _get_ci(info, *id_keys)
            name = _get_ci(info, "name", "title", "display_name", "workspace_path", "path")
            display = str(name) if name else f"{kind} {ext}" if ext else kind
            nodes.append(
                P.NativeLineageNode(
                    kind=kind,
                    name=display,
                    external_id=str(ext) if ext is not None else None,
                )
            )
    return nodes


def _edges_for_item(
    item: dict[str, Any], anchor: P.NativeLineageNode, direction: str
) -> list[P.NativeLineageEdge]:
    """Build edges for one upstream/downstream lineage item.

    An upstream item feeds the anchor (its table + producer assets point
    *into* the anchor); a downstream item is fed by the anchor (anchor
    points *out* to its table + consumer assets).
    """
    edges: list[P.NativeLineageEdge] = []
    table_info = _get_ci(item, "tableInfo", "table_info")
    endpoints: list[P.NativeLineageNode] = []
    if isinstance(table_info, dict):
        node = _table_node_from_info(table_info)
        if node is not None:
            endpoints.append(node)
    endpoints.extend(_asset_nodes_from_item(item))
    for node in endpoints:
        if direction == P.UPSTREAM:
            edges.append(P.NativeLineageEdge(source=node, target=anchor, direction=P.UPSTREAM))
        else:
            edges.append(P.NativeLineageEdge(source=anchor, target=node, direction=P.DOWNSTREAM))
    return edges


def _column_endpoint(item: dict[str, Any]) -> tuple[P.NativeLineageNode | None, str | None]:
    """Resolve one column-lineage endpoint to (table node, column name)."""
    info = _get_ci(item, "tableInfo", "table_info")
    col = _get_ci(item, "name", "column_name", "columnName")
    # Flat shape falls back to catalog/schema/table/name on the item itself.
    node = _table_node_from_info(info if isinstance(info, dict) else item)
    return node, (str(col) if col else None)


__all__ = [
    "DatabricksLineageProvider",
    "build_provider_for_profile",
    "register",
]
