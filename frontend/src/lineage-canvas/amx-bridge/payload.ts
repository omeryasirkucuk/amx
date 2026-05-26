/**
 * AMX payload <-> canvas node/edge converters.
 *
 * The backend returns a normalized shape via /api/lineage/by-id/{id}.
 * The frontend converts it into ReactFlow nodes + edges with the
 * canvas's own type augmentation (per-column handles, operator
 * highlighting, type-color metadata).
 */

import { Position } from "reactflow";
import { EDGE_COLORS, OPERATOR_COLORS } from "../constants";
import type {
  AssetNodeData,
  AssetNodeKind,
  CanvasEdge,
  CanvasNode,
  ColumnSpec,
  OperatorKind,
  TableNodeData,
} from "../types";
import type {
  LoadedCanvas,
  LoadedEdge,
  LoadedNode,
} from "./persistence";

export interface ConvertedCanvas {
  primaryProfile: string;
  artifactId: number;
  artifactName: string;
  anchorEntityId: number;
  nodes: CanvasNode[];
  edges: CanvasEdge[];
  multiProfile: boolean;
}

const PROFILE_CHIP_THRESHOLD = 1;

export function convertLoadedCanvas(loaded: LoadedCanvas): ConvertedCanvas {
  const profileSet = new Set<string>();
  loaded.nodes.forEach((n) => n.profile && profileSet.add(n.profile));
  const multiProfile = profileSet.size > PROFILE_CHIP_THRESHOLD;

  const nodes: CanvasNode[] = loaded.nodes.map((n) =>
    loadedNodeToCanvasNode(n, {
      multiProfile,
      isAnchor: n.entity_id === loaded.anchor_entity_id,
    }),
  );

  // Comments are RFNodes too (NodeResizer + custom CommentNode).
  // Both styles ("note" sticky + "text" plain label) share the same
  // node type — the component branches on data.style.
  for (const c of loaded.comments) {
    nodes.push({
      id: `comment-${c.id}`,
      type: "comment",
      position: { x: c.x, y: c.y },
      width: c.width,
      height: c.height,
      data: {
        kind: "comment",
        id: `comment-${c.id}`,
        commentId: c.id,
        color: (c.color as keyof typeof import("../constants").COMMENT_COLORS) || "amber",
        text: c.text,
        style: c.style === "text" ? "text" : "note",
      },
      dragHandle: ".lcv-comment-grip",
    });
  }

  // Standalone logo nodes (external systems like Power BI / Tableau).
  for (const lg of loaded.logo_nodes ?? []) {
    nodes.push({
      id: `logo-${lg.id}`,
      type: "logo",
      position: { x: lg.x, y: lg.y },
      width: lg.width,
      height: lg.height,
      data: {
        kind: "logo",
        id: `logo-${lg.id}`,
        logoKey: lg.logo_key,
        label: lg.label || lg.logo_label || "",
        logoNodeId: lg.id,
      },
    });
  }

  const entityIdToNodeId = new Map<number, string>();
  const entityIdToKind = new Map<number, string>();
  for (const n of loaded.nodes) {
    entityIdToNodeId.set(n.entity_id, nodeIdFor(n));
    entityIdToKind.set(n.entity_id, n.kind);
  }

  const edges: CanvasEdge[] = [];
  for (const e of loaded.edges) {
    const src = entityIdToNodeId.get(e.from_entity_id);
    const tgt = entityIdToNodeId.get(e.to_entity_id);
    if (!src || !tgt) continue;
    edges.push(
      loadedEdgeToCanvasEdge(
        e,
        src,
        tgt,
        entityIdToKind.get(e.from_entity_id) ?? "table",
        entityIdToKind.get(e.to_entity_id) ?? "table",
      ),
    );
  }

  const collapsed = collapseAssetsIntoBuckets(nodes, edges);
  return {
    primaryProfile: loaded.primary_profile,
    artifactId: loaded.artifact_id,
    artifactName: loaded.name,
    anchorEntityId: loaded.anchor_entity_id,
    nodes: collapsed.nodes,
    edges: collapsed.edges,
    multiProfile,
  };
}

const _BUCKET_ASSET_KINDS = new Set([
  "notebook",
  "query",
  "stream",
  "pipeline",
  "streamlit_app",
  "job",
  "vector_search_index",
  "dashboard",
  "external",
]);

/** Fold each table's producer / consumer asset nodes into a collapsed
 *  "Assets that write / read data" bucket (Databricks-style lean graph).
 *  Member asset nodes + their edges start hidden; the bucket expands
 *  them on click. */
function collapseAssetsIntoBuckets(
  nodes: CanvasNode[],
  edges: CanvasEdge[],
): { nodes: CanvasNode[]; edges: CanvasEdge[] } {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const kindOf = (id: string): string => String((byId.get(id)?.data as { kind?: string })?.kind || "");
  const isAsset = (id: string) => _BUCKET_ASSET_KINDS.has(kindOf(id));
  const isTable = (id: string) => kindOf(id) === "table";

  interface Grp {
    tableId: string;
    dir: "producer" | "consumer";
    assetIds: Set<string>;
    edgeIds: Set<string>;
    kinds: Set<string>;
  }
  const groups = new Map<string, Grp>();
  for (const e of edges) {
    let tableId = "";
    let assetId = "";
    let dir: "producer" | "consumer" | null = null;
    if (isAsset(e.source) && isTable(e.target)) {
      assetId = e.source;
      tableId = e.target;
      dir = "producer";
    } else if (isTable(e.source) && isAsset(e.target)) {
      assetId = e.target;
      tableId = e.source;
      dir = "consumer";
    }
    if (!dir) continue;
    const key = `${tableId}|${dir}`;
    let g = groups.get(key);
    if (!g) {
      g = { tableId, dir, assetIds: new Set(), edgeIds: new Set(), kinds: new Set() };
      groups.set(key, g);
    }
    g.assetIds.add(assetId);
    g.edgeIds.add(e.id);
    g.kinds.add(kindOf(assetId));
  }
  if (groups.size === 0) return { nodes, edges };

  const hiddenAssets = new Set<string>();
  const hiddenEdges = new Set<string>();
  const bucketNodes: CanvasNode[] = [];
  const connectors: CanvasEdge[] = [];
  for (const [key, g] of groups) {
    const table = byId.get(g.tableId);
    if (!table) continue;
    g.assetIds.forEach((a) => hiddenAssets.add(a));
    g.edgeIds.forEach((x) => hiddenEdges.add(x));
    const bucketId = `bucket-${key}`;
    const connectorId = `bucketedge-${key}`;
    const pos =
      g.dir === "producer"
        ? { x: table.position.x - 360, y: table.position.y }
        : { x: table.position.x + 380, y: table.position.y };
    bucketNodes.push({
      id: bucketId,
      type: "asset-bucket",
      position: pos,
      data: {
        kind: "asset-bucket",
        direction: g.dir,
        count: g.assetIds.size,
        assetKinds: [...g.kinds],
        memberNodeIds: [...g.assetIds],
        memberEdgeIds: [...g.edgeIds],
        connectorEdgeId: connectorId,
      },
    });
    const edgeData = {
      relationshipType: "lineage_native_asset",
      source: "databricks_native_lineage",
      confidence: 1,
      verdict: "",
    };
    connectors.push(
      g.dir === "producer"
        ? {
            id: connectorId,
            source: bucketId,
            target: g.tableId,
            sourceHandle: "out",
            targetHandle: "__table__",
            type: "column-edge",
            data: edgeData,
          }
        : {
            id: connectorId,
            source: g.tableId,
            target: bucketId,
            sourceHandle: "__table__",
            targetHandle: "in",
            type: "column-edge",
            data: edgeData,
          },
    );
  }
  return {
    nodes: nodes.map((n) => (hiddenAssets.has(n.id) ? { ...n, hidden: true } : n)).concat(bucketNodes),
    edges: edges.map((e) => (hiddenEdges.has(e.id) ? { ...e, hidden: true } : e)).concat(connectors),
  };
}

function nodeIdFor(n: LoadedNode): string {
  // entity_id keeps node ids stable across profile / database changes
  // and avoids the foot-gun of using FQNs that collide across profiles.
  return `n-${n.entity_id}`;
}

export function loadedNodeToCanvasNode(
  n: LoadedNode,
  opts: { multiProfile: boolean; isAnchor: boolean },
): CanvasNode {
  const ASSET_KINDS: ReadonlySet<AssetNodeKind> = new Set<AssetNodeKind>([
    "notebook",
    "query",
    "stream",
    "pipeline",
    "streamlit_app",
    "job",
    "vector_search_index",
    "dashboard",
    "external",
  ]);
  if (ASSET_KINDS.has(n.kind as AssetNodeKind)) {
    const kind = n.kind as AssetNodeKind;
    return {
      id: nodeIdFor(n),
      type: kind,
      position: { x: n.x, y: n.y },
      data: {
        kind,
        // The backend surfaces the asset's display name in
        // ``label`` (sourced from catalog_entities.search_text);
        // older payloads omitted it so fall back to the bridge
        // table_name ``<kind>#<remote_id>``.
        label: n.label || n.table || kind,
        dbProfile: opts.multiProfile ? n.profile : undefined,
        subtitle: n.schema && n.schema !== "__assets" ? n.schema : undefined,
        metadataState: n.metadata_state === "name_only" ? "name_only" : undefined,
        sourceRemoteId: n.source_remote_id ?? undefined,
      } satisfies AssetNodeData,
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
    };
  }
  if (n.kind === "operator") {
    // The backend now surfaces ``op_kind`` and ``expression`` as
    // first-class fields parsed out of the entity's ``search_text``
    // JSON. Earlier versions read them out of ``column`` / ``table``
    // (which was wrong — those fields hold the operator path + host
    // table); keep a defensive fallback so canvases saved against
    // the older response still round-trip.
    const rawKind = (n.op_kind || "").toLowerCase();
    const validKinds = new Set(["filter", "join", "aggregate", "function", "projection"]);
    const op: OperatorKind = (validKinds.has(rawKind) ? rawKind : "function") as OperatorKind;
    return {
      id: nodeIdFor(n),
      type: "operator",
      position: { x: n.x, y: n.y },
      data: {
        kind: "operator",
        id: nodeIdFor(n),
        opKind: op,
        expression: n.expression ?? "",
        operatorId: n.entity_id,
      },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
    };
  }
  return {
    id: nodeIdFor(n),
    type: "table",
    position: { x: n.x, y: n.y },
    data: {
      kind: "table",
      id: nodeIdFor(n),
      profile: n.profile,
      database: n.database,
      schema: n.schema,
      table: n.table,
      fqn: n.fqn,
      // Hydrate the column rail from the backend when present so the
      // canvas re-renders with real columns instead of the empty
      // "(no columns cached)" placeholder. Older responses omit
      // ``columns`` entirely; falling back to ``[]`` keeps the load
      // path working unchanged.
      columns: Array.isArray(n.columns) ? n.columns : [],
      entityId: n.entity_id,
      showProfileChip: opts.multiProfile,
      isAnchor: opts.isAnchor,
      logoKey: n.logo_key || undefined,
      metadataState: n.metadata_state === "name_only" ? "name_only" : undefined,
    } satisfies TableNodeData,
    sourcePosition: Position.Right,
    targetPosition: Position.Left,
  };
}

/** Pick the ReactFlow handle id for one edge endpoint.
 *  Column edges anchor to the column handle; everything else anchors
 *  to the node-level handle that always exists — ``__table__`` on table
 *  nodes, ``out``/``in`` on asset nodes — so asset↔table and columnless
 *  table edges connect instead of floating. */
function endpointHandle(column: string, kind: string, side: "source" | "target"): string {
  if (column) return column;
  if (kind === "table") return "__table__";
  // asset kinds (notebook/job/pipeline/query/dashboard/vector_search_index/external)
  return side === "source" ? "out" : "in";
}

export function loadedEdgeToCanvasEdge(
  e: LoadedEdge,
  source: string,
  target: string,
  sourceKind = "table",
  targetKind = "table",
): CanvasEdge {
  // Auto-derived defaults from the relationship type / score.
  const defaultColor = EDGE_COLORS[e.relationship_type] ?? EDGE_COLORS.unknown;
  const defaultDashed =
    e.relationship_type === "name_match" ||
    (e.relationship_type === "lineage_llm" && e.score < 0.7);
  // User overrides beat the defaults. ``null`` from the wire means
  // "no override"; ``undefined`` shouldn't happen but is treated the
  // same way to keep TypeScript happy on partial payloads.
  const styleColor = e.style_color ?? undefined;
  const styleDashed = e.style_dashed ?? undefined;
  const cardinality = e.cardinality ?? undefined;
  const effectiveColor = styleColor ?? defaultColor;
  const effectiveDashed = styleDashed ?? defaultDashed;
  return {
    id: `e-${e.id}`,
    source,
    target,
    sourceHandle: endpointHandle(e.from_column, sourceKind, "source"),
    targetHandle: endpointHandle(e.to_column, targetKind, "target"),
    type: "column-edge",
    data: {
      relationshipType: e.relationship_type,
      source: e.source,
      confidence: e.score,
      verdict: e.verdict,
      edgeId: e.id,
      hoverLabel: hoverLabelFor(e),
      styleColor,
      styleDashed,
      cardinality,
    },
    style: {
      stroke: effectiveColor,
      strokeWidth: e.score >= 0.9 ? 1.6 : 1.1,
      strokeDasharray: effectiveDashed ? "5 4" : undefined,
    },
  };
}

function hoverLabelFor(e: LoadedEdge): string {
  const conf = e.score >= 0.001 ? ` · ${Math.round(e.score * 100)}%` : "";
  return `${e.relationship_type}${conf}`;
}

/** Helper for the Add Table flow: synthesize a CanvasNode from a pick. */
export function makeTableNode(args: {
  profile: string;
  database: string;
  schema: string;
  table: string;
  columns: ColumnSpec[];
  position: { x: number; y: number };
  multiProfile: boolean;
  isAnchor?: boolean;
  entityId?: number;
}): CanvasNode {
  const fqn = args.database
    ? `${args.database}.${args.schema}.${args.table}`
    : `${args.schema}.${args.table}`;
  const id = args.entityId ? `n-${args.entityId}` : `n-tmp-${fqn}-${args.profile}`;
  return {
    id,
    type: "table",
    position: args.position,
    data: {
      kind: "table",
      id,
      profile: args.profile,
      database: args.database,
      schema: args.schema,
      table: args.table,
      fqn,
      columns: args.columns,
      entityId: args.entityId,
      showProfileChip: args.multiProfile,
      isAnchor: args.isAnchor,
    },
    sourcePosition: Position.Right,
    targetPosition: Position.Left,
  };
}

/** Live operator-color resolver for OperatorNode. */
export function operatorColor(kind: string): string {
  return OPERATOR_COLORS[kind] ?? OPERATOR_COLORS.function;
}
