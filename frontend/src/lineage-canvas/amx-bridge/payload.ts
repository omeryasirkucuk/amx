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

  const anchorNodeId = entityIdToNodeId.get(loaded.anchor_entity_id) ?? "";
  const collapsed = collapseIntoBuckets(nodes, edges, anchorNodeId);
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

interface BucketGroup {
  groupKind: "asset" | "schema";
  dir: "producer" | "consumer";
  label: string;
  nodeIds: Set<string>;
  edgeIds: Set<string>;
  kinds: Set<string>;
}

/** Collapse the anchor's neighbours into Databricks-style group buckets:
 *  producer / consumer ASSETS into "Assets that write/read data", and
 *  upstream / downstream TABLES folded by ``catalog.schema``. The canvas
 *  then shows just the anchor + a few buckets (left = feeds, right =
 *  consumed-by). Each bucket carries its child nodes + edges as data;
 *  expanding ADDS them to the canvas (see AssetBucketNode), so nothing
 *  has to be pre-rendered hidden. */
function collapseIntoBuckets(
  nodes: CanvasNode[],
  edges: CanvasEdge[],
  anchorNodeId: string,
): { nodes: CanvasNode[]; edges: CanvasEdge[] } {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const anchor = anchorNodeId ? byId.get(anchorNodeId) : undefined;
  if (!anchor) return { nodes, edges };
  const kindOf = (id: string): string => String((byId.get(id)?.data as { kind?: string })?.kind || "");
  const isAsset = (id: string) => _BUCKET_ASSET_KINDS.has(kindOf(id));

  // Which neighbours connect to the anchor, and in which direction.
  const conn = new Map<string, { dir: "producer" | "consumer"; edgeIds: Set<string> }>();
  for (const e of edges) {
    let other = "";
    let dir: "producer" | "consumer" | null = null;
    if (e.target === anchorNodeId && e.source !== anchorNodeId) {
      other = e.source; // feeds the anchor
      dir = "producer";
    } else if (e.source === anchorNodeId && e.target !== anchorNodeId) {
      other = e.target; // consumed by
      dir = "consumer";
    }
    if (!dir) continue;
    let c = conn.get(other);
    if (!c) {
      c = { dir, edgeIds: new Set() };
      conn.set(other, c);
    }
    c.edgeIds.add(e.id);
  }
  if (conn.size === 0) return { nodes, edges };

  const groups = new Map<string, BucketGroup>();
  for (const [nid, c] of conn) {
    const node = byId.get(nid);
    if (!node) continue;
    const k = kindOf(nid);
    let key: string;
    let groupKind: "asset" | "schema";
    let label: string;
    if (isAsset(nid)) {
      groupKind = "asset";
      key = `asset|${c.dir}`;
      label = c.dir === "producer" ? "Assets that write data" : "Assets that read data";
    } else {
      groupKind = "schema";
      const d = node.data as { database?: string; schema?: string };
      label = [d.database, d.schema].filter(Boolean).join(".") || "tables";
      key = `schema|${c.dir}|${label}`;
    }
    let g = groups.get(key);
    if (!g) {
      g = { groupKind, dir: c.dir, label, nodeIds: new Set(), edgeIds: new Set(), kinds: new Set() };
      groups.set(key, g);
    }
    g.nodeIds.add(nid);
    c.edgeIds.forEach((x) => g!.edgeIds.add(x));
    g.kinds.add(k);
  }

  const grouped = new Set<string>();
  const groupedEdges = new Set<string>();
  const bucketNodes: CanvasNode[] = [];
  const connectors: CanvasEdge[] = [];
  // Lone (single-table) neighbours stay on the canvas but get pulled into
  // the column beside the anchor — otherwise they keep whatever far-flung
  // position the seed gave them and their edge shoots off-screen.
  const posOverride = new Map<string, { x: number; y: number }>();
  const ax = anchor.position.x;
  const ay = anchor.position.y;

  const COL_GAP = 460;
  const ROW_GAP = 150;

  // Each side packs the anchor's neighbours into one tidy column: a slot
  // is either a bucket (asset group, or a schema with 2+ tables) or a
  // lone table (single-table schema). Everything sits next to the anchor.
  type Slot = { kind: "bucket"; group: BucketGroup } | { kind: "node"; id: string };

  const sides: Array<["producer" | "consumer", number]> = [
    ["producer", -1],
    ["consumer", 1],
  ];
  for (const [dir, outward] of sides) {
    const slots: Slot[] = [];
    for (const g of groups.values()) {
      if (g.dir !== dir) continue;
      if (g.groupKind === "schema" && g.nodeIds.size < 2) {
        for (const id of g.nodeIds) slots.push({ kind: "node", id });
      } else {
        slots.push({ kind: "bucket", group: g });
      }
    }
    const n = slots.length;
    const x = ax + outward * COL_GAP;
    slots.forEach((slot, i) => {
      const y = ay + (i - (n - 1) / 2) * ROW_GAP;
      if (slot.kind === "node") {
        posOverride.set(slot.id, { x, y });
        return;
      }
      const g = slot.group;
      const bucketId = `bucket-${dir}-${i}`;
      const connectorId = `bconn-${dir}-${i}`;
      const childNodes = [...g.nodeIds].map((id) => byId.get(id)).filter(Boolean) as CanvasNode[];
      const childEdges = edges.filter((e) => g.edgeIds.has(e.id));
      g.nodeIds.forEach((id) => grouped.add(id));
      g.edgeIds.forEach((id) => groupedEdges.add(id));
      bucketNodes.push({
        id: bucketId,
        type: "asset-bucket",
        position: { x, y },
        data: {
          kind: "asset-bucket",
          groupKind: g.groupKind,
          direction: g.dir,
          label: g.groupKind === "schema" ? `${g.label} (${g.nodeIds.size})` : g.label,
          count: g.nodeIds.size,
          iconKinds: [...g.kinds],
          childNodes,
          childEdges,
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
        dir === "producer"
          ? {
              id: connectorId,
              source: bucketId,
              target: anchorNodeId,
              sourceHandle: "out",
              targetHandle: "__table__",
              type: "column-edge",
              data: edgeData,
            }
          : {
              id: connectorId,
              source: anchorNodeId,
              target: bucketId,
              sourceHandle: "__table__",
              targetHandle: "in",
              type: "column-edge",
              data: edgeData,
            },
      );
    });
  }

  return {
    nodes: nodes
      .filter((n) => !grouped.has(n.id))
      .map((n) => {
        const p = posOverride.get(n.id);
        return p ? { ...n, position: p } : n;
      })
      .concat(bucketNodes),
    edges: edges.filter((e) => !groupedEdges.has(e.id)).concat(connectors),
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
        entityId: n.entity_id,
        dbProfile: opts.multiProfile ? n.profile : undefined,
        // Real profile, always populated — drives click-to-ingest
        // (dbProfile is display-only and suppressed single-profile).
        profile: n.profile,
        subtitle: n.schema && n.schema !== "__assets" ? n.schema : undefined,
        metadataState: n.metadata_state === "name_only" ? "name_only" : undefined,
        sourceRemoteId: n.source_remote_id ?? undefined,
        externalId: n.external_id ?? undefined,
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
 *  table edges connect instead of floating.
 *
 *  Native lineage edges are forced to the table level even when they
 *  carry a column: the native view is table-centric, and per-column
 *  anchoring made every column look like its own table. The richer
 *  column-level anchoring stays for non-native (FK / manual) edges. */
function endpointHandle(
  column: string,
  kind: string,
  side: "source" | "target",
  tableLevel: boolean,
): string {
  if (column && !tableLevel) return column;
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
  const tableLevel = String(e.relationship_type || "").startsWith("lineage_native");
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
    sourceHandle: endpointHandle(e.from_column, sourceKind, "source", tableLevel),
    targetHandle: endpointHandle(e.to_column, targetKind, "target", tableLevel),
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
