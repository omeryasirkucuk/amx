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
      },
      dragHandle: ".lcv-comment-grip",
    });
  }

  const entityIdToNodeId = new Map<number, string>();
  for (const n of loaded.nodes) {
    entityIdToNodeId.set(n.entity_id, nodeIdFor(n));
  }

  const edges: CanvasEdge[] = [];
  for (const e of loaded.edges) {
    const src = entityIdToNodeId.get(e.from_entity_id);
    const tgt = entityIdToNodeId.get(e.to_entity_id);
    if (!src || !tgt) continue;
    edges.push(loadedEdgeToCanvasEdge(e, src, tgt));
  }

  return {
    primaryProfile: loaded.primary_profile,
    artifactId: loaded.artifact_id,
    artifactName: loaded.name,
    anchorEntityId: loaded.anchor_entity_id,
    nodes,
    edges,
    multiProfile,
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
  if (n.kind === "operator") {
    const op: OperatorKind = (["filter", "join", "aggregate", "function", "projection"]
      .includes(n.column)
      ? n.column
      : "function") as OperatorKind;
    return {
      id: nodeIdFor(n),
      type: "operator",
      position: { x: n.x, y: n.y },
      data: {
        kind: "operator",
        id: nodeIdFor(n),
        opKind: op,
        expression: n.table || "",
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
      columns: [],
      entityId: n.entity_id,
      showProfileChip: opts.multiProfile,
      isAnchor: opts.isAnchor,
    } satisfies TableNodeData,
    sourcePosition: Position.Right,
    targetPosition: Position.Left,
  };
}

export function loadedEdgeToCanvasEdge(
  e: LoadedEdge,
  source: string,
  target: string,
): CanvasEdge {
  const color = EDGE_COLORS[e.relationship_type] ?? EDGE_COLORS.unknown;
  const dashed =
    e.relationship_type === "name_match" ||
    (e.relationship_type === "lineage_llm" && e.score < 0.7);
  return {
    id: `e-${e.id}`,
    source,
    target,
    sourceHandle: e.from_column || undefined,
    targetHandle: e.to_column || undefined,
    type: "column-edge",
    data: {
      relationshipType: e.relationship_type,
      source: e.source,
      confidence: e.score,
      verdict: e.verdict,
      edgeId: e.id,
      hoverLabel: hoverLabelFor(e),
    },
    style: {
      stroke: color,
      strokeWidth: e.score >= 0.9 ? 1.6 : 1.1,
      strokeDasharray: dashed ? "5 4" : undefined,
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
