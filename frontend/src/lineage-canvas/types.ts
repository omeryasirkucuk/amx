/**
 * Canvas-side type definitions.
 *
 * These mirror the AMX backend payload shapes (see
 * ``amx/web/routers/lineage.py``) but stay independent so the
 * frontend can evolve its own layout state without forcing a
 * backend migration.
 */

import type { Edge as RFEdge, Node as RFNode } from "reactflow";

export type NodeKind = "table" | "operator" | "comment";

export type OperatorKind =
  | "filter"
  | "join"
  | "aggregate"
  | "function"
  | "projection";

export interface ColumnSpec {
  name: string;
  dtype: string;
  isPrimary?: boolean;
  isForeign?: boolean;
  description?: string;
}

export interface TableNodeData {
  kind: "table";
  /** stable identifier used as React Flow node id */
  id: string;
  profile: string;
  database: string;
  schema: string;
  table: string;
  fqn: string;
  columns: ColumnSpec[];
  /** entity_id on the AMX backend, if this node is persisted */
  entityId?: number;
  /** Single-profile canvases suppress the profile chip; multi-profile shows it. */
  showProfileChip?: boolean;
  /** Anchor table (origin of the AI walk) gets a star + amber accent. */
  isAnchor?: boolean;
  /** Highlight column when a trace is active. */
  tracedColumn?: string;
}

export interface CommentNodeData {
  kind: "comment";
  id: string;
  /** backend comment id once persisted */
  commentId?: number;
  color: keyof typeof import("./constants").COMMENT_COLORS;
  text: string;
}

export interface OperatorNodeData {
  kind: "operator";
  id: string;
  /** AMX op_kind from catalog_entities */
  opKind: OperatorKind;
  expression: string;
  /** entity_id on backend once persisted */
  operatorId?: number;
  /** Columns available upstream — populated by the canvas so the
   * Filter node's @-mention dropdown can resolve column choices. */
  upstreamColumns?: string[];
}

export type CanvasNodeData = TableNodeData | OperatorNodeData | CommentNodeData;

export type CanvasNode = RFNode<CanvasNodeData>;

export interface CanvasEdgeData {
  relationshipType: string;
  source: string;
  confidence: number;
  verdict: string;
  /** Backend edge id once persisted (manual edges only). */
  edgeId?: number;
  /** Edge label rendered on hover. */
  hoverLabel?: string;
}

export type CanvasEdge = RFEdge<CanvasEdgeData>;

export interface AddTablePick {
  profile: string;
  database: string;
  schema: string;
  table: string;
  columns: ColumnSpec[];
}

export interface CanvasState {
  artifactId: number | null;
  artifactName: string;
  primaryProfile: string;
  anchorFqn: string;
  nodes: CanvasNode[];
  edges: CanvasEdge[];
}
