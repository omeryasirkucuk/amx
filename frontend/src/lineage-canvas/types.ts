/**
 * Canvas-side type definitions.
 *
 * These mirror the AMX backend payload shapes (see
 * ``amx/web/routers/lineage.py``) but stay independent so the
 * frontend can evolve its own layout state without forcing a
 * backend migration.
 */

import type { Edge as RFEdge, Node as RFNode } from "reactflow";

export type NodeKind = "table" | "operator" | "comment" | "logo";

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
  /** Bumping integer that forces the DataFrameNode to expand its
   *  column rail. The bump (rather than a plain boolean) lets the
   *  same node be expanded again on a later click without the user
   *  having to collapse it manually in between. */
  forceExpandTick?: number;
  /** Whether the column rail is open. Lives on node data (instead
   *  of internal DataFrameNode state) so other components — most
   *  importantly ColumnEdge — can read it and decide whether to
   *  anchor edges to column handles vs the table rect. Default
   *  ``false``. */
  expanded?: boolean;
  /** Header badge logo (lineage_logos.key). Empty/undefined = no badge.
   *  Auto-bound from profile.backend on add; user can override. */
  logoKey?: string;
}

export interface LogoNodeData {
  kind: "logo";
  id: string;
  /** lineage_logos.key — stable across history-store rebuilds. */
  logoKey: string;
  /** Optional inline label override; falls back to the registry label. */
  label: string;
  /** Backend logo_nodes.id once persisted. */
  logoNodeId?: number;
}

export interface CommentNodeData {
  kind: "comment";
  id: string;
  /** backend comment id once persisted */
  commentId?: number;
  color: keyof typeof import("./constants").COMMENT_COLORS;
  text: string;
  /** Render mode:
   *   - "note"  → sticky-note (colored bg, header band, palette picker)
   *   - "text"  → minimal plain text label (transparent, no border)
   *  Default "note" so existing comments keep their look. */
  style?: "note" | "text";
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

export type CanvasNodeData =
  | TableNodeData
  | OperatorNodeData
  | CommentNodeData
  | LogoNodeData;

export type CanvasNode = RFNode<CanvasNodeData>;

export type EdgeCardinality = "1:1" | "1:N" | "N:M";

export interface CanvasEdgeData {
  relationshipType: string;
  source: string;
  confidence: number;
  verdict: string;
  /** Backend edge id once persisted. Set for manual edges and for
   *  any edge that arrived from a saved-artifact load. AI-stream
   *  edges that haven't been saved yet leave this undefined. */
  edgeId?: number;
  /** Edge label rendered on hover. */
  hoverLabel?: string;
  /** Studio-canvas style override: stroke color (CSS hex). When
   *  set, beats the relationship_type-derived default. */
  styleColor?: string;
  /** Studio-canvas style override: forced dashed (true) or solid
   *  (false). Undefined keeps the auto-derived behaviour
   *  (name_match / low-confidence LLM dash by default). */
  styleDashed?: boolean;
  /** Relationship cardinality marker rendered at the edge
   *  endpoints. Undefined = no marker. */
  cardinality?: EdgeCardinality;
}

export type CanvasEdge = RFEdge<CanvasEdgeData>;

export interface AddTablePick {
  profile: string;
  /** Profile's backend (e.g. 'postgresql', 'snowflake'). Used by the
   *  Canvas to auto-bind a header logo via backendMap. Empty when
   *  unknown. */
  backend: string;
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
