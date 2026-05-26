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
  /** Defaults to "full"; "name_only" marks a table discovered via
   *  native lineage fetch without read access (greyed, no drill-in). */
  metadataState?: "full" | "name_only";
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

/** Bridge entity for an ingested remote asset. The canvas treats
 *  these as singleton nodes — no per-column rail; the
 *  ``catalog_entities.source_remote_id`` lookup happens server-side. */
export type AssetNodeKind =
  | "notebook"
  | "query"
  | "stream"
  | "pipeline"
  | "streamlit_app"
  | "job"
  | "vector_search_index"
  | "dashboard"
  | "external";

/** Whether AMX holds the entity in full or discovered it by name only.
 *  Name-only nodes (found via native lineage fetch without read access)
 *  render greyed with a "name only" badge and no drill-in. */
export type MetadataState = "full" | "name_only";

export interface AssetNodeData {
  kind: AssetNodeKind;
  /** Display name sourced from ``catalog_entities.search_text``. */
  label: string;
  /** Owning DB profile chip (shown when multi-profile is in scope). */
  dbProfile?: string;
  /** Optional one-line subtitle (e.g. workspace path). */
  subtitle?: string;
  /** Defaults to "full"; "name_only" greys the node. */
  metadataState?: MetadataState;
  /** ``remote_<kind>s.id`` once the asset is ingested — lets the node
   *  deep-link to the Assets page (new tab) for drill-in. Undefined on
   *  name-only ghosts (nothing to inspect). */
  sourceRemoteId?: number;
}

/** A collapsed "Assets that write / read data" bucket (Databricks-style).
 *  Stands in for a table's producer / consumer asset nodes so the graph
 *  stays table-centric; clicking it reveals the individual assets. */
export interface AssetBucketNodeData {
  kind: "asset-bucket";
  /** "producer" = assets that write the table; "consumer" = read it. */
  direction: "producer" | "consumer";
  count: number;
  /** Distinct asset kinds inside, for the logo row. */
  assetKinds: string[];
  /** Canvas node ids of the member asset nodes this bucket collapses. */
  memberNodeIds: string[];
  /** Canvas edge ids (asset↔table) hidden while collapsed. */
  memberEdgeIds: string[];
  /** The bucket↔table connector edge id, hidden once expanded. */
  connectorEdgeId: string;
}

export type CanvasNodeData =
  | TableNodeData
  | OperatorNodeData
  | CommentNodeData
  | LogoNodeData
  | AssetNodeData
  | AssetBucketNodeData;

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
