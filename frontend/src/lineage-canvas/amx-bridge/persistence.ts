/**
 * Persistence bridge — maps Lineage canvas mutations onto the AMX
 * backend REST surface. The frontend never writes to localStorage:
 * every save/edit/delete round-trips to FastAPI so the change is
 * durable across sessions and visible to other Studio tabs.
 */

import { apiFetch } from "../../lib/api";
import type {
  AssetBucketNodeData,
  AssetNodeData,
  CanvasEdge,
  CanvasNode,
  CommentNodeData,
  OperatorNodeData,
  TableNodeData,
} from "../types";

/** Asset node kinds (notebooks, jobs, …) — they have no FQN and
 *  round-trip purely by ``entity_id``. */
const ASSET_NODE_KINDS: ReadonlySet<string> = new Set([
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

export interface ManualSavePayload {
  profile: string;          // primary profile (used for AI generate/refresh)
  name: string;             // display name only
  anchor_fqn: string;       // primary anchor table
  /** When set, the backend updates this artifact in place (purges
   *  its child rows and re-inserts them under the same id). When
   *  absent, the backend creates a fresh artifact and 409s on
   *  name conflict. */
  artifact_id?: number | null;
  nodes: ManualSaveNode[];
  edges: ManualSaveEdge[];
  /** Operator-kind canvas nodes (filter / join / aggregate / function).
   *  Persisted alongside the table nodes so the load endpoint can
   *  re-render the user's hand-drawn graph exactly as saved. */
  operators: ManualSaveOperator[];
  comments: ManualSaveComment[];
  logo_nodes: ManualSaveLogoNode[];
}

export interface ManualSaveOperator {
  /** ReactFlow node id from the canvas. The backend maps it to a
   *  newly-created (or re-used) operator entity id and stores that
   *  mapping for the same-batch edge resolver. */
  node_id: string;
  op_kind: string;          // filter | join | aggregate | function | projection
  expression: string;       // user-typed expression, may be empty
  x: number;
  y: number;
  width: number;
  height: number;
  z_index?: number;
}

export interface ManualSaveLogoNode {
  logo_key: string;
  label?: string;
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface ManualSaveNode {
  profile: string;
  fqn: string;
  /** catalog_entities.id when known. Required for asset nodes
   *  (notebooks, jobs, dashboards, vector indexes) — they have no
   *  ``database.schema.table`` FQN, so the backend resolves them by id.
   *  Tables carry it too so re-save never re-resolves by name. */
  entity_id?: number;
  x: number;
  y: number;
  width: number;
  height: number;
  z_index?: number;
  /** Optional override for the table's header logo badge. Empty = no override. */
  logo_key?: string;
}

export interface ManualSaveEdge {
  /** Table endpoints round-trip by FQN; operator endpoints round-
   *  trip by their ReactFlow node id (the backend resolves it via
   *  the same-batch ``operators`` array). Exactly one of
   *  ``source_fqn`` / ``source_node_id`` must be set per side. */
  source_fqn?: string;
  source_profile?: string;
  source_node_id?: string;
  /** catalog_entities.id for the endpoint, when known. Lets edges to
   *  asset nodes (no FQN) round-trip; takes priority over FQN on the
   *  backend. */
  source_entity_id?: number;
  target_fqn?: string;
  target_profile?: string;
  target_node_id?: string;
  target_entity_id?: number;
  source_column?: string;
  target_column?: string;
  /** Studio-canvas style override fields — round-trip with the edge
   *  so a saved canvas re-loads with the user's chosen visuals. All
   *  optional; absent / null means "no override". */
  style_color?: string | null;
  style_dashed?: boolean | null;
  cardinality?: "1:1" | "1:N" | "N:M" | null;
}

export interface ManualSaveComment {
  x: number;
  y: number;
  width: number;
  height: number;
  color: string;
  text: string;
  /** "note" (sticky) | "text" (plain label). Default "note". */
  style?: "note" | "text";
}

export interface ManualSaveResponse {
  ok: boolean;
  artifact_id: number;
  persisted_edges: number;
  node_count: number;
  edge_count: number;
  extractors_used?: string[];
}

/** POST /api/lineage/manual — persist the entire canvas atomically. */
export async function saveManualCanvas(
  payload: ManualSavePayload,
): Promise<ManualSaveResponse> {
  return apiFetch<ManualSaveResponse>("/api/lineage/manual", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

/** GET /api/lineage/by-id/{id} — re-open a saved canvas. */
export interface LoadedCanvas {
  artifact_id: number;
  name: string;
  primary_profile: string;
  anchor_entity_id: number;
  generated_at: number;
  nodes: LoadedNode[];
  edges: LoadedEdge[];
  comments: LoadedComment[];
  logo_nodes?: LoadedLogoNode[];
}

export interface LoadedNode {
  entity_id: number;
  profile: string;
  database: string;
  schema: string;
  table: string;
  column: string;
  kind: string;
  fqn: string;
  x: number;
  y: number;
  width: number;
  height: number;
  z_index: number;
  logo_key?: string;
  /** Backend-supplied column rail for table nodes. Pulled from
   *  ``column_comments_cache`` server-side so the canvas re-renders
   *  with full columns instead of the empty "(no columns cached)"
   *  placeholder. Absent on non-table kinds. */
  columns?: Array<{
    name: string;
    dtype: string;
    isPrimary?: boolean;
    isForeign?: boolean;
  }>;
  /** Operator-kind nodes carry these straight out of the entity's
   *  ``search_text`` JSON. ``op_kind`` is one of
   *  filter / join / aggregate / function / projection. */
  op_kind?: string;
  expression?: string;
  /** Asset bridge rows (entity_kind = notebook / query / stream /
   *  pipeline / streamlit_app / job / vector_search_index / dashboard /
   *  external) carry the asset's display name here so the canvas
   *  AssetNode can render it without a second round-trip. Sourced from
   *  catalog_entities.search_text. */
  label?: string;
  /** "full" (default) or "name_only" — native lineage fetch discovered
   *  the entity without read access, so only its name is known. */
  metadata_state?: string;
  /** remote_<kind>s.id for ingested assets — drives the Assets deep-link
   *  on the canvas. Absent on name-only ghosts. */
  source_remote_id?: number | null;
  /** External system identifier (e.g. the Databricks object id) used to
   *  resolve the asset for click-to-ingest. Null / absent when the backend
   *  has no external handle. */
  external_id?: string | null;
}

export interface LoadedEdge {
  id: number;
  from_entity_id: number;
  to_entity_id: number;
  from_column: string;
  to_column: string;
  relationship_type: string;
  source: string;
  score: number;
  verdict: string;
  /** Studio-canvas style override fields. ``null`` (the wire form
   *  for "no override") survives the round-trip so the renderer can
   *  tell "user picked solid" apart from "user never touched". */
  style_color?: string | null;
  style_dashed?: boolean | null;
  cardinality?: "1:1" | "1:N" | "N:M" | null;
}

export interface LoadedComment {
  id: number;
  x: number;
  y: number;
  width: number;
  height: number;
  color: string;
  text: string;
  created_at: number;
  updated_at: number;
  style?: "note" | "text";
}

export interface LoadedLogoNode {
  id: number;
  logo_id: number;
  logo_key: string;
  logo_label: string;
  label: string;
  data_url: string;
  url: string;
  category: string;
  x: number;
  y: number;
  width: number;
  height: number;
  created_at: number;
  updated_at: number;
}

export async function loadCanvas(artifactId: number): Promise<LoadedCanvas> {
  return apiFetch<LoadedCanvas>(`/api/lineage/by-id/${artifactId}`);
}

/** Build the wire payload from canvas state. */
export function buildSavePayload(args: {
  primaryProfile: string;
  artifactName: string;
  anchorFqn: string;
  /** When set, signals the backend to update this artifact in
   *  place instead of creating a fresh one. */
  artifactId?: number | null;
  nodes: CanvasNode[];
  edges: CanvasEdge[];
}): ManualSavePayload {
  const { primaryProfile, artifactName, anchorFqn } = args;
  const nodes: ManualSaveNode[] = [];
  const edges: ManualSaveEdge[] = [];
  const operators: ManualSaveOperator[] = [];
  const comments: ManualSaveComment[] = [];
  const logo_nodes: ManualSaveLogoNode[] = [];
  const tableIndex = new Map<string, TableNodeData>();
  const assetIndex = new Map<string, AssetNodeData>();
  const operatorIds = new Set<string>();

  // Flatten collapsed buckets back to their real members before
  // serializing. A bucket (kind "asset-bucket") is a pure view-layer
  // grouping; its child nodes + edges live in ``data`` whether or not
  // the bucket is expanded. Without this, saving a collapsed native
  // canvas would persist only the anchor and silently drop every
  // producer / consumer asset and folded table. Dedupe by id so an
  // already-expanded bucket (children also present in ``args.nodes``)
  // is not counted twice. The synthetic bucket↔anchor connector edges
  // are dropped — they have no real entity behind them; the per-child
  // edges carried in ``childEdges`` are the truth.
  const bucketIds = new Set<string>();
  const effectiveNodes = new Map<string, CanvasNode>();
  const effectiveEdges = new Map<string, CanvasEdge>();
  for (const n of args.nodes) {
    if (n.data.kind === "asset-bucket") {
      bucketIds.add(n.id);
      const bucket = n.data as AssetBucketNodeData;
      for (const child of bucket.childNodes) {
        if (!effectiveNodes.has(child.id)) effectiveNodes.set(child.id, child);
      }
      for (const ce of bucket.childEdges) {
        if (!effectiveEdges.has(ce.id)) effectiveEdges.set(ce.id, ce);
      }
      continue;
    }
    if (!effectiveNodes.has(n.id)) effectiveNodes.set(n.id, n);
  }
  for (const e of args.edges) {
    if (!effectiveEdges.has(e.id)) effectiveEdges.set(e.id, e);
  }

  for (const n of effectiveNodes.values()) {
    if (n.data.kind === "table") {
      tableIndex.set(n.id, n.data);
      nodes.push({
        profile: n.data.profile || primaryProfile,
        fqn: n.data.fqn,
        entity_id: n.data.entityId,
        x: n.position.x,
        y: n.position.y,
        width: (n.width || 240),
        height: (n.height || 120),
        logo_key: n.data.logoKey || "",
      });
    } else if (ASSET_NODE_KINDS.has(n.data.kind)) {
      const a = n.data as AssetNodeData;
      // Asset nodes have no FQN — without an entity_id the backend
      // cannot place them, so skip ids-less ghosts rather than emit a
      // node the save loop would drop anyway.
      if (a.entityId == null) continue;
      assetIndex.set(n.id, a);
      nodes.push({
        profile: a.dbProfile || primaryProfile,
        fqn: "",
        entity_id: a.entityId,
        x: n.position.x,
        y: n.position.y,
        width: n.width || 240,
        height: n.height || 120,
      });
    } else if (n.data.kind === "operator") {
      const op = n.data as OperatorNodeData;
      operatorIds.add(n.id);
      operators.push({
        node_id: n.id,
        op_kind: op.opKind,
        expression: op.expression || "",
        x: n.position.x,
        y: n.position.y,
        width: n.width || 240,
        height: n.height || 120,
      });
    } else if (n.data.kind === "comment") {
      const c = n.data as CommentNodeData;
      comments.push({
        x: n.position.x,
        y: n.position.y,
        width: n.width || 240,
        height: n.height || 140,
        color: String(c.color || "amber"),
        text: c.text || "",
        style: c.style || "note",
      });
    } else if (n.data.kind === "logo") {
      logo_nodes.push({
        logo_key: n.data.logoKey,
        label: n.data.label || "",
        x: n.position.x,
        y: n.position.y,
        width: n.width || 120,
        height: n.height || 120,
      });
    }
  }

  for (const e of effectiveEdges.values()) {
    // A synthetic bucket connector (endpoint is a bucket node) carries
    // no real entity — its child edges were folded in above.
    if (bucketIds.has(e.source) || bucketIds.has(e.target)) continue;
    const srcT = tableIndex.get(e.source);
    const tgtT = tableIndex.get(e.target);
    const srcA = assetIndex.get(e.source);
    const tgtA = assetIndex.get(e.target);
    const srcIsOp = operatorIds.has(e.source);
    const tgtIsOp = operatorIds.has(e.target);
    // An endpoint must resolve to a table, asset, or operator that is
    // actually on this canvas — anything else (orphan id) is dropped.
    if ((!srcT && !srcA && !srcIsOp) || (!tgtT && !tgtA && !tgtIsOp)) continue;
    const d = e.data;
    // A real column name only exists for a table endpoint anchored to a
    // column handle. The synthetic node-level handles (``__table__`` on
    // tables, ``out``/``in`` on assets) must NOT be stored as columns —
    // doing so makes the reloaded edge fail to reconnect (an asset has
    // no ``__table__`` handle).
    const realColumn = (handle: string | null | undefined, isTable: boolean): string | undefined =>
      isTable && handle && handle !== "__table__" ? handle : undefined;
    edges.push({
      source_fqn: srcT?.fqn,
      source_profile: srcT ? srcT.profile || primaryProfile : undefined,
      source_node_id: srcIsOp ? e.source : undefined,
      source_entity_id: srcT?.entityId ?? srcA?.entityId,
      target_fqn: tgtT?.fqn,
      target_profile: tgtT ? tgtT.profile || primaryProfile : undefined,
      target_node_id: tgtIsOp ? e.target : undefined,
      target_entity_id: tgtT?.entityId ?? tgtA?.entityId,
      source_column: realColumn(e.sourceHandle, !!srcT),
      target_column: realColumn(e.targetHandle, !!tgtT),
      style_color: d?.styleColor ?? null,
      style_dashed: d?.styleDashed ?? null,
      cardinality: d?.cardinality ?? null,
    });
  }

  return {
    profile: primaryProfile,
    name: artifactName,
    anchor_fqn: anchorFqn,
    artifact_id: args.artifactId ?? null,
    nodes,
    edges,
    operators,
    comments,
    logo_nodes,
  };
}

/** Create or update a comment via the REST surface. */
export async function createComment(
  artifactId: number,
  payload: { x: number; y: number; width: number; height: number; color: string; text: string },
): Promise<{ id: number }> {
  return apiFetch<{ id: number }>(
    `/api/lineage/by-id/${artifactId}/comments`,
    { method: "POST", body: JSON.stringify(payload) },
  );
}

export async function updateComment(
  artifactId: number,
  commentId: number,
  payload: Partial<{ x: number; y: number; width: number; height: number; color: string; text: string }>,
): Promise<void> {
  await apiFetch(
    `/api/lineage/by-id/${artifactId}/comments/${commentId}`,
    { method: "PATCH", body: JSON.stringify(payload) },
  );
}

export async function deleteComment(artifactId: number, commentId: number): Promise<void> {
  await fetch(`/api/lineage/by-id/${artifactId}/comments/${commentId}`, {
    method: "DELETE",
  });
}

export interface ManualEdgePayload {
  profile: string;
  source_fqn: string;
  target_fqn: string;
  source_column?: string | null;
  target_column?: string | null;
  notes?: string;
}

export async function createEdge(payload: ManualEdgePayload): Promise<{ id: number }> {
  return apiFetch<{ id: number }>("/api/lineage/edges", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function deleteEdge(edgeId: number): Promise<void> {
  await fetch(`/api/lineage/edges/${edgeId}`, { method: "DELETE" });
}

export interface OperatorPayload {
  profile: string;
  database?: string;
  schema?: string;
  table?: string;
  op_kind: string;
  expression: string;
  upstream_entity_id?: number;
  downstream_entity_id?: number;
}

export async function createOperator(payload: OperatorPayload): Promise<{ operator_id: number }> {
  return apiFetch<{ operator_id: number }>("/api/lineage/operators", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function updateOperator(
  operatorId: number,
  payload: { expression: string },
): Promise<void> {
  await apiFetch(`/api/lineage/operators/${operatorId}`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
}

// Re-export OperatorNodeData so consumers don't need to dual-import.
export type { OperatorNodeData };
