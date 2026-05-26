/**
 * Flatten the visible canvas graph to its real members.
 *
 * The lineage canvas folds an anchor's neighbours into collapsed buckets
 * (``asset-bucket`` nodes) whose children + child-edges live in node
 * ``data`` until expanded. The detail panels need the REAL neighbours
 * (tables, notebooks, jobs), not the buckets — so we expand every bucket
 * back to its members and drop the synthetic bucket↔anchor connectors.
 */

import type { AssetBucketNodeData, CanvasEdge, CanvasNode } from "../types";

export interface FlatGraph {
  /** Every real node by id (canvas nodes minus buckets, plus bucket children). */
  nodeById: Map<string, CanvasNode>;
  /** Real edges (canvas edges minus bucket connectors, plus child edges). */
  edges: CanvasEdge[];
}

export function flattenGraph(nodes: CanvasNode[], edges: CanvasEdge[]): FlatGraph {
  const nodeById = new Map<string, CanvasNode>();
  const bucketIds = new Set<string>();
  const flatEdges: CanvasEdge[] = [];
  const seenEdge = new Set<string>();

  for (const n of nodes) {
    if (n.data.kind === "asset-bucket") {
      bucketIds.add(n.id);
      const b = n.data as AssetBucketNodeData;
      for (const c of b.childNodes) if (!nodeById.has(c.id)) nodeById.set(c.id, c);
      for (const e of b.childEdges) {
        if (!seenEdge.has(e.id)) {
          seenEdge.add(e.id);
          flatEdges.push(e);
        }
      }
      continue;
    }
    if (!nodeById.has(n.id)) nodeById.set(n.id, n);
  }
  for (const e of edges) {
    // Synthetic bucket connector — its real child edges were folded in.
    if (bucketIds.has(e.source) || bucketIds.has(e.target)) continue;
    if (!seenEdge.has(e.id)) {
      seenEdge.add(e.id);
      flatEdges.push(e);
    }
  }
  return { nodeById, edges: flatEdges };
}

export type LineageDirection = "upstream" | "downstream";

export interface Neighbour {
  node: CanvasNode;
  direction: LineageDirection;
  /** The flat edge connecting this neighbour to the focus node. */
  edge: CanvasEdge;
}

/** Neighbours of ``focusId`` in the flattened graph, with direction
 *  relative to the focus (an edge INTO the focus = upstream producer; an
 *  edge OUT of the focus = downstream consumer). Deduped by neighbour id. */
export function neighboursOf(focusId: string, flat: FlatGraph): Neighbour[] {
  const out: Neighbour[] = [];
  const seen = new Set<string>();
  for (const e of flat.edges) {
    let otherId = "";
    let direction: LineageDirection | null = null;
    if (e.source === focusId && e.target !== focusId) {
      otherId = e.target;
      direction = "downstream";
    } else if (e.target === focusId && e.source !== focusId) {
      otherId = e.source;
      direction = "upstream";
    }
    if (!direction || seen.has(otherId)) continue;
    const node = flat.nodeById.get(otherId);
    if (!node) continue;
    seen.add(otherId);
    out.push({ node, direction, edge: e });
  }
  return out;
}
