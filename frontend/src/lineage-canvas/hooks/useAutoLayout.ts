/**
 * Auto-arrange canvas nodes.
 *
 * Two layout modes:
 *
 *   1. **Radial** — anchor at center, every 1-hop neighbour orbits
 *      around it on a single ring whose radius scales with the
 *      neighbour count. Multi-hop descendants and orphan tables fall
 *      back to a dagre LR strip placed to the right of the ring.
 *      This is the default after an AI Generate run because the
 *      streamed output is almost always hub-and-spoke from the
 *      anchor.
 *
 *   2. **Dagre LR** — pure left-to-right hierarchy with generous
 *      spacing. Used when no anchor is supplied, e.g. when the user
 *      taps the manual layout button on a canvas they built by
 *      hand.
 *
 * Comment / logo / operator nodes are excluded from layout — they
 * are presentational and keep their manual positions.
 */

import dagre from "dagre";
import { useCallback } from "react";
import type { CanvasEdge, CanvasNode } from "../types";

interface LayoutOptions {
  /** Node id of the anchor — when present, switches to radial
   *  layout with the anchor at the center. */
  anchorId?: string;
}

const RADIAL_NODE_GAP = 28;
const RADIAL_MIN_RADIUS = 240;
const RADIAL_BASE_RADIUS = 200;

export function useAutoLayout() {
  return useCallback(
    (
      nodes: CanvasNode[],
      edges: CanvasEdge[],
      opts: LayoutOptions = {},
    ): CanvasNode[] => {
      const layoutNodes = nodes.filter((n) => n.data.kind === "table");
      const passthrough = nodes.filter((n) => n.data.kind !== "table");
      if (layoutNodes.length === 0) return nodes;

      const anchor = opts.anchorId
        ? layoutNodes.find((n) => n.id === opts.anchorId)
        : undefined;
      if (anchor && layoutNodes.length > 1) {
        return [
          ...layOutRadial(anchor, layoutNodes, edges),
          ...passthrough,
        ];
      }
      return [...layOutDagre(layoutNodes, edges), ...passthrough];
    },
    [],
  );
}

/** Anchor stays at center; neighbours orbit; deeper nodes go into a
 *  dagre strip to the right of the ring. */
function layOutRadial(
  anchor: CanvasNode,
  tables: CanvasNode[],
  edges: CanvasEdge[],
): CanvasNode[] {
  // Build neighbour-of-anchor set.
  const neighbours = new Set<string>();
  for (const e of edges) {
    if (e.source === anchor.id && e.target !== anchor.id) {
      neighbours.add(e.target);
    } else if (e.target === anchor.id && e.source !== anchor.id) {
      neighbours.add(e.source);
    }
  }
  const neighbourNodes = tables
    .filter((n) => n.id !== anchor.id && neighbours.has(n.id))
    // Deterministic sort so the same canvas always lays out the
    // same way — alphabetical on the human-visible label.
    .sort((a, b) => {
      const al = labelOf(a);
      const bl = labelOf(b);
      return al.localeCompare(bl);
    });
  const orphans = tables.filter(
    (n) => n.id !== anchor.id && !neighbours.has(n.id),
  );

  // Anchor pinned to a logical origin so the layout is stable across
  // re-runs. ``fitView`` (caller's responsibility) will then center
  // the bounding box on screen.
  const cx = 0;
  const cy = 0;
  const N = neighbourNodes.length;
  const radius = N === 0
    ? RADIAL_MIN_RADIUS
    : Math.max(RADIAL_MIN_RADIUS, RADIAL_BASE_RADIUS + N * RADIAL_NODE_GAP);

  const placed: CanvasNode[] = [
    {
      ...anchor,
      position: {
        x: cx - (anchor.width || 240) / 2,
        y: cy - (anchor.height || 140) / 2,
      },
    },
  ];

  for (let i = 0; i < N; i += 1) {
    const n = neighbourNodes[i];
    // Start at -π/2 (12 o'clock) and walk clockwise so the first
    // neighbour lands above the anchor — easier on the eye than
    // starting at 3 o'clock.
    const angle = -Math.PI / 2 + (2 * Math.PI * i) / N;
    placed.push({
      ...n,
      position: {
        x: cx + radius * Math.cos(angle) - (n.width || 240) / 2,
        y: cy + radius * Math.sin(angle) - (n.height || 140) / 2,
      },
    });
  }

  if (orphans.length === 0) {
    return placed;
  }

  // Orphans + their descendants go into a separate dagre LR strip
  // placed to the right of the ring so they do not crash into the
  // radial layout.
  const orphanIds = new Set(orphans.map((n) => n.id));
  const orphanEdges = edges.filter(
    (e) => orphanIds.has(e.source) || orphanIds.has(e.target),
  );
  const orphanLayout = layOutDagre(orphans, orphanEdges);
  // Shift the orphan strip so its left edge sits ~120 px right of
  // the ring.
  const minX = Math.min(...orphanLayout.map((n) => n.position.x));
  const shiftX = cx + radius + 360 - minX;
  return [
    ...placed,
    ...orphanLayout.map((n) => ({
      ...n,
      position: { x: n.position.x + shiftX, y: n.position.y - 200 },
    })),
  ];
}

function layOutDagre(
  tables: CanvasNode[],
  edges: CanvasEdge[],
): CanvasNode[] {
  const g = new dagre.graphlib.Graph().setGraph({
    rankdir: "LR",
    nodesep: 80,
    ranksep: 160,
    marginx: 40,
    marginy: 40,
  });
  g.setDefaultEdgeLabel(() => ({}));

  for (const n of tables) {
    g.setNode(n.id, {
      width: n.width || 240,
      height: n.height || 140,
    });
  }
  const ids = new Set(tables.map((n) => n.id));
  for (const e of edges) {
    if (ids.has(e.source) && ids.has(e.target)) {
      g.setEdge(e.source, e.target);
    }
  }
  dagre.layout(g);

  return tables.map((n) => {
    const pos = g.node(n.id);
    if (!pos) return n;
    return {
      ...n,
      position: {
        x: pos.x - (n.width || 240) / 2,
        y: pos.y - (n.height || 140) / 2,
      },
    };
  });
}

function labelOf(n: CanvasNode): string {
  if (n.data.kind !== "table") return n.id;
  const d = n.data as { schema?: string; table?: string };
  return `${d.schema || ""}.${d.table || ""}`;
}
