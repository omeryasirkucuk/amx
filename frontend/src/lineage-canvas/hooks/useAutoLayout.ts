/**
 * Auto-arrange canvas nodes using dagre (left-to-right).
 *
 * Comment nodes are excluded from the layout pass — they are
 * presentational annotations and should keep their manual position.
 */

import dagre from "dagre";
import { useCallback } from "react";
import type { CanvasEdge, CanvasNode } from "../types";

export function useAutoLayout() {
  return useCallback(
    (nodes: CanvasNode[], edges: CanvasEdge[]): CanvasNode[] => {
      const g = new dagre.graphlib.Graph().setGraph({
        rankdir: "LR",
        nodesep: 48,
        ranksep: 80,
        marginx: 40,
        marginy: 40,
      });
      g.setDefaultEdgeLabel(() => ({}));

      const layoutNodes = nodes.filter((n) => n.data.kind !== "comment");
      const commentNodes = nodes.filter((n) => n.data.kind === "comment");

      for (const n of layoutNodes) {
        g.setNode(n.id, {
          width: n.width || 240,
          height: n.height || 140,
        });
      }
      for (const e of edges) {
        if (layoutNodes.find((n) => n.id === e.source) && layoutNodes.find((n) => n.id === e.target)) {
          g.setEdge(e.source, e.target);
        }
      }

      dagre.layout(g);

      const laidOut = layoutNodes.map((n) => {
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

      return [...laidOut, ...commentNodes];
    },
    [],
  );
}
