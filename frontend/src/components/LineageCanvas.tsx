/**
 * LineageCanvas — React Flow wrapper that renders a LineagePayload.
 *
 * Layout is computed once per payload via dagre (left → right). Each
 * node carries the FQN as `id` so React Flow's edge endpoints line up
 * with the backend payload directly. Anchor is styled distinct; edges
 * are coloured + dashed based on `type` so the user can tell FK from
 * heuristic from LLM at a glance.
 *
 * Selection: clicking an edge raises `onSelectEdge(edge)` so the
 * parent can render the right-side EdgePanel. Read-only canvas in S3;
 * S4 will add drag-to-connect on top.
 */

import { useMemo } from "react";
import ReactFlow, {
  Background,
  Controls,
  MarkerType,
  type Edge as RFEdge,
  type EdgeMouseHandler,
  type Node as RFNode,
} from "reactflow";
import "reactflow/dist/style.css";
import dagre from "dagre";

import type { LineageEdge, LineageNode, LineagePayload } from "../lib/api";

const NODE_W = 220;
const NODE_H = 56;

type Layout = { nodes: RFNode[]; edges: RFEdge[] };

function layout(payload: LineagePayload): Layout {
  const g = new dagre.graphlib.Graph().setGraph({
    rankdir: "LR",
    nodesep: 32,
    ranksep: 80,
    marginx: 24,
    marginy: 24,
  });
  g.setDefaultEdgeLabel(() => ({}));
  payload.nodes.forEach((n) => g.setNode(n.id, { width: NODE_W, height: NODE_H }));
  payload.edges.forEach((e) => g.setEdge(e.from, e.to));
  dagre.layout(g);

  const nodes: RFNode[] = payload.nodes.map((n) => {
    const pos = g.node(n.id);
    return {
      id: n.id,
      type: "default",
      data: { label: nodeLabel(n) },
      position: { x: pos.x - NODE_W / 2, y: pos.y - NODE_H / 2 },
      style: nodeStyle(n),
      draggable: false,
    };
  });

  const edges: RFEdge[] = payload.edges.map((e, i) => ({
    id: `${e.from}->${e.to}#${i}`,
    source: e.from,
    target: e.to,
    label: edgeLabel(e),
    type: "smoothstep",
    markerEnd: { type: MarkerType.ArrowClosed, color: edgeColor(e) },
    style: { stroke: edgeColor(e), strokeWidth: edgeWidth(e), strokeDasharray: edgeDash(e) },
    labelStyle: { fontSize: 10, fill: edgeColor(e) },
    labelBgStyle: { fill: "#ffffffcc" },
    labelBgPadding: [4, 2] as [number, number],
    labelBgBorderRadius: 4,
    data: e,
  }));

  return { nodes, edges };
}

function nodeLabel(n: LineageNode): string {
  const badge = n.described ? "✓" : "○";
  return `${n.anchor ? "★ " : ""}${badge} ${n.label}`;
}

function nodeStyle(n: LineageNode): React.CSSProperties {
  if (n.anchor) {
    return {
      borderColor: "#d97706",
      borderWidth: 2,
      background: "#fff4e6",
      fontWeight: 600,
      padding: 8,
      borderRadius: 8,
    };
  }
  return {
    borderColor: "#cbd5e1",
    background: "#f8fafc",
    padding: 8,
    borderRadius: 8,
  };
}

function edgeColor(e: LineageEdge): string {
  switch (e.type) {
    case "lineage_fk":
      return "#0f172a";
    case "lineage_view_ddl":
      return "#1d4ed8";
    case "lineage_query_log":
      return "#16a34a";
    case "lineage_co_occurs":
      return "#737373";
    case "lineage_llm":
      return "#9333ea";
    case "lineage_name_match":
      return "#9ca3af";
    case "lineage_manual":
      return "#0f172a";
    default:
      return "#475569";
  }
}

function edgeWidth(e: LineageEdge): number {
  return e.confidence >= 0.9 ? 1.6 : 1.0;
}

function edgeDash(e: LineageEdge): string | undefined {
  if (e.type === "lineage_name_match" || e.type === "lineage_co_occurs") {
    return "6 3";
  }
  if (e.type === "lineage_llm" && e.confidence < 0.7) {
    return "4 2";
  }
  return undefined;
}

function edgeLabel(e: LineageEdge): string {
  switch (e.type) {
    case "lineage_fk":
      return "fk";
    case "lineage_view_ddl":
      return "view";
    case "lineage_query_log":
      return "queries";
    case "lineage_co_occurs":
      return "co-occurs";
    case "lineage_llm":
      return `AI (${e.confidence.toFixed(2)})`;
    case "lineage_name_match":
      return "≈name";
    case "lineage_manual":
      return "manual";
    default:
      return e.extractor;
  }
}

interface Props {
  payload: LineagePayload;
  onSelectEdge?: (edge: LineageEdge | null) => void;
  className?: string;
}

export function LineageCanvas({ payload, onSelectEdge, className }: Props) {
  const { nodes, edges } = useMemo(() => layout(payload), [payload]);

  const handleEdgeClick: EdgeMouseHandler = (_, edge) => {
    onSelectEdge?.((edge.data as LineageEdge) ?? null);
  };
  const handlePaneClick = () => onSelectEdge?.(null);

  if (payload.nodes.length === 0) {
    return (
      <div
        className={
          "flex h-full items-center justify-center rounded-xl border border-dashed " +
          "border-surface-border bg-surface-muted text-sm text-fg-muted " +
          (className ?? "")
        }
      >
        Catalog has no entries for this anchor.
      </div>
    );
  }

  return (
    <div className={"h-full w-full " + (className ?? "")} style={{ minHeight: 420 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable
        onEdgeClick={handleEdgeClick}
        onPaneClick={handlePaneClick}
        proOptions={{ hideAttribution: true }}
      >
        <Background gap={20} color="#e2e8f0" />
        <Controls showInteractive={false} position="bottom-right" />
      </ReactFlow>
    </div>
  );
}
