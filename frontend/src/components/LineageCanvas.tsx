/**
 * LineageCanvas — React Flow wrapper that renders a LineagePayload.
 *
 * v3 features baked in:
 * - Provider wrapper exposes `useReactFlow()` so parent search input
 *   can `fitView` on a chosen node.
 * - Click any node → chain highlight: upstream + downstream computed
 *   via BFS over the in-memory edge list; non-chain elements fade to
 *   opacity 0.15. Re-click the same node or click the pane to clear.
 * - Search input lives in `LineageSearchInput.tsx` and is rendered
 *   above the canvas by `LineageDetail.tsx`. The canvas exposes its
 *   node list via `useLineageNodes` for that component to consume.
 *
 * Layout is dagre (left → right). Each node carries the FQN as `id`
 * so React Flow's edge endpoints line up with the backend payload
 * directly. Anchor is styled distinct; edges are coloured + dashed
 * based on `type` so the user can tell FK from heuristic from LLM at
 * a glance.
 *
 * Selection: clicking an edge raises `onSelectEdge(edge)` so the
 * parent can render the right-side EdgePanel. Read-only canvas in S3;
 * S4 will add drag-to-connect on top.
 */

import { forwardRef, useCallback, useEffect, useImperativeHandle, useMemo, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  MarkerType,
  ReactFlowProvider,
  useReactFlow,
  type Connection,
  type Edge as RFEdge,
  type EdgeMouseHandler,
  type Node as RFNode,
  type NodeMouseHandler,
} from "reactflow";
import { Check, Trash2, X } from "lucide-react";
import "reactflow/dist/style.css";
import dagre from "dagre";

import type { LineageEdge, LineageNode, LineagePayload } from "../lib/api";

const NODE_W = 220;
const NODE_H = 56;

type LayoutPair = { nodes: RFNode[]; edges: RFEdge[] };

function layout(payload: LineagePayload): LayoutPair {
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
    case "lineage_codebase":
      return "#ea580c";
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
    case "lineage_codebase":
      return "code";
    case "lineage_manual":
      return "manual";
    default:
      return e.extractor;
  }
}

export interface LineageCanvasHandle {
  /** Centre + zoom on a specific node id; used by the search input. */
  focusNode: (nodeId: string) => void;
}

export type EdgeVerdict = "approved" | "rejected" | "pending" | "";
export type EdgeAction = "approve" | "reject" | "delete";

interface Props {
  payload: LineagePayload;
  onSelectEdge?: (edge: LineageEdge | null) => void;
  /** When provided, the canvas enables drag-to-connect; new edges are
   *  surfaced here so the parent can POST them.
   */
  onCreateEdge?: (sourceId: string, targetId: string) => void;
  /** When provided, right-clicking an edge surfaces a floating action
   *  bar (Approve / Reject / Delete). The parent is responsible for the
   *  actual mutation + refetch.
   */
  onEdgeAction?: (edge: LineageEdge, action: EdgeAction) => void;
  className?: string;
}

export const LineageCanvas = forwardRef<LineageCanvasHandle, Props>(function LineageCanvas(
  { payload, onSelectEdge, onCreateEdge, onEdgeAction, className }: Props,
  ref,
) {
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
    <ReactFlowProvider>
      <CanvasInner
        ref={ref}
        payload={payload}
        onSelectEdge={onSelectEdge}
        onCreateEdge={onCreateEdge}
        onEdgeAction={onEdgeAction}
        className={className}
      />
    </ReactFlowProvider>
  );
});

const CanvasInner = forwardRef<LineageCanvasHandle, Props>(function CanvasInner(
  { payload, onSelectEdge, onCreateEdge, onEdgeAction, className }: Props,
  ref,
) {
  const flow = useReactFlow();
  const { nodes: baseNodes, edges: baseEdges } = useMemo(() => layout(payload), [payload]);
  const [highlightedNode, setHighlightedNode] = useState<string | null>(null);

  // Pre-compute adjacency for BFS chain highlight. Memoised so flips
  // between selections don't rebuild the index every click.
  const adjacency = useMemo(() => {
    const out: Record<string, string[]> = {};
    const inc: Record<string, string[]> = {};
    for (const e of payload.edges) {
      (out[e.from] ??= []).push(e.to);
      (inc[e.to] ??= []).push(e.from);
    }
    return { out, inc };
  }, [payload.edges]);

  const chain = useMemo(() => {
    if (!highlightedNode) return null;
    const upstream = new Set<string>([highlightedNode]);
    const downstream = new Set<string>([highlightedNode]);
    const visitUp: string[] = [highlightedNode];
    while (visitUp.length) {
      const n = visitUp.pop() as string;
      for (const pred of adjacency.inc[n] ?? []) {
        if (!upstream.has(pred)) {
          upstream.add(pred);
          visitUp.push(pred);
        }
      }
    }
    const visitDown: string[] = [highlightedNode];
    while (visitDown.length) {
      const n = visitDown.pop() as string;
      for (const succ of adjacency.out[n] ?? []) {
        if (!downstream.has(succ)) {
          downstream.add(succ);
          visitDown.push(succ);
        }
      }
    }
    const nodes = new Set<string>([...upstream, ...downstream]);
    return { nodes, hub: highlightedNode };
  }, [highlightedNode, adjacency]);

  const nodes = useMemo<RFNode[]>(() => {
    if (!chain) return baseNodes;
    return baseNodes.map((n) =>
      chain.nodes.has(n.id)
        ? n
        : { ...n, style: { ...(n.style ?? {}), opacity: 0.18 } },
    );
  }, [baseNodes, chain]);

  const edges = useMemo<RFEdge[]>(() => {
    if (!chain) return baseEdges;
    return baseEdges.map((e) => {
      const inChain = chain.nodes.has(e.source) && chain.nodes.has(e.target);
      if (inChain) return e;
      return {
        ...e,
        style: { ...(e.style ?? {}), opacity: 0.15 },
        labelStyle: { ...(e.labelStyle ?? {}), opacity: 0.3 },
      };
    });
  }, [baseEdges, chain]);

  const [contextEdge, setContextEdge] = useState<{
    edge: LineageEdge;
    x: number;
    y: number;
  } | null>(null);

  const handleEdgeClick: EdgeMouseHandler = (_, edge) => {
    onSelectEdge?.((edge.data as LineageEdge) ?? null);
  };
  const handleEdgeContextMenu: EdgeMouseHandler = (event, edge) => {
    if (!onEdgeAction) return;
    event.preventDefault();
    const data = edge.data as LineageEdge | undefined;
    if (!data) return;
    setContextEdge({ edge: data, x: event.clientX, y: event.clientY });
  };
  const handleNodeClick: NodeMouseHandler = (_, node) => {
    setHighlightedNode((prev) => (prev === node.id ? null : node.id));
  };
  const handlePaneClick = () => {
    setHighlightedNode(null);
    setContextEdge(null);
    onSelectEdge?.(null);
  };
  const handleConnect = (connection: Connection) => {
    if (!onCreateEdge) return;
    if (!connection.source || !connection.target) return;
    if (connection.source === connection.target) return;
    onCreateEdge(connection.source, connection.target);
  };
  const dismissContext = () => setContextEdge(null);

  const focusNode = useCallback(
    (nodeId: string) => {
      const target = baseNodes.find((n) => n.id === nodeId);
      if (!target) return;
      setHighlightedNode(nodeId);
      flow.fitView({
        nodes: [{ id: nodeId }],
        duration: 320,
        padding: 0.4,
      });
    },
    [baseNodes, flow],
  );

  useImperativeHandle(ref, () => ({ focusNode }), [focusNode]);

  // Reset highlight when payload changes (e.g. switching tabs).
  useEffect(() => {
    setHighlightedNode(null);
  }, [payload]);

  return (
    <div className={"relative h-full w-full " + (className ?? "")} style={{ minHeight: 420 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        nodesDraggable={false}
        nodesConnectable={Boolean(onCreateEdge)}
        elementsSelectable
        onConnect={handleConnect}
        onNodeClick={handleNodeClick}
        onEdgeClick={handleEdgeClick}
        onEdgeContextMenu={handleEdgeContextMenu}
        onPaneClick={handlePaneClick}
        proOptions={{ hideAttribution: true }}
        // v3 S5 — virtualize when the graph passes the soft node limit
        // so large discovery results stay interactive.
        onlyRenderVisibleElements={baseNodes.length > 200}
        minZoom={0.05}
      >
        <Background gap={20} color="#e2e8f0" />
        <Controls showInteractive={false} position="bottom-right" />
      </ReactFlow>
      {contextEdge && onEdgeAction && (
        <div
          className="pointer-events-auto absolute z-20 flex items-center gap-1 rounded-md border border-surface-border bg-surface-raised px-2 py-1 text-xs shadow-md"
          style={{ left: contextEdge.x - 60, top: contextEdge.y - 60 }}
          onMouseLeave={dismissContext}
        >
          <span className="px-1 font-mono text-fg-muted">
            {edgeLabel(contextEdge.edge)}
          </span>
          <button
            type="button"
            className="inline-flex items-center gap-1 rounded px-2 py-1 text-emerald-700 hover:bg-emerald-50"
            onClick={() => {
              onEdgeAction(contextEdge.edge, "approve");
              dismissContext();
            }}
            title="Mark as approved — folds into LLM feedback"
          >
            <Check className="h-3 w-3" /> approve
          </button>
          <button
            type="button"
            className="inline-flex items-center gap-1 rounded px-2 py-1 text-amber-700 hover:bg-amber-50"
            onClick={() => {
              onEdgeAction(contextEdge.edge, "reject");
              dismissContext();
            }}
            title="Mark as rejected — hidden by default + folds into LLM feedback"
          >
            <X className="h-3 w-3" /> reject
          </button>
          <button
            type="button"
            className="inline-flex items-center gap-1 rounded px-2 py-1 text-rose-700 hover:bg-rose-50"
            onClick={() => {
              onEdgeAction(contextEdge.edge, "delete");
              dismissContext();
            }}
            title="Hard-delete the edge (inferred edges may reappear on refresh)"
          >
            <Trash2 className="h-3 w-3" /> delete
          </button>
        </div>
      )}
    </div>
  );
});

LineageCanvas.displayName = "LineageCanvas";
