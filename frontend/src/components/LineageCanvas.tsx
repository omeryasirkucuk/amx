/**
 * LineageCanvas — React Flow wrapper that renders a v4 LineagePayload
 * as a column-level data-flow editor.
 *
 * Node types:
 *   - TableNode: per-column rows with left target + right source ports.
 *     Drag-from-port → drag-to-port lands as `onCreateEdge(sourceId,
 *     targetId, sourceColumn, targetColumn)` so the parent can POST a
 *     column-level edge.
 *   - OperatorNode: synthetic node representing a transformation
 *     (filter / function / aggregate / join). Backend splits any edge
 *     carrying operator metadata into source → op → target.
 *
 * Interactions kept from v3:
 *   - Node click → upstream/downstream chain highlight (BFS over the
 *     edge list).
 *   - Right-click edge → floating Approve / Reject / Delete bar
 *     (parent owns the mutation).
 *   - Imperative `focusNode` for ⌘K search.
 *
 * Auto-fit on mount fires after layout settles, so the canvas no
 * longer renders as a tiny graph in the bottom-left when there's a
 * single node.
 */

import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useState,
} from "react";
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
  type OnInit,
} from "reactflow";
import { Check, Trash2, X } from "lucide-react";
import "reactflow/dist/style.css";
import dagre from "dagre";

import type { LineageEdge, LineageNode, LineagePayload } from "../lib/api";
import { TableNode, type TableNodeData } from "./lineage/TableNode";
import { OperatorNode, type OperatorNodeData } from "./lineage/OperatorNode";

const TABLE_W = 240;
const ROW_H = 22;
const HEADER_H = 28;
const OP_W = 180;
const OP_H = 48;

function nodeWidth(n: LineageNode): number {
  if (n.kind === "operator") return OP_W;
  return TABLE_W;
}

function nodeHeight(n: LineageNode): number {
  if (n.kind === "operator") return OP_H;
  const colRows = n.columns?.length ?? 0;
  return HEADER_H + Math.max(1, colRows) * ROW_H + 4;
}

const NODE_TYPES = {
  amxTable: TableNode,
  amxOperator: OperatorNode,
};

type LayoutPair = { nodes: RFNode[]; edges: RFEdge[] };

function layout(
  payload: LineagePayload,
  tracedColumn: { nodeId: string; column: string } | null,
): LayoutPair {
  const g = new dagre.graphlib.Graph().setGraph({
    rankdir: "LR",
    nodesep: 28,
    ranksep: 100,
    marginx: 24,
    marginy: 24,
  });
  g.setDefaultEdgeLabel(() => ({}));
  payload.nodes.forEach((n) =>
    g.setNode(n.id, { width: nodeWidth(n), height: nodeHeight(n) }),
  );
  payload.edges.forEach((e) => g.setEdge(e.from, e.to));
  dagre.layout(g);

  const nodes: RFNode[] = payload.nodes.map((n) => {
    const pos = g.node(n.id);
    const w = nodeWidth(n);
    const h = nodeHeight(n);
    if (n.kind === "operator") {
      const data: OperatorNodeData = {
        label: n.label,
        op_kind: n.op_kind,
        expression: n.expression,
        operator_id: n.operator_id ?? null,
      };
      return {
        id: n.id,
        type: "amxOperator",
        data,
        position: { x: pos.x - w / 2, y: pos.y - h / 2 },
        draggable: false,
      };
    }
    const data: TableNodeData = {
      label: n.label,
      anchor: n.anchor,
      described: n.described,
      columns: n.columns,
      tracedColumn:
        tracedColumn && tracedColumn.nodeId === n.id
          ? tracedColumn.column
          : null,
    };
    return {
      id: n.id,
      type: "amxTable",
      data,
      position: { x: pos.x - w / 2, y: pos.y - h / 2 },
      draggable: false,
    };
  });

  const edges: RFEdge[] = payload.edges.map((e, i) => ({
    id: `${e.from}->${e.to}#${i}`,
    source: e.from,
    target: e.to,
    sourceHandle: e.from_column || null,
    targetHandle: e.to_column || null,
    label: edgeLabel(e),
    type: "smoothstep",
    markerEnd: { type: MarkerType.ArrowClosed, color: edgeColor(e) },
    style: {
      stroke: edgeColor(e),
      strokeWidth: edgeWidthFor(e),
      strokeDasharray: edgeDash(e),
    },
    labelStyle: { fontSize: 9, fill: edgeColor(e) },
    labelBgStyle: { fill: "#0f172acc" },
    labelBgPadding: [3, 1] as [number, number],
    labelBgBorderRadius: 3,
    data: e,
  }));

  return { nodes, edges };
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

function edgeWidthFor(e: LineageEdge): number {
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
  if (e.role === "operator_input" || e.role === "operator_output") {
    return "";
  }
  switch (e.type) {
    case "lineage_fk":
      return "fk";
    case "lineage_view_ddl":
      return "view";
    case "lineage_query_log":
      return "queries";
    case "lineage_co_occurs":
      return "co";
    case "lineage_llm":
      return `AI ${e.confidence.toFixed(2)}`;
    case "lineage_name_match":
      return "≈";
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

export interface ConnectionPayload {
  source: string;
  target: string;
  sourceColumn: string | null;
  targetColumn: string | null;
}

interface Props {
  payload: LineagePayload;
  onSelectEdge?: (edge: LineageEdge | null) => void;
  /** When provided, the canvas enables drag-to-connect; new edges are
   *  surfaced with the column names from the port handles so the
   *  parent can POST a column-level edge.
   */
  onCreateEdge?: (conn: ConnectionPayload) => void;
  /** When provided, right-clicking an edge surfaces a floating action
   *  bar (Approve / Reject / Delete). The parent is responsible for
   *  the actual mutation + refetch.
   */
  onEdgeAction?: (edge: LineageEdge, action: EdgeAction) => void;
  /** Currently traced column for the right-rail panel. Highlights the
   *  matching row in the corresponding TableNode. */
  tracedColumn?: { nodeId: string; column: string } | null;
  /** Called when the user clicks a column row inside any TableNode.
   *  Wired by the canvas's parent route to open the trace panel. */
  onColumnClick?: (nodeId: string, column: string) => void;
  /** v4 S5 — called when an OperatorNode editor commits a new
   *  expression for a persisted operator entity. */
  onEditOperator?: (operatorId: number, expression: string) => void;
  className?: string;
}

export const LineageCanvas = forwardRef<LineageCanvasHandle, Props>(
  function LineageCanvas(
    {
      payload,
      onSelectEdge,
      onCreateEdge,
      onEdgeAction,
      tracedColumn,
      onColumnClick,
      onEditOperator,
      className,
    }: Props,
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
          No lineage detected yet — try AI suggest or draw manually.
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
          tracedColumn={tracedColumn ?? null}
          onColumnClick={onColumnClick}
          onEditOperator={onEditOperator}
          className={className}
        />
      </ReactFlowProvider>
    );
  },
);

const CanvasInner = forwardRef<LineageCanvasHandle, Props>(function CanvasInner(
  {
    payload,
    onSelectEdge,
    onCreateEdge,
    onEdgeAction,
    tracedColumn = null,
    onColumnClick,
    onEditOperator,
    className,
  }: Props,
  ref,
) {
  const flow = useReactFlow();
  // Inject the column-click handler into each TableNode's data via the
  // layout pass. Memoised on the handler identity so React Flow
  // doesn't see new node objects on every render.
  const layoutResult = useMemo(
    () => layout(payload, tracedColumn ?? null),
    [payload, tracedColumn],
  );
  const baseNodes = useMemo<RFNode[]>(() => {
    if (!onColumnClick && !onEditOperator) return layoutResult.nodes;
    return layoutResult.nodes.map((n) => {
      if (n.type === "amxTable" && onColumnClick) {
        const data = n.data as TableNodeData;
        return {
          ...n,
          data: {
            ...data,
            onColumnClick: (col: string) => onColumnClick(n.id, col),
          },
        };
      }
      if (n.type === "amxOperator" && onEditOperator) {
        const data = n.data as OperatorNodeData;
        return {
          ...n,
          data: {
            ...data,
            onEditExpression: (id: number, expr: string) =>
              onEditOperator(id, expr),
          },
        };
      }
      return n;
    });
  }, [layoutResult.nodes, onColumnClick, onEditOperator]);
  const baseEdges = layoutResult.edges;
  const [highlightedNode, setHighlightedNode] = useState<string | null>(null);

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
      chain.nodes.has(n.id) ? n : { ...n, style: { ...(n.style ?? {}), opacity: 0.18 } },
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
    onCreateEdge({
      source: connection.source,
      target: connection.target,
      sourceColumn: connection.sourceHandle ?? null,
      targetColumn: connection.targetHandle ?? null,
    });
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

  // Auto-fit on mount AND on payload change — without this a single-node
  // canvas renders in the bottom-left at high zoom. fitView is debounced
  // by React Flow itself so we don't hammer the animation loop.
  const handleInit: OnInit = useCallback((instance) => {
    instance.fitView({ padding: 0.2, duration: 200 });
  }, []);
  useEffect(() => {
    const id = window.requestAnimationFrame(() => {
      flow.fitView({ padding: 0.2, duration: 200 });
    });
    return () => window.cancelAnimationFrame(id);
  }, [flow, payload]);

  return (
    <div
      className={"relative h-full w-full " + (className ?? "")}
      style={{ minHeight: 420 }}
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={NODE_TYPES}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        nodesDraggable={false}
        nodesConnectable={Boolean(onCreateEdge)}
        elementsSelectable
        onInit={handleInit}
        onConnect={handleConnect}
        onNodeClick={handleNodeClick}
        onEdgeClick={handleEdgeClick}
        onEdgeContextMenu={handleEdgeContextMenu}
        onPaneClick={handlePaneClick}
        proOptions={{ hideAttribution: true }}
        onlyRenderVisibleElements={baseNodes.length > 100}
        minZoom={0.05}
      >
        <Background gap={20} color="#1e293b" />
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
