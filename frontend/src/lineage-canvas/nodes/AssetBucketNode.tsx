/**
 * AssetBucketNode — a collapsed "Assets that write / read data" group.
 *
 * Mirrors the Databricks lineage UI: the graph shows table relationships
 * first; the producer / consumer assets (notebooks, jobs, queries, …)
 * are folded into a compact bucket with a count + kind logos. Clicking
 * it expands the individual asset nodes (and their edges) and hides the
 * bucket's own connector; clicking again collapses them back.
 *
 * Toggling is done in place via ``useReactFlow`` over the member node /
 * edge ids stored on the node data — no canvas-level wiring needed.
 */

import { memo, useState } from "react";
import { Handle, NodeProps, Position, useReactFlow } from "reactflow";
import {
  Boxes,
  ChevronDown,
  ChevronRight,
  Code2,
  GitBranch,
  LayoutDashboard,
  Network,
  ScrollText,
  Timer,
  Waves,
  type LucideIcon,
} from "lucide-react";
import clsx from "clsx";

import type { AssetBucketNodeData } from "../types";

const KIND_ICON: Record<string, LucideIcon> = {
  notebook: ScrollText,
  query: Code2,
  stream: Waves,
  pipeline: GitBranch,
  streamlit_app: LayoutDashboard,
  job: Timer,
  vector_search_index: Boxes,
  dashboard: LayoutDashboard,
  external: Network,
};

function AssetBucketNodeImpl({ data }: NodeProps<AssetBucketNodeData>) {
  const rf = useReactFlow();
  const [expanded, setExpanded] = useState(false);
  const isProducer = data.direction === "producer";
  const label = isProducer ? "Assets that write data" : "Assets that read data";

  function toggle() {
    const next = !expanded;
    setExpanded(next);
    const members = new Set(data.memberNodeIds);
    const memberEdges = new Set(data.memberEdgeIds);
    rf.setNodes((nodes) =>
      nodes.map((n) => (members.has(n.id) ? { ...n, hidden: !next } : n)),
    );
    rf.setEdges((edges) =>
      edges.map((e) => {
        if (memberEdges.has(e.id)) return { ...e, hidden: !next };
        // Hide the bucket's own connector once the real assets are shown.
        if (e.id === data.connectorEdgeId) return { ...e, hidden: next };
        return e;
      }),
    );
  }

  return (
    <button
      type="button"
      onClick={toggle}
      className={clsx(
        "flex items-center gap-2 rounded-lg border border-dashed border-surface-border",
        "bg-surface-raised px-3 py-2 text-left shadow-md transition hover:border-accent-default/50",
      )}
      style={{ minWidth: 190 }}
      title={`${label} — click to ${expanded ? "collapse" : "expand"}`}
    >
      {!isProducer && (
        <Handle type="target" position={Position.Left} id="in" className="lcv-handle" />
      )}
      {expanded ? (
        <ChevronDown size={14} className="text-fg-muted" />
      ) : (
        <ChevronRight size={14} className="text-fg-muted" />
      )}
      <div className="flex items-center gap-1">
        {data.assetKinds.slice(0, 4).map((k) => {
          const Icon = KIND_ICON[k] ?? ScrollText;
          return <Icon key={k} size={13} className="text-ink-dim" />;
        })}
      </div>
      <span className="text-[12px] font-medium text-ink">{label}</span>
      <span className="ml-auto rounded-full bg-accent-soft px-1.5 text-[10px] font-semibold text-accent-ink">
        {data.count}
      </span>
      {isProducer && (
        <Handle type="source" position={Position.Right} id="out" className="lcv-handle" />
      )}
    </button>
  );
}

export const AssetBucketNode = memo(AssetBucketNodeImpl);
