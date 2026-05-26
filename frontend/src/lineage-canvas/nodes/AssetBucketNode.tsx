/**
 * AssetBucketNode — a collapsed Databricks-style group.
 *
 * Stands in for a cluster the canvas would otherwise explode into:
 * the anchor's producer / consumer assets ("Assets that write/read
 * data"), or its upstream / downstream tables folded by
 * ``catalog.schema``. Collapsed by default so the graph stays lean.
 *
 * Expand ADDS the child nodes + their edges to the canvas (positioned
 * just outward of the bucket) and hides the bucket↔anchor connector;
 * collapse REMOVES them. Adding fresh (rather than un-hiding) means the
 * children's handles are always measured, so their edges actually
 * render — the bug the previous hidden-toggle approach had.
 */

import { memo, useState } from "react";
import { Handle, NodeProps, Position, useReactFlow } from "reactflow";
import {
  Boxes,
  ChevronDown,
  ChevronRight,
  Code2,
  Database,
  GitBranch,
  LayoutDashboard,
  Network,
  ScrollText,
  Table2,
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
  table: Table2,
};

function AssetBucketNodeImpl({ id, data }: NodeProps<AssetBucketNodeData>) {
  const rf = useReactFlow();
  const [expanded, setExpanded] = useState(false);
  const outward = data.direction === "producer" ? -1 : 1; // left vs right of anchor

  function toggle() {
    const next = !expanded;
    setExpanded(next);
    const self = rf.getNode(id);
    const bx = self?.position.x ?? 0;
    const by = self?.position.y ?? 0;
    const childIds = new Set(data.childNodes.map((n) => n.id));
    const childEdgeIds = new Set(data.childEdges.map((e) => e.id));

    if (next) {
      // Position children in a column just outward of the bucket.
      const n = data.childNodes.length;
      const placed = data.childNodes.map((node, i) => ({
        ...node,
        hidden: false,
        position: { x: bx + outward * 340, y: by + (i - (n - 1) / 2) * 150 },
      }));
      rf.setNodes((nodes) => [
        ...nodes.filter((nd) => !childIds.has(nd.id)),
        ...placed,
      ]);
      rf.setEdges((edges) => [
        ...edges
          .filter((e) => !childEdgeIds.has(e.id))
          .map((e) => (e.id === data.connectorEdgeId ? { ...e, hidden: true } : e)),
        ...data.childEdges.map((e) => ({ ...e, hidden: false })),
      ]);
    } else {
      rf.setNodes((nodes) => nodes.filter((nd) => !childIds.has(nd.id)));
      rf.setEdges((edges) =>
        edges
          .filter((e) => !childEdgeIds.has(e.id))
          .map((e) => (e.id === data.connectorEdgeId ? { ...e, hidden: false } : e)),
      );
    }
  }

  return (
    <button
      type="button"
      onClick={toggle}
      className={clsx(
        "flex items-center gap-2 rounded-lg border border-dashed border-surface-border",
        "bg-surface-raised px-3 py-2 text-left shadow-md transition hover:border-accent-default/50",
      )}
      style={{ minWidth: 200 }}
      title={`${data.label} — click to ${expanded ? "collapse" : "expand"}`}
    >
      {data.direction === "consumer" && (
        <Handle type="target" position={Position.Left} id="in" className="lcv-handle" />
      )}
      {expanded ? (
        <ChevronDown size={14} className="text-fg-muted" />
      ) : (
        <ChevronRight size={14} className="text-fg-muted" />
      )}
      {data.groupKind === "schema" ? (
        <Database size={14} className="text-ink-dim" />
      ) : (
        <div className="flex items-center gap-1">
          {data.iconKinds.slice(0, 4).map((k) => {
            const Icon = KIND_ICON[k] ?? ScrollText;
            return <Icon key={k} size={13} className="text-ink-dim" />;
          })}
        </div>
      )}
      <span className="text-[12px] font-medium text-ink">{data.label}</span>
      <span className="ml-auto rounded-full bg-accent-soft px-1.5 text-[10px] font-semibold text-accent-ink">
        {data.count}
      </span>
      {data.direction === "producer" && (
        <Handle type="source" position={Position.Right} id="out" className="lcv-handle" />
      )}
    </button>
  );
}

export const AssetBucketNode = memo(AssetBucketNodeImpl);
