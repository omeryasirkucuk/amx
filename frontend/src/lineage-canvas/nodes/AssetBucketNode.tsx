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

import { memo, useRef, useState } from "react";
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
  // The exact nodes this bucket pushed down (and by how much) so a
  // collapse reverses precisely, even if other buckets expanded too.
  const shiftRef = useRef<{ ids: string[]; h: number } | null>(null);

  const ROW = 116;

  function toggle() {
    const next = !expanded;
    setExpanded(next);
    const self = rf.getNode(id);
    const bx = self?.position.x ?? 0;
    const by = self?.position.y ?? 0;
    const childIds = new Set(data.childNodes.map((n) => n.id));

    if (next) {
      // Children stack directly BELOW the bucket. The bucket keeps its
      // single connector to the anchor (the "main arrow"); children
      // carry no edges of their own — they just nest under the header.
      const placed = data.childNodes.map((node, i) => ({
        ...node,
        hidden: false,
        position: { x: bx, y: by + 64 + i * ROW },
      }));
      const h = placed.length * ROW + 24;
      // Push DOWN every node below the bucket in the same column so the
      // expanded children never collide with a sibling bucket.
      const shiftIds = rf
        .getNodes()
        .filter(
          (n) =>
            n.id !== id &&
            !childIds.has(n.id) &&
            Math.abs(n.position.x - bx) < 260 &&
            n.position.y > by,
        )
        .map((n) => n.id);
      const shiftSet = new Set(shiftIds);
      shiftRef.current = { ids: shiftIds, h };
      rf.setNodes((nodes) => [
        ...nodes
          .filter((nd) => !childIds.has(nd.id))
          .map((nd) =>
            shiftSet.has(nd.id)
              ? { ...nd, position: { ...nd.position, y: nd.position.y + h } }
              : nd,
          ),
        ...placed,
      ]);
    } else {
      const shift = shiftRef.current;
      shiftRef.current = null;
      const shiftSet = new Set(shift?.ids ?? []);
      const h = shift?.h ?? 0;
      rf.setNodes((nodes) =>
        nodes
          .filter((nd) => !childIds.has(nd.id))
          .map((nd) =>
            shiftSet.has(nd.id)
              ? { ...nd, position: { ...nd.position, y: nd.position.y - h } }
              : nd,
          ),
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
