/**
 * Shared node-kind iconography + label helpers for the lineage detail
 * panels. Keeps the table / asset / bucket vocabulary in one place so the
 * inspector and the edge-detail drawer render identical glyphs and names.
 */

import {
  Boxes,
  Code2,
  GitBranch,
  LayoutDashboard,
  Network,
  ScrollText,
  Table2,
  Timer,
  Waves,
  type LucideIcon,
} from "lucide-react";

import type { AssetBucketNodeData, AssetNodeData, CanvasNode, TableNodeData } from "../types";

export const KIND_ICON: Record<string, LucideIcon> = {
  table: Table2,
  notebook: ScrollText,
  query: Code2,
  stream: Waves,
  pipeline: GitBranch,
  streamlit_app: LayoutDashboard,
  job: Timer,
  vector_search_index: Boxes,
  dashboard: LayoutDashboard,
  external: Network,
  "asset-bucket": Boxes,
};

export function iconForKind(kind: string): LucideIcon {
  return KIND_ICON[kind] ?? Table2;
}

/** Full title: a table's FQN, an asset's label, a bucket's header. */
export function nodeTitle(n: CanvasNode): string {
  if (n.data.kind === "table") {
    const d = n.data as TableNodeData;
    return d.fqn || d.table;
  }
  if (n.data.kind === "asset-bucket") return (n.data as AssetBucketNodeData).label;
  return (n.data as AssetNodeData).label || n.data.kind;
}

/** Short display name (table name, asset label, bucket label). */
export function nodeName(n: CanvasNode): string {
  if (n.data.kind === "table") return (n.data as TableNodeData).table;
  if (n.data.kind === "asset-bucket") return (n.data as AssetBucketNodeData).label;
  return (n.data as AssetNodeData).label || n.data.kind;
}

/** Secondary line: a table's ``database.schema``, an asset's subtitle. */
export function nodeSchema(n: CanvasNode): string {
  if (n.data.kind === "table") {
    const d = n.data as TableNodeData;
    return [d.database, d.schema].filter(Boolean).join(".");
  }
  if (n.data.kind === "asset-bucket") {
    const b = n.data as AssetBucketNodeData;
    return `${b.count} asset${b.count === 1 ? "" : "s"}`;
  }
  return (n.data as AssetNodeData).subtitle || "";
}
