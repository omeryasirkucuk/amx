/**
 * AssetNode — renders an ingested-asset bridge entity on the canvas.
 *
 * Asset entities (notebooks, queries, streams, pipelines, streamlit
 * apps, jobs) live in catalog_entities as bridge rows pointing at the
 * canonical content in the remote_* tables. The canvas treats them as
 * singletons (no per-column wiring), so this node is intentionally
 * compact: kind-coloured header band, asset name, single target
 * handle so an asset_references_table edge can land on its right
 * side.
 */

import { memo } from "react";
import { Handle, NodeProps, Position } from "reactflow";
import {
  Code2,
  GitBranch,
  LayoutDashboard,
  ScrollText,
  Timer,
  Waves,
  type LucideIcon,
} from "lucide-react";
import clsx from "clsx";

import { NodeDeleteToolbar } from "../components/NodeDeleteToolbar";

export type AssetKind =
  | "notebook"
  | "query"
  | "stream"
  | "pipeline"
  | "streamlit_app"
  | "job";

interface AssetNodeData {
  kind: AssetKind;
  /** Display name from catalog_entities.search_text. */
  label: string;
  /** Owning DB profile (chip in the header). */
  dbProfile?: string;
  /** Optional one-line subtitle (e.g. workspace path). */
  subtitle?: string;
}

const ICONS: Record<AssetKind, LucideIcon> = {
  notebook: ScrollText,
  query: Code2,
  stream: Waves,
  pipeline: GitBranch,
  streamlit_app: LayoutDashboard,
  job: Timer,
};

// Each kind picks a distinct accent so a complex graph stays readable.
// Values come from the existing constants palette via inline hex so we
// don't have to expand TYPE_COLORS for non-column kinds.
const COLORS: Record<AssetKind, string> = {
  notebook: "#a78bfa", // violet
  query: "#22d3ee", // cyan
  stream: "#34d399", // emerald
  pipeline: "#fbbf24", // amber
  streamlit_app: "#f472b6", // pink
  job: "#94a3b8", // slate
};

const LABELS: Record<AssetKind, string> = {
  notebook: "notebook",
  query: "query",
  stream: "stream",
  pipeline: "pipeline",
  streamlit_app: "streamlit",
  job: "job",
};

function AssetNodeImpl({ id, data, selected }: NodeProps<AssetNodeData>) {
  const Icon = ICONS[data.kind] ?? ScrollText;
  const color = COLORS[data.kind] ?? "#a78bfa";
  const kindLabel = LABELS[data.kind] ?? data.kind;
  return (
    <div
      className={clsx(
        "rounded-lg border bg-surface-raised text-ink shadow-lg",
        selected ? "border-accent-default" : "border-surface-border",
      )}
      style={{ minWidth: 200, maxWidth: 320, borderLeft: `3px solid ${color}` }}
    >
      <div
        className="flex items-center gap-1.5 border-b border-surface-border px-3 py-1.5"
        style={{ color }}
      >
        <Icon size={12} />
        <span className="text-[11px] font-semibold uppercase tracking-wide">
          {kindLabel}
        </span>
        {data.dbProfile && (
          <span className="ml-auto rounded bg-surface-subtle px-1 text-[10px] font-medium text-ink-dim">
            {data.dbProfile}
          </span>
        )}
      </div>
      <div className="px-3 py-2">
        <div
          className="truncate text-[13px] font-medium text-ink"
          title={data.label}
        >
          {data.label}
        </div>
        {data.subtitle && (
          <div
            className="mt-0.5 truncate text-[11px] text-ink-dim"
            title={data.subtitle}
          >
            {data.subtitle}
          </div>
        )}
      </div>
      <Handle
        type="source"
        position={Position.Right}
        id="out"
        className="lcv-handle"
        style={{ background: color }}
      />
      <NodeDeleteToolbar nodeId={id} visible={!!selected} />
    </div>
  );
}

export const AssetNode = memo(AssetNodeImpl);
