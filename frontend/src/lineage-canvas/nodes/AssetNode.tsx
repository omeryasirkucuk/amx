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
  Boxes,
  Code2,
  GitBranch,
  LayoutDashboard,
  MoreHorizontal,
  Network,
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
  | "job"
  | "vector_search_index"
  | "dashboard"
  | "external";

interface AssetNodeData {
  kind: AssetKind;
  /** Display name from catalog_entities.search_text. */
  label: string;
  /** Owning DB profile (chip in the header). */
  dbProfile?: string;
  /** Optional one-line subtitle (e.g. workspace path). */
  subtitle?: string;
  /** "name_only" greys the node — discovered by native lineage fetch
   *  without read access; only its name + relationship are known. */
  metadataState?: "full" | "name_only";
  /** remote_<kind>s.id once ingested — enables the open-in-Assets
   *  drill-in (new tab). Undefined on name-only ghosts. */
  sourceRemoteId?: number;
}

// Asset kinds the Assets page can open in its detail drawer.
const ASSETS_PAGE_KINDS = new Set(["notebook", "query", "job", "pipeline", "stream"]);

function openInAssets(kind: AssetKind, remoteId: number) {
  // New tab so an unsaved lineage canvas isn't lost.
  window.open(
    `/assets?kind=${encodeURIComponent(kind)}&id=${encodeURIComponent(String(remoteId))}`,
    "_blank",
    "noopener",
  );
}

const ICONS: Record<AssetKind, LucideIcon> = {
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
  vector_search_index: "#f97316", // orange
  dashboard: "#60a5fa", // blue
  external: "#9ca3af", // muted grey
};

const LABELS: Record<AssetKind, string> = {
  notebook: "notebook",
  query: "query",
  stream: "stream",
  pipeline: "pipeline",
  streamlit_app: "streamlit",
  job: "job",
  vector_search_index: "vector index",
  dashboard: "dashboard",
  external: "external",
};

function AssetNodeImpl({ id, data, selected }: NodeProps<AssetNodeData>) {
  const Icon = ICONS[data.kind] ?? ScrollText;
  const color = COLORS[data.kind] ?? "#a78bfa";
  const kindLabel = LABELS[data.kind] ?? data.kind;
  const nameOnly = data.metadataState === "name_only";
  return (
    <div
      className={clsx(
        "rounded-lg border bg-surface-raised text-ink shadow-lg",
        selected ? "border-accent-default" : "border-surface-border",
        nameOnly && "opacity-60 border-dashed",
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
        {nameOnly && (
          <span
            className="rounded bg-surface-subtle px-1 text-[10px] font-medium text-ink-dim"
            title="Discovered via lineage — no read access to its contents"
          >
            name only
          </span>
        )}
        {data.dbProfile && (
          <span className="ml-auto rounded bg-surface-subtle px-1 text-[10px] font-medium text-ink-dim">
            {data.dbProfile}
          </span>
        )}
        {data.sourceRemoteId != null && ASSETS_PAGE_KINDS.has(data.kind) && (
          <button
            type="button"
            className={clsx(
              "inline-flex h-5 w-5 items-center justify-center rounded text-ink-dim hover:bg-surface hover:text-ink",
              !data.dbProfile && "ml-auto",
            )}
            title="Open in Assets (new tab)"
            onClick={(e) => {
              e.stopPropagation();
              openInAssets(data.kind, data.sourceRemoteId as number);
            }}
          >
            <MoreHorizontal size={13} />
          </button>
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
      {/* Both handles so asset↔table lineage edges connect in either
          direction: an asset that writes a table (asset → table, uses
          ``out``) and a table consumed by an asset (table → asset,
          uses ``in``). */}
      <Handle
        type="target"
        position={Position.Left}
        id="in"
        className="lcv-handle"
        style={{ background: color }}
      />
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
