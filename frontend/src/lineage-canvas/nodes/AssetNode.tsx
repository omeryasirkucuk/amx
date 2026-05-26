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

import { memo, useState } from "react";
import { Handle, NodeProps, Position } from "reactflow";
import {
  Boxes,
  Code2,
  Download,
  ExternalLink,
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
import { databricksDeepLink } from "../logos/databricksDeepLink";

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
  /** External system identifier (e.g. Databricks object id) — drives
   *  click-to-ingest and external deep-links. Undefined when unknown. */
  externalId?: string;
  /** Owning host (e.g. the Databricks workspace host) used to build the
   *  external deep-link. Undefined when unknown. */
  host?: string;
  /** Real owning DB profile, always populated — used as the POST body
   *  for click-to-ingest (unlike ``dbProfile``, which is display-only
   *  and suppressed on single-profile canvases). */
  profile?: string;
}

// Asset kinds the Assets page can open in its detail drawer.
const ASSETS_PAGE_KINDS = new Set(["notebook", "query", "job", "pipeline", "stream"]);

// Asset kinds that can be lazily ingested from the platform on demand.
const INGESTABLE_KINDS = new Set<AssetKind>(["notebook", "job", "pipeline"]);

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
  const [ingesting, setIngesting] = useState(false);

  // Lazy ingest: a name-only ghost we know the external id for can be
  // pulled into AMX on demand, then opened in the Assets page.
  const canIngest =
    nameOnly && !!data.externalId && INGESTABLE_KINDS.has(data.kind);

  async function ingestAndOpen() {
    if (!data.externalId || !data.profile) return;
    setIngesting(true);
    try {
      const resp = await fetch("/api/lineage/asset/ingest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          profile: data.profile,
          kind: data.kind,
          external_id: data.externalId,
        }),
      });
      if (!resp.ok) return;
      const { remote_id } = (await resp.json()) as { remote_id: number };
      openInAssets(data.kind, remote_id);
    } catch {
      // Network/parse failure — leave the node as-is; the finally clause
      // clears the loading state so the control is clickable again.
    } finally {
      setIngesting(false);
    }
  }

  // External deep-link (e.g. open this asset in the Databricks workspace).
  const href = databricksDeepLink({
    kind: data.kind,
    host: data.host,
    externalId: data.externalId,
  });

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
        <div className="ml-auto flex items-center gap-1">
          {data.dbProfile && (
            <span className="rounded bg-surface-subtle px-1 text-[10px] font-medium text-ink-dim">
              {data.dbProfile}
            </span>
          )}
          {href && (
            <a
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              title="Open in Databricks"
              className="inline-flex h-5 w-5 items-center justify-center rounded text-ink-dim hover:bg-surface hover:text-ink"
              onClick={(e) => e.stopPropagation()}
            >
              <ExternalLink size={12} />
            </a>
          )}
          {canIngest && (
            <button
              type="button"
              disabled={ingesting}
              className={clsx(
                "inline-flex h-5 w-5 items-center justify-center rounded text-ink-dim hover:bg-surface hover:text-ink",
                ingesting && "animate-pulse opacity-60",
              )}
              title="Fetch & open in Assets"
              onClick={(e) => {
                e.stopPropagation();
                void ingestAndOpen();
              }}
            >
              <Download size={13} />
            </button>
          )}
          {data.sourceRemoteId != null && ASSETS_PAGE_KINDS.has(data.kind) && (
            <button
              type="button"
              className="inline-flex h-5 w-5 items-center justify-center rounded text-ink-dim hover:bg-surface hover:text-ink"
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
