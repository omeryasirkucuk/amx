/**
 * LineagePanel — renders asset-to-asset lineage inside the detail drawer.
 *
 * Backed by ``GET /api/assets/{kind}/{id}/lineage`` which materialises
 * edges from ``asset_lineage_edges`` (job → notebook / pipeline /
 * query, pipeline → notebook / target table) plus the ``task_dag``
 * adjacency list for jobs.
 *
 * Renders two sections:
 *  - **Touches** — outgoing edges as clickable asset chips. Clicking a
 *    chip calls ``onOpenAsset`` so the drawer re-fetches that target
 *    in place. Asset rows whose lineage row was deleted between
 *    refreshes get a "(asset removed)" label.
 *  - **Task DAG** (jobs only) — an indented adjacency list showing
 *    which task depends on which. Drawn with plain text + an arrow
 *    so the panel has no graph-library dependency.
 *
 * The endpoint sets ``incoming`` too, but the v1 panel only renders
 * outgoing edges + the DAG — incoming gets surfaced when we add the
 * "Used by" reverse-lineage feature.
 */

import { useQuery } from "@tanstack/react-query";

import { api, type RemoteAssetKind, type RemoteAssetLineageEdge } from "../../lib/api";
import { cn } from "../../lib/cn";

interface Props {
  kind: RemoteAssetKind;
  assetId: string;
  profile: string;
  onOpenAsset?: (kind: RemoteAssetKind, id: string | number) => void;
}

const _EDGE_LABEL: Record<string, string> = {
  task_runs_notebook: "runs notebook",
  task_runs_pipeline: "runs pipeline",
  task_runs_query: "runs query",
  pipeline_includes_notebook: "includes notebook",
  pipeline_writes_table: "writes table",
};

const _CLICKABLE_KINDS: ReadonlySet<string> = new Set([
  "notebook",
  "pipeline",
  "query",
  "job",
  "streamlit",
  "stream",
]);

function kindBadge(kind: string): string {
  if (kind === "table") return "bg-warn/15 text-warn";
  return "bg-accent/15 text-accent";
}

export default function LineagePanel({
  kind,
  assetId,
  profile,
  onOpenAsset,
}: Props) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["asset-lineage", kind, assetId, profile],
    queryFn: () => api.getRemoteAssetLineage(kind, assetId, profile),
    enabled: Boolean(assetId && profile),
    staleTime: 30_000,
  });

  if (kind !== "job" && kind !== "pipeline") {
    // Other kinds don't have outgoing lineage today. Returning null
    // keeps the drawer body unchanged when there's nothing to show.
    return null;
  }

  if (isLoading) {
    return (
      <section>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
          Lineage
        </h3>
        <p className="text-xs text-ink-dim">Loading lineage…</p>
      </section>
    );
  }

  if (error) {
    return (
      <section>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
          Lineage
        </h3>
        <p className="text-xs text-ink-dim">
          Couldn't resolve lineage: {(error as Error).message}
        </p>
      </section>
    );
  }

  const outgoing = data?.outgoing ?? [];
  const taskDag = data?.task_dag ?? [];

  if (outgoing.length === 0 && taskDag.length === 0) {
    return null;
  }

  return (
    <section>
      <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
        Lineage
      </h3>

      {outgoing.length > 0 && (
        <div className="mb-3">
          <p className="mb-1.5 text-[11px] uppercase tracking-wide text-ink-dim">
            Touches ({outgoing.length})
          </p>
          <ul className="space-y-1">
            {outgoing.map((edge) => (
              <li key={`${edge.to_kind}:${edge.to_id}:${edge.edge_type}`}>
                <TouchesRow edge={edge} onOpenAsset={onOpenAsset} />
              </li>
            ))}
          </ul>
        </div>
      )}

      {taskDag.length > 0 && (
        <div>
          <p className="mb-1.5 text-[11px] uppercase tracking-wide text-ink-dim">
            Task DAG ({taskDag.length})
          </p>
          <ul className="space-y-1 rounded-md border border-border bg-surface-subtle p-2 font-mono text-xs">
            {taskDag.map((entry, idx) => (
              <li key={idx} className="flex items-center gap-2">
                <span className="text-ink">{entry.from_task}</span>
                <span className="text-ink-dim">→</span>
                <span className="text-ink">{entry.to_task}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </section>
  );
}

function TouchesRow({
  edge,
  onOpenAsset,
}: {
  edge: RemoteAssetLineageEdge;
  onOpenAsset?: (kind: RemoteAssetKind, id: string | number) => void;
}) {
  const targetKind = String(edge.to_kind || "");
  const targetId = edge.to_id;
  const clickable =
    targetId != null && onOpenAsset && _CLICKABLE_KINDS.has(targetKind);
  const label = edge.to_name || (targetId != null ? `#${targetId}` : "(unknown)");
  const subtitle = edge.to_path || "";
  const verb = _EDGE_LABEL[edge.edge_type] ?? edge.edge_type;

  const inner = (
    <>
      <span
        className={cn(
          "mr-2 shrink-0 rounded px-1.5 py-px text-[10px] font-medium uppercase tracking-wide",
          kindBadge(targetKind),
        )}
      >
        {targetKind}
      </span>
      <span className="truncate font-mono text-xs text-ink">{label}</span>
      {subtitle && (
        <span className="ml-2 truncate font-mono text-[11px] text-ink-dim">
          {subtitle}
        </span>
      )}
      <span className="ml-auto shrink-0 text-[10px] uppercase tracking-wide text-ink-dim">
        {verb}
      </span>
    </>
  );

  if (clickable) {
    return (
      <button
        type="button"
        onClick={() =>
          onOpenAsset!(targetKind as RemoteAssetKind, targetId as number)
        }
        className="flex w-full items-center gap-1 rounded-md border border-border bg-surface px-2.5 py-1.5 text-left transition-colors hover:border-accent/40 hover:bg-surface-subtle"
      >
        {inner}
      </button>
    );
  }

  return (
    <div className="flex w-full items-center gap-1 rounded-md border border-border bg-surface px-2.5 py-1.5">
      {inner}
    </div>
  );
}
