/**
 * AssetDetailDrawer — slide-in panel showing full remote-asset detail.
 *
 * Opens on row click. Fetches api.getRemoteAsset(kind, id) and renders:
 *  - Header: name, platform, last_modified_at, owner
 *  - Notebooks: parsed .ipynb cells with cell_type labels
 *  - Queries: sql_text in <pre>
 *  - Everything else: definition list of raw key/value pairs
 *  - Downstream tables: links to the catalog table page
 */

import { useEffect, useState } from "react";
import { Check, Copy, Loader2, Trash2, X } from "lucide-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { api, ApiError, type RemoteAssetKind } from "../../lib/api";
import { cn } from "../../lib/cn";
import AlertDialog from "../ui/AlertDialog";
import LineagePanel from "./LineagePanel";

interface Props {
  open: boolean;
  onClose: () => void;
  kind: RemoteAssetKind;
  assetId: string;
  profile: string;
  /** Optional: swap the drawer to a sibling asset (e.g. job task → notebook). */
  onOpenAsset?: (kind: RemoteAssetKind, id: string | number) => void;
}

interface NotebookCell {
  cell_type?: string;
  source?: string | string[];
  outputs?: unknown[];
  metadata?: { language?: string };
}

function parseNotebook(text: string): NotebookCell[] | null {
  try {
    const nb = JSON.parse(text) as { cells?: NotebookCell[] };
    return Array.isArray(nb.cells) ? nb.cells : null;
  } catch {
    return null;
  }
}

function cellSource(cell: NotebookCell): string {
  if (typeof cell.source === "string") return cell.source;
  if (Array.isArray(cell.source)) return cell.source.join("");
  return "";
}

function cellLanguage(cell: NotebookCell): string | undefined {
  return cell.metadata?.language;
}

function NotebookCellView({ cell, index }: { cell: NotebookCell; index: number }) {
  const source = cellSource(cell);
  const lang = cellLanguage(cell);
  const isMarkdown = cell.cell_type === "markdown";
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(source);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // clipboard unavailable (no HTTPS, no permission) — silent
    }
  }

  return (
    <div className="rounded-md border border-border bg-surface-subtle">
      <div className="flex items-center gap-2 border-b border-border px-3 py-1.5">
        <span className="rounded bg-surface px-1.5 py-px text-[10px] font-mono uppercase text-ink-dim">
          {cell.cell_type ?? "unknown"}
        </span>
        {lang && lang !== cell.cell_type ? (
          <span className="rounded bg-surface px-1.5 py-px text-[10px] font-mono text-ink-dim">
            {lang}
          </span>
        ) : null}
        <span className="text-[11px] text-ink-dim">Cell {index + 1}</span>
        <button
          type="button"
          onClick={copy}
          aria-label="Copy cell"
          title={copied ? "Copied" : "Copy cell"}
          className="ml-auto rounded p-1 text-ink-dim hover:bg-surface hover:text-ink"
        >
          {copied ? <Check size={12} /> : <Copy size={12} />}
        </button>
      </div>
      {!source ? (
        <p className="p-3 text-xs text-ink-dim">(empty)</p>
      ) : isMarkdown ? (
        <div className="prose prose-sm prose-neutral dark:prose-invert max-w-none px-3 py-2.5 text-xs text-ink">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>{source}</ReactMarkdown>
        </div>
      ) : (
        <pre className="overflow-x-auto p-3 text-xs text-ink whitespace-pre-wrap">
          {source}
        </pre>
      )}
    </div>
  );
}

// ── Job / Pipeline / Streamlit detail sub-renderers ────────────────────────

interface JobTask {
  task_key: string;
  task_type: string;
  notebook_path?: string | null;
  notebook_name?: string | null;
  notebook_id_fk?: number | null;
  sql_warehouse_id?: string | null;
  depends_on?: string[];
}

interface JobRun {
  run_id: number;
  state_result: string;
  start_time: string;
  end_time?: string | null;
  duration_ms?: number | null;
}

function runStateBadgeClass(state: string): string {
  if (state === "SUCCESS") return "bg-positive/15 text-positive";
  if (state === "FAILED" || state === "CANCELED") return "bg-critical/15 text-critical";
  if (state === "RUNNING") return "bg-accent/15 text-accent";
  return "bg-surface-subtle text-ink-dim";
}

function formatDuration(ms: number | null | undefined): string {
  if (!ms || ms <= 0) return "—";
  if (ms < 1000) return `${ms}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${Math.round(s % 60)}s`;
}

function JobDetail({
  data,
  onOpenAsset,
}: {
  data: Record<string, unknown>;
  onOpenAsset?: (kind: RemoteAssetKind, id: string | number) => void;
}) {
  const tasks = (data.tasks as JobTask[] | undefined) ?? [];
  const runs = (data.recent_runs as JobRun[] | undefined) ?? [];
  const successRate = data.success_rate_30d as number | null | undefined;
  return (
    <>
      {/* Schedule strip */}
      <section className="flex flex-wrap items-center gap-2 rounded-md border border-border bg-surface-subtle px-3 py-2 text-xs">
        {data.schedule_cron ? (
          <span className="font-mono text-ink">
            <span className="text-ink-dim">cron:</span> {String(data.schedule_cron)}
            {data.schedule_timezone ? ` · ${String(data.schedule_timezone)}` : ""}
          </span>
        ) : (
          <span className="text-ink-dim">No schedule</span>
        )}
        {data.schedule_pause_status ? (
          <span
            className={cn(
              "rounded px-1.5 py-px text-[10px] font-medium uppercase tracking-wide",
              data.schedule_pause_status === "UNPAUSED"
                ? "bg-positive/15 text-positive"
                : "bg-surface-subtle text-ink-dim",
            )}
          >
            {String(data.schedule_pause_status)}
          </span>
        ) : null}
        {successRate != null ? (
          <span className="text-ink-dim">
            success 30d: <span className="font-mono text-ink">{(successRate * 100).toFixed(0)}%</span>
          </span>
        ) : null}
        {data.creator_user_name ? (
          <span className="text-ink-dim">
            owner: <span className="font-mono text-ink">{String(data.creator_user_name)}</span>
          </span>
        ) : null}
      </section>

      {/* Tasks */}
      <section>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
          Tasks ({tasks.length})
        </h3>
        {tasks.length === 0 ? (
          <p className="text-xs text-ink-dim">No tasks recorded.</p>
        ) : (
          <div className="overflow-x-auto rounded-md border border-border">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border text-left text-ink-dim">
                  <th className="px-2 py-1.5 font-medium">Task</th>
                  <th className="px-2 py-1.5 font-medium">Type</th>
                  <th className="px-2 py-1.5 font-medium">Target</th>
                  <th className="px-2 py-1.5 font-medium">Depends on</th>
                </tr>
              </thead>
              <tbody>
                {tasks.map((t) => {
                  const targetLabel =
                    t.notebook_name ?? t.notebook_path ?? t.sql_warehouse_id ?? "—";
                  const targetIsClickableNotebook =
                    t.notebook_id_fk != null && onOpenAsset != null;
                  return (
                    <tr key={t.task_key} className="border-b border-border last:border-0">
                      <td className="px-2 py-1.5 font-mono text-ink">{t.task_key}</td>
                      <td className="px-2 py-1.5 text-ink-dim">{t.task_type}</td>
                      <td className="px-2 py-1.5 font-mono text-ink-dim">
                        {targetIsClickableNotebook ? (
                          <button
                            type="button"
                            onClick={() =>
                              onOpenAsset!("notebook", t.notebook_id_fk as number)
                            }
                            className="text-accent hover:underline"
                            title={t.notebook_path ?? undefined}
                          >
                            {targetLabel}
                          </button>
                        ) : (
                          targetLabel
                        )}
                      </td>
                      <td className="px-2 py-1.5 font-mono text-ink-dim">
                        {t.depends_on && t.depends_on.length > 0
                          ? t.depends_on.join(", ")
                          : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Recent runs */}
      <section>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
          Recent runs ({runs.length})
        </h3>
        {runs.length === 0 ? (
          <p className="text-xs text-ink-dim">No runs recorded.</p>
        ) : (
          <ul className="space-y-1.5">
            {runs.map((r) => (
              <li
                key={r.run_id}
                className="flex items-center gap-3 rounded-md border border-border px-2.5 py-1.5 text-xs"
              >
                <span
                  className={cn(
                    "rounded px-1.5 py-px text-[10px] font-medium uppercase tracking-wide",
                    runStateBadgeClass(r.state_result),
                  )}
                >
                  {r.state_result}
                </span>
                <span className="font-mono text-ink">#{r.run_id}</span>
                <span className="text-ink-dim">{r.start_time}</span>
                <span className="ml-auto font-mono text-ink-dim">
                  {formatDuration(r.duration_ms)}
                </span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </>
  );
}

interface PipelineLibrary {
  notebook?: { path?: string };
  file?: { path?: string };
  jar?: string;
  whl?: string;
  maven?: { coordinates?: string };
}

function PipelineDetail({ data }: { data: Record<string, unknown> }) {
  const libraries = (data.libraries as PipelineLibrary[] | undefined) ?? [];
  const latest =
    (data.latest_update as { state?: string; created_at?: string } | undefined) ?? {};
  return (
    <>
      <section className="flex flex-wrap items-center gap-2 rounded-md border border-border bg-surface-subtle px-3 py-2 text-xs">
        {data.target_schema ? (
          <span className="text-ink-dim">
            target: <span className="font-mono text-ink">{String(data.target_schema)}</span>
          </span>
        ) : null}
        {data.edition ? (
          <span className="rounded bg-surface px-1.5 py-px text-[10px] font-medium uppercase tracking-wide text-ink-dim">
            {String(data.edition)}
          </span>
        ) : null}
        {data.continuous ? (
          <span className="rounded bg-accent/15 px-1.5 py-px text-[10px] font-medium uppercase tracking-wide text-accent">
            continuous
          </span>
        ) : null}
        {data.photon ? (
          <span className="rounded bg-accent/15 px-1.5 py-px text-[10px] font-medium uppercase tracking-wide text-accent">
            photon
          </span>
        ) : null}
      </section>

      <section>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
          Libraries ({libraries.length})
        </h3>
        {libraries.length === 0 ? (
          <p className="text-xs text-ink-dim">No libraries declared.</p>
        ) : (
          <ul className="space-y-1">
            {libraries.map((lib, idx) => (
              <li
                key={idx}
                className="rounded-md border border-border px-2.5 py-1.5 text-xs"
              >
                {lib.notebook?.path ? (
                  <>
                    <span className="mr-2 rounded bg-accent/15 px-1.5 py-px text-[10px] font-medium uppercase tracking-wide text-accent">
                      notebook
                    </span>
                    <span className="font-mono text-ink">{lib.notebook.path}</span>
                  </>
                ) : lib.file?.path ? (
                  <>
                    <span className="mr-2 rounded bg-surface px-1.5 py-px text-[10px] font-medium uppercase tracking-wide text-ink-dim">
                      file
                    </span>
                    <span className="font-mono text-ink">{lib.file.path}</span>
                  </>
                ) : lib.jar ? (
                  <>
                    <span className="mr-2 rounded bg-surface px-1.5 py-px text-[10px] font-medium uppercase tracking-wide text-ink-dim">
                      jar
                    </span>
                    <span className="font-mono text-ink">{lib.jar}</span>
                  </>
                ) : (
                  <span className="font-mono text-ink-dim">{JSON.stringify(lib)}</span>
                )}
              </li>
            ))}
          </ul>
        )}
      </section>

      {(latest.state || latest.created_at) && (
        <section className="flex flex-wrap items-center gap-2 rounded-md border border-border px-3 py-2 text-xs">
          <span className="text-ink-dim">latest update:</span>
          {latest.state ? (
            <span
              className={cn(
                "rounded px-1.5 py-px text-[10px] font-medium uppercase tracking-wide",
                runStateBadgeClass(latest.state),
              )}
            >
              {latest.state}
            </span>
          ) : null}
          {latest.created_at ? (
            <span className="font-mono text-ink-dim">{latest.created_at}</span>
          ) : null}
        </section>
      )}
    </>
  );
}

function StreamlitDetail({ data }: { data: Record<string, unknown> }) {
  const launch =
    (data.launch_info as
      | { main_file?: string; root_location?: string; query_warehouse?: string }
      | undefined) ?? {};
  return (
    <>
      <section className="flex flex-wrap items-center gap-2 rounded-md border border-border bg-surface-subtle px-3 py-2 text-xs">
        {data.qualified_name ? (
          <span className="font-mono text-ink">{String(data.qualified_name)}</span>
        ) : null}
        {launch.query_warehouse ? (
          <span className="text-ink-dim">
            warehouse: <span className="font-mono text-ink">{launch.query_warehouse}</span>
          </span>
        ) : null}
      </section>

      <section>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
          Launch info
        </h3>
        <dl className="grid grid-cols-1 gap-y-1.5 sm:grid-cols-2">
          {launch.main_file ? (
            <div className="flex flex-col gap-0.5">
              <dt className="text-[10px] font-medium uppercase tracking-wide text-ink-dim">
                Main file
              </dt>
              <dd className="break-all text-xs font-mono text-ink">{launch.main_file}</dd>
            </div>
          ) : null}
          {launch.root_location ? (
            <div className="flex flex-col gap-0.5">
              <dt className="text-[10px] font-medium uppercase tracking-wide text-ink-dim">
                Root location
              </dt>
              <dd className="break-all text-xs font-mono text-ink">{launch.root_location}</dd>
            </div>
          ) : null}
        </dl>
      </section>
    </>
  );
}

export default function AssetDetailDrawer({
  open,
  onClose,
  kind,
  assetId,
  profile,
  onOpenAsset,
}: Props) {
  // Trap ESC to close
  useEffect(() => {
    if (!open) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKey, true);
    return () => window.removeEventListener("keydown", onKey, true);
  }, [open, onClose]);

  const { data, isLoading, error } = useQuery({
    queryKey: ["remote-asset", kind, assetId],
    queryFn: () => api.getRemoteAsset(kind, assetId),
    enabled: open && !!assetId,
    staleTime: 60_000,
  });

  const queryClient = useQueryClient();
  const [confirmOpen, setConfirmOpen] = useState(false);
  const deleteMutation = useMutation({
    mutationFn: () => api.deleteRemoteAsset(kind, assetId),
    onSuccess: () => {
      // Invalidate every cached list of remote assets so the active tab
      // refetches and drops the row immediately. Prefix-match catches all
      // (profile, kind) combinations; a narrower key wouldn't fire because
      // the list query is keyed (profile, kind) and the drawer doesn't
      // always know the profile.
      queryClient.invalidateQueries({ queryKey: ["remote-assets"] });
      queryClient.removeQueries({ queryKey: ["remote-asset", kind, assetId] });
      setConfirmOpen(false);
      onClose();
    },
  });

  const displayName =
    (data && (data["name"] as string | undefined)) ||
    (data && (data["qualified_name"] as string | undefined)) ||
    `${kind} #${assetId}`;

  if (!open) return null;

  // Keys to omit from the generic definition list (shown in header or
  // dedicated sections already).
  const SKIP_KEYS = new Set([
    "id",
    "name",
    "platform",
    "owner",
    "last_modified_at",
    "source_text",
    "sql_text",
    "downstream_tables",
    // Job: handled in dedicated Schedule / Tasks / Recent runs sections.
    "tasks",
    "recent_runs",
    "schedule_cron",
    "schedule_pause_status",
    "schedule_timezone",
    "success_rate_30d",
    "last_run_status",
    "last_run_started_at",
    "creator_user_name",
    "email_notifications_json",
    "tags_json",
    "job_id",
    "max_concurrent_runs",
    // Pipeline: handled in dedicated Libraries / Latest update sections.
    "libraries",
    "libraries_json",
    "latest_update",
    "latest_update_state",
    "latest_update_creation_time",
    "target_schema",
    "edition",
    "continuous",
    "photon",
    "pipeline_id",
    // Streamlit: handled in dedicated Launch section.
    "launch_info",
    "main_file",
    "root_location",
    "query_warehouse",
    "qualified_name",
    "last_altered_at",
  ]);

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 z-40 bg-ink/20"
        aria-hidden="true"
        onClick={onClose}
      />
      {/* Panel */}
      <aside
        className={cn(
          "fixed inset-y-0 right-0 z-50 flex flex-col",
          "w-full sm:w-[640px]",
          "bg-surface-raised shadow-2xl border-l border-border",
          "overflow-y-auto",
        )}
        role="dialog"
        aria-modal="true"
        aria-label="Asset detail"
      >
        {/* Header */}
        <div className="flex shrink-0 items-start justify-between gap-4 border-b border-border px-5 py-4">
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <span className="rounded bg-accent/10 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-accent">
                {kind}
              </span>
              {data?.platform && (
                <span className="text-xs text-ink-dim">{data.platform}</span>
              )}
            </div>
            <h2 className="mt-1 truncate text-base font-semibold text-ink">
              {data?.name ?? assetId}
            </h2>
            {(() => {
              // PR-B: surface the disambiguating path right under the
              // title so two same-name assets in different folders /
              // schemas read distinctly. We pick the first non-empty
              // path-like field; the row carries them via the index
              // signature on RemoteAssetRow.
              if (!data) return null;
              const path =
                (data["workspace_path"] as string | null | undefined) ||
                ((data["qualified_name"] as string | null | undefined) &&
                data["qualified_name"] !== data.name
                  ? (data["qualified_name"] as string)
                  : "") ||
                (data["target_schema"] as string | null | undefined) ||
                "";
              if (!path) return null;
              return (
                <p className="mt-0.5 break-all font-mono text-[11px] text-ink-dim">
                  {path}
                </p>
              );
            })()}
            <div className="mt-0.5 flex flex-wrap gap-3 text-xs text-ink-dim">
              {data?.owner && <span>Owner: {data.owner}</span>}
              {data?.last_modified_at && (
                <span>Modified: {data.last_modified_at}</span>
              )}
            </div>
          </div>
          <div className="flex shrink-0 items-center gap-1">
            <button
              type="button"
              onClick={() => setConfirmOpen(true)}
              disabled={deleteMutation.isPending || !data}
              aria-label="Delete asset"
              title="Delete asset"
              className={cn(
                "rounded-md p-1.5",
                "text-critical hover:bg-critical/10",
                "disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-transparent",
              )}
            >
              {deleteMutation.isPending ? (
                <Loader2 size={16} className="animate-spin" />
              ) : (
                <Trash2 size={16} />
              )}
            </button>
            <button
              type="button"
              onClick={onClose}
              aria-label="Close detail panel"
              className="rounded-md p-1.5 text-ink-dim hover:bg-surface-subtle hover:text-ink"
            >
              <X size={16} />
            </button>
          </div>
        </div>
        {deleteMutation.error && (
          <div className="mx-5 mb-2 rounded-md bg-critical/10 px-3 py-2 text-xs text-critical">
            Failed to delete: {(deleteMutation.error as Error).message}
          </div>
        )}
        {/* Body */}
        <div className="flex-1 px-5 py-4">
          {isLoading && (
            <p className="text-sm text-ink-dim">Loading…</p>
          )}
          {error && (() => {
            // A 404 here means the search hit pointed at a row that is
            // no longer in SQLite — most often a stale Chroma vector
            // from a previous ingest. ``reindex_profile`` now prunes
            // these as a final pass, but until the next refresh runs
            // the drawer still sees them. Render a soft empty-state
            // panel instead of a red error toast so the user can act.
            const apiErr = error as unknown;
            const isNotFound =
              apiErr instanceof ApiError && apiErr.status === 404;
            if (isNotFound) {
              return (
                <div className="rounded-md border border-border bg-surface-subtle px-4 py-6 text-center">
                  <p className="text-sm font-medium text-ink">
                    Asset no longer available
                  </p>
                  <p className="mt-1 text-xs text-ink-dim">
                    This {kind} may have been removed since the search
                    index was last refreshed. Try searching again, or
                    re-run <span className="font-mono">/db assets reindex</span>
                    {" "}to drop the stale entry.
                  </p>
                  <button
                    type="button"
                    onClick={onClose}
                    className="mt-3 rounded-md border border-border bg-surface px-3 py-1 text-xs font-medium text-ink hover:bg-surface-subtle"
                  >
                    Close
                  </button>
                </div>
              );
            }
            return (
              <p className="rounded-md bg-critical/10 px-3 py-2 text-sm text-critical">
                {(error as Error).message}
              </p>
            );
          })()}
          {data && (
            <div className="space-y-5">
              {/* Notebook cells */}
              {kind === "notebook" && data.source_text && (() => {
                const cells = parseNotebook(data.source_text);
                if (!cells) {
                  return (
                    <section>
                      <h3 className="mb-1.5 text-xs font-semibold uppercase tracking-wide text-ink-dim">
                        Source
                      </h3>
                      <pre className="overflow-x-auto rounded-md bg-surface-subtle p-3 text-xs text-ink">
                        {data.source_text}
                      </pre>
                    </section>
                  );
                }
                return (
                  <section>
                    <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
                      Cells ({cells.length})
                    </h3>
                    <div className="space-y-3">
                      {cells.map((cell, idx) => (
                        <NotebookCellView key={idx} cell={cell} index={idx} />
                      ))}
                    </div>
                  </section>
                );
              })()}

              {/* SQL query */}
              {kind === "query" && data.sql_text && (
                <section>
                  <h3 className="mb-1.5 text-xs font-semibold uppercase tracking-wide text-ink-dim">
                    SQL
                  </h3>
                  <pre className="overflow-x-auto rounded-md bg-surface-subtle p-3 text-xs text-ink whitespace-pre-wrap">
                    {data.sql_text}
                  </pre>
                </section>
              )}

              {/* Job detail */}
              {kind === "job" && <JobDetail data={data} onOpenAsset={onOpenAsset} />}

              {/* Pipeline detail */}
              {kind === "pipeline" && <PipelineDetail data={data} />}

              {/* Lineage panel (jobs + pipelines only) — clickable
                  chips swap the drawer to the touched asset. */}
              {(kind === "job" || kind === "pipeline") && (
                <LineagePanel
                  kind={kind}
                  assetId={assetId}
                  profile={profile}
                  onOpenAsset={onOpenAsset}
                />
              )}

              {/* Streamlit detail */}
              {kind === "streamlit" && <StreamlitDetail data={data} />}

              {/* Generic definition list for all other kinds / extra fields */}
              {(() => {
                const entries = Object.entries(data).filter(
                  ([k, v]) => !SKIP_KEYS.has(k) && v != null,
                );
                if (entries.length === 0) return null;
                return (
                  <section>
                    <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
                      Details
                    </h3>
                    <dl className="grid grid-cols-1 gap-y-1.5 sm:grid-cols-2">
                      {entries.map(([k, v]) => (
                        <div key={k} className="flex flex-col gap-0.5">
                          <dt className="text-[10px] font-medium uppercase tracking-wide text-ink-dim">
                            {k.replace(/_/g, " ")}
                          </dt>
                          <dd className="break-all text-xs font-mono text-ink">
                            {typeof v === "object"
                              ? JSON.stringify(v, null, 2)
                              : String(v)}
                          </dd>
                        </div>
                      ))}
                    </dl>
                  </section>
                );
              })()}

              {/* Downstream tables */}
              {data.downstream_tables && data.downstream_tables.length > 0 && (
                <section>
                  <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-dim">
                    Downstream tables ({data.downstream_tables.length})
                  </h3>
                  <ul className="space-y-1">
                    {data.downstream_tables.map((dt, idx) => {
                      // Best-effort link: fqn is usually "schema.table" or
                      // "database.schema.table". Build a /db/{profile}/...
                      // path from the fqn segments.
                      const parts = dt.fqn.split(".");
                      let linkPath: string;
                      if (parts.length >= 3) {
                        linkPath = `/db/${encodeURIComponent(profile)}/${parts
                          .map(encodeURIComponent)
                          .join("/")}`;
                      } else if (parts.length === 2) {
                        linkPath = `/db/${encodeURIComponent(profile)}/_/${parts
                          .map(encodeURIComponent)
                          .join("/")}`;
                      } else {
                        linkPath = `/db/${encodeURIComponent(profile)}`;
                      }
                      return (
                        <li key={idx}>
                          <Link
                            to={linkPath}
                            className="font-mono text-xs text-accent hover:underline"
                          >
                            {dt.fqn}
                          </Link>
                        </li>
                      );
                    })}
                  </ul>
                </section>
              )}
            </div>
          )}
        </div>
      </aside>
      <AlertDialog
        open={confirmOpen}
        onClose={() => setConfirmOpen(false)}
        onConfirm={() => deleteMutation.mutate()}
        tone="danger"
        loading={deleteMutation.isPending}
        confirmLabel="Delete"
        title={`Delete ${kind} "${displayName}"`}
        description={
          <span>
            Removes the row from AMX's catalog. The source {kind} on the
            platform is <strong>untouched</strong>. Lineage edges that
            reference this {kind} are also removed.
          </span>
        }
      />
    </>
  );
}
