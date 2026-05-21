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

import { useEffect } from "react";
import { X } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import { api, type RemoteAssetKind } from "../../lib/api";
import { cn } from "../../lib/cn";

interface Props {
  open: boolean;
  onClose: () => void;
  kind: RemoteAssetKind;
  assetId: string;
  profile: string;
}

interface NotebookCell {
  cell_type?: string;
  source?: string | string[];
  outputs?: unknown[];
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

export default function AssetDetailDrawer({
  open,
  onClose,
  kind,
  assetId,
  profile,
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
            <div className="mt-0.5 flex flex-wrap gap-3 text-xs text-ink-dim">
              {data?.owner && <span>Owner: {data.owner}</span>}
              {data?.last_modified_at && (
                <span>Modified: {data.last_modified_at}</span>
              )}
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close detail panel"
            className="shrink-0 rounded-md p-1.5 text-ink-dim hover:bg-surface-subtle hover:text-ink"
          >
            <X size={16} />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 px-5 py-4">
          {isLoading && (
            <p className="text-sm text-ink-dim">Loading…</p>
          )}
          {error && (
            <p className="rounded-md bg-critical/10 px-3 py-2 text-sm text-critical">
              {(error as Error).message}
            </p>
          )}
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
                        <div
                          key={idx}
                          className="rounded-md border border-border bg-surface-subtle"
                        >
                          <div className="flex items-center gap-2 border-b border-border px-3 py-1.5">
                            <span className="rounded bg-surface px-1.5 py-px text-[10px] font-mono text-ink-dim">
                              {cell.cell_type ?? "unknown"}
                            </span>
                            <span className="text-[11px] text-ink-dim">
                              Cell {idx + 1}
                            </span>
                          </div>
                          <pre className="overflow-x-auto p-3 text-xs text-ink whitespace-pre-wrap">
                            {cellSource(cell) || <span className="text-ink-dim">(empty)</span>}
                          </pre>
                        </div>
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
    </>
  );
}
