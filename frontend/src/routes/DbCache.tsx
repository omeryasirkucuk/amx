/**
 * DbCache — Studio's view into the SQLite catalog caches.
 *
 * Mirrors the REPL ``/db cache-{show,stats,clear}`` surface so a user
 * can see exactly what AMX has indexed per (profile, database) and
 * flush specific scopes without dropping to the terminal. Three
 * pieces:
 *
 * - Stats cards across the top — one per cache table (schemas /
 *   columns / catalog) with total rows + oldest fetch + % expired.
 * - DataTable in the middle — one row per (profile, database) pair
 *   with per-cache row counts and a per-row Clear button.
 * - Top-bar "Clear all" — fires a global flush behind a
 *   confirmation modal (the API rejects it without ``force=true``).
 */

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, RefreshCw, Trash2 } from "lucide-react";

import { Card, CardBody, CardHeader } from "../components/Card";
import PageHeader from "../components/PageHeader";
import Modal from "../components/Modal";
import DataTable, { type DataTableColumn } from "../components/ui/DataTable";
import { cn } from "../lib/cn";
import {
  dbCacheApi,
  type DbCacheRow,
  type DbCacheClearResponse,
  type DbCacheClearRequest,
} from "../lib/dbCacheApi";

const SHOW_KEY = ["db-cache", "show"] as const;
const STATS_KEY = ["db-cache", "stats"] as const;

function formatAge(epoch: number | null): string {
  if (epoch == null) return "—";
  const ageSeconds = Math.max(0, Date.now() / 1000 - epoch);
  if (ageSeconds < 60) return "just now";
  if (ageSeconds < 3600) return `${Math.floor(ageSeconds / 60)}m ago`;
  if (ageSeconds < 86_400) return `${Math.floor(ageSeconds / 3600)}h ago`;
  return `${Math.floor(ageSeconds / 86_400)}d ago`;
}

export default function DbCache() {
  const qc = useQueryClient();
  const [confirmTarget, setConfirmTarget] = useState<
    null | { scope: string; payload: DbCacheClearRequest }
  >(null);

  const show = useQuery({
    queryKey: SHOW_KEY,
    queryFn: () => dbCacheApi.show(),
    refetchOnWindowFocus: false,
  });
  const stats = useQuery({
    queryKey: STATS_KEY,
    queryFn: () => dbCacheApi.stats(),
    refetchOnWindowFocus: false,
  });

  const clear = useMutation({
    mutationFn: (payload: DbCacheClearRequest) => dbCacheApi.clear(payload),
    onSuccess: (resp: DbCacheClearResponse) => {
      // Invalidate every consumer of the cache state so the sidebar
      // tree + freshness pill catch up immediately.
      qc.invalidateQueries({ queryKey: SHOW_KEY });
      qc.invalidateQueries({ queryKey: STATS_KEY });
      qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
      qc.invalidateQueries({ queryKey: ["live-schemas"] });
      qc.invalidateQueries({ queryKey: ["live-catalogs"] });
      qc.invalidateQueries({ queryKey: ["live-databases"] });
      setConfirmTarget(null);
      // Stash the response on the global query cache so the toast /
      // banner can pick it up; we surface counts inline below.
      void resp;
    },
  });

  const rows = show.data?.rows ?? [];
  const statsPayload = stats.data;

  const columns: DataTableColumn<DbCacheRow>[] = [
    {
      id: "profile",
      header: "Profile",
      cell: (row) => row.profile,
      sortValue: (row) => row.profile,
      mono: true,
    },
    {
      id: "database",
      header: "Database",
      cell: (row) => row.database || "—",
      sortValue: (row) => row.database,
      mono: true,
    },
    {
      id: "schemas",
      header: "Schemas",
      cell: (row) => row.schemas_rows,
      sortValue: (row) => row.schemas_rows,
      align: "right",
      hideOnMobile: true,
    },
    {
      id: "columns",
      header: "Columns",
      cell: (row) => row.columns_rows,
      sortValue: (row) => row.columns_rows,
      align: "right",
      hideOnMobile: true,
    },
    {
      id: "catalog",
      header: "Catalog",
      cell: (row) => row.catalog_rows,
      sortValue: (row) => row.catalog_rows,
      align: "right",
    },
    {
      id: "last_fetch",
      header: "Last fetch",
      cell: (row) => formatAge(row.last_fetch),
      sortValue: (row) => row.last_fetch ?? 0,
      hideOnMobile: true,
    },
    {
      id: "actions",
      header: "",
      cell: (row) => (
        <button
          type="button"
          aria-label={`Clear cache for ${row.profile} / ${row.database || "default"}`}
          title="Clear this scope"
          onClick={() =>
            setConfirmTarget({
              scope: `${row.profile} / ${row.database || "(default)"}`,
              payload: { profile: row.profile, database: row.database || "" },
            })
          }
          className={cn(
            "inline-flex h-7 w-7 items-center justify-center rounded text-ink-dim",
            "hover:bg-surface-subtle hover:text-critical",
          )}
        >
          <Trash2 size={14} />
        </button>
      ),
      width: "w-12",
    },
  ];

  return (
    <div className="flex flex-col gap-4 px-6 py-6">
      <PageHeader
        title="DB cache"
        description="What AMX has indexed per (profile, database). Flush specific scopes or refresh the live view."
      />

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        {(["schemas", "columns", "catalog"] as const).map((key) => {
          const s = statsPayload?.[key];
          return (
            <Card key={key}>
              <CardHeader
                title={key === "catalog" ? "catalog_entities" : `${key}_cache`}
                description={
                  s
                    ? `${s.total_rows} rows · ${s.distinct_profiles} profile(s) · ${s.distinct_databases} database(s)`
                    : "—"
                }
              />
              <CardBody>
                <dl className="grid grid-cols-2 gap-y-1 text-[12px]">
                  <dt className="text-ink-dim">Oldest fetch</dt>
                  <dd className="text-right">{formatAge(s?.oldest_fetch ?? null)}</dd>
                  <dt className="text-ink-dim">Newest fetch</dt>
                  <dd className="text-right">{formatAge(s?.newest_fetch ?? null)}</dd>
                  {s?.ttl_aware ? (
                    <>
                      <dt className="text-ink-dim">Expired rows</dt>
                      <dd className="text-right">{s.expired_rows}</dd>
                    </>
                  ) : (
                    <>
                      <dt className="text-ink-dim">TTL</dt>
                      <dd className="text-right">none — rewritten by /sync</dd>
                    </>
                  )}
                </dl>
              </CardBody>
            </Card>
          );
        })}
      </div>

      <Card>
        <CardHeader
          title={`${rows.length} (profile, database) scope(s) cached`}
          actions={
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => {
                  show.refetch();
                  stats.refetch();
                }}
                disabled={show.isFetching || stats.isFetching}
                className={cn(
                  "inline-flex items-center gap-1 rounded border border-surface-border px-2 py-1 text-[12px]",
                  "hover:bg-surface-subtle disabled:opacity-50",
                )}
              >
                {show.isFetching || stats.isFetching ? (
                  <Loader2 size={12} className="animate-spin" />
                ) : (
                  <RefreshCw size={12} />
                )}
                Refresh
              </button>
              <button
                type="button"
                onClick={() =>
                  setConfirmTarget({
                    scope: "EVERY profile, EVERY database",
                    payload: { force: true },
                  })
                }
                className={cn(
                  "inline-flex items-center gap-1 rounded border border-critical/40 bg-critical-soft/30 px-2 py-1 text-[12px] text-critical",
                  "hover:bg-critical-soft/50",
                )}
              >
                <Trash2 size={12} />
                Clear all
              </button>
            </div>
          }
        />
        <CardBody>
          <DataTable
            columns={columns}
            rows={rows}
            rowKey={(row) => `${row.profile}:${row.database}`}
            isLoading={show.isLoading}
            error={show.error ? (show.error as Error).message : null}
            emptyState={
              <div className="px-4 py-6 text-center text-[13px] text-ink-dim">
                No cached rows yet. Run /sync from the sidebar refresh
                icons or the catalog pill to populate the cache.
              </div>
            }
            initialSort={{ id: "profile", direction: "asc" }}
          />
        </CardBody>
      </Card>

      <Modal
        open={!!confirmTarget}
        onClose={() => {
          if (!clear.isPending) setConfirmTarget(null);
        }}
        title="Clear DB cache?"
        description={
          confirmTarget ? (
            <span>
              This will DELETE every cached schema, column comment, and
              catalog row for <strong>{confirmTarget.scope}</strong>. The
              next live read or /sync will repopulate. Continue?
            </span>
          ) : null
        }
        footer={
          <div className="flex justify-end gap-2">
            <button
              type="button"
              onClick={() => setConfirmTarget(null)}
              disabled={clear.isPending}
              className="rounded border border-surface-border px-3 py-1 text-[12px] hover:bg-surface-subtle disabled:opacity-50"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={() => {
                if (confirmTarget) clear.mutate(confirmTarget.payload);
              }}
              disabled={clear.isPending}
              className={cn(
                "inline-flex items-center gap-1 rounded border border-critical/40 bg-critical-soft/40 px-3 py-1 text-[12px] text-critical",
                "hover:bg-critical-soft/60 disabled:opacity-50",
              )}
            >
              {clear.isPending ? (
                <Loader2 size={12} className="animate-spin" />
              ) : (
                <Trash2 size={12} />
              )}
              Clear
            </button>
          </div>
        }
      >
        {clear.isError && (
          <div className="rounded border border-critical/40 bg-critical-soft/30 px-3 py-2 text-[12px] text-critical">
            {(clear.error as Error).message}
          </div>
        )}
        {clear.isSuccess && (
          <div className="rounded border border-success/40 bg-success-soft/30 px-3 py-2 text-[12px] text-success">
            Cleared {clear.data.total} row(s):{" "}
            {Object.entries(clear.data.deleted)
              .map(([k, v]) => `${k} ${v}`)
              .join(" · ")}
          </div>
        )}
      </Modal>
    </div>
  );
}
