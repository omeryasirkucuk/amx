/**
 * DbCache — unified Catalog Cache management page.
 *
 * One surface for every catalog-cache concern so the user never has
 * to bounce between separate pages:
 *
 * 1. **Stats cards** — totals + oldest fetch + TTL state per cache
 *    table (schemas / columns / catalog).
 * 2. **Profile freshness** — per-profile last sync timestamp and
 *    manual "Sync now" / "Retry" / "Sync all" controls.
 * 3. **Cache inventory** — per-(profile, database) row counts with
 *    a per-row Clear button + top-bar "Clear all".
 * 4. **Scheduled refreshes** — recurring or one-shot cache-refresh
 *    jobs (kind='cache_refresh' in scheduled_runs). Create, edit,
 *    pause/resume/run-now/delete from here.
 *
 * Reachable from three entry points (all point at /db-cache):
 *  - DB cache link in the left sidebar
 *  - "Catalog refreshes" button on the Runs page header
 *  - "Manage refreshes →" link in the Catalog Freshness pill dropdown
 */

import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import {
  CalendarPlus,
  DatabaseZap,
  ExternalLink,
  Loader2,
  PauseCircle,
  Pencil,
  PlayCircle,
  RefreshCw,
  Trash2,
} from "lucide-react";

import { Card, CardBody, CardHeader } from "../components/Card";
import PageHeader from "../components/PageHeader";
import Modal from "../components/Modal";
import {
  Badge,
  type BadgeTone,
  Button,
  DataTable,
  type DataTableColumn,
  Dialog,
  IconButton,
  useToast,
} from "../components/ui";
import { cn } from "../lib/cn";
import {
  dbCacheApi,
  type DbCacheRow,
  type DbCacheClearResponse,
  type DbCacheClearRequest,
} from "../lib/dbCacheApi";
import { api, apiFetch, type ScheduleRow } from "../lib/api";
import { recurrenceLabel } from "../lib/cron";
import ScheduleCacheRefreshDialog from "../components/ScheduleCacheRefreshDialog";
import SyncScopeDialog from "../components/SyncScopeDialog";

const SHOW_KEY = ["db-cache", "show"] as const;
const STATS_KEY = ["db-cache", "stats"] as const;

interface FreshnessProfile {
  profile: string;
  entity_count: number;
  last_synced_at: number | null;
  age_seconds: number | null;
  stale: boolean;
  state: "none" | "syncing" | "done" | "failed";
  total_tables: number;
  processed_tables: number;
  started_at: number | null;
  finished_at: number | null;
  last_full_sync_at: number | null;
  last_error: string;
}

interface FreshnessResponse {
  profiles: FreshnessProfile[];
  stale_profile_count: number;
  syncing_profile_count: number;
  stale_after_seconds: number;
}

const STATUS_TONE: Record<string, BadgeTone> = {
  pending: "neutral",
  paused: "warning",
  running: "info",
  completed: "positive",
  failed: "critical",
  missed: "warning",
  cancelled: "neutral",
};

function StatusChip({ status }: { status: string }) {
  return <Badge tone={STATUS_TONE[status] ?? "neutral"}>{status}</Badge>;
}

function formatAge(epoch: number | null): string {
  if (epoch == null) return "—";
  const ageSeconds = Math.max(0, Date.now() / 1000 - epoch);
  if (ageSeconds < 60) return "just now";
  if (ageSeconds < 3600) return `${Math.floor(ageSeconds / 60)}m ago`;
  if (ageSeconds < 86_400) return `${Math.floor(ageSeconds / 3600)}h ago`;
  return `${Math.floor(ageSeconds / 86_400)}d ago`;
}

function relativeAge(ageSec: number | null): string {
  if (ageSec == null) return "never";
  if (ageSec < 60) return "just now";
  if (ageSec < 3600) return `${Math.floor(ageSec / 60)} min ago`;
  if (ageSec < 86_400) return `${Math.floor(ageSec / 3600)} h ago`;
  return `${Math.floor(ageSec / 86_400)} d ago`;
}

function scopeSummary(scopeJson: string): string {
  try {
    const obj = JSON.parse(scopeJson) as {
      mode?: string;
      schemas?: string[];
      tables?: { schema?: string; table?: string }[];
      columns?: { schema?: string; table?: string; column?: string }[];
    };
    const mode = obj.mode || "all";
    if (mode === "all") return "Whole database";
    if (mode === "schemas") return `${(obj.schemas || []).length} schema(s)`;
    if (mode === "tables") return `${(obj.tables || []).length} table(s)`;
    if (mode === "columns") return `${(obj.columns || []).length} column(s)`;
    return mode;
  } catch {
    return "—";
  }
}

interface CatalogRefreshDetailDialogProps {
  open: boolean;
  row: ScheduleRow | null;
  onClose: () => void;
  onNavigate: (path: string) => void;
}

function DetailItem({
  label,
  children,
  className,
}: {
  label: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={className}>
      <div className="mb-1 text-xs uppercase tracking-wide text-ink-dim">
        {label}
      </div>
      <div>{children}</div>
    </div>
  );
}

function CatalogRefreshDetailDialog({
  open,
  row,
  onClose,
  onNavigate,
}: CatalogRefreshDetailDialogProps) {
  if (!row) return null;

  let scopePretty: string;
  try {
    scopePretty = JSON.stringify(JSON.parse(row.scope_json), null, 2);
  } catch {
    scopePretty = row.scope_json || "(empty)";
  }
  const firedAt =
    row.fired_at != null ? new Date(row.fired_at * 1000).toLocaleString() : "—";

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={`Cache refresh #${row.id} — ${row.name}`}
      size="lg"
    >
      <div className="space-y-4">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <DetailItem label="Status">
            <StatusChip status={row.status} />
          </DetailItem>
          <DetailItem label="Recurrence">
            <span className="font-mono text-xs text-ink">
              {recurrenceLabel(row.cron_expr ?? null)}
              {row.cron_expr && (
                <span className="ml-2 text-ink-dim">({row.cron_expr})</span>
              )}
            </span>
          </DetailItem>
          <DetailItem label="Next fire">
            <span className="text-ink">
              {row.fire_at_local}
              <span className="ml-2 text-xs text-ink-dim">{row.fire_at_tz}</span>
            </span>
          </DetailItem>
          <DetailItem label="Last fired">
            <span className="text-ink">{firedAt}</span>
          </DetailItem>
          <DetailItem label="DB profile">
            <span className="font-mono text-xs text-ink">{row.db_profile}</span>
          </DetailItem>
          <DetailItem label={row.catalog ? "Catalog" : "Database"}>
            <span className="font-mono text-xs text-ink">
              {row.catalog ?? row.database ?? "—"}
            </span>
          </DetailItem>
        </div>
        <DetailItem label="Scope">
          <pre className="max-h-60 overflow-auto rounded-md border border-border bg-surface-muted px-3 py-2 text-xs text-ink">
            {scopePretty}
          </pre>
        </DetailItem>
        {row.last_error && (
          <DetailItem label="Last error">
            <pre className="max-h-40 overflow-auto whitespace-pre-wrap rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-xs text-critical">
              {row.last_error}
            </pre>
          </DetailItem>
        )}
        <div className="flex flex-wrap items-center justify-end gap-2 pt-2">
          {row.triggered_run_id != null && (
            <Button
              variant="primary"
              size="md"
              leadingIcon={<ExternalLink size={14} />}
              onClick={() => {
                onNavigate(`/runs/${row.triggered_run_id}`);
                onClose();
              }}
            >
              View triggered run
            </Button>
          )}
          <Button variant="secondary" size="md" onClick={onClose}>
            Close
          </Button>
        </div>
      </div>
    </Dialog>
  );
}

export default function DbCache() {
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();

  // ── Inventory + stats (legacy DbCache content) ────────────────────
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
      qc.invalidateQueries({ queryKey: SHOW_KEY });
      qc.invalidateQueries({ queryKey: STATS_KEY });
      qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
      qc.invalidateQueries({ queryKey: ["live-schemas"] });
      qc.invalidateQueries({ queryKey: ["live-catalogs"] });
      qc.invalidateQueries({ queryKey: ["live-databases"] });
      setConfirmTarget(null);
      void resp;
    },
  });

  // ── Profile freshness (manual sync) ───────────────────────────────
  const freshnessQ = useQuery({
    queryKey: ["catalog-freshness"],
    queryFn: () => apiFetch<FreshnessResponse>("/api/catalog/freshness"),
    refetchInterval: (q) => {
      const d = q.state.data as FreshnessResponse | undefined;
      return d && d.syncing_profile_count > 0 ? 2_000 : 30_000;
    },
    retry: false,
  });
  const syncMut = useMutation({
    mutationFn: (target: string | null) =>
      apiFetch(
        target
          ? `/api/catalog/sync?profile=${encodeURIComponent(target)}`
          : "/api/catalog/sync",
        { method: "POST" },
      ),
    onSettled: () => {
      // Sync refills the cache, so every consumer of the cached state
      // (inventory totals, stats cards, freshness pill) needs to
      // re-paint. Without the extra invalidations the inventory table
      // sat at "5m ago" even right after the user clicked Sync now.
      qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
      qc.invalidateQueries({ queryKey: ["db-cache", "show"] });
      qc.invalidateQueries({ queryKey: ["db-cache", "stats"] });
      qc.invalidateQueries({ queryKey: ["live-schemas"] });
      qc.invalidateQueries({ queryKey: ["live-catalogs"] });
      qc.invalidateQueries({ queryKey: ["live-databases"] });
      window.setTimeout(() => {
        qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
        qc.invalidateQueries({ queryKey: ["db-cache", "show"] });
        qc.invalidateQueries({ queryKey: ["db-cache", "stats"] });
      }, 3000);
    },
    onError: (e) =>
      toast.push({
        tone: "error",
        title: e instanceof Error ? e.message : "Sync failed",
      }),
  });
  // Deep sync — full profile (columns + row counts), opt-in because it
  // runs profile_table + COUNT(*) per table. The plain Sync above is
  // skeleton-only (fast table inventory). Reuses the same freshness
  // state machine, so the pill + invalidations are identical.
  const deepSyncMut = useMutation({
    mutationFn: (target: string | null) =>
      apiFetch(
        target
          ? `/api/catalog/deep-sync?profile=${encodeURIComponent(target)}`
          : "/api/catalog/deep-sync",
        { method: "POST" },
      ),
    onSettled: () => {
      qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
      qc.invalidateQueries({ queryKey: ["db-cache", "show"] });
      qc.invalidateQueries({ queryKey: ["db-cache", "stats"] });
      qc.invalidateQueries({ queryKey: ["live-schemas"] });
      window.setTimeout(() => {
        qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
        qc.invalidateQueries({ queryKey: ["db-cache", "show"] });
      }, 3000);
    },
    onError: (e) =>
      toast.push({
        tone: "error",
        title: e instanceof Error ? e.message : "Deep sync failed",
      }),
  });
  const freshness = freshnessQ.data;
  const freshnessProfiles = freshness?.profiles ?? [];
  const anySyncing = (freshness?.syncing_profile_count ?? 0) > 0;

  // ── Scheduled refreshes ───────────────────────────────────────────
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [scheduleDialogOpen, setScheduleDialogOpen] = useState(false);
  const [syncScopeDialogOpen, setSyncScopeDialogOpen] = useState(false);
  const [editingSchedule, setEditingSchedule] = useState<ScheduleRow | null>(
    null,
  );
  const [detailRow, setDetailRow] = useState<ScheduleRow | null>(null);

  const scheduleStatusFilter =
    statusFilter === "active"
      ? "pending,paused,missed,running"
      : statusFilter === "past"
        ? "completed,failed,cancelled"
        : undefined;

  const schedulesQ = useQuery({
    queryKey: ["schedules", "cache_refresh", statusFilter],
    queryFn: () =>
      api.listSchedules({
        kind: "cache_refresh",
        status: scheduleStatusFilter,
      }),
    refetchInterval: (q) => {
      const rows =
        (q.state.data as { schedules?: { status: string }[] } | undefined)
          ?.schedules ?? [];
      const running = rows.some((r) => r.status === "running");
      return running ? 5000 : 15000;
    },
  });

  const invalidateSchedules = () => {
    qc.invalidateQueries({ queryKey: ["schedules"] });
    qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
  };
  const pauseMut = useMutation({
    mutationFn: (id: number) => api.pauseSchedule(id),
    onSuccess: invalidateSchedules,
  });
  const resumeMut = useMutation({
    mutationFn: (id: number) => api.resumeSchedule(id),
    onSuccess: invalidateSchedules,
  });
  const runNowMut = useMutation({
    mutationFn: (id: number) => api.runScheduleNow(id),
    onSuccess: () => {
      toast.push({ tone: "info", title: "Cache refresh fired" });
      invalidateSchedules();
    },
    onError: (e) =>
      toast.push({
        tone: "error",
        title: e instanceof Error ? e.message : "Fire failed",
      }),
  });
  const deleteScheduleMut = useMutation({
    mutationFn: (id: number) => api.deleteSchedule(id),
    onSuccess: () => {
      toast.push({ tone: "success", title: "Schedule deleted" });
      invalidateSchedules();
    },
  });
  const scheduleRows = schedulesQ.data?.schedules ?? [];

  // ── Inventory table columns ───────────────────────────────────────
  const rows = show.data?.rows ?? [];
  const statsPayload = stats.data;

  const inventoryColumns: DataTableColumn<DbCacheRow>[] = [
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

  // ── Schedule table columns ────────────────────────────────────────
  const scheduleColumns = useMemo<DataTableColumn<ScheduleRow>[]>(
    () => [
      {
        id: "id",
        header: "#",
        sortValue: (row) => row.id,
        cell: (row) => (
          <span className="font-mono text-xs text-ink-dim">{row.id}</span>
        ),
        width: "w-12",
      },
      {
        id: "name",
        header: "Name",
        sortValue: (row) => row.name,
        cell: (row) => <span className="font-medium text-ink">{row.name}</span>,
      },
      {
        id: "scope",
        header: "Scope",
        sortValue: (row) =>
          `${row.db_profile}/${row.database ?? row.catalog ?? ""}`,
        cell: (row) => {
          const overlay = row.database ?? row.catalog;
          return (
            <span className="text-ink-dim">
              {row.db_profile}
              {overlay && (
                <>
                  <span className="text-ink-muted"> · </span>
                  <span className="font-mono text-xs text-ink">{overlay}</span>
                </>
              )}
              <span className="text-ink-muted"> · </span>
              <span className="text-xs text-ink-dim">
                {scopeSummary(row.scope_json)}
              </span>
            </span>
          );
        },
        hideOnMobile: true,
      },
      {
        id: "recurrence",
        header: "Recurrence",
        sortValue: (row) => row.cron_expr ?? "",
        cell: (row) => (
          <span className="text-ink-dim">
            {recurrenceLabel(row.cron_expr ?? null)}
          </span>
        ),
        hideOnMobile: true,
      },
      {
        id: "when",
        header: "Next fire (local)",
        sortValue: (row) => row.fire_at_local,
        cell: (row) => <span className="text-ink">{row.fire_at_local}</span>,
        hideOnMobile: true,
      },
      {
        id: "tz",
        header: "Tz",
        sortValue: (row) => row.fire_at_tz,
        cell: (row) => (
          <span className="font-mono text-xs text-ink-dim">{row.fire_at_tz}</span>
        ),
        hideOnMobile: true,
      },
      {
        id: "status",
        header: "Status",
        sortValue: (row) => row.status,
        cell: (row) => <StatusChip status={row.status} />,
      },
      {
        id: "actions",
        header: "",
        cell: (row) => (
          <div
            className="flex items-center justify-end gap-1"
            onClick={(e) => e.stopPropagation()}
          >
            {(row.status === "pending" || row.status === "paused") && (
              <IconButton
                size="sm"
                icon={<Pencil size={16} />}
                label="Edit"
                title="Edit"
                onClick={() => {
                  setEditingSchedule(row);
                  setScheduleDialogOpen(true);
                }}
              />
            )}
            {row.status === "pending" && (
              <IconButton
                size="sm"
                icon={<PauseCircle size={16} />}
                label="Pause"
                title="Pause"
                onClick={() => pauseMut.mutate(row.id)}
              />
            )}
            {row.status === "paused" && (
              <IconButton
                size="sm"
                icon={<PlayCircle size={16} />}
                label="Resume"
                title="Resume"
                onClick={() => resumeMut.mutate(row.id)}
              />
            )}
            {(row.status === "pending" ||
              row.status === "paused" ||
              row.status === "missed") && (
              <IconButton
                size="sm"
                icon={<PlayCircle size={16} />}
                label="Run now"
                title="Run now"
                onClick={() => runNowMut.mutate(row.id)}
              />
            )}
            <IconButton
              size="sm"
              icon={<Trash2 size={16} />}
              label="Delete"
              title="Delete"
              onClick={() => {
                if (window.confirm(`Delete schedule "${row.name}"?`)) {
                  deleteScheduleMut.mutate(row.id);
                }
              }}
            />
          </div>
        ),
        width: "w-36",
        align: "right",
      },
    ],
    [pauseMut, resumeMut, runNowMut, deleteScheduleMut],
  );

  return (
    <div className="flex flex-col gap-4 px-4 py-4 sm:gap-6 sm:px-6 sm:py-6">
      <PageHeader
        title="Catalog cache"
        description="Single home for the catalog cache: stats, manual refreshes, inventory, and scheduled refresh jobs."
        actions={
          <div className="flex flex-wrap items-center gap-2">
            <Button
              variant="secondary"
              size="md"
              leadingIcon={<DatabaseZap size={14} />}
              onClick={() => setSyncScopeDialogOpen(true)}
            >
              Sync scope
            </Button>
            <Button
              variant="primary"
              size="md"
              leadingIcon={<CalendarPlus size={14} />}
              onClick={() => {
                setEditingSchedule(null);
                setScheduleDialogOpen(true);
              }}
            >
              New schedule
            </Button>
          </div>
        }
      />

      <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
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
          className="flex-wrap"
          title="Profile freshness"
          description="Per-profile last sync timestamp and manual refresh controls."
          actions={
            <div className="flex flex-wrap items-center gap-2">
              <Button
                variant="primary"
                size="sm"
                leadingIcon={
                  <RefreshCw
                    size={12}
                    className={
                      anySyncing || syncMut.isPending ? "animate-spin" : ""
                    }
                  />
                }
                onClick={() => syncMut.mutate(null)}
                disabled={syncMut.isPending || deepSyncMut.isPending || anySyncing}
              >
                {anySyncing || syncMut.isPending ? "Syncing…" : "Sync all"}
              </Button>
              <Button
                variant="secondary"
                size="sm"
                leadingIcon={
                  <RefreshCw
                    size={12}
                    className={
                      anySyncing || deepSyncMut.isPending ? "animate-spin" : ""
                    }
                  />
                }
                onClick={() => deepSyncMut.mutate(null)}
                disabled={syncMut.isPending || deepSyncMut.isPending || anySyncing}
                title="Full profile: fetches columns + row counts for every table (slower)."
              >
                {deepSyncMut.isPending ? "Deep syncing…" : "Deep sync all"}
              </Button>
            </div>
          }
        />
        <CardBody>
          {freshnessProfiles.length === 0 ? (
            <p className="text-xs text-ink-dim">
              {freshnessQ.isLoading
                ? "Loading profiles…"
                : "No DB profile has been indexed yet. Click Sync all to enumerate every schema + table."}
            </p>
          ) : (
            <ul className="space-y-1.5">
              {freshnessProfiles.map((p) => {
                const progressPct =
                  p.total_tables > 0
                    ? Math.min(
                        100,
                        Math.round((p.processed_tables / p.total_tables) * 100),
                      )
                    : 0;
                return (
                  <li
                    key={p.profile}
                    className="rounded-md border border-border/50 bg-surface-subtle/40 px-3 py-2"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2 sm:gap-3">
                      <div className="min-w-0 flex-1">
                        <div className="truncate font-mono text-xs text-ink sm:text-sm">
                          {p.profile}
                        </div>
                        <div
                          className={
                            p.state === "failed"
                              ? "text-xs text-critical"
                              : p.state === "syncing"
                                ? "text-xs text-accent-ink"
                                : p.stale
                                  ? "text-xs text-warning"
                                  : "text-xs text-ink-dim"
                          }
                        >
                          {p.state === "syncing"
                            ? `Syncing ${p.processed_tables} / ${p.total_tables || "…"}`
                            : p.state === "failed"
                              ? "Last sync failed"
                              : `Last sync: ${relativeAge(p.age_seconds)}${
                                  p.stale ? " · stale" : ""
                                }`}
                        </div>
                      </div>
                      <Button
                        variant="secondary"
                        size="sm"
                        leadingIcon={
                          <RefreshCw
                            size={12}
                            className={
                              (syncMut.isPending &&
                                syncMut.variables === p.profile) ||
                              p.state === "syncing"
                                ? "animate-spin"
                                : ""
                            }
                          />
                        }
                        onClick={() => syncMut.mutate(p.profile)}
                        disabled={syncMut.isPending || p.state === "syncing"}
                      >
                        {p.state === "failed" ? "Retry" : "Sync now"}
                      </Button>
                    </div>
                    {p.state === "syncing" && (
                      <div className="mt-1.5 h-1 w-full overflow-hidden rounded-full bg-surface-subtle">
                        <div
                          className="h-full bg-accent transition-all"
                          style={{
                            width:
                              p.total_tables > 0 ? `${progressPct}%` : "30%",
                          }}
                        />
                      </div>
                    )}
                    {p.state === "failed" && p.last_error && (
                      <p className="mt-1.5 break-words text-xs text-critical">
                        ⚠ {p.last_error}
                      </p>
                    )}
                  </li>
                );
              })}
            </ul>
          )}
        </CardBody>
      </Card>

      <Card>
        <CardHeader
          className="flex-wrap"
          title={`${rows.length} (profile, database) scope(s) cached`}
          description="What AMX has indexed per scope. Flush a row to force re-population on next read."
          actions={
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
          }
        />
        <CardBody>
          <DataTable
            columns={inventoryColumns}
            rows={rows}
            rowKey={(row) => `${row.profile}:${row.database}`}
            isLoading={show.isLoading}
            error={show.error ? (show.error as Error).message : null}
            emptyState={
              <div className="px-4 py-6 text-center text-[13px] text-ink-dim">
                No cached rows yet. Sync a profile above to populate the cache.
              </div>
            }
            initialSort={{ id: "profile", direction: "asc" }}
          />
        </CardBody>
      </Card>

      <Card>
        <CardHeader
          className="flex-wrap"
          title="Scheduled refreshes"
          description="Recurring or one-shot cache refresh jobs. The scheduler tick fires these and re-arms recurring entries from the cron."
          actions={
            <label className="flex items-center gap-2 text-xs text-ink-dim">
              <span>Show:</span>
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                className="rounded-md border border-border bg-surface-subtle px-2 py-1 text-xs text-ink"
              >
                <option value="all">All</option>
                <option value="active">
                  Active (pending / paused / missed / running)
                </option>
                <option value="past">
                  Past (completed / failed / cancelled)
                </option>
              </select>
            </label>
          }
        />
        <CardBody>
          <DataTable<ScheduleRow>
            rows={scheduleRows}
            columns={scheduleColumns}
            rowKey={(row) => row.id}
            isLoading={schedulesQ.isLoading}
            emptyState={
              <p className="text-ink-dim">
                No cache refresh schedules yet. Click{" "}
                <strong>New schedule</strong> at the top of the page.
              </p>
            }
            onRowClick={(row) => setDetailRow(row)}
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
              This will DELETE every cached schema, column comment, and catalog
              row for <strong>{confirmTarget.scope}</strong>. The next live read
              or /sync will repopulate. Continue?
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

      <ScheduleCacheRefreshDialog
        open={scheduleDialogOpen}
        onClose={() => {
          setScheduleDialogOpen(false);
          setEditingSchedule(null);
        }}
        onCreated={invalidateSchedules}
        editing={editingSchedule}
      />

      <CatalogRefreshDetailDialog
        open={detailRow != null}
        row={detailRow}
        onClose={() => setDetailRow(null)}
        onNavigate={navigate}
      />

      <SyncScopeDialog
        open={syncScopeDialogOpen}
        onClose={() => setSyncScopeDialogOpen(false)}
      />
    </div>
  );
}
