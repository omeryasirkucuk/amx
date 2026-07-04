import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import {
  CalendarClock,
  DatabaseZap,
  GitCompare,
  History,
  PauseCircle,
  PlayCircle,
  ScrollText,
  Trash2,
} from "lucide-react";

import { api } from "../lib/api";
import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";
import {
  AlertDialog,
  Badge,
  Button,
  DataTable,
  type DataTableColumn,
  type DataTableFilter,
  IconButton,
  useToast,
} from "../components/ui";
import {
  commandKind,
  type CommandKindFilter,
  humanizeCommand,
  relativeTime,
  shortModel,
  statusLabel,
  statusTone,
  summarizeScope,
  summarizeProcessedAssets,
  processedAssetsTooltip,
  type ProcessedAssetsSummary,
} from "../lib/runDisplay";
import { cn } from "../lib/cn";

interface Row {
  id: number;
  command: string;
  // Backend serializes the parsed scope under ``scope_json`` (see
  // ``list_recent_runs`` in sqlite_store). The legacy ``scope`` key is
  // kept here only for resilience against older payloads — without
  // ``scope_json`` every row in this list rendered "All schemas"
  // regardless of what the user actually picked.
  scope_json?: Record<string, unknown> | null;
  scope?: Record<string, unknown> | null;
  /** Backend-aggregated record of the actual (schema, table, column)
   * tuples the run processed. Surfaces column-level scope that
   * ``scope_json`` doesn't carry. Null on legacy / still-running rows. */
  processed_assets?: ProcessedAssetsSummary | null;
  status: string;
  duration_sec: number | null;
  llm_model?: string | null;
  db_profile?: string | null;
  started_at?: number | string | null;
  /** SSE job id when a worker is still alive for this row. Drives the
   *  inline Cancel icon on running rows; null/absent for finished
   *  rows. */
  live_job_id?: string | null;
}

// Persisted kind-filter so the user's pick (Analyze / Ask / Generate /
// Rerun / All) survives page refreshes. Default ``all`` so a fresh
// Browse → AI Generate run shows up at the top of the list without
// the user having to widen the kind filter manually — single-asset
// generations land with ``command="generate.table"`` /
// ``"generate.column"`` and used to hide behind a tab the user
// never opened. The chip group still narrows when wanted.
const KIND_FILTER_STORAGE_KEY = "amx.runs.kindFilter";

function readStoredKindFilter(): CommandKindFilter {
  if (typeof window === "undefined") return "all";
  const raw = window.localStorage.getItem(KIND_FILTER_STORAGE_KEY);
  if (
    raw === "all" ||
    raw === "analyze" ||
    raw === "rerun" ||
    raw === "generate" ||
    raw === "ask" ||
    raw === "schedule"
  ) {
    return raw;
  }
  return "all";
}

const KIND_FILTER_OPTIONS: ReadonlyArray<{
  value: CommandKindFilter;
  label: string;
}> = [
  { value: "all", label: "All activity" },
  { value: "analyze", label: "Analyze" },
  { value: "generate", label: "Generate" },
  { value: "rerun", label: "Re-run" },
  { value: "ask", label: "Ask" },
  { value: "schedule", label: "Schedule" },
];

const PAGE_SIZE = 50;

export default function RunsList() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const toast = useToast();
  const [confirmCancelRow, setConfirmCancelRow] = useState<Row | null>(null);
  // Delete state: one row pending a single-delete confirm, the set of
  // checkbox-selected row ids for a bulk delete, and which bulk confirm
  // dialog is open ("selected" = the checked ids, "all" = every run
  // matching the active filter/search across all pages).
  const [confirmDeleteRow, setConfirmDeleteRow] = useState<Row | null>(null);
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  const [confirmBulk, setConfirmBulk] = useState<"selected" | "all" | null>(null);
  const [kindFilter, setKindFilter] = useState<CommandKindFilter>(readStoredKindFilter);
  // Server-driven controls. The list is paged on the server, so search,
  // status filter, and sort must travel to the API too — otherwise they'd
  // only see the current page. ``searchInput`` is debounced into
  // ``debouncedSearch`` before it hits the query so each keystroke doesn't
  // fire a request.
  const [searchInput, setSearchInput] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<string>("__all");
  const [sort, setSort] = useState<{ id: string; direction: "asc" | "desc" } | null>({
    id: "id",
    direction: "desc",
  });
  const [page, setPage] = useState(0);

  function changeKindFilter(next: CommandKindFilter) {
    setKindFilter(next);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(KIND_FILTER_STORAGE_KEY, next);
    }
  }

  // Debounce the search box (200ms) so typing doesn't fire a request per
  // keystroke — mirrors the Assets page's server-search pattern.
  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(searchInput.trim()), 200);
    return () => clearTimeout(t);
  }, [searchInput]);

  // Any filter/search/sort change resets to the first page so the user
  // doesn't land on an out-of-range page after the result set shrinks.
  useEffect(() => {
    setPage(0);
  }, [debouncedSearch, statusFilter, kindFilter, sort]);

  const runs = useQuery({
    // Every server-driven param is in the key so a change refetches the
    // matching page (and the live poll re-pulls the current view).
    queryKey: [
      "recent-runs",
      "list",
      kindFilter,
      debouncedSearch,
      statusFilter,
      sort?.id ?? "",
      sort?.direction ?? "",
      page,
    ],
    queryFn: () =>
      api.recentRuns(PAGE_SIZE, "all", {
        offset: page * PAGE_SIZE,
        q: debouncedSearch || undefined,
        status: statusFilter !== "__all" ? statusFilter : undefined,
        kind: kindFilter !== "all" ? kindFilter : undefined,
        sortBy: sort?.id,
        sortDir: sort?.direction,
      }),
    retry: false,
    // Poll while there's at least one running row on the current page so a
    // freshly cancelled job's status flips on screen without the user
    // refreshing. Backend's ``live_job_id`` drives the inline Cancel icon —
    // when the worker exits, the next poll returns the row with
    // ``live_job_id=null`` and the icon hides.
    refetchInterval: (query) => {
      const data = (query.state.data as { runs?: Row[] } | undefined)?.runs;
      const stillRunning = (data ?? []).some(
        (r) => r.status === "running" || r.status === "queued",
      );
      // Tight 4s tick while a worker is in flight (so a cancel /
      // completion flips on screen quickly); 15s baseline otherwise
      // so a scheduled run that fires while the user is parked on
      // /runs shows up without a manual refresh.
      return stillRunning ? 4000 : 15000;
    },
  });

  // The page is already the server-resolved slice — no client-side kind
  // filtering. Counts come from the full-dataset facets so the chips stay
  // honest across pages.
  const rows: Row[] = useMemo(() => (runs.data?.runs as Row[] | undefined) ?? [], [runs.data]);
  // Memoized so the empty-object fallback isn't a fresh reference each
  // render (which would churn the `filters` useMemo below).
  const kindCounts = useMemo<Record<string, number>>(
    () => runs.data?.kind_counts ?? {},
    [runs.data],
  );
  const statusCounts = useMemo<Record<string, number>>(
    () => runs.data?.status_counts ?? {},
    [runs.data],
  );
  const totalRows = runs.data?.total ?? rows.length;
  // Whether any server-side filter narrows the view. Gates the
  // "delete all matching" action so it can never fire against an
  // unfiltered full-history view.
  const hasActiveFilter = !!debouncedSearch || statusFilter !== "__all" || kindFilter !== "all";
  // Grand total across statuses (under the active search + kind) for the
  // status chip group's "All" badge.
  const statusTotal = Object.values(statusCounts).reduce((a, b) => a + b, 0);

  const cancelRun = useMutation({
    mutationFn: (jobId: string) => api.cancelRun(jobId),
    onSuccess: () => {
      setConfirmCancelRow(null);
      toast.push({
        title: "Cancellation requested",
        description: "Worker bails between rows; already-written changes stay.",
        tone: "warning",
      });
      queryClient.invalidateQueries({ queryKey: ["recent-runs"] });
    },
    onError: (e: Error) => {
      setConfirmCancelRow(null);
      toast.push({
        title: "Cancel failed",
        description: e.message,
        tone: "error",
      });
    },
  });

  const deleteRun = useMutation({
    mutationFn: (runId: number) => api.deleteRun(runId),
    onSuccess: (res) => {
      setConfirmDeleteRow(null);
      setSelectedIds((prev) => {
        const next = new Set(prev);
        next.delete(res.run_id);
        return next;
      });
      toast.push({
        title: "Run deleted",
        description: `Removed the run and ${res.counts.results} result row(s).`,
        tone: "success",
      });
      queryClient.invalidateQueries({ queryKey: ["recent-runs"] });
    },
    onError: (e: Error) => {
      setConfirmDeleteRow(null);
      toast.push({ title: "Delete failed", description: e.message, tone: "error" });
    },
  });

  const deleteRunsBulk = useMutation({
    mutationFn: (body: Parameters<typeof api.deleteRuns>[0]) => api.deleteRuns(body),
    onSuccess: (res) => {
      setConfirmBulk(null);
      setSelectedIds(new Set());
      toast.push({
        title: "Runs deleted",
        description: `Removed ${res.counts.runs} run(s) and ${res.counts.results} result row(s).`,
        tone: "success",
      });
      queryClient.invalidateQueries({ queryKey: ["recent-runs"] });
    },
    onError: (e: Error) => {
      setConfirmBulk(null);
      toast.push({ title: "Delete failed", description: e.message, tone: "error" });
    },
  });

  // Only finished rows are selectable — running rows can't be deleted
  // (their worker is still live). ``pageSelectable`` is the set of ids on
  // the current page that a select-all should cover.
  const pageSelectableIds = useMemo(
    () => rows.filter((r) => !r.live_job_id).map((r) => r.id),
    [rows],
  );
  const allPageSelected =
    pageSelectableIds.length > 0 && pageSelectableIds.every((id) => selectedIds.has(id));

  function toggleRow(id: number) {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function toggleAllOnPage() {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (allPageSelected) {
        pageSelectableIds.forEach((id) => next.delete(id));
      } else {
        pageSelectableIds.forEach((id) => next.add(id));
      }
      return next;
    });
  }

  const columns: DataTableColumn<Row>[] = useMemo(
    () => [
      {
        id: "select",
        header: (
          <input
            type="checkbox"
            aria-label="Select all runs on this page"
            className="h-3.5 w-3.5 cursor-pointer accent-accent"
            checked={allPageSelected}
            onChange={(e) => {
              e.stopPropagation();
              toggleAllOnPage();
            }}
            onClick={(e) => e.stopPropagation()}
          />
        ),
        width: "w-8",
        cell: (r) =>
          r.live_job_id ? null : (
            <input
              type="checkbox"
              aria-label={`Select run #${r.id}`}
              className="h-3.5 w-3.5 cursor-pointer accent-accent"
              checked={selectedIds.has(r.id)}
              onChange={(e) => {
                e.stopPropagation();
                toggleRow(r.id);
              }}
              onClick={(e) => e.stopPropagation()}
            />
          ),
      },
      {
        id: "id",
        header: "ID",
        width: "w-20",
        sortValue: (r) => r.id,
        cell: (r) => (
          <Link
            to={`/runs/${r.id}`}
            onClick={(e) => e.stopPropagation()}
            className="font-mono text-xs text-ink-dim hover:text-accent"
          >
            #{r.id}
          </Link>
        ),
      },
      {
        id: "command",
        header: "Type",
        sortValue: (r) => humanizeCommand(r.command, r.scope_json ?? r.scope),
        cell: (r) => (
          <div className="flex items-center gap-2">
            <KindPill kind={commandKind(r.command)} />
            <span className="truncate text-sm font-medium text-ink" title={r.command}>
              {humanizeCommand(r.command, r.scope_json ?? r.scope)}
            </span>
          </div>
        ),
      },
      {
        id: "scope",
        header: "Scope",
        cell: (r) => {
          // Prefer the concrete-asset label when the backend supplied
          // ``processed_assets`` (schema.table.column for column-level
          // runs, real schema.table for table-level). Fall back to the
          // legacy schema-count summary for older history rows or for
          // workers that are still running (the aggregate fills in
          // when ``run_results`` rows are written, so an in-flight run
          // has an empty sample until the first per-asset commit).
          const fromAssets = summarizeProcessedAssets(r.processed_assets);
          const label = fromAssets ?? summarizeScope(r.scope_json ?? r.scope);
          const tooltip =
            processedAssetsTooltip(r.processed_assets) ??
            Object.keys(r.scope_json ?? r.scope ?? {}).join(", ");
          return (
            <span className="truncate text-sm text-ink-muted" title={tooltip || undefined}>
              {label}
            </span>
          );
        },
        hideOnMobile: true,
      },
      {
        id: "db",
        header: "DB",
        sortValue: (r) => r.db_profile ?? "",
        cell: (r) => (
          <span className="truncate font-mono text-xs text-ink-muted" title={r.db_profile ?? ""}>
            {r.db_profile ?? "—"}
          </span>
        ),
        hideOnMobile: true,
      },
      {
        id: "model",
        header: "Model",
        sortValue: (r) => shortModel(r.llm_model),
        cell: (r) => (
          <span className="truncate font-mono text-xs text-ink-muted" title={r.llm_model ?? ""}>
            {shortModel(r.llm_model) || "—"}
          </span>
        ),
        hideOnMobile: true,
      },
      {
        id: "status",
        header: "Status",
        width: "w-28",
        sortValue: (r) => r.status,
        cell: (r) => <StatusBadge status={r.status} />,
      },
      {
        id: "duration",
        header: "Duration",
        width: "w-20",
        align: "right",
        sortValue: (r) => r.duration_sec ?? -1,
        cell: (r) => (
          <span className="font-mono text-xs text-ink-muted tabular-nums">
            {r.duration_sec != null ? `${r.duration_sec.toFixed(1)}s` : "—"}
          </span>
        ),
      },
      {
        id: "started",
        header: "Started",
        width: "w-24",
        align: "right",
        sortValue: (r) => {
          const t =
            typeof r.started_at === "number"
              ? r.started_at
              : r.started_at
                ? Date.parse(r.started_at)
                : 0;
          return Number.isFinite(t) ? -t : 0;
        },
        cell: (r) => (
          <span className="font-mono text-xs text-ink-muted tabular-nums">
            {r.started_at != null ? relativeTime(r.started_at) : "—"}
          </span>
        ),
        hideOnMobile: true,
      },
      {
        // Inline Cancel for running rows. Renders blank for finished
        // rows so the column doesn't draw a wide "—" gutter on every
        // line — the icon is only useful for the 0–1 actively
        // running row at any given time.
        id: "actions",
        header: "",
        width: "w-16",
        align: "right",
        cell: (r) =>
          r.live_job_id ? (
            <IconButton
              icon={<PauseCircle size={14} />}
              label="Cancel this run"
              size="sm"
              variant="ghost"
              onClick={(e) => {
                e.stopPropagation();
                setConfirmCancelRow(r);
              }}
              disabled={cancelRun.isPending}
            />
          ) : (
            // Finished rows get a delete control. Running rows show
            // Cancel instead — deleting an in-flight run would orphan
            // its worker, so the trash icon is hidden until it settles.
            <IconButton
              icon={<Trash2 size={14} />}
              label="Delete this run"
              size="sm"
              variant="ghost"
              onClick={(e) => {
                e.stopPropagation();
                setConfirmDeleteRow(r);
              }}
              disabled={deleteRun.isPending}
            />
          ),
      },
    ],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [cancelRun.isPending, deleteRun.isPending, selectedIds, allPageSelected, pageSelectableIds],
  );

  // Status filter chips. Server-side now, so the predicates are unused (the
  // API applies the filter); the badge counts come from the full-dataset
  // ``status_counts`` facet so they reflect every matching run, not just
  // this page. ``running`` covers queued workers too, matching the backend.
  const filters: DataTableFilter<Row>[] = useMemo(
    () => [
      {
        id: "success",
        label: "Succeeded",
        predicate: () => true,
        badge: statusCounts.success ?? 0,
      },
      {
        id: "failed",
        label: "Failed",
        predicate: () => true,
        badge: statusCounts.failed ?? 0,
      },
      {
        id: "running",
        label: "Running",
        predicate: () => true,
        badge: statusCounts.running ?? 0,
      },
      {
        id: "cancelled",
        label: "Cancelled",
        predicate: () => true,
        badge: statusCounts.cancelled ?? 0,
      },
    ],
    [statusCounts],
  );

  return (
    <>
      <PageHeader
        title="Runs"
        breadcrumbs={[{ label: "Runs" }]}
        actions={
          <div className="flex flex-wrap items-center gap-2">
            <Link to="/runs/schedules">
              <Button variant="secondary" size="md" leadingIcon={<CalendarClock size={14} />}>
                Schedules
              </Button>
            </Link>
            <Link to="/db-cache">
              <Button variant="secondary" size="md" leadingIcon={<DatabaseZap size={14} />}>
                Catalog cache
              </Button>
            </Link>
            <Link to="/runs/compare">
              <Button variant="secondary" size="md" leadingIcon={<GitCompare size={14} />}>
                Compare
              </Button>
            </Link>
            <Link to="/audit">
              <Button variant="secondary" size="md" leadingIcon={<ScrollText size={14} />}>
                Audit
              </Button>
            </Link>
            <Link to="/runs/new">
              <Button variant="primary" size="md" leadingIcon={<PlayCircle size={14} />}>
                New run
              </Button>
            </Link>
          </div>
        }
      />
      {/* Kind filter chip group. Lets the user toggle between Analyze
          (default — the historical behaviour), Ask sessions, Generate
          runs, Re-runs, and the union "All activity". The pick is
          persisted in localStorage so a refresh doesn't reset the view. */}
      <div className="mb-3 flex flex-wrap items-center gap-1.5">
        <span className="text-[11px] uppercase tracking-wider text-ink-dim">Kind</span>
        {KIND_FILTER_OPTIONS.map(({ value, label }) => {
          const active = kindFilter === value;
          // Counts come from the server's full-dataset ``kind_counts``
          // facet (keyed by bucket, plus an ``all`` total), so they reflect
          // every matching run regardless of the current page.
          const count = kindCounts[value] ?? 0;
          return (
            <button
              key={value}
              type="button"
              onClick={() => changeKindFilter(value)}
              className={cn(
                "inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-[11px] transition",
                active
                  ? "bg-accent-soft/60 text-accent-ink"
                  : "bg-surface text-ink-muted hover:bg-surface-subtle",
              )}
            >
              {label}
              <span className="font-mono text-[10px] text-ink-dim tabular-nums">{count}</span>
            </button>
          );
        })}
      </div>
      {/* Bulk delete bar. The left cluster appears once rows are checked
          (delete-the-selected-ids); the right "delete all matching" only
          appears when a search / status / kind filter is active, so it
          always maps to the label and can never wipe the whole history
          from a bare, unfiltered view. */}
      {(selectedIds.size > 0 || hasActiveFilter) && (
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2 rounded-md border border-border bg-surface-subtle px-3 py-2">
          <div className="flex items-center gap-2">
            {selectedIds.size > 0 ? (
              <>
                <span className="text-xs text-ink-muted">{selectedIds.size} selected</span>
                <Button
                  variant="danger"
                  size="sm"
                  leadingIcon={<Trash2 size={13} />}
                  onClick={() => setConfirmBulk("selected")}
                  disabled={deleteRunsBulk.isPending}
                >
                  Delete selected
                </Button>
                <Button variant="ghost" size="sm" onClick={() => setSelectedIds(new Set())}>
                  Clear
                </Button>
              </>
            ) : (
              <span className="text-xs text-ink-dim">
                Filter active — {totalRows} matching run(s)
              </span>
            )}
          </div>
          {hasActiveFilter && (
            <Button
              variant="ghost"
              size="sm"
              leadingIcon={<Trash2 size={13} />}
              onClick={() => setConfirmBulk("all")}
              disabled={deleteRunsBulk.isPending}
            >
              Delete all {totalRows} matching
            </Button>
          )}
        </div>
      )}
      <DataTable<Row>
        columns={columns}
        rows={rows}
        rowKey={(r) => r.id}
        onRowClick={(r) => navigate(`/runs/${r.id}`)}
        searchable
        searchPlaceholder="Search by id, command, or scope…"
        filters={filters}
        isLoading={runs.isLoading}
        error={runs.error ? (runs.error as Error).message : null}
        errorTone={
          runs.error && /history store isn't initialized/i.test((runs.error as Error).message || "")
            ? "warning"
            : "critical"
        }
        pageSize={PAGE_SIZE}
        // Server-driven: search, status filter, sort, and paging all travel
        // to the API so they span the whole history, not just this page.
        server={{
          query: searchInput,
          onQueryChange: setSearchInput,
          activeFilter: statusFilter,
          onFilterChange: setStatusFilter,
          allFilterBadge: statusTotal,
          sort,
          onSortChange: setSort,
          page,
          totalRows,
          onPageChange: setPage,
        }}
        emptyState={
          <EmptyState
            icon={History}
            title="No runs yet"
            description="Trigger /run from the CLI or use the New run button above."
            compact
          />
        }
      />
      <AlertDialog
        open={!!confirmCancelRow}
        onClose={() => setConfirmCancelRow(null)}
        onConfirm={() => {
          if (confirmCancelRow?.live_job_id) {
            cancelRun.mutate(confirmCancelRow.live_job_id);
          }
        }}
        loading={cancelRun.isPending}
        title={confirmCancelRow ? `Cancel run #${confirmCancelRow.id}?` : "Cancel this run?"}
        description="The worker exits between rows. Already-written descriptions stay; in-flight assets stop. This cannot be undone."
        confirmLabel="Cancel run"
      />
      <AlertDialog
        open={!!confirmDeleteRow}
        onClose={() => setConfirmDeleteRow(null)}
        onConfirm={() => {
          if (confirmDeleteRow) deleteRun.mutate(confirmDeleteRow.id);
        }}
        loading={deleteRun.isPending}
        tone="danger"
        title={confirmDeleteRow ? `Delete run #${confirmDeleteRow.id}?` : "Delete run?"}
        description="Permanently removes this run and its per-asset results from history. The applied-description audit trail and any live-database comments are untouched. This cannot be undone."
        confirmLabel="Delete"
      />
      <AlertDialog
        open={!!confirmBulk}
        onClose={() => setConfirmBulk(null)}
        onConfirm={() => {
          if (confirmBulk === "selected") {
            deleteRunsBulk.mutate({ run_ids: Array.from(selectedIds) });
          } else if (confirmBulk === "all") {
            deleteRunsBulk.mutate({
              all_matching: {
                q: debouncedSearch || undefined,
                status: statusFilter !== "__all" ? statusFilter : undefined,
                kind: kindFilter !== "all" ? kindFilter : undefined,
                command: "all",
              },
            });
          }
        }}
        loading={deleteRunsBulk.isPending}
        tone="danger"
        title={
          confirmBulk === "all"
            ? `Delete all ${totalRows} matching run(s)?`
            : `Delete ${selectedIds.size} selected run(s)?`
        }
        description="Permanently removes the run(s) and their per-asset results from history. The applied-description audit trail and any live-database comments are untouched. This cannot be undone."
        confirmLabel="Delete"
      />
    </>
  );
}

function StatusBadge({ status }: { status: string }) {
  const tone = statusTone(status);
  const pulse = status === "running" || status === "queued";
  return (
    <Badge tone={tone} dot pulse={pulse}>
      {statusLabel(status)}
    </Badge>
  );
}

/** Compact, color-coded chip next to each row's command label so the
 *  user can scan the timeline at a glance — Generate runs (from
 *  Browse → AI Generate) used to land behind a separate filter tab
 *  that the user never opened. With the default filter now "all",
 *  the kind pill is what distinguishes a single-asset generate row
 *  from an analyze bulk run without forcing a column-width hit. */
function KindPill({
  kind,
}: {
  kind: "analyze" | "rerun" | "generate" | "ask" | "schedule" | "other";
}) {
  const label =
    kind === "analyze"
      ? "Analyze"
      : kind === "generate"
        ? "Generate"
        : kind === "rerun"
          ? "Re-run"
          : kind === "ask"
            ? "Ask"
            : kind === "schedule"
              ? "Schedule"
              : "Other";
  const cls =
    kind === "analyze"
      ? "border-sky-500/30 bg-sky-500/10 text-sky-700 dark:text-sky-200"
      : kind === "generate"
        ? "border-amber-500/30 bg-amber-500/10 text-amber-800 dark:text-amber-200"
        : kind === "rerun"
          ? "border-violet-500/30 bg-violet-500/10 text-violet-700 dark:text-violet-200"
          : kind === "ask"
            ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-200"
            : kind === "schedule"
              ? "border-pink-500/30 bg-pink-500/10 text-pink-700 dark:text-pink-200"
              : "border-border bg-surface-subtle text-ink-muted";
  return (
    <span
      className={cn(
        "shrink-0 rounded-sm border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider",
        cls,
      )}
      aria-label={`Kind: ${label}`}
    >
      {label}
    </span>
  );
}
