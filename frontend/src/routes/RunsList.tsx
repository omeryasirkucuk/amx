import { useMemo, useState } from "react";
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
// Rerun / All) survives page refreshes. Default ``analyze`` mirrors
// the historical behaviour: /runs is the "what AMX did to the
// database" log; Ask sessions live behind /ask. Picking another kind
// from the chip group widens the view.
const KIND_FILTER_STORAGE_KEY = "amx.runs.kindFilter";

function readStoredKindFilter(): CommandKindFilter {
  if (typeof window === "undefined") return "analyze";
  const raw = window.localStorage.getItem(KIND_FILTER_STORAGE_KEY);
  if (raw === "all" || raw === "analyze" || raw === "rerun" || raw === "generate" || raw === "ask") {
    return raw;
  }
  return "analyze";
}

const KIND_FILTER_OPTIONS: ReadonlyArray<{
  value: CommandKindFilter;
  label: string;
}> = [
  { value: "analyze", label: "Analyze" },
  { value: "rerun", label: "Re-run" },
  { value: "generate", label: "Generate" },
  { value: "ask", label: "Ask" },
  { value: "schedule", label: "Schedule" },
  { value: "all", label: "All activity" },
];

export default function RunsList() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const toast = useToast();
  const [confirmCancelRow, setConfirmCancelRow] = useState<Row | null>(null);
  const [kindFilter, setKindFilter] = useState<CommandKindFilter>(readStoredKindFilter);

  function changeKindFilter(next: CommandKindFilter) {
    setKindFilter(next);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(KIND_FILTER_STORAGE_KEY, next);
    }
  }
  const runs = useQuery({
    queryKey: ["recent-runs", "all"],
    queryFn: () => api.recentRuns(50, "all"),
    retry: false,
    // Poll while there's at least one running row so a freshly
    // cancelled job's status flips on screen without the user
    // refreshing the page. Backend's ``live_job_id`` is what drives
    // the inline Cancel icon — when the worker exits, the next poll
    // returns the row with ``live_job_id=null`` and the icon hides.
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

  // /runs defaults to "Analyze" so the page reads as the "what AMX
  // did to the database" log. The kind chip group above the table
  // lets the user widen to All activity, narrow to Ask sessions /
  // Generate / Rerun. ``commandKind`` buckets each row's raw
  // ``command`` field; "other" survives only under "all".
  const allRows: Row[] = (runs.data?.runs as Row[] | undefined) ?? [];
  const rows: Row[] = useMemo(() => {
    if (kindFilter === "all") return allRows;
    return allRows.filter((r) => commandKind(r.command) === kindFilter);
  }, [allRows, kindFilter]);

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

  const columns: DataTableColumn<Row>[] = useMemo(
    () => [
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
          <span className="text-sm font-medium text-ink" title={r.command}>
            {humanizeCommand(r.command, r.scope_json ?? r.scope)}
          </span>
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
            <span
              className="truncate text-sm text-ink-muted"
              title={tooltip || undefined}
            >
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
          <span
            className="truncate font-mono text-xs text-ink-muted"
            title={r.llm_model ?? ""}
          >
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
        width: "w-10",
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
          ) : null,
      },
    ],
    [cancelRun.isPending],
  );

  const filters: DataTableFilter<Row>[] = useMemo(
    () => [
      {
        id: "success",
        label: "Succeeded",
        predicate: (r) => r.status === "success",
        badge: rows.filter((r) => r.status === "success").length,
      },
      {
        id: "failed",
        label: "Failed",
        predicate: (r) => r.status === "failed",
        badge: rows.filter((r) => r.status === "failed").length,
      },
      {
        id: "running",
        label: "Running",
        predicate: (r) => r.status === "running" || r.status === "queued",
        badge: rows.filter((r) => r.status === "running" || r.status === "queued").length,
      },
      {
        id: "cancelled",
        label: "Cancelled",
        predicate: (r) => r.status === "cancelled",
        badge: rows.filter((r) => r.status === "cancelled").length,
      },
    ],
    [rows],
  );

  return (
    <>
      <PageHeader
        title="Runs"
        breadcrumbs={[{ label: "Runs" }]}
        actions={
          <div className="flex flex-wrap items-center gap-2">
            <Link to="/runs/schedules">
              <Button
                variant="secondary"
                size="md"
                leadingIcon={<CalendarClock size={14} />}
              >
                Schedules
              </Button>
            </Link>
            <Link to="/db-cache">
              <Button
                variant="secondary"
                size="md"
                leadingIcon={<DatabaseZap size={14} />}
              >
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
        <span className="text-[11px] uppercase tracking-wider text-ink-dim">
          Kind
        </span>
        {KIND_FILTER_OPTIONS.map(({ value, label }) => {
          const active = kindFilter === value;
          const count = value === "all"
            ? allRows.length
            : allRows.filter((r) => commandKind(r.command) === value).length;
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
              <span className="font-mono text-[10px] text-ink-dim tabular-nums">
                {count}
              </span>
            </button>
          );
        })}
      </div>
      <DataTable<Row>
        columns={columns}
        rows={rows}
        rowKey={(r) => r.id}
        onRowClick={(r) => navigate(`/runs/${r.id}`)}
        searchable
        searchPlaceholder="Search by id, command, or scope…"
        searchAccessor={(r) =>
          [
            String(r.id),
            r.command,
            Object.keys(r.scope_json || r.scope || {}).join(" "),
            r.status,
          ].join(" ")
        }
        filters={filters}
        isLoading={runs.isLoading}
        error={runs.error ? (runs.error as Error).message : null}
        errorTone={
          runs.error &&
          /history store isn't initialized/i.test((runs.error as Error).message || "")
            ? "warning"
            : "critical"
        }
        initialSort={{ id: "id", direction: "desc" }}
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
        title={
          confirmCancelRow
            ? `Cancel run #${confirmCancelRow.id}?`
            : "Cancel this run?"
        }
        description="The worker exits between rows. Already-written descriptions stay; in-flight assets stop. This cannot be undone."
        confirmLabel="Cancel run"
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
