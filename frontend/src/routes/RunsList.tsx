import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { GitCompare, History, PlayCircle } from "lucide-react";

import { api } from "../lib/api";
import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";
import {
  Badge,
  Button,
  DataTable,
  type DataTableColumn,
  type DataTableFilter,
} from "../components/ui";
import {
  humanizeCommand,
  statusLabel,
  statusTone,
  summarizeScope,
} from "../lib/runDisplay";

interface Row {
  id: number;
  command: string;
  scope: Record<string, unknown> | null;
  status: string;
  duration_sec: number | null;
}

export default function RunsList() {
  const navigate = useNavigate();
  const runs = useQuery({
    queryKey: ["recent-runs", "all"],
    queryFn: () => api.recentRuns(50, "all"),
    retry: false,
  });

  // /runs is the "what AMX did to the database" log — Ask sessions
  // are conversational queries that don't touch the warehouse, so
  // they live behind /ask only and are filtered out here.
  const ASK_COMMANDS = new Set(["ask.run", "search.ask"]);
  const rows: Row[] = ((runs.data?.runs as Row[] | undefined) ?? []).filter(
    (r) => !ASK_COMMANDS.has(r.command),
  );

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
        sortValue: (r) => humanizeCommand(r.command),
        cell: (r) => (
          <span className="text-sm font-medium text-ink" title={r.command}>
            {humanizeCommand(r.command)}
          </span>
        ),
      },
      {
        id: "scope",
        header: "Scope",
        cell: (r) => (
          <span className="truncate text-sm text-ink-muted">
            {summarizeScope(r.scope)}
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
        width: "w-24",
        align: "right",
        sortValue: (r) => r.duration_sec ?? -1,
        cell: (r) => (
          <span className="font-mono text-xs text-ink-muted tabular-nums">
            {r.duration_sec != null ? `${r.duration_sec.toFixed(1)}s` : "—"}
          </span>
        ),
      },
    ],
    [],
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
          <div className="flex items-center gap-2">
            <Link to="/runs/compare">
              <Button variant="secondary" size="md" leadingIcon={<GitCompare size={14} />}>
                Compare
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
            Object.keys(r.scope || {}).join(" "),
            r.status,
          ].join(" ")
        }
        filters={filters}
        isLoading={runs.isLoading}
        error={runs.error ? (runs.error as Error).message : null}
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
