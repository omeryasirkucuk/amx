import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { GitCompare, History, PlayCircle } from "lucide-react";

import { api } from "../lib/api";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import StatusPill from "../components/StatusPill";
import EmptyState from "../components/EmptyState";

export default function RunsList() {
  const runs = useQuery({
    queryKey: ["recent-runs", "all"],
    queryFn: () => api.recentRuns(50, "all"),
    retry: false,
  });

  return (
    <>
      <PageHeader
        eyebrow="History"
        title="Runs"
        description="Every /run, /run-apply, and /ask invocation, newest first."
        actions={
          <div className="flex items-center gap-2">
            <Link
              to="/runs/compare"
              className="inline-flex items-center gap-1.5 rounded-md bg-surface-subtle px-3 py-1.5 text-sm text-ink-muted transition hover:bg-surface-border"
            >
              <GitCompare size={14} />
              Compare
            </Link>
            <Link
              to="/runs/new"
              className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90"
            >
              <PlayCircle size={14} />
              New run
            </Link>
          </div>
        }
      />
      <Card>
        <CardHeader
          title={`${runs.data?.count ?? 0} run${runs.data?.count === 1 ? "" : "s"}`}
        />
        <CardBody className="p-0">
          {runs.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading…</div>
          ) : runs.error ? (
            <div className="px-5 py-6 text-sm text-critical">{(runs.error as Error).message}</div>
          ) : runs.data?.runs?.length ? (
            <table className="w-full text-sm">
              <thead className="bg-surface-subtle/60 text-[11px] uppercase tracking-wider text-ink-dim">
                <tr>
                  <th className="px-5 py-2 text-left font-semibold">ID</th>
                  <th className="px-5 py-2 text-left font-semibold">Command</th>
                  <th className="px-5 py-2 text-left font-semibold">Scope</th>
                  <th className="px-5 py-2 text-left font-semibold">Status</th>
                  <th className="px-5 py-2 text-left font-semibold">Duration</th>
                </tr>
              </thead>
              <tbody>
                {runs.data.runs.map((row) => (
                  <tr key={row.id} className="hover:bg-surface-subtle/40">
                    <td className="px-5 py-2 font-mono text-xs text-ink-dim">
                      <Link to={`/runs/${row.id}`} className="hover:text-accent">
                        #{row.id}
                      </Link>
                    </td>
                    <td className="px-5 py-2 font-mono text-xs">{row.command}</td>
                    <td className="px-5 py-2 text-ink-muted">
                      {Object.keys(row.scope || {}).join(", ") || "—"}
                    </td>
                    <td className="px-5 py-2">
                      <StatusPill
                        tone={
                          row.status === "success"
                            ? "positive"
                            : row.status === "failed"
                              ? "critical"
                              : "neutral"
                        }
                      >
                        {row.status}
                      </StatusPill>
                    </td>
                    <td className="px-5 py-2 font-mono text-xs text-ink-muted">
                      {row.duration_sec != null ? `${row.duration_sec.toFixed(1)}s` : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <EmptyState
              icon={History}
              title="No runs yet"
              description="Run /run from the CLI or trigger one from a table once PR-C ships."
            />
          )}
        </CardBody>
      </Card>
    </>
  );
}
