import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import {
  Activity,
  Database,
  Sparkles,
  PlayCircle,
  TrendingUp,
} from "lucide-react";

import { api } from "../lib/api";
import { cn } from "../lib/cn";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import StatusPill from "../components/StatusPill";
import { Button, Skeleton } from "../components/ui";

// Landing dashboard — counts, recent runs, jump-off into the deeper
// routes. Reads /api/context, /api/history/stats, /api/history/runs.
export default function Home() {
  const { data: ctx } = useQuery({ queryKey: ["context"], queryFn: () => api.context() });
  const stats = useQuery({
    queryKey: ["stats"],
    queryFn: () => api.stats(),
    retry: false,
  });
  const runs = useQuery({
    queryKey: ["recent-runs"],
    queryFn: () => api.recentRuns(8),
    retry: false,
  });

  return (
    <>
      <PageHeader
        title="Overview"
        actions={
          <Link to="/runs/new">
            <Button variant="primary" size="md" leadingIcon={<PlayCircle size={14} />}>
              New run
            </Button>
          </Link>
        }
      />

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard
          icon={Database}
          label="Active backend"
          value={ctx?.db_backend ?? "—"}
          tone="neutral"
        />
        <StatCard
          icon={Sparkles}
          label="LLM model"
          value={ctx?.llm_model ?? "—"}
          tone="neutral"
        />
        <StatCard
          icon={PlayCircle}
          label="Total runs"
          value={statValue(stats.data?.total)}
          tone="accent"
        />
        <StatCard
          icon={TrendingUp}
          label="Success rate"
          value={successRate(stats.data)}
          tone="positive"
        />
      </div>

      <div className="mt-6">
        <Card>
          <CardHeader
            title="Recent runs"
            actions={
              <Link to="/runs" className="text-xs font-medium text-accent hover:text-accent-ink">
                View all →
              </Link>
            }
          />
          <CardBody className="p-0">
            {runs.isLoading ? (
              <ul className="divide-y divide-border">
                {Array.from({ length: 5 }).map((_, i) => (
                  <li key={i} className="flex items-center gap-3 px-5 py-2.5">
                    <Skeleton className="h-3 w-10" />
                    <Skeleton className="h-3 w-24" />
                    <Skeleton className="h-3 w-32" />
                    <Skeleton className="ml-auto h-4 w-16" />
                  </li>
                ))}
              </ul>
            ) : runs.error ? (
              <div className="px-5 py-6 text-sm text-critical">
                {(runs.error as Error).message}
              </div>
            ) : runs.data?.runs?.length ? (
              <ul className="divide-y divide-border">
                {runs.data.runs.map((row) => (
                  <li
                    key={row.id}
                    className="flex items-center gap-3 px-5 py-2.5 text-sm hover:bg-surface-subtle/50"
                  >
                    <Activity size={13} className="text-ink-dim" />
                    <Link
                      to={`/runs/${row.id}`}
                      className="font-mono text-xs text-ink-dim hover:text-accent"
                    >
                      #{row.id}
                    </Link>
                    <span className="font-medium text-ink">{row.command}</span>
                    <span className="truncate text-ink-muted">
                      {Object.keys(row.scope || {}).join(", ") || "(no scope)"}
                    </span>
                    <span className="ml-auto">
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
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              <div className="px-5 py-5">
                <EmptyState
                  icon={Activity}
                  title="No runs yet"
                  description="Trigger a /run from the CLI or use the New run button above to get started."
                  compact
                />
              </div>
            )}
          </CardBody>
        </Card>
      </div>
    </>
  );
}

function StatCard({
  icon: Icon,
  label,
  value,
  tone,
}: {
  icon: typeof Database;
  label: string;
  value: string;
  tone: "neutral" | "accent" | "positive";
}) {
  const isLong = value.length > 18;
  return (
    <div
      className="rounded-xl border border-border bg-surface-raised px-4 py-3.5 shadow-xs"
      title={isLong ? value : undefined}
    >
      <div className="flex items-center justify-between">
        <span className="text-[10.5px] font-semibold uppercase tracking-[0.06em] text-ink-dim">
          {label}
        </span>
        <Icon
          size={14}
          className={
            tone === "accent"
              ? "text-accent"
              : tone === "positive"
                ? "text-positive"
                : "text-ink-dim"
          }
        />
      </div>
      <div
        className={cn(
          "mt-1.5 font-mono leading-snug tabular-nums text-ink",
          "line-clamp-2 break-all",
          isLong ? "text-sm" : "text-[22px]",
        )}
      >
        {value}
      </div>
    </div>
  );
}

function statValue(n: unknown): string {
  if (typeof n === "number") return String(n);
  return "—";
}

function successRate(stats: unknown): string {
  if (!stats || typeof stats !== "object") return "—";
  const total = (stats as Record<string, unknown>).total;
  const success = (stats as Record<string, unknown>).success;
  if (typeof total === "number" && typeof success === "number" && total > 0) {
    return `${Math.round((success / total) * 100)}%`;
  }
  return "—";
}
