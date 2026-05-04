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

// Landing dashboard — counts, recent runs, suggested actions.
// Everything renders read-only data fetched from the existing
// /api/history/stats and /api/history/runs endpoints.
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
        eyebrow="Dashboard"
        title={
          <>
            Welcome back to AMX
            {ctx?.active_db_profile ? (
              <span className="ml-3 align-middle text-base font-medium text-ink-dim">
                ({ctx.active_db_profile})
              </span>
            ) : null}
          </>
        }
        description="A bird's-eye view of your metadata work. Browse the live database from the left, trigger /run jobs, or open Ask in the right rail."
      />

      <div className="grid gap-4 md:grid-cols-4">
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

      <div className="mt-6 grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader
            title="Recent runs"
            description="Last 8 /run invocations across all DB profiles."
            actions={
              <Link to="/runs" className="text-xs text-accent hover:underline">
                View all →
              </Link>
            }
          />
          <CardBody className="p-0">
            {runs.isLoading ? (
              <div className="px-5 py-6 text-sm text-ink-dim">Loading recent runs…</div>
            ) : runs.error ? (
              <div className="px-5 py-6 text-sm text-critical">
                {(runs.error as Error).message}
              </div>
            ) : runs.data?.runs?.length ? (
              <ul className="divide-y divide-surface-border">
                {runs.data.runs.map((row) => (
                  <li key={row.id} className="flex items-center gap-3 px-5 py-3 text-sm">
                    <Activity size={14} className="text-ink-dim" />
                    <span className="font-mono text-xs text-ink-dim">#{row.id}</span>
                    <span className="font-medium">{row.command}</span>
                    <span className="text-ink-muted">
                      {Object.keys(row.scope || {}).join(", ") || "(no scope)"}
                    </span>
                    <span className="ml-auto">
                      <StatusPill tone={row.status === "success" ? "positive" : row.status === "failed" ? "critical" : "neutral"}>
                        {row.status}
                      </StatusPill>
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              <EmptyState
                icon={Activity}
                title="No runs yet"
                description="Run /run from the CLI or trigger one from a table page once PR-C ships."
              />
            )}
          </CardBody>
        </Card>

        <Card>
          <CardHeader
            title="What's next"
            description="The visualizer is currently in PR-B (read-only browse)."
          />
          <CardBody>
            <ul className="space-y-3 text-sm text-ink-muted">
              <li>
                <span className="font-medium text-ink">Browse</span> — pick a schema in
                the left tree to drill into tables and columns.
              </li>
              <li>
                <span className="font-medium text-ink">Coming in PR-C</span> — kick off
                /run + /apply jobs from the table page with live progress.
              </li>
              <li>
                <span className="font-medium text-ink">Coming in PR-D</span> — /ask chat
                with streaming reasoning lands in the right panel.
              </li>
              <li>
                <span className="font-medium text-ink">Coming in PR-E</span> — profile
                editor + pending review queue.
              </li>
            </ul>
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
  // Long technical identifiers (LLM model slugs, hostnames) are
  // common here — we never want to truncate mid-token. Use a fluid
  // type ramp + 2-line clamp + word-break so identifiers wrap, and
  // surface the full value in a tooltip for very long ones.
  const isLong = value.length > 18;
  return (
    <div
      className="rounded-xl border border-surface-border bg-surface-raised p-4 shadow-card"
      title={isLong ? value : undefined}
    >
      <div className="flex items-center justify-between">
        <span className="text-[11px] font-semibold uppercase tracking-[0.06em] text-ink-dim">
          {label}
        </span>
        <Icon
          size={16}
          className={
            tone === "accent" ? "text-accent" : tone === "positive" ? "text-positive" : "text-ink-dim"
          }
        />
      </div>
      <div
        className={cn(
          "mt-2 font-mono leading-snug tabular-nums text-ink",
          // Two-line clamp instead of truncate so a value like
          // "moonshotai/kimi-k2-instruct" wraps cleanly.
          "line-clamp-2 break-all",
          // Smaller font when value is long; preserve the visual
          // weight of short numeric values like "12" or "—".
          isLong ? "text-sm" : "text-lg",
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
