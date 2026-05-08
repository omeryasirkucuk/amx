import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import {
  Activity,
  ArrowDownToLine,
  ArrowUpFromLine,
  Database,
  DollarSign,
  Sparkles,
  PlayCircle,
  TrendingUp,
} from "lucide-react";

import { api, apiFetch } from "../lib/api";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import StatusPill from "../components/StatusPill";
import { Button, InfoHint, Skeleton } from "../components/ui";

// Shape of the ``GET /api/usage?window=all`` payload — only the
// fields the Overview cards consume. Kept inline (rather than
// promoted to ``lib/api``) so a backend addition does not force a
// type churn in two places at once; the shared definition can land
// when a third surface starts consuming usage stats.
interface UsageTotals {
  runs: number;
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  cost_usd: number;
}
interface UsageResponse {
  window: string;
  totals: UsageTotals;
}
import {
  humanizeCommand,
  relativeTime,
  shortModel,
  statusLabel,
  statusTone,
  summarizeScope,
} from "../lib/runDisplay";

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
  // Lifetime token + cost totals for the overview cards. Reads the
  // same ``/api/usage`` endpoint the System page uses, with
  // ``window=all`` so the cards reflect every recorded run instead
  // of only the last 7 days. Failures fall through to ``—`` cards
  // (the catch is in ``StatCard``'s ``value`` resolution).
  const usage = useQuery({
    queryKey: ["usage", "all"],
    queryFn: () => apiFetch<UsageResponse>("/api/usage?window=all"),
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

      {/* Top row dropped the "Active backend" card in 0.13: DB profile
          activation was retired (the user picks a profile per-action
          on Run / Ask / Browse), so a single "active backend" string
          no longer described anything. The remaining three -- LLM
          model, total runs, success rate -- are the at-a-glance
          numbers the Overview was actually used for. */}
      <div className="grid gap-4 md:grid-cols-3">
        <StatCard
          icon={Sparkles}
          label="LLM model"
          value={ctx?.llm_model ?? "—"}
          tone="neutral"
          to="/settings"
          hint="The model that generates descriptions. Configured under Settings → LLM."
        />
        <StatCard
          icon={PlayCircle}
          label="Total runs"
          value={totalRuns(stats.data)}
          tone="accent"
          to="/runs"
          hint="All-time count of /run, /apply, and /ask invocations recorded in the local history store."
        />
        <StatCard
          icon={TrendingUp}
          label="Success rate"
          value={successRate(stats.data)}
          tone="positive"
          to="/runs"
          hint="Percentage of runs whose worker exited cleanly. Cancelled and failed runs both count against the total."
        />
      </div>

      <div className="mt-4 grid gap-4 md:grid-cols-3">
        <StatCard
          icon={ArrowUpFromLine}
          label="Input tokens"
          value={formatTokens(usage.data?.totals.input_tokens)}
          tone="neutral"
          to="/system"
          hint="All-time prompt tokens billed at the input rate, summed across every recorded run. Click to drill into the System page's per-(provider, model) breakdown."
        />
        <StatCard
          icon={ArrowDownToLine}
          label="Output tokens"
          value={formatTokens(usage.data?.totals.output_tokens)}
          tone="neutral"
          to="/system"
          hint="All-time completion tokens billed at the output rate. Output is usually the dominant cost contributor on modern models."
        />
        <StatCard
          icon={DollarSign}
          label="Total cost (USD)"
          value={formatCost(usage.data?.totals.cost_usd)}
          tone="accent"
          to="/system"
          hint="All-time USD cost frozen at run time. Older runs without a recorded price contribute zero — refresh prices via the TopBar ↻ button or run /usage --live for a recompute against current rates."
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
                {runs.data.runs.map((row) => {
                  const processed = countProcessed(row.metrics);
                  const total = countSelected(row.metrics);
                  return (
                    <li key={row.id}>
                      <Link
                        to={`/runs/${row.id}`}
                        className="flex items-start gap-3 px-5 py-2.5 text-sm transition-colors duration-fast hover:bg-surface-subtle/60 focus-visible:bg-surface-subtle/60 focus-visible:outline-none"
                      >
                        <Activity size={13} className="mt-1 text-ink-dim shrink-0" />
                        <span className="mt-0.5 font-mono text-xs text-ink-dim">
                          #{row.id}
                        </span>
                        <div className="min-w-0 flex-1">
                          <div className="flex items-baseline gap-2">
                            <span className="font-medium text-ink truncate" title={row.command}>
                              {humanizeCommand(row.command, row.scope_json ?? row.scope)}
                            </span>
                            <span className="truncate text-ink-muted text-xs">
                              · {describeScope(row.scope_json ?? row.scope, row.db_profile)}
                              {processed != null && (
                                <span className="ml-2 text-ink-dim">
                                  {processed}
                                  {total != null && total !== processed ? `/${total}` : ""}{" "}
                                  processed
                                </span>
                              )}
                            </span>
                          </div>
                          <div className="mt-0.5 flex flex-wrap items-center gap-x-2 text-[11px] text-ink-dim">
                            {row.db_profile && (
                              <span className="font-mono" title={`DB profile: ${row.db_profile}`}>
                                {row.db_profile}
                              </span>
                            )}
                            {row.llm_model && (
                              <>
                                {row.db_profile && <span>·</span>}
                                <span className="truncate font-mono" title={row.llm_model}>
                                  {shortModel(row.llm_model)}
                                </span>
                              </>
                            )}
                            {row.duration_sec != null && (
                              <>
                                <span>·</span>
                                <span className="font-mono tabular-nums">
                                  {row.duration_sec.toFixed(1)}s
                                </span>
                              </>
                            )}
                            {row.started_at != null && (
                              <>
                                <span>·</span>
                                <span>{relativeTime(row.started_at)}</span>
                              </>
                            )}
                          </div>
                        </div>
                        <span className="mt-0.5 ml-auto shrink-0">
                          <StatusPill tone={statusTone(row.status)}>
                            {statusLabel(row.status)}
                          </StatusPill>
                        </span>
                      </Link>
                    </li>
                  );
                })}
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
  hint,
  to,
}: {
  icon: typeof Database;
  label: string;
  value: string;
  tone: "neutral" | "accent" | "positive";
  hint?: string;
  /** When set the whole card becomes a navigation target. */
  to?: string;
}) {
  const inner = (
    <>
      <div className="flex items-center justify-between gap-2">
        <span className="inline-flex items-center gap-1 text-[10.5px] font-semibold uppercase tracking-[0.06em] text-ink-dim">
          {label}
          {hint && (
            <span
              onClick={(e) => e.stopPropagation()}
              onMouseDown={(e) => e.stopPropagation()}
            >
              <InfoHint text={hint} />
            </span>
          )}
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
      <div className="mt-1.5 truncate font-mono text-[14px] leading-tight tabular-nums text-ink">
        {value}
      </div>
    </>
  );

  const baseClass =
    "flex min-h-[68px] flex-col rounded-xl border border-border bg-surface-raised px-4 py-3 shadow-xs";

  if (to) {
    return (
      <Link
        to={to}
        title={value}
        className={`${baseClass} transition-colors duration-fast hover:border-accent/40 hover:bg-surface-subtle/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent`}
      >
        {inner}
      </Link>
    );
  }

  return (
    <div title={value} className={baseClass}>
      {inner}
    </div>
  );
}

/**
 * Compute success rate from the stats payload. Backend ships
 * `total_runs` / `success_runs` (canonical) — falls back to the
 * legacy `total` / `success` aliases if those ever come back.
 * Ready-for-review and cancelled runs both count against success.
 */
function successRate(stats: unknown): string {
  if (!stats || typeof stats !== "object") return "—";
  const s = stats as Record<string, unknown>;
  const totalRaw = s.total_runs ?? s.total;
  const successRaw = s.success_runs ?? s.success;
  const total = typeof totalRaw === "number" ? totalRaw : Number(totalRaw);
  const success = typeof successRaw === "number" ? successRaw : Number(successRaw);
  if (Number.isFinite(total) && Number.isFinite(success) && total > 0) {
    return `${Math.round((success / total) * 100)}%`;
  }
  return "—";
}

/**
 * Format an aggregated token count for the Overview StatCard.
 *
 * Long numbers are compacted to ``12.4M`` / ``823.5K`` so the card
 * does not need to grow horizontally on a fresh install with no
 * runs. Anything under 10k stays as the raw integer with a
 * locale-aware thousands separator. Missing / non-numeric input
 * resolves to ``—`` so the card matches the rest of the dashboard's
 * empty-state convention.
 */
function formatTokens(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value) || value <= 0) return "—";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 10_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toLocaleString();
}

/**
 * Format an aggregated USD cost for the Overview StatCard.
 *
 * ``$0.0000`` is misleading on a model with no recorded price —
 * rendered as ``—`` instead. Sub-cent totals fall to ``<$0.01`` so
 * the user is not lulled by a literal zero on a small workload.
 */
function formatCost(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value) || value <= 0) return "—";
  if (value < 0.01) return "<$0.01";
  if (value < 1) return `$${value.toFixed(4)}`;
  return `$${value.toFixed(2)}`;
}

function totalRuns(stats: unknown): string {
  if (!stats || typeof stats !== "object") return "—";
  const s = stats as Record<string, unknown>;
  const raw = s.total_runs ?? s.total;
  return typeof raw === "number" ? String(raw) : "—";
}

/**
 * "All schemas" alone says nothing about which database the run
 * touched. When the scope is empty (full-database run) we tack on
 * the DB profile name so a row reads e.g. "All schemas in
 * local-postgres" instead of just "All schemas".
 */
function describeScope(
  scope: Record<string, string[]> | null | undefined,
  dbProfile: string | null | undefined,
): string {
  const base = summarizeScope(scope);
  if (base === "All schemas" && dbProfile) {
    return `All schemas in ${dbProfile}`;
  }
  return base;
}

/**
 * Pull the most useful processed-asset count out of a run's metrics
 * payload. Different commands write different keys, so we scan a
 * short list of candidates and return the first numeric hit.
 */
function countProcessed(metrics: Record<string, unknown> | null | undefined): number | null {
  if (!metrics) return null;
  for (const key of [
    "processed_count",
    "tables_processed",
    "columns_processed",
    "applied_count",
  ]) {
    const v = metrics[key];
    if (typeof v === "number" && Number.isFinite(v)) return v;
  }
  return null;
}

function countSelected(metrics: Record<string, unknown> | null | undefined): number | null {
  if (!metrics) return null;
  for (const key of ["selected_count", "planned_count", "tables_selected"]) {
    const v = metrics[key];
    if (typeof v === "number" && Number.isFinite(v)) return v;
  }
  return null;
}
