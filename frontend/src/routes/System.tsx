import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
  Activity,
  AlertCircle,
  CheckCircle,
  RefreshCw,
  Stethoscope,
  Wallet,
  Database,
  Wrench,
  Users,
} from "lucide-react";

import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import StatusPill from "../components/StatusPill";
import EmptyState from "../components/EmptyState";
import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";

interface DoctorCheck {
  name: string;
  ok: boolean;
  detail: string;
  hint: string;
}

interface DoctorResponse {
  checks: DoctorCheck[];
  total: number;
  failed: number;
  ok: boolean;
  skip_network: boolean;
}

interface UsageRow {
  provider: string;
  model: string;
  runs: number;
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  cost_usd: string;
}

interface UsageResponse {
  window: string;
  rows: UsageRow[];
  totals: {
    runs: number;
    input_tokens: number;
    output_tokens: number;
    total_tokens: number;
  };
  counted_runs?: number;
  message?: string;
}

interface CatalogStatus {
  ready: boolean;
  message?: string;
  llm_ready?: boolean;
  entities?: Record<string, number>;
  descriptions?: Record<string, number>;
  settings?: Record<string, string>;
  jobs?: Array<Record<string, unknown>>;
}

const WINDOWS: Array<{ id: string; label: string }> = [
  { id: "today", label: "Today" },
  { id: "24h", label: "24h" },
  { id: "7d", label: "7 days" },
  { id: "30d", label: "30 days" },
  { id: "all", label: "All time" },
];

export default function System() {
  return (
    <>
      <PageHeader
        eyebrow="Admin"
        title="System"
        description="Diagnostics, token usage + cost, and search catalog status — the same data the CLI's /doctor, /usage, and /search status surface."
      />
      <div className="space-y-4">
        <DoctorCard />
        <UsageCard />
        <CatalogStatusCard />
        <HistoryStoreCard />
        <MaintenanceCard />
      </div>
    </>
  );
}

function DoctorCard() {
  const [skipNetwork, setSkipNetwork] = useState(false);
  const doctor = useQuery({
    queryKey: ["doctor", skipNetwork],
    queryFn: () =>
      apiFetch<DoctorResponse>(`/api/doctor?skip_network=${skipNetwork}`),
    retry: false,
  });

  return (
    <Card>
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Stethoscope size={16} className="text-accent" />
            Doctor
          </span>
        }
        description="Installation, config, and connectivity checks. Re-runs every time you toggle the network checks below."
        actions={
          <div className="flex items-center gap-2">
            <label className="inline-flex cursor-pointer items-center gap-1.5 text-xs text-ink-muted">
              <input
                type="checkbox"
                checked={skipNetwork}
                onChange={(e) => setSkipNetwork(e.target.checked)}
                className="h-3.5 w-3.5 cursor-pointer"
              />
              Skip network
            </label>
            <button
              type="button"
              onClick={() => doctor.refetch()}
              className="inline-flex items-center gap-1 rounded-md bg-surface-subtle px-2.5 py-1 text-xs text-ink-muted hover:bg-surface-border"
            >
              <RefreshCw size={12} />
              Re-run
            </button>
          </div>
        }
      />
      <CardBody className="p-0">
        {doctor.isLoading ? (
          <div className="px-5 py-6 text-sm text-ink-dim">Running checks…</div>
        ) : doctor.error ? (
          <div className="px-5 py-6 text-sm text-critical">
            {(doctor.error as Error).message}
          </div>
        ) : doctor.data ? (
          <>
            <div
              className={cn(
                "border-b border-surface-border px-5 py-2 text-xs",
                doctor.data.ok ? "text-positive" : "text-critical",
              )}
            >
              {doctor.data.ok
                ? `All ${doctor.data.total} checks passed.`
                : `${doctor.data.failed} of ${doctor.data.total} checks failed.`}
            </div>
            <ul className="divide-y divide-surface-border">
              {doctor.data.checks.map((c) => (
                <li key={c.name} className="px-5 py-2.5 text-sm">
                  <div className="flex items-start gap-3">
                    {c.ok ? (
                      <CheckCircle
                        size={14}
                        className="mt-0.5 shrink-0 text-positive"
                      />
                    ) : (
                      <AlertCircle
                        size={14}
                        className="mt-0.5 shrink-0 text-critical"
                      />
                    )}
                    <div className="min-w-0 flex-1">
                      <div className="font-medium">{c.name}</div>
                      {c.detail && (
                        <div className="text-xs text-ink-muted">{c.detail}</div>
                      )}
                      {c.hint && (
                        <div
                          className={cn(
                            "mt-1 text-xs",
                            c.ok ? "text-ink-dim" : "text-warning",
                          )}
                        >
                          {c.hint}
                        </div>
                      )}
                    </div>
                  </div>
                </li>
              ))}
            </ul>
          </>
        ) : null}
      </CardBody>
    </Card>
  );
}

function UsageCard() {
  const [window, setWindow] = useState("7d");
  const usage = useQuery({
    queryKey: ["usage", window],
    queryFn: () => apiFetch<UsageResponse>(`/api/usage?window=${window}`),
    retry: false,
  });

  return (
    <Card>
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Wallet size={16} className="text-accent" />
            Token usage + cost
          </span>
        }
        description="Aggregated from ~/.amx/history.db. Pricing is approximate — set $0 if your provider isn't in the table yet."
        actions={
          <div className="flex items-center gap-1">
            {WINDOWS.map((w) => (
              <button
                key={w.id}
                type="button"
                onClick={() => setWindow(w.id)}
                className={cn(
                  "rounded-md px-2 py-1 text-xs font-medium transition",
                  window === w.id
                    ? "bg-accent-soft text-accent-ink"
                    : "text-ink-muted hover:bg-surface-subtle",
                )}
              >
                {w.label}
              </button>
            ))}
          </div>
        }
      />
      <CardBody className="p-0">
        {usage.isLoading ? (
          <div className="px-5 py-6 text-sm text-ink-dim">Loading…</div>
        ) : usage.error ? (
          <div className="px-5 py-6 text-sm text-critical">
            {(usage.error as Error).message}
          </div>
        ) : usage.data?.message ? (
          <div className="px-5 py-6 text-sm text-ink-dim">{usage.data.message}</div>
        ) : usage.data?.rows?.length ? (
          <table className="w-full text-sm">
            <thead className="bg-surface-subtle/60 text-[11px] uppercase tracking-wider text-ink-dim">
              <tr>
                <th className="px-5 py-2 text-left font-semibold">Provider</th>
                <th className="px-5 py-2 text-left font-semibold">Model</th>
                <th className="px-5 py-2 text-right font-semibold">Runs</th>
                <th className="px-5 py-2 text-right font-semibold">Input</th>
                <th className="px-5 py-2 text-right font-semibold">Output</th>
                <th className="px-5 py-2 text-right font-semibold">Total</th>
                <th className="px-5 py-2 text-right font-semibold">Cost</th>
              </tr>
            </thead>
            <tbody>
              {usage.data.rows.map((r) => (
                <tr
                  key={`${r.provider}::${r.model}`}
                  className="border-t border-surface-border"
                >
                  <td className="px-5 py-2 text-xs">{r.provider}</td>
                  <td className="px-5 py-2 font-mono text-xs">{r.model}</td>
                  <td className="px-5 py-2 text-right font-mono text-xs">
                    {r.runs}
                  </td>
                  <td className="px-5 py-2 text-right font-mono text-xs tabular-nums">
                    {r.input_tokens.toLocaleString()}
                  </td>
                  <td className="px-5 py-2 text-right font-mono text-xs tabular-nums">
                    {r.output_tokens.toLocaleString()}
                  </td>
                  <td className="px-5 py-2 text-right font-mono text-xs tabular-nums">
                    {r.total_tokens.toLocaleString()}
                  </td>
                  <td className="px-5 py-2 text-right font-mono text-xs">
                    {r.cost_usd}
                  </td>
                </tr>
              ))}
            </tbody>
            <tfoot className="border-t-2 border-surface-border bg-surface-subtle/40 text-[11px] uppercase tracking-wider text-ink-dim">
              <tr>
                <td className="px-5 py-2" colSpan={2}>
                  Total
                </td>
                <td className="px-5 py-2 text-right font-mono">
                  {usage.data.totals.runs}
                </td>
                <td className="px-5 py-2 text-right font-mono tabular-nums">
                  {usage.data.totals.input_tokens.toLocaleString()}
                </td>
                <td className="px-5 py-2 text-right font-mono tabular-nums">
                  {usage.data.totals.output_tokens.toLocaleString()}
                </td>
                <td className="px-5 py-2 text-right font-mono tabular-nums">
                  {usage.data.totals.total_tokens.toLocaleString()}
                </td>
                <td className="px-5 py-2"></td>
              </tr>
            </tfoot>
          </table>
        ) : (
          <EmptyState
            icon={Activity}
            title={`No runs in ${usage.data?.window ?? window}`}
            description="Trigger /run from the Runs page; numbers will populate here."
          />
        )}
      </CardBody>
    </Card>
  );
}

function CatalogStatusCard() {
  const status = useQuery({
    queryKey: ["catalog-status"],
    queryFn: () => apiFetch<CatalogStatus>("/api/catalog/status"),
    retry: false,
  });

  return (
    <Card>
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Database size={16} className="text-accent" />
            Search catalog
          </span>
        }
        description="Entity + description counts that the /ask agent grounds its answers against."
        actions={
          <button
            type="button"
            onClick={() => status.refetch()}
            className="inline-flex items-center gap-1 rounded-md bg-surface-subtle px-2.5 py-1 text-xs text-ink-muted hover:bg-surface-border"
          >
            <RefreshCw size={12} />
            Refresh
          </button>
        }
      />
      <CardBody>
        {status.isLoading ? (
          <div className="text-sm text-ink-dim">Loading…</div>
        ) : status.error ? (
          <div className="text-sm text-critical">
            {(status.error as Error).message}
          </div>
        ) : status.data ? (
          <>
            <div className="mb-3 flex items-center gap-3">
              <StatusPill tone={status.data.ready ? "positive" : "warning"}>
                {status.data.ready ? "Ready" : "Empty"}
              </StatusPill>
              <span className="text-xs text-ink-muted">
                LLM ready:{" "}
                <span className="font-mono">
                  {status.data.llm_ready ? "yes" : "no"}
                </span>
              </span>
            </div>
            {status.data.message && (
              <p className="mb-3 text-xs text-warning">{status.data.message}</p>
            )}
            <div className="grid gap-4 md:grid-cols-2">
              <KvBlock title="Entities" data={status.data.entities ?? {}} />
              <KvBlock
                title="Descriptions"
                data={status.data.descriptions ?? {}}
              />
            </div>
            {status.data.settings && Object.keys(status.data.settings).length > 0 && (
              <div className="mt-4">
                <KvBlock title="Settings" data={status.data.settings} />
              </div>
            )}
          </>
        ) : null}
      </CardBody>
    </Card>
  );
}

interface HistoryStoreStatus {
  enabled: boolean;
  profile: string;
  schema: string;
  outbox_pending: number;
}

function HistoryStoreCard() {
  const status = useQuery({
    queryKey: ["history-store-status"],
    queryFn: () =>
      apiFetch<HistoryStoreStatus>("/api/admin/history-store-status"),
    retry: false,
  });

  return (
    <Card>
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Users size={16} className="text-accent" />
            Team history store
          </span>
        }
        description="Optional shared run-history mode. When enabled, every /run dual-writes to a team database so colleagues can pull each other's runs."
        actions={
          <button
            type="button"
            onClick={() => status.refetch()}
            className="inline-flex items-center gap-1 rounded-md bg-surface-subtle px-2.5 py-1 text-xs text-ink-muted hover:bg-surface-border"
          >
            <RefreshCw size={12} />
            Refresh
          </button>
        }
      />
      <CardBody>
        {status.isLoading ? (
          <div className="text-sm text-ink-dim">Loading…</div>
        ) : status.error ? (
          <div className="text-sm text-critical">
            {(status.error as Error).message}
          </div>
        ) : status.data ? (
          <div className="space-y-2 text-sm">
            <div className="flex items-center gap-3">
              <StatusPill tone={status.data.enabled ? "positive" : "neutral"}>
                {status.data.enabled ? "Enabled" : "Disabled"}
              </StatusPill>
              {status.data.outbox_pending > 0 && (
                <StatusPill tone="warning">
                  {status.data.outbox_pending} pending
                </StatusPill>
              )}
            </div>
            <div className="grid grid-cols-2 gap-3 text-xs text-ink-muted">
              <div>
                <span className="text-ink-dim">Profile:</span>{" "}
                <span className="font-mono text-ink">
                  {status.data.profile || "—"}
                </span>
              </div>
              <div>
                <span className="text-ink-dim">Schema:</span>{" "}
                <span className="font-mono text-ink">
                  {status.data.schema || "—"}
                </span>
              </div>
            </div>
            {!status.data.enabled && (
              <p className="text-xs text-ink-dim">
                Use the CLI's <code className="font-mono">/history-store enable</code>
                {" "}command to bootstrap a team schema. The visualizer surfaces the
                status and outbox depth, but the enable wizard is interactive.
              </p>
            )}
            {status.data.outbox_pending > 0 && (
              <p className="text-xs text-warning">
                {status.data.outbox_pending} dual-writes are queued. Run{" "}
                <code className="font-mono">/history-store flush-pending</code> to
                retry them.
              </p>
            )}
          </div>
        ) : null}
      </CardBody>
    </Card>
  );
}

interface CleanupResult {
  schemas?: string[];
  tables_cleared?: number;
  columns_cleared?: number;
  warnings?: string[];
}

function MaintenanceCard() {
  const [result, setResult] = useState<CleanupResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const cleanup = useMutation({
    mutationFn: () =>
      apiFetch<CleanupResult>("/api/comments/cleanup-placeholders", {
        method: "POST",
        body: JSON.stringify({}),
      }),
    onSuccess: (data) => {
      setResult(data);
      setError(null);
    },
    onError: (e) => {
      setError(e instanceof Error ? e.message : "Cleanup failed.");
      setResult(null);
    },
  });

  return (
    <Card>
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Wrench size={16} className="text-accent" />
            Maintenance
          </span>
        }
        description="One-shot ops that don't fit anywhere else. Right now: strip auto-inference placeholder text from every COMMENT in the active database."
      />
      <CardBody className="space-y-3">
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => cleanup.mutate()}
            disabled={cleanup.isPending}
            className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
          >
            <Wrench size={14} />
            {cleanup.isPending ? "Cleaning…" : "Cleanup placeholder COMMENTs"}
          </button>
          <span className="text-xs text-ink-dim">
            Removes the <code className="font-mono">[inferred by AMX]</code> markers from
            the live DB without touching the descriptions themselves.
          </span>
        </div>
        {error && (
          <div className="rounded-md border border-critical/40 bg-critical/5 px-3 py-2 text-xs text-critical">
            {error}
          </div>
        )}
        {result && (
          <div className="rounded-md border border-positive/40 bg-positive/5 px-3 py-2 text-xs text-positive">
            Cleared {result.tables_cleared ?? 0} table(s) and{" "}
            {result.columns_cleared ?? 0} column(s) across{" "}
            {(result.schemas ?? []).length} schema(s).
            {(result.warnings ?? []).length > 0 && (
              <ul className="mt-1 space-y-0.5 text-warning">
                {(result.warnings ?? []).map((w, i) => (
                  <li key={i}>• {w}</li>
                ))}
              </ul>
            )}
          </div>
        )}
      </CardBody>
    </Card>
  );
}

function KvBlock({
  title,
  data,
}: {
  title: string;
  data: Record<string, unknown>;
}) {
  const entries = Object.entries(data);
  if (entries.length === 0) {
    return (
      <div>
        <h3 className="mb-1 text-[11px] font-semibold uppercase tracking-wider text-ink-dim">
          {title}
        </h3>
        <p className="text-xs text-ink-dim">—</p>
      </div>
    );
  }
  return (
    <div>
      <h3 className="mb-1 text-[11px] font-semibold uppercase tracking-wider text-ink-dim">
        {title}
      </h3>
      <ul className="space-y-0.5">
        {entries.map(([k, v]) => (
          <li
            key={k}
            className="flex items-center justify-between gap-3 text-xs"
          >
            <span className="text-ink-muted">{k}</span>
            <span className="font-mono text-ink">{String(v)}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
