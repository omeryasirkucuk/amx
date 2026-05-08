import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { GitCompare } from "lucide-react";

import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import StatusPill from "../components/StatusPill";
import { apiFetch, api, type RunRow } from "../lib/api";
import { cn } from "../lib/cn";

interface SummaryRow {
  run_id: number;
  command?: string;
  llm_profile?: string;
  llm_model?: string;
  doc_profile?: string;
  code_profile?: string;
  duration_sec?: number;
  status?: string;
  [key: string]: unknown;
}

/** Backend ``compare_runs`` returns ``per_column`` in long format:
 *  one entry per (asset, run) combo with a ``run_id`` field. The
 *  pivot from "long rows" -> "one row per asset, one column per run"
 *  happens client-side in ``PerColumnPivot``. */
interface PerColumnRow {
  schema?: string;
  table?: string;
  column?: string;
  run_id?: number;
  description?: string;
  confidence?: string;
  logprob_score?: number | null;
  token_count?: number | null;
}

interface AggregateRow {
  metric: string;
  run_id: number;
  value: number | null;
}

interface CompareResponse {
  runs: Array<{ id: number; command?: string; status?: string }>;
  summary_rows: SummaryRow[];
  per_column: PerColumnRow[];
  aggregates: AggregateRow[];
  missing: number[];
}

export default function RunsCompare() {
  const [selected, setSelected] = useState<number[]>([]);
  const recent = useQuery({
    queryKey: ["recent-runs", "compare"],
    queryFn: () => api.recentRuns(20, "all"),
    retry: false,
  });

  const compare = useMutation({
    mutationFn: () =>
      apiFetch<CompareResponse>("/api/history/compare", {
        method: "POST",
        body: JSON.stringify({ run_ids: selected }),
      }),
  });

  function toggle(id: number) {
    setSelected((curr) =>
      curr.includes(id) ? curr.filter((x) => x !== id) : [...curr, id],
    );
  }

  return (
    <>
      <PageHeader
        eyebrow="History"
        title="Compare runs"
        description="Pick 2–4 runs from the list below; the compare endpoint mirrors the CLI's /history compare output (same numbers, same per-column pivot)."
        actions={
          <Link to="/runs" className="text-xs text-ink-dim hover:text-ink">
            ← All runs
          </Link>
        }
      />

      <Card className="mb-4">
        <CardHeader
          title={`${selected.length} run${selected.length === 1 ? "" : "s"} selected`}
          description="Click a row to toggle. The compare button is enabled once you've picked at least two."
          actions={
            <button
              type="button"
              onClick={() => compare.mutate()}
              disabled={selected.length < 2 || compare.isPending}
              className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
            >
              <GitCompare size={14} />
              {compare.isPending ? "Comparing…" : "Compare"}
            </button>
          }
        />
        <CardBody className="p-0">
          {recent.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading runs…</div>
          ) : recent.data?.runs?.length ? (
            <ul className="divide-y divide-surface-border">
              {recent.data.runs.map((row: RunRow) => {
                const isPicked = selected.includes(row.id);
                return (
                  <li
                    key={row.id}
                    onClick={() => toggle(row.id)}
                    className={cn(
                      "flex cursor-pointer items-center gap-3 px-5 py-2.5 text-sm transition hover:bg-surface-subtle/40",
                      isPicked && "bg-accent-soft/30",
                    )}
                  >
                    <input
                      type="checkbox"
                      checked={isPicked}
                      readOnly
                      className="h-3.5 w-3.5 cursor-pointer accent-current"
                    />
                    <span className="font-mono text-xs text-ink-dim">#{row.id}</span>
                    <span className="font-mono text-xs">{row.command}</span>
                    <span className="text-ink-muted">
                      {Object.keys(row.scope || {}).join(", ") || "—"}
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
                    <span className="font-mono text-[11px] text-ink-dim">
                      {row.duration_sec != null
                        ? `${row.duration_sec.toFixed(1)}s`
                        : "—"}
                    </span>
                  </li>
                );
              })}
            </ul>
          ) : (
            <EmptyState
              icon={GitCompare}
              title="No runs yet"
              description="Trigger /run from the Runs page to populate this list."
            />
          )}
        </CardBody>
      </Card>

      {compare.isError && (
        <div className="mb-4 rounded-md border border-critical/40 bg-critical/5 p-3 text-sm text-critical">
          {compare.error instanceof Error
            ? compare.error.message
            : "Compare failed."}
        </div>
      )}

      {compare.data && <CompareResults data={compare.data} />}
    </>
  );
}

function CompareResults({ data }: { data: CompareResponse }) {
  const runIds = data.runs.map((r) => r.id);
  return (
    <div className="space-y-4">
      {data.missing.length > 0 && (
        <div className="rounded-md border border-warning/40 bg-warning/10 p-3 text-xs text-warning">
          {data.missing.length} run id(s) not found:{" "}
          {data.missing.map((m) => `#${m}`).join(", ")}
        </div>
      )}

      <Card>
        <CardHeader title="Summary" description="One row per run." />
        <CardBody className="p-0 overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-surface-subtle/60 text-[11px] uppercase tracking-wider text-ink-dim">
              <tr>
                <th className="px-5 py-2 text-left font-semibold">Run</th>
                <th className="px-5 py-2 text-left font-semibold">Command</th>
                <th className="px-5 py-2 text-left font-semibold">LLM</th>
                <th className="px-5 py-2 text-left font-semibold">Doc / Code</th>
                <th className="px-5 py-2 text-right font-semibold">Duration</th>
                <th className="px-5 py-2 text-left font-semibold">Status</th>
              </tr>
            </thead>
            <tbody>
              {data.summary_rows.map((s) => (
                <tr key={s.run_id} className="border-t border-surface-border">
                  <td className="px-5 py-2 font-mono text-xs">
                    <Link to={`/runs/${s.run_id}`} className="hover:text-accent">
                      #{s.run_id}
                    </Link>
                  </td>
                  <td className="px-5 py-2 font-mono text-xs">
                    {String(s.command ?? "")}
                  </td>
                  <td className="px-5 py-2 font-mono text-xs">
                    {String(s.llm_model ?? "—")}
                  </td>
                  <td className="px-5 py-2 text-xs text-ink-muted">
                    {[s.doc_profile, s.code_profile].filter(Boolean).join(" / ") || "—"}
                  </td>
                  <td className="px-5 py-2 text-right font-mono text-xs">
                    {s.duration_sec != null ? `${s.duration_sec.toFixed(2)}s` : "—"}
                  </td>
                  <td className="px-5 py-2 text-xs">{String(s.status ?? "")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </CardBody>
      </Card>

      {data.aggregates.length > 0 && (
        <Card>
          <CardHeader
            title="Aggregates"
            description="Per-run roll-ups (model time, tokens, confidence band split, approval rate)."
          />
          <CardBody className="p-0 overflow-x-auto">
            <AggregatesPivot rows={data.aggregates} runIds={runIds} />
          </CardBody>
        </Card>
      )}

      {data.per_column.length > 0 && (
        <Card>
          <CardHeader
            title="Per-column descriptions"
            description={`First ${Math.min(50, data.per_column.length)} of ${data.per_column.length} rows. The CLI dumps every column; the SPA caps for readability.`}
          />
          <CardBody className="p-0 overflow-x-auto">
            <PerColumnPivot rows={data.per_column.slice(0, 50)} runIds={runIds} />
          </CardBody>
        </Card>
      )}
    </div>
  );
}

function AggregatesPivot({
  rows,
  runIds,
}: {
  rows: AggregateRow[];
  runIds: number[];
}) {
  const byMetric = new Map<string, Map<number, number | null>>();
  for (const r of rows) {
    const inner = byMetric.get(r.metric) ?? new Map<number, number | null>();
    inner.set(r.run_id, r.value);
    byMetric.set(r.metric, inner);
  }
  return (
    <table className="w-full text-sm">
      <thead className="bg-surface-subtle/60 text-[11px] uppercase tracking-wider text-ink-dim">
        <tr>
          <th className="px-5 py-2 text-left font-semibold">Metric</th>
          {runIds.map((id) => (
            <th key={id} className="px-5 py-2 text-right font-semibold">
              #{id}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {Array.from(byMetric.entries()).map(([metric, vals]) => (
          <tr key={metric} className="border-t border-surface-border">
            <td className="px-5 py-2 text-xs font-medium">{metric}</td>
            {runIds.map((id) => {
              const v = vals.get(id);
              return (
                <td
                  key={id}
                  className="px-5 py-2 text-right font-mono text-xs tabular-nums"
                >
                  {formatAggregateCell(metric, v)}
                </td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** Pretty-print one Aggregates cell. ``cost_usd`` reads as
 *  ``$0.0123``; the percentage metrics get a trailing ``%``;
 *  durations land as seconds with one decimal. Everything else
 *  falls through to a thousand-separated integer / locale string
 *  so the numbers stay scannable across orders of magnitude. */
function formatAggregateCell(metric: string, value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  if (metric === "cost_usd") {
    if (value <= 0) return "$0.00";
    return value < 0.01 ? `<$0.01` : `$${value.toFixed(4)}`;
  }
  if (metric.startsWith("pct_")) {
    return `${(value as number).toFixed(0)}%`;
  }
  if (metric === "approval_rate") {
    return `${((value as number) * 100).toFixed(0)}%`;
  }
  if (metric.endsWith("_sec")) {
    return `${(value as number).toFixed(2)}s`;
  }
  return (value as number).toLocaleString();
}

/** Pivot the long-format ``per_column`` rows into a single row per
 *  asset, with one cell per run. Two re-runs of the same column on
 *  the same table now collapse onto one line so the user can read
 *  the v1 vs v2 description side-by-side -- which is the only
 *  "Compare" actually useful for. The previous render kept reading
 *  ``r["run_<id>"]`` (a key the backend never produced) so every
 *  cell was the empty-state dash. */
function PerColumnPivot({
  rows,
  runIds,
}: {
  rows: PerColumnRow[];
  runIds: number[];
}) {
  // Group by (schema, table, column).
  const byAsset = new Map<string, Map<number, PerColumnRow>>();
  const labelByAsset = new Map<string, string>();
  for (const r of rows) {
    const schema = r.schema ?? "";
    const table = r.table ?? "";
    const column = r.column ?? "";
    const key = `${schema}|${table}|${column}`;
    const inner = byAsset.get(key) ?? new Map<number, PerColumnRow>();
    if (typeof r.run_id === "number") {
      inner.set(r.run_id, r);
    }
    byAsset.set(key, inner);
    if (!labelByAsset.has(key)) {
      labelByAsset.set(
        key,
        [schema, table, column].filter(Boolean).join(".") || "—",
      );
    }
  }

  // Sort assets that overlap across the most runs first -- those are
  // the most meaningful comparisons. Tie-break alphabetically so the
  // ordering is stable across re-renders.
  const orderedKeys = Array.from(byAsset.keys()).sort((a, b) => {
    const aOverlap = byAsset.get(a)?.size ?? 0;
    const bOverlap = byAsset.get(b)?.size ?? 0;
    if (aOverlap !== bOverlap) return bOverlap - aOverlap;
    return (labelByAsset.get(a) ?? "").localeCompare(labelByAsset.get(b) ?? "");
  });

  return (
    <table className="w-full text-xs">
      <thead className="bg-surface-subtle/60 text-[11px] uppercase tracking-wider text-ink-dim">
        <tr>
          <th className="px-5 py-2 text-left font-semibold">Asset</th>
          {runIds.map((id) => (
            <th key={id} className="px-5 py-2 text-left font-semibold">
              #{id}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {orderedKeys.map((key) => {
          const cells = byAsset.get(key)!;
          const overlapCount = cells.size;
          const isOverlap = overlapCount >= 2;
          return (
            <tr
              key={key}
              className={
                isOverlap
                  ? "border-t border-accent/30 bg-accent-soft/10"
                  : "border-t border-surface-border"
              }
            >
              <td className="px-5 py-2 align-top font-mono">
                <div>{labelByAsset.get(key)}</div>
                {!isOverlap && (
                  <div className="mt-0.5 text-[10px] uppercase tracking-wider text-ink-dim">
                    only in run #{Array.from(cells.keys())[0]}
                  </div>
                )}
              </td>
              {runIds.map((id) => {
                const cell = cells.get(id);
                return (
                  <td key={id} className="px-5 py-2 align-top text-ink-muted">
                    {cell?.description?.trim() ? (
                      <span>{cell.description}</span>
                    ) : (
                      <span className="text-ink-dim">—</span>
                    )}
                  </td>
                );
              })}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
