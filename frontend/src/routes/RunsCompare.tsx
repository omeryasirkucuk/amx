import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import {
  ChevronLeft,
  ChevronRight,
  Download,
  GitCompare,
  Search,
} from "lucide-react";

import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import StatusPill from "../components/StatusPill";
import Dialog from "../components/ui/Dialog";
import {
  ConfidencePill,
  LogprobBadge,
} from "../components/ui/InsightBadges";
import { apiFetch, api, type RunRow } from "../lib/api";
import { cn } from "../lib/cn";
import {
  humanizeCommand,
  relativeTime,
  shortModel,
  statusTone,
  summarizeScope,
} from "../lib/runDisplay";

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

// ── Picker filtering primitives ────────────────────────────────────────

type CommandKindFilter = "all" | "analyze" | "rerun" | "generate" | "ask";

/** Bucket an ``analysis_runs.command`` string into the picker filter
 *  buckets. Anything that isn't analyze / rerun / generate / ask
 *  falls into "other" and survives only when the filter is "all". */
function commandKind(
  command: string | null | undefined,
): "analyze" | "rerun" | "generate" | "ask" | "other" {
  const cmd = (command ?? "").toLowerCase();
  if (cmd === "analyze.run" || cmd === "analyze.apply") return "analyze";
  if (cmd === "rerun") return "rerun";
  if (cmd.startsWith("generate.")) return "generate";
  if (cmd === "search.ask" || cmd === "ask.run") return "ask";
  return "other";
}

/** Tailwind background class for the run-row command chip. Coloured
 *  so the user can scan a long picker and tell at a glance which
 *  rows are analyze runs vs rerun vs generate vs ask. */
function commandChipClass(kind: ReturnType<typeof commandKind>): string {
  switch (kind) {
    case "analyze":
      return "bg-accent-soft/40 text-accent-ink";
    case "rerun":
      return "bg-warning-soft/40 text-warning-ink";
    case "generate":
      return "bg-positive/15 text-positive";
    case "ask":
      return "bg-info-soft/40 text-info-ink";
    default:
      return "bg-surface-subtle/60 text-ink-muted";
  }
}

// ── Aggregate winner direction (mirrors CLI's _highlight_best) ────────

/** Direction map per aggregate metric. ``min`` = lower is better,
 *  ``max`` = higher is better, ``neutral`` = no winner highlight.
 *  Mirrors ``amx/cli_support/commands/compare.py:_highlight_best``
 *  call sites so the SPA winner ring agrees with the CLI's bold-
 *  green winner cell. New metrics added on either side need to
 *  land here too -- the test in ``tests/test_compare.py`` pins
 *  the set so drift can't slip through silently. */
const AGGREGATE_DIRECTION: Record<string, "min" | "max" | "neutral"> = {
  wall_duration_sec: "min",
  model_processing_sec: "min",
  prompt_tokens: "min",
  completion_tokens: "min",
  total_tokens: "min",
  cost_usd: "min",
  avg_logprob_score: "max",
  pct_high_confidence: "max",
  pct_medium_confidence: "neutral",
  pct_low_confidence: "min",
  approval_rate: "max",
  saved_results: "neutral",
};

/** Pretty label per aggregate metric. Raw key stays in ``title=`` so
 *  power users can still copy the metric name for export / scripts. */
const AGGREGATE_LABEL: Record<string, string> = {
  wall_duration_sec: "Wall duration (s)",
  model_processing_sec: "Model processing (s)",
  prompt_tokens: "Prompt tokens",
  completion_tokens: "Completion tokens",
  total_tokens: "Total tokens",
  cost_usd: "Cost (USD)",
  avg_logprob_score: "Avg logprob",
  pct_high_confidence: "% high confidence",
  pct_medium_confidence: "% medium confidence",
  pct_low_confidence: "% low confidence",
  approval_rate: "Approval rate",
  saved_results: "Saved results",
};

/** Return the run id with the best value for a given metric, or
 *  ``null`` for neutral metrics / when every cell is missing. Skips
 *  ``null`` and non-finite values so a missing reading on one run
 *  doesn't accidentally win as the smallest "0". */
function pickWinnerRunId(
  metric: string,
  vals: Map<number, number | null>,
): number | null {
  const direction = AGGREGATE_DIRECTION[metric] ?? "neutral";
  if (direction === "neutral") return null;
  let bestId: number | null = null;
  let bestVal: number | null = null;
  for (const [rid, raw] of vals.entries()) {
    if (raw == null || !Number.isFinite(raw)) continue;
    if (bestVal == null) {
      bestId = rid;
      bestVal = raw;
      continue;
    }
    if (direction === "min" && raw < bestVal) {
      bestId = rid;
      bestVal = raw;
    } else if (direction === "max" && raw > bestVal) {
      bestId = rid;
      bestVal = raw;
    }
  }
  return bestId;
}

const PAGE_SIZE_OPTIONS = [10, 20, 50, 100] as const;
type PageSize = (typeof PAGE_SIZE_OPTIONS)[number];
const PAGE_SIZE_STORAGE_KEY = "amx.compare.pickerPageSize";

function readStoredPageSize(): PageSize {
  if (typeof window === "undefined") return 20;
  const raw = Number(window.localStorage.getItem(PAGE_SIZE_STORAGE_KEY));
  return (PAGE_SIZE_OPTIONS as readonly number[]).includes(raw)
    ? (raw as PageSize)
    : 20;
}

export default function RunsCompare() {
  const [selected, setSelected] = useState<number[]>([]);
  const [search, setSearch] = useState<string>("");
  const [kindFilter, setKindFilter] = useState<CommandKindFilter>("all");
  const [pageSize, setPageSize] = useState<PageSize>(readStoredPageSize);
  const [page, setPage] = useState<number>(0);
  const [viewerOpen, setViewerOpen] = useState<boolean>(false);
  const recent = useQuery({
    queryKey: ["recent-runs", "compare"],
    // 200 is the server-side max. Paging is now client-side: with
    // 100/page the user can scroll their full recent history without
    // a second round trip. If the wall ever creeps past 200, switch
    // /api/history/runs to offset/limit and page server-side.
    queryFn: () => api.recentRuns(200, "all"),
    retry: false,
  });

  const compare = useMutation({
    mutationFn: () =>
      apiFetch<CompareResponse>("/api/history/compare", {
        method: "POST",
        body: JSON.stringify({ run_ids: selected }),
      }),
    onSuccess: () => setViewerOpen(true),
  });

  const pdf = useMutation({
    mutationFn: () => api.compareAsPdf(selected),
    onSuccess: (blob) => {
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      const date = new Date().toISOString().slice(0, 10);
      const ids = selected.join("-");
      a.href = url;
      a.download = `compare-${ids}-${date}.pdf`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    },
  });

  function toggle(id: number) {
    setSelected((curr) =>
      curr.includes(id) ? curr.filter((x) => x !== id) : [...curr, id],
    );
  }

  function clearSelection() {
    setSelected([]);
  }

  function changePageSize(next: PageSize) {
    setPageSize(next);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(PAGE_SIZE_STORAGE_KEY, String(next));
    }
  }

  // Apply text + kind filters to the run picker. The text filter
  // hits id, command, scope text, model name -- the same fields the
  // user can see in the row, so "what you read is what you filter".
  const filteredRows = useMemo(() => {
    const all = (recent.data?.runs as RunRow[] | undefined) ?? [];
    const needle = search.trim().toLowerCase();
    return all.filter((row) => {
      if (kindFilter !== "all" && commandKind(row.command) !== kindFilter) {
        return false;
      }
      if (!needle) return true;
      const hay = [
        String(row.id),
        row.command ?? "",
        humanizeCommand(row.command),
        summarizeScope(row.scope_json ?? row.scope),
        Object.keys(row.scope_json ?? row.scope ?? {}).join(" "),
        shortModel(row.llm_model),
        row.llm_model ?? "",
        row.db_profile ?? "",
      ]
        .join(" ")
        .toLowerCase();
      return hay.includes(needle);
    });
  }, [recent.data, search, kindFilter]);

  const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
  const safePage = Math.min(page, totalPages - 1);
  const pagedRows = useMemo(
    () => filteredRows.slice(safePage * pageSize, safePage * pageSize + pageSize),
    [filteredRows, safePage, pageSize],
  );

  // Snap back to page 0 whenever the active dataset shrinks under
  // the cursor — search tightens, kind filter narrows, page size
  // grows, or the underlying fetch returns fewer rows. Without this
  // the user lands on an empty page after every filter tweak.
  useEffect(() => {
    setPage(0);
  }, [search, kindFilter, pageSize, recent.data]);

  return (
    <>
      <PageHeader
        eyebrow="History"
        title="Compare runs"
        description="Pick at least 2 runs from the list below; the compare endpoint mirrors the CLI's /history compare output (same numbers, same per-column pivot)."
        actions={
          <Link to="/runs" className="text-xs text-ink-dim hover:text-ink">
            ← All runs
          </Link>
        }
      />

      <Card className="mb-4">
        <div className="sticky top-0 z-10 bg-surface">
          <CardHeader
            title={`${selected.length} run${selected.length === 1 ? "" : "s"} selected`}
            description="Click a row to toggle. The compare button is enabled once you've picked at least two."
            actions={
              <div className="flex items-center gap-2">
                {selected.length > 0 && (
                  <button
                    type="button"
                    onClick={clearSelection}
                    className="text-[11px] text-ink-dim underline-offset-2 hover:text-ink hover:underline"
                  >
                    Clear selection
                  </button>
                )}
                {compare.data && !viewerOpen && (
                  <button
                    type="button"
                    onClick={() => setViewerOpen(true)}
                    className="text-[11px] text-ink-dim underline-offset-2 hover:text-ink hover:underline"
                  >
                    Re-open last comparison
                  </button>
                )}
                <button
                  type="button"
                  onClick={() => compare.mutate()}
                  disabled={selected.length < 2 || compare.isPending}
                  className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
                >
                  <GitCompare size={14} />
                  {compare.isPending ? "Comparing…" : "Compare"}
                </button>
              </div>
            }
          />
          <div className="flex flex-wrap items-center gap-3 border-t border-surface-border bg-surface-subtle/30 px-5 py-2">
            <div className="inline-flex items-center gap-1.5 rounded-md border border-surface-border bg-surface px-2 py-1">
              <Search size={12} className="text-ink-dim" />
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search id, scope, model…"
                className="w-60 bg-transparent text-xs outline-none placeholder:text-ink-dim"
              />
            </div>
            <div className="flex flex-wrap items-center gap-1">
              {(["all", "analyze", "rerun", "generate", "ask"] as const).map(
                (kind) => {
                  const active = kindFilter === kind;
                  return (
                    <button
                      key={kind}
                      type="button"
                      onClick={() => setKindFilter(kind)}
                      className={cn(
                        "rounded-md px-2 py-0.5 text-[11px] capitalize transition",
                        active
                          ? "bg-accent-soft/60 text-accent-ink"
                          : "bg-surface text-ink-muted hover:bg-surface-subtle",
                      )}
                    >
                      {kind}
                    </button>
                  );
                },
              )}
            </div>
            <div className="ml-auto flex items-center gap-3">
              <span className="text-[11px] text-ink-dim tabular-nums">
                {filteredRows.length === 0
                  ? "0 of 0"
                  : `${safePage * pageSize + 1}–${Math.min(
                      (safePage + 1) * pageSize,
                      filteredRows.length,
                    )} of ${filteredRows.length}`}
              </span>
              <label className="inline-flex items-center gap-1 text-[11px] text-ink-dim">
                Page size
                <select
                  value={pageSize}
                  onChange={(e) =>
                    changePageSize(Number(e.target.value) as PageSize)
                  }
                  className="rounded-md border border-surface-border bg-surface px-1.5 py-0.5 text-[11px] text-ink outline-none"
                >
                  {PAGE_SIZE_OPTIONS.map((size) => (
                    <option key={size} value={size}>
                      {size}
                    </option>
                  ))}
                </select>
              </label>
              <div className="inline-flex items-center gap-1">
                <button
                  type="button"
                  onClick={() => setPage((p) => Math.max(0, p - 1))}
                  disabled={safePage === 0}
                  className="rounded-md border border-surface-border bg-surface p-1 text-ink-muted transition hover:text-ink disabled:cursor-not-allowed disabled:opacity-40"
                  aria-label="Previous page"
                >
                  <ChevronLeft size={12} />
                </button>
                <span className="min-w-[3.5rem] text-center text-[11px] text-ink-dim tabular-nums">
                  {safePage + 1} / {totalPages}
                </span>
                <button
                  type="button"
                  onClick={() =>
                    setPage((p) => Math.min(totalPages - 1, p + 1))
                  }
                  disabled={safePage >= totalPages - 1}
                  className="rounded-md border border-surface-border bg-surface p-1 text-ink-muted transition hover:text-ink disabled:cursor-not-allowed disabled:opacity-40"
                  aria-label="Next page"
                >
                  <ChevronRight size={12} />
                </button>
              </div>
            </div>
          </div>
        </div>
        <CardBody className="p-0">
          {recent.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading runs…</div>
          ) : filteredRows.length ? (
            <ul className="divide-y divide-surface-border">
              {pagedRows.map((row) => {
                const isPicked = selected.includes(row.id);
                const kind = commandKind(row.command);
                const scope = row.scope_json ?? row.scope;
                const modelLabel = shortModel(row.llm_model);
                const tokensCostBadge = renderTokensCostBadge(row);
                return (
                  <li
                    key={row.id}
                    onClick={() => toggle(row.id)}
                    className={cn(
                      "flex cursor-pointer flex-wrap items-center gap-3 px-5 py-2.5 text-sm transition hover:bg-surface-subtle/40",
                      isPicked && "bg-accent-soft/30",
                    )}
                  >
                    <input
                      type="checkbox"
                      checked={isPicked}
                      readOnly
                      className="h-3.5 w-3.5 cursor-pointer accent-current"
                    />
                    <span className="font-mono text-xs text-ink-dim">
                      #{row.id}
                    </span>
                    <span
                      className={cn(
                        "rounded-md px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider",
                        commandChipClass(kind),
                      )}
                      title={row.command ?? undefined}
                    >
                      {humanizeCommand(row.command)}
                    </span>
                    <span
                      className="min-w-0 truncate text-ink-muted"
                      title={Object.keys(scope ?? {}).join(", ")}
                    >
                      {summarizeScope(scope)}
                    </span>
                    {modelLabel && (
                      <span
                        className="hidden font-mono text-[11px] text-ink-dim md:inline"
                        title={row.llm_model ?? undefined}
                      >
                        {modelLabel}
                      </span>
                    )}
                    {tokensCostBadge}
                    {row.started_at != null && (
                      <span
                        className="hidden font-mono text-[11px] text-ink-dim md:inline"
                        title={String(row.started_at)}
                      >
                        {relativeTime(row.started_at)}
                      </span>
                    )}
                    <span className="ml-auto inline-flex items-center gap-2">
                      <span className="font-mono text-[11px] text-ink-dim tabular-nums">
                        {row.duration_sec != null
                          ? `${row.duration_sec.toFixed(1)}s`
                          : "—"}
                      </span>
                      <StatusPill tone={statusTone(row.status)}>
                        {row.status}
                      </StatusPill>
                    </span>
                  </li>
                );
              })}
            </ul>
          ) : (
            <EmptyState
              icon={GitCompare}
              title={
                recent.data?.runs?.length
                  ? "No runs match the current filters"
                  : "No runs yet"
              }
              description={
                recent.data?.runs?.length
                  ? "Clear the search or relax the kind filter."
                  : "Trigger /run from the Runs page to populate this list."
              }
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

      {compare.data && (
        <Dialog
          open={viewerOpen}
          onClose={() => setViewerOpen(false)}
          size="xl"
          title={`Comparison · ${compare.data.runs.length} run${compare.data.runs.length === 1 ? "" : "s"}`}
          description={`Run ids: ${compare.data.runs.map((r) => `#${r.id}`).join(", ")}`}
          footer={
            <>
              {pdf.isError && (
                <span className="mr-auto text-xs text-critical">
                  {pdf.error instanceof Error
                    ? pdf.error.message
                    : "PDF export failed."}
                </span>
              )}
              <button
                type="button"
                onClick={() => setViewerOpen(false)}
                className="rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm text-ink-muted transition hover:text-ink"
              >
                Close
              </button>
              <button
                type="button"
                onClick={() => pdf.mutate()}
                disabled={pdf.isPending}
                className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
              >
                <Download size={14} />
                {pdf.isPending ? "Generating PDF…" : "Download PDF"}
              </button>
            </>
          }
        >
          <CompareResults data={compare.data} />
        </Dialog>
      )}
    </>
  );
}

/** Optional pill rendered next to a picker row when the run carries
 *  token / USD-cost metrics. Skipped entirely (returns ``null``)
 *  when the data is missing -- never renders ``—`` so half the rows
 *  don't visually noise the picker. Reads ``metrics_json`` shape
 *  produced by the analyze worker (see runs.py:_run_worker_body). */
function renderTokensCostBadge(row: RunRow) {
  const metrics = row.metrics as Record<string, unknown> | undefined;
  if (!metrics) return null;
  const tokens = metrics.tokens as Record<string, unknown> | undefined;
  const totalTokens =
    typeof tokens?.total_tokens === "number"
      ? Number(tokens.total_tokens)
      : null;
  const costUsd =
    typeof tokens?.total_cost_usd === "number"
      ? Number(tokens.total_cost_usd)
      : null;
  if (totalTokens == null && costUsd == null) return null;
  const pieces: string[] = [];
  if (totalTokens != null && totalTokens > 0) {
    pieces.push(
      totalTokens >= 1000
        ? `${(totalTokens / 1000).toFixed(1)}k tok`
        : `${totalTokens} tok`,
    );
  }
  if (costUsd != null && costUsd > 0) {
    pieces.push(costUsd < 0.01 ? "<$0.01" : `$${costUsd.toFixed(4)}`);
  }
  if (pieces.length === 0) return null;
  return (
    <span
      className="hidden rounded-md bg-surface-subtle/70 px-1.5 py-0.5 font-mono text-[10px] text-ink-muted lg:inline"
      title="Total tokens · USD cost (frozen at run time)"
    >
      {pieces.join(" · ")}
    </span>
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
            description="Per-run roll-ups (model time, tokens, cost, confidence band split, approval rate). Best value per row is highlighted."
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
            description={
              `${data.per_column.length} pivoted row${data.per_column.length === 1 ? "" : "s"}. ` +
              "Each cell shows description + confidence + logprob + tokens. The cell with the highest logprob per asset gets the accent ring."
            }
          />
          <CardBody className="p-0">
            <div className="max-h-[640px] overflow-auto">
              <PerColumnPivot rows={data.per_column} runIds={runIds} />
            </div>
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
        {Array.from(byMetric.entries()).map(([metric, vals]) => {
          const winnerRunId = pickWinnerRunId(metric, vals);
          return (
            <tr key={metric} className="border-t border-surface-border">
              <td
                className="px-5 py-2 text-xs font-medium"
                title={metric}
              >
                {AGGREGATE_LABEL[metric] ?? metric}
              </td>
              {runIds.map((id) => {
                const v = vals.get(id);
                const isWinner =
                  winnerRunId != null && id === winnerRunId;
                return (
                  <td
                    key={id}
                    className={cn(
                      "px-5 py-2 text-right font-mono text-xs tabular-nums",
                      isWinner &&
                        "bg-accent-soft/20 ring-1 ring-inset ring-accent/50",
                    )}
                  >
                    {formatAggregateCell(metric, v)}
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
          // Winner per row: the cell whose run has the highest
          // logprob_score (closer to 0 = more confident). ``null``
          // means no run has a recorded logprob -- in that case
          // every cell stays plain.
          let bestRunId: number | null = null;
          let bestLogprob: number | null = null;
          for (const [rid, cell] of cells.entries()) {
            const lp = cell.logprob_score;
            if (lp == null || !Number.isFinite(lp)) continue;
            if (bestLogprob == null || (lp as number) > bestLogprob) {
              bestRunId = rid;
              bestLogprob = lp as number;
            }
          }
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
                const isWinner =
                  bestRunId != null && id === bestRunId && cell != null;
                return (
                  <td
                    key={id}
                    className={cn(
                      "px-5 py-2 align-top text-ink-muted",
                      isWinner && "ring-1 ring-inset ring-accent/40 rounded-md",
                    )}
                  >
                    {cell?.description?.trim() ? (
                      <div className="space-y-1">
                        <div className="text-ink">{cell.description}</div>
                        <div className="flex flex-wrap items-center gap-2 text-[10px]">
                          {cell.confidence ? (
                            <ConfidencePill
                              value={cell.confidence}
                              score={cell.logprob_score ?? null}
                            />
                          ) : null}
                          <LogprobBadge score={cell.logprob_score ?? null} />
                          {typeof cell.token_count === "number" &&
                          cell.token_count > 0 ? (
                            <span
                              className="font-mono text-ink-dim"
                              title="Output tokens for this asset's chosen description."
                            >
                              tok {cell.token_count}
                            </span>
                          ) : null}
                        </div>
                      </div>
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
