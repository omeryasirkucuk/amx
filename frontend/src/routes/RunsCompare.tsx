import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import {
  ChevronLeft,
  ChevronRight,
  Download,
  GitCompare,
  Search,
  Sparkles,
} from "lucide-react";

import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import StatusPill from "../components/StatusPill";
import Dialog from "../components/ui/Dialog";
import { RouteState, useToast } from "../components/ui";
import {
  AltScoreBadge,
  ConfidencePill,
  LogprobBadge,
} from "../components/ui/InsightBadges";
import AlternativesModeBadge from "../components/ui/AlternativesModeBadge";
import {
  apiFetch,
  api,
  type CompareAggregateRow as AggregateRow,
  type CompareResponse,
  type ComparePerColumnRow as PerColumnRow,
  type RunRow,
} from "../lib/api";
import { cn } from "../lib/cn";
import {
  commandKind,
  type CommandKindFilter,
  humanizeCommand,
  processedAssetsTooltip,
  relativeTime,
  shortModel,
  statusTone,
  summarizeProcessedAssets,
  summarizeScope,
} from "../lib/runDisplay";

// SummaryRow / PerColumnRow / AggregateRow / CompareResponse types
// live in ../lib/api so they can be reused by the runDeepAnalysis
// helper. The local pivot logic below still consumes them via the
// re-imported aliases above.

// ── Picker filtering primitives ────────────────────────────────────────

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

/** Compose the seed prompt the Ask AMX modal hand-off pushes into the
 *  /ask page. Stays short on purpose — the LLM can call the
 *  compare_runs tool with the run ids to fetch the payload itself, no
 *  need to dump the whole pivot into the first user turn. */
function buildCompareSeedPrompt(data: CompareResponse): string {
  const ids = data.runs.map((r) => `#${r.id}`).join(", ");
  return (
    `I just compared ${data.runs.length} runs (${ids}). ` +
    "Walk me through the key differences (model time, tokens, cost, " +
    "confidence band split, avg logprob) and tell me which run produced " +
    "the most reliable descriptions. Use the compare_runs tool to fetch " +
    "the payload."
  );
}

export default function RunsCompare() {
  const navigate = useNavigate();
  const toast = useToast();
  // PR C — top-of-page tab between the existing run-id-level picker
  // ("By Run") and the new cell-mode view ("By Cell"). The mode is
  // mirrored to the URL so deep-links from the pinned-cells drawer
  // land directly on the cell-mode tab with the cell list pre-loaded.
  const [searchParams, setSearchParams] = useSearchParams();
  const mode = searchParams.get("mode") === "cell" ? "cell" : "run";
  const cellsParam = searchParams.get("cells") ?? "";
  function setMode(next: "run" | "cell") {
    const params = new URLSearchParams(searchParams);
    if (next === "cell") params.set("mode", "cell");
    else params.delete("mode");
    setSearchParams(params, { replace: true });
  }
  const [selected, setSelected] = useState<number[]>([]);
  const [search, setSearch] = useState<string>("");
  const [kindFilter, setKindFilter] = useState<CommandKindFilter>("all");
  const [pageSize, setPageSize] = useState<PageSize>(readStoredPageSize);
  const [page, setPage] = useState<number>(0);
  const [viewerOpen, setViewerOpen] = useState<boolean>(false);
  // ``groundTruthRunId`` lets the user pin one of the picked runs as
  // the academic ground-truth baseline for reference-based metrics
  // (chrF, ROUGE-L). Falls through to the live DB COMMENT waterfall
  // in the backend when null.
  const [groundTruthRunId, setGroundTruthRunId] = useState<number | null>(null);
  // Cost-preview dialog for the Tier 2 LLM-as-judge run.
  const [deepAnalysisConfirmOpen, setDeepAnalysisConfirmOpen] =
    useState<boolean>(false);
  const recent = useQuery({
    queryKey: ["recent-runs", "compare"],
    // 200 is the server-side max. Paging is now client-side: with
    // 100/page the user can scroll their full recent history without
    // a second round trip. If the wall ever creeps past 200, switch
    // /api/history/runs to offset/limit and page server-side.
    queryFn: () => api.recentRuns(200, "all"),
    retry: false,
    // Errors render inline via RouteState in the picker body.
    meta: { silentError: true },
  });

  // ``compareableIds(): {ids, droppedAsks}`` filters Ask sessions out
  // of the user's selection before any /compare call. Ask runs are
  // free-text Q&A turns — they don't produce per-asset descriptions
  // the way analyze / generate / rerun runs do, so a side-by-side
  // pivot of them is meaningless. The picker still LISTS them under
  // the "ask" kind chip so the user can preview them, but the
  // submit-time filter quietly drops them and surfaces a toast so
  // the exclusion is visible.
  function compareableSelection(): {
    ids: number[];
    droppedAsks: number[];
  } {
    const allRows = (recent.data?.runs as RunRow[] | undefined) ?? [];
    const askIds = new Set(
      allRows
        .filter((r) => commandKind(r.command) === "ask")
        .map((r) => r.id),
    );
    const ids = selected.filter((id) => !askIds.has(id));
    const droppedAsks = selected.filter((id) => askIds.has(id));
    return { ids, droppedAsks };
  }

  function notifyDroppedAsks(droppedAsks: number[]) {
    if (droppedAsks.length === 0) return;
    const idsLabel = droppedAsks.map((id) => `#${id}`).join(", ");
    toast.push({
      title: `Excluded ${droppedAsks.length} Ask session${droppedAsks.length === 1 ? "" : "s"} from the comparison`,
      description: (
        `Ask runs (${idsLabel}) are conversational Q&A turns and don't ` +
        "produce per-asset descriptions; comparing them isn't meaningful. " +
        "The remaining analyze / generate / rerun selections were compared."
      ),
      tone: "warning",
      duration: 5000,
    });
  }

  const compare = useMutation({
    mutationFn: () => {
      const { ids, droppedAsks } = compareableSelection();
      if (ids.length < 2) {
        throw new Error(
          droppedAsks.length > 0
            ? "Comparison needs at least 2 non-Ask runs. Pick more analyze / generate / rerun runs."
            : "Pick at least 2 runs to compare.",
        );
      }
      notifyDroppedAsks(droppedAsks);
      return apiFetch<CompareResponse>("/api/history/compare", {
        method: "POST",
        body: JSON.stringify({
          run_ids: ids,
          quality_tier: 1, // Tier 0 metrics auto-shown in the modal
          ground_truth_run_id: groundTruthRunId,
        }),
      });
    },
    onSuccess: () => setViewerOpen(true),
    // Errors render inline next to the Compare button.
    meta: { silentError: true },
  });

  // Tier 2 — LLM-as-judge tournament. Only fires after the cost-
  // preview dialog confirms; replaces ``compareData`` so the modal
  // re-renders with the enriched quality_metrics.
  const deepAnalysis = useMutation({
    mutationFn: () => {
      const { ids, droppedAsks } = compareableSelection();
      if (ids.length < 2) {
        throw new Error(
          "Deep analysis needs at least 2 non-Ask runs.",
        );
      }
      notifyDroppedAsks(droppedAsks);
      return api.runDeepAnalysis(ids, { groundTruthRunId });
    },
    onSuccess: (next) => {
      compare.reset();
      // ``compare`` exposes its own ``data`` via mutation state;
      // updating cache via setQueryData isn't applicable since it's
      // a mutation. Easiest pattern: re-trigger ``compare.mutate``
      // with the freshly-loaded payload — but that wastes an API
      // call. Instead, mutate the underlying cache by re-invoking
      // mutateAsync would loop. Pragmatic: stash the payload in a
      // local override that CompareResults reads.
      setDeepAnalysisOverride(next);
      setDeepAnalysisConfirmOpen(false);
    },
    // Errors render inline in the deep-analysis confirmation dialog.
    meta: { silentError: true },
  });

  // Once Tier 2 runs, override the displayed payload without losing
  // the cheap Tier 0 result we already had.
  const [deepAnalysisOverride, setDeepAnalysisOverride] =
    useState<CompareResponse | null>(null);
  const compareData: CompareResponse | null =
    deepAnalysisOverride ?? compare.data ?? null;

  const pdf = useMutation({
    mutationFn: () => {
      // Carry the active quality tier into the PDF render so a
      // "Run deeper analysis" → "Download PDF" sequence produces a
      // report with the Tier 1+2 metrics (judge win-rate, embedding
      // agreement) instead of silently dropping back to Tier 0.
      const tier = compareData?.quality_metrics?.tier ?? 1;
      return api.compareAsPdf(selected, {
        qualityTier: tier,
        groundTruthRunId: groundTruthRunId,
      });
    },
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
    // Errors render inline in the compare result dialog footer.
    meta: { silentError: true },
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
        humanizeCommand(row.command, row.scope_json ?? row.scope),
        summarizeProcessedAssets(row.processed_assets) ?? "",
        summarizeScope(row.scope_json ?? row.scope),
        Object.keys(row.scope_json ?? row.scope ?? {}).join(" "),
        // Also search by the actual processed schema.table.column
        // tuples so the user can find "the run that touched
        // sales.orders.status" by typing any one of those parts.
        ...(row.processed_assets?.sample ?? []).flatMap((a) => [
          a.schema,
          a.table,
          a.column ?? "",
        ]),
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

      <div className="mb-4 flex items-center gap-1 border-b border-border">
        {(["run", "cell"] as const).map((m) => (
          <button
            key={m}
            type="button"
            onClick={() => setMode(m)}
            aria-pressed={mode === m}
            className={cn(
              "relative -mb-px px-3 py-2 text-sm font-medium transition-colors",
              mode === m
                ? "border-b-2 border-accent text-ink"
                : "border-b-2 border-transparent text-ink-muted hover:text-ink",
            )}
          >
            {m === "run" ? "By Run" : "By Cell"}
          </button>
        ))}
      </div>

      {mode === "cell" ? (
        <CellCompareView cellsParam={cellsParam} />
      ) : (
      <>
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
                {compareData && !viewerOpen && (
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
            <div className="px-5 py-6">
              <RouteState
                status="loading"
                hideLoadingTitle
                loadingBlocks={3}
              />
            </div>
          ) : recent.isError ? (
            <div className="px-5 py-6">
              <RouteState
                status="error"
                error={recent.error}
                onRetry={() => recent.refetch()}
              />
            </div>
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
                    {isPicked && (
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          setGroundTruthRunId((curr) =>
                            curr === row.id ? null : row.id,
                          );
                        }}
                        title={
                          groundTruthRunId === row.id
                            ? "This run is the ground truth baseline. Click to unpin."
                            : "Set this run as the ground-truth baseline for reference-based quality metrics."
                        }
                        className={cn(
                          "rounded-md border px-1.5 py-0.5 text-[10px] uppercase tracking-wider transition",
                          groundTruthRunId === row.id
                            ? "border-accent bg-accent-soft/40 text-accent-ink"
                            : "border-surface-border text-ink-dim hover:border-accent/40 hover:text-ink",
                        )}
                      >
                        {groundTruthRunId === row.id ? "Ground truth" : "Set baseline"}
                      </button>
                    )}
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
                      {humanizeCommand(row.command, row.scope_json ?? row.scope)}
                    </span>
                    {(() => {
                      // Prefer the concrete-asset label from
                      // ``processed_assets`` so column-level runs show
                      // ``sales.orders.status`` instead of the
                      // schema-level scope (``sales · 1 table``) the
                      // user picked. Fall back to the legacy scope
                      // summary for runs whose worker hasn't yet
                      // written any ``run_results`` rows.
                      const concrete = summarizeProcessedAssets(row.processed_assets);
                      const label = concrete ?? summarizeScope(scope);
                      const tooltip =
                        processedAssetsTooltip(row.processed_assets) ??
                        Object.keys(scope ?? {}).join(", ");
                      return (
                        <span
                          className="min-w-0 truncate text-ink-muted"
                          title={tooltip || undefined}
                        >
                          {label}
                        </span>
                      );
                    })()}
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

      {compareData && (
        <Dialog
          open={viewerOpen}
          onClose={() => setViewerOpen(false)}
          size="xl"
          title={`Comparison · ${compareData.runs.length} run${compareData.runs.length === 1 ? "" : "s"}`}
          description={`Run ids: ${compareData.runs.map((r) => `#${r.id}`).join(", ")}`}
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
              {!compareData?.quality_metrics?.judge_outcomes?.length && (
                <button
                  type="button"
                  onClick={() => setDeepAnalysisConfirmOpen(true)}
                  disabled={deepAnalysis.isPending}
                  className="inline-flex items-center gap-1.5 rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm text-ink-muted transition hover:text-ink disabled:opacity-40"
                  title="Run Tier 1 embeddings + Tier 2 LLM-as-judge tournament for academic quality scores."
                >
                  <Sparkles size={14} />
                  {deepAnalysis.isPending ? "Analysing…" : "Run deeper analysis"}
                </button>
              )}
              <button
                type="button"
                onClick={() => {
                  const seedPrompt = buildCompareSeedPrompt(compareData!);
                  setViewerOpen(false);
                  navigate("/ask", { state: { seedPrompt } });
                }}
                className="inline-flex items-center gap-1.5 rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm text-ink-muted transition hover:text-ink"
              >
                <Sparkles size={14} />
                Ask AMX
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
          <CompareResults data={compareData} />
        </Dialog>
      )}

      {/* Cost preview for the LLM-as-judge tournament. We only roughly
          estimate cost here — actual usage is captured in tokens_json
          server-side and surfaced once the tournament finishes. */}
      <Dialog
        open={deepAnalysisConfirmOpen}
        onClose={() => setDeepAnalysisConfirmOpen(false)}
        size="md"
        title="Run deeper quality analysis?"
        description={
          "Tier 1 (sentence-transformer embeddings) + Tier 2 (G-Eval " +
          "LLM-as-judge tournament). The judge runs C(N,2) calls per " +
          "asset using the active LLM profile — token cost rolls into " +
          "the active run's tokens_json."
        }
        footer={
          <>
            {deepAnalysis.isError && (
              <span className="mr-auto text-xs text-critical">
                {deepAnalysis.error instanceof Error
                  ? deepAnalysis.error.message
                  : "Deep analysis failed."}
              </span>
            )}
            <button
              type="button"
              onClick={() => setDeepAnalysisConfirmOpen(false)}
              className="rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm text-ink-muted transition hover:text-ink"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={() => deepAnalysis.mutate()}
              disabled={deepAnalysis.isPending}
              className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
            >
              {deepAnalysis.isPending ? "Running…" : "Run analysis"}
            </button>
          </>
        }
      >
        <p className="text-sm text-ink-muted">
          Estimated cost depends on the number of assets × number of run
          pairs and your active LLM's per-token rate. A typical 50-column
          × 3-run comparison runs ~150 judge calls; on{" "}
          <span className="font-mono">gpt-4o-mini</span> that's roughly{" "}
          <span className="font-semibold">$0.01–$0.02</span>. The result
          replaces the current Tier 0 view in this modal.
        </p>
        <p className="mt-3 text-xs text-ink-dim">
          Methods cited in the resulting Quality card: chrF (Popović 2015),
          ROUGE-L (Lin 2004), G-Eval (Liu et al. 2023), Prometheus 2 (Kim
          et al. 2024).
        </p>
      </Dialog>
      </>
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

function QualityCard({ data }: { data: CompareResponse }) {
  const quality = data.quality_metrics;

  // Reference resolution summary across assets — tells the reader whether
  // reference-based metrics had real ground truth. Computed above the early
  // return below so the hook runs unconditionally (rules of hooks); it
  // no-ops when there is no quality payload.
  const refSummary = useMemo(() => {
    if (!quality) return "";
    const counts: Record<string, number> = {};
    for (const r of quality.references) {
      counts[r.source] = (counts[r.source] ?? 0) + 1;
    }
    const labels: Record<string, string> = {
      user_pinned: "user-pinned",
      db_comment: "live DB COMMENT",
      catalog_applied: "catalog applied",
      none: "no reference",
    };
    const parts: string[] = [];
    for (const src of [
      "user_pinned",
      "db_comment",
      "catalog_applied",
      "none",
    ] as const) {
      if (counts[src]) parts.push(`${counts[src]} ${labels[src]}`);
    }
    return parts.join(" · ");
  }, [quality]);

  if (!quality || !quality.per_run.length) return null;

  const fmt = (v: number | null | undefined): string =>
    v == null ? "—" : `${(v * 100).toFixed(0)}%`;

  const hasChrf = quality.per_run.some((r) => r.chrf != null);
  const hasRouge = quality.per_run.some((r) => r.rouge_l != null);
  const hasBert = quality.per_run.some((r) => r.bertscore != null);
  const hasEmbed = quality.per_run.some((r) => r.embedding_agreement != null);
  const hasJudge = quality.per_run.some((r) => r.judge_win_rate != null);

  return (
    <Card>
      <CardHeader
        title={`Quality metrics · Tier ${quality.tier}`}
        description={
          refSummary
            ? `References: ${refSummary}.`
            : "No reference resolved — reference-based metrics skipped."
        }
      />
      <CardBody className="p-0 overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-surface-subtle/60 text-[11px] uppercase tracking-wider text-ink-dim">
            <tr>
              <th className="px-5 py-2 text-left font-semibold">Run</th>
              <th className="px-5 py-2 text-right font-semibold">Diversity</th>
              <th className="px-5 py-2 text-right font-semibold">Schema grounding</th>
              {hasChrf && (
                <th className="px-5 py-2 text-right font-semibold">chrF</th>
              )}
              {hasRouge && (
                <th className="px-5 py-2 text-right font-semibold">ROUGE-L</th>
              )}
              {hasBert && (
                <th className="px-5 py-2 text-right font-semibold">BERTScore</th>
              )}
              {hasEmbed && (
                <th className="px-5 py-2 text-right font-semibold">
                  Embed. agree.
                </th>
              )}
              {hasJudge && (
                <th className="px-5 py-2 text-right font-semibold">
                  Judge win-rate
                </th>
              )}
            </tr>
          </thead>
          <tbody>
            {quality.per_run.map((row) => (
              <tr key={row.run_id} className="border-t border-surface-border">
                <td className="px-5 py-2 font-mono text-xs">#{row.run_id}</td>
                <td className="px-5 py-2 text-right font-mono text-xs">
                  {fmt(row.type_token_ratio)}
                </td>
                <td className="px-5 py-2 text-right font-mono text-xs">
                  {fmt(row.schema_grounding)}
                </td>
                {hasChrf && (
                  <td className="px-5 py-2 text-right font-mono text-xs">
                    {fmt(row.chrf)}
                  </td>
                )}
                {hasRouge && (
                  <td className="px-5 py-2 text-right font-mono text-xs">
                    {fmt(row.rouge_l)}
                  </td>
                )}
                {hasBert && (
                  <td className="px-5 py-2 text-right font-mono text-xs">
                    {fmt(row.bertscore)}
                  </td>
                )}
                {hasEmbed && (
                  <td className="px-5 py-2 text-right font-mono text-xs">
                    {fmt(row.embedding_agreement)}
                  </td>
                )}
                {hasJudge && (
                  <td className="px-5 py-2 text-right font-mono text-xs">
                    {row.judge_win_rate != null
                      ? `${(row.judge_win_rate * 100).toFixed(0)}% (${row.judge_wins}/${row.judge_pairings})`
                      : "—"}
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </CardBody>
      {quality.citations.length > 0 && (
        <div className="border-t border-surface-border bg-surface-subtle/30 px-5 py-3 text-[11px] text-ink-dim">
          {/* Surface only the metric labels and a docs link — the
              card was getting unreadable when seven full bibliographic
              entries stacked under the table on every compare. The
              full citations live at /cli/history/#academic-methods on
              amxcli.com for anyone who wants them. */}
          <span className="font-semibold uppercase tracking-wider">
            Methods
          </span>
          <span>
            {": "}
            {quality.citations.map((c) => c.label).join(" · ")} ·{" "}
            <a
              href="https://amxcli.com/cli/history/#academic-methods"
              target="_blank"
              rel="noreferrer"
              className="text-accent hover:underline"
            >
              full citations
            </a>
          </span>
        </div>
      )}
    </Card>
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

      {/* Quality metric framework — Tier 0/1/2 academic metrics. */}
      <QualityCard data={data} />
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
  // Group by (schema, table, column). Each cell now holds an ordered
  // list of versions (v1 + any v2/v3+ descendants for that asset
  // within this run) rather than a single row, so Variations /
  // Re-Run descendants stack inline under the parent v1 in the same
  // cell -- mirroring the run-detail page's stacked layout.
  const byAsset = new Map<string, Map<number, PerColumnRow[]>>();
  const labelByAsset = new Map<string, string>();
  for (const r of rows) {
    const schema = r.schema ?? "";
    const table = r.table ?? "";
    const column = r.column ?? "";
    const key = `${schema}|${table}|${column}`;
    const inner = byAsset.get(key) ?? new Map<number, PerColumnRow[]>();
    if (typeof r.run_id === "number") {
      const versions = inner.get(r.run_id) ?? [];
      versions.push(r);
      inner.set(r.run_id, versions);
    }
    byAsset.set(key, inner);
    if (!labelByAsset.has(key)) {
      labelByAsset.set(
        key,
        [schema, table, column].filter(Boolean).join(".") || "—",
      );
    }
  }
  // Stable per-cell sort: v1 first, then descendants chronologically
  // (the backend already emits them in that order, but lock it in so
  // a future server-side reorder cannot reshuffle the UI).
  for (const inner of byAsset.values()) {
    for (const versions of inner.values()) {
      versions.sort((a, b) => {
        const av = parseInt((a.version_label || "v1").slice(1), 10);
        const bv = parseInt((b.version_label || "v1").slice(1), 10);
        return (av || 1) - (bv || 1);
      });
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

  // Derive each run's diversity mode from the first non-null
  // ``alternatives_mode`` seen across its v1 cells. The mode is
  // row-level in the DB so every cell carries it, but for the pivot
  // header we show it once next to the run id (per user spec: "tüm
  // run için bir kere vermek yeterli her kolona yazmaya gerek yok").
  // Descendants are skipped here because the descendant's own mode
  // can differ from the parent run's mode (a v1 semantic run can
  // spawn a v2 lexical variation); the header chip describes the
  // parent run only, and the per-version row gets its own mode chip
  // inline below. Returns ``null`` for legacy runs that predate
  // PR #441.
  const modeByRun = new Map<number, "semantic" | "lexical" | null>();
  for (const id of runIds) {
    let resolved: "semantic" | "lexical" | null = null;
    for (const inner of byAsset.values()) {
      const versions = inner.get(id);
      if (!versions || versions.length === 0) continue;
      const v1 = versions[0];
      const mode = v1?.alternatives_mode ?? null;
      if (mode === "semantic" || mode === "lexical") {
        resolved = mode;
        break;
      }
    }
    modeByRun.set(id, resolved);
  }

  return (
    <table className="w-full text-xs">
      <thead className="bg-surface-subtle/60 text-[11px] uppercase tracking-wider text-ink-dim">
        <tr>
          <th className="px-5 py-2 text-left font-semibold">Asset</th>
          {runIds.map((id) => (
            <th key={id} className="px-5 py-2 text-left font-semibold">
              <div className="flex items-center gap-2">
                <span>#{id}</span>
                <AlternativesModeBadge mode={modeByRun.get(id) ?? null} />
              </div>
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
          // every cell stays plain. Winner comparison uses the v1
          // row's logprob -- descendants stack inline under v1 in
          // the same cell and do not compete for the winner ring.
          let bestRunId: number | null = null;
          let bestLogprob: number | null = null;
          for (const [rid, versions] of cells.entries()) {
            const cell = versions[0];
            if (!cell) continue;
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
                const versions = cells.get(id) ?? [];
                const hasContent =
                  versions.length > 0 &&
                  (versions[0]?.description?.trim() || "").length > 0;
                const isWinner =
                  bestRunId != null && id === bestRunId && hasContent;
                return (
                  <td
                    key={id}
                    className={cn(
                      "px-5 py-2 align-top text-ink-muted",
                      isWinner && "ring-1 ring-inset ring-accent/40 rounded-md",
                    )}
                  >
                    {hasContent ? (
                      <div className="space-y-2.5">
                        {versions.map((cell, vidx) => {
                          const label = cell.version_label || "v1";
                          const isDescendant = vidx > 0;
                          const modeLetter =
                            cell.alternatives_mode === "semantic"
                              ? "S"
                              : cell.alternatives_mode === "lexical"
                                ? "L"
                                : null;
                          const modeTitle =
                            cell.alternatives_mode === "semantic"
                              ? "semantic — paraphrase of the seed"
                              : cell.alternatives_mode === "lexical"
                                ? "lexical — same vocabulary, distinct candidate meaning"
                                : "alternatives mode not recorded";
                          return (
                            <div
                              key={`${label}-${vidx}`}
                              className={
                                isDescendant
                                  ? "border-l-2 border-accent/30 pl-2 space-y-1"
                                  : "space-y-1"
                              }
                            >
                              <div className="flex items-center gap-1.5">
                                <span
                                  className={cn(
                                    "inline-flex items-center rounded px-1 text-[10px] font-semibold uppercase tracking-wider",
                                    isDescendant
                                      ? "bg-accent-soft/40 text-accent"
                                      : "bg-surface-subtle text-ink-dim",
                                  )}
                                  title={
                                    cell.descendant_kind === "variations"
                                      ? `${label} — variations (seed: ${cell.seed_alternative_text ?? "—"})`
                                      : cell.descendant_kind === "rerun"
                                        ? `${label} — re-run`
                                        : `${label} — original`
                                  }
                                >
                                  {label}
                                </span>
                                {isDescendant && modeLetter ? (
                                  <span
                                    title={modeTitle}
                                    className="inline-flex h-4 min-w-[1rem] items-center justify-center rounded bg-surface-subtle px-1 text-[10px] font-semibold text-ink-muted"
                                  >
                                    {modeLetter}
                                  </span>
                                ) : null}
                              </div>
                              <div className="text-ink">
                                {cell.description}
                              </div>
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
                              {/* DESCRIPTION_2..N stacked under each
                                  version's headline so semantic /
                                  lexical divergence stays visible
                                  per-version, not just per-run. */}
                              {Array.isArray(cell.alternatives) &&
                              cell.alternatives.length > 1 ? (
                                <div className="mt-1 space-y-0.5 border-t border-surface-border/40 pt-1">
                                  {cell.alternatives
                                    .slice(1)
                                    .map((alt, idx) => (
                                      <div
                                        key={idx}
                                        className="flex items-start justify-between gap-2 text-[11px] leading-snug"
                                      >
                                        <div className="min-w-0 flex-1">
                                          <span className="mr-1.5 inline-block w-3 text-[10px] text-ink-dim">
                                            {String.fromCharCode(66 + idx)}
                                          </span>
                                          <span className="text-ink-muted">
                                            {alt.text}
                                          </span>
                                        </div>
                                        <AltScoreBadge
                                          band={alt.band ?? null}
                                          score={alt.score ?? null}
                                          signal={alt.signal ?? null}
                                        />
                                      </div>
                                    ))}
                                </div>
                              ) : null}
                            </div>
                          );
                        })}
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

// ── PR C — cell-mode comparison ────────────────────────────────────────────
//
// Reads the pinned cells from the URL (``?mode=cell&cells=schema.table.col:rid,..``)
// and renders one section per distinct cell with rows = runs. Talks to
// ``GET /api/history/compare/cell`` — one request per distinct cell.
// The backend marks the winning run per cell so the SPA only has to
// highlight; it doesn't recompute logprob ordering client-side.

interface CellComparePerRun {
  run_id: number;
  result_id?: number | null;
  description: string;
  confidence?: string | null;
  logprob_score?: number | null;
  source?: string | null;
  citations?: unknown[];
  accepted?: boolean;
  applied_at?: number | null;
}

interface CellCompareSingleResponse {
  cell: {
    database: string | null;
    schema: string;
    table: string;
    column: string | null;
  };
  per_run: Array<CellComparePerRun | null>;
  best_run_id: number | null;
}

/** Parse the ``cells`` query param token list — same wire format the
 *  drawer emits via ``pinnedCellToToken``. Returns ``[]`` on a missing
 *  or malformed param so the panel renders an empty state instead of
 *  crashing on a manually-edited URL. */
function parseCellsParam(
  raw: string | null,
): Array<{ path: string; run_id: number }> {
  if (!raw) return [];
  return raw
    .split(",")
    .map((tok) => tok.trim())
    .filter(Boolean)
    .map((tok) => {
      const [path, ridRaw] = tok.split(":");
      const rid = Number(ridRaw);
      if (!path || !Number.isFinite(rid)) return null;
      return { path, run_id: rid };
    })
    .filter((x): x is { path: string; run_id: number } => x !== null);
}

/** Build the set of distinct cell paths + the union of run IDs the
 *  user pinned across them. Used to fire one ``/compare/cell`` request
 *  per distinct cell with the full run list, so two pins of the same
 *  cell from different runs still compare across both. */
function groupCellsByPath(
  tokens: Array<{ path: string; run_id: number }>,
): Array<{ path: string; runs: number[] }> {
  const map = new Map<string, Set<number>>();
  for (const t of tokens) {
    const s = map.get(t.path) ?? new Set<number>();
    s.add(t.run_id);
    map.set(t.path, s);
  }
  return Array.from(map.entries()).map(([path, runs]) => ({
    path,
    runs: Array.from(runs).sort((a, b) => a - b),
  }));
}

function CellCompareView({ cellsParam }: { cellsParam: string }) {
  const tokens = useMemo(() => parseCellsParam(cellsParam), [cellsParam]);
  const grouped = useMemo(() => groupCellsByPath(tokens), [tokens]);

  if (grouped.length === 0) {
    return (
      <EmptyState
        title="No pinned cells yet"
        description="Use the pin icon on any run-detail row to add cells here, then click 'Compare pinned cells' from the topbar drawer."
      />
    );
  }

  return (
    <div className="flex flex-col gap-4">
      {grouped.map(({ path, runs }) => (
        <CellCompareSection key={path} path={path} runs={runs} />
      ))}
    </div>
  );
}

function CellCompareSection({
  path,
  runs,
}: {
  path: string;
  runs: number[];
}) {
  // We don't actually know the database segment client-side — the
  // pin tokens elide it. The endpoint accepts any non-empty string
  // there, so we pass a placeholder; the response echoes ``null``
  // back in ``cell.database`` and we render only the path the user
  // pinned. Future iterations can lift the active db into the token.
  const cellKey = `db.${path}`;
  const query = useQuery({
    queryKey: ["compare-cell", path, runs.join(",")],
    queryFn: () =>
      apiFetch<CellCompareSingleResponse>(
        `/api/history/compare/cell?cell=${encodeURIComponent(cellKey)}&runs=${runs.join(",")}`,
      ),
    retry: false,
    // Errors render inline in the cell card body.
    meta: { silentError: true },
  });

  const isColumnLevel = path.split(".").length === 3;

  return (
    <Card>
      <CardHeader
        title={
          <span className="font-mono text-sm">{path}</span>
        }
        description={
          isColumnLevel ? "Column-level cell" : "Table-level cell"
        }
      />
      <CardBody className="p-0 overflow-x-auto">
        {query.isLoading ? (
          <div className="px-5 py-4 text-xs text-ink-dim">Loading…</div>
        ) : query.isError ? (
          <div className="px-5 py-4 text-xs text-warning-ink">
            Failed to load cell comparison: {(query.error as Error).message}
          </div>
        ) : query.data ? (
          <CellCompareTable data={query.data} />
        ) : null}
      </CardBody>
    </Card>
  );
}

function CellCompareTable({ data }: { data: CellCompareSingleResponse }) {
  return (
    <table className="w-full text-xs">
      <thead className="bg-surface-subtle/40 text-[10px] uppercase tracking-wider text-ink-dim">
        <tr>
          <th className="px-3 py-2 text-left">Run #</th>
          <th className="px-3 py-2 text-left">Description</th>
          <th className="px-3 py-2 text-left">Confidence</th>
          <th className="px-3 py-2 text-left">Logprob</th>
          <th className="px-3 py-2 text-left">Source</th>
          <th className="px-3 py-2 text-left">Citations</th>
          <th className="px-3 py-2 text-left"></th>
        </tr>
      </thead>
      <tbody className="divide-y divide-border">
        {data.per_run.map((entry, idx) => {
          if (!entry) {
            return (
              <tr key={`empty-${idx}`} className="text-ink-dim">
                <td className="px-3 py-2 font-mono">—</td>
                <td className="px-3 py-2" colSpan={6}>
                  (not present in this run)
                </td>
              </tr>
            );
          }
          const isBest = data.best_run_id === entry.run_id;
          const citeCount = Array.isArray(entry.citations)
            ? entry.citations.length
            : 0;
          return (
            <tr
              key={entry.run_id}
              className={cn(isBest && "bg-positive/5")}
            >
              <td className="px-3 py-2 font-mono">
                <Link
                  to={`/runs/${entry.run_id}`}
                  className="text-accent hover:underline"
                >
                  #{entry.run_id}
                </Link>
                {isBest && (
                  <span
                    className="ml-1 rounded bg-positive/15 px-1 text-[9px] uppercase tracking-wider text-positive"
                    title="Highest logprob across pinned runs"
                  >
                    best
                  </span>
                )}
              </td>
              <td className="px-3 py-2">
                <p className="whitespace-pre-line">
                  {entry.description || (
                    <span className="text-ink-dim">(empty)</span>
                  )}
                </p>
              </td>
              <td className="px-3 py-2">
                <ConfidencePill
                  value={entry.confidence ?? ""}
                  score={entry.logprob_score ?? null}
                />
              </td>
              <td className="px-3 py-2">
                <LogprobBadge score={entry.logprob_score ?? null} />
              </td>
              <td className="px-3 py-2 text-ink-dim">
                {entry.source ?? "—"}
              </td>
              <td className="px-3 py-2 text-ink-dim">{citeCount}</td>
              <td className="px-3 py-2 text-right">
                <button
                  type="button"
                  disabled
                  title="Future: apply this version to the live DB."
                  className="rounded border border-border px-2 py-0.5 text-[10px] text-ink-dim opacity-60"
                >
                  Use this
                </button>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
