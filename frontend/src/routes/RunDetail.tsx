import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useParams, Link, useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Activity as ActivityIcon, Loader2, PauseCircle, PlayCircle, RefreshCw, SkipForward, Timer } from "lucide-react";

import { apiFetch, api } from "../lib/api";
import { useEventSource, type SseEvent } from "../lib/sse";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import JobProgress from "../components/JobProgress";
import StatusPill from "../components/StatusPill";
import { ConfidencePill, LogprobBadge } from "../components/ui/InsightBadges";
import RerunDialog from "../components/RerunDialog";
import { cn } from "../lib/cn";
import {
  AlertDialog,
  Badge,
  Button,
  Checkbox,
  IconButton,
  InlineEditText,
  Tab as TabTrigger,
  TabPanel,
  Tabs,
  TabsList,
  useToast,
} from "../components/ui";

interface RunDetailPayload {
  id: number;
  command: string;
  status: string;
  /** Backend keys end in `_json` after the parser hydrates them; the
      legacy aliases are kept for safety. */
  scope_json?: Record<string, string[]> | null;
  metrics_json?: Record<string, unknown> | null;
  settings_json?: Record<string, unknown> | null;
  tokens_json?: Record<string, unknown> | null;
  scope?: Record<string, string[]>;
  metrics?: Record<string, unknown>;
  settings?: Record<string, unknown>;
  llm_model?: string | null;
  /** Provider attached to the LLM model recorded for this run.
   *  Carried by the backend so the run-detail Cost row can resolve a
   *  live price lookup without a second round-trip to fetch profile
   *  metadata. */
  llm_provider?: string | null;
  duration_sec?: number | null;
  /** DB scope the run was rooted at — the apply path needs these
      to land COMMENT statements on the same database the user
      actually scoped (Postgres throws schema-not-found errors when
      the active profile points elsewhere). */
  db_profile?: string | null;
  database?: string | null;
  catalog?: string | null;
  /** Set by the backend when a worker thread is still alive for this
      run id. SPA subscribes to /api/runs/{job}/events while it's
      present so a numeric-id detail page shows live progress. */
  live_job_id?: string | null;
}

interface ResultRow {
  id: number;
  run_id: number;
  schema_name: string;
  table_name: string;
  column_name: string | null;
  asset_kind: string;
  confidence: string;
  source: string;
  logprob_score: number | null;
  alternatives_json?: string[] | unknown;
  chosen_description: string | null;
  evaluation: string | null;
  applied_at: number | null;
  /** One-sentence justification the LLM emitted alongside the
   *  description ("REASONING:" line in the prompt). Persisted in
   *  ``run_results.reasoning`` and surfaced to the CLI's interactive
   *  review prompt; rendered on the Studio Run detail page so the
   *  reviewer can see WHY the model picked the chosen wording
   *  without having to fall back to the CLI. Empty / null on legacy
   *  rows that predate the prompt change demanding the line. */
  reasoning?: string | null;
  /** Re-Run versioning fields. ``rerun_seq`` is 0 for originals,
   *  1+ for successive re-runs. ``parent_result_id`` chains a re-run
   *  back to the original (NULL on originals). ``user_instructions``
   *  is the optional free-text addendum the user typed in the
   *  re-run modal. */
  parent_result_id?: number | null;
  rerun_seq?: number;
  user_instructions?: string | null;
  /** Full version chain when the row was fetched with
   *  ``?include_history=true``. Ordered by ``rerun_seq`` ASC, so
   *  ``history[history.length - 1]`` is always the latest version. */
  history?: ResultRow[];
}

interface ResultsResponse {
  run_id: number;
  results: ResultRow[];
  count: number;
}

type Tab = "summary" | "results" | "scope" | "settings";

// Two URL shapes converge here:
//   * /runs/:runId — historical or completed run (numeric id)
//   * /runs/new-:jobId — a brand-new run still streaming via SSE; we
//     subscribe to /api/runs/{jobId}/events and as soon as the worker
//     emits run.created we rewrite the URL in place to the real id.
export default function RunDetail() {
  const params = useParams();
  const raw = params.runId ?? "";
  const isLive = raw.startsWith("new-");
  const liveJobId = isLive ? raw.slice(4) : "";
  const numericRunId = isLive ? null : Number(raw);

  if (!isLive && (numericRunId === null || !Number.isFinite(numericRunId))) {
    return null;
  }

  return isLive ? (
    <LiveRunStream jobId={liveJobId} />
  ) : (
    <PersistedRunView runId={numericRunId as number} />
  );
}

interface ColumnDetail {
  result_id: number | null;
  schema: string;
  table: string;
  column: string | null;
  asset_kind: string;
  confidence: string;
  logprob_score: number | null;
  alternatives: unknown;
  chosen_description: string;
  source?: string;
  /** LLM's justification for the chosen wording. Forwarded by
   *  ``_column_details_for_table`` (web/routers/runs.py) so the live
   *  per-column card surfaces it in the same place the persisted
   *  Run detail page does. Empty string when missing. */
  reasoning?: string;
}

interface ActivityRow {
  idx: number;
  label: string;
  status: "running" | "done" | "failed";
  detail?: string;
  results?: ColumnDetail[];
}

function LiveRunStream({ jobId }: { jobId: string }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [activities, setActivities] = useState<ActivityRow[]>([]);
  const [resolvedRunId, setResolvedRunId] = useState<number | null>(null);
  const [scope, setScope] = useState<Record<string, string[]>>({});
  // Latest tokens.snapshot — the run worker emits one after every
  // per-table activity.complete so the live banner can show the
  // running USD figure (parity with the CLI's LiveDisplay header).
  const [tokensSnapshot, setTokensSnapshot] = useState<{
    total_tokens: number;
    total_cost_usd: number;
  } | null>(null);
  // Per-step running totals derived from activity.complete events
  // when the SSE payload included a ``results`` array. Lets the
  // Tokens & cost card render an Input / Output / Total breakdown
  // even before the run hits ``finish_run`` (the persisted Metrics
  // card does not exist yet at this point).
  const [tokensBreakdown, setTokensBreakdown] = useState<{
    input: number;
    output: number;
  }>({ input: 0, output: 0 });
  // Latest LiveDisplay step bridged from the worker (Profiling X /
  // Calling LLM batch Y/Z / etc.). Empty string when nothing is in
  // flight (between activities).
  const [lastStep, setLastStep] = useState<string | null>(null);
  // Wall-clock when the current step started — drives the per-step
  // "(12s)" timer so a single long ``step_spinner`` (e.g. a
  // ProfileAgent LLM round-trip on a small table) reads as active
  // instead of frozen. Reset to null between steps.
  const [lastStepStartedAt, setLastStepStartedAt] = useState<number | null>(null);
  const [startTime] = useState(() => Date.now());
  const [now, setNow] = useState(() => Date.now());

  const { events, closed, error } = useEventSource({
    path: `/api/runs/${jobId}/events`,
    enabled: true,
  });

  // REST-side fallback for the ``run.created`` SSE event. EventSource
  // does not replay missed events on reconnect, and ``hs.create_run``
  // exceptions in the worker are caught + logged but never re-emitted.
  // Either case used to leave LiveRunStream stuck on "Run · starting…"
  // forever. Polling the registry every 2s — and once on terminal
  // close — gives us the same numeric ``run_id`` ``run.created`` would
  // have, so the redirect to ``PersistedRunView`` (where the editor
  // lives) becomes reliable.
  const jobInfo = useQuery({
    queryKey: ["job", jobId],
    queryFn: () => apiFetch<{ run_id: number | null; status: string }>(
      `/api/runs/${jobId}`,
    ),
    refetchInterval: closed ? false : 2000,
    refetchOnWindowFocus: false,
    retry: 1,
  });

  // Promote the registry-side ``run_id`` into local state so both
  // the SSE handler and the polling fallback feed the same redirect
  // effect. Whichever lands first wins; the other is a no-op.
  useEffect(() => {
    const fromRegistry = jobInfo.data?.run_id;
    if (typeof fromRegistry === "number" && resolvedRunId == null) {
      setResolvedRunId(fromRegistry);
    }
  }, [jobInfo.data, resolvedRunId]);

  // The moment the worker assigns a numeric run id, hand off to
  // PersistedRunView. That view subscribes to the same SSE stream
  // (via live_job_id) AND mounts the full results editor, so the
  // user can edit alternatives / pick / apply while the worker is
  // still running. Without this redirect the live page is read-only
  // and users had to navigate away to the Runs list to act on the
  // suggestions — exactly the regression reported on PR #219.
  useEffect(() => {
    if (resolvedRunId != null) {
      navigate(`/runs/${resolvedRunId}`, { replace: true });
    }
  }, [resolvedRunId, navigate]);

  // 1 Hz wall-clock so the live banner's elapsed timer updates in
  // real time without re-rendering on every SSE event.
  useEffect(() => {
    if (closed) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [closed]);

  // Each render of this effect must process only the events that
  // arrived since the previous render — iterating ``events`` from the
  // start re-applies every state-mutating dispatch, so a single
  // ``step.complete`` showed up three times in "RECENT STEPS" and the
  // ``activity.added`` branch (no idempotence check) duplicated rows.
  // ``processedRef`` tracks the high-water mark; resets to zero when
  // the SSE hook clears ``events`` on a reconnect.
  const liveProcessedRef = useRef(0);
  useEffect(() => {
    if (events.length < liveProcessedRef.current) {
      liveProcessedRef.current = 0;
    }
    const start = liveProcessedRef.current;
    liveProcessedRef.current = events.length;
    for (let i = start; i < events.length; i++) {
      const event = events[i];
      const t = String(event.type || "");
      if (t === "run.created" && typeof event.run_id === "number") {
        setResolvedRunId(event.run_id);
      } else if (t === "run.scope.resolved" && event.scope) {
        setScope(event.scope as Record<string, string[]>);
      } else if (t === "activity.added") {
        setActivities((curr) => [
          ...curr,
          {
            idx: Number(event.idx ?? curr.length),
            label: String(event.label ?? "—"),
            status: "running",
          },
        ]);
      } else if (t === "activity.complete") {
        const rawResults = Array.isArray(event.results) ? event.results : [];
        const results = rawResults as ColumnDetail[];
        setActivities((curr) =>
          curr.map((a) =>
            a.idx === Number(event.idx)
              ? {
                  ...a,
                  status: "done",
                  detail: String(event.detail ?? ""),
                  results,
                }
              : a,
          ),
        );
      } else if (t === "activity.fail") {
        setActivities((curr) =>
          curr.map((a) =>
            a.idx === Number(event.idx)
              ? { ...a, status: "failed", detail: String(event.detail ?? "") }
              : a,
          ),
        );
      } else if (t === "tokens.snapshot") {
        setTokensSnapshot({
          total_tokens: Number(event.total_tokens ?? 0),
          total_cost_usd: Number(event.total_cost_usd ?? 0),
        });
        setTokensBreakdown({
          input: Number(event.input_tokens ?? 0),
          output: Number(event.output_tokens ?? 0),
        });
      } else if (
        t === "step.added" ||
        t === "step.begin" ||
        t === "step.update"
      ) {
        const label = String(event.label ?? "").trim();
        if (label) {
          setLastStep(label);
          setLastStepStartedAt(Date.now());
        }
      } else if (t === "step.thinking") {
        const label = String(event.label ?? "Thinking").trim();
        setLastStep(label ? `${label}…` : "Thinking…");
        setLastStepStartedAt(Date.now());
      } else if (t === "step.complete" || t === "step.fail") {
        setLastStep(null);
        setLastStepStartedAt(null);
      } else if (t === "tokens.delta") {
        const totalIn = Number(event.total_in ?? 0);
        const totalOut = Number(event.total_out ?? 0);
        const cost = Number(event.total_cost_usd ?? 0);
        if (totalIn || totalOut) {
          setTokensSnapshot({
            total_tokens: totalIn + totalOut,
            total_cost_usd: cost,
          });
        }
      }
    }
  }, [events]);

  // When the run finishes (job.done / job.failed), redirect to the
  // persisted detail view so the user lands on the Results tab.
  useEffect(() => {
    if (closed && resolvedRunId != null) {
      queryClient.invalidateQueries({ queryKey: ["recent-runs"] });
      navigate(`/runs/${resolvedRunId}`, { replace: true });
    }
  }, [closed, resolvedRunId, navigate, queryClient]);

  const toast = useToast();
  const [confirmCancel, setConfirmCancel] = useState(false);
  const cancel = useMutation({
    mutationFn: () => api.cancelRun(jobId),
    onSuccess: () => {
      setConfirmCancel(false);
      toast.push({
        title: "Cancellation requested",
        description: "The worker bails between rows; already-written changes stay.",
        tone: "warning",
      });
    },
    onError: (e: Error) => {
      setConfirmCancel(false);
      toast.push({
        title: "Cancel failed",
        description: e.message,
        tone: "error",
      });
    },
  });

  const lastEvent = events[events.length - 1] as SseEvent | undefined;
  const terminalKind = lastEvent
    ? String(lastEvent.type || "")
    : "";

  // Newest still-running activity is the "current" thing the agent
  // is doing; falls back to the latest entry overall when nothing
  // is in flight (e.g. between two tables).
  const currentActivity = useMemo(() => {
    for (let i = activities.length - 1; i >= 0; i--) {
      if (activities[i].status === "running") return activities[i];
    }
    return activities[activities.length - 1];
  }, [activities]);
  const completedCount = activities.filter((a) => a.status !== "running").length;
  const elapsedSec = Math.max(0, Math.floor((now - startTime) / 1000));
  // Per-step elapsed seconds for the LiveRunStream banner. Same
  // shape + semantics as the LiveProgress card below — see the
  // comment block on ``currentStepElapsedSec`` there for the why.
  const currentStepElapsedSec =
    lastStep && lastStepStartedAt
      ? Math.max(0, Math.floor((now - lastStepStartedAt) / 1000))
      : null;
  const stepTimerSuffix =
    currentStepElapsedSec != null && currentStepElapsedSec > 0
      ? ` (${formatElapsed(currentStepElapsedSec)})`
      : "";

  return (
    <>
      <PageHeader
        title={
          resolvedRunId
            ? `Run #${resolvedRunId}`
            : closed
              ? terminalKind === "job.done"
                ? "Run · ended"
                : terminalKind === "job.cancelled"
                  ? "Run · cancelled"
                  : terminalKind === "job.failed"
                    ? "Run · failed"
                    : "Run · ended"
              : activities.length > 0
                ? "Run · running…"
                : "Run · starting…"
        }
        breadcrumbs={[
          { label: "Runs", to: "/runs" },
          { label: resolvedRunId ? `#${resolvedRunId}` : "live" },
        ]}
        actions={
          !closed && (
            <Button
              variant="danger"
              size="md"
              leadingIcon={<PauseCircle size={14} />}
              onClick={() => setConfirmCancel(true)}
              disabled={cancel.isPending}
            >
              Cancel
            </Button>
          )
        }
      />
      {!closed && (
        <div className="mb-4 flex flex-wrap items-center gap-3 rounded-lg border border-accent/30 bg-accent-soft/40 px-4 py-2.5 text-sm">
          <span className="inline-flex items-center gap-1.5 font-mono text-accent-ink">
            <Loader2 size={14} className="animate-spin" />
            running
          </span>
          <span className="inline-flex items-center gap-1.5 font-mono tabular-nums text-ink">
            <Timer size={13} className="text-ink-muted" />
            {formatElapsed(elapsedSec)}
          </span>
          <span className="inline-flex items-center gap-1.5 text-ink">
            <ActivityIcon size={13} className="text-ink-muted" />
            <span className="text-ink-muted">Now:</span>
            <span className="font-mono">
              {currentActivity
                ? lastStep
                  ? `${currentActivity.label} — ${lastStep}${stepTimerSuffix}`
                  : currentActivity.label
                : lastStep
                  ? `${lastStep}${stepTimerSuffix}`
                  : "Waiting for the worker…"}
            </span>
          </span>
          {tokensSnapshot && tokensSnapshot.total_tokens > 0 && (
            <span
              className="inline-flex items-center gap-1.5 font-mono tabular-nums text-ink"
              title={
                tokensSnapshot.total_cost_usd > 0
                  ? `Running USD cost based on the prices recorded with each LLM call. Refresh prices via /refresh-prices.`
                  : `No price recorded for this model — run /refresh-prices or set a custom override via /cost.`
              }
            >
              <span className="text-ink-muted">↓</span>
              {tokensSnapshot.total_tokens.toLocaleString()} tok
              {tokensSnapshot.total_cost_usd > 0 && (
                <span className="text-positive">
                  · ${tokensSnapshot.total_cost_usd.toFixed(4)}
                </span>
              )}
            </span>
          )}
          <span className="ml-auto text-xs text-ink-dim">
            {completedCount}/{activities.length || "—"} processed
          </span>
        </div>
      )}
      <AlertDialog
        open={confirmCancel}
        onClose={() => setConfirmCancel(false)}
        onConfirm={() => cancel.mutate()}
        loading={cancel.isPending}
        title="Cancel this run?"
        description="The worker exits between rows. Already-written descriptions stay; in-flight assets stop. This cannot be undone."
        confirmLabel="Cancel run"
      />
      {tokensSnapshot && tokensSnapshot.total_tokens > 0 && (
        <Card className="mb-4">
          <CardHeader title="Tokens & cost" description="Running totals while the worker streams. Frozen at run end into the persisted Metrics card." />
          <CardBody>
            <div className="grid grid-cols-2 gap-4 text-sm md:grid-cols-4">
              <Row label="Input">
                <span className="font-mono tabular-nums text-xs">
                  ↑ {tokensBreakdown.input.toLocaleString()}
                </span>
              </Row>
              <Row label="Output">
                <span className="font-mono tabular-nums text-xs">
                  ↓ {tokensBreakdown.output.toLocaleString()}
                </span>
              </Row>
              <Row label="Total">
                <span className="font-mono tabular-nums text-xs font-semibold">
                  {tokensSnapshot.total_tokens.toLocaleString()}
                </span>
              </Row>
              <Row label="Cost">
                <span className="font-mono tabular-nums text-xs">
                  {tokensSnapshot.total_cost_usd > 0 ? (
                    <span className="text-positive">
                      ${tokensSnapshot.total_cost_usd.toFixed(4)}
                    </span>
                  ) : (
                    <span
                      className="text-ink-dim"
                      title="No price recorded for this model — set a custom override via /cost or run /refresh-prices."
                    >
                      —
                    </span>
                  )}
                </span>
              </Row>
            </div>
          </CardBody>
        </Card>
      )}
      <Card>
        <CardHeader
          title="Progress"
          description={
            !closed
              ? "Live activity from the worker."
              : terminalKind === "job.done"
                ? "Run completed."
                : terminalKind === "job.cancelled"
                  ? "Run cancelled."
                  : "Run failed."
          }
        />
        <CardBody className="p-0">
          {Object.keys(scope).length > 0 && (
            <div className="border-b border-surface-border px-5 py-3 text-xs text-ink-muted">
              <span className="text-ink-dim">Scope:</span>{" "}
              <span className="font-mono">
                {Object.entries(scope)
                  .map(([s, t]) =>
                    t.length === 0 ? `${s}.*` : t.map((x) => `${s}.${x}`).join(", "),
                  )
                  .join(" · ")}
              </span>
            </div>
          )}
          {activities.length === 0 ? (
            <div className="px-5 py-6 text-sm text-ink-dim">
              <Loader2 size={14} className="mr-2 inline animate-spin" />
              {lastStep ? `${lastStep}${stepTimerSuffix}` : "Waiting for the worker to begin…"}
            </div>
          ) : (
            <ul className="divide-y divide-surface-border">
              {activities.map((a) => (
                <li key={a.idx} className="px-5 py-2.5 text-sm">
                  <div className="flex items-center gap-3">
                    <ActivityDot status={a.status} />
                    <span className="font-mono">{a.label}</span>
                    {a.detail && (
                      <span className="ml-auto text-xs text-ink-muted">
                        {a.detail}
                      </span>
                    )}
                  </div>
                  {a.results && a.results.length > 0 && (
                    <div className="mt-3 space-y-2 border-l border-surface-border pl-4">
                      {a.results.map((r, idx) => (
                        <ColumnSuggestionCard
                          key={r.result_id ?? `${a.idx}-${idx}`}
                          detail={r}
                        />
                      ))}
                    </div>
                  )}
                </li>
              ))}
            </ul>
          )}
          {error && (
            <div className="border-t border-surface-border px-5 py-2 text-xs text-critical">
              {error}
            </div>
          )}
        </CardBody>
      </Card>
    </>
  );
}

function ColumnSuggestionCard({ detail }: { detail: ColumnDetail }) {
  const alts = normalizeAlternatives(detail.alternatives);
  const tone =
    detail.confidence === "high"
      ? "positive"
      : detail.confidence === "low"
        ? "warning"
        : "neutral";
  return (
    <div className="rounded-md border border-surface-border bg-surface p-3">
      <div className="flex items-center gap-2">
        <span className="font-mono text-xs">
          {detail.column ?? <em className="text-ink-dim">(table)</em>}
        </span>
        <StatusPill tone={tone}>{detail.confidence}</StatusPill>
        {detail.logprob_score != null && (
          <span className="font-mono text-[10px] text-ink-dim">
            logprob {detail.logprob_score.toFixed(3)}
          </span>
        )}
        {detail.source && (
          <span className="ml-auto text-[10px] uppercase tracking-wider text-ink-dim">
            {detail.source}
          </span>
        )}
      </div>
      <ReasoningDisclosure reasoning={detail.reasoning ?? ""} />
      <div className="mt-2 space-y-1">
        {alts.length === 0 ? (
          <div className="text-xs text-ink-dim">{detail.chosen_description || "—"}</div>
        ) : (
          alts.map((alt, idx) => {
            const isChosen =
              alt === detail.chosen_description || (idx === 0 && !detail.chosen_description);
            return (
              <div
                key={idx}
                className={cn(
                  "rounded border px-2 py-1 text-xs",
                  isChosen
                    ? "border-accent/40 bg-accent-soft/30 text-ink"
                    : "border-surface-border text-ink-muted",
                )}
              >
                <span className="mr-1.5 inline-block w-3 text-[10px] text-ink-dim">
                  {String.fromCharCode(65 + idx)}
                </span>
                {alt}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}

/** One-sentence justification the LLM emitted alongside the
 *  description. The CLI prints this inline at review time
 *  (``Reasoning: …``); Studio renders it as a small italic muted
 *  disclosure under the chosen description. Long reasoning clamps
 *  to two lines by default, click to expand the full text inline.
 *  Hidden entirely when the value is empty / missing so legacy
 *  rows that predate the prompt change don't render an empty stub.
 */
function ReasoningDisclosure({ reasoning }: { reasoning: string }) {
  const [expanded, setExpanded] = useState(false);
  const trimmed = (reasoning || "").trim();
  if (!trimmed) return null;
  return (
    <button
      type="button"
      onClick={() => setExpanded((v) => !v)}
      className="mt-2 block w-full rounded-md border border-border bg-surface-subtle/20 px-2.5 py-1.5 text-left text-xs italic text-ink-muted transition hover:border-accent/30 hover:text-ink"
      title={expanded ? "Click to collapse" : trimmed}
    >
      <span className="not-italic mr-2 text-[10px] uppercase tracking-wider text-ink-dim">
        Why
      </span>
      <span className={cn(expanded ? "" : "line-clamp-2")}>{trimmed}</span>
    </button>
  );
}

function formatElapsed(sec: number): string {
  if (!Number.isFinite(sec) || sec < 0) return "0s";
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  if (m < 60) return `${m}m ${s.toString().padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return `${h}h ${rm.toString().padStart(2, "0")}m`;
}

function normalizeAlternatives(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const out: string[] = [];
  for (const entry of raw) {
    if (typeof entry === "string") {
      out.push(entry);
    } else if (entry && typeof entry === "object") {
      const desc = (entry as { description?: unknown }).description;
      if (typeof desc === "string") out.push(desc);
    }
  }
  return out;
}

/** Compact token + cost chip rendered next to the status badge in the
 *  page header. Surfaces the figures users were complaining were
 *  buried inside the Summary tab — once a run finishes you can tell
 *  at a glance how many tokens it burned and what it cost without
 *  switching tabs. Falls back to nothing for runs that never
 *  recorded tokens (legacy runs, runs that crashed before any LLM
 *  call). */
function RunHeaderTokenCost({ run }: { run: RunDetailPayload }) {
  const tokens = (run.tokens_json ?? {}) as Record<string, unknown>;
  let inputTokens = 0;
  let outputTokens = 0;
  const records = (tokens.records ?? []) as Array<{
    prompt_tokens?: number;
    completion_tokens?: number;
  }>;
  if (records.length > 0) {
    for (const r of records) {
      inputTokens += Number(r.prompt_tokens ?? 0);
      outputTokens += Number(r.completion_tokens ?? 0);
    }
  } else if (Array.isArray(tokens.summary)) {
    for (const row of tokens.summary as unknown[]) {
      if (Array.isArray(row) && row.length >= 4) {
        inputTokens += Number(row[1] ?? 0);
        outputTokens += Number(row[2] ?? 0);
      }
    }
  }
  const totalFromBreakdown = inputTokens + outputTokens;
  const totalTokens =
    totalFromBreakdown > 0
      ? totalFromBreakdown
      : typeof tokens.total_tokens === "number"
        ? Number(tokens.total_tokens)
        : 0;
  const cost = typeof tokens.total_cost_usd === "number" ? tokens.total_cost_usd : null;
  if (totalTokens === 0 && cost == null) return null;
  return (
    <span
      className="inline-flex items-center gap-1 rounded-md border border-surface-border bg-surface-subtle/60 px-2 py-0.5 font-mono tabular-nums text-[11px] text-ink"
      title="Tokens consumed by this run (input ↑ + output ↓) and the USD cost frozen at run time. Open the Summary tab for the per-step breakdown."
    >
      {totalFromBreakdown > 0 && (
        <>
          <span className="text-ink-dim">↑</span>
          {compactTokenCount(inputTokens)}
          <span className="text-ink-dim">↓</span>
          {compactTokenCount(outputTokens)}
        </>
      )}
      {totalFromBreakdown === 0 && totalTokens > 0 && (
        <>
          <span className="text-ink-dim">tokens</span>
          {compactTokenCount(totalTokens)}
        </>
      )}
      {cost != null && cost > 0 && (
        <>
          <span className="text-ink-dim">·</span>
          <span className="text-positive">${cost.toFixed(4)}</span>
        </>
      )}
    </span>
  );
}

/** Show the run's effective scope (DB profile + database/catalog +
 *  picked schemas/tables) right next to the status badge so the
 *  header answers "what did this run touch" before the user scrolls.
 *  Empty scope renders nothing — the user already knows it was an
 *  all-schemas run from the lack of a chip. */
function RunHeaderScope({ run }: { run: RunDetailPayload }) {
  const scope = (run.scope_json ?? run.scope ?? {}) as Record<string, unknown>;
  const target =
    (run.database && run.database.trim()) ||
    (run.catalog && run.catalog.trim()) ||
    "";
  const entries = Object.entries(scope).filter(([k]) => k && k !== "db_profile");
  if (entries.length === 0 && !target && !run.db_profile) return null;
  const tableLabel = (() => {
    if (entries.length === 0) return "All schemas";
    let totalTables = 0;
    let allEmpty = true;
    for (const [, v] of entries) {
      if (Array.isArray(v) && v.length > 0) {
        totalTables += v.length;
        allEmpty = false;
      }
    }
    if (entries.length === 1) {
      const [s, v] = entries[0];
      if (Array.isArray(v) && v.length > 0) {
        return `${s} · ${v.length} ${v.length === 1 ? "table" : "tables"}`;
      }
      return `${s} (all tables)`;
    }
    if (allEmpty) return `${entries.length} schemas`;
    return `${entries.length} schemas · ${totalTables} ${totalTables === 1 ? "table" : "tables"}`;
  })();
  const targetLabel = target ? ` @ ${target}` : "";
  const profileLabel = run.db_profile ? `${run.db_profile}` : "";
  return (
    <span
      className="inline-flex items-center gap-1 rounded-md border border-surface-border bg-surface-subtle/60 px-2 py-0.5 font-mono text-[11px] text-ink-muted"
      title="Scope this run targeted. Open the Scope tab for the raw JSON."
    >
      <span className="text-ink-dim">scope</span>
      <span className="text-ink">
        {profileLabel ? `${profileLabel}${targetLabel} · ` : ""}
        {tableLabel}
      </span>
    </span>
  );
}

/** Render token counts with K/M compaction so the header chip stays
 *  one line on narrow viewports. Mirrors the Home overview cards'
 *  formatter so the two surfaces feel consistent. */
function compactTokenCount(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return "0";
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 10_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

function PersistedRunActivityCard({ jobId }: { jobId: string }) {
  // Compact live-progress panel rendered inside PersistedRunView when
  // the run still has a worker thread. Subscribes to the same SSE
  // stream LiveRunStream uses; reusing that whole component would
  // also pull in the page header rewrite + cancel dialog which a
  // user landing on a numeric run id doesn't need.
  const [activities, setActivities] = useState<ActivityRow[]>([]);
  const [tokensSnapshot, setTokensSnapshot] = useState<{
    total_tokens: number;
    total_cost_usd: number;
  } | null>(null);
  // Latest LiveDisplay step bridged from the worker. Mirrors what
  // the CLI shows in its Rich Live region — "Profiling address.state",
  // "Calling LLM (batch 1/3)", "Saving 12 suggestions". Without this,
  // the Live progress card stalled on the current per-table activity
  // for the entire 5–30 minute window the agents took to drive a
  // single table through profile + RAG + LLM batch.
  const [lastStep, setLastStep] = useState<string | null>(null);
  // Wall-clock when the current ``lastStep`` started. The "Now:"
  // line uses this to render "(12s)" next to the step label and
  // tick once per second, so a single long ``step_spinner`` (e.g.
  // a Profile Agent LLM round-trip on a small table) reads as
  // active instead of frozen. CLI's ``console.status`` does the
  // same; this brings Studio to feature parity.
  const [lastStepStartedAt, setLastStepStartedAt] = useState<number | null>(null);
  // Recently-completed sub-steps (last 5) so the user can see a
  // rolling "what just happened" trail instead of just the current
  // step. Each entry carries elapsed-from-start so the history reads
  // like a timeline. Capped to 5 to keep the card height bounded.
  const [recentSteps, setRecentSteps] = useState<
    Array<{ id: number; label: string; offsetSec: number }>
  >([]);
  const recentIdRef = useRef(0);
  // Wall-clock for the elapsed-time chip. Mirrors LiveRunStream so
  // the user can see the run has been going for "1m 47s" without
  // having to wait for the next SSE event to arrive.
  const [startTime] = useState(() => Date.now());
  const [now, setNow] = useState(() => Date.now());
  const { events, closed, error } = useEventSource({
    path: `/api/runs/${jobId}/events`,
    enabled: true,
  });
  useEffect(() => {
    if (closed) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [closed]);

  // Same fix as ``LiveRunStream`` above: only iterate the *new* tail of
  // ``events`` so each ``step.complete`` is appended to ``recentSteps``
  // exactly once. Without this, every fresh SSE event re-ran the
  // dispatch over the whole array, so a single completion showed up
  // three times in the rolling history with the same ``+0s`` offset.
  const persistedProcessedRef = useRef(0);
  useEffect(() => {
    if (events.length < persistedProcessedRef.current) {
      persistedProcessedRef.current = 0;
    }
    const start = persistedProcessedRef.current;
    persistedProcessedRef.current = events.length;
    for (let i = start; i < events.length; i++) {
      const event = events[i];
      const t = String(event.type || "");
      if (t === "activity.added") {
        setActivities((curr) => {
          const idx = Number(event.idx ?? curr.length);
          if (curr.some((a) => a.idx === idx)) return curr;
          return [
            ...curr,
            {
              idx,
              label: String(event.label ?? "—"),
              status: "running",
            },
          ];
        });
      } else if (t === "activity.complete") {
        const rawResults = Array.isArray(event.results) ? event.results : [];
        const results = rawResults as ColumnDetail[];
        setActivities((curr) =>
          curr.map((a) =>
            a.idx === Number(event.idx)
              ? {
                  ...a,
                  status: "done",
                  detail: String(event.detail ?? ""),
                  results,
                }
              : a,
          ),
        );
        setLastStep(null);
        setLastStepStartedAt(null);
      } else if (t === "activity.fail") {
        setActivities((curr) =>
          curr.map((a) =>
            a.idx === Number(event.idx)
              ? { ...a, status: "failed", detail: String(event.detail ?? "") }
              : a,
          ),
        );
        setLastStep(null);
        setLastStepStartedAt(null);
      } else if (
        t === "step.added" ||
        t === "step.begin" ||
        t === "step.update"
      ) {
        const label = String(event.label ?? "").trim();
        if (label) {
          setLastStep(label);
          setLastStepStartedAt(Date.now());
        }
      } else if (t === "step.thinking") {
        const label = String(event.label ?? "Thinking").trim();
        setLastStep(label ? `${label}…` : "Thinking…");
        setLastStepStartedAt(Date.now());
      } else if (t === "step.complete" || t === "step.fail") {
        const label = String(event.label ?? "").trim();
        if (label) {
          const offsetSec = Math.max(0, Math.floor((Date.now() - startTime) / 1000));
          recentIdRef.current += 1;
          const entryId = recentIdRef.current;
          setRecentSteps((curr) => {
            const next = [
              ...curr,
              { id: entryId, label: `${t === "step.fail" ? "✗ " : "✓ "}${label}`, offsetSec },
            ];
            return next.length > 5 ? next.slice(next.length - 5) : next;
          });
        }
        setLastStep(null);
        setLastStepStartedAt(null);
      } else if (t === "tokens.snapshot") {
        setTokensSnapshot({
          total_tokens: Number(event.total_tokens ?? 0),
          total_cost_usd: Number(event.total_cost_usd ?? 0),
        });
      } else if (t === "tokens.delta") {
        // Running totals from the bridged LiveDisplay
        // ``add_session_tokens`` call. Every LLM round-trip pushes
        // this; we coalesce into the same banner the per-table
        // tokens.snapshot drives.
        const totalIn = Number(event.total_in ?? 0);
        const totalOut = Number(event.total_out ?? 0);
        const cost = Number(event.total_cost_usd ?? 0);
        if (totalIn || totalOut) {
          setTokensSnapshot({
            total_tokens: totalIn + totalOut,
            total_cost_usd: cost,
          });
        }
      }
    }
  }, [events]);

  const completed = activities.filter((a) => a.status !== "running").length;
  const total = activities.length;
  const current = activities.find((a) => a.status === "running");
  const elapsedSec = Math.max(0, Math.floor((now - startTime) / 1000));
  // Per-step elapsed seconds, ticking once per second as ``now``
  // advances. Renders next to ``lastStep`` so a long-running
  // single ``step_spinner`` (e.g. a 30s Profile Agent LLM call on
  // a small table) reads as active. Falls back to null when no
  // step is in flight so the description doesn't show a stale
  // timer between steps.
  const currentStepElapsedSec =
    lastStep && lastStepStartedAt
      ? Math.max(0, Math.floor((now - lastStepStartedAt) / 1000))
      : null;
  const stepTimerSuffix =
    currentStepElapsedSec != null && currentStepElapsedSec > 0
      ? ` (${formatElapsed(currentStepElapsedSec)})`
      : "";

  return (
    <Card className="mb-4 border-accent/40">
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Loader2
              size={14}
              className={cn(
                "text-accent",
                !closed && "animate-spin",
              )}
            />
            Live progress
            {total > 0 && (
              <span
                className="rounded bg-surface-subtle px-1.5 py-0.5 font-mono text-[10px] text-ink-muted"
                title={
                  total === 1
                    ? "Tables in this run."
                    : `Tables completed (${completed} of ${total}).`
                }
              >
                {completed}/{total} {total === 1 ? "table" : "tables"}
              </span>
            )}
            {!closed && (
              <span
                className="inline-flex items-center gap-1 rounded bg-surface-subtle px-1.5 py-0.5 font-mono tabular-nums text-[10px] text-ink-muted"
                title="Time elapsed since the run page loaded"
              >
                <Timer size={10} />
                {formatElapsed(elapsedSec)}
              </span>
            )}
          </span>
        }
        description={
          closed
            ? "Worker exited. The status badge above will refresh automatically."
            : current
              ? lastStep
                ? `Now: ${current.label} — ${lastStep}${stepTimerSuffix}`
                : `Now: ${current.label}`
              : lastStep
                ? `Now: ${lastStep}${stepTimerSuffix}`
                : "Waiting for the worker to begin…"
        }
      />
      <CardBody className="p-0">
        {tokensSnapshot && tokensSnapshot.total_tokens > 0 && (
          <div className="border-b border-surface-border px-5 py-2 text-xs">
            <span className="font-mono tabular-nums text-ink">
              <span className="text-ink-muted">Running totals: </span>↓{" "}
              {tokensSnapshot.total_tokens.toLocaleString()} tokens
              {tokensSnapshot.total_cost_usd > 0 && (
                <span className="ml-2 text-positive">
                  ${tokensSnapshot.total_cost_usd.toFixed(4)}
                </span>
              )}
            </span>
          </div>
        )}
        {recentSteps.length > 0 && (
          <div className="border-b border-surface-border px-5 py-2">
            <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-ink-dim">
              Recent steps
            </p>
            <ul className="space-y-0.5 text-xs">
              {recentSteps.map((s) => (
                <li
                  key={s.id}
                  className="flex items-center gap-2 font-mono tabular-nums text-ink-muted"
                >
                  <span className="text-[10px] text-ink-dim">
                    +{formatElapsed(s.offsetSec)}
                  </span>
                  <span className="truncate text-ink">{s.label}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
        {activities.length === 0 ? (
          <div className="px-5 py-4 text-sm text-ink-dim">
            <Loader2 size={14} className="mr-2 inline animate-spin" />
            {lastStep
              ? `Now: ${lastStep}${stepTimerSuffix}`
              : "Waiting for the worker to begin…"}
          </div>
        ) : (
          <ul className="divide-y divide-surface-border">
            {activities.map((a) => (
              <li key={a.idx} className="px-5 py-2 text-sm">
                <div className="flex items-center gap-3">
                  <ActivityDot status={a.status} />
                  <span className="font-mono">{a.label}</span>
                  {a.detail && (
                    <span className="ml-auto text-xs text-ink-muted">
                      {a.detail}
                    </span>
                  )}
                </div>
              </li>
            ))}
          </ul>
        )}
        {error && (
          <div className="border-t border-surface-border px-5 py-2 text-xs text-critical">
            {error}
          </div>
        )}
      </CardBody>
    </Card>
  );
}

function ActivityDot({ status }: { status: "running" | "done" | "failed" }) {
  const cls =
    status === "done"
      ? "bg-positive"
      : status === "failed"
        ? "bg-critical"
        : "bg-accent animate-pulse";
  return <span className={cn("inline-block h-2 w-2 rounded-full", cls)} />;
}

function PersistedRunView({ runId }: { runId: number }) {
  const [tab, setTab] = useState<Tab>("results");
  const [confirmCancel, setConfirmCancel] = useState(false);
  const toast = useToast();
  const queryClient = useQueryClient();
  const run = useQuery({
    queryKey: ["run", runId],
    queryFn: () => apiFetch<RunDetailPayload>(`/api/history/runs/${runId}`),
    // Poll the run row while there's an active worker so the moment
    // it finishes (live_job_id flips to null) the page updates the
    // status badge without needing a manual refresh. Off when there's
    // no live job to keep the SPA quiet for completed runs.
    refetchInterval: (query) => {
      const data = query.state.data as RunDetailPayload | undefined;
      return data?.live_job_id ? 3000 : false;
    },
  });
  const liveJobId = run.data?.live_job_id ?? null;
  const results = useQuery({
    queryKey: ["run-results", runId],
    // ``include_history=true`` attaches the full re-run chain to each
    // row so the UI can render the latest version's alternatives in
    // place. Without it, a row that has been re-run on a parent run
    // page kept showing the original alternatives — the user had to
    // navigate to the new run id to see the regenerated suggestions.
    queryFn: () =>
      apiFetch<ResultsResponse>(
        `/api/history/runs/${runId}/results?include_history=true`,
      ),
    enabled: tab === "results",
    // Tail the results table while live so partial output streams in
    // alongside the activity panel.
    refetchInterval: liveJobId ? 4000 : false,
  });
  // When the live job ends, refresh the queries one more time so the
  // detail page transitions cleanly from the live activity panel into
  // the persisted results view.
  useEffect(() => {
    if (!liveJobId) {
      queryClient.invalidateQueries({ queryKey: ["run", runId] });
      queryClient.invalidateQueries({ queryKey: ["run-results", runId] });
    }
  }, [liveJobId, runId, queryClient]);

  // Cancel a still-running run from the persisted view. The job
  // registry uses the SSE job id (``live_job_id``), not the numeric
  // analysis_runs id; the backend short-circuits when the worker has
  // already exited so the user racing the poll never gets a 500.
  const cancel = useMutation({
    mutationFn: () => {
      if (!liveJobId) throw new Error("This run already finished.");
      return api.cancelRun(liveJobId);
    },
    onSuccess: () => {
      setConfirmCancel(false);
      toast.push({
        title: "Cancellation requested",
        description: "The worker bails between rows; already-written changes stay.",
        tone: "warning",
      });
      queryClient.invalidateQueries({ queryKey: ["run", runId] });
    },
    onError: (e: Error) => {
      setConfirmCancel(false);
      toast.push({
        title: "Cancel failed",
        description: e.message,
        tone: "error",
      });
    },
  });

  return (
    <>
      <PageHeader
        title={run.data?.command ?? "Loading…"}
        breadcrumbs={[
          { label: "Runs", to: "/runs" },
          { label: `#${runId}` },
        ]}
        description={
          run.data ? (
            <span className="inline-flex flex-wrap items-center gap-x-2 gap-y-1 text-xs">
              <Badge
                tone={
                  run.data.status === "success"
                    ? "positive"
                    : run.data.status === "failed"
                      ? "critical"
                      : run.data.status === "cancelled"
                        ? "warning"
                        : "neutral"
                }
                dot
              >
                {run.data.status}
              </Badge>
              <span className="font-mono text-ink-muted tabular-nums">
                {run.data.duration_sec != null
                  ? `${run.data.duration_sec.toFixed(1)}s`
                  : "—"}
              </span>
              <span className="font-mono text-ink-muted">
                {run.data.llm_model ?? "—"}
              </span>
              <RunHeaderTokenCost run={run.data} />
              <RunHeaderScope run={run.data} />
            </span>
          ) : undefined
        }
        actions={
          <div className="flex items-center gap-3">
            {liveJobId && (
              <Button
                variant="danger"
                size="md"
                leadingIcon={<PauseCircle size={14} />}
                onClick={() => setConfirmCancel(true)}
                disabled={cancel.isPending}
              >
                Cancel
              </Button>
            )}
            <Link to="/runs" className="text-xs text-ink-dim hover:text-ink">
              ← All runs
            </Link>
          </div>
        }
      />
      <AlertDialog
        open={confirmCancel}
        onClose={() => setConfirmCancel(false)}
        onConfirm={() => cancel.mutate()}
        loading={cancel.isPending}
        title="Cancel this run?"
        description="The worker exits between rows. Already-written descriptions stay; in-flight assets stop. This cannot be undone."
        confirmLabel="Cancel run"
      />
      {liveJobId && <PersistedRunActivityCard jobId={liveJobId} />}
      <Tabs value={tab} onValueChange={(v) => setTab(v as Tab)}>
        <TabsList>
          {(["summary", "results", "scope", "settings"] as Tab[]).map((t) => (
            <TabTrigger
              key={t}
              value={t}
              badge={
                t === "results" && results.data?.count != null ? (
                  <span className="ml-1 rounded bg-surface-subtle px-1 text-[10px] text-ink-dim">
                    {results.data.count}
                  </span>
                ) : undefined
              }
            >
              {t[0].toUpperCase() + t.slice(1)}
            </TabTrigger>
          ))}
        </TabsList>
        <TabPanel value="summary">
          <SummaryTab run={run.data} />
        </TabPanel>
        <TabPanel value="results">
          <ResultsTab
            runId={runId}
            loading={results.isLoading}
            rows={results.data?.results ?? []}
            error={results.error as Error | undefined}
            scope={{
              db_profile: run.data?.db_profile ?? null,
              database: run.data?.database ?? null,
              catalog: run.data?.catalog ?? null,
            }}
          />
        </TabPanel>
        <TabPanel value="scope">
          <Card>
            <CardBody>
              <pre className="overflow-x-auto whitespace-pre-wrap rounded-md bg-ink p-3 font-mono text-xs text-bg">
                {JSON.stringify(
                  run.data?.scope_json ?? run.data?.scope ?? {},
                  null,
                  2,
                )}
              </pre>
            </CardBody>
          </Card>
        </TabPanel>
        <TabPanel value="settings">
          <Card>
            <CardHeader title="Settings snapshot" />
            <CardBody>
              <pre className="overflow-x-auto whitespace-pre-wrap rounded-md bg-ink p-3 font-mono text-xs text-bg">
                {JSON.stringify(
                  run.data?.settings_json ?? run.data?.settings ?? {},
                  null,
                  2,
                )}
              </pre>
            </CardBody>
          </Card>
        </TabPanel>
      </Tabs>
    </>
  );
}

function SummaryTab({ run }: { run: RunDetailPayload | undefined }) {
  if (!run) return <Card><CardBody>Loading…</CardBody></Card>;
  const m = (run.metrics ?? {}) as Record<string, unknown>;
  return (
    <div className="grid gap-4 md:grid-cols-2">
      <Card>
        <CardHeader title="Status" />
        <CardBody className="space-y-2 text-sm">
          <Row label="Status">
            <StatusPill
              tone={
                run.status === "success"
                  ? "positive"
                  : run.status === "failed"
                    ? "critical"
                    : "neutral"
              }
            >
              {run.status}
            </StatusPill>
          </Row>
          <Row label="Duration">
            <span className="font-mono text-xs">
              {run.duration_sec != null ? `${run.duration_sec.toFixed(2)}s` : "—"}
            </span>
          </Row>
          <Row label="LLM model">
            <span className="font-mono text-xs">{run.llm_model ?? "—"}</span>
          </Row>
        </CardBody>
      </Card>
      <Card>
        <CardHeader title="Metrics" />
        <CardBody className="space-y-2 text-sm">
          <TokensRow run={run} />
          <CostRow run={run} />
          {Object.entries(m).length === 0 ? (
            <div className="text-ink-dim">No metrics recorded.</div>
          ) : (
            Object.entries(m).slice(0, 10).map(([k, v]) => (
              <Row key={k} label={k}>
                <span className="font-mono text-xs">
                  {typeof v === "object" ? JSON.stringify(v) : String(v)}
                </span>
              </Row>
            ))
          )}
        </CardBody>
      </Card>
    </div>
  );
}

/** Aggregate token counts (input / output / total) inside the Metrics card.
 *
 * The figures come from ``tokens_json``: ``records[]`` when the run was
 * created by post-2026-05 worker code that persists per-call data;
 * ``summary`` (the legacy 4- or 5-tuple per-step list) otherwise. Old
 * runs that only have a top-level ``total_tokens`` still surface a
 * total row so the user knows the run did some work.
 */
function TokensRow({ run }: { run: RunDetailPayload }) {
  const tokens = (run.tokens_json ?? {}) as Record<string, unknown>;
  let inputTokens = 0;
  let outputTokens = 0;
  const records = (tokens.records ?? []) as Array<{
    prompt_tokens?: number;
    completion_tokens?: number;
  }>;
  if (records.length > 0) {
    for (const r of records) {
      inputTokens += Number(r.prompt_tokens ?? 0);
      outputTokens += Number(r.completion_tokens ?? 0);
    }
  } else if (Array.isArray(tokens.summary)) {
    for (const row of tokens.summary as unknown[]) {
      if (Array.isArray(row) && row.length >= 4) {
        inputTokens += Number(row[1] ?? 0);
        outputTokens += Number(row[2] ?? 0);
      }
    }
  }
  const totalFromBreakdown = inputTokens + outputTokens;
  const totalTokens =
    totalFromBreakdown > 0
      ? totalFromBreakdown
      : typeof tokens.total_tokens === "number"
        ? Number(tokens.total_tokens)
        : 0;

  if (totalTokens === 0) {
    // No token data at all — keep the card uncluttered. The Cost row
    // below renders the dash itself; no need for a duplicate empty row.
    return null;
  }

  // When we only have a top-level ``total_tokens`` (no per-call /
  // per-step breakdown), show a single Total row instead of three
  // dash-laden rows. Better signal-to-noise on legacy runs.
  if (totalFromBreakdown === 0) {
    return (
      <Row label="Tokens">
        <span className="font-mono tabular-nums text-xs">
          {totalTokens.toLocaleString()} total
          <span className="ml-2 text-[10px] uppercase tracking-wider text-ink-dim">
            (legacy run · no input/output split)
          </span>
        </span>
      </Row>
    );
  }

  return (
    <Row label="Tokens">
      <span className="inline-flex items-center gap-2 font-mono tabular-nums text-xs">
        <span title="Input (prompt) tokens billed at the input rate.">
          ↑ {inputTokens.toLocaleString()}
        </span>
        <span title="Output (completion) tokens billed at the output rate — usually the dominant cost contributor.">
          ↓ {outputTokens.toLocaleString()}
        </span>
        <span className="text-ink-dim">·</span>
        <span title="Sum of input + output.">
          {totalTokens.toLocaleString()} total
        </span>
      </span>
    </Row>
  );
}

/** Cost summary row inside the Metrics card.
 *
 * Reads ``tokens_json.total_cost_usd`` (frozen at run time) for the
 * default render. The ``Recompute`` button resolves today's price for
 * the run's (provider, model) and projects the same recorded token
 * totals against it so users can see "with current prices, this run
 * would cost Y" without re-running anything.
 */
function CostRow({ run }: { run: RunDetailPayload }) {
  const tokens = (run.tokens_json ?? {}) as Record<string, unknown>;
  const frozenCost =
    typeof tokens.total_cost_usd === "number" ? tokens.total_cost_usd : null;
  const totalTokens =
    typeof tokens.total_tokens === "number" ? tokens.total_tokens : null;
  const provider = run.llm_provider ?? "";
  const model = run.llm_model ?? "";
  const [liveCost, setLiveCost] = useState<number | null>(null);
  const [liveSource, setLiveSource] = useState<string>("");
  const [recomputing, setRecomputing] = useState(false);

  const recompute = async () => {
    if (!provider || !model || totalTokens == null) return;
    setRecomputing(true);
    try {
      const price = await api.lookupPrice(provider, model);
      // The API returns rates per 1M tokens. We split tokens evenly
      // here only as a fallback hint; a more accurate live recompute
      // would walk records[]. For this card we use the per-record
      // arrays when available.
      const records = (tokens.records ?? []) as Array<{
        prompt_tokens?: number;
        completion_tokens?: number;
      }>;
      let inputTokens = 0;
      let outputTokens = 0;
      for (const r of records) {
        inputTokens += Number(r.prompt_tokens ?? 0);
        outputTokens += Number(r.completion_tokens ?? 0);
      }
      const cost =
        (inputTokens / 1_000_000) * price.input_per_mtok +
        (outputTokens / 1_000_000) * price.output_per_mtok;
      setLiveCost(cost);
      setLiveSource(price.source);
    } finally {
      setRecomputing(false);
    }
  };

  if (frozenCost == null && liveCost == null) {
    return (
      <Row label="Cost">
        <span className="text-xs text-ink-dim">
          {totalTokens != null
            ? "no cost data — pre-cost run, recompute below"
            : "—"}
          {totalTokens != null && (
            <button
              type="button"
              onClick={recompute}
              disabled={recomputing}
              className="ml-2 rounded border border-surface-border px-2 py-0.5 text-[10px] uppercase tracking-wider text-ink-muted hover:border-accent hover:text-accent disabled:opacity-50"
            >
              {recomputing ? "Recomputing…" : "Recompute"}
            </button>
          )}
        </span>
      </Row>
    );
  }

  const display = liveCost != null ? liveCost : (frozenCost as number);
  const sourceLabel =
    liveCost != null
      ? `live · ${liveSource || "?"}`
      : "frozen at run time";

  return (
    <Row label="Cost">
      <span className="inline-flex items-center gap-2 font-mono text-xs">
        <span>${display.toFixed(4)}</span>
        <span className="text-[10px] uppercase tracking-wider text-ink-dim">
          ({sourceLabel})
        </span>
        {provider && model && (
          <button
            type="button"
            onClick={recompute}
            disabled={recomputing}
            className="rounded border border-surface-border px-2 py-0.5 text-[10px] uppercase tracking-wider text-ink-muted hover:border-accent hover:text-accent disabled:opacity-50"
          >
            {recomputing ? "…" : liveCost != null ? "Refresh" : "Recompute"}
          </button>
        )}
      </span>
    </Row>
  );
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-xs text-ink-dim">{label}</span>
      <span className="truncate">{children}</span>
    </div>
  );
}

interface PendingEntry {
  idx: number;
  schema: string;
  table: string;
  column: string | null;
  final_description: string;
  confidence: string;
  source: string;
  asset_kind: string;
  result_id: number | null;
  alternatives: unknown[];
  logprob_score: number | null;
}
interface PendingResponseLite {
  pending: PendingEntry[];
  count: number;
}

function ResultsTab({
  runId,
  loading,
  rows,
  error,
  scope,
}: {
  runId: number;
  loading: boolean;
  rows: ResultRow[];
  error?: Error;
  scope: {
    db_profile: string | null;
    database: string | null;
    catalog: string | null;
  };
}) {
  const queryClient = useQueryClient();
  const toast = useToast();
  const [activeApplyJob, setActiveApplyJob] = useState<string | null>(null);
  // Multi-select Re-Run state. ``multiSelectMode`` flips the per-row
  // checkbox on; ``selectedIds`` is the set of result_ids the user
  // picked. The bulk dialog opens with ``bulkRerunOpen``; once the
  // worker responds the SPA tails ``bulkRerunJobId`` via SSE so the
  // toast / invalidations only fire once the run is actually done.
  const [multiSelectMode, setMultiSelectMode] = useState(false);
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  const [bulkRerunOpen, setBulkRerunOpen] = useState(false);
  const [bulkRerunJobId, setBulkRerunJobId] = useState<string | null>(null);
  const toggleSelected = useCallback((id: number) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);
  // Old runs (pre-PR #224) didn't persist database/catalog onto the
  // run row, so apply used to fall back to the active profile's
  // default DB and produce ``schema does not exist`` errors when the
  // active profile pinned a different DB than the run was actually
  // scoped to. Let the user override here when the run's scope is
  // missing — defaults to the persisted value when present.
  const [overrideDatabase, setOverrideDatabase] = useState<string>(
    scope.database ?? "",
  );
  // Confirm-before-apply state. The apply path is irreversible
  // (writes COMMENT statements to the live DB), so we render a
  // dialog with the row count + DB target rather than fire on a
  // single click. Without this, a misclick on "Apply pending queue"
  // silently mutated the database — and a user reported exactly
  // this surprise after navigating away and back.
  const [confirmApplyOpen, setConfirmApplyOpen] = useState(false);
  // Catalog override is reserved for Databricks/BigQuery profiles; the
  // run-detail picker focuses on database (Postgres-shape) for now,
  // since that's where the real-world breakage occurred. Catalog stays
  // pinned to whatever the run row already has.
  const [overrideCatalog] = useState<string>(scope.catalog ?? "");
  const dbCandidates = useQuery({
    queryKey: ["live", "databases", scope.db_profile],
    queryFn: () =>
      apiFetch<{ databases: string[] }>(
        `/api/live/databases?profile=${encodeURIComponent(scope.db_profile ?? "")}`,
      ),
    enabled: !!scope.db_profile && !scope.database,
    retry: false,
  });

  const pending = useQuery({
    queryKey: ["pending"],
    queryFn: () => apiFetch<PendingResponseLite>("/api/pending"),
    retry: false,
  });

  // Map result_id → pending entry so each run-result row can find
  // its editable alternatives + the API idx for PATCH/apply calls.
  const pendingByResultId = useMemo(() => {
    const m = new Map<number, PendingEntry>();
    for (const p of pending.data?.pending ?? []) {
      if (p.result_id != null) m.set(p.result_id, p);
    }
    return m;
  }, [pending.data]);

  const queueApply = useMutation({
    mutationFn: () => {
      // Use the persisted scope when available; otherwise fall back
      // to the override picker the user filled out for old runs.
      const effDb = (scope.database || overrideDatabase || "").trim();
      const effCat = (scope.catalog || overrideCatalog || "").trim();
      return apiFetch<{ job_id: string; status: string }>(
        "/api/pending/apply",
        {
          method: "POST",
          body: JSON.stringify({
            // Pin the apply to the run's own scope. Without this the
            // worker falls back to cfg.active_db_profile + its pinned
            // database, which produces ``schema "X" does not exist``
            // errors when the active profile points elsewhere than
            // the database the run was rooted in.
            db_profile: scope.db_profile ?? undefined,
            database: effDb || undefined,
            catalog: effCat || undefined,
          }),
        },
      );
    },
    onSuccess: (result) => {
      setActiveApplyJob(result.job_id);
      toast.push({
        title: "Apply started",
        description: "Watching the live worker stream below.",
        tone: "info",
        duration: 1800,
      });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Apply failed",
        description: e.message,
        tone: "error",
      }),
  });

  const patchPending = useMutation({
    mutationFn: (vars: { idx: number; description: string }) =>
      apiFetch(`/api/pending/${vars.idx}`, {
        method: "PATCH",
        body: JSON.stringify({ final_description: vars.description }),
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["pending"] });
      toast.push({
        title: "Description updated",
        tone: "info",
        duration: 1800,
      });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Update failed",
        description: e.message,
        tone: "error",
      }),
  });

  const skipPending = useMutation({
    mutationFn: (idx: number) =>
      apiFetch(`/api/pending/${idx}`, { method: "DELETE" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["pending"] });
      toast.push({
        title: "Skipped",
        description: "Removed from the pending queue. You can restore it any time.",
        tone: "info",
        duration: 2000,
      });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Skip failed",
        description: e.message,
        tone: "error",
      }),
  });

  const restorePending = useMutation({
    mutationFn: (vars: {
      result_id: number | null;
      schema: string;
      table: string;
      column: string | null;
      final_description: string;
      confidence: string;
      source: string;
      asset_kind: string;
      alternatives: string[];
      logprob_score: number | null;
    }) => api.restorePending(vars),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["pending"] });
      toast.push({
        title: "Restored",
        description: "Back in the pending queue. Apply will write it on next run.",
        tone: "success",
        duration: 2000,
      });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Restore failed",
        description: e.message,
        tone: "error",
      }),
  });

  const grouped = useMemo(() => groupByTable(rows), [rows]);

  if (loading) {
    return <Card><CardBody>Loading results…</CardBody></Card>;
  }
  if (error) {
    return (
      <Card>
        <CardBody className="text-sm text-critical">{error.message}</CardBody>
      </Card>
    );
  }
  if (rows.length === 0) {
    return (
      <Card>
        <CardBody className="text-sm text-ink-dim">
          This run produced no per-column suggestions. (Run failed before
          generating alternatives, or the missing-only filter dropped every
          asset.)
        </CardBody>
      </Card>
    );
  }

  // Count rows still waiting in the pending queue. Includes
  // revisions queued on top of an already-applied row -- the next
  // Apply will overwrite the live comment for those, so they must
  // contribute to the CTA's count or the button stays disabled
  // ("Queue empty -- all applied") even though work is queued.
  const queuedCount = rows.reduce(
    (n, r) => n + (r.id != null && pendingByResultId.has(r.id) ? 1 : 0),
    0,
  );
  // Applied & untouched: the row has a live-DB comment AND no
  // pending revision queued. Used for the "X applied" tally next to
  // the Apply CTA so it reflects committed work, not work currently
  // being revised.
  const appliedCount = rows.filter(
    (r) => !!r.applied_at && !(r.id != null && pendingByResultId.has(r.id)),
  ).length;
  const nothingToApply = queuedCount === 0 && !activeApplyJob;
  const applyLabel = activeApplyJob
    ? "Apply running…"
    : queuedCount === 0
      ? appliedCount > 0
        ? "Queue empty — all applied"
        : "Nothing to apply"
      : `Apply pending queue (${queuedCount})`;

  // Apply needs a database (or catalog for 3-level backends). Old
  // runs that didn't persist this on the row become un-appliable
  // unless the user names the target — block the button until they do.
  const hasDatabaseScope = !!(
    (scope.database && scope.database.trim()) ||
    overrideDatabase.trim() ||
    (scope.catalog && scope.catalog.trim()) ||
    overrideCatalog.trim()
  );

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-xs text-ink-muted">
          Run #{runId} produced <span className="font-mono">{rows.length}</span>{" "}
          suggestion{rows.length === 1 ? "" : "s"} —{" "}
          <span className="text-positive">{appliedCount} applied</span>,{" "}
          <span className="text-ink">{queuedCount} queued</span>. Edit text
          inline, pick an alternative, or skip a row.
        </p>
        <Button
          variant="primary"
          size="md"
          leadingIcon={<PlayCircle size={14} />}
          loading={queueApply.isPending}
          disabled={
            !!activeApplyJob ||
            nothingToApply ||
            queueApply.isPending ||
            !hasDatabaseScope
          }
          onClick={() => setConfirmApplyOpen(true)}
          title={
            !hasDatabaseScope
              ? "Pick a target database below — this run didn't capture one."
              : undefined
          }
        >
          {applyLabel}
        </Button>
      </div>
      <AlertDialog
        open={confirmApplyOpen}
        onClose={() => setConfirmApplyOpen(false)}
        onConfirm={() => {
          setConfirmApplyOpen(false);
          queueApply.mutate();
        }}
        loading={queueApply.isPending}
        title={`Apply ${queuedCount} pending ${queuedCount === 1 ? "row" : "rows"}?`}
        description={(() => {
          const target = (scope.database || overrideDatabase || scope.catalog || "").trim();
          const profile = scope.db_profile || "";
          const where = profile && target
            ? `${profile} @ ${target}`
            : profile || target || "the active database profile";
          return `This writes COMMENT statements to ${where}. The change is permanent — restoring the previous comment requires another apply with the old text. Cancel here to keep reviewing the queue.`;
        })()}
        confirmLabel={`Apply ${queuedCount}`}
      />
      {!scope.database && !scope.catalog && scope.db_profile && (
        <Card>
          <CardBody className="space-y-2 px-4 py-3 text-xs">
            <p className="text-ink-muted">
              This run didn't capture the target database. Pick the
              database the schemas live in so apply targets it directly —
              otherwise the COMMENTs would land in the active profile's
              default and produce
              <span className="font-mono"> schema "…" does not exist</span>{" "}
              errors.
            </p>
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-ink-dim">Profile:</span>
              <span className="font-mono">{scope.db_profile}</span>
              <span className="text-ink-dim">·</span>
              <span className="text-ink-dim">Database:</span>
              <input
                type="text"
                value={overrideDatabase}
                onChange={(e) => setOverrideDatabase(e.target.value)}
                placeholder="e.g. car_retails_db"
                list={`db-suggestions-${runId}`}
                className="rounded-md border border-surface-border bg-surface px-2 py-1 font-mono text-xs"
              />
              <datalist id={`db-suggestions-${runId}`}>
                {(dbCandidates.data?.databases ?? []).map((d) => (
                  <option key={d} value={d} />
                ))}
              </datalist>
              {(dbCandidates.data?.databases ?? []).length > 0 && (
                <span className="text-[10px] text-ink-dim">
                  ({dbCandidates.data!.databases.length} reachable)
                </span>
              )}
            </div>
          </CardBody>
        </Card>
      )}

      {activeApplyJob && (
        <JobProgress
          jobId={activeApplyJob}
          kind="apply"
          onCancel={async () => {
            try {
              await apiFetch(`/api/apply/${activeApplyJob}/cancel`, { method: "POST" });
            } catch {
              /* SSE will surface job.cancelled */
            }
          }}
          onTerminal={() => {
            // Refresh three times: the worker writes both row.applied_at
            // and the pending file, but the SSE terminal event can
            // arrive a tick before the SQLite write has been observed
            // by a fresh GET. Three invalidations (now / 1.2s / 3s)
            // cover the FS sync window even on slower disks. Add
            // ``run`` to flush the run-row metrics card on top.
            const refresh = () => {
              queryClient.invalidateQueries({ queryKey: ["pending"] });
              queryClient.invalidateQueries({ queryKey: ["run", runId] });
              queryClient.invalidateQueries({ queryKey: ["run-results", runId] });
              queryClient.invalidateQueries({ queryKey: ["recent-runs"] });
              queryClient.invalidateQueries({ queryKey: ["stats"] });
              queryClient.invalidateQueries({ queryKey: ["apply-events"] });
            };
            refresh();
            window.setTimeout(refresh, 1200);
            window.setTimeout(refresh, 3000);
            toast.push({
              title: "Apply finished",
              description: "Pending queue and applied badges refreshed.",
              tone: "success",
              duration: 2400,
            });
            setActiveApplyJob(null);
          }}
        />
      )}

      <div className="flex flex-wrap items-center gap-2">
        <Button
          variant="ghost"
          size="sm"
          leadingIcon={<RefreshCw size={12} />}
          onClick={() => {
            setMultiSelectMode((v) => {
              const next = !v;
              if (!next) setSelectedIds(new Set());
              return next;
            });
          }}
        >
          {multiSelectMode ? "Cancel selection" : "Select multiple to re-run"}
        </Button>
        {multiSelectMode && selectedIds.size > 0 && (
          <>
            <Button
              variant="primary"
              size="sm"
              onClick={() => setBulkRerunOpen(true)}
              disabled={!!bulkRerunJobId}
            >
              Re-Run selected ({selectedIds.size})
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setSelectedIds(new Set())}
            >
              Clear
            </Button>
          </>
        )}
        {bulkRerunJobId && (
          <span className="inline-flex items-center gap-1 text-xs text-ink-muted">
            <Loader2 size={12} className="animate-spin" /> Bulk re-run running…
          </span>
        )}
      </div>
      <BulkRerunOrchestrator
        open={bulkRerunOpen}
        onOpenChange={setBulkRerunOpen}
        rows={rows}
        selectedIds={selectedIds}
        bulkRerunJobId={bulkRerunJobId}
        setBulkRerunJobId={setBulkRerunJobId}
        onBulkDone={() => {
          setSelectedIds(new Set());
          setMultiSelectMode(false);
          // Bulk re-run produces a new analysis_runs row; the user is
          // typically still looking at the parent run when the worker
          // wraps. Invalidate by prefix so whichever runs are open
          // pull fresh data, plus ``pending`` so the queue size
          // reflects the new rows.
          queryClient.invalidateQueries({ queryKey: ["run-results"] });
          queryClient.invalidateQueries({ queryKey: ["pending"] });
          queryClient.invalidateQueries({ queryKey: ["recent-runs"] });
        }}
      />
      {grouped.map(({ key, rows: tableRows }) => (
        <Card key={key}>
          <CardHeader
            title={<span className="font-mono text-base">{key}</span>}
            description={`${tableRows.length} suggestion${tableRows.length === 1 ? "" : "s"}`}
          />
          <CardBody className="p-0">
            <ul className="divide-y divide-border">
              {tableRows.map((r) => {
                const pendingEntry =
                  r.id != null ? pendingByResultId.get(r.id) : undefined;
                return (
                  <ResultRowItem
                    key={r.id}
                    row={r}
                    pendingEntry={pendingEntry}
                    pickAlternative={(description) => {
                      if (!pendingEntry) return;
                      patchPending.mutate({ idx: pendingEntry.idx, description });
                    }}
                    skipRow={() => {
                      if (!pendingEntry) return;
                      skipPending.mutate(pendingEntry.idx);
                    }}
                    restoreRow={(description) => {
                      const alts = normalizeAlternatives(r.alternatives_json);
                      restorePending.mutate({
                        result_id: r.id ?? null,
                        schema: r.schema_name,
                        table: r.table_name,
                        column: r.column_name,
                        final_description: description,
                        confidence: r.confidence || "medium",
                        source: r.source || "user_restore",
                        asset_kind: r.asset_kind || "table",
                        alternatives: alts,
                        logprob_score: r.logprob_score,
                      });
                    }}
                    isMutating={
                      patchPending.isPending ||
                      skipPending.isPending ||
                      restorePending.isPending
                    }
                    multiSelectMode={multiSelectMode}
                    isSelected={selectedIds.has(r.id)}
                    onToggleSelected={toggleSelected}
                  />
                );
              })}
            </ul>
          </CardBody>
        </Card>
      ))}
    </div>
  );
}

// Multi-select bulk Re-Run wrapper. Owns the modal + the SSE
// listener for the bulk job; keeps that bookkeeping out of the host
// component so ``ResultsTab`` only has to forward state, not re-run
// the lifecycle effect on every render.
function BulkRerunOrchestrator({
  open,
  onOpenChange,
  rows,
  selectedIds,
  bulkRerunJobId,
  setBulkRerunJobId,
  onBulkDone,
}: {
  open: boolean;
  onOpenChange: (v: boolean) => void;
  rows: ResultRow[];
  selectedIds: Set<number>;
  bulkRerunJobId: string | null;
  setBulkRerunJobId: (v: string | null) => void;
  onBulkDone: () => void;
}) {
  const { push: pushToast } = useToast();
  const targets = useMemo(
    () =>
      rows
        .filter((r) => selectedIds.has(r.id))
        .map((r) => ({
          resultId: r.id,
          label:
            [r.schema_name, r.table_name, r.column_name]
              .filter(Boolean)
              .join(".") || `result #${r.id}`,
        })),
    [rows, selectedIds],
  );
  const sse = useEventSource({
    path: bulkRerunJobId ? `/api/runs/${encodeURIComponent(bulkRerunJobId)}/events` : "",
    enabled: !!bulkRerunJobId,
  });
  useEffect(() => {
    if (!bulkRerunJobId) return;
    const terminal = sse.events.find(
      (e) =>
        e.type === "job.done" || e.type === "job.failed" || e.type === "job.cancelled",
    );
    if (!terminal) return;
    if (terminal.type === "job.done") {
      const summary = (terminal as unknown as { summary?: { successful?: number; total?: number; new_run_id?: number } }).summary;
      pushToast({
        tone: "success",
        title: "Bulk re-run complete",
        description: summary
          ? `${summary.successful}/${summary.total} target(s) succeeded under run #${summary.new_run_id ?? "?"}.`
          : "Re-run finished.",
      });
    } else if (terminal.type === "job.failed") {
      const errMsg = (terminal as unknown as { error?: string }).error;
      pushToast({
        tone: "error",
        title: "Bulk re-run failed",
        description: errMsg || "The worker reported an error.",
      });
    } else {
      pushToast({ tone: "warning", title: "Bulk re-run cancelled" });
    }
    setBulkRerunJobId(null);
    onBulkDone();
    // ``sse.events`` is what changes — depending on every prop here
    // would re-fire the toast on every keystroke elsewhere on the page.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sse.events, bulkRerunJobId]);

  return (
    <RerunDialog
      open={open}
      onClose={() => onOpenChange(false)}
      targets={targets}
      onSubmitted={(jobId) => {
        setBulkRerunJobId(jobId);
        onOpenChange(false);
      }}
      contextSummary={
        targets.length === 0
          ? "Select rows in the lists below before opening this dialog."
          : undefined
      }
    />
  );
}

// ``React.memo`` skips a re-render when none of the row props
// reference-changed. Without this every keystroke into another
// row's edit field, every pending mutation, and every parent state
// flip cascaded a re-render across all rows — measurable lag on
// run detail pages with 200+ rows. The default shallow comparison
// is fine because the parent already memoises ``pendingByResultId``
// and the callback closures are recreated only when the underlying
// mutation hooks change.
function ResultRowItemImpl({
  row,
  pendingEntry,
  pickAlternative,
  skipRow,
  restoreRow,
  isMutating,
  multiSelectMode = false,
  isSelected = false,
  onToggleSelected,
}: {
  row: ResultRow;
  pendingEntry?: PendingEntry;
  pickAlternative: (description: string) => void;
  skipRow: () => void;
  restoreRow: (description: string) => void;
  isMutating: boolean;
  /** When true, render a leading checkbox for bulk-rerun selection. */
  multiSelectMode?: boolean;
  /** Current checkbox state — managed by the parent's selectedIds Set. */
  isSelected?: boolean;
  /** Toggle this row's membership in the parent's selectedIds Set. */
  onToggleSelected?: (id: number) => void;
}) {
  // When the row was fetched with ``include_history=true`` and a
  // re-run produced a v2/v3+ version, surface the latest entry's
  // suggestions in place. Without this swap, the user had to
  // navigate to the new run's detail page to see the regenerated
  // alternatives — defeating the point of an inline Re-Run icon.
  // ``displayRow`` keeps the original ``id`` (+ pending bookkeeping)
  // but prefers the latest chain entry's content fields.
  const latestChainEntry = (() => {
    const chain = row.history;
    if (!Array.isArray(chain) || chain.length === 0) return null;
    // chain is ordered by rerun_seq ASC; last entry is newest.
    const candidate = chain[chain.length - 1];
    if (!candidate || candidate.id === row.id) return null;
    return candidate;
  })();
  const displayRow = latestChainEntry ?? row;
  const sourceAlts = pendingEntry
    ? normalizeAlternativeStrings(pendingEntry.alternatives)
    : normalizeAlternatives(displayRow.alternatives_json);
  const chosen =
    pendingEntry?.final_description ?? displayRow.chosen_description ?? "";
  const visible = chosen && !sourceAlts.includes(chosen)
    ? [chosen, ...sourceAlts]
    : sourceAlts;
  // ``applied`` is the live-DB state: this row's COMMENT was written
  // at some point. ``hasPendingRevision`` is the pending-file state:
  // a (possibly different) description is queued for a future write.
  // The two states co-exist when the user revises an already-applied
  // row -- the next Apply overwrites the live comment with the new
  // pick. ``isAppliedClean`` separates "applied and untouched" from
  // "applied but a revision is queued" so the badge text and the
  // alternative-button affordance can react to each.
  const applied = !!row.applied_at;
  const hasPendingRevision = !!pendingEntry;
  const isAppliedClean = applied && !hasPendingRevision;
  const queued = hasPendingRevision;
  const skipped = !applied && !hasPendingRevision;
  const editable = queued;
  // Show the latest chain entry's seq when present so the v-badge
  // tracks the freshly-rendered alternatives, not the original
  // row's stale "v1" stamp.
  const rerunSeq = (latestChainEntry?.rerun_seq ?? row.rerun_seq) ?? 0;
  const statusTone: "positive" | "neutral" | "warning" = isAppliedClean
    ? "positive"
    : queued
      ? applied
        ? "warning"
        : "neutral"
      : "warning";
  const statusLabel = isAppliedClean
    ? "applied"
    : queued
      ? applied
        ? "applied · revising"
        : "queued"
      : skipped
        ? "skipped"
        : (row.evaluation || "pending");

  // Re-Run state lives per-row. ``rerunJobId`` becomes non-null after
  // the modal submits successfully; the SSE hook below tails the job
  // until ``job.done`` arrives, then invalidates the run-results query
  // so the new alternatives appear in place.
  const queryClient = useQueryClient();
  const { push: pushToast } = useToast();
  const [rerunOpen, setRerunOpen] = useState(false);
  const [rerunJobId, setRerunJobId] = useState<string | null>(null);
  const rerunSse = useEventSource({
    path: rerunJobId ? `/api/runs/${encodeURIComponent(rerunJobId)}/events` : "",
    enabled: !!rerunJobId,
  });
  useEffect(() => {
    if (!rerunJobId) return;
    const terminal = rerunSse.events.find((e) =>
      e.type === "job.done" || e.type === "job.failed" || e.type === "job.cancelled",
    );
    if (!terminal) return;
    if (terminal.type === "job.done") {
      const summary = (terminal as unknown as { summary?: { new_run_id?: number } }).summary;
      const newRunId = summary?.new_run_id;
      pushToast({
        tone: "success",
        title: "Re-run complete",
        description: newRunId
          ? `New alternatives saved under run #${newRunId}.`
          : "New alternatives saved.",
      });
    } else if (terminal.type === "job.failed") {
      const errMsg = (terminal as unknown as { error?: string }).error;
      pushToast({
        tone: "error",
        title: "Re-run failed",
        description: errMsg || "The worker reported an error.",
      });
    } else {
      pushToast({ tone: "warning", title: "Re-run cancelled" });
    }
    // The actual cache keys are ["run-results", runId] for the
    // results table and ["pending"] for the pending-queue bookkeeping
    // — invalidating ["history", ...] was a no-op so the row never
    // refreshed in place. Use prefix-only keys so all open runs get
    // refetched (the user may be on a different run id than the one
    // that spawned the re-run).
    queryClient.invalidateQueries({ queryKey: ["run-results"] });
    queryClient.invalidateQueries({ queryKey: ["pending"] });
    queryClient.invalidateQueries({ queryKey: ["recent-runs"] });
    setRerunJobId(null);
    // ``rerunSse.events`` is the only changing dependency we care
    // about — adding the others would re-fire the toast on every
    // re-render of the row.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rerunSse.events, rerunJobId]);

  const rerunBusy = !!rerunJobId && !rerunSse.closed;
  const rerunLabel = (() => {
    const parts: string[] = [];
    if (row.schema_name) parts.push(row.schema_name);
    if (row.table_name) parts.push(row.table_name);
    if (row.column_name) parts.push(row.column_name);
    return parts.join(".") || "(unnamed)";
  })();

  // Table-level rows (column_name === null) are visually
  // distinguished from columns: a thicker accent strip on the left
  // and a "TABLE" eyebrow next to the asset name. Without this, the
  // table summary blended into the column list and the user had to
  // hunt for the *what is this table about* line — which the agent
  // generates explicitly and is usually the most useful single
  // sentence in the group.
  const isTableLevel = row.column_name == null;
  return (
    <li
      className={cn(
        "px-5 py-3",
        isTableLevel &&
          "border-l-2 border-l-accent/70 bg-accent-soft/10",
      )}
    >
      <div className="flex flex-wrap items-center gap-2 text-xs">
        {multiSelectMode && (
          <Checkbox
            checked={isSelected}
            onChange={() => onToggleSelected?.(row.id)}
            aria-label={`Select ${rerunLabel} for bulk re-run`}
            className="mr-1"
          />
        )}
        {isTableLevel && (
          <span className="rounded-sm bg-accent-soft/60 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-accent-ink">
            Table
          </span>
        )}
        <span
          className={cn(
            "font-mono",
            isTableLevel ? "text-sm font-semibold text-ink" : "text-ink",
          )}
        >
          {row.column_name ?? row.table_name}
        </span>
        <ConfidencePill value={row.confidence} score={row.logprob_score} />
        <StatusPill tone={statusTone}>{statusLabel}</StatusPill>
        <LogprobBadge score={row.logprob_score} />
        {rerunSeq > 0 && (
          <span
            title={
              displayRow.user_instructions
                ? `Re-run version ${rerunSeq}. User added: "${displayRow.user_instructions}"`
                : `Re-run version ${rerunSeq}.`
            }
          >
            <Badge tone="info">v{rerunSeq + 1}</Badge>
          </span>
        )}
        <span className="ml-auto inline-flex items-center gap-2">
          {row.source && (
            <span className="text-[10px] uppercase tracking-wider text-ink-dim">
              {row.source}
            </span>
          )}
          <IconButton
            icon={
              rerunBusy ? <Loader2 size={12} className="animate-spin" /> : <RefreshCw size={12} />
            }
            label={
              rerunBusy
                ? "Re-running…"
                : "Re-Run this item with optional new instructions"
            }
            size="sm"
            variant="ghost"
            onClick={() => setRerunOpen(true)}
            disabled={isMutating || rerunBusy}
          />
          {editable && (
            <IconButton
              icon={<SkipForward size={12} />}
              label="Skip — remove from pending queue"
              size="sm"
              variant="ghost"
              onClick={skipRow}
              disabled={isMutating}
            />
          )}
        </span>
      </div>
      <RerunDialog
        open={rerunOpen}
        onClose={() => setRerunOpen(false)}
        targets={[{ resultId: row.id, label: rerunLabel }]}
        onSubmitted={(jobId) => setRerunJobId(jobId)}
      />
      {editable && (
        <div className="mt-2 rounded-md border border-border bg-surface-subtle/30 px-2.5 py-1.5 text-xs">
          <div className="mb-0.5 text-[10px] uppercase tracking-wider text-ink-dim">
            Chosen — edit text directly
          </div>
          <InlineEditText
            value={chosen}
            onSave={(next) => pickAlternative(next)}
            multiline
            italicEmpty
            emptyLabel="(empty — click to write a custom description)"
          />
        </div>
      )}
      <ReasoningDisclosure reasoning={displayRow.reasoning ?? ""} />
      <div className="mt-2 space-y-1">
        {visible.length === 0 ? (
          <p className="text-xs text-ink-dim">{chosen || "—"}</p>
        ) : (
          visible.map((alt, idx) => {
            const isChosen = alt === chosen;
            // Editable rows pick a new chosen alternative; skipped
            // rows + already-applied rows clicking an alternative
            // restore them to pending with that alternative as the
            // chosen description. Applied + revising rows just need
            // pickAlternative -- pendingEntry is already there.
            const canPick = editable && !isChosen;
            const canRestore = (skipped || isAppliedClean) && !isChosen;
            const clickable = canPick || canRestore;
            return (
              <button
                key={`${row.id}-${idx}`}
                type="button"
                onClick={() => {
                  if (canPick) pickAlternative(alt);
                  else if (canRestore) restoreRow(alt);
                }}
                disabled={!clickable || isMutating}
                title={
                  isAppliedClean
                    ? isChosen
                      ? "Currently applied to the live database."
                      : "Click to queue this alternative as a revision -- the next Apply overwrites the live comment."
                    : applied
                      ? isChosen
                        ? "Pending revision uses this alternative -- Apply to overwrite the live comment."
                        : "Make this the queued revision (does not write to the database until Apply)."
                      : skipped
                        ? "Click to restore this row to the pending queue with this alternative chosen."
                        : isChosen
                          ? "Currently chosen alternative"
                          : "Make this the chosen alternative"
                }
                className={cn(
                  "flex w-full items-start gap-2 rounded-md border px-2.5 py-1.5 text-left text-xs transition-colors duration-fast",
                  isChosen
                    ? "border-accent/40 bg-accent-soft/40 text-ink"
                    : skipped
                      ? "border-warning/30 text-ink-muted hover:border-warning/60 hover:bg-warning-soft/30 hover:text-ink"
                      : "border-border text-ink-muted hover:border-accent/40 hover:bg-surface-subtle/50 hover:text-ink",
                  (!clickable || isMutating) && !isChosen && "cursor-default opacity-70 hover:border-border hover:bg-transparent hover:text-ink-muted",
                )}
              >
                <span
                  className={cn(
                    "mt-0.5 inline-flex h-4 w-4 shrink-0 items-center justify-center rounded text-[10px] font-semibold",
                    isChosen
                      ? "bg-accent text-white"
                      : "bg-surface-subtle text-ink-dim",
                  )}
                >
                  {isChosen ? "✓" : String.fromCharCode(65 + idx)}
                </span>
                <span className="leading-relaxed">{alt}</span>
              </button>
            );
          })
        )}
        {skipped && visible.length > 0 && (
          <p className="px-1 text-[10.5px] text-ink-dim">
            Skipped — click any alternative above to restore this row.
          </p>
        )}
        {isAppliedClean && visible.length > 0 && (
          <p className="px-1 text-[10.5px] text-ink-dim">
            Applied — pick a different alternative above to queue a revision,
            then Apply to overwrite the live comment.
          </p>
        )}
      </div>
    </li>
  );
}

// Public name kept the same so import sites and inspector traces are
// unaffected by the memo wrapping. Default shallow prop equality
// catches the wins we care about — same row id + same pendingEntry
// object reference + same callbacks → bail out of re-render.
const ResultRowItem = memo(ResultRowItemImpl);

function normalizeAlternativeStrings(raw: unknown[]): string[] {
  const out: string[] = [];
  for (const entry of raw ?? []) {
    if (typeof entry === "string") out.push(entry);
    else if (entry && typeof entry === "object") {
      const desc = (entry as { description?: unknown }).description;
      if (typeof desc === "string") out.push(desc);
    }
  }
  const seen = new Set<string>();
  return out.filter((d) => {
    if (seen.has(d)) return false;
    seen.add(d);
    return true;
  });
}

function groupByTable(rows: ResultRow[]) {
  const map = new Map<string, ResultRow[]>();
  for (const r of rows) {
    const key = `${r.schema_name}.${r.table_name}`;
    const list = map.get(key) ?? [];
    list.push(r);
    map.set(key, list);
  }
  // Within each table, surface the table-level description (the row
  // whose column_name is null) before its columns. Without this the
  // table summary buried itself under N column rows — readers scanned
  // the columns first and the *what does this table represent* line
  // appeared as an afterthought at the bottom of the group.
  for (const list of map.values()) {
    list.sort((a, b) => {
      const ax = a.column_name == null ? 0 : 1;
      const bx = b.column_name == null ? 0 : 1;
      if (ax !== bx) return ax - bx;
      return (a.column_name ?? "").localeCompare(b.column_name ?? "");
    });
  }
  return Array.from(map.entries()).map(([key, rows]) => ({ key, rows }));
}
