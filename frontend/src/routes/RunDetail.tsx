import { memo, useEffect, useMemo, useState } from "react";
import { useParams, Link, useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Activity as ActivityIcon, Loader2, PauseCircle, PlayCircle, SkipForward, Timer } from "lucide-react";

import { apiFetch, api } from "../lib/api";
import { useEventSource, type SseEvent } from "../lib/sse";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import JobProgress from "../components/JobProgress";
import StatusPill from "../components/StatusPill";
import { cn } from "../lib/cn";
import {
  AlertDialog,
  Badge,
  Button,
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
  const [startTime] = useState(() => Date.now());
  const [now, setNow] = useState(() => Date.now());

  const { events, closed, error } = useEventSource({
    path: `/api/runs/${jobId}/events`,
    enabled: true,
  });

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

  useEffect(() => {
    for (const event of events) {
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

  return (
    <>
      <PageHeader
        title={resolvedRunId ? `Run #${resolvedRunId}` : "Run · starting…"}
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
              {currentActivity ? currentActivity.label : "Waiting for the worker…"}
            </span>
          </span>
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
              Waiting for the worker to begin…
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

function PersistedRunActivityCard({ jobId }: { jobId: string }) {
  // Compact live-progress panel rendered inside PersistedRunView when
  // the run still has a worker thread. Subscribes to the same SSE
  // stream LiveRunStream uses; reusing that whole component would
  // also pull in the page header rewrite + cancel dialog which a
  // user landing on a numeric run id doesn't need.
  const [activities, setActivities] = useState<ActivityRow[]>([]);
  const { events, closed, error } = useEventSource({
    path: `/api/runs/${jobId}/events`,
    enabled: true,
  });

  useEffect(() => {
    for (const event of events) {
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
      } else if (t === "activity.fail") {
        setActivities((curr) =>
          curr.map((a) =>
            a.idx === Number(event.idx)
              ? { ...a, status: "failed", detail: String(event.detail ?? "") }
              : a,
          ),
        );
      }
    }
  }, [events]);

  const completed = activities.filter((a) => a.status !== "running").length;
  const total = activities.length;
  const current = activities.find((a) => a.status === "running");

  return (
    <Card className="mb-4 border-accent/40">
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Loader2 size={14} className="animate-spin text-accent" />
            Live progress
            {total > 0 && (
              <span className="rounded bg-surface-subtle px-1.5 py-0.5 font-mono text-[10px] text-ink-muted">
                {completed}/{total}
              </span>
            )}
          </span>
        }
        description={
          closed
            ? "Worker exited. The status badge above will refresh automatically."
            : current
              ? `Now: ${current.label}`
              : "Waiting for the worker to begin…"
        }
      />
      <CardBody className="p-0">
        {activities.length === 0 ? (
          <div className="px-5 py-4 text-sm text-ink-dim">
            <Loader2 size={14} className="mr-2 inline animate-spin" />
            Waiting for the worker to begin…
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
    queryFn: () => apiFetch<ResultsResponse>(`/api/history/runs/${runId}/results`),
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
            <span className="inline-flex items-center gap-2 text-xs">
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
            </span>
          ) : undefined
        }
        actions={
          <Link to="/runs" className="text-xs text-ink-dim hover:text-ink">
            ← All runs
          </Link>
        }
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
    mutationFn: () =>
      apiFetch<{ job_id: string; status: string }>("/api/pending/apply", {
        method: "POST",
        body: JSON.stringify({
          // Pin the apply to the run's own scope. Without this the
          // worker falls back to cfg.active_db_profile + its pinned
          // database, which produces ``schema "X" does not exist``
          // errors when the active profile points elsewhere than
          // the database the run was rooted in.
          db_profile: scope.db_profile ?? undefined,
          database: scope.database ?? undefined,
          catalog: scope.catalog ?? undefined,
        }),
      }),
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

  // Count rows still waiting in the pending queue — not in the
  // queue and not already applied = the user skipped them earlier.
  const queuedCount = rows.reduce(
    (n, r) =>
      n + (r.id != null && pendingByResultId.has(r.id) && !r.applied_at ? 1 : 0),
    0,
  );
  const appliedCount = rows.filter((r) => !!r.applied_at).length;
  const nothingToApply = queuedCount === 0 && !activeApplyJob;
  const applyLabel = activeApplyJob
    ? "Apply running…"
    : queuedCount === 0
      ? appliedCount > 0
        ? "Queue empty — all applied"
        : "Nothing to apply"
      : `Apply pending queue (${queuedCount})`;

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
          disabled={!!activeApplyJob || nothingToApply || queueApply.isPending}
          onClick={() => queueApply.mutate()}
        >
          {applyLabel}
        </Button>
      </div>

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
}: {
  row: ResultRow;
  pendingEntry?: PendingEntry;
  pickAlternative: (description: string) => void;
  skipRow: () => void;
  restoreRow: (description: string) => void;
  isMutating: boolean;
}) {
  const sourceAlts = pendingEntry
    ? normalizeAlternativeStrings(pendingEntry.alternatives)
    : normalizeAlternatives(row.alternatives_json);
  const chosen = pendingEntry?.final_description ?? row.chosen_description ?? "";
  const visible = chosen && !sourceAlts.includes(chosen)
    ? [chosen, ...sourceAlts]
    : sourceAlts;
  const applied = !!row.applied_at;
  const queued = !applied && !!pendingEntry;
  const skipped = !applied && !pendingEntry;
  const editable = queued;
  const statusTone: "positive" | "neutral" | "warning" = applied
    ? "positive"
    : queued
      ? "neutral"
      : "warning";
  const statusLabel = applied
    ? "applied"
    : queued
      ? "queued"
      : skipped
        ? "skipped"
        : (row.evaluation || "pending");

  return (
    <li className="px-5 py-3">
      <div className="flex flex-wrap items-center gap-2 text-xs">
        <span className="font-mono text-ink">
          {row.column_name ?? <em className="text-ink-dim">(table)</em>}
        </span>
        <ConfidencePill value={row.confidence} score={row.logprob_score} />
        <StatusPill tone={statusTone}>{statusLabel}</StatusPill>
        <LogprobBadge score={row.logprob_score} />
        {editable && (
          <span className="ml-auto inline-flex items-center gap-2">
            {row.source && (
              <span className="text-[10px] uppercase tracking-wider text-ink-dim">
                {row.source}
              </span>
            )}
            <IconButton
              icon={<SkipForward size={12} />}
              label="Skip — remove from pending queue"
              size="sm"
              variant="ghost"
              onClick={skipRow}
              disabled={isMutating}
            />
          </span>
        )}
        {!editable && row.source && (
          <span className="ml-auto text-[10px] uppercase tracking-wider text-ink-dim">
            {row.source}
          </span>
        )}
      </div>
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
      <div className="mt-2 space-y-1">
        {visible.length === 0 ? (
          <p className="text-xs text-ink-dim">{chosen || "—"}</p>
        ) : (
          visible.map((alt, idx) => {
            const isChosen = alt === chosen;
            // Editable rows pick a new chosen alternative; skipped
            // rows clicking an alternative restore them to pending
            // with that alternative as the chosen description.
            const canPick = editable && !isChosen;
            const canRestore = skipped;
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
                  applied
                    ? "Already applied — re-run to change."
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
      </div>
    </li>
  );
}

// Public name kept the same so import sites and inspector traces are
// unaffected by the memo wrapping. Default shallow prop equality
// catches the wins we care about — same row id + same pendingEntry
// object reference + same callbacks → bail out of re-render.
const ResultRowItem = memo(ResultRowItemImpl);

function LogprobBadge({ score }: { score: number | null }) {
  if (score == null) {
    return (
      <span
        className="font-mono text-[10px] text-ink-dim"
        title="No logprob recorded for this suggestion."
      >
        logprob —
      </span>
    );
  }
  return (
    <span
      className="font-mono text-[10px] text-ink-muted"
      title="Average log-probability — closer to 0 = more confident; very negative = the model was guessing."
    >
      logprob {score.toFixed(3)}
    </span>
  );
}

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

function ConfidencePill({
  value,
  score,
}: {
  value: string;
  score: number | null;
}) {
  const tone =
    value === "high" ? "positive" : value === "low" ? "warning" : "neutral";
  return (
    <span title={score != null ? `logprob ${score.toFixed(3)}` : undefined}>
      <StatusPill tone={tone}>{value}</StatusPill>
    </span>
  );
}

function groupByTable(rows: ResultRow[]) {
  const map = new Map<string, ResultRow[]>();
  for (const r of rows) {
    const key = `${r.schema_name}.${r.table_name}`;
    const list = map.get(key) ?? [];
    list.push(r);
    map.set(key, list);
  }
  return Array.from(map.entries()).map(([key, rows]) => ({ key, rows }));
}
