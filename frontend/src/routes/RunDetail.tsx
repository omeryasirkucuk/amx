import { useEffect, useMemo, useState } from "react";
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
  const run = useQuery({
    queryKey: ["run", runId],
    queryFn: () => apiFetch<RunDetailPayload>(`/api/history/runs/${runId}`),
  });
  const results = useQuery({
    queryKey: ["run-results", runId],
    queryFn: () => apiFetch<ResultsResponse>(`/api/history/runs/${runId}/results`),
    enabled: tab === "results",
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
}: {
  runId: number;
  loading: boolean;
  rows: ResultRow[];
  error?: Error;
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
        body: JSON.stringify({}),
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
        description: "Removed from the pending queue.",
        tone: "info",
        duration: 1800,
      });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Skip failed",
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

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-xs text-ink-muted">
          Run #{runId} produced <span className="font-mono">{rows.length}</span>{" "}
          suggestion{rows.length === 1 ? "" : "s"}. Edit text inline, pick an
          alternative, skip a row — your choices are persisted to the pending
          queue and become the values Apply writes to the live DB.
        </p>
        <Button
          variant="primary"
          size="md"
          leadingIcon={<PlayCircle size={14} />}
          loading={queueApply.isPending}
          disabled={!!activeApplyJob}
          onClick={() => queueApply.mutate()}
        >
          {activeApplyJob ? "Apply running…" : "Apply pending queue"}
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
            queryClient.invalidateQueries({ queryKey: ["pending"] });
            queryClient.invalidateQueries({ queryKey: ["run-results", runId] });
            queryClient.invalidateQueries({ queryKey: ["recent-runs"] });
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
                    isMutating={patchPending.isPending || skipPending.isPending}
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

function ResultRowItem({
  row,
  pendingEntry,
  pickAlternative,
  skipRow,
  isMutating,
}: {
  row: ResultRow;
  pendingEntry?: PendingEntry;
  pickAlternative: (description: string) => void;
  skipRow: () => void;
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
  const editable = !!pendingEntry && !applied;

  return (
    <li className="px-5 py-3">
      <div className="flex flex-wrap items-center gap-2 text-xs">
        <span className="font-mono text-ink">
          {row.column_name ?? <em className="text-ink-dim">(table)</em>}
        </span>
        <ConfidencePill value={row.confidence} score={row.logprob_score} />
        <StatusPill tone={applied ? "positive" : "neutral"}>
          {applied ? "applied" : (row.evaluation || "pending")}
        </StatusPill>
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
            const canPick = editable && !isChosen;
            return (
              <button
                key={`${row.id}-${idx}`}
                type="button"
                onClick={() => canPick && pickAlternative(alt)}
                disabled={!canPick || isMutating}
                title={
                  applied
                    ? "Already applied — re-run to change."
                    : !pendingEntry
                      ? "This row isn't in the pending queue (re-run or open it for editing)."
                      : isChosen
                        ? "Currently chosen alternative"
                        : "Make this the chosen alternative"
                }
                className={cn(
                  "flex w-full items-start gap-2 rounded-md border px-2.5 py-1.5 text-left text-xs transition-colors duration-fast",
                  isChosen
                    ? "border-accent/40 bg-accent-soft/40 text-ink"
                    : "border-border text-ink-muted hover:border-accent/40 hover:bg-surface-subtle/50 hover:text-ink",
                  (!canPick || isMutating) && !isChosen && "cursor-default opacity-70 hover:border-border hover:bg-transparent hover:text-ink-muted",
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
      </div>
    </li>
  );
}

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
