import { useEffect, useMemo, useState } from "react";
import { useParams, Link, useNavigate } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, PauseCircle, PlayCircle } from "lucide-react";

import { apiFetch, api } from "../lib/api";
import { useEventSource, type SseEvent } from "../lib/sse";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import StatusPill from "../components/StatusPill";
import { cn } from "../lib/cn";

interface RunDetailPayload {
  id: number;
  command: string;
  status: string;
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

  const { events, closed, error } = useEventSource({
    path: `/api/runs/${jobId}/events`,
    enabled: true,
  });

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

  const cancel = useMutation({
    mutationFn: () => api.cancelRun(jobId),
  });

  const lastEvent = events[events.length - 1] as SseEvent | undefined;
  const terminalKind = lastEvent
    ? String(lastEvent.type || "")
    : "";

  return (
    <>
      <PageHeader
        eyebrow="Run · live"
        title={resolvedRunId ? `#${resolvedRunId}` : "Starting…"}
        description="Streaming alternatives. Stays here until every selected asset is processed."
        actions={
          !closed && (
            <button
              type="button"
              onClick={() => cancel.mutate()}
              disabled={cancel.isPending}
              className="inline-flex items-center gap-1.5 rounded-md bg-critical/10 px-3 py-1.5 text-sm font-medium text-critical hover:bg-critical/20 disabled:opacity-50"
            >
              <PauseCircle size={14} />
              Cancel
            </button>
          )
        }
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
        eyebrow={`Run #${runId}`}
        title={run.data?.command ?? "Loading…"}
        description={
          run.data
            ? `${run.data.status.toUpperCase()} · ${
                run.data.duration_sec != null ? `${run.data.duration_sec.toFixed(1)}s` : "—"
              } · ${run.data.llm_model ?? "—"}`
            : ""
        }
        actions={
          <Link
            to="/runs"
            className="text-xs text-ink-dim hover:text-ink"
          >
            ← All runs
          </Link>
        }
      />
      <div className="mb-4 flex gap-1">
        {(["summary", "results", "scope", "settings"] as Tab[]).map((t) => (
          <button
            key={t}
            type="button"
            onClick={() => setTab(t)}
            className={cn(
              "rounded-md px-3 py-1.5 text-sm font-medium transition",
              tab === t
                ? "bg-accent-soft text-accent-ink"
                : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
            )}
          >
            {t[0].toUpperCase() + t.slice(1)}
            {t === "results" && results.data?.count != null && (
              <span className="ml-1.5 text-[10px] text-ink-dim">{results.data.count}</span>
            )}
          </button>
        ))}
      </div>

      {tab === "summary" && (
        <SummaryTab run={run.data} />
      )}
      {tab === "results" && (
        <ResultsTab
          runId={runId}
          loading={results.isLoading}
          rows={results.data?.results ?? []}
          error={results.error as Error | undefined}
        />
      )}
      {tab === "scope" && (
        <Card>
          <CardBody>
            <pre className="overflow-x-auto whitespace-pre-wrap rounded-md bg-surface-subtle p-3 font-mono text-xs">
              {JSON.stringify(run.data?.scope ?? {}, null, 2)}
            </pre>
          </CardBody>
        </Card>
      )}
      {tab === "settings" && (
        <Card>
          <CardHeader title="Settings snapshot" description="Captured at the moment the run started." />
          <CardBody>
            <pre className="overflow-x-auto whitespace-pre-wrap rounded-md bg-surface-subtle p-3 font-mono text-xs">
              {JSON.stringify(run.data?.settings ?? {}, null, 2)}
            </pre>
          </CardBody>
        </Card>
      )}
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
  const queueApply = useMutation({
    mutationFn: () =>
      apiFetch<{ job_id: string; status: string }>("/api/pending/apply", {
        method: "POST",
        body: JSON.stringify({}),
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["pending"] });
    },
  });
  // Group by table — easier to scan than a flat list of column rows.
  // Computed unconditionally to keep React hook order stable across
  // renders (loading / error / empty paths still call useMemo).
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
      <div className="flex items-center justify-between">
        <p className="text-xs text-ink-muted">
          Run #{runId} produced <span className="font-mono">{rows.length}</span>{" "}
          alternative slot{rows.length === 1 ? "" : "s"}. Approve below or open
          the Pending tab for bulk actions.
        </p>
        <button
          type="button"
          onClick={() => queueApply.mutate()}
          disabled={queueApply.isPending}
          className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-xs font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-50"
        >
          <PlayCircle size={12} />
          {queueApply.isPending ? "Applying…" : "Apply pending queue"}
        </button>
      </div>
      {queueApply.isError && (
        <div className="rounded-md border border-critical/30 bg-critical/5 px-3 py-2 text-xs text-critical">
          {queueApply.error instanceof Error
            ? queueApply.error.message
            : "Apply failed."}
        </div>
      )}
      {grouped.map(({ key, rows: tableRows }) => (
        <Card key={key}>
          <CardHeader
            title={<span className="font-mono text-base">{key}</span>}
            description={`${tableRows.length} suggestion${tableRows.length === 1 ? "" : "s"}`}
          />
          <CardBody className="p-0">
            <table className="w-full text-sm">
              <thead className="bg-surface-subtle/60 text-[11px] uppercase tracking-wider text-ink-dim">
                <tr>
                  <th className="px-5 py-2 text-left font-semibold">Column</th>
                  <th className="px-5 py-2 text-left font-semibold">Confidence</th>
                  <th className="px-5 py-2 text-left font-semibold">Chosen description</th>
                  <th className="px-5 py-2 text-left font-semibold">Evaluation</th>
                </tr>
              </thead>
              <tbody>
                {tableRows.map((r) => (
                  <tr key={r.id} className="border-t border-surface-border">
                    <td className="px-5 py-2 font-mono text-xs">
                      {r.column_name ?? <em className="text-ink-dim">(table)</em>}
                    </td>
                    <td className="px-5 py-2">
                      <ConfidencePill value={r.confidence} score={r.logprob_score} />
                    </td>
                    <td className="px-5 py-2 text-ink">
                      {r.chosen_description ?? (
                        <span className="text-ink-dim">
                          {firstAlternative(r.alternatives_json) ?? "—"}
                        </span>
                      )}
                    </td>
                    <td className="px-5 py-2 text-xs text-ink-muted">
                      {r.evaluation || (r.applied_at ? "applied" : "pending")}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardBody>
        </Card>
      ))}
    </div>
  );
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

function firstAlternative(value: unknown): string | null {
  if (Array.isArray(value) && value.length > 0) {
    const first = value[0];
    if (typeof first === "string") return first;
    if (first && typeof first === "object" && "description" in (first as object)) {
      const d = (first as { description?: unknown }).description;
      return typeof d === "string" ? d : null;
    }
  }
  return null;
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
