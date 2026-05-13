/**
 * Schedules — list + create + lifecycle controls for one-shot scheduled
 * metadata runs.
 *
 * This is the Phase 5b first cut: read-only list with status chips plus
 * inline pause / resume / run-now / delete actions, and a slim "New
 * schedule" form that mirrors the CLI ``amx schedule add`` surface.
 *
 * The wizard / datetime-picker / scope-builder polish lands in a
 * follow-up; here we keep the page small and rely on raw datetime-local
 * + IANA tz strings so the surface is shippable and exercises every
 * Phase 5a endpoint.
 */

import { useState } from "react";
import {
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";

import { api, type ScheduleCreatePayload, type ScheduleRow } from "../lib/api";

const STATUS_TONE: Record<string, string> = {
  pending: "bg-slate-100 text-slate-700",
  paused: "bg-amber-100 text-amber-800",
  running: "bg-blue-100 text-blue-800",
  completed: "bg-emerald-100 text-emerald-800",
  failed: "bg-red-100 text-red-800",
  missed: "bg-orange-100 text-orange-800",
  cancelled: "bg-slate-200 text-slate-600",
};

function StatusChip({ status }: { status: string }) {
  const tone = STATUS_TONE[status] ?? "bg-slate-100 text-slate-700";
  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${tone}`}
    >
      {status}
    </span>
  );
}

function defaultLocalDatetime() {
  const t = new Date(Date.now() + 60 * 60 * 1000);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${t.getFullYear()}-${pad(t.getMonth() + 1)}-${pad(t.getDate())}T${pad(t.getHours())}:${pad(t.getMinutes())}`;
}

function browserTz() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    return "UTC";
  }
}

function NewScheduleForm({ onCreated }: { onCreated: () => void }) {
  const [name, setName] = useState("");
  const [fireAtLocal, setFireAtLocal] = useState(defaultLocalDatetime());
  const [fireAtTz, setFireAtTz] = useState(browserTz());
  const [dbProfile, setDbProfile] = useState("");
  const [scopeText, setScopeText] = useState("schema:public");
  const [llmProfile, setLlmProfile] = useState("");
  const [reviewStrategy, setReviewStrategy] = useState<"auto" | "manual">(
    "auto",
  );
  const [error, setError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: (body: ScheduleCreatePayload) => api.createSchedule(body),
    onSuccess: () => {
      setName("");
      setError(null);
      onCreated();
    },
    onError: (err) => {
      setError(err instanceof Error ? err.message : String(err));
    },
  });

  function parseScope(raw: string): Record<string, unknown> {
    const trimmed = raw.trim();
    if (trimmed === "all") return { mode: "all" };
    if (trimmed.startsWith("schema:")) {
      return {
        mode: "schemas",
        schemas: trimmed
          .slice("schema:".length)
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
      };
    }
    if (trimmed.startsWith("table:")) {
      const tables = trimmed
        .slice("table:".length)
        .split(",")
        .map((piece) => piece.trim())
        .filter(Boolean)
        .map((piece) => {
          const [schema, table] = piece.split(".");
          return { schema, table };
        });
      return { mode: "tables", tables };
    }
    throw new Error("scope must be 'schema:NAME,...' / 'table:S.T,...' / 'all'");
  }

  function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    try {
      mutation.mutate({
        name: name.trim(),
        fire_at_local: fireAtLocal,
        fire_at_tz: fireAtTz,
        db_profile: dbProfile.trim(),
        scope: parseScope(scopeText),
        llm_profile: llmProfile.trim(),
        review_strategy: reviewStrategy,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <form
      onSubmit={onSubmit}
      className="mb-6 grid grid-cols-1 gap-3 rounded-md border border-slate-200 bg-white p-4 shadow-sm md:grid-cols-2"
    >
      <h2 className="md:col-span-2 text-sm font-semibold text-slate-800">
        New schedule
      </h2>
      <label className="flex flex-col gap-1 text-sm">
        Name
        <input
          required
          className="rounded border border-slate-300 px-2 py-1 text-sm"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
      </label>
      <label className="flex flex-col gap-1 text-sm">
        DB profile
        <input
          required
          className="rounded border border-slate-300 px-2 py-1 text-sm"
          value={dbProfile}
          onChange={(e) => setDbProfile(e.target.value)}
        />
      </label>
      <label className="flex flex-col gap-1 text-sm">
        Fire at (local)
        <input
          required
          type="datetime-local"
          className="rounded border border-slate-300 px-2 py-1 text-sm"
          value={fireAtLocal}
          onChange={(e) => setFireAtLocal(e.target.value)}
        />
      </label>
      <label className="flex flex-col gap-1 text-sm">
        Timezone (IANA)
        <input
          required
          className="rounded border border-slate-300 px-2 py-1 text-sm"
          value={fireAtTz}
          onChange={(e) => setFireAtTz(e.target.value)}
          placeholder="Europe/Istanbul"
        />
      </label>
      <label className="flex flex-col gap-1 text-sm">
        Scope
        <input
          required
          className="rounded border border-slate-300 px-2 py-1 text-sm"
          value={scopeText}
          onChange={(e) => setScopeText(e.target.value)}
          placeholder="schema:public,staging | table:s.t1,s.t2 | all"
        />
      </label>
      <label className="flex flex-col gap-1 text-sm">
        LLM profile
        <input
          required
          className="rounded border border-slate-300 px-2 py-1 text-sm"
          value={llmProfile}
          onChange={(e) => setLlmProfile(e.target.value)}
        />
      </label>
      <label className="flex flex-col gap-1 text-sm">
        Review strategy
        <select
          className="rounded border border-slate-300 px-2 py-1 text-sm"
          value={reviewStrategy}
          onChange={(e) =>
            setReviewStrategy(e.target.value as "auto" | "manual")
          }
        >
          <option value="auto">auto</option>
          <option value="manual">manual</option>
        </select>
      </label>
      {error && (
        <p className="md:col-span-2 text-sm text-red-700">{error}</p>
      )}
      <div className="md:col-span-2 flex items-center justify-end gap-2">
        <button
          type="submit"
          disabled={mutation.isPending}
          className="rounded bg-slate-900 px-3 py-1 text-sm font-medium text-white hover:bg-slate-800 disabled:opacity-50"
        >
          {mutation.isPending ? "Creating…" : "Create schedule"}
        </button>
      </div>
      <p className="md:col-span-2 text-xs text-slate-600">
        Heads-up: AMX is invocation-based — it isn't always running. For
        scheduled runs to fire on time, keep AMX/Studio open OR enable the
        background daemon:{" "}
        <code className="font-mono text-slate-800">
          amx scheduler install-daemon
        </code>
        .
      </p>
    </form>
  );
}

function ScheduleRowActions({
  row,
  onChange,
}: {
  row: ScheduleRow;
  onChange: () => void;
}) {
  const qc = useQueryClient();
  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["schedules"] });
    onChange();
  };

  const pause = useMutation({
    mutationFn: () => api.pauseSchedule(row.id),
    onSuccess: invalidate,
  });
  const resume = useMutation({
    mutationFn: () => api.resumeSchedule(row.id),
    onSuccess: invalidate,
  });
  const runNow = useMutation({
    mutationFn: () => api.runScheduleNow(row.id),
    onSuccess: invalidate,
  });
  const del = useMutation({
    mutationFn: () => api.deleteSchedule(row.id),
    onSuccess: invalidate,
  });

  return (
    <div className="flex gap-2 text-xs">
      {row.status === "pending" && (
        <button
          className="rounded border border-slate-300 px-2 py-0.5 hover:bg-slate-100"
          onClick={() => pause.mutate()}
        >
          Pause
        </button>
      )}
      {row.status === "paused" && (
        <button
          className="rounded border border-slate-300 px-2 py-0.5 hover:bg-slate-100"
          onClick={() => resume.mutate()}
        >
          Resume
        </button>
      )}
      {(row.status === "pending" ||
        row.status === "paused" ||
        row.status === "missed") && (
        <button
          className="rounded border border-emerald-600 px-2 py-0.5 text-emerald-700 hover:bg-emerald-50"
          onClick={() => runNow.mutate()}
        >
          Run now
        </button>
      )}
      <button
        className="rounded border border-red-600 px-2 py-0.5 text-red-700 hover:bg-red-50"
        onClick={() => {
          if (window.confirm(`Delete schedule "${row.name}"?`)) {
            del.mutate();
          }
        }}
      >
        Delete
      </button>
    </div>
  );
}

export default function Schedules() {
  const [statusFilter, setStatusFilter] = useState<string>("active");
  const qc = useQueryClient();

  const apiStatus =
    statusFilter === "active"
      ? "pending,paused,missed,running"
      : statusFilter === "past"
        ? "completed,failed,cancelled"
        : undefined;

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["schedules", statusFilter],
    queryFn: () => api.listSchedules({ status: apiStatus }),
  });

  const statusQ = useQuery({
    queryKey: ["scheduler-status"],
    queryFn: () => api.schedulerStatus(),
  });
  const bootstrap = useQuery({
    queryKey: ["scheduler-bootstrap"],
    queryFn: () => api.schedulerBootstrapReport(),
  });

  const rows = data?.schedules ?? [];

  return (
    <div className="px-6 py-4">
      <header className="mb-4 flex items-end justify-between">
        <div>
          <h1 className="text-xl font-semibold text-slate-900">Schedules</h1>
          <p className="text-sm text-slate-600">
            One-shot scheduled metadata runs. Catch-up surfaces on next open
            when AMX was closed at fire time.
          </p>
        </div>
        <div className="text-xs text-slate-600">
          {statusQ.data && (
            <>
              Daemon:{" "}
              <span
                className={
                  statusQ.data.daemon.installed
                    ? "text-emerald-700"
                    : "text-amber-700"
                }
              >
                {statusQ.data.daemon.installed ? "installed" : "not installed"}
              </span>
              {" · "}Pending: {statusQ.data.pending_count}
              {" · "}Missed: {statusQ.data.missed_count}
            </>
          )}
        </div>
      </header>

      {bootstrap.data &&
        (bootstrap.data.missed_for_review.length > 0 ||
          bootstrap.data.stale_recovered.length > 0) && (
          <div className="mb-4 rounded-md border border-amber-300 bg-amber-50 px-4 py-2 text-sm text-amber-900">
            ⚠️{" "}
            {bootstrap.data.stale_recovered.length > 0 && (
              <span>
                {bootstrap.data.stale_recovered.length} interrupted run(s)
                recovered.{" "}
              </span>
            )}
            {bootstrap.data.missed_for_review.length > 0 && (
              <span>
                {bootstrap.data.missed_for_review.length} schedule(s) missed
                while AMX was closed.
              </span>
            )}
          </div>
        )}

      <NewScheduleForm onCreated={() => qc.invalidateQueries({ queryKey: ["schedules"] })} />

      <div className="mb-3 flex items-center gap-3 text-sm">
        <span className="text-slate-700">Filter:</span>
        <select
          className="rounded border border-slate-300 px-2 py-1 text-sm"
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
        >
          <option value="active">Active</option>
          <option value="past">Past</option>
          <option value="all">All</option>
        </select>
        <button
          className="ml-auto rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-100"
          onClick={() => refetch()}
        >
          Refresh
        </button>
      </div>

      {isLoading && <p className="text-sm text-slate-600">Loading…</p>}
      {error && (
        <p className="text-sm text-red-700">
          {error instanceof Error ? error.message : String(error)}
        </p>
      )}
      {!isLoading && rows.length === 0 && (
        <p className="text-sm text-slate-600">No schedules.</p>
      )}

      {rows.length > 0 && (
        <div className="overflow-x-auto rounded-md border border-slate-200 bg-white">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-xs uppercase text-slate-500">
              <tr>
                <th className="px-3 py-2">#</th>
                <th className="px-3 py-2">Name</th>
                <th className="px-3 py-2">When (local)</th>
                <th className="px-3 py-2">Tz</th>
                <th className="px-3 py-2">Status</th>
                <th className="px-3 py-2">DB</th>
                <th className="px-3 py-2">LLM</th>
                <th className="px-3 py-2 text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {rows.map((row) => (
                <tr key={row.id} className="hover:bg-slate-50">
                  <td className="px-3 py-2 text-slate-500">{row.id}</td>
                  <td className="px-3 py-2 font-medium text-slate-900">
                    {row.name}
                  </td>
                  <td className="px-3 py-2 text-slate-700">
                    {row.fire_at_local}
                  </td>
                  <td className="px-3 py-2 text-slate-700">{row.fire_at_tz}</td>
                  <td className="px-3 py-2">
                    <StatusChip status={row.status} />
                  </td>
                  <td className="px-3 py-2 text-slate-700">{row.db_profile}</td>
                  <td className="px-3 py-2 text-slate-700">{row.llm_profile}</td>
                  <td className="px-3 py-2 text-right">
                    <ScheduleRowActions row={row} onChange={() => refetch()} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
