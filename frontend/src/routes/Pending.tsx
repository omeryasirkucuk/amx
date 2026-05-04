import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Inbox, Play, Trash2, X } from "lucide-react";

import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";
import JobProgress from "../components/JobProgress";
import { Card, CardBody, CardHeader } from "../components/Card";
import StatusPill from "../components/StatusPill";
import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";

interface PendingRow {
  idx: number;
  schema: string;
  table: string;
  column: string | null;
  final_description: string;
  confidence: string;
  source: string;
  asset_kind: string;
}

interface PendingResponse {
  pending: PendingRow[];
  count: number;
}

export default function Pending() {
  const qc = useQueryClient();
  const [activeJob, setActiveJob] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);

  const pending = useQuery({
    queryKey: ["pending"],
    queryFn: () => apiFetch<PendingResponse>("/api/pending"),
    retry: false,
  });

  const removeMutation = useMutation({
    mutationFn: (idx: number) =>
      apiFetch(`/api/pending/${idx}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pending"] }),
  });

  const clearMutation = useMutation({
    mutationFn: () => apiFetch("/api/pending/clear", { method: "POST" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pending"] }),
  });

  async function handleApply() {
    setSubmitError(null);
    try {
      const res = await apiFetch<{ job_id: string }>("/api/pending/apply", {
        method: "POST",
        body: JSON.stringify({}),
      });
      setActiveJob(res.job_id);
    } catch (err) {
      setSubmitError(err instanceof Error ? err.message : "Apply failed.");
    }
  }

  return (
    <>
      <PageHeader
        eyebrow="Review"
        title="Pending review"
        description="Approved suggestions awaiting write-back. Edit, reject, or apply the queue."
        actions={
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => clearMutation.mutate()}
              disabled={!pending.data?.count}
              className="inline-flex items-center gap-1.5 rounded-md bg-surface-subtle px-3 py-1.5 text-sm text-ink-muted hover:bg-critical/10 hover:text-critical disabled:cursor-not-allowed disabled:opacity-50"
            >
              <Trash2 size={14} />
              Clear all
            </button>
            <button
              type="button"
              onClick={handleApply}
              disabled={!pending.data?.count || (!!activeJob && !submitError)}
              className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-40"
            >
              <Play size={14} />
              Apply ({pending.data?.count ?? 0})
            </button>
          </div>
        }
      />

      {activeJob && (
        <div className="mb-6">
          <JobProgress
            jobId={activeJob}
            kind="apply"
            onCancel={async () => {
              try {
                await apiFetch(`/api/apply/${activeJob}/cancel`, { method: "POST" });
              } catch {
                /* SSE will surface job.cancelled */
              }
            }}
            onTerminal={() => {
              qc.invalidateQueries({ queryKey: ["pending"] });
            }}
          />
        </div>
      )}

      {submitError && (
        <div className="mb-4 rounded-md border border-critical/40 bg-critical/5 p-3 text-sm text-critical">
          {submitError}
        </div>
      )}

      <Card>
        <CardHeader
          title={`${pending.data?.count ?? 0} entr${pending.data?.count === 1 ? "y" : "ies"}`}
          description="Edit a row inline, drop it from the queue with the × button, or apply the whole queue at once."
        />
        <CardBody className="p-0">
          {pending.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading…</div>
          ) : pending.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(pending.error as Error).message}
            </div>
          ) : pending.data?.pending?.length ? (
            <table className="w-full text-sm">
              <thead className="bg-surface-subtle/60 text-[11px] uppercase tracking-wider text-ink-dim">
                <tr>
                  <th className="px-5 py-2 text-left font-semibold">Asset</th>
                  <th className="px-5 py-2 text-left font-semibold">Description</th>
                  <th className="px-5 py-2 text-left font-semibold">Confidence</th>
                  <th className="px-5 py-2 text-left font-semibold">Source</th>
                  <th className="w-10 px-5 py-2"></th>
                </tr>
              </thead>
              <tbody>
                {pending.data.pending.map((row) => (
                  <tr key={row.idx} className={cn(row.idx % 2 ? "bg-surface-subtle/30" : "")}>
                    <td className="px-5 py-2 font-mono text-xs">
                      <div>{`${row.schema}.${row.table}`}</div>
                      {row.column && <div className="text-ink-dim">{row.column}</div>}
                    </td>
                    <td className="max-w-md px-5 py-2 text-ink-muted">
                      {row.final_description}
                    </td>
                    <td className="px-5 py-2">
                      <StatusPill
                        tone={
                          row.confidence === "high"
                            ? "positive"
                            : row.confidence === "low"
                              ? "warning"
                              : "accent"
                        }
                      >
                        {row.confidence}
                      </StatusPill>
                    </td>
                    <td className="px-5 py-2 text-xs text-ink-dim">{row.source}</td>
                    <td className="px-5 py-2 text-right">
                      <button
                        type="button"
                        onClick={() => removeMutation.mutate(row.idx)}
                        className="rounded-md p-1 text-ink-dim hover:bg-critical/10 hover:text-critical"
                        title="Remove from queue"
                      >
                        <X size={14} />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <EmptyState
              icon={Inbox}
              title="Queue is empty"
              description="Approved descriptions land here when /run-apply or /apply marks rows for write-back."
            />
          )}
        </CardBody>
      </Card>
    </>
  );
}
