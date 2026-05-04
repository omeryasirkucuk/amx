import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Check, Inbox, Play, Trash2, X } from "lucide-react";

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
  result_id: number | null;
  alternatives: unknown[];
  logprob_score: number | null;
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

  const patchMutation = useMutation({
    mutationFn: (vars: { idx: number; final_description: string }) =>
      apiFetch(`/api/pending/${vars.idx}`, {
        method: "PATCH",
        body: JSON.stringify({ final_description: vars.final_description }),
      }),
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
        description="Approved suggestions awaiting write-back. Pick a different alternative, drop a row, or apply the whole queue."
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
          description="Click an alternative letter (A/B/C…) to swap which one becomes the chosen description. The × removes that row from the queue."
        />
        <CardBody className="p-0">
          {pending.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading…</div>
          ) : pending.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(pending.error as Error).message}
            </div>
          ) : pending.data?.pending?.length ? (
            <ul className="divide-y divide-surface-border">
              {pending.data.pending.map((row) => (
                <PendingItem
                  key={row.idx}
                  row={row}
                  onRemove={() => removeMutation.mutate(row.idx)}
                  onPick={(description) =>
                    patchMutation.mutate({
                      idx: row.idx,
                      final_description: description,
                    })
                  }
                  isPicking={patchMutation.isPending}
                />
              ))}
            </ul>
          ) : (
            <EmptyState
              icon={Inbox}
              title="Queue is empty"
              description="Approved descriptions land here when /run finishes or /apply marks rows for write-back."
            />
          )}
        </CardBody>
      </Card>
    </>
  );
}

function PendingItem({
  row,
  onRemove,
  onPick,
  isPicking,
}: {
  row: PendingRow;
  onRemove: () => void;
  onPick: (description: string) => void;
  isPicking: boolean;
}) {
  const alternatives = normalizeAlternatives(row.alternatives);
  // Always include the chosen description in the visible set, even if
  // it doesn't match any saved alternative (e.g. user typed a custom one).
  const visible = alternatives.includes(row.final_description)
    ? alternatives
    : [row.final_description, ...alternatives];

  return (
    <li className="px-5 py-3">
      <div className="flex items-start gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2 text-xs">
            <span className="font-mono">{`${row.schema}.${row.table}`}</span>
            {row.column && (
              <span className="font-mono text-ink-dim">.{row.column}</span>
            )}
            <span className="ml-2">
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
            </span>
            {row.logprob_score != null && (
              <span className="font-mono text-[10px] text-ink-dim">
                logprob {row.logprob_score.toFixed(3)}
              </span>
            )}
            <span className="ml-auto text-[10px] uppercase tracking-wider text-ink-dim">
              {row.source || row.asset_kind}
            </span>
          </div>

          <div className="mt-2 space-y-1">
            {visible.map((alt, idx) => {
              const isChosen = alt === row.final_description;
              return (
                <button
                  key={`${row.idx}-${idx}`}
                  type="button"
                  onClick={() => {
                    if (!isChosen && !isPicking) onPick(alt);
                  }}
                  disabled={isChosen || isPicking}
                  className={cn(
                    "flex w-full items-start gap-2 rounded-md border px-2.5 py-1.5 text-left text-xs transition",
                    isChosen
                      ? "border-accent/40 bg-accent-soft/30 text-ink"
                      : "border-surface-border text-ink-muted hover:border-accent/30 hover:bg-surface-subtle/40 hover:text-ink",
                    isPicking && "opacity-60",
                  )}
                  title={isChosen ? "This is the chosen alternative" : "Make this the chosen alternative"}
                >
                  <span
                    className={cn(
                      "mt-0.5 inline-flex h-4 w-4 shrink-0 items-center justify-center rounded text-[10px] font-semibold",
                      isChosen
                        ? "bg-accent text-accent-soft"
                        : "bg-surface text-ink-dim",
                    )}
                  >
                    {isChosen ? <Check size={10} /> : String.fromCharCode(65 + idx)}
                  </span>
                  <span className="leading-relaxed">{alt}</span>
                </button>
              );
            })}
            {visible.length === 1 && (
              <p className="px-1 text-[10px] text-ink-dim">
                Only one alternative was saved for this row. Run again with
                a higher <code>n_alternatives</code> setting to surface more.
              </p>
            )}
          </div>
        </div>
        <button
          type="button"
          onClick={onRemove}
          className="shrink-0 rounded-md p-1 text-ink-dim hover:bg-critical/10 hover:text-critical"
          title="Remove from queue"
        >
          <X size={14} />
        </button>
      </div>
    </li>
  );
}

function normalizeAlternatives(raw: unknown[]): string[] {
  const out: string[] = [];
  for (const entry of raw ?? []) {
    if (typeof entry === "string") {
      out.push(entry);
    } else if (entry && typeof entry === "object") {
      const desc = (entry as { description?: unknown }).description;
      if (typeof desc === "string") out.push(desc);
    }
  }
  // Deduplicate while preserving order.
  const seen = new Set<string>();
  return out.filter((d) => {
    if (seen.has(d)) return false;
    seen.add(d);
    return true;
  });
}
