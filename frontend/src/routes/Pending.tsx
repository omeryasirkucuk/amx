import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Check, Inbox, Play, Search, Trash2, X } from "lucide-react";

import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";
import JobProgress from "../components/JobProgress";
import { Card, CardBody } from "../components/Card";
import StatusPill from "../components/StatusPill";
import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";
import {
  AlertDialog,
  Button,
  IconButton,
  Skeleton,
  Tooltip,
  useToast,
} from "../components/ui";

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
  const toast = useToast();
  const [activeJob, setActiveJob] = useState<string | null>(null);
  const [confirmClear, setConfirmClear] = useState(false);
  const [query, setQuery] = useState("");

  const pending = useQuery({
    queryKey: ["pending"],
    queryFn: () => apiFetch<PendingResponse>("/api/pending"),
    retry: false,
  });

  const removeMutation = useMutation({
    mutationFn: (idx: number) =>
      apiFetch(`/api/pending/${idx}`, { method: "DELETE" }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["pending"] });
      toast.push({ title: "Row removed", tone: "info", duration: 2200 });
    },
  });

  const clearMutation = useMutation({
    mutationFn: () => apiFetch("/api/pending/clear", { method: "POST" }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["pending"] });
      toast.push({
        title: "Pending queue cleared",
        tone: "success",
        duration: 2500,
      });
      setConfirmClear(false);
    },
    onError: (err: Error) => {
      toast.push({
        title: "Could not clear queue",
        description: err.message,
        tone: "error",
      });
    },
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
    try {
      const res = await apiFetch<{ job_id: string }>("/api/pending/apply", {
        method: "POST",
        body: JSON.stringify({}),
      });
      setActiveJob(res.job_id);
      toast.push({
        title: "Apply started",
        description: `${pending.data?.count ?? 0} ${pending.data?.count === 1 ? "row" : "rows"} streaming…`,
        tone: "info",
        duration: 2200,
      });
    } catch (err) {
      toast.push({
        title: "Apply failed",
        description: err instanceof Error ? err.message : "Apply failed.",
        tone: "error",
      });
    }
  }

  const filtered = useMemo(() => {
    const list = pending.data?.pending ?? [];
    const q = query.trim().toLowerCase();
    if (!q) return list;
    return list.filter((row) => {
      const haystack = [
        row.schema,
        row.table,
        row.column ?? "",
        row.final_description,
        row.confidence,
        row.asset_kind,
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(q);
    });
  }, [pending.data?.pending, query]);

  const total = pending.data?.count ?? 0;
  const visibleCount = filtered.length;

  return (
    <>
      <PageHeader
        title="Pending review"
        breadcrumbs={[{ label: "Pending" }]}
        actions={
          <div className="flex items-center gap-2">
            <Button
              variant="subtle"
              size="md"
              leadingIcon={<Trash2 size={14} />}
              disabled={!total}
              onClick={() => setConfirmClear(true)}
            >
              Clear all
            </Button>
            <Button
              variant="primary"
              size="md"
              leadingIcon={<Play size={14} />}
              disabled={!total || (!!activeJob)}
              onClick={handleApply}
            >
              Apply ({total})
            </Button>
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

      <Card>
        <header className="flex items-center justify-between gap-3 border-b border-border px-5 py-3">
          <div className="text-sm text-ink-muted">
            <span className="font-medium text-ink">
              {pending.isLoading ? "—" : visibleCount}
            </span>{" "}
            {visibleCount === 1 ? "entry" : "entries"}
            {query && total !== visibleCount && (
              <span className="ml-2 text-xs text-ink-dim">
                (of {total} total)
              </span>
            )}
          </div>
          <div className="relative flex h-8 min-w-[16rem] items-center rounded-md border border-border bg-surface-raised pl-2.5 pr-2 focus-within:border-accent focus-within:ring-2 focus-within:ring-accent/20">
            <Search size={13} className="text-ink-dim" />
            <input
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search schema, table, or text…"
              className="ml-1.5 h-full flex-1 bg-transparent text-sm text-ink placeholder:text-ink-dim focus:outline-none"
            />
            {query && (
              <button
                type="button"
                onClick={() => setQuery("")}
                aria-label="Clear search"
                className="ml-1 rounded p-0.5 text-ink-dim hover:bg-surface-subtle hover:text-ink"
              >
                <X size={12} />
              </button>
            )}
          </div>
        </header>
        <CardBody className="p-0">
          {pending.isLoading ? (
            <ul className="divide-y divide-border">
              {Array.from({ length: 4 }).map((_, i) => (
                <li key={i} className="px-5 py-3">
                  <Skeleton className="h-3 w-1/3" />
                  <Skeleton className="mt-2 h-3 w-full" />
                  <Skeleton className="mt-1 h-3 w-4/5" />
                </li>
              ))}
            </ul>
          ) : pending.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(pending.error as Error).message}
            </div>
          ) : filtered.length ? (
            <ul className="divide-y divide-border">
              {filtered.map((row) => (
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
          ) : query ? (
            <div className="px-5 py-6 text-sm text-ink-dim">
              No rows match <span className="font-mono">"{query}"</span>.
            </div>
          ) : (
            <div className="px-5 py-5">
              <EmptyState
                icon={Inbox}
                title="Queue is empty"
                description="Approved descriptions land here when /run finishes or /apply marks rows for write-back."
                compact
              />
            </div>
          )}
        </CardBody>
      </Card>

      <AlertDialog
        open={confirmClear}
        onClose={() => setConfirmClear(false)}
        onConfirm={() => clearMutation.mutate()}
        loading={clearMutation.isPending}
        title="Clear the entire pending queue?"
        description={`${total} ${total === 1 ? "row" : "rows"} will be removed without being written to the database. This action cannot be undone.`}
        confirmLabel="Clear queue"
      />
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
  const visible = alternatives.includes(row.final_description)
    ? alternatives
    : [row.final_description, ...alternatives];

  return (
    <li className="px-5 py-3">
      <div className="flex items-start gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <span className="font-mono">{`${row.schema}.${row.table}`}</span>
            {row.column && (
              <span className="font-mono text-ink-dim">.{row.column}</span>
            )}
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
                    "flex w-full items-start gap-2 rounded-md border px-2.5 py-1.5 text-left text-xs transition-colors duration-fast",
                    isChosen
                      ? "border-accent/40 bg-accent-soft/40 text-ink"
                      : "border-border text-ink-muted hover:border-accent/40 hover:bg-surface-subtle/50 hover:text-ink",
                    isPicking && "opacity-60",
                  )}
                  title={
                    isChosen
                      ? "This is the chosen alternative"
                      : "Make this the chosen alternative"
                  }
                >
                  <span
                    className={cn(
                      "mt-0.5 inline-flex h-4 w-4 shrink-0 items-center justify-center rounded text-[10px] font-semibold",
                      isChosen
                        ? "bg-accent text-white"
                        : "bg-surface-subtle text-ink-dim",
                    )}
                  >
                    {isChosen ? <Check size={10} /> : String.fromCharCode(65 + idx)}
                  </span>
                  <span className="leading-relaxed">{alt}</span>
                </button>
              );
            })}
          </div>
        </div>
        <Tooltip content="Remove from queue">
          <IconButton
            icon={<X size={13} />}
            label="Remove from queue"
            size="sm"
            onClick={onRemove}
          />
        </Tooltip>
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
  const seen = new Set<string>();
  return out.filter((d) => {
    if (seen.has(d)) return false;
    seen.add(d);
    return true;
  });
}
