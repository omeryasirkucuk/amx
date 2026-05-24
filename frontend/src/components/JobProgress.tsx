import { useEffect, useMemo, useRef } from "react";

import { useEventSource, type SseEvent } from "../lib/sse";
import { cn } from "../lib/cn";
import StatusPill from "./StatusPill";

interface Props {
  jobId: string;
  /** Either "runs" / "apply" / "docs/scan" / "docs/ingest" — picks the SSE path. */
  kind: "runs" | "apply" | "docs/scan" | "docs/ingest";
  /** Called when the user clicks the inline cancel button. */
  onCancel?: () => void;
  /** Called once the stream finishes (any terminal event). */
  onTerminal?: (terminal: SseEvent) => void;
  /** Called when the user dismisses the terminal-state card (e.g. after
   * reading the failure banner). Parent typically clears its job-id
   * state so this card unmounts. Omit when the card should stay
   * pinned until parent decides. */
  onDismiss?: () => void;
}

// JobProgress streams progress events for one /run / /apply / docs op
// into a Rich-CLI-style activity tree. The component is intentionally
// presentational — the parent owns the cancel + retry actions.
export default function JobProgress({ jobId, kind, onCancel, onTerminal, onDismiss }: Props) {
  const ssePath =
    kind === "runs" || kind === "apply"
      ? `/api/${kind}/${jobId}/events`
      : // docs/scan + docs/ingest jobs are tailed via the generic /api/runs
        // SSE endpoint — JobRegistry stores every job kind by id, and the
        // generator just streams the queue.
        `/api/runs/${jobId}/events`;
  const { events, closed, error } = useEventSource({ path: ssePath });

  const terminal = useMemo(() => events.find((e) => /^job\./.test(e.type)), [events]);

  // Fire onTerminal exactly once per terminal event. The legacy
  // pattern called onTerminal during render, which fired the callback
  // on every re-render AND let the parent's state update (e.g.
  // ``setActiveApplyJob(null)``) unmount this component before the
  // failure banner could paint — which is exactly the "no banner
  // shown" symptom the user reported. The ref guards re-firing if the
  // SSE stream emits multiple terminal-shaped events.
  const firedRef = useRef<string | null>(null);
  useEffect(() => {
    if (!terminal || !onTerminal) return;
    const key = `${terminal.type}:${(terminal as { _seq?: number })._seq ?? ""}`;
    if (firedRef.current === key) return;
    firedRef.current = key;
    onTerminal(terminal);
  }, [terminal, onTerminal]);

  const writeback = events.filter((e) => e.type === "writeback.progress");
  const lastWriteback = writeback[writeback.length - 1];
  const progressPct = lastWriteback
    ? Math.round((Number(lastWriteback.done || 0) / Math.max(1, Number(lastWriteback.total || 1))) * 100)
    : 0;

  const tone = terminal
    ? terminal.type === "job.done"
      ? "positive"
      : terminal.type === "job.cancelled"
        ? "warning"
        : "critical"
    : "accent";

  const headline = terminal
    ? terminal.type === "job.done"
      ? "Done"
      : terminal.type === "job.cancelled"
        ? "Cancelled"
        : "Failed"
    : "Running";

  return (
    <section
      className={cn(
        "rounded-xl border bg-surface-raised p-4 shadow-card",
        terminal?.type === "job.failed"
          ? "border-critical/40"
          : "border-surface-border",
      )}
    >
      <header className="mb-2 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <StatusPill tone={tone}>{headline}</StatusPill>
          <span className="font-mono text-xs text-ink-dim">#{jobId.slice(0, 8)}</span>
        </div>
        {!closed && onCancel && (
          <button
            type="button"
            onClick={onCancel}
            className="rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-critical/10 hover:text-critical"
          >
            Cancel
          </button>
        )}
        {closed && terminal && onDismiss && (
          <button
            type="button"
            onClick={onDismiss}
            className="rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-surface hover:text-ink"
            title="Dismiss this card"
          >
            Dismiss
          </button>
        )}
      </header>

      {lastWriteback && !terminal && (
        <>
          <div className="mb-1 flex items-center justify-between text-[11px] text-ink-dim">
            <span>
              {String(lastWriteback.schema)}.{String(lastWriteback.table)}
              {lastWriteback.column ? `.${String(lastWriteback.column)}` : ""}
            </span>
            <span>{`${lastWriteback.done}/${lastWriteback.total}`}</span>
          </div>
          <div className="h-1.5 w-full overflow-hidden rounded-full bg-surface-subtle">
            <div
              className="h-full bg-accent transition-all"
              style={{ width: `${progressPct}%` }}
            />
          </div>
        </>
      )}

      {error && (
        <div className="mt-2 text-xs text-critical">
          {error}
        </div>
      )}
      {terminal && (
        <div className="mt-2 space-y-1 text-xs text-ink-muted">
          {terminal.type === "job.done" && (
            <ApplyDoneSummary summary={terminal.summary as ApplySummary | undefined} />
          )}
          {terminal.type === "job.cancelled" && (
            <span>
              Stopped at{" "}
              <span className="font-medium text-ink">
                {(terminal.summary as { applied?: number } | undefined)?.applied ?? 0}
              </span>{" "}
              row(s) — already-written comments are kept.
            </span>
          )}
          {terminal.type === "job.failed" && (
            <span className="text-critical">
              {String(terminal.error || "Worker failed")}
            </span>
          )}
        </div>
      )}
    </section>
  );
}

// Apply path's job.done summary shape — emitted by
// amx/web/routers/runs.py::_apply_worker. ``failed`` is empty on the
// all-applied happy path; non-empty means the queue was preserved
// for those rows so the user can retry, edit, or skip.
interface ApplyFailedEntry {
  result_id: number | null;
  schema: string;
  table: string;
  column: string | null;
  asset_kind: string;
  error_kind: string;
  error_title: string;
  error_text: string;
  error_action: string;
  // Verbatim driver message (e.g. ``[INSUFFICIENT_PERMISSIONS] …``)
  // — present alongside the classifier's curated body so users can
  // see exactly what the database returned.
  error_raw: string;
}

interface ApplySummary {
  applied?: number;
  total?: number;
  failed_count?: number;
  failed?: ApplyFailedEntry[];
}

function ApplyDoneSummary({ summary }: { summary: ApplySummary | undefined }) {
  const applied = summary?.applied ?? 0;
  const failedCount = summary?.failed_count ?? 0;
  const failed = summary?.failed ?? [];

  if (failedCount === 0) {
    return (
      <span>
        Applied{" "}
        <span className="font-medium text-ink">{applied}</span> row(s).
      </span>
    );
  }

  // Aggregate banner: the user's pending queue still holds the failed
  // rows (the worker called clear_pending_for() only for the applied
  // ids). Show one consolidated headline + a disclosure listing each
  // failing asset with its classified suggested_action — no per-row
  // chips on the table itself, per the user's UX call.
  const primaryKind = failed[0]?.error_kind || "unknown";
  const sameKind = failed.every((f) => f.error_kind === primaryKind);
  const headline =
    sameKind && failed[0]?.error_title
      ? failed[0].error_title
      : `${failedCount} row(s) could not be written`;
  const action = sameKind ? failed[0]?.error_action : "";

  return (
    <div className="space-y-2">
      <div className="rounded-md border border-critical/30 bg-critical/5 px-3 py-2">
        <div className="text-sm font-medium text-critical">
          {applied} applied, {failedCount} failed — queue preserved
        </div>
        <div className="mt-1 text-xs text-ink-muted">{headline}</div>
        {action && (
          <div className="mt-1 text-xs text-ink-muted">
            <span className="text-ink-dim">Suggested:</span> {action}
          </div>
        )}
        <details className="mt-2" open={failedCount === 1}>
          <summary className="cursor-pointer text-xs text-ink-dim hover:text-ink-muted">
            Show failing rows ({failedCount})
          </summary>
          <ul className="mt-1 space-y-1 pl-3 text-[11px] text-ink-muted">
            {failed.map((f, i) => {
              const path = [f.schema, f.table, f.column ?? ""]
                .filter(Boolean)
                .join(".");
              return (
                <li key={i}>
                  <div className="font-mono">
                    {path}
                    {!sameKind && f.error_title && (
                      <span className="ml-2 text-ink-dim">— {f.error_title}</span>
                    )}
                  </div>
                  {f.error_raw && (
                    <details className="mt-0.5">
                      <summary className="cursor-pointer text-[11px] text-ink-dim hover:text-ink-muted">
                        Show driver message
                      </summary>
                      <pre className="mt-0.5 whitespace-pre-wrap break-words rounded-md bg-surface-subtle px-2 py-1 font-mono text-[10px] text-ink">
                        {f.error_raw}
                      </pre>
                    </details>
                  )}
                </li>
              );
            })}
          </ul>
        </details>
      </div>
    </div>
  );
}
