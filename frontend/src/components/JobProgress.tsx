import { useMemo } from "react";

import { useEventSource, type SseEvent } from "../lib/sse";
import { cn } from "../lib/cn";
import StatusPill from "./StatusPill";

interface Props {
  jobId: string;
  /** Either "runs" or "apply" — picks the SSE path. */
  kind: "runs" | "apply";
  /** Called when the user clicks the inline cancel button. */
  onCancel?: () => void;
  /** Called once the stream finishes (any terminal event). */
  onTerminal?: (terminal: SseEvent) => void;
}

// JobProgress streams progress events for one /run or /apply job
// into a Rich-CLI-style activity tree. The component is intentionally
// presentational — the parent owns the cancel + retry actions.
export default function JobProgress({ jobId, kind, onCancel, onTerminal }: Props) {
  const { events, closed, error } = useEventSource({
    path: `/api/${kind}/${jobId}/events`,
  });

  const terminal = useMemo(() => events.find((e) => /^job\./.test(e.type)), [events]);
  if (terminal && onTerminal) onTerminal(terminal);

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
            <span>
              Applied{" "}
              <span className="font-medium text-ink">
                {(terminal.summary as { applied?: number } | undefined)?.applied ?? 0}
              </span>{" "}
              row(s).
            </span>
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
