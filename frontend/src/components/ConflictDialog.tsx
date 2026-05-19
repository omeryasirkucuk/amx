/**
 * ConflictDialog — shown when a save mutation returns a 409 stale_version
 * error from the shared history store.
 *
 * Presents a side-by-side diff (yours vs. what is currently in the
 * shared store) and four resolution options:
 *   Cancel          — discard your edit, keep the current version
 *   Keep theirs     — reload the page with the current server version
 *   Overwrite       — force-write your version on top
 *   Edit my version — close the dialog and let the user revise
 *
 * The diff view uses <pre> side-by-side for simplicity. A full diff
 * library is out of scope for v1.
 */

import { useEffect } from "react";
import { X } from "lucide-react";
import { cn } from "../lib/cn";

export interface ConflictSnapshot {
  version: number;
  updated_by: string;
  updated_at: string;
  /** The current server-side value for the resource being edited. */
  current_value: Record<string, unknown>;
}

interface Props {
  open: boolean;
  /** Text the user was trying to save. */
  myText: string;
  /** Server snapshot at conflict time. */
  actual: ConflictSnapshot;
  /** Called when the user picks Cancel or closes the dialog. */
  onCancel: () => void;
  /** Called when the user picks "Keep theirs" (reload). */
  onKeepTheirs: () => void;
  /** Called when the user picks "Overwrite with mine". */
  onOverwrite: () => void;
  /** Called when the user picks "Edit my version" (close & let them fix). */
  onEdit: () => void;
}

function relativeTime(isoStr: string): string {
  try {
    const diffMs = Date.now() - new Date(isoStr).getTime();
    const sec = Math.max(0, Math.floor(diffMs / 1000));
    if (sec < 60) return "just now";
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min} minute${min === 1 ? "" : "s"} ago`;
    const hrs = Math.floor(min / 60);
    if (hrs < 24) return `${hrs} hour${hrs === 1 ? "" : "s"} ago`;
    const days = Math.floor(hrs / 24);
    return `${days} day${days === 1 ? "" : "s"} ago`;
  } catch {
    return isoStr;
  }
}

/** Extract the most-human-readable text from the server snapshot. */
function extractTheirText(actual: ConflictSnapshot): string {
  const val = actual.current_value;
  if (typeof val.markdown_body === "string") return val.markdown_body;
  if (typeof val.text === "string") return val.text;
  return JSON.stringify(val, null, 2);
}

export default function ConflictDialog({
  open,
  myText,
  actual,
  onCancel,
  onKeepTheirs,
  onOverwrite,
  onEdit,
}: Props) {
  useEffect(() => {
    if (!open) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onCancel();
    }
    document.addEventListener("keydown", onKey);
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prev;
    };
  }, [open, onCancel]);

  if (!open) return null;

  const theirText = extractTheirText(actual);

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/60 px-4 pb-8 pt-16">
      {/* Backdrop */}
      <div className="absolute inset-0" onClick={onCancel} aria-hidden="true" />

      {/* Dialog panel */}
      <div
        className={cn(
          "relative z-10 mx-auto w-full max-w-3xl rounded-xl border border-border bg-surface-raised shadow-2xl",
          "sm:max-w-2xl md:max-w-3xl",
        )}
        role="dialog"
        aria-modal="true"
        aria-labelledby="conflict-dialog-title"
      >
        {/* Header */}
        <div className="flex items-start justify-between gap-3 border-b border-border px-6 py-4">
          <div>
            <h2
              id="conflict-dialog-title"
              className="text-base font-semibold text-ink"
            >
              Edit conflict
            </h2>
            <p className="mt-0.5 text-sm text-ink-muted">
              This was changed by{" "}
              <span className="font-medium text-ink">
                {actual.updated_by || "another user"}
              </span>{" "}
              {relativeTime(actual.updated_at)}.
            </p>
          </div>
          <button
            type="button"
            onClick={onCancel}
            aria-label="Close"
            className="rounded p-1 text-ink-dim hover:bg-surface-subtle hover:text-ink"
          >
            <X size={16} />
          </button>
        </div>

        {/* Diff view */}
        <div className="grid grid-cols-1 gap-3 p-6 sm:grid-cols-2">
          {/* Their version */}
          <div className="flex flex-col gap-1">
            <span className="text-[10px] font-semibold uppercase tracking-wider text-ink-dim">
              Current (their version)
            </span>
            <pre className="max-h-64 overflow-auto rounded-md border border-border bg-surface-subtle px-3 py-2 text-xs text-ink sm:max-h-80">
              {theirText || <em className="text-ink-dim">(empty)</em>}
            </pre>
          </div>

          {/* My version */}
          <div className="flex flex-col gap-1">
            <span className="text-[10px] font-semibold uppercase tracking-wider text-accent-ink">
              Your version
            </span>
            <pre className="max-h-64 overflow-auto rounded-md border border-accent/30 bg-accent-soft/10 px-3 py-2 text-xs text-ink sm:max-h-80">
              {myText || <em className="text-ink-dim">(empty)</em>}
            </pre>
          </div>
        </div>

        {/* Actions */}
        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-border px-6 py-4">
          <button
            type="button"
            onClick={onCancel}
            className="rounded-md border border-border bg-surface px-3 py-1.5 text-sm text-ink-muted hover:bg-surface-subtle hover:text-ink"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={onKeepTheirs}
            className="rounded-md border border-border bg-surface px-3 py-1.5 text-sm text-ink-muted hover:bg-surface-subtle hover:text-ink"
          >
            Keep theirs
          </button>
          <button
            type="button"
            onClick={onEdit}
            className="rounded-md border border-border bg-surface px-3 py-1.5 text-sm text-ink hover:bg-surface-subtle"
          >
            Edit my version
          </button>
          <button
            type="button"
            onClick={onOverwrite}
            className="rounded-md bg-critical px-3 py-1.5 text-sm font-medium text-white hover:bg-critical/90"
          >
            Overwrite with mine
          </button>
        </div>
      </div>
    </div>
  );
}
