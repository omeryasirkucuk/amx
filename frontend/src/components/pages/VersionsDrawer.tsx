// Slide-in panel that lists every saved version of a page.
// Each row carries the saved_at timestamp, the optional note
// (regenerations and steering prompts ride here), and a Restore
// button. Restoring rolls the body back through the same PATCH
// endpoint the autosave loop uses, so it lands as a new
// documentation_page_versions row rather than overwriting history.

import { useEffect } from "react";
import { History, RotateCcw, X } from "lucide-react";

import { cn } from "../../lib/cn";
import type { PageVersion } from "../../hooks/usePages";

interface Props {
  open: boolean;
  onClose: () => void;
  versions: PageVersion[];
  currentBody: string;
  restoring: boolean;
  onRestore: (version: PageVersion) => void;
}

function relative(saved_at: string): string {
  const ts = new Date(saved_at).getTime();
  if (Number.isNaN(ts)) return saved_at;
  const diffMs = Date.now() - ts;
  const sec = Math.round(diffMs / 1000);
  if (sec < 45) return "just now";
  const min = Math.round(sec / 60);
  if (min < 60) return `${min} min ago`;
  const hr = Math.round(min / 60);
  if (hr < 24) return `${hr} h ago`;
  const day = Math.round(hr / 24);
  if (day < 7) return `${day} d ago`;
  return new Date(saved_at).toLocaleDateString();
}

export default function VersionsDrawer({
  open,
  onClose,
  versions,
  currentBody,
  restoring,
  onRestore,
}: Props) {
  // Close on Escape, restore focus to body when the drawer unmounts.
  useEffect(() => {
    if (!open) return;
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-40">
      <div
        role="presentation"
        onClick={onClose}
        className="absolute inset-0 bg-black/40 backdrop-blur-[2px]"
      />
      <aside
        role="dialog"
        aria-label="Version history"
        className="absolute right-0 top-0 flex h-full w-full max-w-md flex-col border-l border-border bg-surface-raised shadow-xl"
      >
        <header className="flex items-center justify-between gap-2 border-b border-border px-4 py-3">
          <div className="inline-flex items-center gap-2 text-sm font-medium text-ink">
            <History size={14} />
            Version history
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="inline-flex h-7 w-7 items-center justify-center rounded text-ink-muted hover:bg-surface-subtle hover:text-ink"
          >
            <X size={14} />
          </button>
        </header>
        <div className="flex-1 overflow-y-auto p-3">
          {versions.length === 0 ? (
            <p className="px-1 py-4 text-xs text-ink-dim">
              No versions yet. Saves and re-generations land here.
            </p>
          ) : (
            <ul className="space-y-2">
              {versions.map((v) => {
                const isCurrent = v.markdown_body === currentBody;
                return (
                  <li
                    key={v.version_no}
                    className={cn(
                      "rounded-md border border-border bg-surface p-3",
                      isCurrent && "border-accent/40",
                    )}
                  >
                    <div className="mb-1 flex items-center justify-between gap-2">
                      <span className="text-xs font-medium text-ink">
                        v{v.version_no}
                      </span>
                      <span className="text-[11px] text-ink-dim">
                        {relative(v.saved_at)}
                      </span>
                    </div>
                    {v.note && (
                      <p className="mb-2 text-[11px] text-ink-muted">
                        {v.note}
                      </p>
                    )}
                    <div className="mb-2 line-clamp-2 font-mono text-[11px] text-ink-dim">
                      {v.markdown_body.slice(0, 200) || "(empty)"}
                    </div>
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-[10px] text-ink-dim">
                        {v.markdown_body.length.toLocaleString()} chars
                      </span>
                      {!isCurrent ? (
                        <button
                          type="button"
                          onClick={() => onRestore(v)}
                          disabled={restoring}
                          className="inline-flex items-center gap-1 rounded border border-border bg-surface px-2 py-1 text-[11px] text-ink-muted hover:border-accent/40 hover:text-ink disabled:cursor-not-allowed disabled:opacity-60"
                        >
                          <RotateCcw size={11} />
                          Restore
                        </button>
                      ) : (
                        <span className="text-[10px] font-medium text-accent-ink">
                          current
                        </span>
                      )}
                    </div>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      </aside>
    </div>
  );
}
