/**
 * Saved-lineages dropdown for the Lineage toolbar.
 *
 * Lists every persisted lineage artifact returned by GET /api/lineage
 * and lets the user jump to one via the existing ?artifact=<id> URL
 * surface. When the canvas has unsaved work, the menu defers the
 * navigation behind an AlertDialog so a stray click cannot blow away
 * in-progress edits.
 */

import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronDown, FolderOpen } from "lucide-react";
import clsx from "clsx";

import { AlertDialog } from "../../components/ui";
import { lineageList, type LineageArtifact } from "../../lib/api";

interface Props {
  /**
   * True when the canvas has nodes the user has not persisted with the
   * exact id the menu would replace. Drives the confirm prompt.
   */
  hasUnsavedWork: boolean;
  /**
   * The id currently loaded into the canvas (if any). Used to mark
   * the active row and to suppress the confirm prompt when picking
   * the artifact that is already open.
   */
  activeArtifactId: number | null;
  /**
   * Called with the chosen artifact's id after the optional confirm.
   * The caller is responsible for updating the URL so the existing
   * load-by-id effect picks it up.
   */
  onPick: (id: number) => void;
}

function relativeTime(epochSeconds: number): string {
  if (!epochSeconds) return "";
  const seconds = Math.max(0, Math.floor(Date.now() / 1000 - epochSeconds));
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months}mo ago`;
  return `${Math.floor(months / 12)}y ago`;
}

export function SavedLineagesMenu({
  hasUnsavedWork,
  activeArtifactId,
  onPick,
}: Props) {
  const [open, setOpen] = useState(false);
  const [pending, setPending] = useState<LineageArtifact | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const listQ = useQuery({
    queryKey: ["lineage-artifacts"],
    queryFn: () => lineageList(),
    // Refetch on open via invalidate-on-mount: query stays fresh
    // because Save flow already invalidates ["lineage-artifacts"].
    staleTime: 30_000,
  });

  // Close on outside click. Skips when the AlertDialog is open so a
  // click on its backdrop does not also close the dropdown.
  useEffect(() => {
    if (!open) return;
    function onDocMouseDown(e: MouseEvent) {
      if (!containerRef.current) return;
      if (containerRef.current.contains(e.target as Node)) return;
      setOpen(false);
    }
    document.addEventListener("mousedown", onDocMouseDown);
    return () => document.removeEventListener("mousedown", onDocMouseDown);
  }, [open]);

  // Close on Escape for keyboard parity with the rest of the toolbar.
  useEffect(() => {
    if (!open) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [open]);

  const artifacts = listQ.data?.artifacts ?? [];
  const count = listQ.data?.count ?? 0;

  function handleRowClick(artifact: LineageArtifact) {
    // Re-opening the artifact already in view is a no-op — no need to
    // prompt or navigate. Just close the menu.
    if (artifact.id === activeArtifactId) {
      setOpen(false);
      return;
    }
    if (hasUnsavedWork) {
      setPending(artifact);
      return;
    }
    onPick(artifact.id);
    setOpen(false);
  }

  function confirmDiscard() {
    if (!pending) return;
    onPick(pending.id);
    setPending(null);
    setOpen(false);
  }

  return (
    <div ref={containerRef} className="relative">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        title="Open saved lineage"
        aria-haspopup="menu"
        aria-expanded={open}
        className={clsx(
          "inline-flex h-8 items-center gap-1.5 rounded-md border border-surface-border px-2 text-[12px] text-fg-muted transition",
          "hover:bg-surface hover:text-ink",
          open && "bg-surface text-ink",
        )}
      >
        <FolderOpen size={13} />
        <span>Saved ({listQ.isLoading ? "…" : count})</span>
        <ChevronDown size={12} className={clsx("transition", open && "rotate-180")} />
      </button>

      {open && (
        <div
          role="menu"
          className="absolute left-0 top-9 z-20 w-[300px] overflow-hidden rounded-md border border-surface-border bg-surface-raised shadow-xl"
        >
          {listQ.isLoading && (
            <div className="px-3 py-4 text-[12px] text-fg-muted">Loading…</div>
          )}
          {listQ.isError && (
            <div className="px-3 py-4 text-[12px] text-critical">
              Could not load saved lineages.
            </div>
          )}
          {!listQ.isLoading && !listQ.isError && artifacts.length === 0 && (
            <div className="px-3 py-4 text-[12px] text-fg-muted">
              No saved lineages yet — Save (⌘S) anytime to populate.
            </div>
          )}
          {artifacts.length > 0 && (
            <ul className="max-h-[320px] overflow-y-auto py-1">
              {artifacts.map((a) => {
                const active = a.id === activeArtifactId;
                return (
                  <li key={a.id}>
                    <button
                      type="button"
                      role="menuitem"
                      onClick={() => handleRowClick(a)}
                      className={clsx(
                        "block w-full px-3 py-2 text-left transition",
                        "hover:bg-surface",
                        active && "bg-surface",
                      )}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <span className="truncate text-[12.5px] font-medium text-ink">
                          {a.name}
                        </span>
                        {active && (
                          <span className="shrink-0 rounded border border-accent-default/40 bg-accent-soft px-1.5 py-0.5 text-[9.5px] uppercase tracking-wide text-accent-ink">
                            open
                          </span>
                        )}
                      </div>
                      <div className="mt-0.5 truncate text-[10.5px] text-fg-muted">
                        {a.node_count} nodes · {a.edge_count} edges ·{" "}
                        {a.db_profile || "no profile"} · saved{" "}
                        {relativeTime(a.generated_at)}
                      </div>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      )}

      <AlertDialog
        open={pending !== null}
        onClose={() => setPending(null)}
        onConfirm={confirmDiscard}
        title="Discard current canvas?"
        description={
          pending
            ? `Opening "${pending.name}" will replace the unsaved canvas you have on screen. Save first (⌘S) if you want to keep it.`
            : ""
        }
        confirmLabel="Discard and open"
        cancelLabel="Keep editing"
      />
    </div>
  );
}
