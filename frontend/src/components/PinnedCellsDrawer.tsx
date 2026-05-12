/**
 * PR C — pinned-cells drawer.
 *
 * Surfaces the user's per-row "Pin to comparison" picks in a single
 * side panel they can open from the TopBar. Shows one entry per
 * pinned cell, lets the user drop individual pins or clear the
 * whole set, and routes through to ``/runs/compare?mode=cell&cells=…``
 * when the user clicks ``Compare pinned cells``.
 *
 * State source: localStorage (``readPinnedCells`` from ``lib/pinnedCells``).
 * The component listens to the ``amx:pinned-cells-changed`` window
 * event so it stays in sync when the user pins/unpins from elsewhere.
 */
import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Trash2, X } from "lucide-react";

import {
  clearPinnedCells,
  pinnedCellToToken,
  readPinnedCells,
  unpinCell,
  type PinnedCell,
} from "../lib/pinnedCells";
import { cn } from "../lib/cn";

export interface PinnedCellsDrawerProps {
  open: boolean;
  onClose: () => void;
  /** Active DB profile — selects which localStorage bucket to render. */
  dbProfile?: string | null;
}

export default function PinnedCellsDrawer({
  open,
  onClose,
  dbProfile,
}: PinnedCellsDrawerProps) {
  const navigate = useNavigate();
  const [cells, setCells] = useState<PinnedCell[]>(() =>
    readPinnedCells(dbProfile),
  );

  // Refresh from localStorage on open + whenever the global "pinned
  // cells changed" event fires (emitted by the pin button in
  // ResultRowItem). Avoids stale UI without a polling timer.
  useEffect(() => {
    if (!open) return;
    setCells(readPinnedCells(dbProfile));
    const handler = () => setCells(readPinnedCells(dbProfile));
    window.addEventListener("amx:pinned-cells-changed", handler);
    return () => window.removeEventListener("amx:pinned-cells-changed", handler);
  }, [open, dbProfile]);

  if (!open) return null;

  function removeOne(cell: PinnedCell) {
    const next = unpinCell(dbProfile, cell);
    setCells(next);
    window.dispatchEvent(new CustomEvent("amx:pinned-cells-changed"));
  }

  function clearAll() {
    clearPinnedCells(dbProfile);
    setCells([]);
    window.dispatchEvent(new CustomEvent("amx:pinned-cells-changed"));
  }

  function compareAll() {
    if (cells.length === 0) return;
    const tokens = cells.map(pinnedCellToToken).join(",");
    navigate(`/runs/compare?mode=cell&cells=${encodeURIComponent(tokens)}`);
    onClose();
  }

  return (
    <>
      <div
        aria-hidden
        onClick={onClose}
        className="fixed inset-0 z-40 bg-ink/30 transition-opacity"
      />
      <aside
        role="dialog"
        aria-label="Pinned for comparison"
        className={cn(
          "fixed right-0 top-0 z-50 flex h-full w-[24rem] flex-col border-l border-border bg-surface shadow-xl",
        )}
      >
        <header className="flex items-center justify-between border-b border-border px-4 py-3">
          <div>
            <h2 className="text-sm font-semibold text-ink">
              Pinned for comparison
            </h2>
            <p className="text-xs text-ink-dim">
              {cells.length} cell{cells.length === 1 ? "" : "s"} ready to compare
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close pinned cells drawer"
            className="rounded p-1 text-ink-muted hover:bg-surface-subtle hover:text-ink"
          >
            <X size={16} />
          </button>
        </header>

        <div className="flex-1 overflow-y-auto">
          {cells.length === 0 ? (
            <div className="px-4 py-8 text-center text-xs text-ink-dim">
              No pinned cells yet. Open a run, click the pin icon on any
              suggestion, and it lands here.
            </div>
          ) : (
            <ul className="divide-y divide-border">
              {cells.map((cell, idx) => {
                const path = [cell.schema, cell.table, cell.column]
                  .filter(Boolean)
                  .join(".");
                return (
                  <li
                    key={`${cell.run_id}-${path}-${idx}`}
                    className="flex items-start gap-2 px-4 py-2.5"
                  >
                    <div className="min-w-0 flex-1">
                      <div className="truncate font-mono text-xs text-ink" title={path}>
                        {path}
                      </div>
                      <div className="mt-0.5 text-[10px] uppercase tracking-wider text-ink-dim">
                        from run #{cell.run_id}
                        {cell.column ? " · column" : " · table-level"}
                      </div>
                    </div>
                    <button
                      type="button"
                      aria-label={`Unpin ${path}`}
                      onClick={() => removeOne(cell)}
                      className="rounded p-1 text-ink-dim hover:bg-surface-subtle hover:text-ink"
                    >
                      <X size={14} />
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </div>

        <footer className="flex items-center justify-between gap-2 border-t border-border px-4 py-3">
          <button
            type="button"
            onClick={clearAll}
            disabled={cells.length === 0}
            className="inline-flex items-center gap-1 text-xs text-ink-dim hover:text-ink disabled:opacity-40"
          >
            <Trash2 size={12} />
            Clear all
          </button>
          <button
            type="button"
            onClick={compareAll}
            disabled={cells.length === 0}
            className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
          >
            Compare pinned cells
          </button>
        </footer>
      </aside>
    </>
  );
}
