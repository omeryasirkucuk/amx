import { useEffect } from "react";
import { useLocation } from "react-router-dom";
import { X } from "lucide-react";

import Sidebar from "./Sidebar";
import { useUi } from "../lib/store";
import { cn } from "../lib/cn";

/**
 * Phone-only sidebar drawer. The inline `<aside>` in AppShell is
 * hidden on `<sm` viewports; this overlay takes its place. Pattern
 * mirrors PinnedCellsDrawer: fixed scrim + slide-in panel, closes on
 * scrim click, Escape, or route change.
 */
export default function MobileSidebar() {
  const open = useUi((s) => s.mobileSidebarOpen);
  const setOpen = useUi((s) => s.setMobileSidebarOpen);
  const location = useLocation();

  useEffect(() => {
    if (!open) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, setOpen]);

  useEffect(() => {
    if (!open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [open]);

  useEffect(() => {
    if (open) setOpen(false);
    // Only react to route changes — `open`/`setOpen` are intentionally
    // omitted from deps to avoid the open-then-close storm.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.pathname]);

  if (!open) return null;

  return (
    <>
      <div
        aria-hidden
        onClick={() => setOpen(false)}
        className="fixed inset-0 z-40 bg-ink/30 sm:hidden"
      />
      <aside
        role="dialog"
        aria-label="Navigation drawer"
        className={cn(
          "fixed left-0 top-0 z-50 flex h-full w-72 max-w-[85vw] flex-col border-r border-border bg-surface shadow-xl sm:hidden",
        )}
      >
        <div className="flex items-center justify-between border-b border-border px-3 py-2">
          <span className="text-xs font-medium uppercase tracking-wider text-ink-dim">
            Browse
          </span>
          <button
            type="button"
            onClick={() => setOpen(false)}
            aria-label="Close navigation drawer"
            className="rounded p-1 text-ink-muted hover:bg-surface-subtle hover:text-ink"
          >
            <X size={16} />
          </button>
        </div>
        <div className="flex-1 min-h-0 overflow-hidden">
          <Sidebar collapsed={false} />
        </div>
      </aside>
    </>
  );
}
