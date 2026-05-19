/**
 * BackfillBanner — shown while the shared history store is migrating
 * local lineage and documentation pages data from SQLite into the team
 * workspace.
 *
 * Polls GET /api/history/status every 10 s. Renders nothing when:
 *   - the shared store is not enabled
 *   - no backfill is in progress
 *   - the backfill has already completed
 *
 * Fades out on completion so the transition is gentle.
 */

import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Loader2, X } from "lucide-react";
import { apiFetch } from "../lib/api";

interface HistoryStatus {
  enabled: boolean;
  shared_profile: string;
  backfill: Record<string, string>;
}

function isBackfillRunning(backfill: Record<string, string>): boolean {
  return Object.values(backfill).some((v) => v === "running");
}

function isBackfillDone(backfill: Record<string, string>): boolean {
  if (Object.keys(backfill).length === 0) return false;
  return Object.values(backfill).every((v) => v === "done");
}

export default function BackfillBanner() {
  const [dismissed, setDismissed] = useState(false);
  const [completedAt, setCompletedAt] = useState<number | null>(null);

  const statusQuery = useQuery<HistoryStatus>({
    queryKey: ["history-status"],
    queryFn: () => apiFetch<HistoryStatus>("/api/history/status"),
    refetchInterval: 10_000,
    retry: false,
    // Silence errors — the store might not be initialized yet and that
    // is expected; we do not want banner errors surfacing everywhere.
    meta: { silentError: true },
  });

  const status = statusQuery.data;
  const backfill = status?.backfill ?? {};
  const running = isBackfillRunning(backfill);
  const done = isBackfillDone(backfill);

  // Track when the backfill transitions to done so we can fade out.
  useEffect(() => {
    if (done && completedAt === null) {
      setCompletedAt(Date.now());
    }
  }, [done, completedAt]);

  if (dismissed) return null;
  if (!status?.enabled) return null;
  if (!running && !done) return null;

  // Fade-out: hide banner 4 s after completion.
  if (done && completedAt !== null && Date.now() - completedAt > 4_000) {
    return null;
  }

  return (
    <div
      className={`flex items-center gap-3 border-b border-accent/30 bg-accent-soft/20 px-4 py-2 text-sm transition-opacity duration-700 ${done ? "opacity-0" : "opacity-100"}`}
      role="status"
      aria-live="polite"
    >
      {running ? (
        <Loader2 size={14} className="shrink-0 animate-spin text-accent" />
      ) : (
        <span className="h-3 w-3 shrink-0 rounded-full bg-positive" />
      )}
      <span className="flex-1 text-ink-muted">
        {running
          ? "Migrating your local lineage and pages to the team workspace…"
          : "Migration to team workspace complete."}
      </span>
      {!running && (
        <button
          type="button"
          onClick={() => setDismissed(true)}
          aria-label="Dismiss"
          className="rounded p-1 text-ink-dim hover:bg-surface-subtle hover:text-ink"
        >
          <X size={13} />
        </button>
      )}
    </div>
  );
}
