import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Database, RefreshCw } from "lucide-react";

import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";

interface FreshnessProfile {
  profile: string;
  entity_count: number;
  last_synced_at: number | null;
  age_seconds: number | null;
  stale: boolean;
  // Skeleton-sync state machine. ``"none"`` = never synced, ``"syncing"`` =
  // background daemon thread is running, ``"done"`` = catalog complete and
  // safe to use, ``"failed"`` = last sync hit an error and gave up.
  state: "none" | "syncing" | "done" | "failed";
  total_tables: number;
  processed_tables: number;
  started_at: number | null;
  finished_at: number | null;
  last_full_sync_at: number | null;
  last_error: string;
}

interface FreshnessResponse {
  profiles: FreshnessProfile[];
  stale_profile_count: number;
  syncing_profile_count: number;
  stale_after_seconds: number;
}

function relativeAge(ageSec: number | null): string {
  if (ageSec == null) return "never";
  if (ageSec < 60) return "just now";
  if (ageSec < 3600) return `${Math.floor(ageSec / 60)} min ago`;
  if (ageSec < 86_400) return `${Math.floor(ageSec / 3600)} h ago`;
  return `${Math.floor(ageSec / 86_400)} d ago`;
}

/**
 * Top-bar pill surfacing the catalog skeleton-sync state.
 *
 * The pill has four tones — neutral (no profile indexed yet), positive
 * (every profile fully synced in last 24h), warning (at least one
 * stale or failed), and "syncing" (an active skeleton sync is in
 * flight).  While any profile is syncing the pill auto-polls every
 * 2s so progress updates feel live; idle pill polls every 30s.
 */
export default function CatalogFreshnessBadge() {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const qc = useQueryClient();

  // Close the dropdown on outside click + Escape, mirroring the
  // pattern used by ProfilePicker so every top-bar dropdown behaves
  // the same.
  useEffect(() => {
    if (!open) return;
    function onMouseDown(e: MouseEvent) {
      if (!wrapperRef.current) return;
      if (!wrapperRef.current.contains(e.target as Node)) setOpen(false);
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    document.addEventListener("mousedown", onMouseDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const query = useQuery({
    queryKey: ["catalog-freshness"],
    queryFn: () => apiFetch<FreshnessResponse>("/api/catalog/freshness"),
    // Fast poll while a sync is in flight — the progress numbers
    // would feel frozen at the idle 30s cadence. Once everything is
    // done/failed/none, drop back to 30s.
    refetchInterval: (q) => {
      const d = q.state.data as FreshnessResponse | undefined;
      return d && d.syncing_profile_count > 0 ? 2_000 : 30_000;
    },
    retry: false,
  });

  const data = query.data;
  const syncingCount = data?.syncing_profile_count ?? 0;
  const staleCount = data?.stale_profile_count ?? 0;
  const failedCount = (data?.profiles ?? []).filter(
    (p) => p.state === "failed",
  ).length;
  const profileCount = data?.profiles.length ?? 0;

  const sync = useMutation({
    mutationFn: (target: string | null) =>
      apiFetch(
        target
          ? `/api/catalog/sync?profile=${encodeURIComponent(target)}`
          : "/api/catalog/sync",
        { method: "POST" },
      ),
    onSettled: () => {
      // Two invalidations: the first picks up the synchronous
      // ``state='syncing'`` flip the backend made before returning;
      // the second 3 s later catches small catalogs that finish
      // before the next poll tick.
      qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
      window.setTimeout(() => {
        qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
      }, 3000);
    },
  });

  const isSyncing = syncingCount > 0 || sync.isPending;
  const tone: "neutral" | "warning" | "positive" | "syncing" =
    isSyncing
      ? "syncing"
      : profileCount === 0
        ? "neutral"
        : failedCount > 0 || staleCount > 0
          ? "warning"
          : "positive";

  const triggerLabel = isSyncing ? "Syncing…" : "Catalog";
  const triggerCount =
    failedCount > 0
      ? failedCount
      : !isSyncing && staleCount > 0
        ? staleCount
        : 0;

  return (
    <div className="relative" ref={wrapperRef}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        title={
          isSyncing
            ? `${syncingCount} profile(s) syncing in background`
            : profileCount === 0
              ? "No catalog data yet — click to sync"
              : failedCount > 0
                ? `${failedCount} profile(s) failed to sync`
                : staleCount > 0
                  ? `${staleCount} of ${profileCount} profile(s) stale (>24h)`
                  : `${profileCount} profile(s) synced within 24h`
        }
        className={cn(
          "inline-flex h-7 items-center gap-1.5 rounded-md border px-2 text-[11px] font-medium transition-colors duration-fast",
          tone === "positive" &&
            "border-positive/30 bg-positive-soft/40 text-positive hover:bg-positive-soft/60",
          tone === "warning" &&
            "border-warning/40 bg-warning-soft/50 text-warning hover:bg-warning-soft/70",
          tone === "neutral" &&
            "border-border bg-surface-subtle text-ink-dim hover:bg-surface-border",
          tone === "syncing" &&
            "border-accent/30 bg-accent-soft/40 text-accent-ink hover:bg-accent-soft/60",
        )}
      >
        {tone === "syncing" ? (
          <RefreshCw size={12} className="animate-spin" />
        ) : (
          <Database size={12} />
        )}
        <span>{triggerLabel}</span>
        {triggerCount > 0 && (
          <span
            className={cn(
              "rounded-full px-1.5 text-[9px] font-bold",
              failedCount > 0
                ? "bg-critical text-critical-soft"
                : "bg-warning text-warning-soft",
            )}
          >
            {triggerCount}
          </span>
        )}
      </button>
      {open && (
        <div className="absolute right-0 top-8 z-30 w-80 rounded-md border border-border bg-surface-raised p-3 shadow-md animate-fade-in">
          <div className="mb-2 flex items-center justify-between">
            <span className="text-xs font-semibold text-ink">
              Catalog freshness
            </span>
            <button
              type="button"
              onClick={() => sync.mutate(null)}
              disabled={sync.isPending || isSyncing}
              className="inline-flex items-center gap-1 rounded-md bg-accent-soft px-2 py-0.5 text-[11px] font-medium text-accent-ink hover:bg-accent-soft/80 disabled:opacity-60"
              title="Sync all profiles"
            >
              <RefreshCw
                size={11}
                className={isSyncing ? "animate-spin" : ""}
              />
              {isSyncing ? "Syncing…" : "Sync all"}
            </button>
          </div>
          {profileCount === 0 ? (
            <p className="text-[11px] text-ink-dim">
              No profile has been indexed yet. Click <strong>Sync all</strong>{" "}
              to enumerate every schema + table for the active DB profiles.
            </p>
          ) : (
            <ul className="space-y-1.5">
              {(data?.profiles ?? []).map((p) => {
                const progressPct =
                  p.total_tables > 0
                    ? Math.min(
                        100,
                        Math.round((p.processed_tables / p.total_tables) * 100),
                      )
                    : 0;
                return (
                  <li
                    key={p.profile}
                    className="rounded border border-border/40 bg-surface-subtle/30 px-2 py-1.5 text-[11px]"
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="truncate font-mono text-ink">
                        {p.profile}
                      </span>
                      <span
                        className={cn(
                          "shrink-0",
                          p.state === "failed"
                            ? "text-critical"
                            : p.state === "syncing"
                              ? "text-accent-ink"
                              : p.stale
                                ? "text-warning"
                                : "text-ink-dim",
                        )}
                      >
                        {p.state === "syncing"
                          ? `${p.processed_tables} / ${p.total_tables || "…"}`
                          : p.state === "failed"
                            ? "failed"
                            : relativeAge(p.age_seconds) +
                              (p.stale ? " · stale" : "")}
                      </span>
                    </div>
                    {p.state === "syncing" && (
                      <div className="mt-1 h-1 w-full overflow-hidden rounded-full bg-surface-subtle">
                        <div
                          className="h-full bg-accent transition-all"
                          style={{
                            width:
                              p.total_tables > 0
                                ? `${progressPct}%`
                                : "30%",
                          }}
                        />
                      </div>
                    )}
                    {p.state === "failed" && (
                      <div className="mt-1 flex items-start gap-1.5">
                        <span
                          aria-hidden="true"
                          className="mt-0.5 shrink-0 text-critical"
                        >
                          ⚠
                        </span>
                        <div className="min-w-0 flex-1 space-y-1">
                          <p className="break-words text-[10.5px] text-critical">
                            {p.last_error || "Unknown error"}
                          </p>
                          <button
                            type="button"
                            onClick={() => sync.mutate(p.profile)}
                            disabled={sync.isPending}
                            className="inline-flex items-center gap-1 rounded bg-critical/10 px-1.5 py-0.5 text-[10px] font-medium text-critical hover:bg-critical/20 disabled:opacity-60"
                          >
                            <RefreshCw size={10} />
                            Retry
                          </button>
                        </div>
                      </div>
                    )}
                  </li>
                );
              })}
            </ul>
          )}
          <p className="mt-2 text-[10px] text-ink-dim">
            Sidebar / Schedule / Run / Ask read from the live DB until a
            full sync completes — never showing a partial catalog as the
            whole picture.
          </p>
        </div>
      )}
    </div>
  );
}
