import { useState } from "react";
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
}

interface FreshnessResponse {
  profiles: FreshnessProfile[];
  stale_profile_count: number;
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
 * Top-bar pill surfacing whether the persistent search catalog is in
 * sync with the live DBs. The pill is green when every saved profile
 * has been synced within the last 24h; warning-yellow when at least
 * one profile is stale (or never synced). Click → POST /api/catalog/sync
 * to kick off a background sync for every profile; the pill polls
 * `/api/catalog/freshness` every 30s so the colour updates without a
 * page refresh.
 */
export default function CatalogFreshnessBadge() {
  const [open, setOpen] = useState(false);
  const qc = useQueryClient();
  const query = useQuery({
    queryKey: ["catalog-freshness"],
    queryFn: () => apiFetch<FreshnessResponse>("/api/catalog/freshness"),
    refetchInterval: 30_000,
    retry: false,
  });
  const sync = useMutation({
    mutationFn: () =>
      apiFetch("/api/catalog/sync", { method: "POST" }),
    onSettled: () => {
      qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
    },
  });

  const data = query.data;
  const staleCount = data?.stale_profile_count ?? 0;
  const profileCount = data?.profiles.length ?? 0;
  const tone =
    profileCount === 0
      ? "neutral"
      : staleCount > 0
      ? "warning"
      : "positive";

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        title={
          profileCount === 0
            ? "No catalog data yet — run /search sync to index"
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
        )}
      >
        <Database size={12} />
        <span>Catalog</span>
        {staleCount > 0 && (
          <span className="rounded-full bg-warning px-1.5 text-[9px] font-bold text-warning-soft">
            {staleCount}
          </span>
        )}
      </button>
      {open && (
        <div className="absolute right-0 top-8 z-30 w-72 rounded-md border border-border bg-surface-raised p-3 shadow-md animate-fade-in">
          <div className="mb-2 flex items-center justify-between">
            <span className="text-xs font-semibold text-ink">Catalog freshness</span>
            <button
              type="button"
              onClick={() => sync.mutate()}
              disabled={sync.isPending}
              className="inline-flex items-center gap-1 rounded-md bg-accent-soft px-2 py-0.5 text-[11px] font-medium text-accent-ink hover:bg-accent-soft/80 disabled:opacity-60"
              title="Sync all profiles"
            >
              <RefreshCw size={11} className={sync.isPending ? "animate-spin" : ""} />
              {sync.isPending ? "Syncing…" : "Sync all"}
            </button>
          </div>
          {profileCount === 0 ? (
            <p className="text-[11px] text-ink-dim">
              No profile has been indexed yet. Click <strong>Sync all</strong>{" "}
              to kick off the first ingest, or run <code>/search sync</code>{" "}
              from the REPL.
            </p>
          ) : (
            <ul className="space-y-1">
              {(data?.profiles ?? []).map((p) => (
                <li
                  key={p.profile}
                  className="flex items-center justify-between gap-2 text-[11px]"
                >
                  <span className="truncate font-mono text-ink">
                    {p.profile}
                  </span>
                  <span
                    className={cn(
                      "shrink-0",
                      p.stale ? "text-warning" : "text-ink-dim",
                    )}
                  >
                    {relativeAge(p.age_seconds)}
                    {p.stale && " · stale"}
                  </span>
                </li>
              ))}
            </ul>
          )}
          <p className="mt-2 text-[10px] text-ink-dim">
            Stale = no sync in the last 24h. New tables and edited
            descriptions land in <code>/ask</code> only after a sync.
          </p>
        </div>
      )}
    </div>
  );
}
