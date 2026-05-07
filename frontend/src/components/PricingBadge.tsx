/**
 * PricingBadge — TopBar pill that reports the LLM-price cache freshness.
 *
 * Reads ``GET /api/pricing/cache-info`` once on mount, then every five
 * minutes. The icon button to the right runs ``POST /api/pricing/refresh``
 * (LiteLLM + OpenRouter pull) and invalidates the cache-info query so
 * the freshness label updates as soon as the worker writes the new
 * cache file.
 *
 * The badge stays out of the way: it hides itself entirely when the
 * cache is fresh AND not in flight, so the TopBar does not gain extra
 * chrome for a state that is already the default. Stale / never-fetched
 * surfaces the warning tone — the user can either click ↻ here or run
 * ``/refresh-prices`` from the CLI.
 */

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, RefreshCw } from "lucide-react";

import { api } from "../lib/api";
import { cn } from "../lib/cn";
import { useToast } from "./ui";

const PRICING_CACHE_KEY = ["pricing", "cache-info"] as const;
const CACHE_REFRESH_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

function formatAge(seconds: number | null): string {
  if (seconds == null) return "never fetched";
  if (seconds < 60) return `${Math.floor(seconds)}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86_400) return `${(seconds / 3600).toFixed(1)}h ago`;
  return `${(seconds / 86_400).toFixed(1)}d ago`;
}

export default function PricingBadge() {
  const { push: pushToast } = useToast();
  const queryClient = useQueryClient();
  const cacheInfo = useQuery({
    queryKey: PRICING_CACHE_KEY,
    queryFn: () => api.pricingCacheInfo(),
    refetchInterval: CACHE_REFRESH_INTERVAL_MS,
    refetchOnWindowFocus: false,
    retry: false,
  });
  const refresh = useMutation({
    mutationFn: () => api.refreshPrices(),
    onSuccess: (result) => {
      const { litellm, openrouter, errors, skipped } = result;
      if (skipped) {
        pushToast({
          tone: "info",
          title: "Prices already fresh",
          description: `${litellm} LiteLLM + ${openrouter} OpenRouter models loaded.`,
          duration: 1800,
        });
      } else if (errors.length > 0) {
        pushToast({
          tone: "warning",
          title: "Prices partially refreshed",
          description: errors.join("; "),
          duration: 4000,
        });
      } else {
        pushToast({
          tone: "success",
          title: "Prices refreshed",
          description: `${litellm} LiteLLM + ${openrouter} OpenRouter models. Cache valid for 24h.`,
          duration: 2400,
        });
      }
      queryClient.invalidateQueries({ queryKey: PRICING_CACHE_KEY });
    },
    onError: (e: Error) => {
      pushToast({
        tone: "error",
        title: "Price refresh failed",
        description: e.message,
        duration: 4000,
      });
    },
  });

  const data = cacheInfo.data;
  // The badge is always visible (when ``cacheInfo`` has resolved) so
  // users always know where prices come from + when they were last
  // pulled. An earlier iteration hid the badge while the cache was
  // fresh; in practice users discovered the refresh path by accident
  // when the badge eventually went stale, which made "I need to
  // refresh" feel surprise-driven instead of routine. Stale shows in
  // a warning tone; fresh stays in the neutral chrome row so it does
  // not pull focus.
  if (!data) return null;
  const tone = data.is_stale ? "warning" : "info";
  const label =
    data.fetched_at == null
      ? "Prices: never fetched"
      : `Prices: ${formatAge(data.age_seconds)}`;

  return (
    <span
      className={cn(
        "hidden h-7 items-center gap-1.5 rounded-md border px-2 text-[11px] font-medium md:inline-flex",
        tone === "warning"
          ? "border-warning/40 bg-warning-soft/40 text-warning"
          : "border-border bg-surface-subtle/60 text-ink-muted",
      )}
      title={
        data.is_stale
          ? `Price cache is older than ${Math.floor(data.ttl_seconds / 3600)}h. Click to refresh.`
          : "Price cache freshness."
      }
    >
      <span>{label}</span>
      <button
        type="button"
        onClick={() => refresh.mutate()}
        disabled={refresh.isPending}
        aria-label="Refresh LLM price cache"
        className={cn(
          "inline-flex h-5 w-5 items-center justify-center rounded text-ink-muted transition-colors",
          "hover:bg-surface-border hover:text-ink",
          "disabled:cursor-wait disabled:opacity-60",
        )}
      >
        {refresh.isPending ? (
          <Loader2 size={11} className="animate-spin" />
        ) : (
          <RefreshCw size={11} />
        )}
      </button>
    </span>
  );
}
