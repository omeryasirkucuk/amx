/**
 * LlmProfilePriceLine — small "$X / $Y per 1M (source)" hint shown
 * under the LLM profile name on Settings and the active-profile chip
 * in the sidebar.
 *
 * Uses the same ``api.lookupPrice`` query the cost-override editor
 * already calls, keyed by ``["pricing", "model", provider, model]``
 * so a refresh of the price cache propagates through every surface
 * at once via TanStack Query's shared cache. Profiles without a
 * provider/model render nothing — falls back to the parent's existing
 * label with no awkward "—" placeholder.
 */

import { useQuery } from "@tanstack/react-query";

import { api } from "../lib/api";
import { cn } from "../lib/cn";

interface Props {
  provider: string;
  model: string;
  /** Bolder rendering for the active profile; muted otherwise. */
  isActive?: boolean;
  /** Visual density. ``compact`` is for the sidebar (smaller font,
   *  no source label) where horizontal space is scarce; ``default``
   *  is for the Settings list where the full hint reads naturally. */
  density?: "compact" | "default";
  className?: string;
}

export default function LlmProfilePriceLine({
  provider,
  model,
  isActive = false,
  density = "default",
  className,
}: Props) {
  const enabled = Boolean(provider && model);
  const price = useQuery({
    queryKey: ["pricing", "model", provider, model],
    queryFn: () => api.lookupPrice(provider, model),
    enabled,
    refetchOnWindowFocus: false,
    retry: false,
  });

  if (!enabled) return null;

  const fontClass = density === "compact" ? "text-[10.5px]" : "text-[11px]";
  const baseTone = isActive ? "text-ink" : "text-ink-muted";

  if (price.isLoading) {
    return (
      <div className={cn(fontClass, "text-ink-dim", className)}>
        Loading price…
      </div>
    );
  }
  const data = price.data;
  if (!data || data.source === "unknown") {
    return (
      <div className={cn(fontClass, "text-ink-dim", className)}>
        no price data — refresh in topbar
      </div>
    );
  }

  if (density === "compact") {
    return (
      <div className={cn(fontClass, baseTone, className)}>
        ${data.input_per_mtok.toFixed(4)} / ${data.output_per_mtok.toFixed(4)} per 1M
      </div>
    );
  }
  return (
    <div className={cn(fontClass, baseTone, className)}>
      ${data.input_per_mtok.toFixed(4)} / ${data.output_per_mtok.toFixed(4)} per 1M{" "}
      <span className="text-ink-dim">({data.source})</span>
    </div>
  );
}
