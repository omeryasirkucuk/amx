/**
 * ResultsFilterBar — PR A of the bulk-run review UX series.
 *
 * Sits above the per-run suggestions table on the Run detail page.
 * Surfaces the same vocabulary the CLI uses (filter / sort / group /
 * status / presets) so a reviewer can browse a 200-row run without
 * scrolling for ages. All state is owned by the parent (RunDetail) —
 * this component is purely controlled.
 *
 * Parity reference: amx/cli_support/review_filter.py (sort keys, group
 * keys, status names). Any change here MUST land on the CLI side too.
 */

import { useEffect, useState } from "react";
import { Search, X } from "lucide-react";

import { cn } from "../lib/cn";

export type SortKey =
  | "natural"
  | "conf-asc"
  | "conf-desc"
  | "logprob-asc"
  | "logprob-desc"
  | "name-asc"
  | "status";

export type GroupKey = "none" | "schema" | "table";

export type StatusFilter = "all" | "unreviewed" | "accepted" | "skipped";

export type ReviewPreset = null | "low_conf" | "has_citations" | "table_only";

interface Props {
  query: string;
  onQueryChange: (next: string) => void;
  sort: SortKey;
  onSortChange: (next: SortKey) => void;
  group: GroupKey;
  onGroupChange: (next: GroupKey) => void;
  statusFilter: StatusFilter;
  onStatusChange: (next: StatusFilter) => void;
  presetActive: ReviewPreset;
  onPresetChange: (next: ReviewPreset) => void;
  totalCount: number;
  visibleCount: number;
  /** Optional per-status counts shown in the chip labels. */
  statusCounts?: {
    all: number;
    unreviewed: number;
    accepted: number;
    skipped: number;
  };
}

/**
 * Tiny debounce hook embedded here to avoid a top-level lib addition
 * for a single use site. Returns ``value`` after ``delay`` ms of
 * quiet — the search input does not trigger an expensive filter on
 * every keystroke.
 */
function useDebouncedValue<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = window.setTimeout(() => setDebounced(value), delay);
    return () => window.clearTimeout(id);
  }, [value, delay]);
  return debounced;
}

const SORT_OPTIONS: { value: SortKey; label: string }[] = [
  { value: "natural", label: "Natural order" },
  { value: "conf-asc", label: "Confidence (low → high)" },
  { value: "conf-desc", label: "Confidence (high → low)" },
  { value: "logprob-asc", label: "Logprob (low → high)" },
  { value: "logprob-desc", label: "Logprob (high → low)" },
  { value: "name-asc", label: "Asset name (A → Z)" },
  { value: "status", label: "Status (unreviewed first)" },
];

const GROUP_OPTIONS: { value: GroupKey; label: string }[] = [
  { value: "none", label: "None" },
  { value: "schema", label: "Schema" },
  { value: "table", label: "Table" },
];

const STATUS_OPTIONS: { value: StatusFilter; label: string }[] = [
  { value: "all", label: "All" },
  { value: "unreviewed", label: "Unreviewed" },
  { value: "accepted", label: "Accepted" },
  { value: "skipped", label: "Skipped" },
];

const PRESETS: { value: NonNullable<ReviewPreset>; label: string }[] = [
  { value: "low_conf", label: "Low confidence (<0.7)" },
  { value: "has_citations", label: "Has citations" },
  { value: "table_only", label: "Table-level only" },
];

export default function ResultsFilterBar({
  query,
  onQueryChange,
  sort,
  onSortChange,
  group,
  onGroupChange,
  statusFilter,
  onStatusChange,
  presetActive,
  onPresetChange,
  totalCount,
  visibleCount,
  statusCounts,
}: Props) {
  // Local mirror of the search box so typing feels instant; the
  // debounced value is what propagates upward and triggers re-filter.
  const [localQuery, setLocalQuery] = useState(query);
  const debounced = useDebouncedValue(localQuery, 150);
  useEffect(() => {
    if (debounced !== query) onQueryChange(debounced);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [debounced]);
  // Keep the input in sync if the parent resets the query externally
  // (clearing a preset can wipe filters).
  useEffect(() => {
    setLocalQuery(query);
  }, [query]);

  const togglePreset = (p: NonNullable<ReviewPreset>) => {
    onPresetChange(presetActive === p ? null : p);
  };

  return (
    <div className="space-y-2 rounded-lg border border-surface-border bg-surface-subtle/40 px-3 py-2.5">
      {/* Row 1 — search + sort + group + result count */}
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-[18rem] flex-1">
          <Search
            size={13}
            className="absolute left-2.5 top-1/2 -translate-y-1/2 text-ink-dim"
          />
          <input
            type="text"
            value={localQuery}
            onChange={(e) => setLocalQuery(e.target.value)}
            placeholder="Search schema · table · column · description"
            aria-label="Filter suggestions"
            className="w-full rounded-md border border-surface-border bg-surface py-1.5 pl-8 pr-7 text-xs text-ink placeholder:text-ink-dim focus:border-accent/40 focus:outline-none focus:ring-1 focus:ring-accent/30"
          />
          {localQuery && (
            <button
              type="button"
              onClick={() => setLocalQuery("")}
              aria-label="Clear search"
              className="absolute right-2 top-1/2 -translate-y-1/2 text-ink-dim hover:text-ink"
            >
              <X size={12} />
            </button>
          )}
        </div>

        <label className="inline-flex items-center gap-1.5 text-xs text-ink-muted">
          <span className="text-ink-dim">Sort</span>
          <select
            value={sort}
            onChange={(e) => onSortChange(e.target.value as SortKey)}
            className="rounded-md border border-surface-border bg-surface px-1.5 py-1 text-xs text-ink focus:border-accent/40 focus:outline-none"
          >
            {SORT_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </label>

        <div className="inline-flex items-center gap-1 text-xs text-ink-muted">
          <span className="text-ink-dim">Group</span>
          <div
            role="group"
            aria-label="Group by"
            className="inline-flex overflow-hidden rounded-md border border-surface-border"
          >
            {GROUP_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                type="button"
                onClick={() => onGroupChange(opt.value)}
                className={cn(
                  "px-2 py-1 text-xs transition-colors",
                  group === opt.value
                    ? "bg-accent-soft text-accent-ink"
                    : "bg-surface text-ink-muted hover:bg-surface-subtle",
                )}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>

        <span className="ml-auto rounded-md bg-surface-subtle px-2 py-1 font-mono text-[11px] tabular-nums text-ink-muted">
          {visibleCount === totalCount
            ? `${totalCount} suggestions`
            : `Showing ${visibleCount} of ${totalCount}`}
        </span>
      </div>

      {/* Row 2 — status chips + presets */}
      <div className="flex flex-wrap items-center gap-1.5">
        {STATUS_OPTIONS.map((opt) => {
          const active = statusFilter === opt.value;
          const count = statusCounts?.[opt.value];
          return (
            <button
              key={opt.value}
              type="button"
              onClick={() => onStatusChange(opt.value)}
              className={cn(
                "rounded-full border px-2.5 py-0.5 text-[11px] transition-colors",
                active
                  ? "border-accent/40 bg-accent-soft text-accent-ink"
                  : "border-surface-border bg-surface text-ink-muted hover:border-accent/30 hover:bg-surface-subtle",
              )}
            >
              {opt.label}
              {typeof count === "number" && (
                <span className="ml-1 font-mono tabular-nums text-ink-dim">
                  ({count})
                </span>
              )}
            </button>
          );
        })}

        <span className="mx-1 hidden h-4 w-px bg-surface-border sm:inline-block" />

        {PRESETS.map((p) => {
          const active = presetActive === p.value;
          return (
            <button
              key={p.value}
              type="button"
              onClick={() => togglePreset(p.value)}
              className={cn(
                "rounded-full border px-2.5 py-0.5 text-[11px] transition-colors",
                active
                  ? "border-warning/40 bg-warning-soft/40 text-ink"
                  : "border-surface-border bg-surface text-ink-muted hover:border-warning/40 hover:bg-warning-soft/20",
              )}
              title={
                active
                  ? "Click to clear this preset"
                  : "Click to apply this preset"
              }
            >
              {p.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}
