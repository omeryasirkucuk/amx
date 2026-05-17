/**
 * LineageTabBar — multi-anchor tab strip above the canvas.
 *
 * The URL is the single source of truth: the path param holds the
 * active anchor; the `#tabs=` hash holds the rest. Clicking a tab
 * swaps which one is the path param. Closing the active tab promotes
 * the first remaining tab. Closing the last tab navigates back to
 * the browse page.
 *
 * The component is presentational — the parent
 * (`LineageDetail.tsx`) owns navigation logic; this is just an
 * accessible button row.
 */

import { X } from "lucide-react";

interface Props {
  tabs: string[];
  activeTab: string;
  onPick: (tab: string) => void;
  onClose: (tab: string) => void;
}

export default function LineageTabBar({ tabs, activeTab, onPick, onClose }: Props) {
  return (
    <div
      className="flex flex-wrap items-center gap-1 rounded-md border border-surface-border bg-surface-raised px-2 py-1"
      role="tablist"
      aria-label="Open lineage anchors"
    >
      {tabs.map((tab) => {
        const isActive = tab === activeTab;
        return (
          <div
            key={tab}
            className={
              "inline-flex items-center gap-1 rounded-md text-xs " +
              (isActive
                ? "bg-accent-default/10 text-accent-default"
                : "text-fg-default hover:bg-surface-muted")
            }
          >
            <button
              type="button"
              role="tab"
              aria-selected={isActive}
              onClick={() => onPick(tab)}
              className="px-2 py-1 font-mono"
              title={tab}
            >
              {tab}
            </button>
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onClose(tab);
              }}
              aria-label={`Close ${tab}`}
              className="rounded p-0.5 text-fg-muted hover:bg-surface-muted hover:text-fg-default"
            >
              <X className="h-3 w-3" />
            </button>
          </div>
        );
      })}
    </div>
  );
}
