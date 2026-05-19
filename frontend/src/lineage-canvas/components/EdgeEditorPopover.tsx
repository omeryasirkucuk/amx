/**
 * Floating popover that lets the user re-style or delete a single
 * lineage edge. Mounted by ColumnEdge inside an ``EdgeLabelRenderer``
 * so the popover positions itself at the edge midpoint regardless of
 * pan / zoom.
 *
 * The popover does not own persistence. It calls ``onChange`` for
 * each visual mutation and ``onDelete`` for removal; the caller
 * decides whether to optimistically update local canvas state and
 * whether to fan out a backend PATCH/DELETE.
 */

import type { CSSProperties } from "react";
import clsx from "clsx";
import { ArrowRight, Columns3, Trash2, X } from "lucide-react";

import type { EdgeCardinality } from "../types";

const COLOR_SWATCHES: { label: string; value: string }[] = [
  { label: "Slate", value: "#e2e8f0" },
  { label: "Blue", value: "#60a5fa" },
  { label: "Green", value: "#4ade80" },
  { label: "Violet", value: "#c084fc" },
  { label: "Orange", value: "#fb923c" },
  { label: "Pink", value: "#f472b6" },
];

const CARDINALITIES: EdgeCardinality[] = ["1:1", "1:N", "N:M"];

export interface EdgeStylePatch {
  styleColor?: string | null;
  styleDashed?: boolean | null;
  cardinality?: EdgeCardinality | null;
}

interface Props {
  x: number;
  y: number;
  styleColor?: string;
  styleDashed?: boolean;
  cardinality?: EdgeCardinality;
  /** Column names on either side of the edge — when both are
   *  present, the popover surfaces a "Columns" line plus a
   *  "Jump to columns" affordance that expands both tables and
   *  highlights the rows in question. */
  fromColumn?: string;
  toColumn?: string;
  onChange: (patch: EdgeStylePatch) => void;
  onDelete: () => void;
  onClose: () => void;
  /** Called when the user clicks the "Jump to columns" affordance.
   *  Caller is responsible for expanding both tables and bumping
   *  ``forceExpandTick`` / ``tracedColumn`` on the relevant nodes. */
  onJumpToColumns?: () => void;
}

export function EdgeEditorPopover({
  x,
  y,
  styleColor,
  styleDashed,
  cardinality,
  fromColumn,
  toColumn,
  onChange,
  onDelete,
  onClose,
  onJumpToColumns,
}: Props) {
  const hasColumnPair = !!(fromColumn && toColumn);
  const transform: CSSProperties = {
    position: "absolute",
    transform: `translate(-50%, -120%) translate(${x}px, ${y}px)`,
    pointerEvents: "all",
    zIndex: 30,
  };
  return (
    <div
      style={transform}
      role="dialog"
      aria-label="Edit edge"
      className="rounded-md border border-surface-border bg-surface-raised px-2 py-1.5 text-[11px] text-fg-muted shadow-xl"
      onClick={(e) => e.stopPropagation()}
      onMouseDown={(e) => e.stopPropagation()}
    >
      <div className="flex items-center gap-1.5">
        <button
          type="button"
          onClick={onDelete}
          title="Delete edge"
          aria-label="Delete edge"
          className="inline-flex h-6 w-6 items-center justify-center rounded text-fg-muted transition hover:bg-critical-soft hover:text-critical"
        >
          <Trash2 size={12} />
        </button>
        <span className="h-4 w-px bg-surface-border" />
        <div className="inline-flex overflow-hidden rounded border border-surface-border text-[10px]">
          <button
            type="button"
            onClick={() => onChange({ styleDashed: false })}
            aria-pressed={styleDashed === false}
            className={clsx(
              "px-2 py-0.5 transition",
              styleDashed === false
                ? "bg-accent-soft text-accent-ink"
                : "hover:bg-surface",
            )}
            title="Solid"
          >
            ── solid
          </button>
          <button
            type="button"
            onClick={() => onChange({ styleDashed: true })}
            aria-pressed={styleDashed === true}
            className={clsx(
              "px-2 py-0.5 transition",
              styleDashed === true
                ? "bg-accent-soft text-accent-ink"
                : "hover:bg-surface",
            )}
            title="Dashed"
          >
            ┄ dashed
          </button>
        </div>
        <span className="h-4 w-px bg-surface-border" />
        <div className="flex items-center gap-1">
          {COLOR_SWATCHES.map((s) => (
            <button
              key={s.value}
              type="button"
              onClick={() => onChange({ styleColor: s.value })}
              title={s.label}
              aria-label={`Color: ${s.label}`}
              className={clsx(
                "h-3.5 w-3.5 rounded-full border transition",
                styleColor === s.value
                  ? "border-ink ring-1 ring-ink/40"
                  : "border-surface-border hover:border-ink/60",
              )}
              style={{ backgroundColor: s.value }}
            />
          ))}
        </div>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close edge editor"
          title="Close"
          className="ml-1 inline-flex h-6 w-6 items-center justify-center rounded text-fg-muted transition hover:bg-surface hover:text-ink"
        >
          <X size={12} />
        </button>
      </div>
      <div className="mt-1 flex items-center gap-1.5">
        <span className="text-[9.5px] uppercase tracking-wide text-fg-muted">
          Cardinality
        </span>
        <div className="inline-flex overflow-hidden rounded border border-surface-border text-[10px]">
          <button
            type="button"
            onClick={() => onChange({ cardinality: null })}
            aria-pressed={cardinality === undefined}
            className={clsx(
              "px-2 py-0.5 transition",
              cardinality === undefined
                ? "bg-accent-soft text-accent-ink"
                : "hover:bg-surface",
            )}
            title="No marker"
          >
            none
          </button>
          {CARDINALITIES.map((c) => (
            <button
              key={c}
              type="button"
              onClick={() => onChange({ cardinality: c })}
              aria-pressed={cardinality === c}
              className={clsx(
                "border-l border-surface-border px-2 py-0.5 transition",
                cardinality === c
                  ? "bg-accent-soft text-accent-ink"
                  : "hover:bg-surface",
              )}
              title={c}
            >
              {c}
            </button>
          ))}
        </div>
      </div>
      {hasColumnPair && (
        <div className="mt-1 flex items-center gap-1.5">
          <span className="text-[9.5px] uppercase tracking-wide text-fg-muted">
            Columns
          </span>
          <button
            type="button"
            onClick={() => onJumpToColumns?.()}
            disabled={!onJumpToColumns}
            title="Open both tables and highlight these columns"
            className={clsx(
              "inline-flex items-center gap-1 rounded border border-surface-border px-1.5 py-0.5 font-mono text-[10.5px] transition",
              onJumpToColumns
                ? "text-ink hover:bg-surface"
                : "cursor-default text-fg-muted",
            )}
          >
            <Columns3 size={10} />
            <span className="truncate">{fromColumn}</span>
            <ArrowRight size={9} className="text-fg-muted" />
            <span className="truncate">{toColumn}</span>
          </button>
        </div>
      )}
    </div>
  );
}
