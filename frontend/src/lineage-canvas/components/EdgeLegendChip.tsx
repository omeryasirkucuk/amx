/**
 * Edge legend — small expandable chip mounted inside the canvas
 * (top-right). Click the chip to reveal a panel listing every
 * extractor's swatch + label + description, plus the convention
 * that dashed edges mean low-confidence or name-match.
 */

import { useEffect, useRef, useState } from "react";
import { Info, X } from "lucide-react";
import clsx from "clsx";

import { EDGE_COLORS } from "../constants";
import { EDGE_KIND_META } from "../edgeLabels";

const LEGEND_ORDER = [
  "fk",
  "view_ddl",
  "query_log",
  "codebase",
  "co_occur",
  "lineage_llm",
  "name_match",
  "lineage_manual",
] as const;

export function EdgeLegendChip() {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) return;
    function onDocMouseDown(e: MouseEvent) {
      if (!containerRef.current) return;
      if (containerRef.current.contains(e.target as Node)) return;
      setOpen(false);
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    document.addEventListener("mousedown", onDocMouseDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDocMouseDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  return (
    <div ref={containerRef} className="pointer-events-auto relative">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        title="Edge legend"
        aria-haspopup="dialog"
        aria-expanded={open}
        className={clsx(
          "inline-flex h-7 items-center gap-1.5 rounded-md border border-surface-border bg-surface-raised/90 px-2 text-[11px] text-fg-muted shadow transition",
          "hover:bg-surface hover:text-ink",
          open && "bg-surface text-ink",
        )}
      >
        <Info size={12} />
        <span>Legend</span>
      </button>

      {open && (
        <div className="absolute right-0 top-9 z-20 w-[280px] rounded-md border border-surface-border bg-surface-raised p-3 shadow-xl">
          <div className="mb-2 flex items-center justify-between">
            <span className="text-[11px] font-semibold uppercase tracking-wide text-fg-muted">
              Edge legend
            </span>
            <button
              type="button"
              onClick={() => setOpen(false)}
              aria-label="Close legend"
              className="text-fg-muted transition hover:text-ink"
            >
              <X size={12} />
            </button>
          </div>
          <ul className="space-y-1.5">
            {LEGEND_ORDER.map((key) => {
              const meta = EDGE_KIND_META[key];
              const color = EDGE_COLORS[key] ?? EDGE_COLORS.unknown;
              if (!meta) return null;
              return (
                <li key={key} className="flex items-start gap-2">
                  <span
                    aria-hidden
                    className="mt-1.5 inline-block h-[2px] w-5 shrink-0"
                    style={{ backgroundColor: color }}
                  />
                  <div className="min-w-0">
                    <div className="text-[12px] text-ink">{meta.label}</div>
                    <div className="text-[10.5px] text-fg-muted">
                      {meta.description}
                    </div>
                  </div>
                </li>
              );
            })}
          </ul>
          <p className="mt-3 border-t border-surface-border pt-2 text-[10.5px] text-fg-muted">
            <span className="inline-block h-[2px] w-5 align-middle"
              style={{ backgroundImage: "linear-gradient(to right, currentColor 60%, transparent 0)", backgroundSize: "5px 2px", backgroundRepeat: "repeat-x" }} />{" "}
            Dashed = name-match or LLM confidence under 70%.
          </p>
        </div>
      )}
    </div>
  );
}
