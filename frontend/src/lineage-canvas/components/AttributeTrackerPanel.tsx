/**
 * AttributeTrackerPanel — Cmd/Ctrl+Shift+F floating tracker for
 * finding columns by name across the canvas. Highlights matching
 * column rows on every node.
 */

import { useEffect, useMemo, useState } from "react";
import { X } from "lucide-react";
import type { CanvasNode } from "../types";

interface Props {
  open: boolean;
  onClose: () => void;
  nodes: CanvasNode[];
  onHighlight: (nodeId: string, column: string) => void;
}

export function AttributeTrackerPanel({ open, onClose, nodes, onHighlight }: Props) {
  const [q, setQ] = useState("");
  useEffect(() => {
    if (open) setQ("");
  }, [open]);

  const matches = useMemo(() => {
    const needle = q.trim().toLowerCase();
    if (!needle) return [];
    const out: Array<{ nodeId: string; nodeLabel: string; column: string }> = [];
    for (const n of nodes) {
      if (n.data.kind !== "table") continue;
      const tbl = n.data.fqn || n.id;
      for (const col of n.data.columns || []) {
        if (col.name.toLowerCase().includes(needle)) {
          out.push({ nodeId: n.id, nodeLabel: tbl, column: col.name });
        }
      }
    }
    return out.slice(0, 80);
  }, [nodes, q]);

  if (!open) return null;
  return (
    <div className="fixed right-4 top-20 z-40 w-80 rounded-xl border border-amber-500/60 bg-surface-raised shadow-2xl">
      <div className="flex items-center gap-2 border-b border-surface-border px-3 py-2">
        <input
          autoFocus
          value={q}
          placeholder="Track attribute by name…"
          onChange={(e) => setQ(e.target.value)}
          className="flex-1 bg-transparent text-sm outline-none placeholder:text-fg-muted"
        />
        <span className="rounded bg-surface px-1.5 py-0.5 text-[10px] text-fg-muted">
          {matches.length}
        </span>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close"
          className="text-fg-muted hover:text-ink"
        >
          <X size={14} />
        </button>
      </div>
      <ul className="max-h-72 overflow-y-auto text-[12px]">
        {matches.length === 0 ? (
          <li className="px-3 py-2 text-fg-muted">
            {q.trim() ? "No matches." : "Type to search columns…"}
          </li>
        ) : (
          matches.map((m, i) => (
            <li
              key={`${m.nodeId}-${m.column}-${i}`}
              className="flex cursor-pointer items-center justify-between gap-2 px-3 py-1.5 hover:bg-surface"
              onClick={() => onHighlight(m.nodeId, m.column)}
            >
              <span className="font-mono">
                <span className="text-amber-300">{m.column}</span>
                <span className="text-fg-muted"> · {m.nodeLabel}</span>
              </span>
            </li>
          ))
        )}
      </ul>
    </div>
  );
}
