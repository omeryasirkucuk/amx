/**
 * SearchModal — Cmd/Ctrl+K palette for finding tables/operators on the
 * canvas. Highlights match position and focuses the chosen node.
 */

import { useEffect, useMemo, useState } from "react";
import { Search } from "lucide-react";
import type { CanvasNode } from "../types";

interface Props {
  open: boolean;
  onClose: () => void;
  nodes: CanvasNode[];
  onSelect: (id: string) => void;
}

export function SearchModal({ open, onClose, nodes, onSelect }: Props) {
  const [q, setQ] = useState("");
  const [activeIdx, setActiveIdx] = useState(0);

  useEffect(() => {
    if (open) {
      setQ("");
      setActiveIdx(0);
    }
  }, [open]);

  const results = useMemo(() => {
    const needle = q.trim().toLowerCase();
    const out: Array<{ id: string; label: string; hint: string }> = [];
    for (const n of nodes) {
      let label = "";
      let hint = "";
      if (n.data.kind === "table") {
        label = n.data.fqn;
        hint = n.data.profile || "table";
      } else if (n.data.kind === "operator") {
        label = `${n.data.opKind}: ${n.data.expression || "—"}`;
        hint = "operator";
      } else {
        label = (n.data as { text?: string }).text || "(empty note)";
        hint = "note";
      }
      if (!needle || label.toLowerCase().includes(needle) || hint.toLowerCase().includes(needle)) {
        out.push({ id: n.id, label, hint });
      }
    }
    return out.slice(0, 20);
  }, [nodes, q]);

  if (!open) return null;
  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center bg-black/40 backdrop-blur-sm"
      onClick={onClose}
    >
      <div
        className="mt-24 w-full max-w-lg rounded-xl border border-surface-border bg-surface-raised shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2 border-b border-surface-border px-3 py-2">
          <Search size={14} className="text-fg-muted" />
          <input
            autoFocus
            value={q}
            placeholder="Search canvas… (Cmd+K)"
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "ArrowDown") {
                e.preventDefault();
                setActiveIdx((i) => (i + 1) % Math.max(results.length, 1));
              } else if (e.key === "ArrowUp") {
                e.preventDefault();
                setActiveIdx((i) =>
                  (i - 1 + results.length) % Math.max(results.length, 1),
                );
              } else if (e.key === "Enter") {
                const hit = results[activeIdx];
                if (hit) {
                  onSelect(hit.id);
                  onClose();
                }
              } else if (e.key === "Escape") {
                onClose();
              }
            }}
            className="w-full bg-transparent text-sm outline-none placeholder:text-fg-muted"
          />
        </div>
        <ul className="max-h-80 overflow-y-auto">
          {results.length === 0 ? (
            <li className="px-3 py-3 text-xs text-fg-muted">No matches.</li>
          ) : (
            results.map((r, idx) => (
              <li
                key={r.id}
                className={
                  "flex cursor-pointer items-center justify-between px-3 py-2 text-[12px] " +
                  (idx === activeIdx ? "bg-accent-soft text-accent-ink" : "")
                }
                onMouseEnter={() => setActiveIdx(idx)}
                onMouseDown={() => {
                  onSelect(r.id);
                  onClose();
                }}
              >
                <span className="font-mono truncate">{r.label}</span>
                <span className="ml-3 text-[10px] uppercase tracking-wide text-fg-muted">
                  {r.hint}
                </span>
              </li>
            ))
          )}
        </ul>
      </div>
    </div>
  );
}
