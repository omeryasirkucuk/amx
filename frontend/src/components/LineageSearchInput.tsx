/**
 * LineageSearchInput — a small fuzzy-search input pinned to the
 * top-left of the lineage canvas. Type to filter; ↑/↓ to navigate;
 * Enter to focus the canvas on the selected node.
 *
 * The matcher is intentionally tiny — `Array.prototype.filter` over
 * the node id + label with a case-insensitive substring + a cheap
 * "starts with" boost. Pulls Fuse.js or Fuzzysort would be overkill
 * for the < 1000 node graphs this canvas targets.
 *
 * Opens automatically when the global ⌘K / Ctrl-K shortcut is fired
 * via the CommandPalette registry (see LineageDetail.tsx wiring).
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { Search, X } from "lucide-react";

import type { LineageNode } from "../lib/api";

interface Match {
  /** Node id to focus. */
  nodeId: string;
  /** Display label — full FQN, "table.column" when the match was on a
   *  per-column row. */
  label: string;
  /** When non-null, the consumer should also surface the column in
   *  the trace panel. */
  column: string | null;
}

interface Props {
  nodes: LineageNode[];
  /** Pass-through callback when the user picks a match. ``column``
   *  is non-null when the matched row was a per-column entry inside
   *  a TableNode (v4 S6 — datapav "Track attribute by name"). */
  onPick: (nodeId: string, column: string | null) => void;
  /** Set true to render the dropdown expanded immediately (⌘K). */
  openSignal?: number;
}

export default function LineageSearchInput({ nodes, onPick, openSignal }: Props) {
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const [open, setOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement | null>(null);

  // External "open me" pulses (⌘K signal): focus + select-all.
  useEffect(() => {
    if (openSignal === undefined) return;
    setOpen(true);
    requestAnimationFrame(() => {
      inputRef.current?.focus();
      inputRef.current?.select();
    });
  }, [openSignal]);

  const matches = useMemo<Match[]>(() => {
    const q = query.trim().toLowerCase();
    if (!q) {
      return nodes
        .slice(0, 20)
        .map((n) => ({ nodeId: n.id, label: n.label, column: null }));
    }
    const startsWith: Match[] = [];
    const contains: Match[] = [];
    for (const n of nodes) {
      // Match against the node label first.
      const hay = (n.id + " " + n.label).toLowerCase();
      if (hay.includes(q)) {
        const entry: Match = { nodeId: n.id, label: n.label, column: null };
        if (n.id.toLowerCase().startsWith(q) || n.label.toLowerCase().startsWith(q)) {
          startsWith.push(entry);
        } else {
          contains.push(entry);
        }
      }
      // v4 S6 — match individual column rows so the user can jump
      // directly to "orders_raw.amount" instead of just "orders_raw".
      if (n.kind === "table" && n.columns) {
        for (const col of n.columns) {
          if (col.name.toLowerCase().includes(q)) {
            const label = `${n.label}.${col.name}`;
            const entry: Match = { nodeId: n.id, label, column: col.name };
            if (col.name.toLowerCase().startsWith(q)) {
              startsWith.push(entry);
            } else {
              contains.push(entry);
            }
          }
        }
      }
    }
    return [...startsWith, ...contains].slice(0, 20);
  }, [nodes, query]);

  useEffect(() => {
    setActive(0);
  }, [query]);

  const handleKey = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((i) => Math.min(matches.length - 1, i + 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((i) => Math.max(0, i - 1));
    } else if (e.key === "Enter") {
      e.preventDefault();
      const pick = matches[active];
      if (pick) {
        onPick(pick.nodeId, pick.column);
        setOpen(false);
        setQuery("");
      }
    } else if (e.key === "Escape") {
      e.preventDefault();
      setOpen(false);
      setQuery("");
    }
  };

  return (
    <div className="pointer-events-auto absolute left-3 top-3 z-10 w-72">
      <div className="flex items-center gap-1 rounded-md border border-surface-border bg-surface-raised px-2 py-1.5 shadow-sm">
        <Search className="h-3.5 w-3.5 text-fg-muted" />
        <input
          ref={inputRef}
          type="text"
          placeholder="Search nodes (⌘K)"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => setOpen(true)}
          onBlur={() => setTimeout(() => setOpen(false), 120)}
          onKeyDown={handleKey}
          className="flex-1 bg-transparent text-xs focus:outline-none"
        />
        {query && (
          <button
            type="button"
            onClick={() => {
              setQuery("");
              inputRef.current?.focus();
            }}
            className="rounded p-0.5 text-fg-muted hover:bg-surface-muted"
            aria-label="Clear search"
          >
            <X className="h-3 w-3" />
          </button>
        )}
      </div>
      {open && matches.length > 0 && (
        <ul className="mt-1 max-h-72 overflow-y-auto rounded-md border border-surface-border bg-surface-raised text-xs shadow-md">
          {matches.map((m, i) => (
            <li key={`${m.nodeId}:${m.column ?? ""}`}>
              <button
                type="button"
                onMouseDown={(e) => {
                  e.preventDefault();
                  onPick(m.nodeId, m.column);
                  setOpen(false);
                  setQuery("");
                }}
                className={
                  "block w-full truncate px-2 py-1.5 text-left " +
                  (i === active ? "bg-accent-default/10 text-accent-default" : "")
                }
              >
                <span className="font-mono">{m.label}</span>
                {m.column && (
                  <span className="ml-2 text-[9px] uppercase tracking-wide text-fg-muted">
                    column
                  </span>
                )}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
