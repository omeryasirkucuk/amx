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

interface Props {
  nodes: LineageNode[];
  /** Pass-through callback when the user picks a node. */
  onPick: (nodeId: string) => void;
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

  const matches = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return nodes.slice(0, 20);
    const startsWith: LineageNode[] = [];
    const contains: LineageNode[] = [];
    for (const n of nodes) {
      const hay = (n.id + " " + n.label).toLowerCase();
      if (!hay.includes(q)) continue;
      if (n.id.toLowerCase().startsWith(q) || n.label.toLowerCase().startsWith(q)) {
        startsWith.push(n);
      } else {
        contains.push(n);
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
        onPick(pick.id);
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
            <li key={m.id}>
              <button
                type="button"
                onMouseDown={(e) => {
                  e.preventDefault();
                  onPick(m.id);
                  setOpen(false);
                  setQuery("");
                }}
                className={
                  "block w-full truncate px-2 py-1.5 text-left " +
                  (i === active ? "bg-accent-default/10 text-accent-default" : "")
                }
              >
                <span className="font-mono">{m.id}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
