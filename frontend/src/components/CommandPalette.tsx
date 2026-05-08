import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  Command,
  Database,
  History,
  Inbox,
  Settings as SettingsIcon,
  Sparkles,
  Search,
} from "lucide-react";

import { api } from "../lib/api";
import { useUi } from "../lib/store";
import { scopePath } from "../lib/scope";
import { cn } from "../lib/cn";

interface PaletteItem {
  id: string;
  label: string;
  hint?: string;
  group: string;
  icon: typeof Sparkles;
  run: () => void;
  keywords?: string;
}

// Cmd-K palette. AMX Studio's primary navigation is route-based
// but power users want one-key access; this component renders a
// modal with fuzzy search across routes + dynamic actions
// ("Open table sales.orders", "Switch DB to prod"). Mac users press
// ⌘K, others press Ctrl-K.
export default function CommandPalette() {
  const navigate = useNavigate();
  const lastTable = useUi((s) => s.lastOpenedTable);
  const lastSchema = useUi((s) => s.lastOpenedSchema);
  const lastScope = useUi((s) => s.lastOpened);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIdx, setActiveIdx] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  const ctx = useQuery({
    queryKey: ["context"],
    queryFn: () => api.context(),
  });

  // Global Cmd-K / Ctrl-K shortcut. Stops propagation when handled
  // so the palette doesn't open inside an input edit.
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const isMod = e.metaKey || e.ctrlKey;
      if (isMod && (e.key === "k" || e.key === "K")) {
        e.preventDefault();
        setOpen((current) => !current);
        setQuery("");
        setActiveIdx(0);
      } else if (e.key === "Escape" && open) {
        setOpen(false);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open]);

  useEffect(() => {
    if (open) {
      // Defer focus until the modal is in the DOM.
      requestAnimationFrame(() => inputRef.current?.focus());
    }
  }, [open]);

  const items = useMemo<PaletteItem[]>(() => {
    const base: PaletteItem[] = [
      {
        id: "go-home",
        label: "Go to dashboard",
        group: "Navigate",
        icon: Database,
        run: () => navigate("/"),
        keywords: "home dashboard",
      },
      {
        id: "go-runs",
        label: "Go to runs",
        group: "Navigate",
        icon: History,
        run: () => navigate("/runs"),
        keywords: "history runs analyze",
      },
      {
        id: "go-ask",
        label: "Open Ask",
        group: "Navigate",
        icon: Sparkles,
        run: () => navigate("/ask"),
        keywords: "ask chat search agent",
      },
      {
        id: "go-pending",
        label: "Open pending review",
        group: "Navigate",
        icon: Inbox,
        run: () => navigate("/pending"),
        keywords: "pending review approve apply",
      },
      {
        id: "go-settings",
        label: "Open settings",
        group: "Navigate",
        icon: SettingsIcon,
        run: () => navigate("/settings"),
        keywords: "settings profile config",
      },
    ];
    if (lastScope) {
      base.push({
        id: "go-last-table",
        label: `Reopen ${lastScope.profile} · ${lastScope.schema}.${lastScope.table}`,
        hint: "Last viewed",
        group: "Navigate",
        icon: Database,
        run: () =>
          navigate(scopePath(lastScope, lastScope.schema, lastScope.table)),
      });
    } else if (lastSchema && lastTable) {
      // Pre-multi-profile state in localStorage — show a degraded
      // entry that takes the user to home; once they click into a
      // table the new scope-aware version replaces it.
      base.push({
        id: "go-last-table-legacy",
        label: `Reopen ${lastSchema}.${lastTable}`,
        hint: "Last viewed (pick profile from sidebar)",
        group: "Navigate",
        icon: Database,
        run: () => navigate("/"),
      });
    }
    // The "Active DB profile (CLI): {name}" line was retired in 0.13:
    // /api/context no longer surfaces ``active_db_profile`` and Studio
    // picks per-action everywhere. The CLI's default-fallback pointer
    // is set via ``/use-db <name>`` and is intentionally not shown
    // here so users don't grow a dependency on a command-palette
    // affordance that would re-introduce the very concept we removed.
    return base;
  }, [ctx.data, lastScope, lastSchema, lastTable, navigate]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return items;
    return items.filter((item) => {
      const haystack = [item.label, item.keywords, item.hint, item.group]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return haystack.includes(q);
    });
  }, [items, query]);

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        title="Open command palette (⌘K)"
        className="ml-2 inline-flex items-center gap-1.5 rounded-md border border-surface-border bg-surface-subtle px-2.5 py-1 text-[11px] font-medium text-ink-dim hover:border-accent/40 hover:text-ink"
      >
        <Search size={12} />
        <span className="hidden md:inline">Search</span>
        <kbd className="rounded bg-surface-raised px-1 py-px font-mono text-[10px] tracking-wider text-ink-muted">
          ⌘K
        </kbd>
      </button>
    );
  }

  function activate(item: PaletteItem) {
    item.run();
    setOpen(false);
  }

  return (
    <div
      role="dialog"
      aria-modal="true"
      className="fixed inset-0 z-50 flex items-start justify-center bg-black/40 px-4 pt-[10vh] backdrop-blur-sm"
      onClick={() => setOpen(false)}
    >
      <div
        className="w-full max-w-lg overflow-hidden rounded-xl border border-surface-border bg-surface-raised shadow-floating"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2 border-b border-surface-border px-4 py-3">
          <Command size={14} className="text-ink-dim" />
          <input
            ref={inputRef}
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setActiveIdx(0);
            }}
            onKeyDown={(e) => {
              if (e.key === "ArrowDown") {
                e.preventDefault();
                setActiveIdx((i) => Math.min(filtered.length - 1, i + 1));
              } else if (e.key === "ArrowUp") {
                e.preventDefault();
                setActiveIdx((i) => Math.max(0, i - 1));
              } else if (e.key === "Enter" && filtered[activeIdx]) {
                e.preventDefault();
                activate(filtered[activeIdx]);
              }
            }}
            placeholder="Where to?"
            className="flex-1 border-0 bg-transparent text-sm placeholder:text-ink-dim focus:outline-none"
          />
          <kbd className="rounded bg-surface-subtle px-1.5 py-px font-mono text-[10px] tracking-wider text-ink-dim">
            esc
          </kbd>
        </div>
        <ul className="max-h-[50vh] overflow-y-auto py-1">
          {filtered.length === 0 ? (
            <li className="px-4 py-6 text-center text-sm text-ink-dim">No matches.</li>
          ) : (
            filtered.map((item, idx) => (
              <li key={item.id}>
                <button
                  type="button"
                  onMouseEnter={() => setActiveIdx(idx)}
                  onClick={() => activate(item)}
                  className={cn(
                    "flex w-full items-center gap-3 px-4 py-2 text-left text-sm transition",
                    idx === activeIdx
                      ? "bg-accent-soft text-accent-ink"
                      : "text-ink hover:bg-surface-subtle",
                  )}
                >
                  <item.icon size={14} className="shrink-0" />
                  <span className="truncate">{item.label}</span>
                  {item.hint && (
                    <span className="ml-auto truncate text-xs text-ink-dim">
                      {item.hint}
                    </span>
                  )}
                </button>
              </li>
            ))
          )}
        </ul>
      </div>
    </div>
  );
}
