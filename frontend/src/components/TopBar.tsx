import { Link, NavLink } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import {
  Activity,
  Check,
  ChevronDown,
  Database,
  PanelLeft,
  Sparkles,
  History as HistoryIcon,
  Inbox,
  Settings as SettingsIcon,
} from "lucide-react";

import { api } from "../lib/api";
import { useUi } from "../lib/store";
import { cn } from "../lib/cn";
import ThemeToggle from "./ThemeToggle";
import CommandPalette from "./CommandPalette";

const navItems = [
  { to: "/", label: "Browse", icon: Database, end: true },
  { to: "/runs", label: "Runs", icon: HistoryIcon },
  { to: "/ask", label: "Ask", icon: Sparkles },
  { to: "/pending", label: "Pending", icon: Inbox },
  { to: "/settings", label: "Settings", icon: SettingsIcon },
  { to: "/system", label: "System", icon: Activity },
];

// Persistent top bar. Renders the active DB / LLM "pills" so the
// user always sees the current scope, plus the primary nav. The
// catalog pill becomes a dropdown for 3-level backends so the user
// can pick a catalog without leaving the page.
export default function TopBar() {
  const toggleSidebar = useUi((s) => s.toggleSidebar);
  const { data: ctx } = useQuery({
    queryKey: ["context"],
    queryFn: () => api.context(),
  });
  const catalogs = useQuery({
    queryKey: ["live-catalogs"],
    queryFn: () => api.liveCatalogs(),
    retry: false,
  });
  // Only fetch the database list when the backend is 2-level (no
  // catalog support). 3-level backends like Databricks expose
  // `/api/live/databases` as an empty list and we'd just render an
  // empty dropdown.
  const databases = useQuery({
    queryKey: ["live-databases"],
    queryFn: () => api.liveDatabases(),
    retry: false,
    enabled: catalogs.data ? !catalogs.data.supports_catalogs : false,
  });

  return (
    <header className="border-b border-surface-border bg-surface-raised">
      <div className="flex items-center gap-4 px-6 py-3">
        <button
          type="button"
          onClick={toggleSidebar}
          aria-label="Toggle sidebar"
          className="rounded-md p-1.5 text-ink-muted hover:bg-surface-subtle hover:text-ink"
        >
          <PanelLeft size={18} />
        </button>
        <Link to="/" className="flex items-center gap-2">
          <span className="inline-flex h-7 w-7 items-center justify-center rounded-md bg-accent text-accent-soft font-semibold">
            A
          </span>
          <span className="text-sm font-semibold tracking-tight">
            AMX <span className="text-ink-dim font-normal">Visualizer</span>
          </span>
        </Link>

        <div className="ml-2 flex items-center gap-2 text-xs">
          <Pill
            label="DB"
            value={ctx?.active_db_profile ?? "—"}
            tone={ctx?.active_db_profile ? "accent" : "neutral"}
            tooltip={ctx?.db_backend ?? undefined}
          />
          <Pill
            label="LLM"
            value={ctx?.active_llm_profile ?? "—"}
            tone={ctx?.active_llm_profile ? "accent" : "neutral"}
            tooltip={ctx?.llm_model ?? undefined}
          />
          {catalogs.data?.supports_catalogs && (
            <CatalogPicker
              activeCatalog={catalogs.data.active_catalog}
              catalogs={catalogs.data.catalogs}
            />
          )}
          {catalogs.data && !catalogs.data.supports_catalogs && databases.data && (
            <DatabasePicker
              activeDatabase={databases.data.active_database}
              databases={databases.data.databases}
            />
          )}
        </div>

        <nav className="ml-auto flex items-center gap-1">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                cn(
                  "flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition",
                  isActive
                    ? "bg-accent-soft text-accent-ink"
                    : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
                )
              }
            >
              <item.icon size={15} />
              {item.label}
            </NavLink>
          ))}
          <CommandPalette />
          <ThemeToggle />
        </nav>
      </div>
    </header>
  );
}

function Pill({
  label,
  value,
  tone,
  tooltip,
}: {
  label: string;
  value: string;
  tone: "accent" | "neutral";
  tooltip?: string;
}) {
  return (
    <span
      title={tooltip}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 font-medium",
        tone === "accent"
          ? "bg-accent-soft text-accent-ink"
          : "bg-surface-subtle text-ink-dim",
      )}
    >
      <span className="text-[10px] uppercase tracking-wider opacity-70">
        {label}
      </span>
      <span className="font-mono text-[11px]">{value}</span>
    </span>
  );
}

function CatalogPicker({
  activeCatalog,
  catalogs,
}: {
  activeCatalog: string | null;
  catalogs: string[];
}) {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const queryClient = useQueryClient();

  const activate = useMutation({
    mutationFn: (name: string) => api.activateCatalog(name, true),
    onSuccess: () => {
      // Selecting a catalog invalidates everything that depended on
      // the old scope: catalog list (active flag), schemas, assets,
      // even the dashboard context.
      queryClient.invalidateQueries({ queryKey: ["live-catalogs"] });
      queryClient.invalidateQueries({ queryKey: ["live-schemas"] });
      queryClient.invalidateQueries({ queryKey: ["live-assets"] });
      queryClient.invalidateQueries({ queryKey: ["context"] });
      setOpen(false);
    },
  });

  useEffect(() => {
    if (!open) return;
    function onClick(e: MouseEvent) {
      if (!wrapperRef.current) return;
      if (!wrapperRef.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, [open]);

  const display = activeCatalog || "Select catalog";
  const empty = catalogs.length === 0;

  return (
    <div className="relative" ref={wrapperRef}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className={cn(
          "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium transition",
          activeCatalog
            ? "border-accent/30 bg-accent-soft text-accent-ink"
            : "border-warning/40 bg-warning/10 text-warning",
        )}
      >
        <span className="text-[10px] uppercase tracking-wider opacity-70">Catalog</span>
        <span className="font-mono text-[11px]">{display}</span>
        <ChevronDown size={12} className="opacity-70" />
      </button>
      {open && (
        <div className="absolute right-0 z-30 mt-1 w-56 overflow-hidden rounded-md border border-surface-border bg-surface-raised shadow-lg">
          {empty ? (
            <div className="px-3 py-2 text-xs text-ink-dim">No catalogs visible.</div>
          ) : (
            <ul className="max-h-72 overflow-y-auto py-1">
              {catalogs.map((name) => {
                const isActive = name === activeCatalog;
                return (
                  <li key={name}>
                    <button
                      type="button"
                      onClick={() => activate.mutate(name)}
                      disabled={activate.isPending}
                      className={cn(
                        "flex w-full items-center justify-between gap-2 px-3 py-1.5 text-left text-xs hover:bg-surface-subtle disabled:opacity-50",
                        isActive && "bg-surface-subtle/60",
                      )}
                    >
                      <span className="truncate font-mono">{name}</span>
                      {isActive && <Check size={12} className="text-accent" />}
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
          {activate.isError && (
            <div className="border-t border-surface-border px-3 py-1.5 text-[11px] text-critical">
              {activate.error instanceof Error
                ? activate.error.message
                : "Activation failed."}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function DatabasePicker({
  activeDatabase,
  databases,
}: {
  activeDatabase: string | null;
  databases: string[];
}) {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const queryClient = useQueryClient();

  const activate = useMutation({
    mutationFn: (name: string) => api.activateDatabase(name, true),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["live-databases"] });
      queryClient.invalidateQueries({ queryKey: ["live-schemas"] });
      queryClient.invalidateQueries({ queryKey: ["live-assets"] });
      queryClient.invalidateQueries({ queryKey: ["context"] });
      setOpen(false);
    },
  });

  useEffect(() => {
    if (!open) return;
    function onClick(e: MouseEvent) {
      if (!wrapperRef.current) return;
      if (!wrapperRef.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, [open]);

  const display = activeDatabase || "Select database";
  const empty = databases.length === 0;

  return (
    <div className="relative" ref={wrapperRef}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className={cn(
          "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium transition",
          activeDatabase
            ? "border-accent/30 bg-accent-soft text-accent-ink"
            : "border-warning/40 bg-warning/10 text-warning",
        )}
      >
        <span className="text-[10px] uppercase tracking-wider opacity-70">Database</span>
        <span className="font-mono text-[11px]">{display}</span>
        <ChevronDown size={12} className="opacity-70" />
      </button>
      {open && (
        <div className="absolute right-0 z-30 mt-1 w-56 overflow-hidden rounded-md border border-surface-border bg-surface-raised shadow-lg">
          {empty ? (
            <div className="px-3 py-2 text-xs text-ink-dim">No databases visible.</div>
          ) : (
            <ul className="max-h-72 overflow-y-auto py-1">
              {databases.map((name) => {
                const isActive = name === activeDatabase;
                return (
                  <li key={name}>
                    <button
                      type="button"
                      onClick={() => activate.mutate(name)}
                      disabled={activate.isPending}
                      className={cn(
                        "flex w-full items-center justify-between gap-2 px-3 py-1.5 text-left text-xs hover:bg-surface-subtle disabled:opacity-50",
                        isActive && "bg-surface-subtle/60",
                      )}
                    >
                      <span className="truncate font-mono">{name}</span>
                      {isActive && <Check size={12} className="text-accent" />}
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
          {activate.isError && (
            <div className="border-t border-surface-border px-3 py-1.5 text-[11px] text-critical">
              {activate.error instanceof Error
                ? activate.error.message
                : "Activation failed."}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
