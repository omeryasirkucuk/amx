import { Link, NavLink, useLocation, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  Activity,
  Check,
  ChevronDown,
  ChevronRight,
  Database,
  PanelLeft,
  Sparkles,
  History as HistoryIcon,
  Settings as SettingsIcon,
} from "lucide-react";

import { api } from "../lib/api";
import { useUi } from "../lib/store";
import { cn } from "../lib/cn";
import CommandPalette from "./CommandPalette";
import IconButton from "./ui/IconButton";
import Logo from "./brand/Logo";
import StudioMark from "./brand/StudioMark";
import ProfilePicker from "./topbar/ProfilePicker";

const navItems = [
  { to: "/", label: "Browse", icon: Database, end: true, match: ["/"] },
  { to: "/runs", label: "Runs", icon: HistoryIcon, match: ["/runs"] },
  { to: "/ask", label: "Ask", icon: Sparkles, match: ["/ask"] },
  { to: "/settings", label: "Settings", icon: SettingsIcon, match: ["/settings"] },
  { to: "/system", label: "System", icon: Activity, match: ["/system"] },
];

/**
 * Persistent top bar. Three regions, left to right:
 *   1. Sidebar toggle + brand + breadcrumb (where am I?)
 *   2. Primary nav (where can I go?)
 *   3. Context pills (DB / LLM / catalog) + ⌘K + theme toggle
 *      (what scope am I working in?)
 */
export default function TopBar() {
  const toggleSidebar = useUi((s) => s.toggleSidebar);
  const location = useLocation();
  const params = useParams();

  const { data: ctx } = useQuery({
    queryKey: ["context"],
    queryFn: () => api.context(),
  });
  const catalogs = useQuery({
    queryKey: ["live-catalogs"],
    queryFn: () => api.liveCatalogs(),
    retry: false,
  });
  const databases = useQuery({
    queryKey: ["live-databases"],
    queryFn: () => api.liveDatabases(),
    retry: false,
    enabled: catalogs.data ? !catalogs.data.supports_catalogs : false,
  });

  const crumbs = useMemo(
    () => buildCrumbs(location.pathname, params),
    [location.pathname, params],
  );

  return (
    <header className="border-b border-border bg-surface-raised">
      <div className="flex h-12 items-center gap-3 px-4">
        <IconButton
          icon={<PanelLeft size={16} />}
          label="Toggle sidebar"
          size="sm"
          onClick={toggleSidebar}
        />
        <Link
          to="/"
          aria-label="AMX Studio home"
          className="group flex items-end gap-1.5 rounded-md px-1.5 py-1 -mx-1 hover:bg-surface-subtle"
        >
          <Logo size={16} />
          <StudioMark size={10} />
        </Link>
        {crumbs.length > 0 && (
          <nav aria-label="Breadcrumb" className="hidden md:flex">
            <ol className="flex items-center gap-1 text-xs text-ink-dim">
              {crumbs.map((c, i) => (
                <li key={i} className="flex items-center gap-1">
                  <ChevronRight size={12} className="text-border-strong" />
                  {c.to && i < crumbs.length - 1 ? (
                    <Link
                      to={c.to}
                      className="rounded text-ink-muted hover:text-ink"
                    >
                      {c.label}
                    </Link>
                  ) : (
                    <span
                      aria-current={i === crumbs.length - 1 ? "page" : undefined}
                      className={
                        i === crumbs.length - 1
                          ? "max-w-[16rem] truncate font-medium text-ink"
                          : "max-w-[10rem] truncate"
                      }
                      title={typeof c.label === "string" ? c.label : undefined}
                    >
                      {c.label}
                    </span>
                  )}
                </li>
              ))}
            </ol>
          </nav>
        )}

        <nav className="ml-auto flex items-center gap-0.5">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                cn(
                  "flex h-7 items-center gap-1.5 rounded-md px-2.5 text-xs font-medium transition-colors duration-fast",
                  isActive
                    ? "bg-accent-soft text-accent-ink"
                    : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
                )
              }
            >
              <item.icon size={13} />
              {item.label}
            </NavLink>
          ))}
        </nav>

        <span className="mx-1 h-5 w-px bg-border" aria-hidden="true" />

        <div className="flex items-center gap-1.5">
          <ProfilePicker
            kind="db"
            label="DB"
            activeName={ctx?.active_db_profile ?? null}
            tooltip={ctx?.db_backend ?? undefined}
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
          <ProfilePicker
            kind="llm"
            label="LLM"
            activeName={ctx?.active_llm_profile ?? null}
            tooltip={ctx?.llm_model ?? undefined}
          />
          <CommandPalette />
        </div>
      </div>
    </header>
  );
}

interface Crumb {
  label: string;
  to?: string;
}

function buildCrumbs(
  pathname: string,
  params: Record<string, string | undefined>,
): Crumb[] {
  if (pathname === "/" || pathname === "") return [];
  const segs = pathname.split("/").filter(Boolean);
  const root = segs[0];

  if (root === "db") {
    const out: Crumb[] = [{ label: "Browse", to: "/" }];
    if (params.schema) {
      out.push({
        label: params.schema,
        to: `/db/${params.profile ?? "active"}/${params.schema}`,
      });
    }
    if (params.table) {
      out.push({ label: params.table });
    }
    return out;
  }
  if (root === "runs") {
    const out: Crumb[] = [{ label: "Runs", to: "/runs" }];
    if (segs[1] === "new") out.push({ label: "New run" });
    else if (segs[1] === "compare") out.push({ label: "Compare" });
    else if (params.runId) out.push({ label: `#${params.runId}` });
    return out;
  }
  const labels: Record<string, string> = {
    ask: "Ask",
    pending: "Pending",
    settings: "Settings",
    system: "System",
  };
  if (root && labels[root]) return [{ label: labels[root] }];
  return [];
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
          "inline-flex h-7 items-center gap-1.5 rounded-md border px-2 text-[11px] font-medium transition-colors duration-fast",
          activeCatalog
            ? "border-accent/20 bg-accent-soft text-accent-ink hover:bg-accent-soft/80"
            : "border-warning/40 bg-warning-soft text-warning hover:bg-warning-soft/80",
        )}
      >
        <span className="text-[10px] uppercase tracking-wider opacity-70">
          Catalog
        </span>
        <span className="max-w-[8rem] truncate font-mono">{display}</span>
        <ChevronDown size={12} className="opacity-70" />
      </button>
      {open && (
        <div className="absolute right-0 z-30 mt-1 w-56 overflow-hidden rounded-md border border-border bg-surface-raised shadow-md animate-fade-in">
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
            <div className="border-t border-border px-3 py-1.5 text-[11px] text-critical">
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
          "inline-flex h-7 items-center gap-1.5 rounded-md border px-2 text-[11px] font-medium transition-colors duration-fast",
          activeDatabase
            ? "border-accent/20 bg-accent-soft text-accent-ink hover:bg-accent-soft/80"
            : "border-warning/40 bg-warning-soft text-warning hover:bg-warning-soft/80",
        )}
      >
        <span className="text-[10px] uppercase tracking-wider opacity-70">
          Database
        </span>
        <span className="max-w-[8rem] truncate font-mono">{display}</span>
        <ChevronDown size={12} className="opacity-70" />
      </button>
      {open && (
        <div className="absolute right-0 z-30 mt-1 w-56 overflow-hidden rounded-md border border-border bg-surface-raised shadow-md animate-fade-in">
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
            <div className="border-t border-border px-3 py-1.5 text-[11px] text-critical">
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
