import { Link, NavLink } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
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

const navItems = [
  { to: "/", label: "Browse", icon: Database, end: true },
  { to: "/runs", label: "Runs", icon: HistoryIcon },
  { to: "/ask", label: "Ask", icon: Sparkles },
  { to: "/pending", label: "Pending", icon: Inbox },
  { to: "/settings", label: "Settings", icon: SettingsIcon },
];

// Persistent top bar. Renders the active DB / LLM "pills" so the
// user always sees the current scope, plus the primary nav.
export default function TopBar() {
  const toggleSidebar = useUi((s) => s.toggleSidebar);
  const { data: ctx } = useQuery({
    queryKey: ["context"],
    queryFn: () => api.context(),
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
