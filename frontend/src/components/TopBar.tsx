import { Link, NavLink, useLocation, useParams } from "react-router-dom";
import { useMemo } from "react";
import {
  Activity,
  ChevronRight,
  Database,
  PanelLeft,
  ScrollText,
  Sparkles,
  History as HistoryIcon,
  Settings as SettingsIcon,
} from "lucide-react";

import { useUi } from "../lib/store";
import { cn } from "../lib/cn";
import IconButton from "./ui/IconButton";
import Logo from "./brand/Logo";
import PricingBadge from "./PricingBadge";

const navItems = [
  { to: "/", label: "Browse", icon: Database, end: true, match: ["/"] },
  { to: "/runs", label: "Runs", icon: HistoryIcon, match: ["/runs"] },
  { to: "/ask", label: "Ask", icon: Sparkles, match: ["/ask"] },
  { to: "/audit", label: "Audit", icon: ScrollText, match: ["/audit"] },
  { to: "/settings", label: "Settings", icon: SettingsIcon, match: ["/settings"] },
  { to: "/system", label: "System", icon: Activity, match: ["/system"] },
];

/**
 * Persistent top bar. Three regions, left to right:
 *   1. Sidebar toggle + brand + breadcrumb (where am I?)
 *   2. Primary nav (where can I go?)
 *
 * Profile / database scope was previously rendered here as pills.
 * It now lives only in the Sidebar's Profiles section so we don't
 * surface the same control twice.
 */
export default function TopBar() {
  const toggleSidebar = useUi((s) => s.toggleSidebar);
  const location = useLocation();
  const params = useParams();

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
          className="group flex items-baseline gap-1.5 rounded-md px-1.5 py-1 -mx-1 hover:bg-surface-subtle"
        >
          <Logo size={16} />
          <span className="text-sm font-medium text-ink">Studio</span>
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

        <div className="ml-auto flex items-center gap-2">
          <PricingBadge />
          <nav className="flex items-center gap-0.5">
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

  if (root === "db" || root === "cat") {
    const out: Crumb[] = [{ label: "Browse", to: "/" }];
    const scopeSeg = params.database ?? params.catalog;
    if (params.profile && scopeSeg) {
      out.push({
        label: params.profile,
      });
      out.push({
        label: scopeSeg,
        to: `/${root}/${params.profile}/${scopeSeg}`,
      });
    }
    if (params.schema && scopeSeg && params.profile) {
      out.push({
        label: params.schema,
        to: `/${root}/${params.profile}/${scopeSeg}/${params.schema}`,
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
