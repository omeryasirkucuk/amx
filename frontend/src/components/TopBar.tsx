import { Link, NavLink, useLocation, useParams } from "react-router-dom";
import { useEffect, useMemo, useState } from "react";
import {
  Activity,
  BarChart3,
  ChevronRight,
  PanelLeft,
  Pin,
  Sparkles,
  History as HistoryIcon,
  Settings as SettingsIcon,
} from "lucide-react";

import { useUi } from "../lib/store";
import { useIsMobile } from "../lib/useIsMobile";
import { cn } from "../lib/cn";
import { readPinnedCells } from "../lib/pinnedCells";
import IconButton from "./ui/IconButton";
import Logo from "./brand/Logo";
import PinnedCellsDrawer from "./PinnedCellsDrawer";
import CatalogFreshnessBadge from "./CatalogFreshnessBadge";
import PricingBadge from "./PricingBadge";
import MobileNavMenu from "./topbar/MobileNavMenu";

// 0.13: ``/`` is the calm Landing page (entry surface). The
// dashboard view that used to live there moved to ``/overview``;
// the nav link below points users at it for the token / cost
// breakdown. Browse no longer needs its own top-bar link because
// the sidebar tree is the browse surface and it's always visible.
const navItems = [
  { to: "/overview", label: "Overview", icon: BarChart3, match: ["/overview"] },
  {
    to: "/runs",
    label: "Runs",
    icon: HistoryIcon,
    match: ["/runs", "/runs/schedules"],
  },
  { to: "/ask", label: "Ask", icon: Sparkles, match: ["/ask"] },
  // Audit moved out of the top bar — it lives on the Runs page header
  // as a sibling of Schedules / Compare / New run because the apply
  // trail is a per-run artefact, not a top-level mode.
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
  const setMobileSidebarOpen = useUi((s) => s.setMobileSidebarOpen);
  const isMobile = useIsMobile();
  const location = useLocation();
  const params = useParams();

  function onToggle() {
    if (isMobile) setMobileSidebarOpen(true);
    else toggleSidebar();
  }

  // PR C — pinned-cells drawer. ``dbProfile`` is null at the TopBar
  // level (we don't have a per-page profile in scope here), so the
  // drawer reads the ``__global`` bucket — which is also the bucket
  // ``ResultRowItem`` writes to when run.db_profile is null. When
  // the active profile is known on the page side (e.g. RunDetail),
  // the writes land in that profile's bucket and the global drawer
  // simply doesn't see them; future iterations can lift the active
  // profile into a global store and pass it down here.
  const [pinnedOpen, setPinnedOpen] = useState(false);
  const [pinnedCount, setPinnedCount] = useState<number>(0);

  // Refresh the count badge whenever the pinned set changes. We also
  // refresh on mount so a fresh page load reflects whatever the user
  // pinned in a previous session.
  useEffect(() => {
    function refresh() {
      // Sum across all profile buckets so the badge reflects the
      // user's total pin count regardless of which profile they're
      // looking at. Cheap because localStorage is in-process.
      if (typeof window === "undefined") return;
      let total = 0;
      for (let i = 0; i < window.localStorage.length; i++) {
        const key = window.localStorage.key(i);
        if (!key || !key.startsWith("amx.compare.pinnedCells.")) continue;
        // readPinnedCells expects the profile slug (after the prefix).
        const profile = key.slice("amx.compare.pinnedCells.".length);
        total += readPinnedCells(profile === "__global" ? null : profile).length;
      }
      setPinnedCount(total);
    }
    refresh();
    window.addEventListener("amx:pinned-cells-changed", refresh);
    return () =>
      window.removeEventListener("amx:pinned-cells-changed", refresh);
  }, []);

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
          onClick={onToggle}
        />
        <Link
          to="/"
          aria-label="AMX Studio home"
          className="group flex items-center rounded-md px-1.5 py-1 -mx-1 hover:bg-surface-subtle"
        >
          <Logo size={22} suffix="Studio" />
        </Link>
        {crumbs.length > 0 && (
          <nav aria-label="Breadcrumb" className="hidden md:flex">
            <ol className="flex items-center gap-1 text-xs text-ink-dim">
              {crumbs.map((c, i) => (
                <li key={i} className="flex items-center gap-1">
                  <ChevronRight
                    size={14}
                    strokeWidth={2.5}
                    className="text-ink-muted"
                    aria-hidden="true"
                  />
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
          <button
            type="button"
            onClick={() => setPinnedOpen(true)}
            aria-label={`Open pinned-cells drawer (${pinnedCount} pinned)`}
            className={cn(
              "relative inline-flex h-7 items-center gap-1 rounded-md px-2 text-xs font-medium transition-colors",
              pinnedCount > 0
                ? "text-accent hover:bg-surface-subtle"
                : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
            )}
          >
            <Pin size={13} />
            {pinnedCount > 0 && (
              <span className="rounded-full bg-accent px-1.5 text-[10px] font-bold text-accent-soft">
                {pinnedCount}
              </span>
            )}
          </button>
          <CatalogFreshnessBadge />
          <PricingBadge />
          <nav className="hidden sm:flex items-center gap-0.5">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
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
          <MobileNavMenu items={navItems} />
        </div>
      </div>
      <PinnedCellsDrawer
        open={pinnedOpen}
        onClose={() => setPinnedOpen(false)}
      />
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
      // The profile crumb links back to Landing because there is no
      // profile-level page in the SPA — ``/db/:profile`` and
      // ``/cat/:profile`` both redirect to ``/`` (see App.tsx). The
      // Landing page hosts the sidebar tree, which is the natural
      // place to pick a different database / catalog under the same
      // profile, so the redirect is semantic rather than a dead end.
      out.push({
        label: params.profile,
        to: "/",
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
