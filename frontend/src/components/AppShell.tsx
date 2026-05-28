import { Outlet, useLocation } from "react-router-dom";

import ErrorBoundary from "./ErrorBoundary";
import TopBar from "./TopBar";
import Sidebar from "./Sidebar";
import Footer from "./Footer";
import MobileSidebar from "./MobileSidebar";
import BackfillBanner from "./BackfillBanner";
import { useUi } from "../lib/store";
import { cn } from "../lib/cn";

/**
 * App shell. TopBar carries primary nav and global context; sidebar
 * is the collapsible asset tree; main canvas hosts the routed page.
 * Footer sits beside the sidebar in a fixed slot at the bottom of the
 * viewport so it stays visible regardless of how tall the routed page
 * is — previously it lived inside the scrolling main canvas, which
 * pushed it below the fold on long pages.
 */
export default function AppShell() {
  const sidebarCollapsed = useUi((s) => s.sidebarCollapsed);
  const location = useLocation();
  return (
    <div className="flex h-screen flex-col overflow-hidden bg-bg text-ink">
      <TopBar />
      <BackfillBanner />
      <div className="flex flex-1 min-h-0">
        <aside
          className={cn(
            "hidden sm:block min-h-0 overflow-hidden border-r border-border bg-surface-subtle/50 transition-all duration-fast",
            sidebarCollapsed ? "w-12" : "w-64",
          )}
        >
          <Sidebar collapsed={sidebarCollapsed} />
        </aside>
        <MobileSidebar />
        <div className="flex flex-1 min-w-0 flex-col">
          <main className="flex-1 min-h-0 overflow-y-auto">
            <div className="mx-auto w-full max-w-[1280px] px-4 py-4 sm:px-6 sm:py-6 md:px-8 md:py-7">
              {/* Per-route boundary: a crash in the routed page renders a
                  contained fallback here while the top bar + sidebar stay
                  interactive. Keyed on pathname so navigating away clears it. */}
              <ErrorBoundary scoped resetKey={location.pathname}>
                <Outlet />
              </ErrorBoundary>
            </div>
          </main>
          <div className="mx-auto w-full max-w-[1280px] px-4 sm:px-6 md:px-8">
            <Footer />
          </div>
        </div>
      </div>
    </div>
  );
}
