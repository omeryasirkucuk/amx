import { Outlet } from "react-router-dom";

import TopBar from "./TopBar";
import Sidebar from "./Sidebar";
import Footer from "./Footer";
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
  return (
    <div className="flex h-screen flex-col overflow-hidden bg-bg text-ink">
      <TopBar />
      <div className="flex flex-1 min-h-0">
        <aside
          className={cn(
            "min-h-0 overflow-hidden border-r border-border bg-surface-subtle/50 transition-all duration-fast",
            sidebarCollapsed ? "w-12" : "w-64",
          )}
        >
          <Sidebar collapsed={sidebarCollapsed} />
        </aside>
        <div className="flex flex-1 min-w-0 flex-col">
          <main className="flex-1 min-h-0 overflow-y-auto">
            <div className="mx-auto w-full max-w-[1280px] px-8 py-7">
              <Outlet />
            </div>
          </main>
          <div className="mx-auto w-full max-w-[1280px] px-8">
            <Footer />
          </div>
        </div>
      </div>
    </div>
  );
}
