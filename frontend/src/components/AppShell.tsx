import { Outlet } from "react-router-dom";

import TopBar from "./TopBar";
import Sidebar from "./Sidebar";
import Footer from "./Footer";
import { useUi } from "../lib/store";
import { cn } from "../lib/cn";

/**
 * App shell. TopBar carries primary nav and global context; sidebar
 * is the collapsible asset tree; main canvas hosts the routed page.
 * Standardized padding + a wider max-width than before so 13"+
 * monitors get more usable space. Footer sits inside the main
 * canvas (not below the sidebar) so it scrolls with the content.
 */
export default function AppShell() {
  const sidebarCollapsed = useUi((s) => s.sidebarCollapsed);
  return (
    <div className="flex min-h-screen flex-col bg-bg text-ink">
      <TopBar />
      <div className="flex flex-1 min-h-0">
        <aside
          className={cn(
            "border-r border-border bg-surface-subtle/50 transition-all duration-fast",
            sidebarCollapsed ? "w-12" : "w-64",
          )}
        >
          <Sidebar collapsed={sidebarCollapsed} />
        </aside>
        <main className="flex-1 min-w-0 overflow-y-auto">
          <div className="mx-auto flex min-h-full w-full max-w-[1280px] flex-col px-8 py-7">
            <div className="flex-1">
              <Outlet />
            </div>
            <Footer />
          </div>
        </main>
      </div>
    </div>
  );
}
