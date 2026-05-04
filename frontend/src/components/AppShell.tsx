import { Outlet } from "react-router-dom";

import TopBar from "./TopBar";
import Sidebar from "./Sidebar";
import { useUi } from "../lib/store";
import { cn } from "../lib/cn";

/**
 * App shell. TopBar carries primary nav and global context; sidebar
 * is the collapsible asset tree; main canvas hosts the routed page.
 * Standardized padding + a wider max-width than before so 13"+
 * monitors get more usable space.
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
          <div className="mx-auto w-full max-w-[1280px] px-8 py-7">
            <Outlet />
          </div>
        </main>
      </div>
    </div>
  );
}
