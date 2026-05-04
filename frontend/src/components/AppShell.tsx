import { Outlet } from "react-router-dom";

import TopBar from "./TopBar";
import Sidebar from "./Sidebar";
import { useUi } from "../lib/store";
import { cn } from "../lib/cn";

// 3-pane IDE shell. Left rail (asset tree + nav), center canvas,
// right panel placeholder (Ask + pending + activity tabs land here in
// PR-D / PR-E).
export default function AppShell() {
  const sidebarCollapsed = useUi((s) => s.sidebarCollapsed);
  return (
    <div className="flex min-h-screen flex-col bg-surface text-ink">
      <TopBar />
      <div className="flex flex-1 min-h-0">
        <aside
          className={cn(
            "border-r border-surface-border bg-surface-subtle/60 transition-all duration-150 ease-out",
            sidebarCollapsed ? "w-14" : "w-72",
          )}
        >
          <Sidebar collapsed={sidebarCollapsed} />
        </aside>
        <main className="flex-1 min-w-0 overflow-y-auto">
          <div className="mx-auto w-full max-w-6xl px-8 py-8">
            <Outlet />
          </div>
        </main>
      </div>
    </div>
  );
}
