// Cross-page Zustand store. Keeps the sidebar collapse state, the
// last-opened schema/table pair (so the empty home page can deep-
// link "Resume where you left off"), and the active job IDs the
// progress panel polls.
//
// Each slice is tiny on purpose — TanStack Query owns server state;
// this is purely UI / navigation memory.

import { create } from "zustand";

interface UiSlice {
  sidebarCollapsed: boolean;
  toggleSidebar: () => void;
  lastOpenedSchema: string | null;
  lastOpenedTable: string | null;
  rememberOpenedTable: (schema: string | null, table: string | null) => void;
}

export const useUi = create<UiSlice>((set) => ({
  sidebarCollapsed: false,
  toggleSidebar: () =>
    set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
  lastOpenedSchema: null,
  lastOpenedTable: null,
  rememberOpenedTable: (schema, table) =>
    set({ lastOpenedSchema: schema, lastOpenedTable: table }),
}));
