// Cross-page Zustand store. Keeps the sidebar collapse state, the
// last-opened table's full scope (so the empty home page and command
// palette can deep-link "Resume where you left off"), and the active
// job IDs the progress panel polls.
//
// Each slice is tiny on purpose — TanStack Query owns server state;
// this is purely UI / navigation memory.

import { create } from "zustand";

interface LastOpened {
  profile: string;
  database?: string;
  catalog?: string;
  schema: string;
  table: string;
}

interface UiSlice {
  sidebarCollapsed: boolean;
  toggleSidebar: () => void;
  lastOpenedSchema: string | null;
  lastOpenedTable: string | null;
  /** Full scope of the most recently viewed table (null until the
   *  user navigates into one). Powers the command palette's
   *  "Reopen …" entry across multi-profile layouts. */
  lastOpened: LastOpened | null;
  rememberOpenedTable: (schema: string | null, table: string | null) => void;
  rememberOpenedScope: (last: LastOpened) => void;
  /** Per-chat-session sticky DB scope for /ask. Keyed by session id;
   *  ``null`` value means "all profiles". A new chat ("+ New") drops
   *  the entry so the next session starts clean. */
  askScopeBySession: Record<string, string[] | null>;
  setAskScope: (sessionKey: string, scope: string[] | null) => void;
  clearAskScope: (sessionKey: string) => void;
}

export const useUi = create<UiSlice>((set) => ({
  sidebarCollapsed: false,
  toggleSidebar: () =>
    set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
  lastOpenedSchema: null,
  lastOpenedTable: null,
  lastOpened: null,
  rememberOpenedTable: (schema, table) =>
    set({ lastOpenedSchema: schema, lastOpenedTable: table }),
  rememberOpenedScope: (last) =>
    set({
      lastOpened: last,
      lastOpenedSchema: last.schema,
      lastOpenedTable: last.table,
    }),
  askScopeBySession: {},
  setAskScope: (sessionKey, scope) =>
    set((state) => ({
      askScopeBySession: { ...state.askScopeBySession, [sessionKey]: scope },
    })),
  clearAskScope: (sessionKey) =>
    set((state) => {
      const next = { ...state.askScopeBySession };
      delete next[sessionKey];
      return { askScopeBySession: next };
    }),
}));
