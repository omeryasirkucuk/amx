// Cross-page Zustand store. Keeps the sidebar collapse state, the
// last-opened table's full scope (so the empty home page and command
// palette can deep-link "Resume where you left off"), and the active
// job IDs the progress panel polls.
//
// Each slice is tiny on purpose — TanStack Query owns server state;
// this is purely UI / navigation memory.

import { create } from "zustand";
import { persist, createJSONStorage } from "zustand/middleware";

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
  /** Imperative setter so a route can force the collapsed state on
   *  mount (e.g. the Landing page wants the sidebar tucked away by
   *  default; the user can still re-open it from the topbar). */
  setSidebarCollapsed: (next: boolean) => void;
  /** Distinct from sidebarCollapsed: on phone viewports the sidebar is
   *  a drawer (overlay + scrim) rather than an inline collapsed rail,
   *  so it needs its own open/closed flag. */
  mobileSidebarOpen: boolean;
  setMobileSidebarOpen: (next: boolean) => void;
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
  /** Per-session in-flight ask job id. Persisted via localStorage so
   *  navigating away from /ask (or reloading the tab) and coming back
   *  can reattach the SSE stream — the worker thread keeps running
   *  on the backend regardless of the SPA's state, and JobRegistry
   *  buffers events until a consumer drains them. Cleared on terminal
   *  events (job.done / failed / cancelled) inside AskChat. */
  askActiveJobBySession: Record<string, string>;
  setAskActiveJob: (sessionKey: string, jobId: string) => void;
  clearAskActiveJob: (sessionKey: string) => void;
}

export const useUi = create<UiSlice>()(
  persist(
    (set) => ({
      sidebarCollapsed: false,
      toggleSidebar: () =>
        set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
      setSidebarCollapsed: (next: boolean) => set({ sidebarCollapsed: next }),
      mobileSidebarOpen: false,
      setMobileSidebarOpen: (next: boolean) => set({ mobileSidebarOpen: next }),
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
      askActiveJobBySession: {},
      setAskActiveJob: (sessionKey, jobId) =>
        set((state) => ({
          askActiveJobBySession: {
            ...state.askActiveJobBySession,
            [sessionKey]: jobId,
          },
        })),
      clearAskActiveJob: (sessionKey) =>
        set((state) => {
          const next = { ...state.askActiveJobBySession };
          delete next[sessionKey];
          return { askActiveJobBySession: next };
        }),
    }),
    {
      name: "amx-studio-ui",
      storage: createJSONStorage(() => localStorage),
      // Only persist the keys that need to survive a reload. Sidebar
      // collapse state and last-opened breadcrumbs are intentionally
      // session-only — persisting them was never the intent of this
      // store. Persisting askActiveJobBySession lets a hard reload
      // (Cmd-R) still find an in-flight ask job.
      partialize: (state) => ({
        askActiveJobBySession: state.askActiveJobBySession,
      }),
    },
  ),
);
