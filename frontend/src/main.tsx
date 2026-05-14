import React from "react";
import ReactDOM from "react-dom/client";
import {
  MutationCache,
  QueryCache,
  QueryClient,
  QueryClientProvider,
} from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";

import App from "./App";
import { captureTokenFromUrl, consumeDeeplink } from "./lib/auth";
import { publishQueryError } from "./lib/queryErrorBus";
import "./styles/index.css";

// Capture the bearer token before the React tree mounts so the very
// first /api/* fetch already has it. The launcher embeds the token
// as `?t=<token>` on the URL the browser tab opens.
captureTokenFromUrl();

// Honor the deep link a previous tab session stashed before it
// bounced through ``/`` to refresh the bearer token (see the 401
// branch in ``lib/api.ts``). Without this, a user who landed on
// ``/ask`` right after a Studio restart would re-capture the token
// but lose the page they were trying to read. ``replaceState`` keeps
// the history clean — Back / Forward still walk the user's real
// path, not the home bounce.
(() => {
  const target = consumeDeeplink();
  if (!target) return;
  // Only redirect if we actually landed on home — if BrowserRouter
  // already has a non-home location (e.g. user typed the deep link
  // again themselves) leave it alone.
  if (window.location.pathname !== "/" || window.location.search) return;
  window.history.replaceState({}, document.title, target);
})();

// Strip the stale-bundle reload bust query if we just landed from
// one. Keeps the URL clean for Back / Forward / Bookmark and the
// session flag fresh so a future stale event still gets one
// auto-reload chance.
(() => {
  const params = new URLSearchParams(window.location.search);
  if (params.has("__amx_reload")) {
    params.delete("__amx_reload");
    sessionStorage.removeItem("amx-stale-reload-fired");
    const trimmed = params.toString();
    const next =
      window.location.pathname + (trimmed ? `?${trimmed}` : "") + window.location.hash;
    window.history.replaceState({}, document.title, next);
  }
})();

// Auto-recovery for stale chunk references. After AMX Studio is
// upgraded, the old ``index.html`` may still live in the browser
// cache; its dynamic ``import()`` calls then point at JS chunks
// whose hashes no longer exist on disk (the new build replaced
// them). The browser surfaces this as a hard
// ``TypeError: Failed to fetch dynamically imported module``,
// which used to crash the page and force the user into a manual
// hard-refresh. Listen for the two flavours of the error (the Vite
// custom event + the generic unhandled rejection from a route's
// lazy import) and reload once with a query bust so the next
// request returns the fresh ``index.html`` and its current chunk
// hashes. ``sessionStorage`` keeps us from looping if the failure
// is something other than a stale bundle.
function _scheduleStaleBundleReload(reason: string): void {
  const flagKey = "amx-stale-reload-fired";
  if (sessionStorage.getItem(flagKey)) {
    // Already tried once this session; let the error surface so the
    // user (or the ErrorBoundary copy-to-clipboard flow) sees it.
    return;
  }
  sessionStorage.setItem(flagKey, "1");
  console.warn("[amx] stale bundle detected (%s); reloading once", reason);
  // Cache-busting query string nudges any intermediary that may be
  // serving a stale ``index.html`` (corporate proxy, browser
  // memory-cache) to revalidate. Replace so the bust query does not
  // pile up on Back history.
  const url = new URL(window.location.href);
  url.searchParams.set("__amx_reload", String(Date.now()));
  window.location.replace(url.toString());
}

window.addEventListener("vite:preloadError", (event) => {
  event.preventDefault();
  _scheduleStaleBundleReload("vite:preloadError");
});

window.addEventListener("unhandledrejection", (event) => {
  const message = String(event.reason?.message ?? event.reason ?? "");
  if (
    message.includes("Failed to fetch dynamically imported module") ||
    message.includes("error loading dynamically imported module")
  ) {
    event.preventDefault();
    _scheduleStaleBundleReload("dynamic import rejection");
  }
});

// Treat /api/* responses as cacheable for 30s — most pages re-render
// repeatedly while the user is exploring (asset tree expansions,
// dashboard refreshes). 30s is short enough that a /sync run reflects
// quickly, long enough that scrubbing doesn't hammer the local
// uvicorn.
// QueryCache + MutationCache surface every failure that wasn't opted
// out via `meta: { silentError: true }` through the global toast
// listener (see `<QueryErrorListener />` in App.tsx). Routes that
// already render an inline error banner (Audit, Ask, RunsCompare cell
// deep-dives) keep that flag set so the toast doesn't duplicate.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
  queryCache: new QueryCache({
    onError: (error, query) => {
      if (query.meta?.silentError) return;
      publishQueryError({
        error,
        source: "query",
        scope: Array.isArray(query.queryKey)
          ? query.queryKey.map((k) => String(k)).join(":")
          : String(query.queryKey),
      });
    },
  }),
  mutationCache: new MutationCache({
    onError: (error, _variables, _context, mutation) => {
      if (mutation.meta?.silentError) return;
      publishQueryError({ error, source: "mutation" });
    },
  }),
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </React.StrictMode>,
);

// Register the offline-fallback Service Worker.
//
// The SW caches a single ``/offline.html`` page on install and serves
// it whenever a navigation request fails — i.e. when the AMX CLI
// process backing this tab stops (Ctrl-C / crash / terminal closed).
// Without this, the browser falls to its built-in "This site can't be
// reached" page which gives the user no clue about the right next
// step (re-run ``amx /studio`` in a terminal).
//
// Production-only: Vite's dev server has its own HMR machinery and
// caching the dev bundle through a SW makes the workflow brittle.
// ``import.meta.env.PROD`` is true in any ``vite build`` output.
if (import.meta.env.PROD && "serviceWorker" in navigator) {
  // Defer registration until after the first paint so the SW
  // install doesn't compete with the main bundle's network budget.
  window.addEventListener("load", () => {
    navigator.serviceWorker
      .register("/sw.js")
      .catch((err) => {
        // SW registration failures are recoverable — Studio still works
        // online, the user just loses the offline fallback. Log to the
        // console so a power user can spot it; don't bother the
        // ErrorBoundary or the toast layer.
        // eslint-disable-next-line no-console
        console.warn("[amx] service worker registration failed:", err);
      });
  });
}
