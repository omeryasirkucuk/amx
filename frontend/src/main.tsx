import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";

import App from "./App";
import { captureTokenFromUrl } from "./lib/auth";
import "./styles/index.css";

// Capture the bearer token before the React tree mounts so the very
// first /api/* fetch already has it. The launcher embeds the token
// as `?t=<token>` on the URL the browser tab opens.
captureTokenFromUrl();

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
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
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
