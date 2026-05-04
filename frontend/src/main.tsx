import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";

import App from "./App";
import { captureTokenFromUrl } from "./lib/auth";
import { applyTheme, readStoredTheme } from "./lib/theme";
import "./styles/index.css";

// Capture the bearer token before the React tree mounts so the very
// first /api/* fetch already has it. The launcher embeds the token
// as `?t=<token>` on the URL the browser tab opens.
captureTokenFromUrl();

// Apply the persisted theme synchronously before React paints so the
// page never flashes the wrong palette on dark-mode users.
applyTheme(readStoredTheme());

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
