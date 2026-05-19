// Shared render helpers for Pages component tests.
// Each test gets a fresh QueryClient + MemoryRouter so cross-test
// state can't leak through the React Query cache.

import { type ReactElement, type ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";
import { render, type RenderResult } from "@testing-library/react";

import { ToastProvider } from "../components/ui";

export function renderWithProviders(
  ui: ReactElement,
  options: { route?: string; client?: QueryClient } = {},
): RenderResult {
  const client =
    options.client ??
    new QueryClient({
      defaultOptions: {
        queries: { retry: false, gcTime: 0 },
        mutations: { retry: false },
      },
    });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[options.route ?? "/"]}>
        <ToastProvider>{ui}</ToastProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

export function withProviders(children: ReactNode) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return (
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <ToastProvider>{children}</ToastProvider>
      </MemoryRouter>
    </QueryClientProvider>
  );
}
