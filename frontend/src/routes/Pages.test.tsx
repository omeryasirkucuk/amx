import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";

import PagesRoute from "./Pages";
import { renderWithProviders } from "../test/render";

const originalFetch = globalThis.fetch;

beforeEach(() => {
  globalThis.fetch = vi.fn(
    async () =>
      new Response(JSON.stringify([]), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
  ) as unknown as typeof fetch;
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("Pages route", () => {
  it("renders the empty state when no pages exist", async () => {
    renderWithProviders(<PagesRoute />, { route: "/pages" });
    await waitFor(() =>
      expect(screen.getByText(/No pages yet/i)).toBeInTheDocument(),
    );
    expect(
      screen.getByRole("button", { name: /Create your first page/i }),
    ).toBeInTheDocument();
  });
});
