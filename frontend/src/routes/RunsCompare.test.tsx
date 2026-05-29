import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";

import RunsCompare from "./RunsCompare";
import { renderWithProviders } from "../test/render";

const originalFetch = globalThis.fetch;
let fetchCalls: string[] = [];

beforeEach(() => {
  fetchCalls = [];
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = typeof input === "string" ? input : input.toString();
    fetchCalls.push(url);
    // The picker only needs the recent-runs feed to render its chips;
    // an empty feed keeps the row renderer out of the assertion.
    return new Response(
      JSON.stringify({
        runs: [],
        count: 0,
        pending_review_total: 0,
        command_filter: null,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } },
    );
  }) as unknown as typeof fetch;
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("RunsCompare picker", () => {
  it("requests only the comparable run feed", async () => {
    renderWithProviders(<RunsCompare />, { route: "/compare" });
    await waitFor(() => expect(fetchCalls.some((u) => u.includes("/api/history/runs"))).toBe(true));
    const runsCall = fetchCalls.find((u) => u.includes("/api/history/runs"));
    expect(runsCall).toContain("command=comparable");
  });

  it("offers schedule as a kind chip and drops the ask chip", async () => {
    renderWithProviders(<RunsCompare />, { route: "/compare" });
    await waitFor(() => expect(screen.getByText("schedule")).toBeInTheDocument());
    expect(screen.queryByText("ask")).not.toBeInTheDocument();
  });
});
