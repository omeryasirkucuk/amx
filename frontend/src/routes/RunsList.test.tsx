import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { fireEvent, screen, waitFor } from "@testing-library/react";

import RunsList from "./RunsList";
import { renderWithProviders } from "../test/render";

const originalFetch = globalThis.fetch;
let fetchUrls: string[] = [];

function runsResponse() {
  return {
    command_filter: null,
    runs: [
      {
        id: 186,
        command: "rerun",
        status: "success",
        started_at: 1_700_000_000,
        duration_sec: 34.5,
        scope_json: { nyctaxi: ["trips"] },
        llm_model: "claude-opus-4-6",
        db_profile: "dbr-oyk",
      },
    ],
    count: 1,
    total: 186,
    has_more: true,
    kind_counts: {
      all: 186,
      analyze: 6,
      generate: 6,
      rerun: 1,
      ask: 169,
      schedule: 4,
      other: 0,
    },
    status_counts: { success: 178, failed: 4, running: 0, cancelled: 0 },
    pending_review_total: 0,
  };
}

beforeEach(() => {
  fetchUrls = [];
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = typeof input === "string" ? input : input.toString();
    fetchUrls.push(url);
    return new Response(JSON.stringify(runsResponse()), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }) as unknown as typeof fetch;
  // RunsList persists the kind filter; start clean so the default is "all".
  window.localStorage.clear();
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("RunsList server-side pagination", () => {
  it("requests the runs feed with server-side paging params", async () => {
    renderWithProviders(<RunsList />, { route: "/runs" });
    await waitFor(() =>
      expect(fetchUrls.some((u) => u.includes("/api/history/runs"))).toBe(true),
    );
    const call = fetchUrls.find((u) => u.includes("/api/history/runs"))!;
    expect(call).toContain("limit=50");
    expect(call).toContain("offset=0");
    // command=all so the kind facet covers every bucket.
    expect(call).toContain("command=all");
  });

  it("shows true totals from the server facets, not the page length", async () => {
    renderWithProviders(<RunsList />, { route: "/runs" });
    // Only 1 row is on the page, but the pager + KIND "All" chip reflect 186
    // (the value appears in both, hence getAllByText).
    await waitFor(() =>
      expect(screen.getAllByText("186").length).toBeGreaterThan(0),
    );
    // The Ask kind chip count comes from kind_counts (169), not the page.
    expect(screen.getByText("169")).toBeInTheDocument();
  });

  it("sends the search query to the server (debounced)", async () => {
    renderWithProviders(<RunsList />, { route: "/runs" });
    await waitFor(() =>
      expect(fetchUrls.some((u) => u.includes("/api/history/runs"))).toBe(true),
    );
    fireEvent.change(
      screen.getByPlaceholderText("Search by id, command, or scope…"),
      { target: { value: "nyctaxi" } },
    );
    await waitFor(() =>
      expect(fetchUrls.some((u) => u.includes("q=nyctaxi"))).toBe(true),
    );
  });
});
