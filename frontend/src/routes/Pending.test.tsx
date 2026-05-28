import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { fireEvent, screen, waitFor } from "@testing-library/react";

import Pending from "./Pending";
import { renderWithProviders } from "../test/render";

const originalFetch = globalThis.fetch;
const originalEventSource = globalThis.EventSource;

interface Call {
  url: string;
  method: string;
}
let calls: Call[] = [];

beforeEach(() => {
  calls = [];
  // jsdom has no EventSource; JobProgress opens one after apply starts.
  class FakeEventSource {
    close(): void {}
    addEventListener(): void {}
    removeEventListener(): void {}
    onmessage: ((ev: MessageEvent) => void) | null = null;
    onerror: ((ev: Event) => void) | null = null;
  }
  globalThis.EventSource = FakeEventSource as unknown as typeof EventSource;

  globalThis.fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString();
    const method = (init?.method ?? "GET").toUpperCase();
    calls.push({ url, method });

    if (url.endsWith("/api/pending") && method === "GET") {
      return jsonResponse({
        count: 2,
        pending: [
          row(0, "orders", "id"),
          row(1, "orders", "status"),
        ],
      });
    }
    if (url.endsWith("/api/pending/preview")) {
      return jsonResponse({
        count: 2,
        events: [
          previewEvent(0, "orders", "id"),
          previewEvent(1, "orders", "status"),
        ],
      });
    }
    if (url.endsWith("/api/pending/apply")) {
      return jsonResponse({ job_id: "job-1" });
    }
    return jsonResponse({});
  }) as unknown as typeof fetch;
});

afterEach(() => {
  globalThis.fetch = originalFetch;
  globalThis.EventSource = originalEventSource;
});

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

function row(idx: number, table: string, column: string) {
  return {
    idx,
    schema: "public",
    table,
    column,
    final_description: `desc for ${column}`,
    confidence: "high",
    source: "profile",
    asset_kind: "column",
    result_id: idx + 1,
    alternatives: [],
    logprob_score: null,
  };
}

function previewEvent(idx: number, table: string, column: string) {
  return {
    idx,
    schema: "public",
    table,
    column,
    asset_kind: "column",
    new_comment: `desc for ${column}`,
    sql_template: `COMMENT ON COLUMN public.${table}.${column} IS 'desc for ${column}';`,
  };
}

function applyCalls(): Call[] {
  return calls.filter((c) => c.url.endsWith("/api/pending/apply"));
}

describe("Pending — Apply safety gate", () => {
  it("does not write to the DB on the Apply click; it opens the dry-run preview first", async () => {
    renderWithProviders(<Pending />, { route: "/pending" });

    const applyBtn = await screen.findByRole("button", { name: /Apply \(2\)/i });
    fireEvent.click(applyBtn);

    // The preview (dry-run) is fetched and the modal appears...
    await waitFor(() =>
      expect(calls.some((c) => c.url.endsWith("/api/pending/preview"))).toBe(true),
    );
    await screen.findByText(/Apply preview/i);
    // ...but NOTHING has been written yet.
    expect(applyCalls()).toHaveLength(0);
  });

  it("writes only after confirming inside the preview modal", async () => {
    renderWithProviders(<Pending />, { route: "/pending" });

    fireEvent.click(await screen.findByRole("button", { name: /Apply \(2\)/i }));
    // The modal's confirm button is labelled with the live-database write.
    const confirmBtn = await screen.findByRole("button", {
      name: /Apply 2 statements to the live database/i,
    });
    expect(applyCalls()).toHaveLength(0);

    fireEvent.click(confirmBtn);
    await waitFor(() => expect(applyCalls()).toHaveLength(1));
    expect(applyCalls()[0].method).toBe("POST");
  });
});
