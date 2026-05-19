import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { screen, waitFor, fireEvent } from "@testing-library/react";

import AssetPicker from "./AssetPicker";
import { renderWithProviders } from "../../test/render";

const originalFetch = globalThis.fetch;

function mockFetch(handler: (url: string) => unknown) {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = typeof input === "string" ? input : input.toString();
    const body = handler(url);
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }) as unknown as typeof fetch;
}

beforeEach(() => {
  mockFetch((url) => {
    if (url.includes("/api/profiles/db")) {
      return { profiles: [{ name: "local-pg", backend: "postgresql" }] };
    }
    if (url.includes("/api/profiles/docs")) {
      return { profiles: [] };
    }
    if (url.includes("/api/lineage")) {
      return { artifacts: [], count: 0 };
    }
    if (url.includes("/api/db/cache/tree/databases")) {
      return { items: [{ name: "sales_db", last_synced_at: 0 }], synced: true };
    }
    return {};
  });
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("AssetPicker", () => {
  it("renders the DB profile name fetched from /api/profiles/db", async () => {
    const onChange = vi.fn();
    renderWithProviders(<AssetPicker value={[]} onChange={onChange} />);
    // The picker renders both the md+ tab panel and the sm accordion so the
    // profile name may appear more than once in the test DOM; getAllByText
    // tolerates that without changing the production behaviour.
    await waitFor(() =>
      expect(screen.getAllByText("local-pg").length).toBeGreaterThan(0),
    );
  });

  it("expands a profile and selects a database entity", async () => {
    const onChange = vi.fn();
    renderWithProviders(<AssetPicker value={[]} onChange={onChange} />);
    await waitFor(() =>
      expect(screen.getAllByText("local-pg").length).toBeGreaterThan(0),
    );
    // Click the profile row to expand the tree.
    fireEvent.click(screen.getAllByText("local-pg")[0]);
    // Database row appears after /api/live/databases resolves.
    await waitFor(() =>
      expect(screen.getAllByText("sales_db").length).toBeGreaterThan(0),
    );
    // Clicking the database label toggles a db_database selection
    // scoped under the active profile.
    fireEvent.click(screen.getAllByText("sales_db")[0]);
    expect(onChange).toHaveBeenCalledWith([
      { kind: "db_database", ref: "local-pg/sales_db" },
    ]);
  });
});
