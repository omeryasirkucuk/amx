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
      return { profiles: [{ name: "local-pg", backend: "postgres" }] };
    }
    if (url.includes("/api/profiles/docs")) {
      return { profiles: [] };
    }
    if (url.includes("/api/lineage")) {
      return { artifacts: [], count: 0 };
    }
    return {};
  });
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("AssetPicker", () => {
  it("renders DB profile rows fetched from /api/profiles/db", async () => {
    const onChange = vi.fn();
    renderWithProviders(<AssetPicker value={[]} onChange={onChange} />);
    // The picker renders both the md+ tab panel and the sm accordion so the
    // profile name may appear more than once in the test DOM; getAllByText
    // tolerates that without changing the production behaviour.
    await waitFor(() =>
      expect(screen.getAllByText("local-pg").length).toBeGreaterThan(0),
    );
  });

  it("toggles a DB profile selection on click", async () => {
    const onChange = vi.fn();
    renderWithProviders(<AssetPicker value={[]} onChange={onChange} />);
    await waitFor(() =>
      expect(screen.getAllByText("local-pg").length).toBeGreaterThan(0),
    );
    const rows = screen.getAllByText("local-pg");
    fireEvent.click(rows[0]);
    expect(onChange).toHaveBeenCalledWith([
      { kind: "db_profile", ref: "local-pg" },
    ]);
  });
});
