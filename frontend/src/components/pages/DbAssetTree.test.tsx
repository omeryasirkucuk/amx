import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { screen, waitFor, fireEvent } from "@testing-library/react";

import DbAssetTree from "./DbAssetTree";
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
      return {
        profiles: [
          { name: "pg_prod", backend: "postgresql" },
          { name: "dbr_main", backend: "databricks" },
        ],
      };
    }
    if (url.includes("/api/db/cache/tree/databases?profile=pg_prod")) {
      return {
        items: [{ name: "orders_db", last_synced_at: 0 }],
        synced: true,
      };
    }
    if (url.includes("/api/db/cache/tree/databases?profile=dbr_main")) {
      return { items: [{ name: "main", last_synced_at: 0 }], synced: true };
    }
    if (
      url.includes("/api/db/cache/tree/schemas") &&
      url.includes("profile=pg_prod")
    ) {
      return { items: [{ name: "public", last_synced_at: 0 }], synced: true };
    }
    if (
      url.includes("/api/db/cache/tree/tables") &&
      url.includes("profile=pg_prod")
    ) {
      return { items: [{ name: "orders", last_synced_at: 0 }], synced: true };
    }
    if (
      url.includes("/api/db/cache/tree/columns") &&
      url.includes("profile=pg_prod")
    ) {
      return {
        items: [
          { name: "id", dtype: "bigint", nullable: false, pk_flag: true, fk_flag: false, last_synced_at: 0 },
          { name: "amount", dtype: "numeric", nullable: true, pk_flag: false, fk_flag: false, last_synced_at: 0 },
        ],
        synced: true,
      };
    }
    return {};
  });
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("DbAssetTree", () => {
  it("lists every configured profile", async () => {
    renderWithProviders(<DbAssetTree value={[]} onChange={() => {}} />);
    await waitFor(() => {
      expect(screen.getByText("pg_prod")).toBeTruthy();
      expect(screen.getByText("dbr_main")).toBeTruthy();
    });
  });

  it("drills profile → database → schema → table → column and emits the right refs", async () => {
    const onChange = vi.fn();
    renderWithProviders(<DbAssetTree value={[]} onChange={onChange} />);
    await waitFor(() => expect(screen.getByText("pg_prod")).toBeTruthy());

    // Expand profile.
    fireEvent.click(screen.getByText("pg_prod"));
    await waitFor(() => expect(screen.getByText("orders_db")).toBeTruthy());

    // Expand database (click the chevron so we don't accidentally toggle the row).
    const dbExpand = screen.getAllByLabelText("Expand")[0];
    fireEvent.click(dbExpand);
    await waitFor(() => expect(screen.getByText("public")).toBeTruthy());

    // Expand schema.
    const schemaExpand = screen.getAllByLabelText("Expand")[0];
    fireEvent.click(schemaExpand);
    await waitFor(() => expect(screen.getByText("orders")).toBeTruthy());

    // Expand table to load columns.
    const tableExpand = screen.getAllByLabelText("Expand")[0];
    fireEvent.click(tableExpand);
    await waitFor(() => expect(screen.getByText("id")).toBeTruthy());

    // Toggle column selection.
    fireEvent.click(screen.getByText("amount"));
    expect(onChange).toHaveBeenCalledWith([
      { kind: "db_column", ref: "pg_prod/orders_db/public/orders.amount" },
    ]);
  });

  it("renders a selected-count badge when a descendant is in value", async () => {
    renderWithProviders(
      <DbAssetTree
        value={[{ kind: "db_column", ref: "pg_prod/orders_db/public/orders.amount" }]}
        onChange={() => {}}
      />,
    );
    await waitFor(() => expect(screen.getByText("1 selected")).toBeTruthy());
  });

  it("reads from the cache for catalog-style backends too — no live round-trip", async () => {
    const calls: string[] = [];
    globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      calls.push(url);
      // Reuse the same handler as beforeEach so the responses match.
      let body: unknown = {};
      if (url.includes("/api/profiles/db")) {
        body = {
          profiles: [{ name: "dbr_main", backend: "databricks" }],
        };
      } else if (url.includes("/api/db/cache/tree/databases?profile=dbr_main")) {
        body = { items: [{ name: "main", last_synced_at: 0 }], synced: true };
      }
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }) as unknown as typeof fetch;

    renderWithProviders(<DbAssetTree value={[]} onChange={() => {}} />);
    await waitFor(() => expect(screen.getByText("dbr_main")).toBeTruthy());
    fireEvent.click(screen.getByText("dbr_main"));
    await waitFor(() => expect(screen.getByText("main")).toBeTruthy());

    // Hard guarantee: no /api/live/* URL was touched during the drill.
    expect(calls.some((u) => u.includes("/api/live/"))).toBe(false);
  });
});
