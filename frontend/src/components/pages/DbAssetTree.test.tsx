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
    if (url.includes("/api/live/databases")) {
      return { databases: ["orders_db"], active_database: null };
    }
    if (url.includes("/api/live/catalogs")) {
      return { supports_catalogs: true, catalogs: ["main"], active_catalog: null };
    }
    if (url.endsWith("/api/live/schemas?profile=pg_prod&database=orders_db")) {
      return { catalog: null, schemas: ["public"], items: [] };
    }
    if (
      url.includes("/api/live/schemas/public/assets") &&
      url.includes("pg_prod")
    ) {
      return { schema: "public", assets: [{ name: "orders", kind: "table", comment: "" }], count: 1 };
    }
    if (
      url.includes("/api/live/schemas/public/tables/orders/columns") &&
      url.includes("pg_prod")
    ) {
      return {
        schema: "public",
        table: "orders",
        columns: [
          { name: "id", dtype: "bigint", nullable: false },
          { name: "amount", dtype: "numeric", nullable: true },
        ],
        count: 2,
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

  it("uses /api/live/catalogs for catalog-style backends", async () => {
    renderWithProviders(<DbAssetTree value={[]} onChange={() => {}} />);
    await waitFor(() => expect(screen.getByText("dbr_main")).toBeTruthy());

    fireEvent.click(screen.getByText("dbr_main"));
    await waitFor(() => expect(screen.getByText("main")).toBeTruthy());
  });
});
