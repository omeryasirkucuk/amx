// Unit tests for the catalog resolver, run against a real
// CatalogCache fed by a stubbed REST client.
import { beforeEach, describe, expect, it } from "vitest";

import { CatalogCache } from "../../src/api/catalogCache";
import type { AmxClient } from "../../src/api/client";
import type { InventoryTable, TableExplain } from "../../src/api/types";
import { CatalogResolver } from "../../src/language/resolver";
import { scanSql } from "../../src/language/scanner";
import type { ScanResult, SqlToken } from "../../src/language/types";

const INVENTORY: InventoryTable[] = [
  {
    db_profile: "pg",
    schema_name: "raw",
    table_name: "orders",
    effective_description: "Raw order events",
  },
  { db_profile: "pg", schema_name: "dim", table_name: "orders" },
  {
    db_profile: "pg",
    database_name: "analytics",
    schema_name: "dim",
    table_name: "customers",
  },
];

const EXPLAINS: Record<string, TableExplain> = {
  "raw.orders": {
    table: {},
    columns: [
      { column_name: "id", data_type: "integer", effective_description: "Order id" },
      { column_name: "amount", data_type: "numeric" },
    ],
  },
};

function stubClient(): AmxClient {
  return {
    catalog: {
      inventory: async () => INVENTORY,
      explain: async (path: string) => EXPLAINS[path] ?? { table: {}, columns: [] },
    },
  } as unknown as AmxClient;
}

function tablePositionTokens(scan: ScanResult): SqlToken[] {
  return scan.tokens.filter((token) => token.context === "tablePosition");
}

describe("CatalogResolver", () => {
  let cache: CatalogCache;
  let resolver: CatalogResolver;

  beforeEach(async () => {
    cache = new CatalogCache(stubClient());
    resolver = new CatalogResolver(cache);
    await resolver.ensureWarm();
  });

  it("resolves a qualified schema.table reference exactly", () => {
    const scan = scanSql("SELECT 1 FROM raw.orders");
    const resolved = resolver.resolve(scan, tablePositionTokens(scan)[0]!);
    expect(resolved?.kind).toBe("table");
    expect(resolved?.confidence).toBe("exact");
    expect(resolved?.kind === "table" && resolved.table.description).toBe("Raw order events");
  });

  it("marks a bare table name shared by two schemas as ambiguous", () => {
    const scan = scanSql("SELECT 1 FROM orders");
    const resolved = resolver.resolve(scan, tablePositionTokens(scan)[0]!);
    expect(resolved?.kind).toBe("table");
    expect(resolved?.confidence).toBe("ambiguous");
    expect(resolved?.candidates).toHaveLength(2);
  });

  it("resolves a unique bare table name exactly", () => {
    const scan = scanSql("SELECT 1 FROM customers");
    const resolved = resolver.resolve(scan, tablePositionTokens(scan)[0]!);
    expect(resolved?.confidence).toBe("exact");
    expect(resolved?.kind === "table" && resolved.table.schema).toBe("dim");
  });

  it("matches case-insensitively", () => {
    const scan = scanSql("SELECT 1 FROM RAW.ORDERS");
    const resolved = resolver.resolve(scan, tablePositionTokens(scan)[0]!);
    expect(resolved?.confidence).toBe("exact");
    expect(resolved?.kind === "table" && resolved.table.name).toBe("orders");
  });

  it("resolves database.schema.table chains", () => {
    const scan = scanSql("SELECT 1 FROM analytics.dim.customers");
    const resolved = resolver.resolve(scan, tablePositionTokens(scan)[0]!);
    expect(resolved?.kind).toBe("table");
    expect(resolved?.confidence).toBe("exact");
  });

  it("never resolves CTE names, even when a catalog table matches", () => {
    const sql = "WITH customers AS (SELECT 1) SELECT 1 FROM customers";
    const scan = scanSql(sql);
    const tables = tablePositionTokens(scan);
    const cteRef = tables[tables.length - 1]!;
    expect(resolver.resolve(scan, cteRef)).toBeUndefined();
  });

  it("resolves alias.column through the statement alias map once warm", async () => {
    await cache.getColumns("raw", "orders");
    const scan = scanSql("SELECT o.id FROM raw.orders o");
    const columnToken = scan.tokens.find((token) => token.context === "columnPosition")!;
    const resolved = resolver.resolve(scan, columnToken);
    expect(resolved?.kind).toBe("column");
    expect(resolved?.confidence).toBe("exact");
    expect(resolved?.kind === "column" && resolved.column.description).toBe("Order id");
  });

  it("treats cold column sets as unresolved and warms them in the background", async () => {
    const scan = scanSql("SELECT o.id FROM raw.orders o");
    const columnToken = scan.tokens.find((token) => token.context === "columnPosition")!;
    expect(resolver.resolve(scan, columnToken)).toBeUndefined();
    // The miss kicked off a background fetch; once it lands the same
    // token resolves.
    await cache.getColumns("raw", "orders");
    expect(resolver.resolve(scan, columnToken)?.confidence).toBe("exact");
  });

  it("resolves a bare column against the statement's referenced tables", async () => {
    await cache.getColumns("raw", "orders");
    const scan = scanSql("SELECT amount FROM raw.orders");
    const columnToken = scan.tokens.find((token) => token.context === "columnPosition")!;
    const resolved = resolver.resolve(scan, columnToken);
    expect(resolved?.kind).toBe("column");
    expect(resolved?.confidence).toBe("exact");
  });

  it("rebuilds the index after a cache invalidation", async () => {
    cache.invalidate();
    const scan = scanSql("SELECT 1 FROM raw.orders");
    // Warm tables survive invalidation, so the rebuilt index still
    // answers; a re-warm refreshes it from the (stub) backend.
    expect(resolver.resolve(scan, tablePositionTokens(scan)[0]!)?.confidence).toBe("exact");
    await resolver.ensureWarm();
    expect(resolver.resolve(scan, tablePositionTokens(scan)[0]!)?.confidence).toBe("exact");
  });
});
