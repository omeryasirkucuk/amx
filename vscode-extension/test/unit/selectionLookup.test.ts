// Selection-lookup local resolution + catalogArgFromNode mappings for
// the new database/schema tree nodes.
import { describe, expect, it } from "vitest";

import { CatalogCache } from "../../src/api/catalogCache";
import type { AmxClient } from "../../src/api/client";
import type { InventoryTable } from "../../src/api/types";
import { catalogArgFromNode } from "../../src/management/catalogNodeArg";
import { CatalogResolver } from "../../src/language/resolver";
import { resolveSelectionLocally } from "../../src/language/selectionResolve";

function cacheWith(rows: InventoryTable[]): CatalogCache {
  const client = {
    catalog: {
      inventory: async () => rows,
      explain: async () => ({ table: {}, columns: [] }),
    },
  } as unknown as AmxClient;
  return new CatalogCache(client, 60_000);
}

const DBR_ROWS: InventoryTable[] = [
  {
    db_profile: "dbr-oyk",
    database_name: "main",
    schema_name: "amx",
    table_name: "test",
    effective_description: "Scratch table",
  },
];

describe("resolveSelectionLocally", () => {
  it("resolves a qualified table from a non-active profile after warm-all", async () => {
    const cache = cacheWith(DBR_ROWS);
    const resolver = new CatalogResolver(cache);
    await resolver.ensureWarmAll(["dbr-oyk"]);
    const matches = resolveSelectionLocally('SELECT * FROM amx.test', resolver);
    expect(matches).toEqual([
      {
        profile: "dbr-oyk",
        database: "main",
        schema: "amx",
        table: "test",
        description: "Scratch table",
      },
    ]);
  });

  it("returns no match for unknown identifiers", async () => {
    const cache = cacheWith(DBR_ROWS);
    const resolver = new CatalogResolver(cache);
    await resolver.ensureWarmAll(["dbr-oyk"]);
    expect(resolveSelectionLocally("SELECT * FROM nope.missing", resolver)).toEqual([]);
  });

  it("dedupes repeated references", async () => {
    const cache = cacheWith(DBR_ROWS);
    const resolver = new CatalogResolver(cache);
    await resolver.ensureWarmAll(["dbr-oyk"]);
    const matches = resolveSelectionLocally(
      "SELECT * FROM amx.test JOIN amx.test t2 ON 1=1",
      resolver,
    );
    expect(matches).toHaveLength(1);
  });
});

describe("CatalogCache per-database scope keys", () => {
  it("caches database scopes independently", async () => {
    let calls = 0;
    const client = {
      catalog: {
        inventory: async () => {
          calls += 1;
          return DBR_ROWS;
        },
        explain: async () => ({ table: {}, columns: [] }),
      },
    } as unknown as AmxClient;
    const cache = new CatalogCache(client, 60_000);
    await cache.getTables({ profile: "p", database: "db1" });
    await cache.getTables({ profile: "p", database: "db2" });
    await cache.getTables({ profile: "p", database: "db1" });
    expect(calls).toBe(2);
  });
});

describe("catalogArgFromNode (database/schema nodes)", () => {
  it("maps databaseScope nodes", () => {
    expect(
      catalogArgFromNode({ type: "databaseScope", profile: "p", database: "dwh", label: "dwh" }),
    ).toEqual({ profile: "p", database: "dwh" });
  });

  it("maps legacy (default) database nodes without a database field", () => {
    expect(catalogArgFromNode({ type: "databaseScope", profile: "p", label: "(default)" })).toEqual(
      { profile: "p" },
    );
  });

  it("maps schema nodes with database context", () => {
    expect(
      catalogArgFromNode({ type: "schema", profile: "p", database: "dwh", schema: "sales" }),
    ).toEqual({ profile: "p", database: "dwh", schema: "sales" });
  });
});
