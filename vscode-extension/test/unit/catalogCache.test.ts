// CatalogCache TTL, in-flight dedup, lazy columns, invalidation events.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { CatalogCache } from "../../src/api/catalogCache";
import type { AmxClient } from "../../src/api/client";
import type { InventoryTable, TableExplain } from "../../src/api/types";

function fakeClient(overrides: {
  inventory?: () => Promise<InventoryTable[]>;
  explain?: (path: string) => Promise<TableExplain>;
}): AmxClient {
  return {
    catalog: {
      inventory: overrides.inventory ?? (async () => []),
      explain: overrides.explain ?? (async () => ({ table: {}, columns: [] })),
    },
  } as unknown as AmxClient;
}

const ROW: InventoryTable = {
  schema_name: "sales",
  table_name: "orders",
  effective_description: "All orders",
  column_count: 3,
};

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("CatalogCache", () => {
  it("maps inventory rows into TableMeta", async () => {
    const cache = new CatalogCache(fakeClient({ inventory: async () => [ROW] }));
    const tables = await cache.getTables();
    expect(tables).toEqual([
      { schema: "sales", name: "orders", description: "All orders", columnCount: 3 },
    ]);
  });

  it("serves cached tables within the TTL and refetches after it", async () => {
    const inventory = vi.fn(async () => [ROW]);
    const cache = new CatalogCache(fakeClient({ inventory }), 1000);
    await cache.getTables();
    await cache.getTables();
    expect(inventory).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1500);
    await cache.getTables();
    expect(inventory).toHaveBeenCalledTimes(2);
  });

  it("dedupes concurrent table fetches", async () => {
    const inventory = vi.fn(async () => [ROW]);
    const cache = new CatalogCache(fakeClient({ inventory }), 1000);
    await Promise.all([cache.getTables(), cache.getTables(), cache.getTables()]);
    expect(inventory).toHaveBeenCalledTimes(1);
  });

  it("lazily loads and caches columns per table", async () => {
    const explain = vi.fn(async () => ({
      table: {},
      columns: [{ column_name: "id", data_type: "INT", effective_description: "PK" }],
    }));
    const cache = new CatalogCache(fakeClient({ explain }), 1000);
    const key = CatalogCache.tableKey("Sales", "Orders");
    expect(cache.warmColumns(key)).toBeUndefined();
    const columns = await cache.getColumns("Sales", "Orders");
    expect(columns).toEqual([{ tableKey: key, name: "id", dataType: "INT", description: "PK" }]);
    await cache.getColumns("Sales", "Orders");
    expect(explain).toHaveBeenCalledTimes(1);
    expect(cache.warmColumns(key)).toHaveLength(1);
  });

  it("invalidate clears columns, resets TTL, and notifies listeners", async () => {
    const inventory = vi.fn(async () => [ROW]);
    const cache = new CatalogCache(fakeClient({ inventory }), 60_000);
    await cache.getTables();
    const seen: unknown[] = [];
    cache.onDidInvalidate((scope) => seen.push(scope));
    cache.invalidate();
    expect(seen).toEqual([undefined]);
    await cache.getTables();
    expect(inventory).toHaveBeenCalledTimes(2);
  });

  it("listener disposal stops notifications", async () => {
    const cache = new CatalogCache(fakeClient({}), 1000);
    const seen: unknown[] = [];
    const subscription = cache.onDidInvalidate((scope) => seen.push(scope));
    subscription.dispose();
    cache.invalidate();
    expect(seen).toEqual([]);
  });
});
