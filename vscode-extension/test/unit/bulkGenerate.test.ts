// Bulk-generate enumeration + generate/apply logic. The vscode UI
// orchestration is exercised manually; these tests cover the pure
// pieces that decide what gets generated and how failures are handled.
import { describe, expect, it, vi } from "vitest";

import {
  applyOne,
  enumerateMissing,
  generateOne,
  type BulkGenerateDeps,
  type GenTarget,
} from "../../src/management/bulkGenerateCore";

type TableMeta = {
  schema: string;
  name: string;
  profile?: string;
  description?: string;
};
type ColumnMeta = { name: string; description?: string };

const makeDeps = (options: {
  tables: TableMeta[];
  columns?: Record<string, ColumnMeta[]>;
  generate?: Partial<BulkGenerateDeps["client"]["generate"]>;
  comments?: Partial<BulkGenerateDeps["client"]["comments"]>;
}): BulkGenerateDeps => ({
  catalog: {
    getTables: vi.fn().mockResolvedValue(options.tables),
    getColumns: vi.fn(async (schema: string, table: string) => {
      return options.columns?.[`${schema}.${table}`] ?? [];
    }),
    invalidate: vi.fn(),
  } as unknown as BulkGenerateDeps["catalog"],
  client: {
    generate: {
      table: vi.fn(),
      column: vi.fn(),
      ...options.generate,
    } as unknown as BulkGenerateDeps["client"]["generate"],
    comments: {
      setLocal: vi.fn(),
      setTable: vi.fn(),
      setColumn: vi.fn(),
      ...options.comments,
    } as unknown as BulkGenerateDeps["client"]["comments"],
  },
});

describe("enumerateMissing", () => {
  it("returns only undocumented tables when columns are excluded", async () => {
    const deps = makeDeps({
      tables: [
        { schema: "sales", name: "orders", profile: "pg", description: "" },
        { schema: "sales", name: "customers", profile: "pg", description: "has one" },
        { schema: "other", name: "ignored", profile: "pg" },
      ],
    });
    const targets = await enumerateMissing(deps, { schema: "sales", profile: "pg" }, false);
    expect(targets).toEqual([
      {
        kind: "table",
        profile: "pg",
        schema: "sales",
        table: "orders",
        label: "sales.orders",
      },
    ]);
  });

  it("includes undocumented columns when requested", async () => {
    const deps = makeDeps({
      tables: [{ schema: "sales", name: "orders", profile: "pg", description: "documented" }],
      columns: {
        "sales.orders": [
          { name: "id", description: "pk" },
          { name: "total", description: "" },
        ],
      },
    });
    const targets = await enumerateMissing(deps, { schema: "sales", profile: "pg" }, true);
    expect(targets).toEqual([
      {
        kind: "column",
        profile: "pg",
        schema: "sales",
        table: "orders",
        column: "total",
        label: "sales.orders.total",
      },
    ]);
  });

  it("narrows to a single table when the node is a table", async () => {
    const deps = makeDeps({
      tables: [
        { schema: "sales", name: "orders", profile: "pg" },
        { schema: "sales", name: "refunds", profile: "pg" },
      ],
    });
    const targets = await enumerateMissing(
      deps,
      { schema: "sales", table: "orders", profile: "pg" },
      false,
    );
    expect(targets.map((t) => t.label)).toEqual(["sales.orders"]);
  });

  it("skips tables without a resolvable profile", async () => {
    const deps = makeDeps({ tables: [{ schema: "sales", name: "orders" }] });
    const targets = await enumerateMissing(deps, { schema: "sales" }, false);
    expect(targets).toEqual([]);
  });

  it("falls back to the node profile when the table omits one", async () => {
    const deps = makeDeps({ tables: [{ schema: "sales", name: "orders" }] });
    const targets = await enumerateMissing(deps, { schema: "sales", profile: "node-pg" }, false);
    expect(targets[0]?.profile).toBe("node-pg");
  });
});

describe("generateOne", () => {
  const tableTarget: GenTarget = {
    kind: "table",
    profile: "pg",
    schema: "sales",
    table: "orders",
    label: "sales.orders",
  };

  it("returns the generated description on success", async () => {
    const deps = makeDeps({
      tables: [],
      generate: { table: vi.fn().mockResolvedValue({ description: "All orders" }) },
    });
    const proposal = await generateOne(deps, tableTarget);
    expect(proposal).toEqual({ target: tableTarget, description: "All orders" });
  });

  it("captures the error instead of throwing", async () => {
    const deps = makeDeps({
      tables: [],
      generate: { table: vi.fn().mockRejectedValue(new Error("rate limited")) },
    });
    const proposal = await generateOne(deps, tableTarget);
    expect(proposal.description).toBeUndefined();
    expect(proposal.error).toBe("rate limited");
  });

  it("routes column targets to the column endpoint", async () => {
    const column = vi.fn().mockResolvedValue({ description: "the total" });
    const deps = makeDeps({ tables: [], generate: { column } });
    await generateOne(deps, {
      kind: "column",
      profile: "pg",
      schema: "sales",
      table: "orders",
      column: "total",
      label: "sales.orders.total",
    });
    expect(column).toHaveBeenCalledWith("sales", "orders", "total", "pg");
  });
});

describe("applyOne", () => {
  it("writes a table description to the local catalog", async () => {
    const setLocal = vi.fn();
    const deps = makeDeps({ tables: [], comments: { setLocal } });
    await applyOne(
      deps,
      {
        target: { kind: "table", profile: "pg", schema: "sales", table: "orders", label: "x" },
        description: "All orders",
      },
      false,
    );
    expect(setLocal).toHaveBeenCalledWith({
      profile: "pg",
      schema: "sales",
      table: "orders",
      description: "All orders",
    });
  });

  it("writes a column description to the database", async () => {
    const setColumn = vi.fn();
    const deps = makeDeps({ tables: [], comments: { setColumn } });
    await applyOne(
      deps,
      {
        target: {
          kind: "column",
          profile: "pg",
          schema: "sales",
          table: "orders",
          column: "total",
          label: "x",
        },
        description: "the total",
      },
      true,
    );
    expect(setColumn).toHaveBeenCalledWith("sales", "orders", "total", "the total", "pg");
  });

  it("is a no-op for a proposal without a description", async () => {
    const setLocal = vi.fn();
    const setTable = vi.fn();
    const deps = makeDeps({ tables: [], comments: { setLocal, setTable } });
    await applyOne(
      deps,
      {
        target: { kind: "table", profile: "pg", schema: "sales", table: "orders", label: "x" },
        error: "failed",
      },
      false,
    );
    expect(setLocal).not.toHaveBeenCalled();
    expect(setTable).not.toHaveBeenCalled();
  });
});
