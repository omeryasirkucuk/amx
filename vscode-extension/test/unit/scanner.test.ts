// Unit tests for the catalog-anchored SQL scanner.
import { describe, expect, it } from "vitest";

import { scanSql, tokenAt } from "../../src/language/scanner";
import type { SqlToken } from "../../src/language/types";

function chainText(token: SqlToken): string {
  return token.parts.map((part) => part.text).join(".");
}

function byContext(sql: string, context: SqlToken["context"]): string[] {
  return scanSql(sql)
    .tokens.filter((token) => token.context === context)
    .map(chainText);
}

describe("scanSql", () => {
  it("tags identifier chains after FROM and JOIN as table positions", () => {
    const sql = "SELECT o.id FROM raw.orders o JOIN dim.customers c ON o.cid = c.id";
    expect(byContext(sql, "tablePosition")).toEqual(["raw.orders", "dim.customers"]);
    expect(byContext(sql, "aliasDef")).toEqual(["o", "c"]);
  });

  it("records alias mappings for alias.column resolution", () => {
    const scan = scanSql("SELECT o.amount FROM raw.orders AS o");
    const statement = scan.statements[0]!;
    expect(statement.aliases.get("o")).toBe("raw.orders");
    const columnChains = scan.tokens
      .filter((token) => token.context === "columnPosition")
      .map(chainText);
    expect(columnChains).toContain("o.amount");
  });

  it("reads all three quoted identifier styles", () => {
    const sql = 'SELECT 1 FROM "My Schema"."Orders" JOIN `db`.`tbl` JOIN [dbo].[Users]';
    const tables = scanSql(sql).tokens.filter((token) => token.context === "tablePosition");
    expect(tables.map(chainText)).toEqual(["My Schema.Orders", "db.tbl", "dbo.Users"]);
    expect(tables.every((token) => token.parts.every((part) => part.quoted))).toBe(true);
  });

  it("reads dotted chains up to four parts", () => {
    const scan = scanSql("SELECT db.sch.tbl.col FROM db.sch.tbl");
    const column = scan.tokens.find((token) => token.context === "columnPosition");
    const table = scan.tokens.find((token) => token.context === "tablePosition");
    expect(column && chainText(column)).toBe("db.sch.tbl.col");
    expect(column?.parts).toHaveLength(4);
    expect(table && chainText(table)).toBe("db.sch.tbl");
  });

  it("registers CTE names so they can shadow catalog tables", () => {
    const sql = "WITH orders AS (SELECT * FROM raw.orders) SELECT * FROM orders";
    const scan = scanSql(sql);
    const statement = scan.statements[0]!;
    expect(statement.ctes.has("orders")).toBe(true);
    const cteDefs = scan.tokens.filter((token) => token.context === "cteDef");
    expect(cteDefs.map(chainText)).toEqual(["orders"]);
    const tables = scan.tokens.filter((token) => token.context === "tablePosition");
    expect(tables.map(chainText)).toEqual(["raw.orders", "orders"]);
  });

  it("registers every CTE in a multi-CTE WITH list", () => {
    const sql = "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a JOIN b ON 1 = 1";
    const statement = scanSql(sql).statements[0]!;
    expect([...statement.ctes].sort()).toEqual(["a", "b"]);
  });

  it("ignores keywords and identifiers inside comments and strings", () => {
    const sql = [
      "SELECT '-- from fake_table' AS label, 'it''s' AS quoted",
      "FROM t -- join nothing",
      "/* update t2 set x = 1 */",
      "# from t3",
    ].join("\n");
    const tables = byContext(sql, "tablePosition");
    expect(tables).toEqual(["t"]);
    const allChains = scanSql(sql).tokens.map(chainText);
    expect(allChains).not.toContain("fake_table");
    expect(allChains).not.toContain("t2");
    expect(allChains).not.toContain("t3");
  });

  it("skips Jinja expression and statement blocks", () => {
    const sql =
      "SELECT * FROM {{ ref('orders') }} JOIN dim.users u {% if flag %}WHERE x = 1{% endif %}";
    expect(byContext(sql, "tablePosition")).toEqual(["dim.users"]);
    const allChains = scanSql(sql).tokens.map(chainText);
    expect(allChains).not.toContain("ref");
    expect(allChains).not.toContain("flag");
  });

  it("splits statements on top-level semicolons", () => {
    const sql = "SELECT 1 FROM a; SELECT 2 FROM b;";
    const scan = scanSql(sql);
    expect(scan.statements.length).toBeGreaterThanOrEqual(2);
    const tableA = scan.tokens.find((token) => chainText(token) === "a");
    const tableB = scan.tokens.find((token) => chainText(token) === "b");
    expect(tableA?.statementIndex).toBe(0);
    expect(tableB?.statementIndex).toBe(1);
    expect(scan.statements[0]!.end).toBe(sql.indexOf(";"));
  });

  it("does not split statements on semicolons inside parentheses or strings", () => {
    const scan = scanSql("SELECT ';' FROM a WHERE x IN (SELECT y FROM b)");
    const tables = scan.tokens.filter((token) => token.context === "tablePosition");
    expect(tables.every((token) => token.statementIndex === 0)).toBe(true);
  });

  it("skips function calls in table and column position", () => {
    const sql = "SELECT count(*), upper(name) FROM generate_series(1, 5) JOIN raw.orders";
    const chains = scanSql(sql).tokens.map(chainText);
    expect(chains).not.toContain("count");
    expect(chains).not.toContain("upper");
    expect(chains).not.toContain("generate_series");
    expect(byContext(sql, "tablePosition")).toEqual(["raw.orders"]);
  });

  it("treats comma-separated FROM lists as table positions", () => {
    expect(byContext("SELECT 1 FROM a x, b y", "tablePosition")).toEqual(["a", "b"]);
  });

  it("finds the token covering a given offset", () => {
    const sql = "SELECT 1 FROM raw.orders";
    const scan = scanSql(sql);
    const token = tokenAt(scan, sql.indexOf("orders") + 2);
    expect(token && chainText(token)).toBe("raw.orders");
    expect(tokenAt(scan, 0)).toBeUndefined();
  });
});
