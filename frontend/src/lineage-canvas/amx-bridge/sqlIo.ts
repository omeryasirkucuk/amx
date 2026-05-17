/**
 * SQL import / export bridge — thin wrappers over the new
 * /api/lineage/sql/parse and /sql/render endpoints.
 */

import { apiFetch } from "../../lib/api";

export interface ParsedSql {
  tables: Array<{ id: string; schema: string; table: string; columns: string[] }>;
  operators: Array<{ id: string; kind: string; expression: string }>;
  edges: Array<{ source: string; source_column: string; target: string; target_column: string }>;
}

export async function parseSql(sql: string, dialect?: string): Promise<ParsedSql> {
  return apiFetch<ParsedSql>("/api/lineage/sql/parse", {
    method: "POST",
    body: JSON.stringify({ sql, dialect }),
  });
}

export interface RenderedSql {
  sql: string;
  dialect: string;
}

export async function renderSql(canvas: unknown, dialect?: string): Promise<RenderedSql> {
  return apiFetch<RenderedSql>("/api/lineage/sql/render", {
    method: "POST",
    body: JSON.stringify({ canvas, dialect }),
  });
}
