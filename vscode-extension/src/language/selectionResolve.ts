// Pure half of the selection lookup: scan the selected text as SQL
// and resolve identifier chains against the warm multi-profile
// resolver index. No vscode imports — unit-tested directly; the UI
// wrapper (lightbulb, QuickPick, panel open) lives in
// selectionLookup.ts.
import type { CatalogResolver } from "./resolver";
import { scanSql } from "./scanner";

export interface SelectionMatch {
  kind: "database" | "schema" | "table" | "column";
  profile?: string;
  database?: string;
  schema?: string;
  table?: string;
  column?: string;
  description?: string;
}

/** Resolve identifier chains in the selection against the warm index. */
export function resolveSelectionLocally(
  text: string,
  resolver: CatalogResolver,
): SelectionMatch[] {
  const scan = scanSql(text);
  const seen = new Set<string>();
  const matches: SelectionMatch[] = [];
  const add = (match: SelectionMatch): void => {
    const key = `${match.profile ?? ""}|${match.schema}|${match.table}|${match.column ?? ""}`;
    if (seen.has(key)) return;
    seen.add(key);
    matches.push(match);
  };
  for (const token of scan.tokens) {
    const resolved = resolver.resolve(scan, token);
    if (!resolved || resolved.confidence === "weak") continue;
    if (resolved.kind === "table") {
      for (const table of resolved.candidates) {
        const match: SelectionMatch = { kind: "table", schema: table.schema, table: table.name };
        if (table.profile) match.profile = table.profile;
        if (table.database) match.database = table.database;
        if (table.description) match.description = table.description;
        add(match);
      }
    } else {
      for (const candidate of resolved.candidates) {
        const match: SelectionMatch = {
          kind: "column",
          schema: candidate.table.schema,
          table: candidate.table.name,
          column: candidate.column.name,
        };
        if (candidate.table.profile) match.profile = candidate.table.profile;
        if (candidate.table.database) match.database = candidate.table.database;
        if (candidate.column.description) match.description = candidate.column.description;
        add(match);
      }
    }
  }
  return matches;
}

/**
 * Granularity-aware container matching: when the selection is a
 * single bare identifier, the user is naming the thing itself — a
 * database ("sap") or a schema ("sap_s6p") — so exact name matches
 * against the warm table index's containers rank FIRST, ahead of
 * table/column hits. Multi-part selections (sap.amx.orders) express
 * table intent and skip this path.
 */
export function matchContainers(
  text: string,
  tables: readonly { profile?: string; database?: string; schema: string }[],
): SelectionMatch[] {
  const bare = text.trim();
  if (!/^[A-Za-z_][A-Za-z0-9_$]*$/.test(bare)) return [];
  const wanted = bare.toLowerCase();
  const matches: SelectionMatch[] = [];
  const seen = new Set<string>();
  for (const table of tables) {
    if (table.database && table.database.toLowerCase() === wanted) {
      const key = `db|${table.profile ?? ""}|${table.database.toLowerCase()}`;
      if (!seen.has(key)) {
        seen.add(key);
        const match: SelectionMatch = { kind: "database", database: table.database };
        if (table.profile) match.profile = table.profile;
        matches.push(match);
      }
    }
    if (table.schema.toLowerCase() === wanted) {
      const key = `schema|${table.profile ?? ""}|${(table.database ?? "").toLowerCase()}|${table.schema.toLowerCase()}`;
      if (!seen.has(key)) {
        seen.add(key);
        const match: SelectionMatch = { kind: "schema", schema: table.schema };
        if (table.profile) match.profile = table.profile;
        if (table.database) match.database = table.database;
        matches.push(match);
      }
    }
  }
  // Databases ahead of schemas at equal specificity.
  return matches.sort((a, b) => (a.kind === b.kind ? 0 : a.kind === "database" ? -1 : 1));
}
