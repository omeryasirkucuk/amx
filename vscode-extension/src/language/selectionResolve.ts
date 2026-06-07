// Pure half of the selection lookup: scan the selected text as SQL
// and resolve identifier chains against the warm multi-profile
// resolver index. No vscode imports — unit-tested directly; the UI
// wrapper (lightbulb, QuickPick, panel open) lives in
// selectionLookup.ts.
import type { CatalogResolver } from "./resolver";
import { scanSql } from "./scanner";

export interface SelectionMatch {
  profile?: string;
  database?: string;
  schema: string;
  table: string;
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
        const match: SelectionMatch = { schema: table.schema, table: table.name };
        if (table.profile) match.profile = table.profile;
        if (table.database) match.database = table.database;
        if (table.description) match.description = table.description;
        add(match);
      }
    } else {
      for (const candidate of resolved.candidates) {
        const match: SelectionMatch = {
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
