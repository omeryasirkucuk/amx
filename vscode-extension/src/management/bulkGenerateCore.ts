// Pure enumeration + generate/apply logic for bulk description
// generation. No vscode imports, so the decision-making (what counts
// as undocumented, how a target maps to an endpoint, how failures are
// captured) is unit-tested directly. The vscode UI orchestration lives
// in bulkGenerate.ts.
import { AmxApiError } from "../api/errors";
import type { ExtensionServices } from "../services";
import type { CatalogNodeArg } from "./catalogNodeArg";

/** A single undocumented asset queued for generation. */
export interface GenTarget {
  kind: "table" | "column";
  profile: string;
  schema: string;
  table: string;
  column?: string;
  /** schema.table or schema.table.column — display + dedupe key. */
  label: string;
}

/** A target paired with the outcome of its generate call. */
export interface GenProposal {
  target: GenTarget;
  description?: string;
  error?: string;
}

/** The slices of ExtensionServices this logic needs — narrowed so the
 *  functions are testable with light fakes. */
export interface BulkGenerateDeps {
  catalog: Pick<ExtensionServices["catalog"], "getTables" | "getColumns" | "invalidate">;
  client: {
    generate: ExtensionServices["client"]["generate"];
    comments: ExtensionServices["client"]["comments"];
  };
}

/**
 * Find the undocumented assets under a schema or table node. Tables
 * come from the (already-fetched) catalog inventory; columns are
 * pulled per table only when requested. A table without a resolvable
 * profile is skipped — the generate endpoints require one.
 */
export async function enumerateMissing(
  deps: BulkGenerateDeps,
  node: CatalogNodeArg,
  includeColumns: boolean,
): Promise<GenTarget[]> {
  if (!node.schema) return [];
  const scope: { profile?: string; database?: string } = {};
  if (node.profile !== undefined) scope.profile = node.profile;
  if (node.database !== undefined) scope.database = node.database;

  const allTables = await deps.catalog.getTables(scope);
  const inScope = allTables.filter(
    (t) =>
      t.schema.toLowerCase() === node.schema!.toLowerCase() &&
      // A table node narrows to a single table; a schema node takes all.
      (node.table === undefined || t.name.toLowerCase() === node.table.toLowerCase()),
  );

  const targets: GenTarget[] = [];
  for (const table of inScope) {
    const profile = table.profile ?? node.profile;
    if (!profile) continue; // can't generate without a profile
    if (!table.description?.trim()) {
      targets.push({
        kind: "table",
        profile,
        schema: table.schema,
        table: table.name,
        label: `${table.schema}.${table.name}`,
      });
    }
    if (!includeColumns) continue;
    const columns = await deps.catalog.getColumns(table.schema, table.name, profile);
    for (const column of columns) {
      if (column.description?.trim()) continue;
      targets.push({
        kind: "column",
        profile,
        schema: table.schema,
        table: table.name,
        column: column.name,
        label: `${table.schema}.${table.name}.${column.name}`,
      });
    }
  }
  return targets;
}

/**
 * Generate a description for one target. Never throws — failures are
 * captured on the returned proposal so a single bad asset doesn't abort
 * the batch.
 */
export async function generateOne(
  deps: BulkGenerateDeps,
  target: GenTarget,
): Promise<GenProposal> {
  try {
    const result =
      target.kind === "column"
        ? await deps.client.generate.column(
            target.schema,
            target.table,
            target.column!,
            target.profile,
          )
        : await deps.client.generate.table(target.schema, target.table, target.profile);
    return { target, description: result.description };
  } catch (error) {
    return { target, error: describeError(error) };
  }
}

/** Write one approved proposal to the catalog or the source database. */
export async function applyOne(
  deps: BulkGenerateDeps,
  proposal: GenProposal,
  toDatabase: boolean,
): Promise<void> {
  const { target, description } = proposal;
  if (!description) return;
  if (toDatabase) {
    if (target.column) {
      await deps.client.comments.setColumn(
        target.schema,
        target.table,
        target.column,
        description,
        target.profile,
      );
    } else {
      await deps.client.comments.setTable(target.schema, target.table, description, target.profile);
    }
    return;
  }
  await deps.client.comments.setLocal({
    profile: target.profile,
    schema: target.schema,
    table: target.table,
    ...(target.column ? { column: target.column } : {}),
    description,
  });
}

export function describeError(error: unknown): string {
  if (error instanceof AmxApiError) {
    return error.hint ? `${error.detail} — ${error.hint}` : error.detail;
  }
  return error instanceof Error ? error.message : String(error);
}
