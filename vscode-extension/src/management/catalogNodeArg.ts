// Pure mapping from catalog tree elements (the raw node objects the
// tree provider returns) onto the flat argument shape management
// commands consume. No vscode imports — unit-tested directly.

export interface CatalogNodeArg {
  profile?: string;
  database?: string;
  schema?: string;
  table?: string;
  column?: string;
  description?: string;
  /** Schema-level bulk run: analyze every table without a picker. */
  allTables?: boolean;
}

/**
 * Map a VS Code tree element onto CatalogNodeArg. Returns undefined
 * when the element is not a recognizable catalog node.
 */
export function catalogArgFromNode(element: unknown): CatalogNodeArg | undefined {
  if (typeof element !== "object" || element === null) return undefined;
  const node = element as Record<string, unknown>;

  if (node["type"] === "profileScope") {
    const profile = node["profile"] as string | undefined;
    return profile !== undefined ? { profile } : {};
  }

  if (node["type"] === "databaseScope") {
    const arg: CatalogNodeArg = {};
    const profile = node["profile"] as string | undefined;
    const database = node["database"] as string | undefined;
    if (profile !== undefined) arg.profile = profile;
    if (database !== undefined) arg.database = database;
    return arg;
  }

  if (node["type"] === "schema") {
    const arg: CatalogNodeArg = { schema: node["schema"] as string };
    const profile = node["profile"] as string | undefined;
    const database = node["database"] as string | undefined;
    if (profile !== undefined) arg.profile = profile;
    if (database !== undefined) arg.database = database;
    return arg;
  }

  if (node["type"] === "table") {
    const meta = node["meta"] as {
      schema: string;
      name: string;
      profile?: string;
      description?: string;
    };
    const arg: CatalogNodeArg = { schema: meta.schema, table: meta.name };
    if (meta.profile) arg.profile = meta.profile;
    if (meta.description) arg.description = meta.description;
    return arg;
  }

  if (node["type"] === "column") {
    const table = node["table"] as { schema: string; name: string; profile?: string };
    const meta = node["meta"] as { name: string; description?: string };
    const arg: CatalogNodeArg = { schema: table.schema, table: table.name, column: meta.name };
    if (table.profile) arg.profile = table.profile;
    if (meta.description) arg.description = meta.description;
    return arg;
  }

  return undefined;
}
