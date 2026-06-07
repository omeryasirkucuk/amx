// TTL cache over the catalog REST surface. Feeds the catalog tree
// and (through the resolver index) every language provider, so the
// hover hot path never blocks on HTTP: lookups are synchronous
// against warmed data, fetches happen on demand or via warm().
//
// No vscode imports — events use a tiny local emitter so the module
// tests in plain vitest.
import type { AmxClient } from "./client";
import type { CatalogScope, ExplainColumn, InventoryTable } from "./types";

export interface TableMeta {
  profile?: string;
  database?: string;
  schema: string;
  name: string;
  description?: string;
  assetKind?: string;
  rowCount?: number;
  columnCount?: number;
}

export interface ColumnMeta {
  /** Lowercased `schema.table` of the owning table. */
  tableKey: string;
  name: string;
  dataType?: string;
  description?: string;
}

type Listener = (scope: CatalogScope | undefined) => void;

const DEFAULT_TTL_MS = 300_000;

export class CatalogCache {
  private tables: TableMeta[] = [];
  private tablesFetchedAt = 0;
  private tablesScope: CatalogScope = {};
  private readonly columnsByTable = new Map<string, ColumnMeta[]>();
  private readonly columnFetches = new Map<string, Promise<ColumnMeta[]>>();
  private readonly listeners = new Set<Listener>();
  private tableFetch: Promise<TableMeta[]> | undefined;

  constructor(
    private readonly client: AmxClient,
    private ttlMs: number = DEFAULT_TTL_MS,
  ) {}

  setTtl(ttlMs: number): void {
    this.ttlMs = ttlMs;
  }

  onDidInvalidate(listener: Listener): { dispose(): void } {
    this.listeners.add(listener);
    return { dispose: () => this.listeners.delete(listener) };
  }

  /** Lowercased `schema.table` key used across cache and resolver. */
  static tableKey(schema: string, table: string): string {
    return `${schema.toLowerCase()}.${table.toLowerCase()}`;
  }

  /**
   * All indexed tables for the scope (TTL-cached, deduped in-flight).
   * The inventory endpoint already carries effective descriptions, so
   * table-level features need no further calls.
   */
  async getTables(scope: CatalogScope = {}): Promise<readonly TableMeta[]> {
    const fresh =
      Date.now() - this.tablesFetchedAt < this.ttlMs &&
      sameScope(scope, this.tablesScope) &&
      this.tables.length > 0;
    if (fresh) return this.tables;
    this.tableFetch ??= this.fetchTables(scope).finally(() => {
      this.tableFetch = undefined;
    });
    return this.tableFetch;
  }

  /** Warmed tables only — synchronous, for the hover hot path. */
  get warmTables(): readonly TableMeta[] {
    return this.tables;
  }

  /** Columns for one table, lazily fetched and cached. */
  async getColumns(schema: string, table: string, profile?: string): Promise<readonly ColumnMeta[]> {
    const key = CatalogCache.tableKey(schema, table);
    const cached = this.columnsByTable.get(key);
    if (cached) return cached;
    let inFlight = this.columnFetches.get(key);
    if (!inFlight) {
      inFlight = this.fetchColumns(schema, table, key, profile).finally(() => {
        this.columnFetches.delete(key);
      });
      this.columnFetches.set(key, inFlight);
    }
    return inFlight;
  }

  /** Warmed columns only — synchronous, undefined when not loaded. */
  warmColumns(tableKey: string): readonly ColumnMeta[] | undefined {
    return this.columnsByTable.get(tableKey);
  }

  async warm(scope: CatalogScope = {}): Promise<void> {
    await this.getTables(scope);
  }

  invalidate(scope?: CatalogScope): void {
    this.tablesFetchedAt = 0;
    this.columnsByTable.clear();
    for (const listener of this.listeners) listener(scope);
  }

  private async fetchTables(scope: CatalogScope): Promise<TableMeta[]> {
    const rows = await this.client.catalog.inventory(scope);
    this.tables = rows.map((row) => toTableMeta(row));
    this.tablesFetchedAt = Date.now();
    this.tablesScope = scope;
    return this.tables;
  }

  private async fetchColumns(
    schema: string,
    table: string,
    key: string,
    profile?: string,
  ): Promise<ColumnMeta[]> {
    const payload = await this.client.catalog.explain(`${schema}.${table}`, profile);
    const columns = payload.columns.map((column) => toColumnMeta(column, key));
    this.columnsByTable.set(key, columns);
    return columns;
  }
}

function sameScope(a: CatalogScope, b: CatalogScope): boolean {
  return a.profile === b.profile && a.database === b.database && a.schema === b.schema;
}

function toTableMeta(row: InventoryTable): TableMeta {
  const meta: TableMeta = {
    schema: row.schema_name,
    name: row.table_name,
  };
  if (row.db_profile) meta.profile = row.db_profile;
  if (row.database_name) meta.database = row.database_name;
  if (row.effective_description) meta.description = row.effective_description;
  if (row.asset_kind) meta.assetKind = row.asset_kind;
  if (typeof row.row_count === "number") meta.rowCount = row.row_count;
  if (typeof row.column_count === "number") meta.columnCount = row.column_count;
  return meta;
}

function toColumnMeta(column: ExplainColumn, tableKey: string): ColumnMeta {
  const meta: ColumnMeta = { tableKey, name: column.column_name };
  if (column.data_type) meta.dataType = column.data_type;
  if (column.effective_description) meta.description = column.effective_description;
  return meta;
}
