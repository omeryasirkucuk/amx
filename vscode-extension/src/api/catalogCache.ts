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

interface TableCacheEntry {
  tables: TableMeta[];
  fetchedAt: number;
}

export class CatalogCache {
  /** Per-profile table cache; key "" is the active-profile scope. */
  private readonly tablesByScope = new Map<string, TableCacheEntry>();
  private readonly tableFetches = new Map<string, Promise<TableMeta[]>>();
  private readonly columnsByTable = new Map<string, ColumnMeta[]>();
  private readonly columnFetches = new Map<string, Promise<ColumnMeta[]>>();
  private readonly listeners = new Set<Listener>();

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
   * All indexed tables for the scope (TTL-cached per profile, deduped
   * in-flight). The inventory endpoint already carries effective
   * descriptions, so table-level features need no further calls.
   */
  async getTables(scope: CatalogScope = {}): Promise<readonly TableMeta[]> {
    const key = scopeKey(scope);
    const entry = this.tablesByScope.get(key);
    if (entry && Date.now() - entry.fetchedAt < this.ttlMs) return entry.tables;
    let inFlight = this.tableFetches.get(key);
    if (!inFlight) {
      inFlight = this.fetchTables(scope, key).finally(() => {
        this.tableFetches.delete(key);
      });
      this.tableFetches.set(key, inFlight);
    }
    return inFlight;
  }

  /** Warmed tables across every fetched scope — synchronous, for the
   *  hover hot path. Profiles can index the same schema.table, so the
   *  union is deduped on profile+schema+name. */
  get warmTables(): readonly TableMeta[] {
    const seen = new Set<string>();
    const union: TableMeta[] = [];
    for (const entry of this.tablesByScope.values()) {
      for (const table of entry.tables) {
        const key = `${table.profile ?? ""}|${table.schema.toLowerCase()}|${table.name.toLowerCase()}`;
        if (seen.has(key)) continue;
        seen.add(key);
        union.push(table);
      }
    }
    return union;
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
    // Mark stale rather than delete: warmTables keeps answering from
    // the previous snapshot (hover stays responsive) while the next
    // getTables() refetches fresh data.
    if (scope) {
      const entry = this.tablesByScope.get(scopeKey(scope));
      if (entry) entry.fetchedAt = 0;
    } else {
      for (const entry of this.tablesByScope.values()) entry.fetchedAt = 0;
    }
    this.columnsByTable.clear();
    for (const listener of this.listeners) listener(scope);
  }

  private async fetchTables(scope: CatalogScope, key: string): Promise<TableMeta[]> {
    const rows = await this.client.catalog.inventory(scope);
    const tables = rows.map((row) => toTableMeta(row, scope.profile));
    this.tablesByScope.set(key, { tables, fetchedAt: Date.now() });
    return tables;
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

function scopeKey(scope: CatalogScope): string {
  // Database participates in the key so per-database inventories
  // (profile → database → schema tree level) cache independently.
  return `${scope.profile ?? ""}|${scope.database ?? ""}`;
}

function toTableMeta(row: InventoryTable, scopeProfile?: string): TableMeta {
  const meta: TableMeta = {
    schema: row.schema_name,
    name: row.table_name,
  };
  // Inventory rows don't always echo the profile back — fall back to
  // the scope the fetch was made for so tree nodes and deep links
  // stay profile-qualified.
  const profile = row.db_profile || scopeProfile;
  if (profile) meta.profile = profile;
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
