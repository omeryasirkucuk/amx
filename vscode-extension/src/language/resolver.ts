// Resolves scanner tokens against the warmed catalog. The catalog is
// the only disambiguator: chains that match nothing stay unannotated,
// CTE names never resolve, and cold column sets trigger a background
// load instead of blocking. Pure module — no vscode imports — so it
// tests in plain vitest against a CatalogCache with a stubbed client.
import { CatalogCache, type ColumnMeta, type TableMeta } from "../api/catalogCache";
import type { CatalogScope } from "../api/types";
import type {
  ColumnCandidate,
  ResolvedColumnRef,
  ResolvedRef,
  ResolvedTableRef,
  ScanResult,
  SqlToken,
  StatementInfo,
} from "./types";

/** The slice of CatalogCache the resolver consumes (test seam). */
export interface ResolverCatalog {
  readonly warmTables: readonly TableMeta[];
  warmColumns(tableKey: string): readonly ColumnMeta[] | undefined;
  getTables(scope?: CatalogScope): Promise<readonly TableMeta[]>;
  getColumns(schema: string, table: string, profile?: string): Promise<readonly ColumnMeta[]>;
  onDidInvalidate(listener: (scope: CatalogScope | undefined) => void): { dispose(): void };
}

/** Column lookup against a not-yet-warmed table. */
const LOADING = Symbol("loading");

export class CatalogResolver {
  private dirty = true;
  private tablesByName = new Map<string, TableMeta[]>();
  private tablesByQualified = new Map<string, TableMeta>();
  private readonly pendingColumnLoads = new Set<string>();
  private readonly invalidateSub: { dispose(): void };

  constructor(private readonly catalog: ResolverCatalog) {
    this.invalidateSub = catalog.onDidInvalidate(() => {
      this.dirty = true;
      this.pendingColumnLoads.clear();
    });
  }

  dispose(): void {
    this.invalidateSub.dispose();
  }

  /** Load tables for the scope and rebuild the index from them. */
  async ensureWarm(scope: CatalogScope = {}): Promise<void> {
    await this.catalog.getTables(scope);
    this.dirty = true;
    this.rebuildIfDirty();
  }

  /** Resolve one scanner token against the warmed catalog. */
  resolve(scan: ScanResult, token: SqlToken): ResolvedRef | undefined {
    this.rebuildIfDirty();
    if (token.context === "cteDef" || token.context === "aliasDef") return undefined;
    const statement = scan.statements[token.statementIndex];
    const parts = token.parts.map((part) => part.text.toLowerCase());
    const head = parts[0];
    if (head === undefined) return undefined;
    if (parts.length === 1 && statement?.ctes.has(head)) return undefined;
    if (parts.length >= 2 && statement) {
      const aliasTarget = statement.aliases.get(head);
      if (aliasTarget !== undefined) {
        const expanded = [...aliasTarget.split("."), ...parts.slice(1)];
        if (statement.ctes.has(expanded[0] ?? "")) return undefined;
        return this.resolveQualified(expanded, true);
      }
    }
    if (parts.length >= 2) return this.resolveQualified(parts, false);
    if (token.context === "tablePosition") return this.resolveBareTable(head);
    return this.resolveBareColumn(head, scan, token.statementIndex);
  }

  /**
   * Resolve a dotted chain to a table for completion: alias target,
   * `schema.table` qualified, or a uniquely named bare table.
   */
  tableForChain(parts: readonly string[], statement?: StatementInfo): TableMeta | undefined {
    this.rebuildIfDirty();
    const lower = parts.map((part) => part.toLowerCase());
    const head = lower[0];
    if (head === undefined) return undefined;
    if (statement?.ctes.has(head)) return undefined;
    if (lower.length === 1) {
      const aliasTarget = statement?.aliases.get(head);
      if (aliasTarget !== undefined) {
        if (statement?.ctes.has(aliasTarget.split(".")[0] ?? "")) return undefined;
        return this.tableForChain(aliasTarget.split("."));
      }
      const named = this.tablesByName.get(head);
      return named?.length === 1 ? named[0] : undefined;
    }
    if (lower.length === 2) return this.tablesByQualified.get(`${lower[0]}.${lower[1]}`);
    if (lower.length === 3) {
      const table = this.tablesByQualified.get(`${lower[1]}.${lower[2]}`);
      if (table && table.database?.toLowerCase() === lower[0]) return table;
    }
    return undefined;
  }

  /** Every warmed table (for table-position completion). */
  allTables(): readonly TableMeta[] {
    this.rebuildIfDirty();
    return this.catalog.warmTables;
  }

  /** Catalog tables sharing a bare name (case-insensitive). */
  tablesNamed(name: string): readonly TableMeta[] {
    this.rebuildIfDirty();
    return this.tablesByName.get(name.toLowerCase()) ?? [];
  }

  /**
   * Tables referenced in table position within one statement,
   * deduplicated, CTEs excluded, ambiguous bare names skipped.
   */
  statementTables(scan: ScanResult, statementIndex: number): TableMeta[] {
    this.rebuildIfDirty();
    const seen = new Map<string, TableMeta>();
    for (const token of scan.tokens) {
      if (token.statementIndex !== statementIndex || token.context !== "tablePosition") continue;
      const resolved = this.resolve(scan, token);
      if (resolved?.kind === "table" && resolved.confidence === "exact") {
        const key = CatalogCache.tableKey(resolved.table.schema, resolved.table.name);
        seen.set(key, resolved.table);
      }
    }
    return [...seen.values()];
  }

  /** Async column load for one table (completion's dotted path). */
  columnsFor(table: TableMeta): Promise<readonly ColumnMeta[]> {
    return this.catalog.getColumns(table.schema, table.name, table.profile);
  }

  /** Warm columns of one table; undefined when not loaded yet. */
  warmColumnsFor(table: TableMeta): readonly ColumnMeta[] | undefined {
    return this.catalog.warmColumns(CatalogCache.tableKey(table.schema, table.name));
  }

  // --- internals ---

  private rebuildIfDirty(): void {
    if (!this.dirty) return;
    this.dirty = false;
    this.tablesByName = new Map();
    this.tablesByQualified = new Map();
    for (const table of this.catalog.warmTables) {
      const nameKey = table.name.toLowerCase();
      const named = this.tablesByName.get(nameKey);
      if (named) named.push(table);
      else this.tablesByName.set(nameKey, [table]);
      const qualified = CatalogCache.tableKey(table.schema, table.name);
      if (!this.tablesByQualified.has(qualified)) this.tablesByQualified.set(qualified, table);
    }
  }

  private resolveQualified(parts: string[], preferColumn: boolean): ResolvedRef | undefined {
    if (parts.length === 2) {
      const [a, b] = parts as [string, string];
      const asTable = (): ResolvedTableRef | undefined => {
        const table = this.tablesByQualified.get(`${a}.${b}`);
        return table ? exactTable(table) : undefined;
      };
      const asColumn = (): ResolvedColumnRef | undefined => {
        const named = this.tablesByName.get(a);
        if (named?.length !== 1) return undefined;
        const owner = named[0]!;
        const column = this.lookupColumn(owner, b);
        if (column === LOADING || column === undefined) return undefined;
        return exactColumn(owner, column, preferColumn ? "exact" : "weak");
      };
      return preferColumn ? (asColumn() ?? asTable()) : (asTable() ?? asColumn());
    }
    if (parts.length === 3) {
      const [a, b, c] = parts as [string, string, string];
      const owner = this.tablesByQualified.get(`${a}.${b}`);
      if (owner) {
        const column = this.lookupColumn(owner, c);
        if (column !== LOADING && column !== undefined) return exactColumn(owner, column, "exact");
        if (column === LOADING) return undefined;
      }
      const dbQualified = this.tablesByQualified.get(`${b}.${c}`);
      if (dbQualified && dbQualified.database?.toLowerCase() === a) return exactTable(dbQualified);
      return undefined;
    }
    if (parts.length === 4) {
      const [a, b, c, d] = parts as [string, string, string, string];
      const owner = this.tablesByQualified.get(`${b}.${c}`);
      if (!owner || owner.database?.toLowerCase() !== a) return undefined;
      const column = this.lookupColumn(owner, d);
      if (column === LOADING || column === undefined) return undefined;
      return exactColumn(owner, column, "exact");
    }
    return undefined;
  }

  private resolveBareTable(name: string): ResolvedTableRef | undefined {
    const candidates = this.tablesByName.get(name);
    if (!candidates || candidates.length === 0) return undefined;
    if (candidates.length === 1) return exactTable(candidates[0]!);
    return {
      kind: "table",
      confidence: "ambiguous",
      table: candidates[0]!,
      candidates: [...candidates],
    };
  }

  private resolveBareColumn(
    name: string,
    scan: ScanResult,
    statementIndex: number,
  ): ResolvedColumnRef | undefined {
    const hits: ColumnCandidate[] = [];
    for (const table of this.statementTables(scan, statementIndex)) {
      const column = this.lookupColumn(table, name);
      if (column !== LOADING && column !== undefined) hits.push({ table, column });
    }
    const first = hits[0];
    if (!first) return undefined;
    return {
      kind: "column",
      confidence: hits.length === 1 ? "exact" : "ambiguous",
      table: first.table,
      column: first.column,
      candidates: hits,
    };
  }

  /**
   * Synchronous column lookup. When the table's columns aren't warm
   * yet, kick off a background fetch (deduplicated) and report
   * LOADING — the next pass over the document sees the warm data.
   */
  private lookupColumn(table: TableMeta, columnName: string): ColumnMeta | typeof LOADING | undefined {
    const key = CatalogCache.tableKey(table.schema, table.name);
    const columns = this.catalog.warmColumns(key);
    if (!columns) {
      if (!this.pendingColumnLoads.has(key)) {
        this.pendingColumnLoads.add(key);
        void this.catalog.getColumns(table.schema, table.name, table.profile).catch(() => {
          // Background warm-up only — failures simply leave the
          // column unresolved; the next invalidation retries.
          this.pendingColumnLoads.delete(key);
        });
      }
      return LOADING;
    }
    return columns.find((column) => column.name.toLowerCase() === columnName.toLowerCase());
  }
}

function exactTable(table: TableMeta): ResolvedTableRef {
  return { kind: "table", confidence: "exact", table, candidates: [table] };
}

function exactColumn(
  table: TableMeta,
  column: ColumnMeta,
  confidence: "exact" | "weak",
): ResolvedColumnRef {
  return { kind: "column", confidence, table, column, candidates: [{ table, column }] };
}
