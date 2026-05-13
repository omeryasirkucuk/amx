/**
 * ScopeTree — cascading Profile → Database → Schema → Table → Column picker.
 *
 * Consumed by the Schedules dialog (and a follow-up will adopt it on
 * the RunNew page). The component is stateless from the caller's
 * perspective: the parent owns the ``picks`` array and gets an
 * ``onChange`` whenever the user clicks anywhere. The parent then
 * compiles the picks into whatever wire shape it needs
 * (``scope_json`` for schedules, a different shape for live runs).
 *
 * UX rules:
 *   * Empty schemas list → ``{mode:'all'}`` on the wire (DB-wide).
 *   * Schema ticked, no tables under it → all tables in that schema.
 *   * Table ticked, no columns under it → all columns in that table.
 *   * Column ticked → only those columns.
 *
 * The hierarchy is purely additive: ticking deeper narrows scope;
 * un-ticking deeper widens it back to "everything below". The user
 * never has to think about a separate "mode" toggle.
 */

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronDown, ChevronRight } from "lucide-react";

import { api, apiFetch } from "../lib/api";
import { cn } from "../lib/cn";
import { Skeleton } from "./ui";

export interface TablePick {
  table: string;
  /** Empty = all columns. */
  columns: string[];
}

export interface SchemaPick {
  schema: string;
  /** Empty = all tables in this schema. */
  tables: TablePick[];
}

export interface ScopeTreeProps {
  /** DB profile name. Mandatory — picker is hidden until parent supplies one. */
  dbProfile: string;
  /** Selected ``{database / catalog}`` for the profile. Mandatory before the
   * schemas list can render. */
  database: string;
  /** Whether the selected backend treats ``database`` as a catalog. */
  isCatalogBackend: boolean;
  /** Current selection — controlled. */
  picks: SchemaPick[];
  onChange: (next: SchemaPick[]) => void;
}

interface SchemasResponse {
  /** Flat list of schema names. */
  schemas: string[];
}

interface AssetRow {
  name: string;
  kind: string;
}

interface AssetsResponse {
  schema: string;
  assets: AssetRow[];
}

interface ColumnRow {
  name: string;
  dtype: string;
}

interface ColumnsResponse {
  columns: ColumnRow[];
}

function findSchema(picks: SchemaPick[], schema: string): SchemaPick | undefined {
  return picks.find((p) => p.schema === schema);
}

function findTable(
  schemaPick: SchemaPick | undefined,
  table: string,
): TablePick | undefined {
  return schemaPick?.tables.find((t) => t.table === table);
}

export default function ScopeTree({
  dbProfile,
  database,
  isCatalogBackend,
  picks,
  onChange,
}: ScopeTreeProps) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const schemasQ = useQuery({
    queryKey: ["scope-tree-schemas", dbProfile, database, isCatalogBackend],
    queryFn: () => {
      const param = isCatalogBackend ? "catalog" : "database";
      return apiFetch<SchemasResponse>(
        `/api/live/schemas?profile=${encodeURIComponent(dbProfile)}&${param}=${encodeURIComponent(database)}`,
      );
    },
    enabled: Boolean(dbProfile && database),
  });

  function toggleSchema(name: string) {
    const existing = findSchema(picks, name);
    if (existing) {
      onChange(picks.filter((p) => p.schema !== name));
      // Collapse when un-ticked.
      const next = new Set(expanded);
      next.delete(`schema:${name}`);
      setExpanded(next);
    } else {
      onChange([...picks, { schema: name, tables: [] }]);
      // Auto-expand when ticked so the user immediately sees the table list.
      const next = new Set(expanded);
      next.add(`schema:${name}`);
      setExpanded(next);
    }
  }

  function toggleSchemaExpanded(name: string) {
    const key = `schema:${name}`;
    const next = new Set(expanded);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setExpanded(next);
  }

  function toggleTable(schema: string, table: string) {
    onChange(
      picks.map((p) => {
        if (p.schema !== schema) return p;
        const has = p.tables.some((t) => t.table === table);
        if (has) {
          return { ...p, tables: p.tables.filter((t) => t.table !== table) };
        }
        return { ...p, tables: [...p.tables, { table, columns: [] }] };
      }),
    );
  }

  function toggleTableExpanded(schema: string, table: string) {
    const key = `table:${schema}.${table}`;
    const next = new Set(expanded);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setExpanded(next);
  }

  function toggleColumn(schema: string, table: string, column: string) {
    onChange(
      picks.map((p) => {
        if (p.schema !== schema) return p;
        return {
          ...p,
          tables: p.tables.map((t) => {
            if (t.table !== table) return t;
            const has = t.columns.includes(column);
            return {
              ...t,
              columns: has
                ? t.columns.filter((c) => c !== column)
                : [...t.columns, column],
            };
          }),
        };
      }),
    );
  }

  if (!dbProfile || !database) {
    return (
      <p className="rounded-md border border-border bg-surface-muted px-3 py-2 text-xs text-ink-dim">
        Pick a DB profile and database first; then the schema tree
        appears here.
      </p>
    );
  }

  if (schemasQ.isLoading) {
    return (
      <div className="space-y-1.5">
        <Skeleton className="h-3 w-1/3" />
        <Skeleton className="h-3 w-1/2" />
        <Skeleton className="h-3 w-2/5" />
      </div>
    );
  }
  if (schemasQ.isError) {
    return (
      <p className="rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-xs text-critical">
        Could not load schemas:{" "}
        {(schemasQ.error as Error).message}
      </p>
    );
  }
  const schemas = schemasQ.data?.schemas ?? [];
  if (!schemas.length) {
    return (
      <p className="rounded-md border border-border bg-surface-muted px-3 py-2 text-xs text-ink-dim">
        No schemas visible on "{dbProfile}".
      </p>
    );
  }

  return (
    <div className="rounded-md border border-border bg-surface-raised">
      <div className="flex items-center justify-between px-3 py-2 text-xs text-ink-dim">
        <span>
          {picks.length === 0
            ? `No tick = entire database (${schemas.length} schemas)`
            : `${picks.length} schema${picks.length === 1 ? "" : "s"} ticked`}
        </span>
        {picks.length > 0 && (
          <button
            type="button"
            onClick={() => onChange([])}
            className="rounded border border-border px-2 py-0.5 hover:bg-surface-muted"
          >
            Clear
          </button>
        )}
      </div>
      <div className="max-h-72 overflow-auto border-t border-border px-2 py-2">
        {schemas.map((name) => {
          const schemaPick = findSchema(picks, name);
          const isOpen = expanded.has(`schema:${name}`);
          return (
            <SchemaRow
              key={name}
              dbProfile={dbProfile}
              database={database}
              isCatalogBackend={isCatalogBackend}
              schemaName={name}
              checked={Boolean(schemaPick)}
              open={Boolean(schemaPick) && isOpen}
              schemaPick={schemaPick}
              onToggleCheck={() => toggleSchema(name)}
              onToggleOpen={() => toggleSchemaExpanded(name)}
              onToggleTable={(t) => toggleTable(name, t)}
              onToggleTableExpanded={(t) => toggleTableExpanded(name, t)}
              tableExpanded={(t) => expanded.has(`table:${name}.${t}`)}
              onToggleColumn={(t, c) => toggleColumn(name, t, c)}
            />
          );
        })}
      </div>
    </div>
  );
}

function SchemaRow(props: {
  dbProfile: string;
  database: string;
  isCatalogBackend: boolean;
  schemaName: string;
  checked: boolean;
  open: boolean;
  schemaPick: SchemaPick | undefined;
  onToggleCheck: () => void;
  onToggleOpen: () => void;
  onToggleTable: (table: string) => void;
  onToggleTableExpanded: (table: string) => void;
  tableExpanded: (table: string) => boolean;
  onToggleColumn: (table: string, column: string) => void;
}) {
  return (
    <div className="text-sm">
      <div className="flex items-center gap-1 rounded px-1 py-0.5 hover:bg-surface-muted">
        {props.checked ? (
          <button
            type="button"
            onClick={props.onToggleOpen}
            className="text-ink-dim hover:text-ink"
            aria-label="toggle"
          >
            {props.open ? (
              <ChevronDown size={14} />
            ) : (
              <ChevronRight size={14} />
            )}
          </button>
        ) : (
          <span className="inline-block w-[14px]" />
        )}
        <label className="flex flex-1 cursor-pointer items-center gap-2 select-none">
          <input
            type="checkbox"
            checked={props.checked}
            onChange={props.onToggleCheck}
          />
          <span className="font-mono text-xs text-ink">{props.schemaName}</span>
          {props.checked && props.schemaPick?.tables.length === 0 && (
            <span className="text-[10px] text-ink-dim">(all tables)</span>
          )}
        </label>
      </div>
      {props.open && (
        <SchemaTables
          dbProfile={props.dbProfile}
          database={props.database}
          isCatalogBackend={props.isCatalogBackend}
          schemaName={props.schemaName}
          schemaPick={props.schemaPick}
          onToggleTable={props.onToggleTable}
          onToggleTableExpanded={props.onToggleTableExpanded}
          tableExpanded={props.tableExpanded}
          onToggleColumn={props.onToggleColumn}
        />
      )}
    </div>
  );
}

function SchemaTables(props: {
  dbProfile: string;
  database: string;
  isCatalogBackend: boolean;
  schemaName: string;
  schemaPick: SchemaPick | undefined;
  onToggleTable: (table: string) => void;
  onToggleTableExpanded: (table: string) => void;
  tableExpanded: (table: string) => boolean;
  onToggleColumn: (table: string, column: string) => void;
}) {
  const assetsQ = useQuery({
    queryKey: [
      "scope-tree-assets",
      props.dbProfile,
      props.database,
      props.isCatalogBackend,
      props.schemaName,
    ],
    queryFn: () => {
      const param = props.isCatalogBackend ? "catalog" : "database";
      return apiFetch<AssetsResponse>(
        `/api/live/schemas/${encodeURIComponent(props.schemaName)}/assets?profile=${encodeURIComponent(props.dbProfile)}&${param}=${encodeURIComponent(props.database)}`,
      );
    },
  });

  if (assetsQ.isLoading) {
    return (
      <div className="ml-7 space-y-1 py-1">
        <Skeleton className="h-3 w-1/3" />
        <Skeleton className="h-3 w-1/4" />
      </div>
    );
  }
  if (assetsQ.isError) {
    return (
      <p className="ml-7 py-1 text-xs text-critical">
        Could not load tables for "{props.schemaName}".
      </p>
    );
  }
  const tables = (assetsQ.data?.assets ?? []).filter(
    (a) => a.kind !== "column",
  );
  if (!tables.length) {
    return (
      <p className="ml-7 py-1 text-[11px] text-ink-dim">
        No tables in "{props.schemaName}".
      </p>
    );
  }
  return (
    <div className="ml-5 border-l border-border pl-2">
      {tables.map((t) => {
        const picked = findTable(props.schemaPick, t.name);
        const open = picked && props.tableExpanded(t.name);
        return (
          <div key={t.name} className="text-sm">
            <div className="flex items-center gap-1 rounded px-1 py-0.5 hover:bg-surface-muted">
              {picked ? (
                <button
                  type="button"
                  onClick={() => props.onToggleTableExpanded(t.name)}
                  className="text-ink-dim hover:text-ink"
                  aria-label="toggle"
                >
                  {open ? (
                    <ChevronDown size={12} />
                  ) : (
                    <ChevronRight size={12} />
                  )}
                </button>
              ) : (
                <span className="inline-block w-[12px]" />
              )}
              <label className="flex flex-1 cursor-pointer items-center gap-2 select-none">
                <input
                  type="checkbox"
                  checked={Boolean(picked)}
                  onChange={() => props.onToggleTable(t.name)}
                />
                <span className="font-mono text-[11px] text-ink">{t.name}</span>
                {picked && picked.columns.length === 0 && (
                  <span className="text-[10px] text-ink-dim">
                    (all columns)
                  </span>
                )}
              </label>
            </div>
            {open && (
              <TableColumns
                dbProfile={props.dbProfile}
                database={props.database}
                isCatalogBackend={props.isCatalogBackend}
                schemaName={props.schemaName}
                tableName={t.name}
                tablePick={picked}
                onToggleColumn={(c) => props.onToggleColumn(t.name, c)}
              />
            )}
          </div>
        );
      })}
    </div>
  );
}

function TableColumns(props: {
  dbProfile: string;
  database: string;
  isCatalogBackend: boolean;
  schemaName: string;
  tableName: string;
  tablePick: TablePick | undefined;
  onToggleColumn: (column: string) => void;
}) {
  const columnsQ = useQuery({
    queryKey: [
      "scope-tree-columns",
      props.dbProfile,
      props.database,
      props.isCatalogBackend,
      props.schemaName,
      props.tableName,
    ],
    queryFn: () => {
      const param = props.isCatalogBackend ? "catalog" : "database";
      return apiFetch<ColumnsResponse>(
        `/api/live/schemas/${encodeURIComponent(props.schemaName)}/tables/${encodeURIComponent(props.tableName)}/columns?profile=${encodeURIComponent(props.dbProfile)}&${param}=${encodeURIComponent(props.database)}`,
      );
    },
  });

  if (columnsQ.isLoading) {
    return (
      <div className="ml-7 py-1">
        <Skeleton className="h-3 w-1/4" />
      </div>
    );
  }
  if (columnsQ.isError) {
    return (
      <p className="ml-7 py-1 text-[11px] text-critical">
        Could not load columns for "{props.tableName}".
      </p>
    );
  }
  const cols = columnsQ.data?.columns ?? [];
  if (!cols.length) {
    return null;
  }
  return (
    <div className="ml-5 border-l border-border pl-2 py-1">
      <div className="flex flex-wrap gap-1.5">
        {cols.map((c) => {
          const on =
            !props.tablePick ||
            props.tablePick.columns.length === 0 ||
            props.tablePick.columns.includes(c.name);
          return (
            <button
              key={c.name}
              type="button"
              onClick={() => props.onToggleColumn(c.name)}
              className={cn(
                "rounded-md border px-2 py-0.5 font-mono text-[10px] transition-colors duration-fast",
                on
                  ? "border-accent/40 bg-accent/10 text-ink"
                  : "border-border text-ink-dim hover:border-accent/40 hover:text-ink",
              )}
            >
              {c.name}
            </button>
          );
        })}
      </div>
    </div>
  );
}

/** Compile picks into the canonical scope_json shape Schedules uses. */
export function picksToScopeJson(picks: SchemaPick[]): Record<string, unknown> {
  if (!picks.length) return { mode: "all" };
  // Any column-level selection? -> columns mode (most specific).
  const explicitColumns: { schema: string; table: string; column: string }[] =
    [];
  for (const p of picks) {
    for (const t of p.tables) {
      for (const c of t.columns) {
        explicitColumns.push({
          schema: p.schema,
          table: t.table,
          column: c,
        });
      }
    }
  }
  if (explicitColumns.length) {
    return { mode: "columns", columns: explicitColumns };
  }
  // Any explicit table selection? -> tables mode.
  const explicitTables: { schema: string; table: string }[] = [];
  let anyTablePicked = false;
  for (const p of picks) {
    for (const t of p.tables) {
      anyTablePicked = true;
      explicitTables.push({ schema: p.schema, table: t.table });
    }
  }
  if (anyTablePicked) {
    return { mode: "tables", tables: explicitTables };
  }
  // Only schemas ticked, no table specificity -> schemas mode.
  return { mode: "schemas", schemas: picks.map((p) => p.schema) };
}

// ``api`` import kept so ScopeTree can be extended to use typed wrappers
// without touching imports; the live calls above hand-roll the URL only
// because of the optional ``database`` / ``catalog`` query parameter.
void api;
