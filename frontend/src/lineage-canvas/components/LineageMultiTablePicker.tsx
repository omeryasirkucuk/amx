/**
 * Multi-select asset tree for the Lineage Add Table modal.
 *
 * Same shape as the Pages wizard's ``DbAssetTree`` (profile → database
 * → schema → table), but every leaf carries a checkbox and the parent
 * rows cascade: ticking a schema selects every table under it,
 * ticking a database expands every schema first and then ticks every
 * table. The modal owns the selected set; this component just
 * surfaces toggles.
 *
 * Reads exclusively from ``/api/db/cache/tree/*`` so drilling into a
 * 5,000-table workspace stays a local SQLite query.
 */

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import {
  Check,
  ChevronDown,
  ChevronRight,
  Database,
  Minus,
} from "lucide-react";
import clsx from "clsx";

import { api, apiFetch } from "../../lib/api";

interface DbProfileSummary {
  name: string;
  backend?: string;
}
interface DbProfilesResponse {
  profiles: DbProfileSummary[];
}

/** Stable shape used by the modal to track selection + backend lookup. */
export interface PickedTable {
  profile: string;
  backend: string;
  database: string;
  schema: string;
  table: string;
}

interface Props {
  /** Currently-checked tables keyed by ``profile/database/schema/table``. */
  selected: Map<string, PickedTable>;
  onChange: (next: Map<string, PickedTable>) => void;
}

export function tableKey(p: PickedTable): string {
  return `${p.profile}/${p.database}/${p.schema}/${p.table}`;
}

export function LineageMultiTablePicker({ selected, onChange }: Props) {
  const profilesQ = useQuery({
    queryKey: ["lineage", "asset-picker", "db-profiles"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
    staleTime: 30_000,
  });
  if (profilesQ.isLoading) return <Hint>Loading DB profiles…</Hint>;
  if (profilesQ.error)
    return <ErrorLine>{(profilesQ.error as Error).message}</ErrorLine>;
  const profiles = profilesQ.data?.profiles ?? [];
  if (profiles.length === 0)
    return <Hint>No DB profiles configured.</Hint>;
  return (
    <div className="space-y-2">
      {profiles.map((p) => (
        <ProfileCard
          key={p.name}
          profile={p.name}
          backend={p.backend ?? "unknown"}
          selected={selected}
          onChange={onChange}
        />
      ))}
    </div>
  );
}

interface NodeProps {
  selected: Map<string, PickedTable>;
  onChange: (next: Map<string, PickedTable>) => void;
}

function ProfileCard({
  profile,
  backend,
  selected,
  onChange,
}: NodeProps & { profile: string; backend: string }) {
  const [open, setOpen] = useState(false);
  const selectedCount = Array.from(selected.keys()).filter((k) =>
    k.startsWith(`${profile}/`),
  ).length;
  return (
    <div className="rounded-md border border-surface-border bg-surface">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center justify-between gap-2 px-3 py-2 text-sm"
      >
        <span className="inline-flex min-w-0 items-center gap-1.5 font-medium text-ink">
          {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          <Database size={14} />
          <span className="truncate">{profile}</span>
          <span className="text-[11px] text-fg-muted">({backend})</span>
        </span>
        {selectedCount > 0 && (
          <span className="rounded-full bg-accent-soft px-1.5 text-[10px] font-semibold text-accent-ink">
            {selectedCount} selected
          </span>
        )}
      </button>
      {open && (
        <div className="border-t border-surface-border p-2">
          <DatabaseList
            profile={profile}
            backend={backend}
            selected={selected}
            onChange={onChange}
          />
        </div>
      )}
    </div>
  );
}

function DatabaseList({
  profile,
  backend,
  selected,
  onChange,
}: NodeProps & { profile: string; backend: string }) {
  const q = useQuery({
    queryKey: ["lineage", "tree-cache", profile, "databases"],
    queryFn: () => api.dbCacheTreeDatabases(profile),
    staleTime: 30_000,
  });
  if (q.isLoading) return <Hint indent={1}>Loading…</Hint>;
  if (q.error) return <ErrorLine indent={1}>{(q.error as Error).message}</ErrorLine>;
  if (!q.data?.synced) return <Hint indent={1}>Profile not synced.</Hint>;
  const items = q.data.items;
  if (items.length === 0) return <Hint indent={1}>No databases in cache.</Hint>;
  return (
    <ul className="space-y-0.5">
      {items.map((it) => (
        <DatabaseNode
          key={it.name}
          profile={profile}
          backend={backend}
          database={it.name}
          selected={selected}
          onChange={onChange}
        />
      ))}
    </ul>
  );
}

function DatabaseNode({
  profile,
  backend,
  database,
  selected,
  onChange,
}: NodeProps & { profile: string; backend: string; database: string }) {
  const [open, setOpen] = useState(false);
  const qc = useQueryClient();
  const prefix = `${profile}/${database}/`;
  const matched = Array.from(selected.keys()).filter((k) => k.startsWith(prefix));
  const tristate = matched.length === 0 ? "off" : "on";
  // We do not eagerly expand the schema tree to know the "total" count,
  // so a partial tristate appears only when the user has explicitly
  // mixed picks (the schema/table rows compute their own indeterminate
  // state from cached data).
  async function toggleDatabase() {
    if (matched.length > 0) {
      // Uncheck: drop every selected table under this database.
      const next = new Map(selected);
      for (const k of matched) next.delete(k);
      onChange(next);
      return;
    }
    // Check: walk every schema (cache-only) and every table inside it,
    // then add them all. Cheap because the cache endpoints are local
    // SQLite reads.
    const schemas = await qc.fetchQuery({
      queryKey: ["lineage", "tree-cache", profile, database, "schemas"],
      queryFn: () => api.dbCacheTreeSchemas(profile, database),
      staleTime: 30_000,
    });
    if (!schemas?.items) return;
    const tableLists = await Promise.all(
      schemas.items.map((s) =>
        qc.fetchQuery({
          queryKey: [
            "lineage",
            "tree-cache",
            profile,
            database,
            s.name,
            "tables",
          ],
          queryFn: () => api.dbCacheTreeTables(profile, database, s.name),
          staleTime: 30_000,
        }),
      ),
    );
    const next = new Map(selected);
    schemas.items.forEach((s, idx) => {
      const tables = tableLists[idx]?.items ?? [];
      for (const t of tables) {
        const p: PickedTable = {
          profile,
          backend,
          database,
          schema: s.name,
          table: t.name,
        };
        next.set(tableKey(p), p);
      }
    });
    onChange(next);
    setOpen(true);
  }
  return (
    <li>
      <Row
        indent={1}
        label={database}
        tristate={tristate}
        onToggleSelect={toggleDatabase}
        canExpand
        open={open}
        onToggleOpen={() => setOpen((o) => !o)}
      />
      {open && (
        <SchemaList
          profile={profile}
          backend={backend}
          database={database}
          selected={selected}
          onChange={onChange}
        />
      )}
    </li>
  );
}

function SchemaList({
  profile,
  backend,
  database,
  selected,
  onChange,
}: NodeProps & { profile: string; backend: string; database: string }) {
  const q = useQuery({
    queryKey: ["lineage", "tree-cache", profile, database, "schemas"],
    queryFn: () => api.dbCacheTreeSchemas(profile, database),
    staleTime: 30_000,
  });
  if (q.isLoading) return <Hint indent={2}>Loading schemas…</Hint>;
  if (q.error) return <ErrorLine indent={2}>{(q.error as Error).message}</ErrorLine>;
  const items = q.data?.items ?? [];
  if (items.length === 0) return <Hint indent={2}>No schemas in cache.</Hint>;
  return (
    <ul className="space-y-0.5">
      {items.map((s) => (
        <SchemaNode
          key={s.name}
          profile={profile}
          backend={backend}
          database={database}
          schema={s.name}
          selected={selected}
          onChange={onChange}
        />
      ))}
    </ul>
  );
}

function SchemaNode({
  profile,
  backend,
  database,
  schema,
  selected,
  onChange,
}: NodeProps & {
  profile: string;
  backend: string;
  database: string;
  schema: string;
}) {
  const [open, setOpen] = useState(false);
  const qc = useQueryClient();
  const prefix = `${profile}/${database}/${schema}/`;
  const matched = Array.from(selected.keys()).filter((k) => k.startsWith(prefix));
  const tristate = matched.length === 0 ? "off" : "on";
  async function toggleSchema() {
    if (matched.length > 0) {
      const next = new Map(selected);
      for (const k of matched) next.delete(k);
      onChange(next);
      return;
    }
    const tables = await qc.fetchQuery({
      queryKey: ["lineage", "tree-cache", profile, database, schema, "tables"],
      queryFn: () => api.dbCacheTreeTables(profile, database, schema),
      staleTime: 30_000,
    });
    if (!tables?.items) return;
    const next = new Map(selected);
    for (const t of tables.items) {
      const p: PickedTable = {
        profile,
        backend,
        database,
        schema,
        table: t.name,
      };
      next.set(tableKey(p), p);
    }
    onChange(next);
    setOpen(true);
  }
  return (
    <li>
      <Row
        indent={2}
        label={schema}
        tristate={tristate}
        onToggleSelect={toggleSchema}
        canExpand
        open={open}
        onToggleOpen={() => setOpen((o) => !o)}
      />
      {open && (
        <TableList
          profile={profile}
          backend={backend}
          database={database}
          schema={schema}
          selected={selected}
          onChange={onChange}
        />
      )}
    </li>
  );
}

function TableList({
  profile,
  backend,
  database,
  schema,
  selected,
  onChange,
}: NodeProps & {
  profile: string;
  backend: string;
  database: string;
  schema: string;
}) {
  const q = useQuery({
    queryKey: ["lineage", "tree-cache", profile, database, schema, "tables"],
    queryFn: () => api.dbCacheTreeTables(profile, database, schema),
    staleTime: 30_000,
  });
  if (q.isLoading) return <Hint indent={3}>Loading tables…</Hint>;
  if (q.error) return <ErrorLine indent={3}>{(q.error as Error).message}</ErrorLine>;
  const items = q.data?.items ?? [];
  if (items.length === 0) return <Hint indent={3}>No tables in cache.</Hint>;
  return (
    <ul className="space-y-0.5">
      {items.map((t) => {
        const p: PickedTable = {
          profile,
          backend,
          database,
          schema,
          table: t.name,
        };
        const key = tableKey(p);
        const checked = selected.has(key);
        return (
          <li key={t.name}>
            <Row
              indent={3}
              label={t.name}
              tristate={checked ? "on" : "off"}
              onToggleSelect={() => {
                const next = new Map(selected);
                if (checked) next.delete(key);
                else next.set(key, p);
                onChange(next);
              }}
            />
          </li>
        );
      })}
    </ul>
  );
}

interface RowProps {
  indent: number;
  label: string;
  tristate: "on" | "off" | "partial";
  onToggleSelect: () => void;
  canExpand?: boolean;
  open?: boolean;
  onToggleOpen?: () => void;
}

function Row({
  indent,
  label,
  tristate,
  onToggleSelect,
  canExpand,
  open,
  onToggleOpen,
}: RowProps) {
  return (
    <div
      className="flex items-center gap-1 rounded-sm px-1 py-0.5 text-[12.5px] hover:bg-surface-raised"
      style={{ paddingLeft: 4 + indent * 14 }}
    >
      {canExpand ? (
        <button
          type="button"
          onClick={onToggleOpen}
          aria-label={open ? "Collapse" : "Expand"}
          className="inline-flex h-4 w-4 items-center justify-center text-fg-muted hover:text-ink"
        >
          {open ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
        </button>
      ) : (
        <span className="inline-block h-4 w-4" />
      )}
      <button
        type="button"
        role="checkbox"
        aria-checked={tristate === "on" ? "true" : tristate === "partial" ? "mixed" : "false"}
        onClick={(e) => {
          e.stopPropagation();
          onToggleSelect();
        }}
        className={clsx(
          "inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded border transition",
          tristate === "on" && "border-accent-default bg-accent-default text-accent-ink",
          tristate === "partial" && "border-accent-default bg-accent-soft text-accent-ink",
          tristate === "off" && "border-surface-border bg-transparent text-transparent hover:border-ink/60",
        )}
      >
        {tristate === "on" && <Check size={10} />}
        {tristate === "partial" && <Minus size={10} />}
      </button>
      <span className="ml-1 truncate text-ink">{label}</span>
    </div>
  );
}

function Hint({ children, indent = 0 }: { children: React.ReactNode; indent?: number }) {
  return (
    <div
      className="text-[11px] text-fg-muted"
      style={{ paddingLeft: 4 + indent * 14 }}
    >
      {children}
    </div>
  );
}

function ErrorLine({ children, indent = 0 }: { children: React.ReactNode; indent?: number }) {
  return (
    <div
      className="text-[11px] text-critical"
      style={{ paddingLeft: 4 + indent * 14 }}
    >
      {children}
    </div>
  );
}
