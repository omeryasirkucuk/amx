// Per-asset DB tree picker for the Pages wizard.
// Reads exclusively from the persistent catalog cache — every level
// (database / schema / table / column) is served from a local SQLite
// query, so drilling into a 5,000-table Databricks workspace never
// triggers a live round-trip. Cold cache surfaces an inline "sync
// this profile first" hint instead of silently hanging.

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Check, ChevronDown, ChevronRight, Database } from "lucide-react";

import { api, apiFetch } from "../../lib/api";
import { cn } from "../../lib/cn";
import type { PageAssetRef } from "../../hooks/usePages";

interface DbProfileSummary {
  name: string;
  backend?: string;
}
interface DbProfilesResponse {
  profiles: DbProfileSummary[];
}

interface Props {
  value: PageAssetRef[];
  onChange: (next: PageAssetRef[]) => void;
}

export default function DbAssetTree({ value, onChange }: Props) {
  const profilesQ = useQuery({
    queryKey: ["pages", "asset-picker", "db-profiles"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
    staleTime: 30_000,
  });

  if (profilesQ.isLoading) return <Hint>Loading DB profiles...</Hint>;
  if (profilesQ.error)
    return <ErrorLine>{(profilesQ.error as Error).message}</ErrorLine>;

  const profiles = profilesQ.data?.profiles ?? [];
  if (profiles.length === 0)
    return <Hint>No DB profiles configured. Add one from Settings.</Hint>;

  return (
    <div className="grid gap-2 md:grid-cols-2">
      {profiles.map((p) => (
        <ProfileCard
          key={p.name}
          profile={p.name}
          backend={p.backend ?? "unknown"}
          value={value}
          onChange={onChange}
        />
      ))}
    </div>
  );
}

interface NodeProps {
  value: PageAssetRef[];
  onChange: (next: PageAssetRef[]) => void;
}

function ProfileCard({
  profile,
  backend,
  value,
  onChange,
}: NodeProps & { profile: string; backend: string }) {
  const [open, setOpen] = useState(false);
  const profilePrefix = `${profile}/`;
  const selectedCount = value.filter(
    (a) => a.kind.startsWith("db_") && a.ref.startsWith(profilePrefix),
  ).length;

  return (
    <div className="rounded-md border border-border bg-surface">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        className="flex w-full items-center justify-between gap-2 px-3 py-2 text-sm"
      >
        <span className="inline-flex min-w-0 items-center gap-1.5 font-medium text-ink">
          {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          <Database size={14} />
          <span className="truncate">{profile}</span>
          <span className="text-[11px] text-ink-dim">({backend})</span>
        </span>
        {selectedCount > 0 && (
          <span className="rounded-full bg-accent-soft px-1.5 text-[10px] font-semibold text-accent-ink">
            {selectedCount} selected
          </span>
        )}
      </button>
      {open && (
        <div className="border-t border-border p-2">
          <DatabaseList
            profile={profile}
            value={value}
            onChange={onChange}
          />
        </div>
      )}
    </div>
  );
}

function NotSyncedHint({ indent = 1 }: { indent?: number }) {
  // Cache is empty for this profile. Surfacing the action instead of
  // a vague "nothing here" so the user knows the next step (run a
  // /sync in the REPL or the catalog cache page).
  return (
    <Hint indent={indent}>
      Profile not synced. Run <code className="text-ink-muted">/sync</code> or
      open the DB cache page to populate.
    </Hint>
  );
}

function DatabaseList({ profile, value, onChange }: NodeProps & { profile: string }) {
  const q = useQuery({
    queryKey: ["pages", "tree-cache", profile, "databases"],
    queryFn: () => api.dbCacheTreeDatabases(profile),
    staleTime: 30_000,
  });

  if (q.isLoading) return <Hint indent={1}>Loading...</Hint>;
  if (q.error) return <ErrorLine indent={1}>{(q.error as Error).message}</ErrorLine>;
  const items = q.data?.items ?? [];
  if (!q.data?.synced || items.length === 0) return <NotSyncedHint />;

  return (
    <ul className="space-y-0.5">
      {items.map((item) => (
        <DatabaseNode
          key={item.name}
          profile={profile}
          database={item.name}
          value={value}
          onChange={onChange}
        />
      ))}
    </ul>
  );
}

function DatabaseNode({
  profile,
  database,
  value,
  onChange,
}: NodeProps & { profile: string; database: string }) {
  const [open, setOpen] = useState(false);
  const ref = `${profile}/${database}`;
  const selected = isSelected(value, "db_database", ref);

  return (
    <li>
      <TreeRow
        selected={selected}
        onToggleSelect={() => onChange(toggle(value, "db_database", ref))}
        open={open}
        onToggleOpen={() => setOpen((o) => !o)}
        canExpand
        indent={1}
        label={database}
      />
      {open && (
        <SchemaList
          profile={profile}
          database={database}
          value={value}
          onChange={onChange}
        />
      )}
    </li>
  );
}

function SchemaList({
  profile,
  database,
  value,
  onChange,
}: NodeProps & { profile: string; database: string }) {
  const q = useQuery({
    queryKey: ["pages", "tree-cache", profile, database, "schemas"],
    queryFn: () => api.dbCacheTreeSchemas(profile, database),
    staleTime: 30_000,
  });

  if (q.isLoading) return <Hint indent={2}>Loading schemas...</Hint>;
  if (q.error) return <ErrorLine indent={2}>{(q.error as Error).message}</ErrorLine>;
  const items = q.data?.items ?? [];
  if (items.length === 0) return <Hint indent={2}>No schemas in cache.</Hint>;

  return (
    <ul className="space-y-0.5">
      {items.map((item) => (
        <SchemaNode
          key={item.name}
          profile={profile}
          database={database}
          schema={item.name}
          value={value}
          onChange={onChange}
        />
      ))}
    </ul>
  );
}

function SchemaNode({
  profile,
  database,
  schema,
  value,
  onChange,
}: NodeProps & { profile: string; database: string; schema: string }) {
  const [open, setOpen] = useState(false);
  const ref = `${profile}/${database}/${schema}`;
  const selected = isSelected(value, "db_schema", ref);

  return (
    <li>
      <TreeRow
        selected={selected}
        onToggleSelect={() => onChange(toggle(value, "db_schema", ref))}
        open={open}
        onToggleOpen={() => setOpen((o) => !o)}
        canExpand
        indent={2}
        label={schema}
      />
      {open && (
        <TableList
          profile={profile}
          database={database}
          schema={schema}
          value={value}
          onChange={onChange}
        />
      )}
    </li>
  );
}

function TableList({
  profile,
  database,
  schema,
  value,
  onChange,
}: NodeProps & { profile: string; database: string; schema: string }) {
  const q = useQuery({
    queryKey: ["pages", "tree-cache", profile, database, schema, "tables"],
    queryFn: () => api.dbCacheTreeTables(profile, database, schema),
    staleTime: 30_000,
  });

  if (q.isLoading) return <Hint indent={3}>Loading tables...</Hint>;
  if (q.error) return <ErrorLine indent={3}>{(q.error as Error).message}</ErrorLine>;
  const items = q.data?.items ?? [];
  if (items.length === 0) return <Hint indent={3}>No tables in cache.</Hint>;

  return (
    <ul className="space-y-0.5">
      {items.map((item) => (
        <TableNode
          key={item.name}
          profile={profile}
          database={database}
          schema={schema}
          table={item.name}
          value={value}
          onChange={onChange}
        />
      ))}
    </ul>
  );
}

function TableNode({
  profile,
  database,
  schema,
  table,
  value,
  onChange,
}: NodeProps & {
  profile: string;
  database: string;
  schema: string;
  table: string;
}) {
  const [open, setOpen] = useState(false);
  const ref = `${profile}/${database}/${schema}/${table}`;
  const selected = isSelected(value, "db_table", ref);

  return (
    <li>
      <TreeRow
        selected={selected}
        onToggleSelect={() => onChange(toggle(value, "db_table", ref))}
        open={open}
        onToggleOpen={() => setOpen((o) => !o)}
        canExpand
        indent={3}
        label={table}
      />
      {open && (
        <ColumnList
          profile={profile}
          database={database}
          schema={schema}
          table={table}
          value={value}
          onChange={onChange}
        />
      )}
    </li>
  );
}

function ColumnList({
  profile,
  database,
  schema,
  table,
  value,
  onChange,
}: NodeProps & {
  profile: string;
  database: string;
  schema: string;
  table: string;
}) {
  const q = useQuery({
    queryKey: ["pages", "tree-cache", profile, database, schema, table, "columns"],
    queryFn: () => api.dbCacheTreeColumns(profile, database, schema, table),
    staleTime: 30_000,
  });

  if (q.isLoading) return <Hint indent={4}>Loading columns...</Hint>;
  if (q.error) return <ErrorLine indent={4}>{(q.error as Error).message}</ErrorLine>;
  const cols = q.data?.items ?? [];
  if (cols.length === 0) return <Hint indent={4}>No columns in cache.</Hint>;

  return (
    <ul className="space-y-0.5">
      {cols.map((c) => {
        const ref = `${profile}/${database}/${schema}/${table}.${c.name}`;
        const selected = isSelected(value, "db_column", ref);
        return (
          <li key={c.name}>
            <TreeRow
              selected={selected}
              onToggleSelect={() => onChange(toggle(value, "db_column", ref))}
              open={false}
              canExpand={false}
              indent={4}
              label={c.name}
              sublabel={c.dtype}
            />
          </li>
        );
      })}
    </ul>
  );
}

interface TreeRowProps {
  selected: boolean;
  onToggleSelect: () => void;
  open: boolean;
  onToggleOpen?: () => void;
  canExpand: boolean;
  indent: number;
  label: string;
  sublabel?: string;
}

function TreeRow({
  selected,
  onToggleSelect,
  open,
  onToggleOpen,
  canExpand,
  indent,
  label,
  sublabel,
}: TreeRowProps) {
  return (
    <div
      className={cn(
        "flex items-center gap-1.5 rounded px-1 py-1 text-[13px] hover:bg-surface-subtle",
        selected && "bg-accent-soft/40",
      )}
      style={{ paddingLeft: `${4 + indent * 12}px` }}
    >
      {canExpand ? (
        <button
          type="button"
          onClick={onToggleOpen}
          aria-label={open ? "Collapse" : "Expand"}
          className="inline-flex h-4 w-4 items-center justify-center text-ink-muted hover:text-ink"
        >
          {open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        </button>
      ) : (
        <span className="inline-block h-4 w-4" aria-hidden="true" />
      )}
      <button
        type="button"
        onClick={onToggleSelect}
        aria-pressed={selected}
        className="flex flex-1 items-center gap-1.5 truncate text-left"
      >
        <span
          className={cn(
            "inline-flex h-4 w-4 shrink-0 items-center justify-center rounded border",
            selected
              ? "border-accent bg-accent text-white"
              : "border-border bg-surface",
          )}
          aria-hidden="true"
        >
          {selected && <Check size={10} />}
        </span>
        <span className="truncate text-ink">{label}</span>
        {sublabel && (
          <span className="truncate text-[11px] text-ink-dim">{sublabel}</span>
        )}
      </button>
    </div>
  );
}

function Hint({ children, indent = 0 }: { children: React.ReactNode; indent?: number }) {
  return (
    <div
      className="text-[11px] text-ink-dim"
      style={{ paddingLeft: `${4 + indent * 12}px` }}
    >
      {children}
    </div>
  );
}

function ErrorLine({
  children,
  indent = 0,
}: {
  children: React.ReactNode;
  indent?: number;
}) {
  return (
    <div
      className="text-[11px] text-critical"
      style={{ paddingLeft: `${4 + indent * 12}px` }}
    >
      {children}
    </div>
  );
}

function isSelected(value: PageAssetRef[], kind: string, ref: string): boolean {
  return value.some((a) => a.kind === kind && a.ref === ref);
}

function toggle(
  value: PageAssetRef[],
  kind: string,
  ref: string,
): PageAssetRef[] {
  if (isSelected(value, kind, ref)) {
    return value.filter((a) => !(a.kind === kind && a.ref === ref));
  }
  return [...value, { kind, ref }];
}
