/**
 * Single-select cache table tree (profile → database → schema → table).
 *
 * Mirrors the Add-Table modal's tree (``LineageMultiTablePicker``) and
 * reads the same ``/api/db/cache/tree/*`` endpoints, but with two
 * deliberate differences for the native-lineage fetch flow:
 *
 *   1. Single-select — database / schema rows only expand; clicking a
 *      table picks exactly one (radio semantics).
 *   2. No sync gate — the cache tree always lists whatever rows are
 *      cached, even when the profile isn't "fully synced". Native
 *      lineage fetch is the tool you reach for precisely when a
 *      profile hasn't been fully synced, so gating it on a completed
 *      sync would defeat the purpose. The cache already holds the
 *      skeleton (schemas + tables) once a sync has touched it.
 *
 * Cache-only: drilling a 5,000-table workspace stays a local SQLite
 * read.
 */

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { ChevronDown, ChevronRight, Database, Table2 } from "lucide-react";
import clsx from "clsx";

import { api, apiFetch } from "../../lib/api";
import type { PickedTable } from "./LineageMultiTablePicker";
import { tableKey } from "./LineageMultiTablePicker";

interface DbProfilesResponse {
  profiles: Array<{ name: string; backend?: string }>;
}

interface Props {
  value: PickedTable | null;
  onChange: (next: PickedTable | null) => void;
}

export function CacheTableTreePicker({ value, onChange }: Props) {
  const profilesQ = useQuery({
    queryKey: ["native-fetch", "tree", "db-profiles"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
    staleTime: 30_000,
  });
  if (profilesQ.isLoading) return <Hint>Loading DB profiles…</Hint>;
  if (profilesQ.error) return <ErrorLine>{(profilesQ.error as Error).message}</ErrorLine>;
  const profiles = profilesQ.data?.profiles ?? [];
  if (profiles.length === 0) return <Hint>No DB profiles configured.</Hint>;
  return (
    <div className="max-h-[46vh] space-y-1.5 overflow-y-auto pr-1">
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
  value: PickedTable | null;
  onChange: (next: PickedTable | null) => void;
}

function ProfileCard({
  profile,
  backend,
  value,
  onChange,
}: NodeProps & { profile: string; backend: string }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="rounded-md border border-surface-border bg-surface">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center gap-1.5 px-3 py-2 text-sm"
      >
        {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        <Database size={14} />
        <span className="truncate font-medium text-ink">{profile}</span>
        <span className="text-[11px] text-fg-muted">({backend})</span>
      </button>
      {open && (
        <div className="border-t border-surface-border p-2">
          <DatabaseList
            profile={profile}
            backend={backend}
            value={value}
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
  value,
  onChange,
}: NodeProps & { profile: string; backend: string }) {
  const q = useQuery({
    queryKey: ["native-fetch", "tree", profile, "databases"],
    queryFn: () => api.dbCacheTreeDatabases(profile),
    staleTime: 30_000,
  });
  if (q.isLoading) return <Hint indent={1}>Loading…</Hint>;
  if (q.error) return <ErrorLine indent={1}>{(q.error as Error).message}</ErrorLine>;
  // No sync gate: render whatever the cache holds. ``synced`` is ignored
  // on purpose — see the module docstring.
  const items = q.data?.items ?? [];
  if (items.length === 0) return <Hint indent={1}>Nothing cached yet for this profile.</Hint>;
  return (
    <ul className="space-y-0.5">
      {items.map((it) => (
        <DatabaseNode
          key={it.name}
          profile={profile}
          backend={backend}
          database={it.name}
          value={value}
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
  value,
  onChange,
}: NodeProps & { profile: string; backend: string; database: string }) {
  const [open, setOpen] = useState(false);
  return (
    <li>
      <Row indent={1} label={database} expandable open={open} onToggle={() => setOpen((o) => !o)} />
      {open && (
        <SchemaList
          profile={profile}
          backend={backend}
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
  backend,
  database,
  value,
  onChange,
}: NodeProps & { profile: string; backend: string; database: string }) {
  const q = useQuery({
    queryKey: ["native-fetch", "tree", profile, database, "schemas"],
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
          value={value}
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
  value,
  onChange,
}: NodeProps & { profile: string; backend: string; database: string; schema: string }) {
  const [open, setOpen] = useState(false);
  return (
    <li>
      <Row indent={2} label={schema} expandable open={open} onToggle={() => setOpen((o) => !o)} />
      {open && (
        <TableList
          profile={profile}
          backend={backend}
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
  backend,
  database,
  schema,
  value,
  onChange,
}: NodeProps & { profile: string; backend: string; database: string; schema: string }) {
  const q = useQuery({
    queryKey: ["native-fetch", "tree", profile, database, schema, "tables"],
    queryFn: () => api.dbCacheTreeTables(profile, database, schema),
    staleTime: 30_000,
  });
  if (q.isLoading) return <Hint indent={3}>Loading tables…</Hint>;
  if (q.error) return <ErrorLine indent={3}>{(q.error as Error).message}</ErrorLine>;
  const items = q.data?.items ?? [];
  if (items.length === 0) return <Hint indent={3}>No tables in cache.</Hint>;
  const selectedKey = value ? tableKey(value) : "";
  return (
    <ul className="space-y-0.5">
      {items.map((t) => {
        const picked: PickedTable = { profile, backend, database, schema, table: t.name };
        const isSelected = tableKey(picked) === selectedKey;
        return (
          <li key={t.name}>
            <Row
              indent={3}
              label={t.name}
              isTable
              selected={isSelected}
              onSelect={() => onChange(isSelected ? null : picked)}
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
  expandable?: boolean;
  open?: boolean;
  onToggle?: () => void;
  isTable?: boolean;
  selected?: boolean;
  onSelect?: () => void;
}

function Row({ indent, label, expandable, open, onToggle, isTable, selected, onSelect }: RowProps) {
  return (
    <button
      type="button"
      onClick={expandable ? onToggle : onSelect}
      className={clsx(
        "flex w-full items-center gap-1.5 rounded-sm px-1 py-0.5 text-left text-[12.5px] transition hover:bg-surface-raised",
        selected && "bg-accent-soft text-accent-ink hover:bg-accent-soft",
      )}
      style={{ paddingLeft: 4 + indent * 14 }}
    >
      {expandable ? (
        open ? (
          <ChevronDown size={11} className="text-fg-muted" />
        ) : (
          <ChevronRight size={11} className="text-fg-muted" />
        )
      ) : (
        <span className="inline-block h-3 w-3" />
      )}
      {isTable && (
        <span
          className={clsx(
            "inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded-full border",
            selected ? "border-accent-default bg-accent-default" : "border-surface-border",
          )}
        >
          {selected && <span className="h-1.5 w-1.5 rounded-full bg-accent-ink" />}
        </span>
      )}
      {isTable && <Table2 size={12} className="shrink-0 text-fg-muted" />}
      <span className="truncate text-ink">{label}</span>
    </button>
  );
}

function Hint({ children, indent = 0 }: { children: React.ReactNode; indent?: number }) {
  return (
    <div className="text-[11px] text-fg-muted" style={{ paddingLeft: 4 + indent * 14 }}>
      {children}
    </div>
  );
}

function ErrorLine({ children, indent = 0 }: { children: React.ReactNode; indent?: number }) {
  return (
    <div className="text-[11px] text-critical" style={{ paddingLeft: 4 + indent * 14 }}>
      {children}
    </div>
  );
}
