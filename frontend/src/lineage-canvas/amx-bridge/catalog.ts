/**
 * Catalog helpers used by the Add-Table modal and the @-mention
 * dropdown. All requests are cache-first (TanStack Query caches the
 * underlying ``/api/live-*`` calls so reopening a picker is instant).
 */

import { api, apiFetch } from "../../lib/api";
import type { ColumnSpec } from "../types";

export interface ProfileSummary {
  name: string;
  backend?: string;
  database?: string;
  catalog?: string;
}

export interface ProfilesResponse {
  profiles: ProfileSummary[];
}

const CATALOG_BACKENDS = new Set(["databricks", "bigquery", "snowflake"]);

export function supportsCatalogs(p: ProfileSummary | undefined): boolean {
  if (!p) return false;
  return CATALOG_BACKENDS.has(String(p.backend || "").toLowerCase());
}

export async function fetchProfiles(): Promise<ProfileSummary[]> {
  const res = await apiFetch<ProfilesResponse>("/api/profiles/db");
  return res.profiles;
}

export async function fetchSchemas(args: {
  profile: string;
  database: string;
  catalog: string;
}): Promise<string[]> {
  const kind: "catalog" | "database" = args.catalog ? "catalog" : "database";
  const res = await api.liveSchemas({
    profile: args.profile,
    database: args.database,
    catalog: args.catalog,
    kind,
  });
  return res.schemas ?? [];
}

export async function fetchAssets(args: {
  profile: string;
  database: string;
  catalog: string;
  schema: string;
}): Promise<Array<{ name: string; kind: string }>> {
  const kind: "catalog" | "database" = args.catalog ? "catalog" : "database";
  const res = await api.liveAssets(
    {
      profile: args.profile,
      database: args.database,
      catalog: args.catalog,
      kind,
    },
    args.schema,
  );
  return (res.assets ?? []).filter((a) => a.kind === "table" || a.kind === "view");
}

export interface TableMeta {
  columns: ColumnSpec[];
}

/**
 * Fetch column metadata for one table. We piggyback on /api/catalog/columns
 * which is the canonical cache-first source; if that endpoint isn't
 * exposed for the active profile the canvas degrades to a column-less
 * node (still draggable, still typed by inference, just no badges).
 */
export async function fetchTableColumns(args: {
  profile: string;
  database: string;
  schema: string;
  table: string;
}): Promise<ColumnSpec[]> {
  try {
    const params = new URLSearchParams({
      profile: args.profile,
      database: args.database,
      schema: args.schema,
      table: args.table,
    });
    const res = await apiFetch<{ columns?: Array<Record<string, unknown>> }>(
      `/api/catalog/columns?${params.toString()}`,
    );
    return (res.columns || []).map((c) => ({
      name: String(c.column_name || c.name || ""),
      dtype: String(c.data_type || c.dtype || ""),
      isPrimary: Boolean(c.is_primary || c.is_pk),
      isForeign: Boolean(c.is_foreign || c.is_fk),
      description: typeof c.description === "string" ? c.description : undefined,
    }));
  } catch {
    // Endpoint shape differs across forks of the catalog router; degrade
    // gracefully so the canvas still renders even when columns are
    // unavailable.
    return [];
  }
}

export function profileChipLabel(profile: string): string {
  if (!profile) return "—";
  if (profile.length <= 14) return profile;
  return profile.slice(0, 12) + "…";
}
