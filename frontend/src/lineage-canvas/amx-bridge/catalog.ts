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

/**
 * Fetch the db-profiles response shape.
 *
 * Returns the raw ``{profiles: ProfileSummary[]}`` envelope rather
 * than the unwrapped array. The Sidebar + AskScopeDropdown cache this
 * under the same TanStack key ``["db-profiles", "list"]``; an array
 * vs object mismatch on that key crashes any consumer that calls
 * ``.find`` on whichever shape lands first. Keeping the envelope keeps
 * us cache-compatible with the rest of the Studio.
 */
export async function fetchProfilesResponse(): Promise<ProfilesResponse> {
  return apiFetch<ProfilesResponse>("/api/profiles/db");
}

/**
 * The fetchers below return the *raw* response envelope on purpose.
 *
 * The Sidebar and other parts of Studio cache the same TanStack keys
 * (``["live-schemas", ...]`` etc.) with the full response shape. If
 * we unwrapped to the inner array here we would either crash any
 * other consumer that reads ``.data.schemas`` from the same key, or
 * crash ourselves when their query lands first. Keeping the shape
 * stable means our queries hit the warm cache for free.
 */
export async function fetchSchemasResponse(args: {
  profile: string;
  database: string;
  catalog: string;
}) {
  const kind: "catalog" | "database" = args.catalog ? "catalog" : "database";
  return api.liveSchemas({
    profile: args.profile,
    database: args.database,
    catalog: args.catalog,
    kind,
  });
}

export async function fetchAssetsResponse(args: {
  profile: string;
  database: string;
  catalog: string;
  schema: string;
}) {
  const kind: "catalog" | "database" = args.catalog ? "catalog" : "database";
  return api.liveAssets(
    {
      profile: args.profile,
      database: args.database,
      catalog: args.catalog,
      kind,
    },
    args.schema,
  );
}

export interface TableMeta {
  columns: ColumnSpec[];
}

/**
 * Fetch column metadata for one table via the canonical live-db route
 * ``GET /api/live/schemas/{schema}/tables/{table}/columns``. The route
 * is cache-first: when the catalog already knows the table (sidebar
 * has expanded the schema once) the response comes back without a
 * live round-trip; otherwise it falls through to the connector.
 *
 * Either ``database`` or ``catalog`` qualifies the table — catalog
 * profiles (Databricks, BigQuery, Snowflake) need ``catalog``;
 * everything else uses ``database``.
 */
export async function fetchTableColumns(args: {
  profile: string;
  database?: string;
  catalog?: string;
  schema: string;
  table: string;
}): Promise<ColumnSpec[]> {
  const params = new URLSearchParams({ profile: args.profile });
  if (args.catalog) params.set("catalog", args.catalog);
  if (args.database) params.set("database", args.database);
  const path = `/api/live/schemas/${encodeURIComponent(
    args.schema,
  )}/tables/${encodeURIComponent(args.table)}/columns?${params.toString()}`;
  try {
    const res = await apiFetch<{
      columns?: Array<{ name?: string; dtype?: string; nullable?: boolean }>;
    }>(path);
    return (res.columns || []).map((c) => ({
      name: String(c.name || ""),
      dtype: String(c.dtype || ""),
    }));
  } catch {
    // Live route can 4xx for unauthorised profiles or unreachable
    // connectors — the canvas still renders a column-less node so the
    // graph stays drawable.
    return [];
  }
}

export function profileChipLabel(profile: string): string {
  if (!profile) return "—";
  if (profile.length <= 14) return profile;
  return profile.slice(0, 12) + "…";
}
