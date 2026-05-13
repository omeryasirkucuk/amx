// Typed wrappers around /api/db/cache/{show,stats,clear}. Lives in
// its own file so the new feature can ship without touching the WIP
// region in `api.ts` (per the standing rule that we don't edit user
// WIP files).

import { apiFetch } from "./api";

export interface DbCacheRow {
  profile: string;
  database: string;
  schemas_rows: number;
  columns_rows: number;
  catalog_rows: number;
  last_fetch: number | null;
}

export interface DbCacheShowResponse {
  rows: DbCacheRow[];
}

export interface DbCacheStat {
  table: string;
  total_rows: number;
  distinct_profiles: number;
  distinct_databases: number;
  oldest_fetch: number | null;
  newest_fetch: number | null;
  expired_rows: number;
  ttl_aware: boolean;
}

export type DbCacheStatsResponse = Record<"schemas" | "columns" | "catalog", DbCacheStat>;

export interface DbCacheClearRequest {
  profile?: string | null;
  database?: string | null;
  types?: string[];
  force?: boolean;
}

export interface DbCacheClearResponse {
  scope: { profile: string | null; database: string | null };
  types: string[];
  deleted: Record<string, number>;
  total: number;
  valid_types: string[];
}

export const dbCacheApi = {
  show: (params: { profile?: string; database?: string } = {}) => {
    const search = new URLSearchParams();
    if (params.profile) search.set("profile", params.profile);
    if (params.database !== undefined && params.database !== "")
      search.set("database", params.database);
    const qs = search.toString();
    return apiFetch<DbCacheShowResponse>(
      qs ? `/api/db/cache/show?${qs}` : "/api/db/cache/show",
    );
  },
  stats: () => apiFetch<DbCacheStatsResponse>("/api/db/cache/stats"),
  clear: (body: DbCacheClearRequest) =>
    apiFetch<DbCacheClearResponse>("/api/db/cache/clear", {
      method: "POST",
      body: JSON.stringify(body),
    }),
};
