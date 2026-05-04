// Thin fetch wrapper + TanStack Query helpers.
//
// Every /api/* request goes through `apiFetch`, which:
//   * attaches the bearer token from localStorage,
//   * surfaces non-2xx responses as a typed `ApiError` so the UI
//     can render the backend's `detail` message verbatim.

import { getStoredToken } from "./auth";

export class ApiError extends Error {
  status: number;
  detail: string;
  hint?: string;

  constructor(status: number, detail: string, hint?: string) {
    super(detail);
    this.status = status;
    this.detail = detail;
    this.hint = hint;
  }
}

export async function apiFetch<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const token = getStoredToken();
  const headers = new Headers(init.headers || {});
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  headers.set("Accept", "application/json");

  const res = await fetch(path, { ...init, headers });
  if (!res.ok) {
    const text = await res.text();
    let detail = text;
    let hint: string | undefined;
    try {
      const parsed = JSON.parse(text);
      detail = String(parsed.detail ?? text);
      hint = parsed.hint ? String(parsed.hint) : undefined;
    } catch {
      /* response wasn't JSON; keep the raw text */
    }
    throw new ApiError(res.status, detail || res.statusText, hint);
  }
  // 204 No Content
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

// ── Typed wrappers for the read-only endpoints we ship in PR-B ──
// Keeping them in one module lets every page stay strictly typed
// without the React components owning fetch shape knowledge.

export interface HealthResponse {
  ok: boolean;
  version: string;
}
export interface ContextResponse {
  active_db_profile: string | null;
  active_db_profiles: string[];
  active_llm_profile: string | null;
  active_doc_profile: string | null;
  active_code_profile: string | null;
  current_schema: string | null;
  current_table: string | null;
  db_backend: string | null;
  llm_provider: string | null;
  llm_model: string | null;
}

export interface CatalogsResponse {
  supports_catalogs: boolean;
  catalogs: string[];
  active_catalog: string | null;
}

export interface DatabasesResponse {
  databases: string[];
  active_database: string | null;
}

export interface SchemasResponse {
  catalog: string | null;
  schemas: string[];
}

export interface AssetRow {
  name: string;
  kind: string;
}
export interface AssetsResponse {
  schema: string;
  assets: AssetRow[];
  count: number;
}

export interface ColumnRow {
  name: string;
  dtype: string;
  nullable: boolean;
}
export interface ColumnsResponse {
  schema: string;
  table: string;
  columns: ColumnRow[];
  count: number;
}

export interface SnapshotColumn {
  name: string;
  dtype: string;
  nullable: boolean;
  comment: string;
}
export interface SnapshotResponse {
  schema: string;
  table: string;
  table_comment: string;
  columns: SnapshotColumn[];
}

export interface RunRow {
  id: number;
  command: string;
  status: string;
  started_at: number | string;
  duration_sec: number | null;
  scope?: Record<string, string[]>;
  llm_model?: string | null;
  metrics?: Record<string, unknown>;
}
export interface RecentRunsResponse {
  command_filter: string | null;
  runs: RunRow[];
  count: number;
}

export interface StatsResponse {
  total?: number;
  success?: number;
  failed?: number;
  ready_for_review?: number;
  avg_duration_sec?: number;
  last_started_at?: number | string | null;
  [key: string]: unknown;
}

export const api = {
  health: () => apiFetch<HealthResponse>("/api/health"),
  context: () => apiFetch<ContextResponse>("/api/context"),
  liveCatalogs: () => apiFetch<CatalogsResponse>("/api/live/catalogs"),
  activateCatalog: (name: string, persist = true) =>
    apiFetch<{ catalog: string; profile: string; persisted: boolean }>(
      `/api/live/catalogs/${encodeURIComponent(name)}/activate`,
      { method: "POST", body: JSON.stringify({ persist }) },
    ),
  liveDatabases: () => apiFetch<DatabasesResponse>("/api/live/databases"),
  activateDatabase: (name: string, persist = true) =>
    apiFetch<{ database: string; profile: string; persisted: boolean }>(
      `/api/live/databases/${encodeURIComponent(name)}/activate`,
      { method: "POST", body: JSON.stringify({ persist }) },
    ),
  liveSchemas: (catalog?: string) =>
    apiFetch<SchemasResponse>(
      catalog ? `/api/live/schemas?catalog=${encodeURIComponent(catalog)}` : "/api/live/schemas",
    ),
  liveAssets: (schema: string) =>
    apiFetch<AssetsResponse>(`/api/live/schemas/${encodeURIComponent(schema)}/assets`),
  liveColumns: (schema: string, table: string) =>
    apiFetch<ColumnsResponse>(
      `/api/live/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/columns`,
    ),
  liveSnapshot: (schema: string, table: string) =>
    apiFetch<SnapshotResponse>(
      `/api/live/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/snapshot`,
    ),
  recentRuns: (limit = 20, command: string | null = "analyze.run") => {
    const params = new URLSearchParams({ limit: String(limit) });
    if (command) params.set("command", command);
    return apiFetch<RecentRunsResponse>(`/api/history/runs?${params.toString()}`);
  },
  stats: () => apiFetch<StatsResponse>("/api/history/stats"),
};
