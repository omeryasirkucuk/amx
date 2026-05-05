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
  scope?: Record<string, string[]> | null;
  /** Parsed metrics_json — shape varies by command. */
  metrics?: Record<string, unknown> | null;
  /** Active DB profile at the time of the run (e.g. "local-postgres"). */
  db_profile?: string | null;
  db_backend?: string | null;
  llm_model?: string | null;
  llm_profile?: string | null;
  llm_provider?: string | null;
  mode?: string | null;
}
export interface RecentRunsResponse {
  command_filter: string | null;
  runs: RunRow[];
  count: number;
}

/**
 * Backend returns total_runs / success_runs / failed_runs /
 * ready_for_review_runs. Older callers in this file used the shorter
 * `total` / `success` aliases — both shapes are now declared so
 * consumers can pick whichever is convenient.
 */
export interface StatsResponse {
  total_runs?: number;
  success_runs?: number;
  failed_runs?: number;
  ready_for_review_runs?: number;
  avg_duration_sec?: number;
  avg_model_processing_sec?: number;
  last_started_at?: number | string | null;
  total_events?: number;
  // Legacy aliases — kept so older code paths keep compiling.
  total?: number;
  success?: number;
  failed?: number;
  ready_for_review?: number;
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
  setDatabaseComment: (comment: string) =>
    apiFetch<{ comment: string }>("/api/comments/database", {
      method: "PUT",
      body: JSON.stringify({ comment }),
    }),
  restorePending: (body: {
    result_id?: number | null;
    schema: string;
    table: string;
    column?: string | null;
    final_description: string;
    confidence?: string;
    source?: string;
    asset_kind?: string;
    alternatives?: string[];
    logprob_score?: number | null;
  }) =>
    apiFetch<{ ok: boolean; idx: number; count: number; already_present: boolean }>(
      "/api/pending/restore",
      { method: "POST", body: JSON.stringify(body) },
    ),
  enableHistoryStore: (body: { profile: string; schema?: string; database?: string }) =>
    apiFetch<{
      enabled: boolean;
      profile: string;
      schema: string;
      database: string;
    }>("/api/admin/history-store/enable", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  disableHistoryStore: () =>
    apiFetch<{ enabled: boolean }>("/api/admin/history-store/disable", {
      method: "POST",
    }),
  generateDatabaseDescription: () =>
    apiFetch<{ description: string; run_id: number | null; result_id: number | null }>("/api/generate/database", { method: "POST" }),
  generateSchemaDescription: (schema: string) =>
    apiFetch<{ description: string; run_id: number | null; result_id: number | null }>(
      `/api/generate/schema/${encodeURIComponent(schema)}`,
      { method: "POST" },
    ),
  generateTableDescription: (schema: string, table: string) =>
    apiFetch<{ description: string; run_id: number | null; result_id: number | null }>(
      `/api/generate/table/${encodeURIComponent(schema)}/${encodeURIComponent(table)}`,
      { method: "POST" },
    ),
  generateColumnDescription: (schema: string, table: string, column: string) =>
    apiFetch<{ description: string; run_id: number | null; result_id: number | null }>(
      `/api/generate/column/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/${encodeURIComponent(column)}`,
      { method: "POST" },
    ),
  setSchemaComment: (schema: string, comment: string) =>
    apiFetch<{ schema: string; comment: string }>(
      `/api/comments/schemas/${encodeURIComponent(schema)}`,
      { method: "PUT", body: JSON.stringify({ comment }) },
    ),
  setTableComment: (schema: string, table: string, comment: string) =>
    apiFetch<{ schema: string; table: string; comment: string }>(
      `/api/comments/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}`,
      { method: "PUT", body: JSON.stringify({ comment }) },
    ),
  setColumnComment: (
    schema: string,
    table: string,
    column: string,
    comment: string,
  ) =>
    apiFetch<{ schema: string; table: string; column: string; comment: string }>(
      `/api/comments/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/columns/${encodeURIComponent(column)}`,
      { method: "PUT", body: JSON.stringify({ comment }) },
    ),
  stats: (command: string | null = "analyze.run") => {
    const params = new URLSearchParams();
    if (command) params.set("command", command);
    const qs = params.toString();
    return apiFetch<StatsResponse>(
      qs ? `/api/history/stats?${qs}` : "/api/history/stats",
    );
  },
  submitRun: (body: {
    scope: Record<string, string[]>;
    apply?: boolean;
    missing_only?: boolean;
  }) =>
    apiFetch<{ job_id: string; status: string }>("/api/runs", {
      method: "POST",
      body: JSON.stringify({
        scope: body.scope,
        apply: !!body.apply,
        missing_only: !!body.missing_only,
      }),
    }),
  cancelRun: (jobId: string) =>
    apiFetch<{ ok: boolean; job_id: string }>(`/api/runs/${encodeURIComponent(jobId)}/cancel`, {
      method: "POST",
    }),
};
