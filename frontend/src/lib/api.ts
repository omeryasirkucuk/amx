// Thin fetch wrapper + TanStack Query helpers.
//
// Every /api/* request goes through `apiFetch`, which:
//   * attaches the bearer token from localStorage,
//   * surfaces non-2xx responses as a typed `ApiError` so the UI
//     can render the backend's `detail` message verbatim.
//
// Browse helpers take a Scope ({profile, database?, catalog?}) so each
// request encodes the multi-profile target — see `lib/scope.ts`. The
// browse pages read scope from the URL via `useScope()`; the sidebar
// builds scopes when the user expands a tree node.

import { getStoredToken } from "./auth";
import type { Scope } from "./scope";
import { scopeQuery } from "./scope";

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
      // FastAPI puts HTTPException payload under ``detail``. When the
      // route raises ``HTTPException(detail=str)`` the body is
      // ``{detail: "..."}``; when the route raises with a dict
      // (e.g. /api/ask 412 ships ``{message, hint}`` so the SPA can
      // render a "Open LLM settings" CTA) the body is
      // ``{detail: {message, hint}}``. Without this branch the dict
      // path stringifies to ``[object Object]`` and the hint never
      // surfaces.
      const rawDetail = parsed.detail;
      if (rawDetail && typeof rawDetail === "object") {
        const message = (rawDetail as { message?: unknown }).message;
        const detailHint = (rawDetail as { hint?: unknown }).hint;
        detail =
          typeof message === "string" && message
            ? message
            : JSON.stringify(rawDetail);
        if (typeof detailHint === "string" && detailHint) {
          hint = detailHint;
        }
      } else if (rawDetail != null) {
        detail = String(rawDetail);
      }
      // Some endpoints also include hint at the top level (legacy).
      if (!hint && parsed.hint) {
        hint = String(parsed.hint);
      }
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
  llm_supports_batch: boolean;
  /** Local OS user + hostname surfaced so the SPA can colour code
   *  apply events as "you" vs "{teammate}" without re-deriving the
   *  identity on every render. Both are nullable when the OS query
   *  failed (very rare). */
  current_user: string | null;
  current_hostname: string | null;
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

export interface SchemaItem {
  name: string;
  comment: string;
}
export interface SchemasResponse {
  catalog: string | null;
  /** Legacy flat name list — kept for callers that don't need comments. */
  schemas: string[];
  /** Enriched list with per-schema existing comment. */
  items: SchemaItem[];
}

export interface AssetRow {
  name: string;
  kind: string;
  /** The asset's current table/view COMMENT, or "" when none. */
  comment: string;
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
  /** ``list_recent_runs`` returns the parsed scope under
   *  ``scope_json``. The plain ``scope`` alias is kept for the run-
   *  detail endpoint and for any older payloads still cached client-
   *  side. Always read ``scope_json ?? scope``. */
  scope_json?: Record<string, string[]> | null;
  scope?: Record<string, string[]> | null;
  /** Set when a worker thread is still alive for this row in the
   *  job registry. The Studio uses the id to render an inline Cancel
   *  control on running rows; null/absent for finished rows. */
  live_job_id?: string | null;
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

export interface ApplyEvent {
  id: number;
  applied_at: number;
  run_id: number | null;
  result_id: number | null;
  profile_name: string;
  schema_name: string;
  table_name: string;
  column_name: string | null;
  asset_kind: string;
  old_comment: string | null;
  new_comment: string;
  applied_by: string;
  hostname: string;
  sql_template: string;
}

export interface ApplyEventsResponse {
  events: ApplyEvent[];
  count: number;
}

/** Per-asset latest apply event. ``new_comment`` lets the SPA show
 *  the *current* live-DB state without a second introspection
 *  round-trip when the user is about to overwrite it. */
export interface ApplyAttribution {
  schema_name: string;
  table_name: string;
  column_name: string | null;
  applied_at: number;
  applied_by: string;
  hostname: string;
  run_id: number | null;
  new_comment: string;
}
export interface ApplyAttributionResponse {
  profile: string;
  schemas: string[] | null;
  events: ApplyAttribution[];
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

/** Helper: append `?profile=…&database=…` to a path. */
function withScope(path: string, scope: Scope, extra?: string): string {
  const qs = scopeQuery(scope) + (extra ? `&${extra}` : "");
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}${qs}`;
}

/** Profile-only scope (used by sidebar tree expand: list catalogs/databases for a profile). */
export type ProfileScope = { profile: string };

export const api = {
  health: () => apiFetch<HealthResponse>("/api/health"),
  context: () => apiFetch<ContextResponse>("/api/context"),
  /** List catalogs for a profile (3-level backends). */
  liveCatalogs: (scope: ProfileScope) =>
    apiFetch<CatalogsResponse>(
      `/api/live/catalogs?profile=${encodeURIComponent(scope.profile)}`,
    ),
  /** List databases for a profile (2-level backends). */
  liveDatabases: (scope: ProfileScope) =>
    apiFetch<DatabasesResponse>(
      `/api/live/databases?profile=${encodeURIComponent(scope.profile)}`,
    ),
  liveSchemas: (scope: Scope) =>
    apiFetch<SchemasResponse>(withScope("/api/live/schemas", scope)),
  liveAssets: (scope: Scope, schema: string) =>
    apiFetch<AssetsResponse>(
      withScope(`/api/live/schemas/${encodeURIComponent(schema)}/assets`, scope),
    ),
  liveColumns: (scope: Scope, schema: string, table: string) =>
    apiFetch<ColumnsResponse>(
      withScope(
        `/api/live/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/columns`,
        scope,
      ),
    ),
  liveSnapshot: (scope: Scope, schema: string, table: string) =>
    apiFetch<SnapshotResponse>(
      withScope(
        `/api/live/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/snapshot`,
        scope,
      ),
    ),
  recentRuns: (limit = 20, command: string | null = "analyze.run") => {
    const params = new URLSearchParams({ limit: String(limit) });
    if (command) params.set("command", command);
    return apiFetch<RecentRunsResponse>(`/api/history/runs?${params.toString()}`);
  },
  applyEvents: (params: { runId?: number | null; profileName?: string | null; limit?: number } = {}) => {
    const qs = new URLSearchParams();
    if (params.runId != null) qs.set("run_id", String(params.runId));
    if (params.profileName) qs.set("profile_name", params.profileName);
    qs.set("limit", String(params.limit ?? 100));
    return apiFetch<ApplyEventsResponse>(`/api/history/apply-events?${qs.toString()}`);
  },
  /** Latest apply event per asset for one profile. Drives the
   *  RunNew pre-flight banner that warns when a teammate already
   *  touched a picked asset. Returns an empty events list when the
   *  history store has no rows for the profile yet. */
  applyAttribution: (profile: string, schemas?: string[]) => {
    const qs = new URLSearchParams({ profile });
    if (schemas && schemas.length > 0) {
      qs.set("schemas", schemas.join(","));
    }
    return apiFetch<ApplyAttributionResponse>(
      `/api/history/apply-attribution?${qs.toString()}`,
    );
  },
  setDatabaseComment: (scope: Scope, comment: string) =>
    apiFetch<{ comment: string }>(withScope("/api/comments/database", scope), {
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
  generateDatabaseDescription: (scope: Scope) =>
    apiFetch<{
      description: string;
      run_id: number | null;
      result_id: number | null;
      alternatives_count: number;
      verbosity: string;
    }>(
      withScope("/api/generate/database", scope),
      { method: "POST" },
    ),
  generateSchemaDescription: (scope: Scope, schema: string) =>
    apiFetch<{
      description: string;
      run_id: number | null;
      result_id: number | null;
      alternatives_count: number;
      verbosity: string;
    }>(
      withScope(`/api/generate/schema/${encodeURIComponent(schema)}`, scope),
      { method: "POST" },
    ),
  generateTableDescription: (scope: Scope, schema: string, table: string) =>
    apiFetch<{
      description: string;
      run_id: number | null;
      result_id: number | null;
      alternatives_count: number;
      verbosity: string;
    }>(
      withScope(
        `/api/generate/table/${encodeURIComponent(schema)}/${encodeURIComponent(table)}`,
        scope,
      ),
      { method: "POST" },
    ),
  generateColumnDescription: (
    scope: Scope,
    schema: string,
    table: string,
    column: string,
  ) =>
    apiFetch<{
      description: string;
      run_id: number | null;
      result_id: number | null;
      alternatives_count: number;
      verbosity: string;
    }>(
      withScope(
        `/api/generate/column/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/${encodeURIComponent(column)}`,
        scope,
      ),
      { method: "POST" },
    ),
  setSchemaComment: (scope: Scope, schema: string, comment: string) =>
    apiFetch<{ schema: string; comment: string }>(
      withScope(`/api/comments/schemas/${encodeURIComponent(schema)}`, scope),
      { method: "PUT", body: JSON.stringify({ comment }) },
    ),
  setTableComment: (scope: Scope, schema: string, table: string, comment: string) =>
    apiFetch<{ schema: string; table: string; comment: string }>(
      withScope(
        `/api/comments/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}`,
        scope,
      ),
      { method: "PUT", body: JSON.stringify({ comment }) },
    ),
  setColumnComment: (
    scope: Scope,
    schema: string,
    table: string,
    column: string,
    comment: string,
  ) =>
    apiFetch<{ schema: string; table: string; column: string; comment: string }>(
      withScope(
        `/api/comments/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/columns/${encodeURIComponent(column)}`,
        scope,
      ),
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
    batch_mode?: boolean;
    db_profile?: string;
    database?: string;
    catalog?: string;
  }) =>
    apiFetch<{ job_id: string; status: string }>("/api/runs", {
      method: "POST",
      body: JSON.stringify({
        scope: body.scope,
        apply: !!body.apply,
        missing_only: !!body.missing_only,
        batch_mode: !!body.batch_mode,
        db_profile: body.db_profile,
        database: body.database,
        catalog: body.catalog,
      }),
    }),
  cancelRun: (jobId: string) =>
    apiFetch<{ ok: boolean; job_id: string }>(`/api/runs/${encodeURIComponent(jobId)}/cancel`, {
      method: "POST",
    }),
  /** Spawn a re-run for one or many ``run_results`` rows.
   *
   * Caller subscribes to ``/api/runs/{job_id}/events`` for SSE
   * progress; ``job.done.summary.outcomes`` carries the new alternatives
   * once finished. ``user_instructions`` is appended to the existing
   * agent prompts — original DB / docs / code context is preserved.
   */
  rerunItems: (body: {
    result_ids: number[];
    user_instructions?: string | null;
    temperature_override?: number | null;
  }) =>
    apiFetch<{ job_id: string; status: string; new_run_id?: number | null }>(
      "/api/runs/rerun-item",
      {
        method: "POST",
        body: JSON.stringify({
          result_ids: body.result_ids,
          user_instructions: body.user_instructions ?? null,
          temperature_override: body.temperature_override ?? null,
        }),
      },
    ),
  /** Fetch the full re-run chain for one ``run_results`` row.
   *
   * Used by the version-history drawer when the user clicks a "v2/v3"
   * badge on an item. Returns the original + every child re-run row
   * ordered by ``rerun_seq`` ASC so the drawer can render them as a
   * timeline.
   */
  resultHistory: (resultId: number) =>
    apiFetch<{ result_id: number; chain: ResultRow[]; count: number }>(
      `/api/history/results/${resultId}/history`,
    ),
  /** Resolve the live price for a (provider, model) pair.
   *
   * Mirrors the resolution order used by the rest of AMX:
   * ``user_override`` (when ``profile_name`` carries a custom rate),
   * then ``litellm`` / ``openrouter`` / ``fallback`` / ``unknown``.
   * The Settings cost editor calls this as the user types so the
   * effective rate updates inline next to the input fields.
   */
  lookupPrice: (provider: string, model: string, profileName?: string) => {
    const params = new URLSearchParams({ provider, model });
    if (profileName) params.set("profile_name", profileName);
    return apiFetch<ModelPrice>(`/api/pricing/model?${params.toString()}`);
  },
  /** Force-fetch fresh prices from LiteLLM + OpenRouter. */
  refreshPrices: () =>
    apiFetch<{
      litellm: number;
      openrouter: number;
      errors: string[];
      skipped: boolean;
    }>("/api/pricing/refresh", { method: "POST" }),
  /** Inspect the local price cache (age, source counts, freshness). */
  pricingCacheInfo: () =>
    apiFetch<{
      fetched_at: number | null;
      age_seconds: number | null;
      ttl_seconds: number;
      is_stale: boolean;
      litellm_count: number;
      openrouter_count: number;
      fallback_count: number;
    }>("/api/pricing/cache-info"),
};

/** Shape returned by ``lookupPrice`` — mirrors the backend
 *  :class:`ModelPrice` dataclass. ``source`` is one of
 *  ``user_override`` / ``litellm`` / ``openrouter`` / ``fallback`` /
 *  ``unknown``.
 */
export interface ModelPrice {
  input_per_mtok: number;
  output_per_mtok: number;
  source: string;
  fetched_at: number | null;
}

/** Shape of one row in the re-run chain returned by ``resultHistory``.
 *
 * Mirrors the backend ``run_results`` schema; alternatives are already
 * parsed JSON arrays. ``parent_result_id`` / ``rerun_seq`` /
 * ``user_instructions`` are populated for re-run children only.
 */
export interface ResultRow {
  id: number;
  run_id: number;
  schema_name: string;
  table_name: string;
  column_name: string | null;
  asset_kind: string;
  confidence: string;
  source: string;
  logprob_score: number | null;
  alternatives_json: unknown;
  chosen_description: string | null;
  evaluation: string | null;
  applied_at: number | null;
  parent_result_id: number | null;
  rerun_seq: number;
  user_instructions: string | null;
}
