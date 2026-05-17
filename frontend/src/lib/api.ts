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

import { clearStoredToken, getStoredToken, rememberDeeplink } from "./auth";
import type { Scope } from "./scope";
import { scopeQuery } from "./scope";

// Set once per page load so the 401 self-recovery only ever bounces
// the user back to ``/`` a single time per session — without it a
// token source that keeps handing out a stale token would put the SPA
// into a reload loop. After the first bounce we surface the 401 as a
// normal ``ApiError`` so the existing UI banners can render and the
// user can decide what to do.
let _tokenRotateAttempted = false;

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
    // Self-recover from a stale Studio token. The launcher rotates the
    // ``~/.amx/studio.token`` on every restart (the ``deploy`` script
    // does this on the remote box). The SPA still has the previous
    // token in localStorage; the first ``/api/*`` call after the rotate
    // 401s with "Invalid AMX Studio token" or "Missing AMX Studio
    // token". Without this branch the user would see a wall of red
    // toasts on every page until they manually refreshed the home
    // page. Instead we drop the stale token, stash the current
    // pathname so the next boot can land back here, and bounce
    // through ``/`` — the token source / launcher injects ``?t=<fresh>``
    // and the SPA captures the new token before redirecting to the
    // remembered deep link. Guarded by a single-shot flag so a
    // genuinely broken token source can't push the SPA into a reload
    // loop.
    if (
      res.status === 401 &&
      !_tokenRotateAttempted &&
      typeof window !== "undefined" &&
      /AMX Studio token/i.test(text)
    ) {
      _tokenRotateAttempted = true;
      try {
        clearStoredToken();
        rememberDeeplink();
        window.location.replace("/");
        // Return a never-resolving promise so callers don't render a
        // half-error state during the navigation tick.
        return await new Promise<T>(() => {});
      } catch {
        /* fall through to the normal error path */
      }
    }
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
/** Active LLM profile's tuning knobs, surfaced by /api/context so the
 *  RunNew "Advanced LLM settings" disclosure can pre-fill defaults
 *  without a second round-trip. Read-only mirror of `cfg.llm`; the
 *  per-run override is sent on POST /api/runs (`llm_overrides`) and
 *  never written back to the saved profile. */
export interface LLMProfileDefaults {
  temperature: number | null;
  max_tokens: number | null;
  n_alternatives: number | null;
  column_batch_size: number | null;
  prompt_detail: string | null;
  description_verbosity: string | null;
  confidence_signal: string | null;
  alternatives_mode: AlternativesMode | null;
  thinking_budget: number | null;
  logprob_high: number | null;
  logprob_medium: number | null;
  custom_input_cost_per_mtok: number | null;
  custom_output_cost_per_mtok: number | null;
}

/** Diversity mode for the LLM alternatives. ``semantic`` (default) means
 *  the model is asked to emit meaningfully different interpretations;
 *  ``lexical`` means same-meaning phrasing variants only. */
export type AlternativesMode = "semantic" | "lexical";

/** Per-run override of the active LLM profile's tuning knobs. Every
 *  field is optional; omitted = use the saved profile's value.
 *  ``profile`` is a saved-profile reference that swaps the whole
 *  provider/model/api_key/api_base bundle atomically — other fields
 *  layer on top. */
export interface LLMOverrides {
  profile?: string;
  temperature?: number;
  max_tokens?: number;
  n_alternatives?: number;
  column_batch_size?: number;
  prompt_detail?: string;
  description_verbosity?: string;
  confidence_signal?: string;
  alternatives_mode?: AlternativesMode;
  thinking_budget?: number;
  logprob_high?: number;
  logprob_medium?: number;
  custom_input_cost_per_mtok?: number | null;
  custom_output_cost_per_mtok?: number | null;
}

/** Capability flags returned by ``GET /api/llm/capabilities``. */
export interface LLMCapabilities {
  provider: string;
  model: string;
  supports_thinking: boolean;
  supports_logprobs: boolean;
}

export interface ContextResponse {
  // ``active_db_profile`` / ``active_db_profiles`` were dropped in
  // 0.13: every defined DB profile is selectable from Run / Ask /
  // Browse directly; nothing is "active" globally. The CLI keeps a
  // default-fallback pointer internally (set via ``/use-db <name>``)
  // but the SPA must never read or render it.
  active_llm_profile: string | null;
  active_doc_profile: string | null;
  active_code_profile: string | null;
  current_schema: string | null;
  current_table: string | null;
  db_backend: string | null;
  llm_provider: string | null;
  llm_model: string | null;
  llm_supports_batch: boolean;
  /** Tuning values from the active LLM profile (RunNew pre-fill). */
  llm_profile_defaults: LLMProfileDefaults | null;
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
  /** BigQuery's project — same role as a Databricks catalog. */
  active_project?: string | null;
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
  /** Databricks ``cfg.database`` — the wizard's "Schema / database
   * (optional)" field. When set, the SPA renders only this schema. */
  active_schema?: string | null;
  /** BigQuery ``cfg.dataset`` — same role as a Databricks schema. */
  active_dataset?: string | null;
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

/** One row in the catalog-cache search response. ``table`` / ``column``
 *  are nullable so the same shape covers schema-level, table-level,
 *  and column-level matches. ``match_field`` discriminates which
 *  field actually matched the query — the sidebar renders the
 *  matching segment bolded and ranks rows by it. */
export interface DbCacheSearchResult {
  profile: string;
  db_backend: string;
  database: string;
  schema: string;
  table: string | null;
  column: string | null;
  match_field: "schema" | "table" | "column";
}
export interface DbCacheSearchResponse {
  query: string;
  truncated: boolean;
  results: DbCacheSearchResult[];
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
  /** Backend-aggregated record of the actual ``(schema, table,
   *  column)`` tuples the run processed. Surfaces column-level scope
   *  that ``scope_json`` doesn't carry (the latter only stores the
   *  user-picked schema-level scope; a ``/rerun --column`` run looked
   *  identical to a full-table run in the listing before this field).
   *  Always treat as optional — legacy rows + still-running workers
   *  may not have it populated. */
  processed_assets?: {
    schemas: number;
    tables: number;
    columns: number;
    sample: Array<{ schema: string; table: string; column: string | null }>;
  } | null;
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
  /**
   * Global count of runs whose status is ``ready_for_review`` or
   * ``applied_partial`` — i.e. runs that still have unreviewed result
   * rows. Computed against the full ``analysis_runs`` table on the
   * server so the Studio Landing chip is honest regardless of the
   * recent-feed window. Optional for forward-compatibility with older
   * API revisions.
   */
  pending_review_total?: number;
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

// ── Compare payload (used by /history compare modal + deep analysis) ──
//
// CompareResponse mirrors the dict the backend's ``compare_runs`` helper
// builds (see ``amx/cli_support/commands/compare.py``). Quality_metrics is
// optional — only populated when ``quality_tier > 0`` was passed (Tier 0
// metrics, Tier 1 embeddings, Tier 2 LLM-as-judge), and the deep-analysis
// endpoint always returns it filled. Lives in api.ts so the runDeepAnalysis
// helper can declare its return type without RunsCompare.tsx having to
// re-export anything.

export interface CompareSummaryRow {
  run_id: number;
  command?: string;
  llm_profile?: string;
  llm_model?: string;
  doc_profile?: string;
  code_profile?: string;
  duration_sec?: number;
  status?: string;
  [key: string]: unknown;
}

export interface CompareAlternative {
  text: string;
  signal?: string | null;
  score?: number | null;
  band?: "high" | "med" | "low" | null;
}

export interface ComparePerColumnRow {
  schema?: string;
  table?: string;
  column?: string;
  run_id?: number;
  description?: string;
  confidence?: string;
  logprob_score?: number | null;
  token_count?: number | null;
  /** Full DESCRIPTION_1..N list parsed from ``alternatives_json``. The
   *  Compare pivot shows DESCRIPTION_1 in the cell headline and the
   *  remaining alts stacked underneath so semantic / lexical
   *  divergence is visible side-by-side. Empty / undefined on legacy
   *  rows that predate structured scoring — render the headline
   *  only. */
  alternatives?: CompareAlternative[];
  /** Diversity mode recorded at write time. ``null`` on legacy rows
   *  (column shipped in PR #441). Studio surfaces this once per run
   *  column header rather than repeating per cell. */
  alternatives_mode?: AlternativesMode | null;
  /** Version label within this asset / run cell. ``"v1"`` on the
   *  parent row; ``"v2"``, ``"v3"``, ... on Re-Run / Variations
   *  descendants stacked below it. Defaults to ``"v1"`` on legacy
   *  payloads that predate the descendant unfurl in compare. */
  version_label?: string;
  /** Set on descendant rows: the originating ``analysis_runs.id`` of
   *  the parent run this descendant was generated against. ``null``
   *  on the v1 row itself. */
  parent_run_id?: number | null;
  /** ``"variations"`` | ``"rerun"`` on descendant rows; ``null`` on
   *  v1. Drives the version chip styling and the seed tooltip. */
  descendant_kind?: "variations" | "rerun" | null;
  /** The child run's own ``analysis_runs.id``. The SPA uses this to
   *  deep-link the version chip into the descendant's run-detail
   *  page. ``null`` on v1. */
  descendant_run_id?: number | null;
  /** Verbatim text of the alternative the user picked as the seed
   *  (Variations only). ``null`` on Re-Run descendants and v1. */
  seed_alternative_text?: string | null;
}

export interface CompareAggregateRow {
  metric: string;
  run_id: number;
  value: number | null;
}

export interface QualityPerRun {
  run_id: number;
  type_token_ratio: number | null;
  schema_grounding: number | null;
  chrf: number | null;
  rouge_l: number | null;
  bertscore: number | null;
  levenshtein: number | null;
  embedding_agreement: number | null;
  semantic_grounding: number | null;
  judge_win_rate: number | null;
  judge_pairings: number;
  judge_wins: number;
}

export interface QualityReference {
  schema: string;
  table: string;
  column: string;
  source: "user_pinned" | "db_comment" | "catalog_applied" | "none";
  text: string;
  run_id: number | null;
}

export interface QualityCitation {
  key: string;
  label: string;
  citation: string;
  url: string;
}

export interface QualityMetrics {
  per_asset: Array<Record<string, unknown>>;
  per_run: QualityPerRun[];
  references: QualityReference[];
  judge_outcomes: Array<{
    run_a: number;
    run_b: number;
    winner: "A" | "B" | "tie";
    reasoning: string;
    confidence: number;
  }>;
  citations: QualityCitation[];
  cost: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
  tier: number;
}

export interface CompareResponse {
  runs: Array<{ id: number; command?: string; status?: string }>;
  summary_rows: CompareSummaryRow[];
  per_column: ComparePerColumnRow[];
  aggregates: CompareAggregateRow[];
  missing: number[];
  quality_metrics?: QualityMetrics;
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
  refreshSchemaMetadata: (scope: Scope, schema: string) =>
    apiFetch<AssetsResponse>(
      withScope(`/api/live/schemas/${encodeURIComponent(schema)}/refresh`, scope),
      { method: "POST" },
    ),
  liveColumns: (scope: Scope, schema: string, table: string) =>
    apiFetch<ColumnsResponse>(
      withScope(
        `/api/live/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/columns`,
        scope,
      ),
    ),
  /** Substring search over the persistent catalog cache —
   *  schema / table / column names. Powers the sidebar's column-level
   *  search. ``profile`` narrows to a single profile; omit it to
   *  search every fully-synced profile. */
  dbCacheSearch: (q: string, profile?: string | null, limit?: number) => {
    const params = new URLSearchParams({ q });
    if (profile) params.set("profile", profile);
    if (typeof limit === "number") params.set("limit", String(limit));
    return apiFetch<DbCacheSearchResponse>(
      `/api/db/cache/search?${params.toString()}`,
    );
  },
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
  /** Trigger Tier 1+2 quality analysis on an existing comparison.
   *
   * The default ``/api/history/compare`` endpoint runs Tier 0 only
   * (offline metrics) so the modal opens cheap and fast. The
   * Studio "Run deeper analysis" button posts here when the user
   * accepts the cost preview — this re-runs ``compare_runs`` with
   * tier=2, which adds local sentence-transformer embedding
   * agreement (Tier 1) and the LLM-as-judge tournament (Tier 2,
   * G-Eval style — Liu et al. 2023). The response shape is
   * identical to /compare; only ``quality_metrics`` is enriched.
   */
  runDeepAnalysis: (
    runIds: number[],
    options?: { groundTruthRunId?: number | null },
  ) =>
    apiFetch<CompareResponse>("/api/history/compare/deep-analysis", {
      method: "POST",
      body: JSON.stringify({
        run_ids: runIds,
        quality_tier: 2,
        ground_truth_run_id: options?.groundTruthRunId ?? null,
      }),
    }),
  /** Download the PDF rendering of a Compare result.
   *
   * Posts ``run_ids`` to ``/api/history/compare/pdf``; the server
   * re-runs the comparison and pipes WeasyPrint's bytes back as a
   * single ``application/pdf`` blob. Bypasses ``apiFetch`` because
   * that helper expects JSON responses — here we want the raw blob
   * so the UI can build an ``<a download>`` and let the browser
   * save the file with the suggested name.
   */
  compareAsPdf: async (
    runIds: number[],
    options?: {
      qualityTier?: number;
      groundTruthRunId?: number | null;
    },
  ): Promise<Blob> => {
    const token = getStoredToken();
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      Accept: "application/pdf",
    };
    if (token) headers.Authorization = `Bearer ${token}`;
    // Forward the user's current quality state so a "Run deeper
    // analysis" → "Download PDF" sequence carries Tier 1+2 metrics
    // (judge win-rate, embedding agreement) into the printed
    // report; without this the PDF endpoint defaults to Tier 0.
    const res = await fetch("/api/history/compare/pdf", {
      method: "POST",
      headers,
      body: JSON.stringify({
        run_ids: runIds,
        quality_tier: options?.qualityTier ?? 1,
        ground_truth_run_id: options?.groundTruthRunId ?? null,
      }),
    });
    if (!res.ok) {
      const text = await res.text();
      let detail = text;
      try {
        const parsed = JSON.parse(text);
        if (parsed?.detail) detail = String(parsed.detail);
      } catch {
        /* not JSON, keep raw */
      }
      throw new ApiError(res.status, detail || res.statusText);
    }
    return res.blob();
  },
  applyEvents: (params: { runId?: number | null; profileName?: string | null; limit?: number } = {}) => {
    const qs = new URLSearchParams();
    if (params.runId != null) qs.set("run_id", String(params.runId));
    if (params.profileName) qs.set("profile_name", params.profileName);
    qs.set("limit", String(params.limit ?? 100));
    return apiFetch<ApplyEventsResponse>(`/api/history/apply-events?${qs.toString()}`);
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
    /** Per-table column restriction. Keys are ``"schema.table"`` strings;
     *  values are the explicit column names to process. ``undefined`` (or
     *  an empty dict) means "process every column" — the pre-existing
     *  behaviour. The orchestrator's pre-existing ``column_overrides``
     *  map consumes this. */
    column_overrides?: Record<string, string[]>;
    apply?: boolean;
    missing_only?: boolean;
    batch_mode?: boolean;
    db_profile?: string;
    database?: string;
    catalog?: string;
    /** Per-run override of the active LLM profile's tuning knobs. */
    llm_overrides?: LLMOverrides;
    doc_profiles?: string[];
    code_profiles?: string[];
  }) =>
    apiFetch<{ job_id: string; status: string }>("/api/runs", {
      method: "POST",
      body: JSON.stringify({
        scope: body.scope,
        column_overrides: body.column_overrides,
        apply: !!body.apply,
        missing_only: !!body.missing_only,
        batch_mode: !!body.batch_mode,
        db_profile: body.db_profile,
        database: body.database,
        catalog: body.catalog,
        llm_overrides: body.llm_overrides,
        doc_profiles: body.doc_profiles,
        code_profiles: body.code_profiles,
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
    /** Full per-run LLM override block — same shape as
     *  ``LLMOverrides`` on ``POST /api/runs``. New surface, parity
     *  with RunNew's Advanced LLM settings. ``temperature_override``
     *  above stays for one release as a back-compat shim. */
    llm_overrides?: LLMOverrides;
  }) =>
    apiFetch<{ job_id: string; status: string; new_run_id?: number | null }>(
      "/api/runs/rerun-item",
      {
        method: "POST",
        body: JSON.stringify({
          result_ids: body.result_ids,
          user_instructions: body.user_instructions ?? null,
          temperature_override: body.temperature_override ?? null,
          llm_overrides: body.llm_overrides ?? null,
        }),
      },
    ),
  /** Generate seeded variations from one chosen alternative.
   *
   * Distinct from ``rerunItems``: the modal's top-level radio supplies
   * ``mode`` directly (semantic / lexical), and the executor anchors
   * the new alternatives on the chosen ``seed_text``. The seed itself
   * is filtered out of the result list. Subscribe to the SSE stream
   * the same way as Re-Run. */
  generateVariations: (body: {
    original_run_id: number;
    result_id: number;
    alternative_index: number;
    seed_text: string;
    mode: AlternativesMode;
    user_instructions?: string | null;
    llm_overrides?: LLMOverrides;
  }) =>
    apiFetch<{ job_id: string; status: string; new_run_id?: number | null }>(
      "/api/runs/variations",
      {
        method: "POST",
        body: JSON.stringify({
          original_run_id: body.original_run_id,
          result_id: body.result_id,
          alternative_index: body.alternative_index,
          seed_text: body.seed_text,
          mode: body.mode,
          user_instructions: body.user_instructions ?? null,
          llm_overrides: body.llm_overrides ?? null,
        }),
      },
    ),
  /** Static capability lookup for a ``(provider, model)`` pair.
   *  Drives the Studio's "Advanced LLM settings" knob-gating UI. */
  llmCapabilities: (provider: string, model: string) => {
    const params = new URLSearchParams({ provider, model });
    return apiFetch<LLMCapabilities>(
      `/api/llm/capabilities?${params.toString()}`,
    );
  },
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
  /** Flat catalog of every model AMX has price data for.
   *
   * Powers the topbar's model-browser dialog and the dedicated
   * ``/pricing`` page. Backend reads only from the in-memory cache —
   * no network fetch — so the UI stays snappy when refetched.
   */
  listModelPrices: () =>
    apiFetch<ModelCatalog>("/api/pricing/models"),

  // ── Scheduled runs (Phase 5a routes) ───────────────────────────────
  listSchedules: (
    params: { status?: string; db_profile?: string; kind?: string } = {},
  ) => {
    const qs = new URLSearchParams();
    if (params.status) qs.set("status_filter", params.status);
    if (params.db_profile) qs.set("db_profile", params.db_profile);
    if (params.kind) qs.set("kind", params.kind);
    return apiFetch<SchedulesListResponse>(
      `/api/schedules${qs.toString() ? `?${qs.toString()}` : ""}`,
    );
  },
  createSchedule: (body: ScheduleCreatePayload) =>
    apiFetch<ScheduleRow>(`/api/schedules`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  getSchedule: (id: number) => apiFetch<ScheduleRow>(`/api/schedules/${id}`),
  patchSchedule: (id: number, body: Partial<ScheduleCreatePayload>) =>
    apiFetch<ScheduleRow>(`/api/schedules/${id}`, {
      method: "PATCH",
      body: JSON.stringify(body),
    }),
  pauseSchedule: (id: number) =>
    apiFetch<ScheduleRow>(`/api/schedules/${id}/pause`, { method: "POST" }),
  resumeSchedule: (id: number) =>
    apiFetch<ScheduleRow>(`/api/schedules/${id}/resume`, { method: "POST" }),
  runScheduleNow: (id: number) =>
    apiFetch<{ fired: number[] }>(`/api/schedules/${id}/run-now`, {
      method: "POST",
    }),
  deleteSchedule: (id: number) =>
    apiFetch<void>(`/api/schedules/${id}`, { method: "DELETE" }),
  schedulerStatus: () =>
    apiFetch<SchedulerStatusResponse>(`/api/scheduler/status`),
  schedulerBootstrapReport: () =>
    apiFetch<BootstrapReport>(`/api/scheduler/bootstrap-report`),
  installDaemon: () =>
    apiFetch<{ message: string; path: string | null }>(
      `/api/scheduler/install-daemon`,
      { method: "POST" },
    ),
  uninstallDaemon: () =>
    apiFetch<{ message: string }>(`/api/scheduler/uninstall-daemon`, {
      method: "POST",
    }),

  /** Ad-hoc synchronous cache refresh — invalidates + warms the cache
   * for the picked scope without writing a scheduled_runs row. Backs
   * the Catalog cache page's "Sync scope…" dialog. */
  refreshCatalogScope: (body: CatalogScopeRefreshPayload) =>
    apiFetch<{ ok: boolean; profile: string; mode: string }>(
      `/api/catalog/refresh`,
      {
        method: "POST",
        body: JSON.stringify(body),
      },
    ),
};

export interface CatalogScopeRefreshPayload {
  profile: string;
  database?: string | null;
  catalog?: string | null;
  scope: Record<string, unknown>;
}

export interface ScheduleRow {
  id: number;
  name: string;
  fire_at_utc: number;
  fire_at_tz: string;
  fire_at_local: string;
  status: string;
  db_profile: string;
  // Per-schedule DB / catalog overlay. Null for legacy rows created
  // before the picker started persisting this field — those rows
  // connect to the profile default at fire time and need a re-save
  // via the Edit dialog to start firing against the right DB.
  database: string | null;
  catalog: string | null;
  scope_json: string;
  llm_profile: string;
  review_strategy: string;
  triggered_run_id: number | null;
  last_error: string | null;
  // Epoch seconds when the schedule actually fired (run was created
  // and the executor was dispatched). Null until the schedule fires.
  fired_at: number | null;
  // Discriminator: 'analyze' (legacy run) or 'cache_refresh'
  // (Catalog Freshness refresh). Defaulted on the server so legacy
  // rows keep the analyze behaviour.
  kind?: "analyze" | "cache_refresh";
  // Optional croniter expression. NULL = one-shot; non-NULL = the
  // scheduler re-arms the row with a fresh fire_at_utc after every
  // fire so the schedule keeps cycling.
  cron_expr?: string | null;
}

export interface SchedulesListResponse {
  schedules: ScheduleRow[];
}

export interface ScheduleCreatePayload {
  name: string;
  fire_at_local: string;
  fire_at_tz: string;
  db_profile: string;
  database?: string | null;
  catalog?: string | null;
  scope: Record<string, unknown>;
  llm_profile?: string;
  review_strategy?: "auto" | "manual";
  kind?: "analyze" | "cache_refresh";
  cron_expr?: string | null;
}

export interface SchedulerStatusResponse {
  pending_count: number;
  missed_count: number;
  paused_count: number;
  next_fire: ScheduleRow | null;
  daemon: {
    installed: boolean;
    /** ``true`` when the daemon is registered with the OS scheduler
     *  AND ticking. Distinct from ``installed``: a plist can be on
     *  disk without being bootstrapped, which is the silent-failure
     *  state the modern macOS launchctl path produced. */
    loaded?: boolean;
    path: string | null;
    last_tick_log: string | null;
    log_size_bytes?: number;
    log_mtime?: number | null;
  };
}

export interface BootstrapReport {
  fired: number[];
  failed_resolution: [number, string][];
  missed_for_review: number[];
  stale_recovered: number[];
}

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

/** One row of the cross-source pricing catalog. Mirrors the backend
 *  :class:`ModelCatalogEntryResponse`. ``provider_hint`` is the first
 *  segment of the canonical key when prefixed (display only, never
 *  used for resolution).
 */
export interface ModelCatalogEntry {
  model_id: string;
  provider_hint: string;
  input_per_mtok: number;
  output_per_mtok: number;
  source: string;
  fetched_at: number | null;
}

/** Response from ``GET /api/pricing/models``. ``fetched_at`` is the
 *  newest fetched_at across the catalog (drives the freshness badge);
 *  ``is_stale`` mirrors :func:`cache_info`'s flag.
 */
export interface ModelCatalog {
  models: ModelCatalogEntry[];
  fetched_at: number | null;
  is_stale: boolean;
}

/**
 * Confidence scoring (single-signal pivot): each alternative carries
 * the active signal name, its raw 0–1 score, and the HIGH / MED / LOW
 * band derived from absolute cut-offs. Legacy rows that predate the
 * feature surface ``signal=null`` / ``score=null`` / ``band=null`` so
 * the UI can render them as plain alternatives without a pill.
 */
export interface StructuredAlternative {
  text: string;
  signal?: string | null;
  score?: number | null;
  band?: "HIGH" | "MED" | "LOW" | null;
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
  alternatives_mode: AlternativesMode | null;
}

// ── Lineage (v2) ────────────────────────────────────────────────────────

export interface LineageNodeColumn {
  name: string;
  dtype: string;
}

export interface LineageNode {
  id: string;
  label: string;
  /** v4 — 'operator' is the synthetic node kind that appears between
   *  source and target columns when a transform was detected (filter,
   *  join, function, aggregate). */
  kind: "table" | "column" | "operator";
  anchor: boolean;
  described: boolean;
  /** v4 — per-column rows on a table node. Empty / undefined when the
   *  catalog cache has no column entries for the table. */
  columns?: LineageNodeColumn[];
  /** v4 operator nodes only — the SQL fragment + classification. */
  op_kind?: string;
  expression?: string;
}

export interface LineageEdgeOperator {
  op_kind: string;
  expression: string;
}

export interface LineageEdge {
  /** ``catalog_relationships.id`` when sourced from the local catalog
   *  (FK / LLM / codebase / manual). Null for ephemeral edges. */
  id: number | null;
  from: string;
  to: string;
  type: string;
  extractor: string;
  confidence: number;
  evidence: string;
  /** S4 authoring verdict: '', 'approved', 'rejected', 'pending'. */
  verdict?: string;
  /** v4 — column-level edges populate these; empty string for
   *  table-grain edges (legacy v1-v3 behaviour). */
  from_column?: string;
  to_column?: string;
  /** v4 — present on the operator→target half of an operator chain,
   *  so the consuming canvas can label the edge or expose the
   *  expression on hover. */
  operator?: LineageEdgeOperator;
  /** v4 — 'operator_input' / 'operator_output' marker on the two
   *  halves of an operator chain. Empty for plain edges. */
  role?: string;
}

export interface LineagePayload {
  anchor: {
    database: string;
    schema: string;
    table: string;
    column: string | null;
  };
  nodes: LineageNode[];
  edges: LineageEdge[];
  partial: boolean;
  extractors_used: string[];
  generated_at: number;
}

export interface LineageArtifact {
  id: number;
  name: string;
  db_profile: string;
  anchor_entity_id: number;
  depth_up: number;
  depth_down: number;
  format: string;
  output_path: string;
  edge_set_hash: string;
  node_count: number;
  edge_count: number;
  generated_at: number;
  extractors_used: string[];
  extractors_partial: boolean;
  /** Enrichments added by the router from `catalog_entities` so the
   *  Studio canvas can rebuild the anchor scope without a second
   *  lookup. Optional because legacy rows may not carry them. */
  anchor_database?: string;
  anchor_schema?: string;
  anchor_table?: string;
  anchor_column?: string;
}

export interface LineageArtifactList {
  artifacts: LineageArtifact[];
  count: number;
}

export interface LineageRefreshResponse {
  ok: boolean;
  artifact_id: number;
  node_count: number;
  edge_count: number;
  extractors_used: string[];
  extractors_partial: boolean;
  aborted: boolean;
  abort_reason: string;
}

export interface LineageSuggestResponse {
  edges: LineageEdge[];
  persisted: number;
  model: string;
}

function encodeAnchorPath(anchor: string): string {
  // Each segment is URL-encoded; dots stay as dots so FastAPI's
  // ``{anchor_path:path}`` route matcher receives ``schema.table``
  // verbatim.
  return anchor
    .split(".")
    .map((part) => encodeURIComponent(part))
    .join(".");
}

export async function lineageList(profile?: string): Promise<LineageArtifactList> {
  const qs = profile ? `?profile=${encodeURIComponent(profile)}` : "";
  return apiFetch<LineageArtifactList>(`/api/lineage${qs}`);
}

export async function lineageFetch(
  anchor: string,
  opts: { profile?: string; database?: string; depthUp?: number; depthDown?: number } = {},
): Promise<LineagePayload> {
  const params = new URLSearchParams();
  if (opts.profile) params.set("profile", opts.profile);
  if (opts.database) params.set("database", opts.database);
  if (opts.depthUp !== undefined) params.set("depth_up", String(opts.depthUp));
  if (opts.depthDown !== undefined) params.set("depth_down", String(opts.depthDown));
  const qs = params.toString();
  return apiFetch<LineagePayload>(
    `/api/lineage/${encodeAnchorPath(anchor)}${qs ? `?${qs}` : ""}`,
  );
}

export async function lineageRefresh(
  anchor: string,
  opts: { profile?: string; database?: string; noCache?: boolean } = {},
): Promise<LineageRefreshResponse> {
  const params = new URLSearchParams();
  if (opts.profile) params.set("profile", opts.profile);
  if (opts.database) params.set("database", opts.database);
  if (opts.noCache) params.set("no_cache", "true");
  const qs = params.toString();
  return apiFetch<LineageRefreshResponse>(
    `/api/lineage/${encodeAnchorPath(anchor)}/refresh${qs ? `?${qs}` : ""}`,
    { method: "POST" },
  );
}

export async function lineageSuggest(
  anchor: string,
  opts: { profile?: string; database?: string } = {},
): Promise<LineageSuggestResponse> {
  const params = new URLSearchParams();
  if (opts.profile) params.set("profile", opts.profile);
  if (opts.database) params.set("database", opts.database);
  const qs = params.toString();
  return apiFetch<LineageSuggestResponse>(
    `/api/lineage/${encodeAnchorPath(anchor)}/suggest${qs ? `?${qs}` : ""}`,
    { method: "POST" },
  );
}

export interface LineageDiscoveredAnchor {
  database: string;
  schema: string;
  table: string;
  fqn: string;
  edge_count: number;
  extractors_used: string[];
  partial: boolean;
}

export interface LineageDiscoverResponse {
  profile: string;
  anchors: LineageDiscoveredAnchor[];
  tables_examined: number;
  tables_with_edges: number;
  total_edges: number;
  truncated: boolean;
  duration_sec: number;
}

export async function lineageDiscover(
  opts: { profile?: string; maxTables?: number } = {},
): Promise<LineageDiscoverResponse> {
  const params = new URLSearchParams();
  if (opts.profile) params.set("profile", opts.profile);
  if (opts.maxTables !== undefined) params.set("max_tables", String(opts.maxTables));
  const qs = params.toString();
  return apiFetch<LineageDiscoverResponse>(
    `/api/lineage/discover${qs ? `?${qs}` : ""}`,
    { method: "POST" },
  );
}

// ── Authoring — v3 S4 ──────────────────────────────────────────────────

export interface LineageManualEdgeResponse {
  id: number;
  from: string;
  to: string;
  verdict: string;
  audit_actor: string;
  audit_at: number;
}

export async function lineageCreateEdge(payload: {
  profile: string;
  source_fqn: string;
  target_fqn: string;
  notes?: string;
  /** v4 — column-level overrides for the parsed-from-FQN values. The
   *  backend honours these when both are present. */
  source_column?: string | null;
  target_column?: string | null;
}): Promise<LineageManualEdgeResponse> {
  return apiFetch<LineageManualEdgeResponse>("/api/lineage/edges", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function lineageDeleteEdge(edgeId: number): Promise<void> {
  await apiFetch<void>(`/api/lineage/edges/${edgeId}`, { method: "DELETE" });
}

export async function lineageSetVerdict(
  edgeId: number,
  verdict: "approved" | "rejected" | "pending" | "",
): Promise<{ id: number; verdict: string }> {
  return apiFetch<{ id: number; verdict: string }>(
    `/api/lineage/edges/${edgeId}/verdict`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ verdict }),
    },
  );
}

export interface LineageAuditEntry {
  edge_id: number;
  relationship_type: string;
  verdict: string;
  actor: string;
  at: number;
  source: string;
  from: string;
  to: string;
  note: string;
}

export interface LineageAuditResponse {
  profile: string;
  entries: LineageAuditEntry[];
  count: number;
}

export async function lineageAudit(
  opts: { profile?: string; limit?: number } = {},
): Promise<LineageAuditResponse> {
  const params = new URLSearchParams();
  if (opts.profile) params.set("profile", opts.profile);
  if (opts.limit !== undefined) params.set("limit", String(opts.limit));
  const qs = params.toString();
  return apiFetch<LineageAuditResponse>(`/api/lineage/audit${qs ? `?${qs}` : ""}`);
}
