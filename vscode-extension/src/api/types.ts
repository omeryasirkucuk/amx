// DTOs for the slice of the AMX Studio REST API the extension
// consumes. Field names mirror the FastAPI payloads verbatim
// (snake_case) so contract-fixture tests can assert against recorded
// responses without a mapping layer.

// --- /api/health /api/version /api/context (system.py) ---

export interface Health {
  ok: boolean;
  version: string;
}

export interface VersionInfo {
  amx: string;
  schema: number;
  web: string;
}

export interface ContextInfo {
  active_llm_profile: string | null;
  active_doc_profile: string | null;
  active_code_profile: string | null;
  current_schema: string | null;
  current_table: string | null;
  db_backend: string | null;
  llm_provider: string | null;
  llm_model: string | null;
  llm_supports_batch: boolean;
}

// --- /api/profiles (profiles.py) ---

export interface DbProfileSummary {
  name: string;
  backend: string;
  host: string;
  database: string;
  catalog: string;
  project: string;
  is_active: boolean;
}

export interface LlmProfileSummary {
  name: string;
  provider: string;
  model: string;
  is_active: boolean;
}

export interface NamedProfileSummary {
  name: string;
  is_active?: boolean;
  [key: string]: unknown;
}

// --- /api/catalog (catalog.py) ---

export interface InventoryTable {
  db_profile?: string;
  database_name?: string | null;
  schema_name: string;
  table_name: string;
  asset_kind?: string;
  row_count?: number | null;
  column_count?: number | null;
  effective_description?: string | null;
}

export interface ExplainColumn {
  column_name: string;
  data_type?: string | null;
  effective_description?: string | null;
  [key: string]: unknown;
}

export interface TableExplain {
  table: Record<string, unknown>;
  columns: ExplainColumn[];
  relationships?: unknown[];
}

// --- /api/history (history.py) ---

export interface RunSummary {
  run_id: string;
  command?: string;
  status?: string;
  kind?: string;
  scope?: string;
  started_at?: string;
  finished_at?: string | null;
  live_job_id?: string | null;
  [key: string]: unknown;
}

export interface RunsPage {
  runs: RunSummary[];
  total?: number;
  has_more?: boolean;
}

// --- /api/schedules (schedules.py) ---

export interface ScheduleSummary {
  id: string | number;
  name?: string;
  cron?: string;
  enabled?: boolean;
  paused?: boolean;
  next_run_at?: string | null;
  [key: string]: unknown;
}

// --- /api/generate (generate.py) ---

export interface GenerateResult {
  description: string;
  run_id: number | null;
  result_id: number | null;
  alternatives_count: number;
  verbosity: string;
}

// --- SSE ---

export interface SseEvent {
  /** Event payload parsed from the data line (JSON when possible). */
  data: unknown;
  /** Raw data string as received. */
  raw: string;
  event?: string;
  id?: string;
}

// --- scope shared by catalog calls ---

export interface CatalogScope {
  profile?: string;
  database?: string;
  schema?: string;
}
