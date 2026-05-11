// Shared formatting helpers for run rows. The raw history-store
// records carry developer-facing identifiers (e.g. `analyze.run`)
// and a scope object whose key cardinality is the only useful
// signal at a glance — neither communicates anything to a fresh
// reader. These helpers turn them into compact, human labels.

const COMMAND_LABELS: Record<string, string> = {
  // ``analyze.run`` is intentionally absent — it spans
  // database / schema / table / column granularity, and the right
  // label depends on the run's scope, not the command id. Callers
  // pass the scope into ``humanizeCommand`` so we can pick the
  // accurate label.
  "analyze.apply": "Apply changes",
  "apply.run": "Apply changes",
  "search.ask": "Ask query",
  "ask.run": "Ask query",
  "search.sync": "Catalog sync",
  "sync.run": "Catalog sync",
  "code.scan": "Code scan",
  "doc.scan": "Doc scan",
  "docs.scan": "Doc scan",
  "doc.ingest": "Doc ingest",
  "docs.ingest": "Doc ingest",
  "generate.database": "Database description",
  "generate.schema": "Schema description",
  "generate.table": "Table description",
  "generate.column": "Column description",
};

/** Pick the right ``analyze.run`` label from the scope shape.
 *
 *   - empty / null scope             → "Database description"
 *   - one or many schemas, no tables → "Schema description"
 *   - one schema with one table      → "Table description"
 *   - many tables (any schema mix)   → "Tables description"
 *
 * Accepts ``unknown`` because the API row type widens scope to
 * ``Record<string, unknown>`` to keep room for legacy payloads;
 * we narrow defensively here.
 */
function analyzeRunLabel(scope: unknown): string {
  if (!scope || typeof scope !== "object") return "Database description";
  const entries = Object.entries(scope as Record<string, unknown>);
  if (entries.length === 0) return "Database description";
  let totalTables = 0;
  let allEmpty = true;
  for (const [, tables] of entries) {
    if (Array.isArray(tables) && tables.length > 0) {
      totalTables += tables.length;
      allEmpty = false;
    }
  }
  if (allEmpty) return "Schema description";
  if (totalTables === 1) return "Table description";
  return "Tables description";
}

/** Map a raw command identifier to a label suited for end-user lists.
 *
 * ``analyze.run`` is scope-dependent: a run pinned to one table
 * produces a "Table description", a run on a whole schema produces
 * a "Schema description", and a run with no scope at all is a
 * "Database description". Callers that have the scope in hand
 * (RunsList, RunsCompare, the picker) should pass it through.
 * Callers that don't (legacy contexts) get a generic "Analyze run"
 * fallback instead of the historically incorrect blanket
 * "Schema description" label.
 */
export function humanizeCommand(
  command: string | null | undefined,
  scope?: unknown,
): string {
  if (!command) return "Run";
  if (command === "analyze.run") {
    return scope === undefined ? "Analyze run" : analyzeRunLabel(scope);
  }
  if (COMMAND_LABELS[command]) return COMMAND_LABELS[command];
  // Fallback: strip the trailing `.<verb>` and Title-case the head.
  const head = command.split(".")[0] ?? command;
  return head.charAt(0).toUpperCase() + head.slice(1);
}

/** Shape of the per-run ``processed_assets`` envelope the backend
 * now returns alongside each row. Carries the actual ``(schema,
 * table, column)`` tuples taken from ``run_results`` so the listing
 * UI can show a concrete asset label instead of the schema-level
 * scope ("sales · 1 table") the user originally picked.
 */
export interface ProcessedAssetsSummary {
  schemas: number;
  tables: number;
  columns: number;
  sample: Array<{
    schema: string;
    table: string;
    column: string | null;
  }>;
}

/**
 * Format a run's actual processed assets into a one-line label. This
 * is what RunsList + RunsCompare render in the "Scope" column.
 *
 *   - 1 column-level asset      → "sales.orders.status"
 *   - 1 table, full-table run   → "sales.orders"
 *   - 1 table, N columns        → "sales.orders (3 columns)"
 *   - many tables, one schema   → "sales · 4 tables"
 *   - many schemas              → "3 schemas · 12 tables"
 *
 * Returns ``null`` when the run has no processed-asset data yet
 * (worker still running, or pre-0.14 row without the aggregate);
 * callers should fall back to the legacy ``summarizeScope`` in that
 * case so the cell never renders as empty.
 */
export function summarizeProcessedAssets(
  assets: ProcessedAssetsSummary | null | undefined,
): string | null {
  if (!assets) return null;
  const { schemas, tables, columns, sample } = assets;
  if (tables === 0 && columns === 0) return null;

  // Exactly one column processed across the whole run — happens when
  // the user picked a single column or triggered ``/rerun --column``.
  if (tables === 1 && columns === 1) {
    const a = sample[0];
    if (a) {
      return a.column ? `${a.schema}.${a.table}.${a.column}` : `${a.schema}.${a.table}`;
    }
  }

  // One table, many columns — typical "analyze one table" run.
  if (tables === 1) {
    const a = sample[0];
    if (a) {
      // Every sample row shares the same schema.table; surface the
      // column count if any per-column rows exist, otherwise it's a
      // pure table-level row.
      if (columns > 0) {
        return `${a.schema}.${a.table} (${columns} column${columns === 1 ? "" : "s"})`;
      }
      return `${a.schema}.${a.table}`;
    }
  }

  // Many tables, one schema.
  if (schemas === 1) {
    const first = sample[0];
    const schema = first ? first.schema : "";
    return `${schema} · ${tables} table${tables === 1 ? "" : "s"}`;
  }

  // Cross-schema run.
  return `${schemas} schemas · ${tables} table${tables === 1 ? "" : "s"}`;
}

/** Build a multi-line tooltip enumerating every distinct asset the
 * run processed (capped at the backend's sample size). Used as the
 * ``title=`` attribute so users hovering the cell see exactly what
 * was run when the headline label collapses many assets into "N
 * tables". */
export function processedAssetsTooltip(
  assets: ProcessedAssetsSummary | null | undefined,
): string | undefined {
  if (!assets || assets.sample.length === 0) return undefined;
  const lines = assets.sample.map((a) =>
    a.column ? `${a.schema}.${a.table}.${a.column}` : `${a.schema}.${a.table}`,
  );
  const more = Math.max(0, assets.tables + assets.columns - lines.length);
  if (more > 0) lines.push(`… +${more} more`);
  return lines.join("\n");
}

/**
 * Summarise a run's scope for an inline label. Returns a one-line
 * string suited for the secondary text in a run row.
 *
 * **Legacy helper** — preserved for compatibility with rows that
 * don't yet carry the ``processed_assets`` envelope (worker still
 * running, or old history rows). New code should prefer
 * ``summarizeProcessedAssets`` which surfaces the actual
 * schema.table.column the run touched.
 *
 *   - `{}` or null     → "All schemas"
 *   - one schema, []   → "sales (all tables)"
 *   - one schema, [t]  → "sales · 1 table"
 *   - many schemas     → "3 schemas · 12 tables"
 */
export function summarizeScope(scope: unknown): string {
  if (!scope || typeof scope !== "object") return "All schemas";
  const entries = Object.entries(scope as Record<string, unknown>);
  if (entries.length === 0) return "All schemas";
  let totalTables = 0;
  let allEmpty = true;
  for (const [, tables] of entries) {
    if (Array.isArray(tables) && tables.length > 0) {
      totalTables += tables.length;
      allEmpty = false;
    }
  }
  if (entries.length === 1) {
    const [schema, tables] = entries[0];
    if (Array.isArray(tables) && tables.length > 0) {
      return `${schema} · ${tables.length} ${tables.length === 1 ? "table" : "tables"}`;
    }
    return `${schema} (all tables)`;
  }
  if (allEmpty) {
    return `${entries.length} schemas`;
  }
  return `${entries.length} schemas · ${totalTables} ${totalTables === 1 ? "table" : "tables"}`;
}

/** Map a status string to a Badge tone. */
export function statusTone(
  status: string | null | undefined,
): "positive" | "critical" | "warning" | "accent" | "neutral" {
  if (status === "success") return "positive";
  if (status === "failed") return "critical";
  if (status === "cancelled") return "warning";
  if (status === "running" || status === "queued") return "accent";
  return "neutral";
}

/** Display-friendly status: lowercase but readable. */
export function statusLabel(status: string | null | undefined): string {
  if (!status) return "—";
  if (status === "ready_for_review") return "ready";
  return status;
}

/**
 * Coerce the history-store's `started_at` (unix-seconds, unix-millis,
 * or ISO string) into a millisecond epoch. Returns NaN when the input
 * can't be parsed so callers can fall back gracefully.
 */
export function parseStartedAt(input: number | string | null | undefined): number {
  if (input == null) return NaN;
  if (typeof input === "number") {
    // Heuristic: anything below 10^12 is seconds, above is millis.
    return input < 1e12 ? input * 1000 : input;
  }
  const parsed = Date.parse(input);
  return Number.isFinite(parsed) ? parsed : NaN;
}

/**
 * "just now" / "5m ago" / "3h ago" / "2d ago". For anything older than
 * 30 days we fall back to a calendar date so the label stays readable.
 */
export function relativeTime(input: number | string | null | undefined): string {
  const ts = parseStartedAt(input);
  if (!Number.isFinite(ts)) return "—";
  const diffMs = Date.now() - ts;
  if (diffMs < 0) return "just now";
  const sec = Math.floor(diffMs / 1000);
  if (sec < 45) return "just now";
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  if (day < 30) return `${day}d ago`;
  return new Date(ts).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

/** Strip provider/path prefixes from an LLM model slug for display. */
export function shortModel(model: string | null | undefined): string {
  if (!model) return "";
  const slash = model.lastIndexOf("/");
  return slash >= 0 ? model.slice(slash + 1) : model;
}

/** Bucket an ``analysis_runs.command`` string into one of the
 *  user-visible kind filters. Anything that isn't analyze / rerun /
 *  generate / ask lands in "other" so the "All" filter still picks
 *  up tools we add later (sync, scan, ingest, …) without forcing a
 *  schema migration on every pre-existing chip group. Shared by the
 *  Runs list filter chips and the Compare picker chip group. */
export function commandKind(
  command: string | null | undefined,
): "analyze" | "rerun" | "generate" | "ask" | "other" {
  const cmd = (command ?? "").toLowerCase();
  if (cmd === "analyze.run" || cmd === "analyze.apply") return "analyze";
  if (cmd === "rerun") return "rerun";
  if (cmd.startsWith("generate.")) return "generate";
  if (cmd === "search.ask" || cmd === "ask.run") return "ask";
  return "other";
}

/** Filter values surfaced as chips in both the Runs list and the
 *  Compare picker. ``all`` returns every row regardless of kind. */
export type CommandKindFilter = "all" | "analyze" | "rerun" | "generate" | "ask";
