// Shared formatting helpers for run rows. The raw history-store
// records carry developer-facing identifiers (e.g. `analyze.run`)
// and a scope object whose key cardinality is the only useful
// signal at a glance — neither communicates anything to a fresh
// reader. These helpers turn them into compact, human labels.

const COMMAND_LABELS: Record<string, string> = {
  "analyze.run": "Schema description",
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
};

/** Map a raw command identifier to a label suited for end-user lists. */
export function humanizeCommand(command: string | null | undefined): string {
  if (!command) return "Run";
  if (COMMAND_LABELS[command]) return COMMAND_LABELS[command];
  // Fallback: strip the trailing `.<verb>` and Title-case the head.
  const head = command.split(".")[0] ?? command;
  return head.charAt(0).toUpperCase() + head.slice(1);
}

/**
 * Summarise a run's scope for an inline label. Returns a one-line
 * string suited for the secondary text in a run row.
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
