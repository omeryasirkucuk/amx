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
