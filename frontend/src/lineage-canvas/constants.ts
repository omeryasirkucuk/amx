/**
 * Shared design tokens for the Lineage canvas.
 *
 * The canvas keeps data-type color coding as its visual signature
 * (column handle dot color, type badge background) — those values are
 * the only hex constants in the package and intentionally don't read
 * from CSS variables so the palette stays stable regardless of theme.
 * Everything else (surface backgrounds, borders, ink) reads from AMX's
 * design tokens via Tailwind classes (`bg-surface-raised`, etc.).
 */

export type ColumnDataType =
  | "str"
  | "int"
  | "flt"
  | "dat"
  | "bool"
  | "json"
  | "uuid"
  | "blob"
  | "unknown";

/** Per-type color used for handles + badges. Bright on dark navy. */
export const TYPE_COLORS: Record<ColumnDataType, string> = {
  str: "#60a5fa", // blue-400
  int: "#4ade80", // green-400
  flt: "#2dd4bf", // teal-400
  dat: "#fb923c", // orange-400
  bool: "#c084fc", // purple-400
  json: "#f472b6", // pink-400
  uuid: "#a3a3a3", // neutral-400
  blob: "#94a3b8", // slate-400
  unknown: "#737373", // neutral-500
};

/** Operator nodes get their own color band (border + header tag). */
export const OPERATOR_COLORS: Record<string, string> = {
  filter: "#fb923c", // orange — WHERE
  join: "#a78bfa", // violet — JOIN
  aggregate: "#22d3ee", // cyan — GROUP BY
  function: "#34d399", // emerald — UDF
  projection: "#94a3b8", // slate — SELECT *
};

/** Edge style mapping by relationship type. */
export const EDGE_COLORS: Record<string, string> = {
  fk: "#e2e8f0",
  view_ddl: "#60a5fa",
  query_log: "#4ade80",
  co_occur: "#94a3b8",
  lineage_llm: "#c084fc",
  name_match: "#737373",
  codebase: "#fb923c",
  lineage_manual: "#f5f4f2",
  unknown: "#a3a3a3",
};

/** Comment / sticky-note color palette. Keys persist; hex values may shift. */
export const COMMENT_COLORS: Record<string, { bg: string; border: string }> = {
  amber: { bg: "rgba(251,191,36,0.16)", border: "#fbbf24" },
  rose: { bg: "rgba(244,114,182,0.16)", border: "#f472b6" },
  emerald: { bg: "rgba(52,211,153,0.16)", border: "#34d399" },
  sky: { bg: "rgba(56,189,248,0.16)", border: "#38bdf8" },
  violet: { bg: "rgba(167,139,250,0.16)", border: "#a78bfa" },
  slate: { bg: "rgba(148,163,184,0.16)", border: "#94a3b8" },
};

/** Default node dimensions used by dagre layout. */
export const DEFAULT_NODE_W = 260;
export const DEFAULT_NODE_H = 140;

export function normalizeType(raw: string | null | undefined): ColumnDataType {
  if (!raw) return "unknown";
  const t = String(raw).toLowerCase();
  if (t.includes("char") || t.includes("text") || t.includes("string")) return "str";
  if (t.includes("int") || t === "bigint" || t === "smallint") return "int";
  if (
    t.includes("float") ||
    t.includes("double") ||
    t.includes("decimal") ||
    t.includes("numeric") ||
    t.includes("real")
  )
    return "flt";
  if (t.includes("date") || t.includes("time") || t.includes("stamp")) return "dat";
  if (t.includes("bool")) return "bool";
  if (t.includes("json")) return "json";
  if (t.includes("uuid")) return "uuid";
  if (t.includes("blob") || t.includes("bytea") || t.includes("binary")) return "blob";
  return "unknown";
}

/** Short label rendered inside the column type badge. */
export function shortTypeLabel(raw: string | null | undefined): string {
  const t = normalizeType(raw);
  if (t === "unknown") return (raw || "?").slice(0, 4).toLowerCase();
  return t;
}
