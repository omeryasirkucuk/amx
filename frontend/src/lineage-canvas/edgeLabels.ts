/**
 * Human-readable labels for lineage edge types + confidence values.
 *
 * The canvas tooltip and the on-canvas legend chip both share these
 * strings so the wording stays consistent: hovering an edge says the
 * same thing the legend swatch says.
 */

export interface EdgeKindMeta {
  /** Short title shown in tooltips and legend chips. */
  label: string;
  /** One-line plain-English explanation of where this edge type
   *  comes from and how to interpret its confidence value. */
  description: string;
  /** When ``true``, the extractor produced this edge from real
   *  metadata (FK constraints, view DDL, query logs, etc.) and
   *  the confidence is effectively a constant — the percentage
   *  in the tooltip is not informative. */
  deterministic: boolean;
}

export const EDGE_KIND_META: Record<string, EdgeKindMeta> = {
  fk: {
    label: "Foreign key",
    description: "Declared FK constraint extracted from catalog metadata.",
    deterministic: true,
  },
  view_ddl: {
    label: "View DDL",
    description: "Parsed from the view's CREATE statement.",
    deterministic: true,
  },
  query_log: {
    label: "Query log",
    description: "Observed in the warehouse query history.",
    deterministic: true,
  },
  codebase: {
    label: "Codebase",
    description: "Found by scanning the connected code repository.",
    deterministic: true,
  },
  co_occur: {
    label: "Co-occurrence",
    description: "Tables that show up together in audit queries.",
    deterministic: false,
  },
  lineage_llm: {
    label: "LLM estimate",
    description: "Proposed by an LLM; the percentage is its self-reported confidence.",
    deterministic: false,
  },
  name_match: {
    label: "Name match",
    description: "Heuristic column-name similarity — no schema evidence.",
    deterministic: false,
  },
  lineage_manual: {
    label: "Manual",
    description: "Drawn by you on the canvas.",
    deterministic: true,
  },
  unknown: {
    label: "Unknown",
    description: "Source extractor did not tag this edge.",
    deterministic: false,
  },
};

export function edgeKindMeta(type: string): EdgeKindMeta {
  return EDGE_KIND_META[type] ?? EDGE_KIND_META.unknown;
}

interface HoverLabelInput {
  fromColumn?: string;
  toColumn?: string;
  type: string;
  confidence: number;
  dashed: boolean;
}

/** Format the multi-line tooltip a user sees when hovering an edge.
 *  Lines are separated by ``\n`` — the ColumnEdge tooltip uses
 *  ``white-space: pre-line`` so newlines render as visual breaks. */
export function buildEdgeHoverLabel(input: HoverLabelInput): string {
  const meta = edgeKindMeta(input.type);
  const pct = Math.round((input.confidence ?? 0) * 100);
  const lines: string[] = [];
  if (input.fromColumn && input.toColumn) {
    lines.push(`${input.fromColumn} → ${input.toColumn}`);
  }
  lines.push(meta.label);
  if (meta.deterministic) {
    lines.push("Deterministic source");
  } else {
    const qualifier = input.dashed ? " (low)" : "";
    lines.push(`Confidence ${pct}%${qualifier}`);
  }
  return lines.join("\n");
}
