/**
 * Pure heuristic that proposes ``name_match`` edges between every
 * pair of table nodes on the canvas. Used by the "Discover related"
 * toolbar action when the backend's ``catalog_relationships`` table
 * has no FK / view-DDL / query-log rows between the tables in
 * question — the user still expects some "best guess" overlap
 * surfaced from column names alone.
 *
 * Matching rule (intentionally simple — v1):
 *   1. Normalise each column name: lowercase + strip non-alphanumeric.
 *   2. For each (tableA, tableB) pair, find columns whose normalised
 *      names exactly match.
 *   3. Emit one edge per match, tagged ``relationship_type =
 *      "name_match"`` so the rest of the canvas (legend, color,
 *      dashed-by-default style, popover) treats it exactly like an
 *      LLM-proposed weak edge. The user can promote it (change
 *      cardinality / colour) or delete it via the popover.
 *
 * Out of scope for v1: type-aware match, fuzzy / substring match,
 * embedding-based similarity, ranking. The first two are easy
 * follow-ups but the user explicitly asked for "isimlerden string
 * match" — start there.
 */

import type { CanvasEdge, CanvasNode, TableNodeData } from "../types";
import { EDGE_COLORS } from "../constants";

/** Local-only edge id prefix so we can tell unsaved heuristic edges
 *  apart from server-persisted ones at a glance. */
const ID_PREFIX = "name-match-local";

const NAME_MATCH_CONFIDENCE = 0.5;

function normaliseColumnName(raw: string): string {
  return raw.toLowerCase().replace(/[^a-z0-9]+/g, "");
}

interface ProposeOptions {
  /** Skip pairs already connected by an existing edge (any
   *  relationship_type) at the same column endpoints. Stops the
   *  helper from drowning a canvas in duplicates of the LLM /
   *  catalog edges that already cover the same pair. */
  existingEdges: CanvasEdge[];
  /** Cap per (tableA, tableB) pair so very wide SAP-style tables
   *  (200+ columns each) do not spawn hundreds of low-signal
   *  edges. Sorted alphabetically by column name so the kept set
   *  is deterministic across runs. */
  maxPerPair?: number;
}

export interface NameMatchProposeResult {
  edges: CanvasEdge[];
  /** Pairs that were skipped because at least one side had no
   *  ``columns`` cached — surface this to the caller so it can
   *  decide whether to fetch the columns and try again. */
  pairsSkippedForMissingColumns: number;
}

export function proposeNameMatchEdges(
  nodes: CanvasNode[],
  opts: ProposeOptions,
): NameMatchProposeResult {
  const tables = nodes.filter(
    (n) => n.data.kind === "table",
  ) as Array<CanvasNode & { data: TableNodeData }>;
  if (tables.length < 2) {
    return { edges: [], pairsSkippedForMissingColumns: 0 };
  }
  const maxPerPair = Math.max(1, opts.maxPerPair ?? 12);
  // Index existing edges by ``${sourceId}|${sourceCol}|${targetId}|${targetCol}``
  // and its reverse so we skip pairs that are already covered
  // regardless of direction.
  const existingKey = new Set<string>();
  for (const e of opts.existingEdges) {
    const s = e.sourceHandle || "";
    const t = e.targetHandle || "";
    existingKey.add(`${e.source}|${s}|${e.target}|${t}`);
    existingKey.add(`${e.target}|${t}|${e.source}|${s}`);
  }
  const colorNameMatch = EDGE_COLORS.name_match ?? EDGE_COLORS.unknown;
  const out: CanvasEdge[] = [];
  let skipped = 0;
  for (let i = 0; i < tables.length; i += 1) {
    for (let j = i + 1; j < tables.length; j += 1) {
      const a = tables[i];
      const b = tables[j];
      const aCols = a.data.columns ?? [];
      const bCols = b.data.columns ?? [];
      if (aCols.length === 0 || bCols.length === 0) {
        skipped += 1;
        continue;
      }
      const aIndex = new Map<string, string>();
      for (const c of aCols) {
        const key = normaliseColumnName(c.name);
        if (key) aIndex.set(key, c.name);
      }
      const matches: Array<{ aName: string; bName: string }> = [];
      for (const c of bCols) {
        const key = normaliseColumnName(c.name);
        const hit = aIndex.get(key);
        if (hit) matches.push({ aName: hit, bName: c.name });
      }
      // Determinism: cap by sorted column name so the same canvas
      // always produces the same edge set.
      matches.sort((x, y) => x.aName.localeCompare(y.aName));
      const kept = matches.slice(0, maxPerPair);
      for (const m of kept) {
        const fwd = `${a.id}|${m.aName}|${b.id}|${m.bName}`;
        if (existingKey.has(fwd)) continue;
        existingKey.add(fwd);
        existingKey.add(`${b.id}|${m.bName}|${a.id}|${m.aName}`);
        out.push({
          id: `${ID_PREFIX}-${a.id}-${m.aName}-${b.id}-${m.bName}`,
          source: a.id,
          target: b.id,
          sourceHandle: m.aName,
          targetHandle: m.bName,
          type: "column-edge",
          data: {
            relationshipType: "name_match",
            source: "name_match_heuristic",
            confidence: NAME_MATCH_CONFIDENCE,
            verdict: "",
            hoverLabel: `${m.aName} → ${m.bName}\nName match\nConfidence ${Math.round(
              NAME_MATCH_CONFIDENCE * 100,
            )}% (low)`,
          },
          style: {
            stroke: colorNameMatch,
            strokeWidth: 1.1,
            strokeDasharray: "5 4",
          },
        });
      }
    }
  }
  return { edges: out, pairsSkippedForMissingColumns: skipped };
}
