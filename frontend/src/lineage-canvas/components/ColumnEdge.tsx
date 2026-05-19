/**
 * ColumnEdge — bezier edge with hover-only label, optional
 * cardinality markers, and a floating editor popover when selected.
 *
 * A 16px-wide invisible overlay path catches hover and click events
 * so the user can land on thin / dashed edges easily (Dataloom
 * trick). The label is a small monospace pill anchored to the path
 * midpoint and is suppressed while the editor popover is open so the
 * two surfaces never collide.
 */

import { memo, useCallback, useState } from "react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  EdgeProps,
  getBezierPath,
  useReactFlow,
} from "reactflow";

import type { CanvasEdge, CanvasEdgeData, EdgeCardinality } from "../types";
import { lineageDeleteEdge, lineageEdgeStyle } from "../../lib/api";
import { EdgeEditorPopover, type EdgeStylePatch } from "./EdgeEditorPopover";

// SVG marker ids — referenced by the BaseEdge's markerStart /
// markerEnd attributes. Defined once at module level (in
// ``ColumnEdgeMarkerDefs``) so every edge points at the same
// ``<marker>`` elements without redeclaring them per render.
const MARKER_IDS = {
  one: "lcv-marker-one",
  many: "lcv-marker-many",
} as const;

function endsFor(c?: EdgeCardinality): { source: string | undefined; target: string | undefined } {
  if (!c) return { source: undefined, target: undefined };
  if (c === "1:1") return { source: markerUrl("one"), target: markerUrl("one") };
  if (c === "1:N") return { source: markerUrl("one"), target: markerUrl("many") };
  return { source: markerUrl("many"), target: markerUrl("many") };
}

function markerUrl(key: keyof typeof MARKER_IDS): string {
  return `url(#${MARKER_IDS[key]})`;
}

function ColumnEdgeImpl({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  selected,
  style,
}: EdgeProps<CanvasEdgeData>) {
  const rf = useReactFlow<unknown, CanvasEdgeData>();
  const [hovered, setHovered] = useState(false);
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });
  const cardinality = data?.cardinality;
  const markers = endsFor(cardinality);
  const showPopover = !!selected;

  const applyChange = useCallback(
    (patch: EdgeStylePatch) => {
      // Optimistic local update so the canvas reflects the change
      // immediately, regardless of whether a backend PATCH is
      // possible (AI-stream edges without a persisted ``edgeId``
      // skip the PATCH entirely).
      rf.setEdges((eds) =>
        (eds as CanvasEdge[]).map((e) => {
          if (e.id !== id) return e;
          const nextData: CanvasEdgeData = { ...(e.data || {} as CanvasEdgeData) };
          if ("styleColor" in patch) {
            nextData.styleColor = patch.styleColor ?? undefined;
          }
          if ("styleDashed" in patch) {
            nextData.styleDashed = patch.styleDashed ?? undefined;
          }
          if ("cardinality" in patch) {
            nextData.cardinality = (patch.cardinality ?? undefined) as
              | EdgeCardinality
              | undefined;
          }
          const nextStyle = { ...(e.style || {}) };
          if (nextData.styleColor) {
            nextStyle.stroke = nextData.styleColor;
          }
          if (nextData.styleDashed === true) {
            nextStyle.strokeDasharray = "5 4";
          } else if (nextData.styleDashed === false) {
            nextStyle.strokeDasharray = undefined;
          }
          return { ...e, data: nextData, style: nextStyle };
        }),
      );
      const edgeId = data?.edgeId;
      if (edgeId) {
        // Fire and forget — failures are surfaced via global toast
        // pipeline, the optimistic update stays put so the user
        // does not see a UI flicker.
        lineageEdgeStyle(edgeId, {
          style_color: "styleColor" in patch ? patch.styleColor ?? null : undefined,
          style_dashed: "styleDashed" in patch ? patch.styleDashed ?? null : undefined,
          cardinality: "cardinality" in patch ? patch.cardinality ?? null : undefined,
        }).catch(() => undefined);
      }
    },
    [data?.edgeId, id, rf],
  );

  const handleDelete = useCallback(() => {
    const edgeId = data?.edgeId;
    rf.setEdges((eds) => (eds as CanvasEdge[]).filter((e) => e.id !== id));
    if (edgeId) {
      lineageDeleteEdge(edgeId).catch(() => undefined);
    }
  }, [data?.edgeId, id, rf]);

  const handleClose = useCallback(() => {
    rf.setEdges((eds) =>
      (eds as CanvasEdge[]).map((e) =>
        e.id === id ? { ...e, selected: false } : e,
      ),
    );
  }, [id, rf]);

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        style={style}
        markerStart={markers.source}
        markerEnd={markers.target}
      />
      <path
        d={edgePath}
        fill="none"
        stroke="transparent"
        strokeWidth={16}
        style={{ pointerEvents: "stroke" }}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
      />
      {!showPopover && hovered && data?.hoverLabel && (
        <EdgeLabelRenderer>
          <div
            className="lcv-edge-label"
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
              pointerEvents: "none",
            }}
          >
            {data.hoverLabel}
          </div>
        </EdgeLabelRenderer>
      )}
      {showPopover && (
        <EdgeLabelRenderer>
          <EdgeEditorPopover
            x={labelX}
            y={labelY}
            styleColor={data?.styleColor}
            styleDashed={data?.styleDashed}
            cardinality={data?.cardinality}
            onChange={applyChange}
            onDelete={handleDelete}
            onClose={handleClose}
          />
        </EdgeLabelRenderer>
      )}
    </>
  );
}

export const ColumnEdge = memo(ColumnEdgeImpl);

/** Module-level SVG marker defs used by every ColumnEdge. Mounted
 *  once near the top of the canvas so multiple edges share the
 *  underlying ``<marker>`` elements instead of redeclaring them per
 *  render. */
export function ColumnEdgeMarkerDefs() {
  return (
    <svg
      aria-hidden
      style={{ position: "absolute", width: 0, height: 0 }}
    >
      <defs>
        {/* One — a single short stroke perpendicular to the edge. */}
        <marker
          id={MARKER_IDS.one}
          markerWidth="10"
          markerHeight="10"
          refX="9"
          refY="5"
          orient="auto-start-reverse"
          markerUnits="userSpaceOnUse"
        >
          <line x1="5" y1="0" x2="5" y2="10" stroke="currentColor" strokeWidth="1.2" />
        </marker>
        {/* Many — crow's foot. */}
        <marker
          id={MARKER_IDS.many}
          markerWidth="12"
          markerHeight="10"
          refX="11"
          refY="5"
          orient="auto-start-reverse"
          markerUnits="userSpaceOnUse"
        >
          <path
            d="M 0 5 L 10 0 M 0 5 L 10 5 M 0 5 L 10 10"
            stroke="currentColor"
            strokeWidth="1.2"
            fill="none"
          />
        </marker>
      </defs>
    </svg>
  );
}

export const edgeTypes = {
  "column-edge": ColumnEdge,
};
