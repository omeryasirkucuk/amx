/**
 * ColumnEdge — bezier edge with hover-only label, optional
 * cardinality markers, and a floating editor popover when selected.
 *
 * The endpoints float: instead of trusting the source / target
 * positions baked into the node (every table declares
 * ``sourcePosition: Right`` / ``targetPosition: Left`` at creation),
 * we compute, at render time, which side of each node faces the
 * other along the center-to-center axis. That switch is what
 * removes the "wrap-around bow" shape that appeared when neighbours
 * were placed above / below / left of the anchor in radial layout.
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
  Position,
  getBezierPath,
  useReactFlow,
  useStore,
  type ReactFlowState,
} from "reactflow";

import type {
  CanvasEdge,
  CanvasEdgeData,
  CanvasNode,
  EdgeCardinality,
  TableNodeData,
} from "../types";
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

/** Pick the side of a node-rect (T/R/B/L) that faces ``target``
 *  along the center-to-center vector, then return the midpoint of
 *  that side as the floating handle anchor. */
function floatingEndpoint(
  rect: { x: number; y: number; width: number; height: number },
  target: { x: number; y: number },
): { x: number; y: number; pos: Position } {
  const cx = rect.x + rect.width / 2;
  const cy = rect.y + rect.height / 2;
  const dx = target.x - cx;
  const dy = target.y - cy;
  // Compare horizontal vs vertical reach relative to the node's
  // half-extent so wider-than-tall nodes still favour their short
  // sides when the target is mostly above / below.
  const ratioH = Math.abs(dx) / (rect.width / 2);
  const ratioV = Math.abs(dy) / (rect.height / 2);
  if (ratioH >= ratioV) {
    return dx >= 0
      ? { x: rect.x + rect.width, y: cy, pos: Position.Right }
      : { x: rect.x, y: cy, pos: Position.Left };
  }
  return dy >= 0
    ? { x: cx, y: rect.y + rect.height, pos: Position.Bottom }
    : { x: cx, y: rect.y, pos: Position.Top };
}

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
  source,
  target,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  sourceHandleId,
  targetHandleId,
  data,
  selected,
  style,
}: EdgeProps<CanvasEdgeData>) {
  const rf = useReactFlow<unknown, CanvasEdgeData>();
  // ``useStore`` re-runs the selector on every store change but the
  // shallow-equal comparison keeps re-renders to the cases where the
  // source / target node moved or resized. We need the geometry to
  // compute the floating handle position.
  const floating = useStore(
    useCallback(
      (s: ReactFlowState) => {
        const sNode = s.nodeInternals.get(source);
        const tNode = s.nodeInternals.get(target);
        if (!sNode || !tNode) return null;
        const sRect = {
          x: sNode.positionAbsolute?.x ?? sNode.position?.x ?? 0,
          y: sNode.positionAbsolute?.y ?? sNode.position?.y ?? 0,
          width: sNode.width ?? 240,
          height: sNode.height ?? 140,
        };
        const tRect = {
          x: tNode.positionAbsolute?.x ?? tNode.position?.x ?? 0,
          y: tNode.positionAbsolute?.y ?? tNode.position?.y ?? 0,
          width: tNode.width ?? 240,
          height: tNode.height ?? 140,
        };
        const sCenter = {
          x: sRect.x + sRect.width / 2,
          y: sRect.y + sRect.height / 2,
        };
        const tCenter = {
          x: tRect.x + tRect.width / 2,
          y: tRect.y + tRect.height / 2,
        };
        // When both endpoints are tables that the user has opened
        // AND the edge carries a column handle id, anchor to the
        // actual column row instead of the floating rect midpoint.
        // ReactFlow stores the per-handle bounds on the internal
        // node once it has measured the handles, so we can read
        // their absolute position from there.
        const sData = sNode.data as TableNodeData | undefined;
        const tData = tNode.data as TableNodeData | undefined;
        const bothExpanded = !!sData?.expanded && !!tData?.expanded;
        // ``handleBounds`` lives on the internal node shape but is
        // not part of ReactFlow's public ``Node`` type; cast to
        // ``unknown`` first so the typechecker lets us read it.
        type HandleBound = {
          id: string | null;
          x: number;
          y: number;
          width: number;
          height: number;
        };
        type WithBounds = {
          handleBounds?: {
            source?: HandleBound[];
            target?: HandleBound[];
          };
        };
        const sBounds = (sNode as unknown as WithBounds).handleBounds;
        const tBounds = (tNode as unknown as WithBounds).handleBounds;
        const sHandle =
          bothExpanded && sourceHandleId
            ? sBounds?.source?.find((h) => h.id === sourceHandleId)
            : undefined;
        const tHandle =
          bothExpanded && targetHandleId
            ? tBounds?.target?.find((h) => h.id === targetHandleId)
            : undefined;
        const src =
          sHandle
            ? {
                x: sRect.x + sHandle.x + sHandle.width,
                y: sRect.y + sHandle.y + sHandle.height / 2,
                pos: Position.Right,
              }
            : floatingEndpoint(sRect, tCenter);
        const tgt =
          tHandle
            ? {
                x: tRect.x + tHandle.x,
                y: tRect.y + tHandle.y + tHandle.height / 2,
                pos: Position.Left,
              }
            : floatingEndpoint(tRect, sCenter);
        return { src, tgt };
      },
      [source, target, sourceHandleId, targetHandleId],
    ),
  );
  const [hovered, setHovered] = useState(false);
  // Use the floating geometry when available; fall back to ReactFlow's
  // baked-in coordinates if the store lookup fails (defensive — the
  // store should always have both nodes for a rendered edge).
  const sx = floating?.src.x ?? sourceX;
  const sy = floating?.src.y ?? sourceY;
  const tx = floating?.tgt.x ?? targetX;
  const ty = floating?.tgt.y ?? targetY;
  const sPos = floating?.src.pos ?? sourcePosition;
  const tPos = floating?.tgt.pos ?? targetPosition;
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX: sx,
    sourceY: sy,
    sourcePosition: sPos,
    targetX: tx,
    targetY: ty,
    targetPosition: tPos,
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

  /** Force both endpoints' tables to expand and highlight the
   *  source / target column rows. The forceExpandTick bump on each
   *  node nudges DataFrameNode's useEffect to flip its local
   *  ``expanded`` state without depending on a parent reset. */
  const handleJumpToColumns = useCallback(() => {
    if (!sourceHandleId || !targetHandleId) return;
    rf.setNodes((nds) =>
      (nds as CanvasNode[]).map((n) => {
        if (n.data.kind !== "table") return n;
        const td = n.data as TableNodeData;
        if (n.id === source) {
          return {
            ...n,
            data: {
              ...td,
              expanded: true,
              forceExpandTick: (td.forceExpandTick ?? 0) + 1,
              tracedColumn: sourceHandleId,
            },
          };
        }
        if (n.id === target) {
          return {
            ...n,
            data: {
              ...td,
              expanded: true,
              forceExpandTick: (td.forceExpandTick ?? 0) + 1,
              tracedColumn: targetHandleId,
            },
          };
        }
        return n;
      }),
    );
  }, [rf, source, sourceHandleId, target, targetHandleId]);

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
            fromColumn={sourceHandleId ?? undefined}
            toColumn={targetHandleId ?? undefined}
            onChange={applyChange}
            onDelete={handleDelete}
            onClose={handleClose}
            onJumpToColumns={handleJumpToColumns}
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
