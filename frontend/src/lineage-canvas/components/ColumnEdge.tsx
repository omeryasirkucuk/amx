/**
 * ColumnEdge — bezier edge with hover-only label.
 *
 * Renders a smooth bezier path between two ports. A 16px-wide
 * invisible overlay path catches hover so the label appears even when
 * the cursor lands between dashes on dashed edges (Dataloom trick).
 * The label is a small monospace pill anchored to the path midpoint.
 */

import { memo, useState } from "react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  EdgeProps,
  getBezierPath,
} from "reactflow";
import type { CanvasEdgeData } from "../types";

function ColumnEdgeImpl({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  style,
  markerEnd,
}: EdgeProps<CanvasEdgeData>) {
  const [hovered, setHovered] = useState(false);
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });
  return (
    <>
      <BaseEdge id={id} path={edgePath} style={style} markerEnd={markerEnd} />
      <path
        d={edgePath}
        fill="none"
        stroke="transparent"
        strokeWidth={16}
        style={{ pointerEvents: "stroke" }}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
      />
      {hovered && data?.hoverLabel && (
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
    </>
  );
}

export const ColumnEdge = memo(ColumnEdgeImpl);

export const edgeTypes = {
  "column-edge": ColumnEdge,
};
