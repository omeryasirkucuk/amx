/**
 * FilterNode — WHERE-clause operator.
 *
 * Editable expression with @-mention support. The single ``in`` handle
 * accepts an incoming table; the ``out`` handle feeds the filtered
 * result to the next node.
 *
 * Editing notes (matter — these are the reason the previous build's
 * inputs felt "dead"):
 *
 *   1. ReactFlow makes the whole node draggable by default. Any input
 *      nested inside the node steals the pointerdown but loses it to
 *      the drag listener unless the wrapper carries the ``nodrag``
 *      class. We mark every interactive area with ``nodrag``.
 *   2. ReactFlow's node `data` is stored in canvas state. Mutating
 *      ``data.expression`` directly does NOT trigger a re-render and
 *      does NOT persist through Save — the user's typing disappears.
 *      We hold a local state for the textarea and commit back into
 *      canvas state via :func:`useReactFlow().setNodes` on every
 *      change.
 *   3. ``nowheel`` lets the textarea scroll without zooming the
 *      canvas under the cursor.
 */

import { memo, useState } from "react";
import { Handle, NodeProps, Position, useReactFlow } from "reactflow";
import { Filter } from "lucide-react";
import clsx from "clsx";

import { HighlightedConditionInput } from "../components/HighlightedConditionInput";
import { OPERATOR_COLORS } from "../constants";
import type { OperatorNodeData } from "../types";

function FilterNodeImpl({ id, data, selected }: NodeProps<OperatorNodeData>) {
  const rf = useReactFlow();
  const [expression, setExpression] = useState<string>(data.expression || "");
  const color = OPERATOR_COLORS.filter;
  const cols = data.upstreamColumns || [];

  function commit(next: string) {
    setExpression(next);
    rf.setNodes((nodes) =>
      nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, expression: next } } : n,
      ),
    );
  }

  return (
    <div
      className={clsx(
        "rounded-lg border bg-surface-raised text-ink shadow-lg",
        selected ? "border-accent-default" : "border-surface-border",
      )}
      style={{ minWidth: 260, maxWidth: 340, borderLeft: `3px solid ${color}` }}
    >
      <Handle
        type="target"
        position={Position.Left}
        id="in"
        className="lcv-handle"
        style={{ background: color }}
      />
      <div className="flex items-center gap-1.5 border-b border-surface-border px-3 py-1.5">
        <Filter size={11} style={{ color }} />
        <span className="text-[12px] font-semibold uppercase tracking-wide" style={{ color }}>
          where
        </span>
        <span className="ml-auto text-[10px] text-fg-muted">filter</span>
      </div>
      <div className="nodrag nowheel px-2 pb-2 pt-1">
        <HighlightedConditionInput
          value={expression}
          onChange={commit}
          columns={cols}
          rows={3}
        />
      </div>
      <Handle
        type="source"
        position={Position.Right}
        id="out"
        className="lcv-handle"
        style={{ background: color }}
      />
    </div>
  );
}

export const FilterNode = memo(FilterNodeImpl);
