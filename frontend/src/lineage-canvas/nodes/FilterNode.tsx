/**
 * FilterNode — WHERE-clause operator.
 *
 * Editable expression with @-mention support. The single ``in`` handle
 * accepts an incoming table; the ``out`` handle feeds the filtered
 * result to the next node. Double-click enters edit mode.
 */

import { memo, useState } from "react";
import { Handle, NodeProps, Position } from "reactflow";
import { Filter } from "lucide-react";
import clsx from "clsx";

import { HighlightedConditionInput } from "../components/HighlightedConditionInput";
import { OPERATOR_COLORS } from "../constants";
import type { OperatorNodeData } from "../types";

function FilterNodeImpl({ data, selected }: NodeProps<OperatorNodeData>) {
  const [editing, setEditing] = useState(false);
  const color = OPERATOR_COLORS.filter;
  const cols = data.upstreamColumns || [];
  return (
    <div
      className={clsx(
        "rounded-lg border bg-surface-raised text-ink shadow-lg",
        selected ? "border-accent-default" : "border-surface-border",
      )}
      style={{ minWidth: 240, maxWidth: 320, borderLeft: `3px solid ${color}` }}
      onDoubleClick={() => setEditing(true)}
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
      <div className="px-2 pb-2 pt-1">
        {editing ? (
          <HighlightedConditionInput
            value={data.expression}
            onChange={(next) => {
              data.expression = next;
            }}
            columns={cols}
            rows={3}
          />
        ) : (
          <pre
            className="cursor-text whitespace-pre-wrap rounded border border-dashed border-surface-border bg-surface px-2 py-1.5 font-mono text-[12px] text-ink"
            onClick={() => setEditing(true)}
          >
            {data.expression || (
              <span className="text-fg-muted">double-click to edit…</span>
            )}
          </pre>
        )}
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
