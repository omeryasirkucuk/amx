/**
 * Generic OperatorNode for join / aggregate / function / projection.
 *
 * Simpler than FilterNode — no @-mention input — but visually
 * consistent: colored left border + uppercase tag in the header. The
 * expression is editable inline as a plain textarea (double-click).
 */

import { memo, useState } from "react";
import { Handle, NodeProps, Position } from "reactflow";
import { Code2, Combine, Layers3, type LucideIcon, Sparkles } from "lucide-react";
import clsx from "clsx";

import { OPERATOR_COLORS } from "../constants";
import type { OperatorNodeData } from "../types";

const ICONS: Record<string, LucideIcon> = {
  join: Combine,
  aggregate: Layers3,
  function: Code2,
  projection: Sparkles,
};

const LABELS: Record<string, string> = {
  join: "join",
  aggregate: "group by",
  function: "fn",
  projection: "select",
};

function OperatorNodeImpl({ data, selected }: NodeProps<OperatorNodeData>) {
  const [editing, setEditing] = useState(false);
  const color = OPERATOR_COLORS[data.opKind] ?? OPERATOR_COLORS.function;
  const Icon = ICONS[data.opKind] ?? Code2;
  return (
    <div
      className={clsx(
        "rounded-lg border bg-surface-raised text-ink shadow-lg",
        selected ? "border-accent-default" : "border-surface-border",
      )}
      style={{ minWidth: 220, maxWidth: 320, borderLeft: `3px solid ${color}` }}
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
        <Icon size={11} style={{ color }} />
        <span className="text-[12px] font-semibold uppercase tracking-wide" style={{ color }}>
          {LABELS[data.opKind] || data.opKind}
        </span>
      </div>
      <div className="px-2 pb-2 pt-1">
        {editing ? (
          <textarea
            value={data.expression}
            onChange={(e) => {
              data.expression = e.target.value;
            }}
            rows={3}
            spellCheck={false}
            autoFocus
            onBlur={() => setEditing(false)}
            className="w-full resize-none rounded border border-surface-border bg-transparent px-2 py-1.5 font-mono text-[12px] text-ink outline-none focus:border-accent-default"
          />
        ) : (
          <pre
            className="cursor-text whitespace-pre-wrap rounded border border-dashed border-surface-border bg-surface px-2 py-1.5 font-mono text-[12px]"
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

export const OperatorNode = memo(OperatorNodeImpl);
