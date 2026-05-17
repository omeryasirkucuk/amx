/**
 * Generic OperatorNode for join / aggregate / function / projection.
 *
 * Editable expression. Same draggability + commit-back rules as
 * FilterNode: ``nodrag`` on the editor wrapper, ``useReactFlow().setNodes``
 * for state commits, local React state for the textarea value so the
 * caret behaves.
 */

import { memo, useState } from "react";
import { Handle, NodeProps, Position, useReactFlow } from "reactflow";
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

const PLACEHOLDERS: Record<string, string> = {
  join: "customers.id = orders.customer_id",
  aggregate: "country, status",
  function: "round(amount, 2) AS amount_r",
  projection: "id, name, status",
};

function OperatorNodeImpl({ id, data, selected }: NodeProps<OperatorNodeData>) {
  const rf = useReactFlow();
  const [expression, setExpression] = useState<string>(data.expression || "");
  const color = OPERATOR_COLORS[data.opKind] ?? OPERATOR_COLORS.function;
  const Icon = ICONS[data.opKind] ?? Code2;

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
      style={{ minWidth: 240, maxWidth: 340, borderLeft: `3px solid ${color}` }}
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
      <div className="nodrag nowheel px-2 pb-2 pt-1">
        <textarea
          value={expression}
          onChange={(e) => commit(e.target.value)}
          rows={3}
          spellCheck={false}
          placeholder={PLACEHOLDERS[data.opKind] || "expression…"}
          className="block w-full resize-none rounded border border-surface-border bg-transparent px-2 py-1.5 font-mono text-[12.5px] leading-snug text-ink outline-none focus:border-accent-default"
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

export const OperatorNode = memo(OperatorNodeImpl);
