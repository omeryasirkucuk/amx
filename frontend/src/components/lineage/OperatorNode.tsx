/**
 * OperatorNode — v4 transformation operator (filter / join / function /
 * aggregate / projection) rendered as a first-class node between
 * source and target columns. Mirrors the datapav model.
 *
 * Body is read-only in S3 — the inline expression editor lands in S5.
 * Kind drives the border colour so the user can scan filter vs
 * function vs aggregate at a glance.
 */

import { memo } from "react";
import { Handle, Position, type NodeProps } from "reactflow";

export interface OperatorNodeData {
  label: string;
  op_kind?: string;
  expression?: string;
}

const KIND_BORDER: Record<string, string> = {
  filter: "border-orange-500/80 bg-orange-950/50 text-orange-100",
  join: "border-purple-500/80 bg-purple-950/50 text-purple-100",
  aggregate: "border-cyan-500/80 bg-cyan-950/50 text-cyan-100",
  function: "border-emerald-500/80 bg-emerald-950/50 text-emerald-100",
  projection: "border-slate-500/80 bg-slate-900 text-slate-100",
};

const KIND_LABEL: Record<string, string> = {
  filter: "WHERE",
  join: "JOIN",
  aggregate: "GROUP",
  function: "ƒ()",
  projection: "AS",
};

function OperatorNodeImpl({ data }: NodeProps<OperatorNodeData>) {
  const kind = (data.op_kind || "function").toLowerCase();
  const cls = KIND_BORDER[kind] ?? KIND_BORDER.function;
  const kindLabel = KIND_LABEL[kind] ?? kind;
  const expression = data.expression ?? "";
  return (
    <div
      className={
        "rounded-md border-2 px-2 py-1 text-[10px] font-mono shadow-sm " +
        "min-w-[120px] max-w-[240px] " +
        cls
      }
      title={expression || kind}
    >
      <Handle
        id="in"
        type="target"
        position={Position.Left}
        className="!h-2 !w-2 !rounded-full !border-slate-300 !bg-slate-100"
      />
      <div className="flex items-baseline gap-1.5">
        <span className="rounded bg-black/30 px-1 py-px text-[9px] uppercase tracking-wider">
          {kindLabel}
        </span>
        <span className="truncate text-[10px]">{expression || kind}</span>
      </div>
      <Handle
        id="out"
        type="source"
        position={Position.Right}
        className="!h-2 !w-2 !rounded-full !border-slate-300 !bg-slate-100"
      />
    </div>
  );
}

export const OperatorNode = memo(OperatorNodeImpl);
OperatorNode.displayName = "OperatorNode";
