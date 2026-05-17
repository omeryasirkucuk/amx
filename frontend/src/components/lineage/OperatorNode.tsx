/**
 * OperatorNode — v4 transformation operator (filter / join / function /
 * aggregate / projection) rendered as a first-class node between
 * source and target columns. Mirrors the datapav model.
 *
 * Body is read-only in S3 — the inline expression editor lands in S5.
 * Kind drives the border colour so the user can scan filter vs
 * function vs aggregate at a glance.
 */

import { memo, useEffect, useRef, useState } from "react";
import { Handle, Position, type NodeProps } from "reactflow";
import { Check, X } from "lucide-react";

export interface OperatorNodeData {
  label: string;
  op_kind?: string;
  expression?: string;
  /** v4 S5 — operator entity id from the catalog so the editor can
   *  PATCH it back. None means the node is purely synthetic (an
   *  in-memory split of an extractor edge); the editor stays
   *  disabled in that case. */
  operator_id?: number | null;
  /** Called with the new expression text when the user finishes
   *  editing. Parent owns the network call + cache invalidation. */
  onEditExpression?: (operatorId: number, expression: string) => void;
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

  const editable = data.operator_id != null && Boolean(data.onEditExpression);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(expression);
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => {
    if (editing) {
      setDraft(expression);
      // requestAnimationFrame so React has flushed the textarea into the DOM.
      window.requestAnimationFrame(() => {
        textareaRef.current?.focus();
        textareaRef.current?.select();
      });
    }
  }, [editing, expression]);

  const commit = () => {
    if (!editable || data.operator_id == null) return;
    const next = draft.trim();
    if (next && next !== expression) {
      data.onEditExpression?.(data.operator_id, next);
    }
    setEditing(false);
  };

  const cancel = () => {
    setDraft(expression);
    setEditing(false);
  };

  const startEdit = (event: React.MouseEvent) => {
    if (!editable) return;
    event.stopPropagation();
    setEditing(true);
  };

  return (
    <div
      className={
        "rounded-md border-2 px-2 py-1 text-[10px] font-mono shadow-sm " +
        "min-w-[140px] max-w-[260px] " +
        cls
      }
      title={editable ? "Double-click to edit expression" : (expression || kind)}
      onDoubleClick={startEdit}
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
        {editing ? (
          <div className="flex flex-1 items-center gap-1">
            <textarea
              ref={textareaRef}
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  commit();
                } else if (e.key === "Escape") {
                  e.preventDefault();
                  cancel();
                }
              }}
              onClick={(e) => e.stopPropagation()}
              rows={2}
              className="flex-1 resize-none rounded border border-white/30 bg-black/30 px-1 py-0.5 text-[10px] font-mono text-white outline-none"
            />
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                commit();
              }}
              className="rounded p-0.5 text-emerald-300 hover:bg-emerald-900/50"
              title="Save (Enter)"
            >
              <Check className="h-3 w-3" />
            </button>
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                cancel();
              }}
              className="rounded p-0.5 text-rose-300 hover:bg-rose-900/50"
              title="Cancel (Esc)"
            >
              <X className="h-3 w-3" />
            </button>
          </div>
        ) : (
          <span className="truncate text-[10px]">{expression || kind}</span>
        )}
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
