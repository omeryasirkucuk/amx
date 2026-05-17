/**
 * TableNode — v4 column-level lineage table.
 *
 * Header carries the table label + anchor badge. Body lists every
 * known column from the catalog cache (data.columns). Each row has a
 * left target Handle and a right source Handle whose `id` is the
 * column name, so React Flow's onConnect callback receives the column
 * pair directly without us having to look it up by node id.
 *
 * The "+ link" affordance from datapav is the green plus on the right
 * port — it appears on row hover via the `group/row` Tailwind utility
 * and only when the row's column name is present.
 *
 * The node is intentionally not draggable inside the canvas (dagre
 * lays it out); the user expresses changes via the column ports
 * instead.
 */

import { memo } from "react";
import { Handle, Position, type NodeProps, useStore } from "reactflow";

import type { LineageNodeColumn } from "../../lib/api";

/** v4 S6 — collapse columns into a header+count summary when the
 *  zoom falls below this threshold. Pulled out so the OperatorNode
 *  rendering can read the same threshold if needed. */
const LOD_ZOOM_CUTOFF = 0.55;

export interface TableNodeData {
  label: string;
  anchor: boolean;
  described: boolean;
  columns?: LineageNodeColumn[];
  /** When set, this column name is highlighted as the trace target. */
  tracedColumn?: string | null;
  /** Called when the user clicks a column row. Wired by the canvas
   *  parent to open the trace panel for that column. */
  onColumnClick?: (column: string) => void;
}

function dtypeBadge(dtype: string): string {
  const d = dtype.toLowerCase();
  if (d.includes("int") || d.includes("serial")) return "int";
  if (d.includes("float") || d.includes("numeric") || d.includes("decimal") || d.includes("double"))
    return "flt";
  if (d.includes("date") || d.includes("time")) return "dat";
  if (d.includes("bool")) return "bool";
  if (d.includes("char") || d.includes("text") || d.includes("string") || d.includes("varchar"))
    return "str";
  if (d) return d.slice(0, 4);
  return "—";
}

function TableNodeImpl({ data }: NodeProps<TableNodeData>) {
  const cols = data.columns ?? [];
  // v4 S6 — collapse rows when the canvas is zoomed out far enough
  // that text would be unreadable anyway. Keeps 1000-column scenes
  // responsive without losing the table outline.
  const zoom = useStore((s) => s.transform[2]);
  const collapsed = zoom < LOD_ZOOM_CUTOFF;
  const headerCls = data.anchor
    ? "border-amber-500 bg-amber-50 text-amber-900"
    : "border-slate-700 bg-slate-900 text-slate-50";
  return (
    <div
      className={
        "rounded-lg border shadow-sm text-xs font-sans overflow-hidden " +
        "min-w-[200px] max-w-[280px] " +
        (data.anchor ? "border-amber-500" : "border-slate-700")
      }
    >
      <div
        className={"flex items-center gap-1 px-3 py-1.5 font-mono text-[11px] " + headerCls}
        title={data.label}
      >
        {data.anchor && <span aria-hidden="true">★</span>}
        <span className="truncate">{data.label}</span>
        {data.described && (
          <span className="ml-auto text-[10px]" title="Has description">
            ✓
          </span>
        )}
      </div>
      {cols.length === 0 ? (
        <div className="bg-slate-800 px-3 py-2 text-[10px] italic text-slate-400">
          No columns cached yet.
        </div>
      ) : collapsed ? (
        <div className="flex items-center justify-between bg-slate-900 px-3 py-1.5 text-[10px] text-slate-400">
          <span>{cols.length} cols</span>
          <span className="text-slate-500" aria-hidden="true">
            ⤓
          </span>
          {/* Keep at least one source + target handle so dangling
              edges still render correctly even while collapsed. */}
          <Handle
            id="__lod_in"
            type="target"
            position={Position.Left}
            className="!h-2 !w-2 !rounded-full !border-slate-500 !bg-slate-700"
          />
          <Handle
            id="__lod_out"
            type="source"
            position={Position.Right}
            className="!h-2 !w-2 !rounded-full !border-slate-500 !bg-slate-700"
          />
        </div>
      ) : (
        <ul className="divide-y divide-slate-800 bg-slate-900">
          {cols.map((col) => {
            const isTraced = data.tracedColumn === col.name;
            const handleClick = () => data.onColumnClick?.(col.name);
            return (
              <li
                key={col.name}
                className={
                  "group/row relative flex items-center gap-2 px-3 py-1 font-mono text-[11px] cursor-pointer " +
                  (isTraced
                    ? "bg-amber-900/30 text-amber-100"
                    : "text-slate-200 hover:bg-slate-800")
                }
                data-column={col.name}
                onClick={handleClick}
              >
                <Handle
                  id={col.name}
                  type="target"
                  position={Position.Left}
                  className="!h-2 !w-2 !rounded-full !border-slate-500 !bg-slate-700"
                />
                <span
                  className="text-[9px] font-semibold uppercase tracking-wide text-slate-500"
                  title={col.dtype || "unknown"}
                >
                  {dtypeBadge(col.dtype)}
                </span>
                <span className="truncate" title={col.name}>
                  {col.name}
                </span>
                <Handle
                  id={col.name}
                  type="source"
                  position={Position.Right}
                  className="!h-2 !w-2 !rounded-full !border-slate-500 !bg-slate-700"
                />
                <span
                  aria-hidden="true"
                  className="pointer-events-none absolute -right-2 top-1/2 hidden -translate-y-1/2 text-[10px] text-emerald-400 group-hover/row:block"
                >
                  +
                </span>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

export const TableNode = memo(TableNodeImpl);
TableNode.displayName = "TableNode";
