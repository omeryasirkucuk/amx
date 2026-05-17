/**
 * DataFrameNode — the canvas's signature node.
 *
 * Renders a table header (schema.table + optional profile chip + anchor
 * badge) plus one row per column. Each column row carries:
 *   - left target handle (column-level)
 *   - colored type badge
 *   - column name (clickable for trace)
 *   - right source handle (column-level)
 *
 * Handles are color-coded by data type so the user can read the wiring
 * at a glance. The whole node is draggable; column rows participate in
 * connect-by-drag via React Flow's per-handle id.
 */

import { memo, useMemo } from "react";
import { Handle, NodeProps, Position } from "reactflow";
import { Star } from "lucide-react";
import clsx from "clsx";

import { TYPE_COLORS, normalizeType, shortTypeLabel } from "../constants";
import type { TableNodeData } from "../types";

interface RowProps {
  name: string;
  dtype: string;
  isPrimary?: boolean;
  isForeign?: boolean;
  traced?: boolean;
}

function ColumnRow({ name, dtype, isPrimary, isForeign, traced }: RowProps) {
  const typeKey = normalizeType(dtype);
  const color = TYPE_COLORS[typeKey];
  return (
    <div
      className={clsx(
        "lcv-col-row",
        traced && "bg-accent-soft text-accent-ink",
      )}
      style={{ position: "relative" }}
    >
      <Handle
        type="target"
        position={Position.Left}
        id={name}
        className="lcv-handle"
        style={{ background: color, left: -5 }}
      />
      <span
        className="lcv-type-badge"
        style={{ background: color }}
        title={dtype || "unknown"}
      >
        {shortTypeLabel(dtype)}
      </span>
      <span className="flex-1 truncate font-mono text-[12px]">
        {name}
        {isPrimary && (
          <span className="ml-1 text-[9px] uppercase tracking-wide text-amber-300">
            pk
          </span>
        )}
        {isForeign && (
          <span className="ml-1 text-[9px] uppercase tracking-wide text-sky-300">
            fk
          </span>
        )}
      </span>
      <Handle
        type="source"
        position={Position.Right}
        id={name}
        className="lcv-handle"
        style={{ background: color, right: -5 }}
      />
    </div>
  );
}

function DataFrameNodeImpl({ data, selected }: NodeProps<TableNodeData>) {
  const columns = useMemo(() => data.columns || [], [data.columns]);
  const headerLabel = data.schema ? `${data.schema}.${data.table}` : data.table;
  return (
    <div
      className={clsx(
        "rounded-lg border bg-surface-raised text-ink shadow-lg",
        "transition-shadow duration-150",
        selected
          ? "border-accent-default shadow-accent-soft/40"
          : data.isAnchor
            ? "border-amber-400/70"
            : "border-surface-border",
      )}
      style={{ minWidth: 240, maxWidth: 320 }}
    >
      <div className="flex items-center gap-2 border-b border-surface-border px-3 py-2">
        {data.isAnchor && <Star size={12} className="text-amber-300" fill="currentColor" />}
        <span className="flex-1 truncate text-[13px] font-semibold">
          {headerLabel}
        </span>
        {data.showProfileChip && data.profile && (
          <span
            title={data.profile}
            className="rounded bg-surface px-1.5 py-0.5 text-[9px] uppercase tracking-wide text-fg-muted"
          >
            {data.profile.slice(0, 10)}
          </span>
        )}
      </div>
      {/* Dangling handles when no columns are present yet — keeps
          table-level edges drawable while the catalog loads columns. */}
      {columns.length === 0 ? (
        <>
          <Handle
            type="target"
            position={Position.Left}
            id="__table__"
            className="lcv-handle"
            style={{ background: TYPE_COLORS.unknown, top: "50%" }}
          />
          <div className="px-3 py-3 text-[11px] text-fg-muted">
            (no columns cached)
          </div>
          <Handle
            type="source"
            position={Position.Right}
            id="__table__"
            className="lcv-handle"
            style={{ background: TYPE_COLORS.unknown, top: "50%" }}
          />
        </>
      ) : (
        <div className="py-1">
          {columns.map((col) => (
            <ColumnRow
              key={col.name}
              name={col.name}
              dtype={col.dtype}
              isPrimary={col.isPrimary}
              isForeign={col.isForeign}
              traced={data.tracedColumn === col.name}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export const DataFrameNode = memo(DataFrameNodeImpl);
