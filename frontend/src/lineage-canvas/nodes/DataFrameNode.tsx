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
 * at a glance.
 *
 * Collapse / search for wide tables:
 *   SAP-style schemas (BSEG, MARA, …) routinely carry hundreds of
 *   columns. Letting the node render every row uncapped turns the
 *   canvas into an unusable pillar. We:
 *
 *     - Auto-collapse the column list when the table has > 25 columns
 *       (user can override via the header chevron).
 *     - Add a fast in-place search input above the rows when at least
 *       8 columns are present.
 *     - Cap the visible rows at ~400px tall with native scroll inside
 *       the node (``nowheel`` so it doesn't fight canvas zoom).
 *     - When the list is collapsed, we still mount the per-column
 *       Handle components inside a clipped wrapper so ReactFlow can
 *       resolve edges pointing at specific column ids — the edges
 *       still anchor to the right column, they just visually cluster
 *       at the header. The handles are ``visibility: hidden`` while
 *       collapsed so they don't render as floating dots.
 */

import { memo, useEffect, useMemo, useState } from "react";
import { Handle, NodeProps, Position, useReactFlow } from "reactflow";
import { ChevronDown, ChevronRight, Search, Star } from "lucide-react";
import clsx from "clsx";

import { TYPE_COLORS, normalizeType, shortTypeLabel } from "../constants";
import { LogoBadge } from "../logos/LogoBadge";
import { LogoPicker } from "../logos/LogoPicker";
import type { LogoRow } from "../logos/registry";
import { NodeDeleteToolbar } from "../components/NodeDeleteToolbar";
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

/** Hidden column handles — keep edges anchored when the table is collapsed. */
function HiddenHandles({ columns }: { columns: TableNodeData["columns"] }) {
  return (
    <div
      aria-hidden
      style={{
        position: "absolute",
        left: 0,
        right: 0,
        top: 28,
        height: 0,
        overflow: "hidden",
        visibility: "hidden",
      }}
    >
      {columns.map((c) => (
        <div key={c.name} className="lcv-col-row" style={{ position: "relative" }}>
          <Handle
            type="target"
            position={Position.Left}
            id={c.name}
            className="lcv-handle"
            style={{ left: -5 }}
          />
          <Handle
            type="source"
            position={Position.Right}
            id={c.name}
            className="lcv-handle"
            style={{ right: -5 }}
          />
        </div>
      ))}
    </div>
  );
}

const SEARCH_THRESHOLD = 8;

function DataFrameNodeImpl({ id, data, selected }: NodeProps<TableNodeData>) {
  const rf = useReactFlow();
  const columns = useMemo(() => data.columns || [], [data.columns]);
  const headerLabel = data.schema ? `${data.schema}.${data.table}` : data.table;

  // Tables render collapsed by default — the column rail crowds the
  // canvas after AI Generate when 6+ neighbours land at once. Users
  // open individual tables on demand via the header chevron or the
  // "+ show N columns" affordance.
  const [expanded, setExpanded] = useState<boolean>(false);
  // External callers (e.g. clicking the "Columns" line on an edge
  // popover) bump ``forceExpandTick`` to force this node open
  // without forcing the user to find the chevron.
  useEffect(() => {
    if (data.forceExpandTick) setExpanded(true);
  }, [data.forceExpandTick]);
  const [search, setSearch] = useState<string>("");
  const [logoPickerOpen, setLogoPickerOpen] = useState(false);

  function setLogoKey(next: string) {
    rf.setNodes((nodes) =>
      nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, logoKey: next } } : n,
      ),
    );
  }

  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return columns;
    return columns.filter((c) => c.name.toLowerCase().includes(needle));
  }, [columns, search]);

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
      style={{ minWidth: 260, maxWidth: 340 }}
    >
      <div className="flex items-center gap-2 border-b border-surface-border px-3 py-2">
        <button
          type="button"
          aria-label={expanded ? "Collapse columns" : "Expand columns"}
          onClick={(e) => {
            e.stopPropagation();
            setExpanded((v) => !v);
          }}
          className="nodrag flex h-4 w-4 items-center justify-center rounded text-fg-muted hover:text-ink"
        >
          {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        </button>
        <LogoBadge
          logoKey={data.logoKey}
          onClick={() => setLogoPickerOpen(true)}
        />
        {data.isAnchor && <Star size={12} className="text-amber-300" fill="currentColor" />}
        <span className="flex-1 truncate text-[13px] font-semibold" title={data.fqn}>
          {headerLabel}
        </span>
        {columns.length > 0 && (
          <span
            className="rounded bg-surface px-1.5 py-0.5 font-mono text-[10px] text-fg-muted"
            title={`${columns.length} columns`}
          >
            {columns.length}
          </span>
        )}
        {data.showProfileChip && data.profile && (
          <span
            title={data.profile}
            className="rounded bg-surface px-1.5 py-0.5 text-[9px] uppercase tracking-wide text-fg-muted"
          >
            {data.profile.slice(0, 10)}
          </span>
        )}
      </div>

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
      ) : !expanded ? (
        // Collapsed: header-only visual + hidden handle rail so per-column
        // edges still anchor correctly.
        <>
          <HiddenHandles columns={columns} />
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              setExpanded(true);
            }}
            className="nodrag block w-full px-3 py-1.5 text-left text-[11px] text-fg-muted hover:bg-surface"
          >
            + show {columns.length} column{columns.length === 1 ? "" : "s"}
          </button>
        </>
      ) : (
        <>
          {columns.length >= SEARCH_THRESHOLD && (
            <div className="nodrag relative border-b border-surface-border px-2 py-1">
              <Search
                size={11}
                className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-fg-muted"
              />
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="filter columns…"
                className="block w-full rounded bg-transparent pl-5 pr-1 py-0.5 text-[11.5px] outline-none placeholder:text-fg-muted"
              />
              <span className="absolute right-3 top-1/2 -translate-y-1/2 font-mono text-[10px] text-fg-muted">
                {filtered.length}/{columns.length}
              </span>
            </div>
          )}
          <div
            className="nowheel py-1"
            style={{ maxHeight: 400, overflowY: "auto" }}
          >
            {filtered.length === 0 ? (
              <div className="px-3 py-2 text-[11px] text-fg-muted">
                No matching columns.
              </div>
            ) : (
              filtered.map((col) => (
                <ColumnRow
                  key={col.name}
                  name={col.name}
                  dtype={col.dtype}
                  isPrimary={col.isPrimary}
                  isForeign={col.isForeign}
                  traced={data.tracedColumn === col.name}
                />
              ))
            )}
            {/* Mount handles for rows hidden by the search filter so
                edges to those columns stay anchored. */}
            {search.trim() && (
              <HiddenHandles
                columns={columns.filter(
                  (c) => !filtered.some((f) => f.name === c.name),
                )}
              />
            )}
          </div>
        </>
      )}
      <LogoPicker
        open={logoPickerOpen}
        onClose={() => setLogoPickerOpen(false)}
        onPick={(row: LogoRow) => setLogoKey(row.key)}
        onClear={data.logoKey ? () => setLogoKey("") : undefined}
        title="Pick header logo"
      />
      <NodeDeleteToolbar nodeId={id} visible={!!selected} />
    </div>
  );
}

export const DataFrameNode = memo(DataFrameNodeImpl);
