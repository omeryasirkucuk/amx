/**
 * DataFrameNode — the canvas's signature table node, styled 1:1 with
 * Databricks Unity Catalog's lineage table card:
 *
 *   Table                         ← kicker label above the card
 *   ┌──────────────────────────┐
 *   │ ▦ table_name          ▮▮▮ │  header: icon · name · popularity bars
 *   │ database.schema           │  schema breadcrumb
 *   │ ◔ Owner Name              │  owner (when known)
 *   │ [ Search columns…       ] │  per-column filter
 *   │ 47 columns                │  count
 *   │ 123  cust_key      bigint │  typed column rows (paged, 8/page)
 *   │ …                         │
 *   │      ‹ Previous  Next ›   │  paging footer
 *   └──────────────────────────┘
 *
 * Wide tables (SAP-style hundreds of columns) page through 8 rows at a
 * time rather than scrolling, matching Databricks. Off-page columns keep
 * hidden Handle components mounted so per-column edges stay anchored to
 * the right column even when that row isn't on screen.
 *
 * ``data.expanded`` (not local state) drives the column rail so ColumnEdge
 * can read it and decide whether to anchor edges to a column handle vs the
 * table rect. Tables open collapsed by default so a freshly-seeded lineage
 * canvas stays lean; the user opens columns on demand.
 */

import { memo, useEffect, useMemo, useState } from "react";
import { Handle, NodeProps, Position, useReactFlow } from "reactflow";
import { BarChart3, ChevronDown, ChevronRight, Search, Star, Table2, User } from "lucide-react";
import clsx from "clsx";

import { fetchTableColumns } from "../amx-bridge/catalog";
import { TYPE_COLORS } from "../constants";
import { ColumnTypeGlyph } from "./ColumnTypeGlyph";
import { LogoBadge } from "../logos/LogoBadge";
import { LogoPicker } from "../logos/LogoPicker";
import type { LogoRow } from "../logos/registry";
import { NodeDeleteToolbar } from "../components/NodeDeleteToolbar";
import type { TableNodeData } from "../types";

const PAGE_SIZE = 8;
const SEARCH_THRESHOLD = 8;

interface RowProps {
  name: string;
  dtype: string;
  isPrimary?: boolean;
  isForeign?: boolean;
  traced?: boolean;
}

function ColumnRow({ name, dtype, isPrimary, isForeign, traced }: RowProps) {
  return (
    <div
      className={clsx(
        "lcv-col-row flex items-center gap-2 px-3 py-1",
        traced && "bg-accent-soft text-accent-ink",
      )}
      style={{ position: "relative" }}
    >
      <Handle
        type="target"
        position={Position.Left}
        id={name}
        className="lcv-handle"
        style={{ left: -5 }}
      />
      <ColumnTypeGlyph dtype={dtype} />
      <span className="min-w-0 flex-1 truncate text-[12px] text-ink">
        {name}
        {isPrimary && (
          <span className="ml-1 text-[9px] uppercase tracking-wide text-amber-300">pk</span>
        )}
        {isForeign && (
          <span className="ml-1 text-[9px] uppercase tracking-wide text-sky-300">fk</span>
        )}
      </span>
      <span className="shrink-0 font-mono text-[11px] text-fg-muted">{dtype || "—"}</span>
      <Handle
        type="source"
        position={Position.Right}
        id={name}
        className="lcv-handle"
        style={{ right: -5 }}
      />
    </div>
  );
}

/** Hidden column handles — keep edges anchored for columns not currently
 *  rendered (collapsed node, off the active page, or filtered out). */
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
      {/* Always-present table-level handles so columnless edges
          (asset → table lineage, FK/native table edges) anchor to the
          node even when per-column handles also exist. */}
      <Handle type="target" position={Position.Left} id="__table__" className="lcv-handle" />
      <Handle type="source" position={Position.Right} id="__table__" className="lcv-handle" />
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

function DataFrameNodeImpl({ id, data, selected }: NodeProps<TableNodeData>) {
  const rf = useReactFlow();
  const columns = useMemo(() => data.columns || [], [data.columns]);
  const schemaPath = [data.database, data.schema].filter(Boolean).join(".");

  const expanded = !!data.expanded;
  const writeExpanded = (next: boolean) => {
    rf.setNodes((nodes) =>
      nodes.map((n) => (n.id === id ? { ...n, data: { ...n.data, expanded: next } } : n)),
    );
  };
  useEffect(() => {
    if (data.forceExpandTick && !data.expanded) writeExpanded(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data.forceExpandTick]);

  // Lazy column load (cache-first, else source) on first expand.
  const [colsTried, setColsTried] = useState(false);
  useEffect(() => {
    if (!expanded || columns.length > 0 || colsTried || !data.profile || !data.table) return;
    setColsTried(true);
    void fetchTableColumns({
      profile: data.profile,
      database: data.database,
      schema: data.schema,
      table: data.table,
    })
      .then((cols) => {
        if (!cols || cols.length === 0) return;
        rf.setNodes((nodes) =>
          nodes.map((n) => (n.id === id ? { ...n, data: { ...n.data, columns: cols } } : n)),
        );
      })
      .catch(() => {
        /* cache miss + source unreachable → stay "(no columns cached)" */
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expanded]);

  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const [logoPickerOpen, setLogoPickerOpen] = useState(false);

  function setLogoKey(next: string) {
    rf.setNodes((nodes) =>
      nodes.map((n) => (n.id === id ? { ...n, data: { ...n.data, logoKey: next } } : n)),
    );
  }

  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return columns;
    return columns.filter((c) => c.name.toLowerCase().includes(needle));
  }, [columns, search]);

  // Keep the page in range when the filter shrinks the list.
  const pageCount = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, pageCount - 1);
  useEffect(() => {
    setPage(0);
  }, [search]);

  const pageItems = filtered.slice(safePage * PAGE_SIZE, safePage * PAGE_SIZE + PAGE_SIZE);
  const offPage = useMemo(() => {
    const onPage = new Set(pageItems.map((c) => c.name));
    return columns.filter((c) => !onPage.has(c.name));
  }, [columns, pageItems]);

  return (
    <div style={{ position: "relative", minWidth: 280, maxWidth: 340 }}>
      {/* Kicker label above the card, Databricks-style. */}
      <div className="mb-1 pl-1 text-[10px] uppercase tracking-wide text-fg-muted">Table</div>

      <div
        className={clsx(
          "rounded-lg border bg-surface-raised text-ink shadow-lg transition-shadow duration-150",
          selected
            ? "border-accent-default shadow-accent-soft/40"
            : data.isAnchor
              ? "border-amber-400/70"
              : "border-surface-border",
        )}
      >
        {/* Header: icon · name · popularity bars. */}
        <div className="flex items-center gap-2 px-3 pt-2.5">
          <button
            type="button"
            aria-label={expanded ? "Collapse columns" : "Expand columns"}
            onClick={(e) => {
              e.stopPropagation();
              writeExpanded(!expanded);
            }}
            className="nodrag flex h-4 w-4 shrink-0 items-center justify-center rounded text-fg-muted hover:text-ink"
          >
            {expanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          </button>
          {data.logoKey ? (
            <LogoBadge
              logoKey={data.logoKey}
              onClick={() => setLogoPickerOpen(true)}
            />
          ) : (
            <button
              type="button"
              aria-label="Set header logo"
              onClick={(e) => {
                e.stopPropagation();
                setLogoPickerOpen(true);
              }}
              className="nodrag flex h-5 w-5 shrink-0 items-center justify-center rounded bg-surface text-fg-muted"
            >
              <Table2 size={13} />
            </button>
          )}
          {data.isAnchor && <Star size={12} className="shrink-0 text-amber-300" fill="currentColor" />}
          <span className="min-w-0 flex-1 truncate text-[13px] font-semibold" title={data.fqn}>
            {data.table}
          </span>
          <BarChart3 size={13} className="shrink-0 text-fg-muted/70" />
        </div>

        {/* Schema breadcrumb. */}
        {schemaPath && (
          <div className="truncate px-3 pt-0.5 text-[11px] text-fg-muted" title={schemaPath}>
            {schemaPath}
          </div>
        )}

        {/* Owner (only when known — populated by lineage fetch). */}
        {data.owner && (
          <div className="flex items-center gap-1.5 px-3 pt-1 text-[11px] text-fg-muted">
            <User size={11} className="shrink-0" />
            <span className="truncate">{data.owner}</span>
          </div>
        )}

        {columns.length === 0 ? (
          <>
            <Handle
              type="target"
              position={Position.Left}
              id="__table__"
              className="lcv-handle"
              style={{ background: TYPE_COLORS.unknown, top: "50%" }}
            />
            <div className="px-3 pb-2.5 pt-2 text-[11px] text-fg-muted">(no columns cached)</div>
            <Handle
              type="source"
              position={Position.Right}
              id="__table__"
              className="lcv-handle"
              style={{ background: TYPE_COLORS.unknown, top: "50%" }}
            />
          </>
        ) : !expanded ? (
          <>
            <HiddenHandles columns={columns} />
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                writeExpanded(true);
              }}
              className="nodrag mt-1.5 block w-full border-t border-surface-border px-3 py-1.5 text-left text-[11px] text-fg-muted hover:bg-surface"
            >
              + show {columns.length} column{columns.length === 1 ? "" : "s"}
            </button>
          </>
        ) : (
          <div className="mt-2 border-t border-surface-border pt-2">
            {/* Search columns. */}
            {columns.length >= SEARCH_THRESHOLD && (
              <div className="nodrag relative px-2 pb-1">
                <Search
                  size={11}
                  className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-fg-muted"
                />
                <input
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search columns…"
                  className="block w-full rounded border border-surface-border bg-surface px-2 py-1 pl-6 text-[11.5px] outline-none placeholder:text-fg-muted focus:border-accent-default"
                />
              </div>
            )}

            {/* Column count. */}
            <div className="px-3 pb-0.5 pt-1 text-[11px] text-fg-muted">
              {filtered.length === columns.length
                ? `${columns.length} columns`
                : `${filtered.length} of ${columns.length} columns`}
            </div>

            {/* Paged rows. */}
            {filtered.length === 0 ? (
              <div className="px-3 py-2 text-[11px] text-fg-muted">No matching columns.</div>
            ) : (
              pageItems.map((col) => (
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

            {/* Off-page / filtered-out handles stay mounted for edge anchoring. */}
            <HiddenHandles columns={offPage} />

            {/* Paging footer. */}
            {filtered.length > PAGE_SIZE && (
              <div className="nodrag flex items-center justify-center gap-4 border-t border-surface-border px-3 py-1.5 text-[11px]">
                <button
                  type="button"
                  disabled={safePage === 0}
                  onClick={(e) => {
                    e.stopPropagation();
                    setPage((p) => Math.max(0, p - 1));
                  }}
                  className={clsx(
                    "inline-flex items-center gap-0.5",
                    safePage === 0
                      ? "cursor-default text-fg-muted/40"
                      : "text-accent-ink hover:underline",
                  )}
                >
                  <ChevronRight size={11} className="rotate-180" /> Previous
                </button>
                <button
                  type="button"
                  disabled={safePage >= pageCount - 1}
                  onClick={(e) => {
                    e.stopPropagation();
                    setPage((p) => Math.min(pageCount - 1, p + 1));
                  }}
                  className={clsx(
                    "inline-flex items-center gap-0.5",
                    safePage >= pageCount - 1
                      ? "cursor-default text-fg-muted/40"
                      : "text-accent-ink hover:underline",
                  )}
                >
                  Next <ChevronRight size={11} />
                </button>
              </div>
            )}
          </div>
        )}
      </div>

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
