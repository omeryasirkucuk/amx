/**
 * LineageDetailPanel — the right-hand inspector that opens when a table /
 * asset node is clicked, mirroring Databricks Unity Catalog's lineage
 * detail drawer:
 *
 *   ┌ dap_eu_60_prod.schema.table ↗
 *   │ [Delta] [owner]
 *   │ ── Columns | Lineage ──
 *   │ [Tables ▾] [Up and Downstream ▾]
 *   │ Name           Direction      Last activity
 *   │ ▦ neighbour    ↓ Downstream   —
 *   │ …
 *
 * The "Columns" tab lists the focus node's columns; the "Lineage" tab
 * lists its neighbours (computed from the de-bucketed canvas graph) with
 * an asset-type filter and a direction filter. Last-activity timestamps
 * are shown when known.
 */

import { useMemo, useState } from "react";
import { ArrowDown, ArrowUp, Database, ExternalLink, X } from "lucide-react";
import clsx from "clsx";

import type { CanvasEdge, CanvasNode, TableNodeData } from "../types";
import { flattenGraph, neighboursOf } from "../amx-bridge/graphFlatten";
import { ColumnTypeGlyph } from "../nodes/ColumnTypeGlyph";
import { iconForKind, nodeName, nodeSchema, nodeTitle } from "./assetIcons";
import { PanelDropdown, type DropdownOption } from "./PanelDropdown";

const ASSET_TYPE_OPTIONS: DropdownOption[] = [
  { value: "all", label: "All assets" },
  { value: "table", label: "Tables" },
  { value: "notebook", label: "Notebooks" },
  { value: "job", label: "Jobs" },
  { value: "pipeline", label: "Pipelines" },
  { value: "query", label: "Queries" },
  { value: "dashboard", label: "Dashboards" },
];

const DIRECTION_OPTIONS: DropdownOption[] = [
  { value: "updown", label: "Up and Downstream" },
  { value: "upstream", label: "Upstream" },
  { value: "downstream", label: "Downstream" },
];

interface Props {
  node: CanvasNode | null;
  nodes: CanvasNode[];
  edges: CanvasEdge[];
  onClose: () => void;
  onOpenNeighbour: (nodeId: string) => void;
}

export function LineageDetailPanel({ node, nodes, edges, onClose, onOpenNeighbour }: Props) {
  const [tab, setTab] = useState<"columns" | "lineage">("lineage");
  const [assetType, setAssetType] = useState("all");
  const [direction, setDirection] = useState("updown");

  const flat = useMemo(() => flattenGraph(nodes, edges), [nodes, edges]);
  const neighbours = useMemo(
    () => (node ? neighboursOf(node.id, flat) : []),
    [node, flat],
  );

  const shown = useMemo(() => {
    return neighbours.filter((nb) => {
      if (assetType !== "all" && nb.node.data.kind !== assetType) return false;
      if (direction === "upstream" && nb.direction !== "upstream") return false;
      if (direction === "downstream" && nb.direction !== "downstream") return false;
      return true;
    });
  }, [neighbours, assetType, direction]);

  if (!node) return null;

  const isTable = node.data.kind === "table";
  const td = isTable ? (node.data as TableNodeData) : null;
  const columns = td?.columns ?? [];

  return (
    <div className="absolute inset-y-0 right-0 z-20 flex w-[440px] max-w-[85%] flex-col border-l border-surface-border bg-surface-raised shadow-2xl">
      {/* Title. */}
      <div className="flex items-start gap-2 border-b border-surface-border px-5 py-4">
        <button
          type="button"
          onClick={() => onOpenNeighbour(node.id)}
          className="min-w-0 flex-1 text-left"
          title={nodeTitle(node)}
        >
          <span className="inline-flex items-start gap-1.5 text-[15px] font-semibold leading-snug text-accent-ink">
            <span className="break-words">{nodeTitle(node)}</span>
            <ExternalLink size={13} className="mt-1 shrink-0" />
          </span>
        </button>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close"
          className="shrink-0 text-fg-muted hover:text-ink"
        >
          <X size={16} />
        </button>
      </div>

      {/* Badges. */}
      {(td?.format || td?.owner) && (
        <div className="flex flex-wrap items-center gap-2 px-5 pt-3">
          {td?.format && (
            <span className="rounded-md bg-surface px-2 py-1 text-[12px] text-ink">{td.format}</span>
          )}
          {td?.owner && (
            <span className="rounded-md bg-surface px-2 py-1 text-[12px] text-ink">{td.owner}</span>
          )}
        </div>
      )}

      {/* Tabs. */}
      <div className="flex items-center gap-5 border-b border-surface-border px-5 pt-3">
        {isTable && (
          <TabButton active={tab === "columns"} onClick={() => setTab("columns")}>
            Columns
          </TabButton>
        )}
        <TabButton active={tab === "lineage"} onClick={() => setTab("lineage")}>
          Lineage
        </TabButton>
      </div>

      {/* Body. */}
      <div className="min-h-0 flex-1 overflow-y-auto">
        {tab === "columns" && isTable ? (
          <div className="py-2">
            {columns.length === 0 ? (
              <div className="px-5 py-4 text-[12px] text-fg-muted">No columns cached.</div>
            ) : (
              columns.map((c) => (
                <div
                  key={c.name}
                  className="flex items-center gap-2 px-5 py-1.5 text-[12.5px] hover:bg-surface"
                >
                  <ColumnTypeGlyph dtype={c.dtype} />
                  <span className="min-w-0 flex-1 truncate text-ink">{c.name}</span>
                  <span className="shrink-0 font-mono text-[11px] text-fg-muted">
                    {c.dtype || "—"}
                  </span>
                </div>
              ))
            )}
          </div>
        ) : (
          <div className="px-5 py-3">
            <div className="flex items-center gap-2">
              <PanelDropdown
                value={assetType}
                options={ASSET_TYPE_OPTIONS}
                onChange={setAssetType}
              />
              <PanelDropdown
                value={direction}
                options={DIRECTION_OPTIONS}
                onChange={setDirection}
              />
            </div>

            {/* Column headers. */}
            <div className="mt-4 grid grid-cols-[1fr_auto_auto] items-center gap-3 border-b border-surface-border pb-2 text-[12px] font-semibold text-ink">
              <span>Name</span>
              <span className="w-28">Direction</span>
              <span className="w-24 text-right">Last activity</span>
            </div>

            {shown.length === 0 ? (
              <div className="py-4 text-[12px] text-fg-muted">No lineage in this view.</div>
            ) : (
              shown.map((nb) => {
                const Icon = iconForKind(nb.node.data.kind);
                return (
                  <button
                    key={nb.node.id}
                    type="button"
                    onClick={() => onOpenNeighbour(nb.node.id)}
                    className="grid w-full grid-cols-[1fr_auto_auto] items-center gap-3 border-b border-surface-border/60 py-2.5 text-left hover:bg-surface"
                  >
                    <span className="flex min-w-0 items-center gap-2">
                      <Icon size={15} className="shrink-0 text-fg-muted" />
                      <span className="min-w-0">
                        <span className="block truncate text-[12.5px] text-accent-ink">
                          {nodeName(nb.node)}
                        </span>
                        <span className="block truncate text-[11px] text-fg-muted">
                          {nodeSchema(nb.node)}
                        </span>
                      </span>
                    </span>
                    <span
                      className={clsx(
                        "inline-flex w-28 items-center gap-1 text-[12px]",
                        nb.direction === "upstream" ? "text-emerald-300" : "text-sky-300",
                      )}
                    >
                      {nb.direction === "upstream" ? (
                        <ArrowUp size={13} />
                      ) : (
                        <ArrowDown size={13} />
                      )}
                      {nb.direction === "upstream" ? "Upstream" : "Downstream"}
                    </span>
                    <span className="w-24 text-right text-[12px] text-fg-muted">—</span>
                  </button>
                );
              })
            )}
          </div>
        )}
      </div>

      <div className="flex items-center gap-2 border-t border-surface-border px-5 py-2 text-[11px] text-fg-muted">
        <Database size={12} /> {shown.length} related asset{shown.length === 1 ? "" : "s"}
      </div>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={clsx(
        "-mb-px border-b-2 pb-2 text-[13px]",
        active
          ? "border-accent-default font-semibold text-ink"
          : "border-transparent text-fg-muted hover:text-ink",
      )}
    >
      {children}
    </button>
  );
}
