/**
 * EdgeDetailPanel — the right-hand drawer that opens when an edge's
 * midpoint dot (or notebook/asset marker) is clicked, mirroring
 * Databricks' "Lineage details" panel:
 *
 *   Lineage details
 *   Last activity: —
 *   Source   ▦ table  schema
 *   Target   ▦ table  schema
 *   [All assets ▾]
 *   Name                 Last activity
 *   ▱ silver_direct…     —
 *
 * Source / Target are the edge's endpoints. The asset list shows the
 * assets that mediate the relationship: a collapsed bucket's children
 * when an endpoint is a bucket, otherwise the asset nodes adjacent to
 * both endpoints in the de-bucketed graph.
 */

import { useMemo, useState } from "react";
import { ExternalLink, X } from "lucide-react";

import type { AssetBucketNodeData, CanvasEdge, CanvasNode } from "../types";
import { flattenGraph } from "../amx-bridge/graphFlatten";
import { iconForKind, nodeName, nodeSchema } from "./assetIcons";
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

interface Props {
  edge: CanvasEdge | null;
  nodes: CanvasNode[];
  edges: CanvasEdge[];
  onClose: () => void;
  onOpenNode: (nodeId: string) => void;
}

export function EdgeDetailPanel({ edge, nodes, edges, onClose, onOpenNode }: Props) {
  const [assetType, setAssetType] = useState("all");
  const rawById = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);
  const flat = useMemo(() => flattenGraph(nodes, edges), [nodes, edges]);

  const sourceNode = edge ? rawById.get(edge.source) : undefined;
  const targetNode = edge ? rawById.get(edge.target) : undefined;

  // The assets on this relationship: a bucket endpoint contributes its
  // children; otherwise the asset nodes wired to both endpoints.
  const assets = useMemo<CanvasNode[]>(() => {
    if (!edge) return [];
    const out: CanvasNode[] = [];
    const seen = new Set<string>();
    const push = (n: CanvasNode | undefined) => {
      if (n && !seen.has(n.id)) {
        seen.add(n.id);
        out.push(n);
      }
    };
    for (const ep of [sourceNode, targetNode]) {
      if (ep?.data.kind === "asset-bucket") {
        for (const c of (ep.data as AssetBucketNodeData).childNodes) push(c);
      }
    }
    if (out.length === 0) {
      // Intermediary asset nodes adjacent to both endpoints.
      const adj = (id: string) =>
        new Set(
          flat.edges
            .filter((e) => e.source === id || e.target === id)
            .map((e) => (e.source === id ? e.target : e.source)),
        );
      const srcAdj = adj(edge.source);
      const tgtAdj = adj(edge.target);
      for (const id of srcAdj) {
        if (tgtAdj.has(id)) push(flat.nodeById.get(id));
      }
    }
    return out;
  }, [edge, sourceNode, targetNode, flat]);

  const shownAssets = useMemo(
    () => assets.filter((a) => assetType === "all" || a.data.kind === assetType),
    [assets, assetType],
  );

  if (!edge) return null;

  return (
    <div className="absolute inset-y-0 right-0 z-20 flex w-[440px] max-w-[85%] flex-col border-l border-surface-border bg-surface-raised shadow-2xl">
      <div className="flex items-center justify-between border-b border-surface-border px-5 py-4">
        <span className="text-[15px] font-semibold text-ink">Lineage details</span>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close"
          className="text-fg-muted hover:text-ink"
        >
          <X size={16} />
        </button>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto px-5 py-4">
        <div className="text-[11px] text-fg-muted">Last activity: —</div>

        <Endpoint label="Source" node={sourceNode} onOpen={onOpenNode} />
        <Endpoint label="Target" node={targetNode} onOpen={onOpenNode} />

        <div className="mt-5">
          <PanelDropdown value={assetType} options={ASSET_TYPE_OPTIONS} onChange={setAssetType} />
        </div>

        <div className="mt-4 grid grid-cols-[1fr_auto] items-center gap-3 border-b border-surface-border pb-2 text-[12px] font-semibold text-ink">
          <span>Name</span>
          <span className="w-24 text-right">Last activity</span>
        </div>

        {shownAssets.length === 0 ? (
          <div className="py-4 text-[12px] text-fg-muted">No related assets in this view.</div>
        ) : (
          shownAssets.map((a) => {
            const Icon = iconForKind(a.data.kind);
            return (
              <button
                key={a.id}
                type="button"
                onClick={() => onOpenNode(a.id)}
                className="grid w-full grid-cols-[1fr_auto] items-center gap-3 border-b border-surface-border/60 py-2.5 text-left hover:bg-surface"
              >
                <span className="flex min-w-0 items-center gap-2">
                  <Icon size={15} className="shrink-0 text-fg-muted" />
                  <span className="min-w-0">
                    <span className="block truncate text-[12.5px] text-accent-ink">
                      {nodeName(a)}
                    </span>
                    <span className="block truncate text-[11px] text-fg-muted">
                      {nodeSchema(a)}
                    </span>
                  </span>
                </span>
                <span className="w-24 text-right text-[12px] text-fg-muted">—</span>
              </button>
            );
          })
        )}
      </div>
    </div>
  );
}

function Endpoint({
  label,
  node,
  onOpen,
}: {
  label: string;
  node: CanvasNode | undefined;
  onOpen: (nodeId: string) => void;
}) {
  const Icon = node ? iconForKind(node.data.kind) : null;
  return (
    <div className="mt-4">
      <div className="text-[12px] font-semibold text-ink">{label}</div>
      {node && Icon ? (
        <button
          type="button"
          onClick={() => onOpen(node.id)}
          className="mt-1.5 flex w-full items-center gap-2 text-left"
        >
          <Icon size={15} className="shrink-0 text-fg-muted" />
          <span className="min-w-0">
            <span className="inline-flex items-center gap-1 text-[12.5px] text-accent-ink">
              <span className="truncate">{nodeName(node)}</span>
              <ExternalLink size={11} className="shrink-0" />
            </span>
            <span className="block truncate text-[11px] text-fg-muted">{nodeSchema(node)}</span>
          </span>
        </button>
      ) : (
        <div className="mt-1.5 text-[12px] text-fg-muted">—</div>
      )}
    </div>
  );
}
