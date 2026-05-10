/**
 * PricingBrowser — model-price catalog dialog.
 *
 * Mounted as a sibling of ``<PricingBadge>`` in the topbar; opens
 * when the user clicks the chip body (not the refresh icon, which
 * keeps its own click handler). Renders every model AMX has price
 * data for in a searchable, source-filterable table — the same
 * data the dedicated ``/pricing`` route shows full-page, just
 * scoped to a dialog panel.
 *
 * Data layer: TanStack Query against ``GET /api/pricing/models``.
 * Backend reads only from the in-memory cache, no network hop, so
 * refetches stay snappy. The "Open full pricing page" link
 * navigates to ``/pricing`` and closes the dialog so the user
 * lands on the deep-linkable URL with no modal in the way.
 */

import { useQuery } from "@tanstack/react-query";
import { ArrowUpRight, ExternalLink } from "lucide-react";
import { Link } from "react-router-dom";

import { api, type ModelCatalogEntry } from "../lib/api";
import Dialog from "./ui/Dialog";
import DataTable, {
  type DataTableColumn,
  type DataTableFilter,
} from "./ui/DataTable";

interface Props {
  open: boolean;
  onClose: () => void;
}

const PRICING_CATALOG_KEY = ["pricing", "models"] as const;

function formatRate(value: number): string {
  if (value === 0) return "$0.0000";
  // 4 decimals matches the CLI ``/cost`` output so the two surfaces
  // present rates the user can compare verbatim.
  return `$${value.toFixed(4)}`;
}

function formatFetchedAt(epoch: number | null): string {
  if (epoch == null) return "—";
  const ageSeconds = Math.max(0, Date.now() / 1000 - epoch);
  if (ageSeconds < 60) return `${Math.floor(ageSeconds)}s ago`;
  if (ageSeconds < 3600) return `${Math.floor(ageSeconds / 60)}m ago`;
  if (ageSeconds < 86_400) return `${(ageSeconds / 3600).toFixed(1)}h ago`;
  return `${(ageSeconds / 86_400).toFixed(1)}d ago`;
}

const COLUMNS: DataTableColumn<ModelCatalogEntry>[] = [
  {
    id: "model_id",
    header: "Model",
    cell: (row) => row.model_id,
    sortValue: (row) => row.model_id,
    mono: true,
  },
  {
    id: "provider_hint",
    header: "Provider",
    cell: (row) => row.provider_hint || "—",
    sortValue: (row) => row.provider_hint,
    width: "w-32",
  },
  {
    id: "input_per_mtok",
    header: "Input $/Mtok",
    cell: (row) => formatRate(row.input_per_mtok),
    sortValue: (row) => row.input_per_mtok,
    align: "right",
    width: "w-32",
  },
  {
    id: "output_per_mtok",
    header: "Output $/Mtok",
    cell: (row) => formatRate(row.output_per_mtok),
    sortValue: (row) => row.output_per_mtok,
    align: "right",
    width: "w-32",
  },
  {
    id: "source",
    header: "Source",
    cell: (row) => row.source,
    sortValue: (row) => row.source,
    width: "w-28",
    hideOnMobile: true,
  },
  {
    id: "fetched_at",
    header: "Fetched",
    cell: (row) => formatFetchedAt(row.fetched_at),
    sortValue: (row) => row.fetched_at ?? 0,
    width: "w-24",
    hideOnMobile: true,
  },
];

const FILTERS: DataTableFilter<ModelCatalogEntry>[] = [
  {
    id: "litellm",
    label: "LiteLLM",
    predicate: (row) => row.source === "litellm",
  },
  {
    id: "openrouter",
    label: "OpenRouter",
    predicate: (row) => row.source === "openrouter",
  },
  {
    id: "fallback",
    label: "Bundled",
    predicate: (row) => row.source === "fallback",
  },
];

export default function PricingBrowser({ open, onClose }: Props) {
  const catalog = useQuery({
    queryKey: PRICING_CATALOG_KEY,
    queryFn: () => api.listModelPrices(),
    enabled: open,
    refetchOnWindowFocus: false,
  });

  const rows = catalog.data?.models ?? [];
  const isStale = catalog.data?.is_stale ?? false;
  const fetchedAt = catalog.data?.fetched_at ?? null;

  return (
    <Dialog
      open={open}
      onClose={onClose}
      size="xl"
      title="LLM model prices"
      description={
        fetchedAt != null
          ? `Cache snapshot from ${formatFetchedAt(fetchedAt)} — ${rows.length} models.`
          : `${rows.length} models loaded.`
      }
    >
      <div className="flex flex-col gap-3">
        {isStale && (
          <div className="rounded-md border border-warning/40 bg-warning-soft/40 px-3 py-2 text-[12px] text-warning">
            These prices are more than 24h old. Click the refresh icon in the
            topbar to pull fresh data from LiteLLM and OpenRouter.
          </div>
        )}
        <DataTable
          columns={COLUMNS}
          rows={rows}
          rowKey={(row) => row.model_id}
          searchable
          searchPlaceholder="Search by model id…"
          searchAccessor={(row) => `${row.model_id} ${row.provider_hint} ${row.source}`}
          filters={FILTERS}
          isLoading={catalog.isLoading}
          error={catalog.error ? (catalog.error as Error).message : null}
          emptyState={
            <div className="px-4 py-6 text-center text-[13px] text-ink-dim">
              No price data loaded yet. Click the refresh icon in the topbar
              to populate the catalog from LiteLLM and OpenRouter.
            </div>
          }
          initialSort={{ id: "model_id", direction: "asc" }}
          pageSize={20}
          toolbar={
            <Link
              to="/pricing"
              onClick={onClose}
              className="inline-flex items-center gap-1 rounded-md border border-border px-2.5 py-1 text-[12px] font-medium text-ink-muted hover:bg-surface-border hover:text-ink"
            >
              <ExternalLink size={12} />
              Open full pricing page
              <ArrowUpRight size={11} />
            </Link>
          }
        />
      </div>
    </Dialog>
  );
}
