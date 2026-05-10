/**
 * Pricing — full-page LLM model price catalog.
 *
 * Same data as the topbar's <PricingBrowser> dialog (same TanStack
 * Query key so navigating dialog -> page does not refetch), just
 * unwrapped to fill the route canvas. Useful when the user wants to
 * deep-link, share a URL, or scan more rows than the dialog can show.
 *
 * Refresh stays owned by the topbar chip — keeps a single canonical
 * "↻ refresh" affordance instead of two competing buttons.
 */

import { useQuery } from "@tanstack/react-query";

import { Card, CardBody, CardHeader } from "../components/Card";
import PageHeader from "../components/PageHeader";
import StatusPill from "../components/StatusPill";
import { api, type ModelCatalogEntry } from "../lib/api";
import DataTable, {
  type DataTableColumn,
  type DataTableFilter,
} from "../components/ui/DataTable";

const PRICING_CATALOG_KEY = ["pricing", "models"] as const;

function formatRate(value: number): string {
  if (value === 0) return "$0.0000";
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
  },
  {
    id: "fetched_at",
    header: "Fetched",
    cell: (row) => formatFetchedAt(row.fetched_at),
    sortValue: (row) => row.fetched_at ?? 0,
    width: "w-24",
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

export default function Pricing() {
  const catalog = useQuery({
    queryKey: PRICING_CATALOG_KEY,
    queryFn: () => api.listModelPrices(),
    refetchOnWindowFocus: false,
  });

  const rows = catalog.data?.models ?? [];
  const isStale = catalog.data?.is_stale ?? false;
  const fetchedAt = catalog.data?.fetched_at ?? null;

  return (
    <div className="flex flex-col gap-4 px-6 py-6">
      <PageHeader
        title="LLM model prices"
        description="Every model AMX has price data for, refreshed from LiteLLM and OpenRouter."
      />

      <Card>
        <CardHeader
          title={`${rows.length} models loaded`}
          description={`Cache snapshot: ${formatFetchedAt(fetchedAt)}`}
          actions={isStale ? <StatusPill tone="warning">Stale</StatusPill> : undefined}
        />
        <CardBody>
          {isStale && (
            <div className="mb-3 rounded-md border border-warning/40 bg-warning-soft/40 px-3 py-2 text-[12px] text-warning">
              These prices are more than 24h old. Click the refresh icon
              in the topbar to pull fresh data.
            </div>
          )}
          <DataTable
            columns={COLUMNS}
            rows={rows}
            rowKey={(row) => row.model_id}
            searchable
            searchPlaceholder="Search by model id…"
            searchAccessor={(row) =>
              `${row.model_id} ${row.provider_hint} ${row.source}`
            }
            filters={FILTERS}
            isLoading={catalog.isLoading}
            error={
              catalog.error ? (catalog.error as Error).message : null
            }
            emptyState={
              <div className="px-4 py-6 text-center text-[13px] text-ink-dim">
                No price data loaded yet. Click the refresh icon in the
                topbar to populate the catalog from LiteLLM and OpenRouter.
              </div>
            }
            initialSort={{ id: "model_id", direction: "asc" }}
            pageSize={50}
          />
        </CardBody>
      </Card>
    </div>
  );
}
