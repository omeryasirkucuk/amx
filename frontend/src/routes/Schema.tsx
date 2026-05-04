import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { Database, FileText, Layers, Table as TableIcon } from "lucide-react";

import { ApiError, api } from "../lib/api";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import StatusPill from "../components/StatusPill";
import { Skeleton } from "../components/ui";

const ASSET_TONE: Record<string, "accent" | "positive" | "neutral" | "warning"> = {
  table: "accent",
  view: "positive",
  materialized_view: "warning",
};

export default function Schema() {
  const params = useParams();
  const schema = params.schema!;
  const profile = params.profile || "active";

  const assets = useQuery({
    queryKey: ["live-assets", schema],
    queryFn: () => api.liveAssets(schema),
    retry: false,
  });
  const needsCatalog =
    assets.error instanceof ApiError &&
    assets.error.status === 412 &&
    assets.error.hint === "select-catalog";
  const needsDatabase =
    assets.error instanceof ApiError &&
    assets.error.status === 412 &&
    assets.error.hint === "select-database";

  return (
    <>
      <PageHeader
        title={schema}
        breadcrumbs={[{ label: "Browse", to: "/" }, { label: schema }]}
      />

      <Card>
        <CardHeader
          title="Assets"
          description={
            assets.data
              ? `${assets.data.count} asset${assets.data.count === 1 ? "" : "s"} reachable.`
              : undefined
          }
        />
        <CardBody className="p-0">
          {assets.isLoading ? (
            <ul className="divide-y divide-border">
              {Array.from({ length: 5 }).map((_, i) => (
                <li key={i} className="flex items-center gap-3 px-5 py-3">
                  <Skeleton shape="circle" className="h-3.5 w-3.5" />
                  <Skeleton className="h-3 w-1/3" />
                  <Skeleton className="ml-auto h-4 w-16" />
                </li>
              ))}
            </ul>
          ) : needsCatalog ? (
            <CatalogPickerInline />
          ) : needsDatabase ? (
            <DatabasePickerInline />
          ) : assets.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(assets.error as Error).message}
            </div>
          ) : assets.data?.assets?.length ? (
            <ul className="divide-y divide-border">
              {assets.data.assets.map((asset) => (
                <li key={asset.name}>
                  <Link
                    to={`/db/${profile}/${schema}/${asset.name}`}
                    className="flex items-center gap-3 px-5 py-2.5 text-sm transition-colors duration-fast hover:bg-surface-subtle/50"
                  >
                    <AssetIcon kind={asset.kind} />
                    <span className="font-medium text-ink">{asset.name}</span>
                    <span className="ml-auto">
                      <StatusPill tone={ASSET_TONE[asset.kind] ?? "neutral"}>
                        {asset.kind.replace("_", " ")}
                      </StatusPill>
                    </span>
                  </Link>
                </li>
              ))}
            </ul>
          ) : (
            <div className="px-5 py-5">
              <EmptyState
                icon={Database}
                title="Schema is empty"
                description="No tables, views, or materialized views in this schema yet."
                compact
              />
            </div>
          )}
        </CardBody>
      </Card>
    </>
  );
}

function AssetIcon({ kind }: { kind: string }) {
  if (kind === "view") return <FileText size={14} className="text-positive" />;
  if (kind === "materialized_view") return <Layers size={14} className="text-warning" />;
  return <TableIcon size={14} className="text-accent" />;
}

function CatalogPickerInline() {
  const queryClient = useQueryClient();
  const catalogs = useQuery({
    queryKey: ["live-catalogs"],
    queryFn: () => api.liveCatalogs(),
    retry: false,
  });
  const activate = useMutation({
    mutationFn: (name: string) => api.activateCatalog(name, true),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["live-catalogs"] });
      queryClient.invalidateQueries({ queryKey: ["live-assets"] });
      queryClient.invalidateQueries({ queryKey: ["live-schemas"] });
      queryClient.invalidateQueries({ queryKey: ["context"] });
    },
  });
  const list = catalogs.data?.catalogs ?? [];

  return (
    <div className="px-5 py-6">
      <div className="flex flex-col gap-3">
        <div>
          <p className="text-sm font-medium text-ink">No catalog selected.</p>
          <p className="mt-1 text-xs text-ink-muted">
            This backend exposes multiple catalogs. Pick one below — your choice
            is persisted to the active DB profile.
          </p>
        </div>
        {catalogs.isLoading ? (
          <div className="text-xs text-ink-dim">Loading catalogs…</div>
        ) : list.length === 0 ? (
          <div className="text-xs text-critical">
            No catalogs visible. Check your DB profile credentials.
          </div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {list.map((name) => (
              <button
                key={name}
                type="button"
                onClick={() => activate.mutate(name)}
                disabled={activate.isPending}
                className="rounded-md border border-border bg-surface px-3 py-1.5 font-mono text-xs text-ink-muted transition-colors duration-fast hover:border-accent/40 hover:text-ink disabled:opacity-50"
              >
                {name}
              </button>
            ))}
          </div>
        )}
        {activate.isError && (
          <p className="text-xs text-critical">
            {activate.error instanceof Error
              ? activate.error.message
              : "Activation failed."}
          </p>
        )}
      </div>
    </div>
  );
}

function DatabasePickerInline() {
  const queryClient = useQueryClient();
  const databases = useQuery({
    queryKey: ["live-databases"],
    queryFn: () => api.liveDatabases(),
    retry: false,
  });
  const activate = useMutation({
    mutationFn: (name: string) => api.activateDatabase(name, true),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["live-databases"] });
      queryClient.invalidateQueries({ queryKey: ["live-assets"] });
      queryClient.invalidateQueries({ queryKey: ["live-schemas"] });
      queryClient.invalidateQueries({ queryKey: ["context"] });
    },
  });
  const list = databases.data?.databases ?? [];

  return (
    <div className="px-5 py-6">
      <div className="flex flex-col gap-3">
        <div>
          <p className="text-sm font-medium text-ink">No database selected.</p>
          <p className="mt-1 text-xs text-ink-muted">
            The active profile didn&apos;t pin a database. Pick one below — your
            choice is persisted so you don&apos;t have to repeat it.
          </p>
        </div>
        {databases.isLoading ? (
          <div className="text-xs text-ink-dim">Loading databases…</div>
        ) : list.length === 0 ? (
          <div className="text-xs text-critical">
            No databases visible. Check your DB profile credentials.
          </div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {list.map((name) => (
              <button
                key={name}
                type="button"
                onClick={() => activate.mutate(name)}
                disabled={activate.isPending}
                className="rounded-md border border-border bg-surface px-3 py-1.5 font-mono text-xs text-ink-muted transition-colors duration-fast hover:border-accent/40 hover:text-ink disabled:opacity-50"
              >
                {name}
              </button>
            ))}
          </div>
        )}
        {activate.isError && (
          <p className="text-xs text-critical">
            {activate.error instanceof Error
              ? activate.error.message
              : "Activation failed."}
          </p>
        )}
      </div>
    </div>
  );
}
