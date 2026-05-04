import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { Database, FileText, Layers, Table as TableIcon } from "lucide-react";

import { api } from "../lib/api";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import StatusPill from "../components/StatusPill";

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
  });

  return (
    <>
      <PageHeader
        eyebrow="Schema"
        title={schema}
        description="Tables, views, and materialized views in this schema. Click any asset to inspect its columns."
        actions={
          <Link
            to="/"
            className="text-xs text-ink-dim hover:text-ink"
          >
            ← Back to dashboard
          </Link>
        }
      />

      <Card>
        <CardHeader
          title="Assets"
          description={
            assets.data
              ? `${assets.data.count} asset${assets.data.count === 1 ? "" : "s"} reachable.`
              : "Loading…"
          }
        />
        <CardBody className="p-0">
          {assets.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading assets…</div>
          ) : assets.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(assets.error as Error).message}
            </div>
          ) : assets.data?.assets?.length ? (
            <ul className="divide-y divide-surface-border">
              {assets.data.assets.map((asset) => (
                <li key={asset.name}>
                  <Link
                    to={`/db/${profile}/${schema}/${asset.name}`}
                    className="flex items-center gap-3 px-5 py-3 text-sm transition hover:bg-surface-subtle/50"
                  >
                    <AssetIcon kind={asset.kind} />
                    <span className="font-medium">{asset.name}</span>
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
            <EmptyState
              icon={Database}
              title="Schema is empty"
              description="No tables, views, or materialized views in this schema yet."
            />
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
