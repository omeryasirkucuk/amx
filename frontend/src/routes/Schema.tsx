import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Database, FileText, Layers, Sparkles, Table as TableIcon } from "lucide-react";

import { ApiError, api } from "../lib/api";
import { cn } from "../lib/cn";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import GenerateScopeDialog from "../components/GenerateScopeDialog";
import StatusPill from "../components/StatusPill";
import {
  Button,
  InlineEditText,
  Skeleton,
  useToast,
} from "../components/ui";

const ASSET_TONE: Record<string, "accent" | "positive" | "neutral" | "warning"> = {
  table: "accent",
  view: "positive",
  materialized_view: "warning",
};

export default function Schema() {
  const params = useParams();
  const schema = params.schema!;
  const profile = params.profile || "active";
  const toast = useToast();
  const navigate = useNavigate();
  const [confirmGenerate, setConfirmGenerate] = useState(false);
  const [draftDescription, setDraftDescription] = useState("");

  const qc = useQueryClient();
  const assets = useQuery({
    queryKey: ["live-assets", schema],
    queryFn: () => api.liveAssets(schema),
    retry: false,
  });

  // Per-row "Gen" mutation — generates a description for one table
  // under this schema. Mirrors the per-column Gen button on Table.tsx
  // (no scope dialog, single LLM call, lands on /pending for review).
  const generateTableOne = useMutation({
    mutationFn: (table: string) => api.generateTableDescription(schema, table),
    onSuccess: (result, table) => {
      qc.invalidateQueries({ queryKey: ["live-assets", schema] });
      toast.push({
        title: result.run_id
          ? `Table queued for review (Run #${result.run_id})`
          : "Table description queued for review",
        description: `${schema}.${table} — approve from /pending.`,
        tone: "success",
        duration: 3000,
      });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Generation failed",
        description: e.message,
        tone: "error",
      }),
  });
  const needsCatalog =
    assets.error instanceof ApiError &&
    assets.error.status === 412 &&
    assets.error.hint === "select-catalog";
  const needsDatabase =
    assets.error instanceof ApiError &&
    assets.error.status === 412 &&
    assets.error.hint === "select-database";

  // The live-DB inventory does not currently surface the schema's
  // own COMMENT, so we hold whatever the user types locally and
  // PUT it back; the next browse round-trip picks up the new value.
  async function saveSchemaDescription(next: string) {
    await api.setSchemaComment(schema, next);
    setDraftDescription(next);
    toast.push({
      title: "Schema description saved",
      tone: "success",
      duration: 2000,
    });
  }

  // Two flavours of "Generate":
  // - the single-shot endpoint writes ONLY this schema's COMMENT
  //   (no tables, no columns, one LLM call). Fast.
  // - the bulk run path spawns the full analyze.run worker so every
  //   table + column under the schema also gets generated.
  const generateSchemaOnly = useMutation({
    mutationFn: () => api.generateSchemaDescription(schema),
    onSuccess: (result) => {
      setDraftDescription(result.description);
      toast.push({
        title: result.run_id
          ? `Queued for review (Run #${result.run_id})`
          : "Description queued for review",
        description: "Approve from the Pending page to write it to the live database.",
        tone: "success",
        duration: 3200,
      });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Generation failed",
        description: e.message,
        tone: "error",
      }),
  });

  const generate = useMutation({
    mutationFn: () =>
      api.submitRun({
        scope: { [schema]: [] },
        apply: false,
        missing_only: false,
      }),
    onSuccess: (result) => {
      setConfirmGenerate(false);
      toast.push({
        title: "Bulk run queued for review",
        description: `Streaming activity for every table under ${schema}; results land on the Pending page for approval.`,
        tone: "info",
        duration: 2600,
      });
      navigate(`/runs/new-${result.job_id}`);
    },
    onError: (e: Error) => {
      setConfirmGenerate(false);
      toast.push({
        title: "Could not start generation",
        description: e.message,
        tone: "error",
      });
    },
  });

  return (
    <>
      <PageHeader
        title={schema}
        breadcrumbs={[{ label: "Browse", to: "/" }, { label: schema }]}
        description={
          <InlineEditText
            value={draftDescription}
            onSave={saveSchemaDescription}
            multiline
            italicEmpty
            emptyLabel="No schema description yet — click to add one or use Generate."
          />
        }
        actions={
          <Button
            variant="primary"
            size="md"
            leadingIcon={<Sparkles size={14} />}
            loading={generateSchemaOnly.isPending || generate.isPending}
            onClick={() => setConfirmGenerate(true)}
          >
            Generate description
          </Button>
        }
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
              {assets.data.assets.map((asset) => {
                const isGenerating =
                  generateTableOne.isPending &&
                  generateTableOne.variables === asset.name;
                const comment = asset.comment?.trim() ?? "";
                return (
                  <li
                    key={asset.name}
                    className="group flex items-start text-sm transition-colors duration-fast hover:bg-surface-subtle/50"
                  >
                    <Link
                      to={`/db/${profile}/${schema}/${asset.name}`}
                      className="flex flex-1 items-start gap-3 px-5 py-2.5"
                    >
                      <span className="mt-0.5">
                        <AssetIcon kind={asset.kind} />
                      </span>
                      <span className="min-w-0 flex-1">
                        <span className="block font-medium text-ink">{asset.name}</span>
                        <span
                          className={cn(
                            "mt-0.5 line-clamp-2 block text-xs",
                            comment ? "text-ink-muted" : "italic text-ink-dim/70",
                          )}
                          title={comment || undefined}
                        >
                          {comment || "no description yet"}
                        </span>
                      </span>
                      <span className="mt-0.5">
                        <StatusPill tone={ASSET_TONE[asset.kind] ?? "neutral"}>
                          {asset.kind.replace("_", " ")}
                        </StatusPill>
                      </span>
                    </Link>
                    <div className="px-3 py-2">
                      <Button
                        variant="ghost"
                        size="sm"
                        leadingIcon={<Sparkles size={11} />}
                        loading={isGenerating}
                        disabled={generateTableOne.isPending}
                        onClick={() => generateTableOne.mutate(asset.name)}
                        title={`Generate description for ${asset.name}`}
                      >
                        Gen
                      </Button>
                    </div>
                  </li>
                );
              })}
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

      <GenerateScopeDialog
        open={confirmGenerate}
        onClose={() => setConfirmGenerate(false)}
        title={`Generate description for ${schema}`}
        description="Pick the scope. Single-shot writes only the schema's own COMMENT (one fast LLM call). Bulk run spawns the full analyze worker for every table and column under the schema."
        singleOption={{
          label: "Just this schema",
          description:
            "Writes only the schema's own COMMENT. One LLM call, no tables touched.",
          loading: generateSchemaOnly.isPending,
          onClick: () => {
            generateSchemaOnly.mutate(undefined, {
              onSettled: () => setConfirmGenerate(false),
            });
          },
        }}
        bulkOption={{
          label: "All tables (bulk run)",
          description:
            "Full analyze.run worker — every reachable table and column gets a generated description; recorded in history, redirected to run-detail.",
          loading: generate.isPending,
          onClick: () => generate.mutate(),
        }}
      />
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
