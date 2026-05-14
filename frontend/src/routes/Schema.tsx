import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { Database, FileText, Layers, Sparkles, Table as TableIcon } from "lucide-react";

import { api } from "../lib/api";
import { cn } from "../lib/cn";
import { useScope, scopePath } from "../lib/scope";
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
  const { scope } = useScope();
  const schema = scope?.schema ?? "";
  const toast = useToast();
  const navigate = useNavigate();
  const [confirmGenerate, setConfirmGenerate] = useState(false);
  const [draftDescription, setDraftDescription] = useState("");
  // PR E: per-run doc profile override forwarded to api.submitRun
  // when the user submits the bulk Generate action below.
  const [runDocProfiles, setRunDocProfiles] = useState<string[] | null>(null);
  // PR δ: parallel code-profile override forwarded to api.submitRun.
  const [runCodeProfiles, setRunCodeProfiles] = useState<string[] | null>(null);

  const qc = useQueryClient();
  const assets = useQuery({
    queryKey: [
      "live-assets",
      scope?.profile ?? "",
      scope?.database ?? "",
      scope?.catalog ?? "",
      schema,
    ],
    queryFn: () => api.liveAssets(scope!, schema),
    enabled: !!scope && !!schema,
    retry: false,
  });

  useEffect(() => {
    setDraftDescription("");
  }, [scope?.profile, scope?.database, scope?.catalog, schema]);

  // Per-table "Gen" button. The previous wiring called the inline,
  // synchronous ``/api/generate/table`` endpoint, which blocked for
  // the whole LLM round-trip and produced no run row in the registry
  // -- so a refresh mid-flight cancelled the request silently and
  // /runs showed nothing. Submit a regular analyze run scoped to
  // exactly one table instead: the worker spawns in the background,
  // the user lands on the live-progress page (cancellable, visible
  // in Runs), and the result is reviewable on the run detail page.
  const generateTableOne = useMutation({
    mutationFn: (table: string) =>
      api.submitRun({
        scope: { [schema]: [table] },
        apply: false,
        missing_only: false,
        db_profile: scope?.profile,
        database: scope?.database,
        catalog: scope?.catalog,
      }),
    onSuccess: (result, table) => {
      toast.push({
        title: `Run started for ${schema}.${table}`,
        description: "Streaming activity on the run detail page; results land in the pending queue.",
        tone: "success",
        duration: 2400,
      });
      qc.invalidateQueries({ queryKey: ["recent-runs"] });
      navigate(`/runs/new-${result.job_id}`);
    },
    onError: (e: Error) =>
      toast.push({
        title: "Could not start run",
        description: e.message,
        tone: "error",
      }),
  });

  async function saveSchemaDescription(next: string) {
    if (!scope) return;
    await api.setSchemaComment(scope, schema, next);
    setDraftDescription(next);
    toast.push({
      title: "Schema description saved",
      tone: "success",
      duration: 2000,
    });
  }

  const generateSchemaOnly = useMutation({
    mutationFn: () => api.generateSchemaDescription(scope!, schema),
    onSuccess: (result) => {
      setDraftDescription(result.description);
      const altCount = result.alternatives_count ?? 1;
      toast.push({
        title: result.run_id
          ? `Queued for review (Run #${result.run_id})`
          : "Description queued for review",
        description:
          altCount > 1
            ? `${altCount} alternatives generated (${result.verbosity}). Pick one and approve from the Pending page.`
            : "Approve from the Pending page to write it to the live database.",
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
        db_profile: scope?.profile,
        database: scope?.database,
        catalog: scope?.catalog,
        doc_profiles: runDocProfiles ?? undefined,
        code_profiles: runCodeProfiles ?? undefined,
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

  if (!scope || !schema) {
    return (
      <EmptyState
        icon={Database}
        title="Pick a schema from the sidebar"
        description="Expand a database/catalog to see its schemas."
      />
    );
  }

  return (
    <>
      <PageHeader
        title={schema}
        breadcrumbs={[
          { label: "Browse", to: "/" },
          {
            label: scope.profile,
            to: `/${scope.kind === "catalog" ? "cat" : "db"}/${encodeURIComponent(scope.profile)}`,
          },
          {
            label: scope.database ?? scope.catalog ?? "",
            to: scopePath(scope),
          },
          { label: schema },
        ]}
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
                      to={scopePath(scope, schema, asset.name)}
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
        docProfiles={runDocProfiles}
        onDocProfilesChange={setRunDocProfiles}
        codeProfiles={runCodeProfiles}
        onCodeProfilesChange={setRunCodeProfiles}
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
