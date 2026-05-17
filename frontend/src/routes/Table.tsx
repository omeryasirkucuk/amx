import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { AlignLeft, Columns, Sparkles, Database, Workflow } from "lucide-react";

import { api, lineageRefresh } from "../lib/api";
import { useScope, scopePath } from "../lib/scope";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import GenerateScopeDialog from "../components/GenerateScopeDialog";
import StatusPill from "../components/StatusPill";
import { useUi } from "../lib/store";
import {
  Button,
  InlineEditText,
  Skeleton,
  useToast,
} from "../components/ui";

export default function Table() {
  const { scope } = useScope();
  const schema = scope?.schema ?? "";
  const table = scope?.table ?? "";
  const remember = useUi((s) => s.rememberOpenedTable);
  const rememberScope = useUi((s) => s.rememberOpenedScope);
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const [confirmGenerate, setConfirmGenerate] = useState(false);
  // PR E: per-run doc profile override for the bulk path.
  const [runDocProfiles, setRunDocProfiles] = useState<string[] | null>(null);
  // PR δ: parallel code-profile override forwarded to api.submitRun.
  const [runCodeProfiles, setRunCodeProfiles] = useState<string[] | null>(null);

  useEffect(() => {
    if (scope && schema && table) {
      remember(schema, table);
      rememberScope({
        profile: scope.profile,
        database: scope.database,
        catalog: scope.catalog,
        schema,
        table,
      });
    }
  }, [scope, schema, table, remember, rememberScope]);

  const snapshot = useQuery({
    queryKey: [
      "live-snapshot",
      scope?.profile ?? "",
      scope?.database ?? "",
      scope?.catalog ?? "",
      schema,
      table,
    ],
    queryFn: () => api.liveSnapshot(scope!, schema, table),
    enabled: !!scope && !!schema && !!table,
  });
  const columns = useQuery({
    queryKey: [
      "live-columns",
      scope?.profile ?? "",
      scope?.database ?? "",
      scope?.catalog ?? "",
      schema,
      table,
    ],
    queryFn: () => api.liveColumns(scope!, schema, table),
    enabled: !!scope && !!schema && !!table,
  });

  const totalCols = snapshot.data?.columns?.length ?? columns.data?.count ?? 0;
  const commented =
    snapshot.data?.columns?.filter((c) => (c.comment || "").trim().length > 0).length ?? 0;
  const tableComment = snapshot.data?.table_comment ?? "";

  async function saveTableComment(next: string) {
    if (!scope) return;
    await api.setTableComment(scope, schema, table, next);
    qc.invalidateQueries({ queryKey: ["live-snapshot"] });
    toast.push({
      title: "Description saved",
      tone: "success",
      duration: 2000,
    });
  }

  async function saveColumnComment(column: string, next: string) {
    if (!scope) return;
    await api.setColumnComment(scope, schema, table, column, next);
    qc.invalidateQueries({ queryKey: ["live-snapshot"] });
    toast.push({
      title: `Comment saved`,
      description: `${schema}.${table}.${column}`,
      tone: "success",
      duration: 2000,
    });
  }

  const generateTableOnly = useMutation({
    mutationFn: () => api.generateTableDescription(scope!, schema, table),
    onSuccess: (result) => {
      qc.invalidateQueries({ queryKey: ["live-snapshot"] });
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

  const generateColumnOne = useMutation({
    mutationFn: (column: string) =>
      api.generateColumnDescription(scope!, schema, table, column),
    onSuccess: (result, column) => {
      qc.invalidateQueries({ queryKey: ["live-snapshot"] });
      const altCount = result.alternatives_count ?? 1;
      toast.push({
        title: result.run_id
          ? `Column queued for review (Run #${result.run_id})`
          : "Column description queued for review",
        description:
          altCount > 1
            ? `${schema}.${table}.${column} — ${altCount} alternatives (${result.verbosity}); pick one in /pending.`
            : `${schema}.${table}.${column} — approve from /pending.`,
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

  const generate = useMutation({
    mutationFn: () =>
      api.submitRun({
        scope: { [schema]: [table] },
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
        description: `Streaming every column of ${schema}.${table}; results land on the Pending page.`,
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

  // "Open lineage" button on the table page: ensures an artifact
  // exists for the current anchor (creating it if needed), then routes
  // the user into the canvas. Refresh is cache-only by default so the
  // round-trip never blocks on a live DB call.
  const openLineage = useMutation({
    mutationFn: async () => {
      if (!scope || !schema || !table) return null;
      const anchorPath = [schema, table].filter(Boolean).join(".");
      const result = await lineageRefresh(anchorPath, {
        profile: scope.profile,
        // Always pass the active database / catalog so the backend
        // can look the anchor up correctly — without it the lookup
        // falls back to profile.database which is empty for most
        // multi-database backends and 404s.
        database: scope.database || scope.catalog || undefined,
      });
      const slug = `${schema}-${table}`.replace(/[^A-Za-z0-9_-]+/g, "_");
      return { slug, anchorPath, profile: scope.profile, result };
    },
    onSuccess: (payload) => {
      if (!payload) return;
      navigate(
        `/lineage/${encodeURIComponent(payload.profile)}/${encodeURIComponent(payload.slug)}`,
      );
    },
    onError: (e: Error) => {
      toast.push({
        title: "Could not open lineage",
        description: e.message,
        tone: "error",
      });
    },
  });

  if (!scope || !schema || !table) {
    return (
      <EmptyState
        icon={Database}
        title="Pick a table from the sidebar"
        description="Expand a schema to see its tables."
      />
    );
  }

  return (
    <>
      <PageHeader
        title={table}
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
          { label: schema, to: scopePath(scope, schema) },
          { label: table },
        ]}
        description={
          <InlineEditText
            value={tableComment}
            onSave={saveTableComment}
            multiline
            italicEmpty
            emptyLabel="No table description yet — click to add one or use Generate."
          />
        }
        actions={
          <div className="flex items-center gap-2">
            <Button
              variant="secondary"
              size="md"
              leadingIcon={<Workflow size={14} />}
              loading={openLineage.isPending}
              onClick={() => openLineage.mutate()}
              title="Render this table's lineage and open it in the canvas"
            >
              Open lineage
            </Button>
            <Button
              variant="primary"
              size="md"
              leadingIcon={<Sparkles size={14} />}
              loading={generateTableOnly.isPending || generate.isPending}
              onClick={() => setConfirmGenerate(true)}
            >
              Generate description
            </Button>
          </div>
        }
      />

      <div className="grid gap-4 md:grid-cols-3">
        <SummaryCard label="Columns" value={String(totalCols)} icon={Columns} />
        <SummaryCard
          label="With comments"
          value={totalCols ? `${commented}/${totalCols}` : "—"}
          icon={AlignLeft}
        />
        <SummaryCard
          label="Coverage"
          value={
            totalCols ? `${Math.round((commented / totalCols) * 100)}%` : "—"
          }
          icon={AlignLeft}
        />
      </div>

      <Card className="mt-6">
        <CardHeader title="Columns" />
        <CardBody className="p-0 overflow-x-auto">
          {columns.isLoading ? (
            <div className="px-5 py-4 space-y-2">
              {Array.from({ length: 6 }).map((_, i) => (
                <div key={i} className="flex items-center gap-3">
                  <Skeleton className="h-3 w-1/6" />
                  <Skeleton className="h-3 w-1/12" />
                  <Skeleton className="h-3 w-1/12" />
                  <Skeleton className="h-3 flex-1" />
                </div>
              ))}
            </div>
          ) : columns.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(columns.error as Error).message}
            </div>
          ) : columns.data && columns.data.columns.length > 0 ? (
            <table className="w-full text-sm">
              <thead className="border-b border-border bg-surface-subtle/60 text-[10.5px] uppercase tracking-wider text-ink-dim">
                <tr>
                  <th className="px-5 py-2 text-left font-semibold">Name</th>
                  <th className="px-5 py-2 text-left font-semibold">Type</th>
                  <th className="px-5 py-2 text-left font-semibold">Nullable</th>
                  <th className="px-5 py-2 text-left font-semibold">Comment</th>
                  <th className="px-2 py-2 text-right font-semibold w-20">Generate</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {columns.data.columns.map((col) => {
                  const snap = snapshot.data?.columns.find((c) => c.name === col.name);
                  const isGenerating =
                    generateColumnOne.isPending && generateColumnOne.variables === col.name;
                  return (
                    <tr key={col.name} className="align-top hover:bg-surface-subtle/40">
                      <td className="px-5 py-2 font-mono text-xs text-ink">{col.name}</td>
                      <td className="px-5 py-2 font-mono text-xs text-ink-muted">
                        {col.dtype}
                      </td>
                      <td className="px-5 py-2">
                        <StatusPill tone={col.nullable ? "neutral" : "accent"}>
                          {col.nullable ? "nullable" : "required"}
                        </StatusPill>
                      </td>
                      <td className="px-5 py-2 text-ink-muted">
                        <InlineEditText
                          value={snap?.comment ?? ""}
                          onSave={(next) => saveColumnComment(col.name, next)}
                          multiline
                          italicEmpty
                          emptyLabel="no comment — click to add"
                        />
                      </td>
                      <td className="px-2 py-2 text-right">
                        <Button
                          variant="ghost"
                          size="sm"
                          leadingIcon={<Sparkles size={11} />}
                          loading={isGenerating}
                          disabled={generateColumnOne.isPending}
                          onClick={() => generateColumnOne.mutate(col.name)}
                          title={`Generate description for ${col.name}`}
                        >
                          Gen
                        </Button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          ) : (
            <div className="px-5 py-6 text-sm text-ink-dim">
              This table has no introspectable columns.
            </div>
          )}
        </CardBody>
      </Card>

      <div className="mt-4">
        <Link
          to={scopePath(scope, schema)}
          className="text-xs text-ink-dim hover:text-ink"
        >
          ← Back to schema
        </Link>
      </div>

      <GenerateScopeDialog
        open={confirmGenerate}
        onClose={() => setConfirmGenerate(false)}
        docProfiles={runDocProfiles}
        onDocProfilesChange={setRunDocProfiles}
        codeProfiles={runCodeProfiles}
        onCodeProfilesChange={setRunCodeProfiles}
        title={`Generate description for ${schema}.${table}`}
        description="Pick the scope. Just-the-table writes only the table's own COMMENT (one fast LLM call). Bulk run also generates a description for every column."
        singleOption={{
          label: "Just this table",
          description: "Writes only the table's COMMENT. One LLM call, columns untouched.",
          loading: generateTableOnly.isPending,
          onClick: () => {
            generateTableOnly.mutate(undefined, {
              onSettled: () => setConfirmGenerate(false),
            });
          },
        }}
        bulkOption={{
          label: "Whole table — every column too (bulk run)",
          description:
            "Spawns analyze.run for the full table; each column gets its own generated description, recorded in history.",
          loading: generate.isPending,
          onClick: () => generate.mutate(),
        }}
      />
    </>
  );
}

function SummaryCard({
  label,
  value,
  icon: Icon,
}: {
  label: string;
  value: string;
  icon: typeof Columns;
}) {
  return (
    <div className="rounded-xl border border-border bg-surface-raised p-4 shadow-xs">
      <div className="flex items-center justify-between text-[10.5px] uppercase tracking-wider text-ink-dim">
        <span>{label}</span>
        <Icon size={14} />
      </div>
      <div className="mt-1.5 font-mono text-xl text-ink tabular-nums">{value}</div>
    </div>
  );
}
