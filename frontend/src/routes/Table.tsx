import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router-dom";
import { AlignLeft, Columns, Sparkles } from "lucide-react";

import { api } from "../lib/api";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
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
  const params = useParams();
  const schema = params.schema!;
  const table = params.table!;
  const profile = params.profile || "active";
  const remember = useUi((s) => s.rememberOpenedTable);
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const [confirmGenerate, setConfirmGenerate] = useState(false);

  useEffect(() => {
    remember(schema, table);
  }, [schema, table, remember]);

  const snapshot = useQuery({
    queryKey: ["live-snapshot", schema, table],
    queryFn: () => api.liveSnapshot(schema, table),
  });
  const columns = useQuery({
    queryKey: ["live-columns", schema, table],
    queryFn: () => api.liveColumns(schema, table),
  });

  const totalCols = snapshot.data?.columns?.length ?? columns.data?.count ?? 0;
  const commented =
    snapshot.data?.columns?.filter((c) => (c.comment || "").trim().length > 0).length ?? 0;
  const tableComment = snapshot.data?.table_comment ?? "";

  // Inline-edit save handlers — they invalidate the snapshot so the
  // visible value re-fetches with the freshly written COMMENT.
  async function saveTableComment(next: string) {
    await api.setTableComment(schema, table, next);
    qc.invalidateQueries({ queryKey: ["live-snapshot", schema, table] });
    toast.push({
      title: "Description saved",
      tone: "success",
      duration: 2000,
    });
  }

  async function saveColumnComment(column: string, next: string) {
    await api.setColumnComment(schema, table, column, next);
    qc.invalidateQueries({ queryKey: ["live-snapshot", schema, table] });
    toast.push({
      title: `Comment saved`,
      description: `${schema}.${table}.${column}`,
      tone: "success",
      duration: 2000,
    });
  }

  // Two flavours of "Generate":
  // - Single-shot endpoint writes ONLY the table's own COMMENT
  //   (no columns). Fast, one LLM call, returns the new text.
  // - Bulk run path spawns the analyze.run worker for the whole
  //   table so every column also gets a generated description.
  const generateTableOnly = useMutation({
    mutationFn: () => api.generateTableDescription(schema, table),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["live-snapshot", schema, table] });
      toast.push({
        title: "Table description generated",
        description: "Written straight to the live database.",
        tone: "success",
        duration: 2400,
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
      api.generateColumnDescription(schema, table, column),
    onSuccess: (_data, column) => {
      qc.invalidateQueries({ queryKey: ["live-snapshot", schema, table] });
      toast.push({
        title: "Column description generated",
        description: `${schema}.${table}.${column}`,
        tone: "success",
        duration: 2200,
      });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Generation failed",
        description: e.message,
        tone: "error",
      }),
  });

  // Auto-generate via the existing /run worker — spawns a job and
  // redirects to the run-detail page so the user can watch the
  // streaming progress.
  const generate = useMutation({
    mutationFn: () =>
      api.submitRun({
        scope: { [schema]: [table] },
        apply: true,
        missing_only: false,
      }),
    onSuccess: (result) => {
      setConfirmGenerate(false);
      toast.push({
        title: "Bulk run started",
        description: `Streaming every column of ${schema}.${table}…`,
        tone: "info",
        duration: 2200,
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
        title={table}
        breadcrumbs={[
          { label: "Browse", to: "/" },
          { label: schema, to: `/db/${profile}/${schema}` },
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
          <Button
            variant="primary"
            size="md"
            leadingIcon={<Sparkles size={14} />}
            loading={generateTableOnly.isPending || generate.isPending}
            onClick={() => setConfirmGenerate(true)}
          >
            Generate description
          </Button>
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
        <CardBody className="p-0">
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
          to={`/db/${profile}/${schema}`}
          className="text-xs text-ink-dim hover:text-ink"
        >
          ← Back to schema
        </Link>
      </div>

      <GenerateScopeDialog
        open={confirmGenerate}
        onClose={() => setConfirmGenerate(false)}
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
