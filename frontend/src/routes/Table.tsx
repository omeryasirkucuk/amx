import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { Columns, AlignLeft } from "lucide-react";

import { api } from "../lib/api";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import StatusPill from "../components/StatusPill";
import { useUi } from "../lib/store";

export default function Table() {
  const params = useParams();
  const schema = params.schema!;
  const table = params.table!;
  const profile = params.profile || "active";
  const remember = useUi((s) => s.rememberOpenedTable);

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

  return (
    <>
      <PageHeader
        eyebrow={`${schema}`}
        title={table}
        description={
          tableComment
            ? tableComment
            : "No table comment. Run /run on this schema to generate one — or write one inline once PR-E lands."
        }
        actions={
          <Link
            to={`/db/${profile}/${schema}`}
            className="text-xs text-ink-dim hover:text-ink"
          >
            ← Back to schema
          </Link>
        }
      />

      <div className="grid gap-4 md:grid-cols-3">
        <SummaryCard label="Columns" value={String(totalCols)} icon={Columns} />
        <SummaryCard
          label="With comments"
          value={
            totalCols ? `${commented}/${totalCols}` : "—"
          }
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
        <CardHeader title="Columns" description="Schema-level view of every column. PR-C adds inline column profiling and PR-E adds inline comment editing." />
        <CardBody className="p-0">
          {columns.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading columns…</div>
          ) : columns.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(columns.error as Error).message}
            </div>
          ) : columns.data && columns.data.columns.length > 0 ? (
            <table className="w-full text-sm">
              <thead className="bg-surface-subtle/60 text-[11px] uppercase tracking-wider text-ink-dim">
                <tr>
                  <th className="px-5 py-2 text-left font-semibold">Name</th>
                  <th className="px-5 py-2 text-left font-semibold">Type</th>
                  <th className="px-5 py-2 text-left font-semibold">Nullable</th>
                  <th className="px-5 py-2 text-left font-semibold">Comment</th>
                </tr>
              </thead>
              <tbody>
                {columns.data.columns.map((col, idx) => {
                  const snap = snapshot.data?.columns.find((c) => c.name === col.name);
                  return (
                    <tr
                      key={col.name}
                      className={idx % 2 === 0 ? "" : "bg-surface-subtle/30"}
                    >
                      <td className="px-5 py-2 font-mono text-xs">{col.name}</td>
                      <td className="px-5 py-2 font-mono text-xs text-ink-muted">
                        {col.dtype}
                      </td>
                      <td className="px-5 py-2">
                        <StatusPill tone={col.nullable ? "neutral" : "accent"}>
                          {col.nullable ? "nullable" : "required"}
                        </StatusPill>
                      </td>
                      <td className="px-5 py-2 text-ink-muted">
                        {snap?.comment ? snap.comment : (
                          <span className="italic text-ink-dim">no comment</span>
                        )}
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
    <div className="rounded-xl border border-surface-border bg-surface-raised p-4 shadow-card">
      <div className="flex items-center justify-between text-[11px] uppercase tracking-wider text-ink-dim">
        <span>{label}</span>
        <Icon size={14} />
      </div>
      <div className="mt-2 font-mono text-xl">{value}</div>
    </div>
  );
}
