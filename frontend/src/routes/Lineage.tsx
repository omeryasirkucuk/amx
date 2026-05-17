/**
 * Lineage browse route — lists rendered artifacts and links each to
 * the detail canvas. Mirrors the RunsList pattern (loading via
 * TanStack Query, EmptyState when nothing landed yet).
 */

import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { Workflow } from "lucide-react";

import { lineageList, type LineageArtifact } from "../lib/api";
import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";
import { Badge } from "../components/ui";

function fmtTime(ts: number): string {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString();
}

export default function Lineage() {
  const list = useQuery({
    queryKey: ["lineage-artifacts"],
    queryFn: () => lineageList(),
  });

  const rows = useMemo<LineageArtifact[]>(() => list.data?.artifacts ?? [], [list.data]);

  return (
    <div className="flex h-full flex-col gap-4">
      <PageHeader
        title="Lineage"
        description="Column-level lineage diagrams across your connected DB profiles. Open one to see the React-Flow canvas."
      />
      {list.isLoading && (
        <div className="text-sm text-fg-muted">Loading artifacts…</div>
      )}
      {!list.isLoading && rows.length === 0 && (
        <EmptyState
          icon={Workflow}
          title="No lineage artifacts yet"
          description="Run /lineage create in the CLI, or open a table page and use the Lineage tab to render one."
        />
      )}
      {rows.length > 0 && (
        <div className="overflow-hidden rounded-xl border border-surface-border bg-surface-raised">
          <table className="w-full text-sm">
            <thead className="bg-surface-muted text-left text-xs uppercase tracking-wide text-fg-muted">
              <tr>
                <th className="px-4 py-2">Name</th>
                <th className="px-4 py-2">Profile</th>
                <th className="px-4 py-2">Nodes</th>
                <th className="px-4 py-2">Edges</th>
                <th className="px-4 py-2">Extractors</th>
                <th className="px-4 py-2">Generated</th>
                <th className="px-4 py-2">Open</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.id} className="border-t border-surface-border">
                  <td className="px-4 py-2 font-mono text-xs">{row.name}</td>
                  <td className="px-4 py-2">
                    <Badge tone="neutral">{row.db_profile}</Badge>
                  </td>
                  <td className="px-4 py-2 text-right">{row.node_count}</td>
                  <td className="px-4 py-2 text-right">{row.edge_count}</td>
                  <td className="px-4 py-2">
                    <span className="text-xs">
                      {row.extractors_used.join(", ") || "—"}
                      {row.extractors_partial && (
                        <Badge tone="warning" className="ml-2">
                          partial
                        </Badge>
                      )}
                    </span>
                  </td>
                  <td className="px-4 py-2 text-xs text-fg-muted">
                    {fmtTime(row.generated_at)}
                  </td>
                  <td className="px-4 py-2">
                    <Link
                      to={`/lineage/${encodeURIComponent(row.db_profile)}/${encodeURIComponent(row.name)}`}
                      className="text-sm text-accent-default underline-offset-2 hover:underline"
                    >
                      Open canvas →
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
