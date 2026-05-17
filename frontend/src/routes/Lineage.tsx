/**
 * Lineage browse route — lists rendered artifacts plus the
 * Create-new entry point. The browse page is the user's discovery
 * surface: from here they either pick an existing artifact or fire
 * the wizard to make a new one.
 */

import { useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { Plus, Sparkles, Workflow } from "lucide-react";

import {
  lineageDiscover,
  lineageList,
  type LineageArtifact,
  type LineageDiscoverResponse,
} from "../lib/api";
import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";
import LineageCreateModal from "../components/LineageCreateModal";
import { Badge, Button } from "../components/ui";

function fmtTime(ts: number): string {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString();
}

export default function Lineage() {
  const navigate = useNavigate();
  const [createOpen, setCreateOpen] = useState(false);
  const [discovery, setDiscovery] = useState<LineageDiscoverResponse | null>(null);
  const list = useQuery({
    queryKey: ["lineage-artifacts"],
    queryFn: () => lineageList(),
  });

  const rows = useMemo<LineageArtifact[]>(() => list.data?.artifacts ?? [], [list.data]);

  const discover = useMutation({
    mutationFn: () => lineageDiscover({}),
    onSuccess: (data) => setDiscovery(data),
  });

  const openDiscoveredAnchor = async (
    anchor: { fqn: string; database: string; schema: string; table: string },
    profile: string,
  ) => {
    // Refresh first so an artifact exists for this anchor + the
    // returned slug links to a known row.
    const anchorPath = [anchor.schema, anchor.table].filter(Boolean).join(".");
    try {
      await import("../lib/api").then((m) =>
        m.lineageRefresh(anchorPath, { profile, database: anchor.database }),
      );
    } catch {
      /* fall through — the canvas page will surface the error */
    }
    const slug = `${anchor.schema}-${anchor.table}`.replace(/[^A-Za-z0-9_-]+/g, "_");
    navigate(`/lineage/${encodeURIComponent(profile)}/${encodeURIComponent(slug)}`);
  };

  return (
    <div className="flex h-full flex-col gap-4">
      <PageHeader
        title="Lineage"
        description="Column-level lineage diagrams across your connected DB profiles. Open one to see the React Flow canvas, or render a new one from any table you have catalogued."
        actions={
          <Button
            variant="primary"
            size="md"
            leadingIcon={<Plus size={14} />}
            onClick={() => setCreateOpen(true)}
          >
            Create new lineage
          </Button>
        }
      />

      {list.isLoading && (
        <div className="text-sm text-fg-muted">Loading artifacts…</div>
      )}

      {!list.isLoading && (
        <div className="flex items-start justify-between gap-3 rounded-md border border-surface-border bg-surface-muted px-3 py-2 text-sm">
          <div className="flex-1">
            <p className="font-medium text-fg-default">Auto-discover</p>
            <p className="text-xs text-fg-muted">
              Walks every cached table in the active profile and surfaces the
              anchors that already carry lineage signal (FK / view DDL / query
              history / codebase / previously-suggested LLM edges). Cache-only
              — never opens a wire connection.
            </p>
          </div>
          <Button
            variant="secondary"
            size="sm"
            leadingIcon={<Sparkles size={14} />}
            onClick={() => discover.mutate()}
            loading={discover.isPending}
          >
            Discover lineage
          </Button>
        </div>
      )}

      {discovery && (
        <div className="rounded-md border border-surface-border bg-surface-raised">
          <div className="flex items-center justify-between border-b border-surface-border px-4 py-2">
            <p className="text-sm">
              <span className="font-medium text-fg-default">
                {discovery.tables_with_edges}
              </span>{" "}
              of {discovery.tables_examined} tables carry lineage signal
              {discovery.truncated && (
                <Badge tone="warning" className="ml-2">
                  truncated
                </Badge>
              )}{" "}
              <span className="text-xs text-fg-muted">
                ({discovery.total_edges} edges, {discovery.duration_sec.toFixed(2)}s)
              </span>
            </p>
            <button
              type="button"
              onClick={() => setDiscovery(null)}
              className="text-xs text-fg-muted hover:text-fg-default"
            >
              Dismiss
            </button>
          </div>
          <ul className="max-h-72 divide-y divide-surface-border overflow-y-auto">
            {discovery.anchors.length === 0 && (
              <li className="px-4 py-3 text-xs text-fg-muted">
                No anchors with cached lineage yet. Try Create new lineage or
                inspect more of your catalog first.
              </li>
            )}
            {discovery.anchors.map((a) => (
              <li
                key={a.fqn}
                className="flex items-center justify-between px-4 py-2 text-xs"
              >
                <div className="flex flex-col">
                  <span className="font-mono">{a.fqn}</span>
                  <span className="text-fg-muted">
                    {a.extractors_used.join(", ") || "—"}
                    {a.partial && (
                      <Badge tone="warning" className="ml-2">
                        partial
                      </Badge>
                    )}
                  </span>
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-fg-muted">{a.edge_count} edges</span>
                  <button
                    type="button"
                    onClick={() => openDiscoveredAnchor(a, discovery.profile)}
                    className="text-accent-default hover:underline"
                  >
                    Open →
                  </button>
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}

      {!list.isLoading && rows.length === 0 && (
        <EmptyState
          icon={Workflow}
          title="No lineage artifacts yet"
          description="Click Create new lineage above, or open any table from the sidebar and use its Open lineage button."
          actions={
            <Button
              variant="primary"
              size="md"
              leadingIcon={<Plus size={14} />}
              onClick={() => setCreateOpen(true)}
            >
              Create new lineage
            </Button>
          }
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

      <LineageCreateModal open={createOpen} onClose={() => setCreateOpen(false)} />
    </div>
  );
}
