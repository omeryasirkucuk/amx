/**
 * Admin Audit tab — chronological list of admin audit events.
 * Filters: actor, action, target resource.
 * Pulls from GET /api/admin/audit.
 */

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "../../lib/api";
import EmptyState from "../../components/EmptyState";
import Skeleton from "../../components/ui/Skeleton";
import { Badge } from "../../components/ui";

interface AuditEvent {
  id: string;
  actor_user_id: string | null;
  actor_username: string | null;
  action: string;
  target_resource: string | null;
  details: Record<string, unknown> | null;
  occurred_at: string;
}

interface AuditResponse {
  events: AuditEvent[];
  count: number;
}

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString(undefined, {
      dateStyle: "short",
      timeStyle: "short",
    });
  } catch {
    return iso;
  }
}

function actionTone(action: string): "positive" | "warning" | "neutral" | "info" {
  if (action.startsWith("promote")) return "info";
  if (action.startsWith("demote") || action.startsWith("revoke")) return "warning";
  if (action.startsWith("forced_overwrite")) return "warning";
  return "neutral";
}

export default function AdminAudit() {
  const [actorFilter, setActorFilter] = useState("");
  const [actionFilter, setActionFilter] = useState("");

  const audit = useQuery<AuditResponse>({
    queryKey: ["admin-audit", actorFilter, actionFilter],
    queryFn: () => {
      const params = new URLSearchParams({ limit: "200" });
      if (actorFilter.trim()) params.set("actor", actorFilter.trim());
      if (actionFilter.trim()) params.set("action", actionFilter.trim());
      return apiFetch<AuditResponse>(`/api/admin/audit?${params.toString()}`);
    },
    retry: false,
  });

  const list = audit.data?.events ?? [];

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex flex-wrap gap-3">
        <label className="flex flex-col text-sm">
          <span className="mb-1 text-xs text-ink-muted">Actor</span>
          <input
            value={actorFilter}
            onChange={(e) => setActorFilter(e.target.value)}
            placeholder="username…"
            className="rounded border border-border bg-surface px-2 py-1 text-sm sm:w-44"
          />
        </label>
        <label className="flex flex-col text-sm">
          <span className="mb-1 text-xs text-ink-muted">Action</span>
          <input
            value={actionFilter}
            onChange={(e) => setActionFilter(e.target.value)}
            placeholder="e.g. promote"
            className="rounded border border-border bg-surface px-2 py-1 text-sm sm:w-44"
          />
        </label>
        {(actorFilter || actionFilter) && (
          <div className="flex items-end">
            <button
              type="button"
              onClick={() => {
                setActorFilter("");
                setActionFilter("");
              }}
              className="rounded border border-border px-2 py-1 text-sm hover:bg-surface-subtle"
            >
              Clear
            </button>
          </div>
        )}
      </div>

      {audit.isLoading ? (
        <div className="space-y-2">
          {[...Array(4)].map((_, i) => (
            <Skeleton key={i} className="h-10 w-full" />
          ))}
        </div>
      ) : audit.isError ? (
        <EmptyState
          title="Could not load audit log"
          description={(audit.error as Error).message}
        />
      ) : list.length === 0 ? (
        <EmptyState
          title="No audit events"
          description={
            actorFilter || actionFilter
              ? "No events match the current filters."
              : "Admin actions (promote, demote, revoke, forced overwrites) appear here."
          }
        />
      ) : (
        <div className="overflow-hidden rounded-lg border border-border">
          <table className="hidden w-full text-sm sm:table">
            <thead className="border-b border-border bg-surface-subtle/50">
              <tr>
                <th className="px-4 py-2 text-left font-medium text-ink-dim">Actor</th>
                <th className="px-4 py-2 text-left font-medium text-ink-dim">Action</th>
                <th className="hidden px-4 py-2 text-left font-medium text-ink-dim md:table-cell">
                  Target
                </th>
                <th className="px-4 py-2 text-left font-medium text-ink-dim">When</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {list.map((ev) => (
                <tr
                  key={ev.id}
                  className="transition-colors hover:bg-surface-subtle/40"
                >
                  <td className="px-4 py-3 font-medium text-ink">
                    {ev.actor_username || "system"}
                  </td>
                  <td className="px-4 py-3">
                    <Badge tone={actionTone(ev.action)}>{ev.action}</Badge>
                  </td>
                  <td className="hidden px-4 py-3 font-mono text-xs text-ink-dim md:table-cell">
                    {ev.target_resource || "—"}
                  </td>
                  <td className="px-4 py-3 text-xs text-ink-dim">
                    {formatDate(ev.occurred_at)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          {/* Mobile */}
          <ul className="divide-y divide-border sm:hidden">
            {list.map((ev) => (
              <li key={ev.id} className="px-4 py-3">
                <div className="flex items-center justify-between gap-2">
                  <span className="font-medium text-ink">
                    {ev.actor_username || "system"}
                  </span>
                  <Badge tone={actionTone(ev.action)}>{ev.action}</Badge>
                </div>
                <div className="mt-0.5 text-xs text-ink-dim">
                  {formatDate(ev.occurred_at)}
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
