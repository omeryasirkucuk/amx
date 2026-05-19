/**
 * Admin Activity tab — live session event feed.
 * Polls GET /api/admin/sessions every 30 s.
 */

import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "../../lib/api";
import EmptyState from "../../components/EmptyState";
import Skeleton from "../../components/ui/Skeleton";
import { Badge } from "../../components/ui";

interface SessionEvent {
  id: string;
  username: string;
  hostname: string;
  event_type: string;
  client_version: string | null;
  occurred_at: string;
  db_profiles_seen: string[];
}

interface SessionsResponse {
  events: SessionEvent[];
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

export default function AdminActivity() {
  const events = useQuery<SessionsResponse>({
    queryKey: ["admin-sessions"],
    queryFn: () => apiFetch<SessionsResponse>("/api/admin/sessions?limit=100"),
    refetchInterval: 30_000,
    retry: false,
  });

  if (events.isLoading) {
    return (
      <div className="space-y-2">
        {[...Array(5)].map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    );
  }

  if (events.isError) {
    return (
      <EmptyState
        title="Could not load session events"
        description={(events.error as Error).message}
      />
    );
  }

  const list = events.data?.events ?? [];

  if (list.length === 0) {
    return (
      <EmptyState
        title="No session events yet"
        description="Connection events appear here as team members connect to the shared workspace."
      />
    );
  }

  return (
    <div className="overflow-hidden rounded-lg border border-border">
      <table className="hidden w-full text-sm sm:table">
        <thead className="border-b border-border bg-surface-subtle/50">
          <tr>
            <th className="px-4 py-2 text-left font-medium text-ink-dim">User</th>
            <th className="px-4 py-2 text-left font-medium text-ink-dim">Event</th>
            <th className="hidden px-4 py-2 text-left font-medium text-ink-dim md:table-cell">
              Profiles
            </th>
            <th className="px-4 py-2 text-left font-medium text-ink-dim">When</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {list.map((ev) => (
            <tr key={ev.id} className="transition-colors hover:bg-surface-subtle/40">
              <td className="px-4 py-3">
                <span className="font-medium text-ink">{ev.username}</span>
                <span className="ml-1 font-mono text-xs text-ink-dim">@{ev.hostname}</span>
              </td>
              <td className="px-4 py-3">
                <Badge tone={ev.event_type === "connect" ? "positive" : "neutral"}>
                  {ev.event_type}
                </Badge>
                {ev.client_version && (
                  <span className="ml-2 font-mono text-xs text-ink-dim">
                    v{ev.client_version}
                  </span>
                )}
              </td>
              <td className="hidden px-4 py-3 md:table-cell">
                <div className="flex flex-wrap gap-1">
                  {(ev.db_profiles_seen ?? []).map((p) => (
                    <span
                      key={p}
                      className="rounded bg-surface-subtle px-1.5 py-px font-mono text-[11px] text-ink-muted"
                    >
                      {p}
                    </span>
                  ))}
                  {!ev.db_profiles_seen?.length && (
                    <span className="text-xs text-ink-dim">—</span>
                  )}
                </div>
              </td>
              <td className="px-4 py-3 text-xs text-ink-dim">
                {formatDate(ev.occurred_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Mobile list */}
      <ul className="divide-y divide-border sm:hidden">
        {list.map((ev) => (
          <li key={ev.id} className="px-4 py-3">
            <div className="flex items-center justify-between gap-2">
              <span className="font-medium text-ink">{ev.username}</span>
              <Badge tone={ev.event_type === "connect" ? "positive" : "neutral"}>
                {ev.event_type}
              </Badge>
            </div>
            <div className="mt-0.5 font-mono text-xs text-ink-dim">
              @{ev.hostname} · {formatDate(ev.occurred_at)}
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}
