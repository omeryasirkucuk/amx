/**
 * Admin Settings sub-tab — read-only display of shared workspace config.
 * Reads from GET /api/history/status.
 */

import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "../../lib/api";
import EmptyState from "../../components/EmptyState";
import Skeleton from "../../components/ui/Skeleton";
import { Badge } from "../../components/ui";

interface HistoryStatus {
  enabled: boolean;
  shared_profile: string;
  backfill: Record<string, string>;
}

interface BackfillRowProps {
  scope: string;
  state: string;
}

function BackfillRow({ scope, state }: BackfillRowProps) {
  const tone =
    state === "done"
      ? "positive"
      : state === "running"
        ? "info"
        : "neutral";
  return (
    <tr className="border-b border-border last:border-0">
      <td className="py-2 pr-4 font-mono text-xs text-ink-dim">{scope}</td>
      <td className="py-2">
        <Badge tone={tone}>{state}</Badge>
      </td>
    </tr>
  );
}

export default function AdminSettings() {
  const status = useQuery<HistoryStatus>({
    queryKey: ["history-status"],
    queryFn: () => apiFetch<HistoryStatus>("/api/history/status"),
    staleTime: 30_000,
  });

  if (status.isLoading) {
    return (
      <div className="space-y-2">
        {[...Array(3)].map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    );
  }

  if (status.isError) {
    return (
      <EmptyState
        title="Could not load workspace settings"
        description={(status.error as Error).message}
      />
    );
  }

  const data = status.data;
  const backfillEntries = Object.entries(data?.backfill ?? {});

  return (
    <div className="space-y-6">
      {/* Shared store config */}
      <div className="rounded-lg border border-border bg-surface-subtle/40 p-5">
        <h3 className="mb-4 text-sm font-semibold text-ink">
          Team workspace configuration
        </h3>
        <dl className="grid grid-cols-1 gap-y-3 text-sm sm:grid-cols-2">
          <div>
            <dt className="text-xs font-medium text-ink-dim">Status</dt>
            <dd className="mt-0.5">
              <Badge tone={data?.enabled ? "positive" : "neutral"}>
                {data?.enabled ? "enabled" : "disabled"}
              </Badge>
            </dd>
          </div>
          <div>
            <dt className="text-xs font-medium text-ink-dim">Shared profile</dt>
            <dd className="mt-0.5 font-mono text-ink">
              {data?.shared_profile || (
                <span className="text-ink-dim">not configured</span>
              )}
            </dd>
          </div>
        </dl>
      </div>

      {/* Backfill state */}
      {backfillEntries.length > 0 && (
        <div className="rounded-lg border border-border bg-surface-subtle/40 p-5">
          <h3 className="mb-4 text-sm font-semibold text-ink">
            Migration / backfill state
          </h3>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border">
                <th className="pb-2 pr-4 text-left text-xs font-medium text-ink-dim">
                  Scope
                </th>
                <th className="pb-2 text-left text-xs font-medium text-ink-dim">
                  State
                </th>
              </tr>
            </thead>
            <tbody>
              {backfillEntries.map(([scope, state]) => (
                <BackfillRow key={scope} scope={scope} state={state} />
              ))}
            </tbody>
          </table>
        </div>
      )}

      {backfillEntries.length === 0 && (
        <p className="text-sm text-ink-dim">
          No migration state recorded. Backfill runs automatically when the
          shared workspace is first enabled.
        </p>
      )}
    </div>
  );
}
