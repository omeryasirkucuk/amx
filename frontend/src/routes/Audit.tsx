import { useState } from "react";
import { useQuery } from "@tanstack/react-query";

import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import Skeleton from "../components/ui/Skeleton";
import { api, type ApplyEvent } from "../lib/api";

// Studio Recent Applies panel.
//
// Reads from /api/history/apply-events (the new endpoint introduced
// alongside the apply_events SQLite table). One row per successful
// COMMENT write; newest first. Optional filters by run id / DB
// profile so a future "show only this run's writes" affordance can
// pivot via URL params without re-fetching.

function formatTimestamp(epochSec: number): string {
  return new Date(epochSec * 1000).toLocaleString();
}

function assetLabel(event: ApplyEvent): string {
  const parts = [event.schema_name, event.table_name].filter(Boolean);
  if (event.column_name) parts.push(event.column_name);
  return parts.join(".");
}

export default function Audit() {
  const [runFilter, setRunFilter] = useState<string>("");
  const [profileFilter, setProfileFilter] = useState<string>("");

  const runIdNumber = runFilter.trim() ? Number(runFilter.trim()) : null;
  const profileQuery = profileFilter.trim() || null;

  const query = useQuery({
    queryKey: ["apply-events", runIdNumber, profileQuery],
    queryFn: () =>
      api.applyEvents({
        runId: runIdNumber,
        profileName: profileQuery,
        limit: 200,
      }),
    refetchInterval: 30_000,
    refetchOnWindowFocus: true,
  });

  return (
    <div className="space-y-4">
      <PageHeader
        title="Recent Applies"
        breadcrumbs={[{ label: "Audit" }]}
        description="Every COMMENT successfully written by /apply or Studio. Newest first."
      />

      <Card>
        <CardHeader title="Filters" />
        <CardBody>
          <div className="flex flex-wrap items-end gap-3">
            <label className="flex flex-col text-sm">
              <span className="mb-1 text-text-soft">Run id</span>
              <input
                value={runFilter}
                onChange={(e) => setRunFilter(e.target.value)}
                placeholder="e.g. 42"
                inputMode="numeric"
                className="rounded border border-border bg-surface px-2 py-1 font-mono text-sm"
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1 text-text-soft">DB profile</span>
              <input
                value={profileFilter}
                onChange={(e) => setProfileFilter(e.target.value)}
                placeholder="e.g. prod_pg"
                className="rounded border border-border bg-surface px-2 py-1 font-mono text-sm"
              />
            </label>
            {(runFilter || profileFilter) && (
              <button
                type="button"
                onClick={() => {
                  setRunFilter("");
                  setProfileFilter("");
                }}
                className="rounded border border-border px-2 py-1 text-sm hover:bg-surface-raised"
              >
                Clear filters
              </button>
            )}
          </div>
        </CardBody>
      </Card>

      <Card>
        <CardHeader
          title="Apply timeline"
          description={
            query.data
              ? `${query.data.count} event${query.data.count === 1 ? "" : "s"}`
              : undefined
          }
        />
        <CardBody className="p-0">
          {query.isLoading ? (
            <div className="space-y-2 p-4">
              <Skeleton className="h-5 w-2/3" />
              <Skeleton className="h-5 w-3/4" />
              <Skeleton className="h-5 w-1/2" />
            </div>
          ) : query.isError ? (
            <EmptyState
              title="Couldn't load apply events"
              description={(query.error as Error).message}
            />
          ) : !query.data || query.data.events.length === 0 ? (
            <EmptyState
              title="No apply events yet"
              description={
                runFilter || profileFilter
                  ? "Try clearing the filters — the audit log may still be empty for this scope."
                  : "Run /analyze apply (or hit Apply in Studio) to start filling the audit log."
              }
            />
          ) : (
            <ul className="divide-y divide-border">
              {query.data.events.map((event) => (
                <li key={event.id} className="p-4">
                  <div className="flex flex-wrap items-baseline justify-between gap-2">
                    <span className="font-mono text-sm">{assetLabel(event)}</span>
                    <span className="text-xs text-text-soft">
                      {formatTimestamp(event.applied_at)}
                    </span>
                  </div>
                  <p className="mt-1 text-sm">{event.new_comment}</p>
                  {event.old_comment != null && (
                    <p className="mt-1 text-xs text-text-soft">
                      <span className="opacity-70">replaced: </span>
                      <span className="line-through">{event.old_comment}</span>
                    </p>
                  )}
                  <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-text-soft">
                    {event.profile_name && <span>profile: {event.profile_name}</span>}
                    {event.run_id != null && <span>run #{event.run_id}</span>}
                    {event.applied_by && <span>by {event.applied_by}</span>}
                    {event.hostname && <span>on {event.hostname}</span>}
                    {event.asset_kind && event.asset_kind !== "table" && (
                      <span>kind: {event.asset_kind}</span>
                    )}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </CardBody>
      </Card>
    </div>
  );
}
