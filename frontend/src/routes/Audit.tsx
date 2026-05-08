import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import Skeleton from "../components/ui/Skeleton";
import { Badge } from "../components/ui";
import { api, type ApplyEvent, type ApplyEventsResponse } from "../lib/api";

// AMX Studio Audit timeline.
//
// Reads from /api/history/apply-events. Each row is one successful
// COMMENT write -- columns the user cares about: who/when/where/what
// changed (old -> new). The previous design rendered a bare flat
// list which buried the cross-user signal. This rebuild groups by
// date, separates "mine" vs "others", surfaces a visible diff, and
// adds a substring search across schema / table / column / comment
// text so a teammate's change to a specific asset is one query
// away.

type IdentityFilter = "all" | "mine" | "others";

interface DayGroup {
  label: string;       // "Today", "Yesterday", "Mon, Apr 28"
  bucket: string;      // YYYY-MM-DD; stable React key
  events: ApplyEvent[];
}

function startOfDay(d: Date): Date {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

function dayBucketKey(epochSec: number): string {
  const d = new Date(epochSec * 1000);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function dayLabel(epochSec: number): string {
  const d = new Date(epochSec * 1000);
  const today = startOfDay(new Date());
  const eventDay = startOfDay(d);
  const diffDays = Math.round(
    (today.getTime() - eventDay.getTime()) / (24 * 60 * 60 * 1000),
  );
  if (diffDays === 0) return "Today";
  if (diffDays === 1) return "Yesterday";
  if (diffDays < 7) {
    return d.toLocaleDateString(undefined, { weekday: "long" });
  }
  return d.toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
}

function timeOnly(epochSec: number): string {
  return new Date(epochSec * 1000).toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function assetLabel(event: ApplyEvent): string {
  const parts = [event.schema_name, event.table_name].filter(Boolean);
  if (event.column_name) parts.push(event.column_name);
  return parts.join(".");
}

function isMine(
  event: ApplyEvent,
  user: string | null | undefined,
  host: string | null | undefined,
): boolean {
  // We treat user equality as the strong signal; hostname is the
  // tiebreaker for shared usernames (e.g. "ubuntu" / "deploy") on
  // multiple boxes. A missing event.applied_by is foreign by default.
  if (!event.applied_by) return false;
  if (!user) return false;
  if (event.applied_by !== user) return false;
  if (host && event.hostname && event.hostname !== host) return false;
  return true;
}

function groupByDay(events: ApplyEvent[]): DayGroup[] {
  const map = new Map<string, ApplyEvent[]>();
  for (const event of events) {
    const key = dayBucketKey(event.applied_at);
    const list = map.get(key) ?? [];
    list.push(event);
    map.set(key, list);
  }
  // Map insertion order tracks the input (already newest-first), so
  // groups come out newest-day first too.
  return Array.from(map.entries()).map(([bucket, evs]) => ({
    bucket,
    label: dayLabel(evs[0].applied_at),
    events: evs,
  }));
}

export default function Audit() {
  const [runFilter, setRunFilter] = useState<string>("");
  const [profileFilter, setProfileFilter] = useState<string>("");
  const [search, setSearch] = useState<string>("");
  const [identityFilter, setIdentityFilter] = useState<IdentityFilter>("all");

  const runIdNumber = runFilter.trim() ? Number(runFilter.trim()) : null;
  const profileQuery = profileFilter.trim() || null;

  const ctx = useQuery({
    queryKey: ["context"],
    queryFn: () => api.context(),
  });
  const me = ctx.data?.current_user ?? null;
  const myHost = ctx.data?.current_hostname ?? null;

  const query = useQuery<ApplyEventsResponse>({
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

  const allEvents = query.data?.events ?? [];

  // Identity counts drive the filter chip badges so the user can
  // tell at a glance how much of the timeline is theirs.
  const mineCount = useMemo(
    () => allEvents.filter((e) => isMine(e, me, myHost)).length,
    [allEvents, me, myHost],
  );
  const othersCount = allEvents.length - mineCount;

  const filteredEvents = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return allEvents.filter((event) => {
      if (identityFilter === "mine" && !isMine(event, me, myHost)) return false;
      if (identityFilter === "others" && isMine(event, me, myHost)) return false;
      if (needle) {
        const haystack = [
          event.schema_name,
          event.table_name,
          event.column_name ?? "",
          event.applied_by,
          event.hostname,
          event.new_comment,
          event.old_comment ?? "",
          event.profile_name,
        ]
          .join(" ")
          .toLowerCase();
        if (!haystack.includes(needle)) return false;
      }
      return true;
    });
  }, [allEvents, me, myHost, identityFilter, search]);

  const groups = useMemo(() => groupByDay(filteredEvents), [filteredEvents]);

  return (
    <div className="space-y-4">
      <PageHeader
        title="Audit"
        breadcrumbs={[{ label: "Audit" }]}
        description="Every COMMENT successfully written by /apply or Studio. Newest first; grouped by day."
      />

      <Card>
        <CardHeader title="Filters" />
        <CardBody>
          <div className="flex flex-wrap items-end gap-3">
            <label className="flex flex-col text-sm">
              <span className="mb-1 text-text-soft">Search</span>
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="schema, table, column, comment text…"
                className="w-72 rounded border border-border bg-surface px-2 py-1 text-sm"
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1 text-text-soft">Run id</span>
              <input
                value={runFilter}
                onChange={(e) => setRunFilter(e.target.value)}
                placeholder="e.g. 42"
                inputMode="numeric"
                className="w-24 rounded border border-border bg-surface px-2 py-1 font-mono text-sm"
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1 text-text-soft">DB profile</span>
              <input
                value={profileFilter}
                onChange={(e) => setProfileFilter(e.target.value)}
                placeholder="e.g. prod_pg"
                className="w-44 rounded border border-border bg-surface px-2 py-1 font-mono text-sm"
              />
            </label>
            <div className="flex flex-col text-sm">
              <span className="mb-1 text-text-soft">Author</span>
              <div className="flex gap-1">
                <FilterChip
                  active={identityFilter === "all"}
                  onClick={() => setIdentityFilter("all")}
                  label="All"
                  count={allEvents.length}
                />
                <FilterChip
                  active={identityFilter === "mine"}
                  onClick={() => setIdentityFilter("mine")}
                  label="Mine"
                  count={mineCount}
                />
                <FilterChip
                  active={identityFilter === "others"}
                  onClick={() => setIdentityFilter("others")}
                  label="Others"
                  count={othersCount}
                />
              </div>
            </div>
            {(runFilter || profileFilter || search || identityFilter !== "all") && (
              <button
                type="button"
                onClick={() => {
                  setRunFilter("");
                  setProfileFilter("");
                  setSearch("");
                  setIdentityFilter("all");
                }}
                className="rounded border border-border px-2 py-1 text-sm hover:bg-surface-raised"
              >
                Clear all
              </button>
            )}
          </div>
        </CardBody>
      </Card>

      {query.isLoading ? (
        <Card>
          <CardBody>
            <div className="space-y-2">
              <Skeleton className="h-5 w-2/3" />
              <Skeleton className="h-5 w-3/4" />
              <Skeleton className="h-5 w-1/2" />
            </div>
          </CardBody>
        </Card>
      ) : query.isError ? (
        <Card>
          <CardBody>
            <EmptyState
              title="Couldn't load apply events"
              description={(query.error as Error).message}
            />
          </CardBody>
        </Card>
      ) : filteredEvents.length === 0 ? (
        <Card>
          <CardBody>
            <EmptyState
              title={
                allEvents.length === 0
                  ? "No apply events yet"
                  : "No events match the current filters"
              }
              description={
                allEvents.length === 0
                  ? "Run /analyze apply (or hit Apply in Studio) to start filling the audit log."
                  : "Try clearing search or relaxing the Author filter."
              }
            />
          </CardBody>
        </Card>
      ) : (
        groups.map((group) => (
          <Card key={group.bucket}>
            <CardHeader
              title={group.label}
              description={`${group.events.length} event${group.events.length === 1 ? "" : "s"}`}
            />
            <CardBody className="p-0">
              <ul className="divide-y divide-border">
                {group.events.map((event) => (
                  <EventRow key={event.id} event={event} mine={isMine(event, me, myHost)} />
                ))}
              </ul>
            </CardBody>
          </Card>
        ))
      )}
    </div>
  );
}

function FilterChip({
  active,
  onClick,
  label,
  count,
}: {
  active: boolean;
  onClick: () => void;
  label: string;
  count: number;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={
        active
          ? "rounded-md border border-accent/60 bg-accent-soft/40 px-2 py-1 text-xs text-ink"
          : "rounded-md border border-border bg-surface px-2 py-1 text-xs text-ink-muted hover:border-accent/30 hover:text-ink"
      }
    >
      {label}
      <span className="ml-1.5 rounded bg-surface-subtle px-1 text-[10px] text-ink-dim tabular-nums">
        {count}
      </span>
    </button>
  );
}

function EventRow({ event, mine }: { event: ApplyEvent; mine: boolean }) {
  const oldText = event.old_comment ?? "";
  const newText = event.new_comment ?? "";
  const hasDiff = oldText.trim().length > 0 && oldText !== newText;
  const isFirstWrite = !oldText.trim();
  return (
    <li className="px-4 py-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <div className="flex items-baseline gap-2">
          <span className="font-mono text-sm font-medium">{assetLabel(event)}</span>
          {event.asset_kind && event.asset_kind !== "table" && (
            <Badge tone="neutral">{event.asset_kind}</Badge>
          )}
          {isFirstWrite ? (
            <Badge tone="positive">first write</Badge>
          ) : hasDiff ? (
            <Badge tone="warning">overwrote</Badge>
          ) : (
            <Badge tone="neutral">re-applied</Badge>
          )}
        </div>
        <span className="font-mono text-[11px] text-ink-dim tabular-nums">
          {timeOnly(event.applied_at)}
        </span>
      </div>

      {hasDiff ? (
        <div className="mt-2 space-y-1 text-sm">
          <div className="flex gap-2 rounded-md border border-border bg-surface-subtle/40 px-2 py-1.5">
            <span className="select-none text-[10px] font-semibold uppercase tracking-wider text-ink-dim">
              Was
            </span>
            <span className="line-through opacity-70">{oldText}</span>
          </div>
          <div className="flex gap-2 rounded-md border border-accent/30 bg-accent-soft/20 px-2 py-1.5">
            <span className="select-none text-[10px] font-semibold uppercase tracking-wider text-accent-ink">
              Now
            </span>
            <span>{newText}</span>
          </div>
        </div>
      ) : (
        <p className="mt-2 text-sm">{newText || <em className="text-ink-dim">(empty)</em>}</p>
      )}

      <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-ink-dim">
        <span className="inline-flex items-center gap-1">
          {mine ? (
            <Badge tone="info">you</Badge>
          ) : (
            <Badge tone="warning">{event.applied_by || "unknown"}</Badge>
          )}
          {!mine && event.hostname && (
            <span className="font-mono text-[11px]">@ {event.hostname}</span>
          )}
        </span>
        {event.profile_name && (
          <span className="font-mono">profile: {event.profile_name}</span>
        )}
        {event.run_id != null && <span>run #{event.run_id}</span>}
      </div>
    </li>
  );
}
