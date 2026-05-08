import { useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { AlertTriangle, PlayCircle, Settings as SettingsIcon } from "lucide-react";

import { api, apiFetch, type ApplyAttribution } from "../lib/api";
import { cn } from "../lib/cn";
import type { Scope } from "../lib/scope";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import {
  AlertDialog,
  Badge,
  Button,
  InfoHint,
  Skeleton,
  Switch,
  useToast,
} from "../components/ui";

interface SchemaPickState {
  schema: string;
  tables: string[]; // empty = "every reachable table"
}

/** Read scope from `?profile=…&database=…` (or `&catalog=…`) — the
 * Database/Schema/Table pages link here with the scope encoded. */
function useRunScope(): Scope | null {
  const [params] = useSearchParams();
  const profile = params.get("profile") || "";
  const database = params.get("database") || "";
  const catalog = params.get("catalog") || "";
  if (!profile) return null;
  if (catalog) return { profile, catalog, kind: "catalog" };
  if (database) return { profile, database, kind: "database" };
  return null;
}

export default function RunNew() {
  const navigate = useNavigate();
  const toast = useToast();
  const scope = useRunScope();
  const [picked, setPicked] = useState<SchemaPickState[]>([]);
  const [missingOnly, setMissingOnly] = useState(true);
  const [autoApply, setAutoApply] = useState(false);
  const [batchMode, setBatchMode] = useState(false);

  const ctx = useQuery({ queryKey: ["context"], queryFn: () => api.context() });
  const supportsBatch = !!ctx.data?.llm_supports_batch;
  const llmProvider = ctx.data?.llm_provider ?? null;
  const me = ctx.data?.current_user ?? null;
  const myHost = ctx.data?.current_hostname ?? null;
  // Pre-flight conflict warning. We pull every asset on the active
  // profile that has been applied at least once, then cross-reference
  // with the schemas/tables the user is about to run. Empty payload
  // when the history store has no events yet (fresh install) — the
  // warning never fires, the run starts immediately.
  const attribution = useQuery({
    queryKey: [
      "apply-attribution",
      scope?.profile ?? "",
      // Re-fetch when picked schemas change so we narrow the response.
      // Only the schemas the user picked are useful; pulling every
      // schema's history would be wasteful on a busy team profile.
      picked.map((p) => p.schema).sort().join(","),
    ],
    queryFn: () =>
      api.applyAttribution(
        scope?.profile ?? "",
        picked.map((p) => p.schema),
      ),
    enabled: !!scope?.profile && picked.length > 0,
    retry: false,
  });
  const [confirmOverwriteOpen, setConfirmOverwriteOpen] = useState(false);

  // Map (schema.table.column?) -> latest apply event so the table
  // picker can show a per-asset badge. Column-level events fall
  // back to their parent table when the run targets the table.
  const attributionByAsset = useMemo(() => {
    const m = new Map<string, ApplyAttribution>();
    for (const ev of attribution.data?.events ?? []) {
      const key = ev.column_name
        ? `${ev.schema_name}.${ev.table_name}.${ev.column_name}`
        : `${ev.schema_name}.${ev.table_name}`;
      m.set(key, ev);
    }
    return m;
  }, [attribution.data]);

  // Conflicts = picked assets that someone *else* applied. A picked
  // schema with empty tables (= all reachable) is conservative here:
  // we list every applied asset under that schema as a potential
  // overwrite. The user resolves the warning by picking specific
  // tables (which narrows the conflict list) or by acknowledging.
  const conflicts = useMemo<ApplyAttribution[]>(() => {
    if (!me || !attribution.data?.events) return [];
    const out: ApplyAttribution[] = [];
    for (const ev of attribution.data.events) {
      // Same OS user + same host means this row is "yours"; anyone
      // else's row counts as a foreign change worth surfacing.
      if (ev.applied_by === me && (myHost ? ev.hostname === myHost : true)) continue;
      // Find the picked schema entry; skip if user didn't pick this schema.
      const pick = picked.find((p) => p.schema === ev.schema_name);
      if (!pick) continue;
      // Empty tables = "all tables in this schema" — every applied
      // row under it is in scope. Otherwise, only conflicts on the
      // tables the user specifically picked.
      if (pick.tables.length > 0 && !pick.tables.includes(ev.table_name)) continue;
      out.push(ev);
    }
    return out;
  }, [attribution.data, me, myHost, picked]);
  // Pre-flight gate: the worker fails fast in _run_worker_body when
  // cfg.llm.provider/model are missing, but the SPA shouldn't even
  // accept the click — the user only sees the error after the job
  // status flips to failed in the run detail. Surfacing the gap on
  // RunNew keeps the bad path out of history altogether.
  const llmReady = !!(
    ctx.data?.llm_provider && ctx.data?.llm_model && ctx.data?.active_llm_profile
  );

  const schemas = useQuery({
    queryKey: [
      "live-schemas",
      scope?.profile ?? "",
      scope?.database ?? "",
      scope?.catalog ?? "",
    ],
    queryFn: () => api.liveSchemas(scope!),
    enabled: !!scope,
    retry: false,
  });

  const scopeUnavailable = !scope;

  const submit = useMutation({
    mutationFn: () =>
      api.submitRun({
        scope: Object.fromEntries(picked.map((p) => [p.schema, p.tables])),
        apply: autoApply,
        missing_only: missingOnly,
        batch_mode: batchMode && supportsBatch,
        db_profile: scope?.profile,
        database: scope?.database,
        catalog: scope?.catalog,
      }),
    onSuccess: (result) => {
      toast.push({
        title: "Run started",
        description: `${picked.length} ${picked.length === 1 ? "schema" : "schemas"} queued.`,
        tone: "success",
        duration: 2200,
      });
      navigate(`/runs/new-${result.job_id}`);
    },
    onError: (err: Error) => {
      toast.push({
        title: "Could not start run",
        description: err.message,
        tone: "error",
      });
    },
  });

  const totalAssets = useMemo(
    () =>
      picked.reduce(
        (acc, p) => acc + (p.tables.length === 0 ? 1 : p.tables.length),
        0,
      ),
    [picked],
  );

  function toggleSchema(name: string) {
    setPicked((curr) => {
      const idx = curr.findIndex((p) => p.schema === name);
      if (idx >= 0) {
        return [...curr.slice(0, idx), ...curr.slice(idx + 1)];
      }
      return [...curr, { schema: name, tables: [] }];
    });
  }

  function isPicked(name: string) {
    return picked.some((p) => p.schema === name);
  }

  return (
    <>
      <PageHeader
        title="New run"
        breadcrumbs={[{ label: "Runs", to: "/runs" }, { label: "New" }]}
      />

      {scopeUnavailable ? (
        <ScopePicker />
      ) : !ctx.isLoading && !llmReady ? (
        <Card>
          <CardBody className="px-6 py-8">
            <div className="flex items-start gap-3">
              <SettingsIcon
                size={18}
                className="mt-0.5 flex-none text-warning"
                aria-hidden="true"
              />
              <div className="min-w-0 flex-1 space-y-2">
                <p className="text-sm font-semibold text-ink">
                  Configure an LLM before running.
                </p>
                <p className="text-sm text-ink-muted">
                  {ctx.data?.llm_provider
                    ? "The active LLM profile has no model selected — Studio needs both a provider and a model to generate metadata."
                    : "No LLM profile is active yet. Add one in Settings → LLM and Studio will route generation through it."}
                </p>
                <Link
                  to="/settings?tab=llm"
                  className="mt-2 inline-flex h-8 items-center gap-1.5 rounded-md bg-accent px-3 text-xs font-medium text-accent-soft transition hover:opacity-90"
                >
                  <SettingsIcon size={12} />
                  Open LLM settings
                </Link>
              </div>
            </div>
          </CardBody>
        </Card>
      ) : (
        <div className="grid gap-4 lg:grid-cols-[2fr_1fr]">
          <Card>
            <CardHeader title="Scope" />
            <CardBody className="p-0">
              {schemas.isLoading ? (
                <ul className="divide-y divide-border">
                  {Array.from({ length: 5 }).map((_, i) => (
                    <li key={i} className="px-5 py-3">
                      <Skeleton className="h-3 w-1/3" />
                    </li>
                  ))}
                </ul>
              ) : schemas.error ? (
                <div className="px-5 py-6 text-sm text-critical">
                  {(schemas.error as Error).message}
                </div>
              ) : !schemas.data?.schemas?.length ? (
                <div className="px-5 py-6 text-sm text-ink-dim">
                  No schemas reachable.
                </div>
              ) : (
                <ul className="divide-y divide-border">
                  {schemas.data.schemas.map((name) => (
                    <li key={name}>
                      <button
                        type="button"
                        onClick={() => toggleSchema(name)}
                        className={cn(
                          "flex w-full items-center justify-between px-5 py-2.5 text-left text-sm transition-colors duration-fast hover:bg-surface-subtle/50",
                          isPicked(name) && "bg-accent-soft/40",
                        )}
                      >
                        <span className="font-medium text-ink">{name}</span>
                        <span
                          className={cn(
                            "text-[10.5px] uppercase tracking-wider",
                            isPicked(name) ? "text-accent-ink" : "text-ink-dim",
                          )}
                        >
                          {isPicked(name) ? "selected" : "—"}
                        </span>
                      </button>
                      {isPicked(name) && (
                        <SchemaTablePicker
                          schema={name}
                          selected={
                            picked.find((p) => p.schema === name)?.tables ?? []
                          }
                          onChange={(tables) =>
                            setPicked((curr) =>
                              curr.map((p) =>
                                p.schema === name ? { ...p, tables } : p,
                              ),
                            )
                          }
                          attribution={attributionByAsset}
                          currentUser={me}
                          currentHost={myHost}
                        />
                      )}
                    </li>
                  ))}
                </ul>
              )}
            </CardBody>
          </Card>

          <Card>
            <CardHeader title="Options" />
            <CardBody className="space-y-4 text-sm">
              <Switch
                checked={missingOnly}
                onChange={(e) => setMissingOnly(e.target.checked)}
                label={
                  <span className="inline-flex items-center gap-1">
                    Missing only
                    <InfoHint text="Only process tables/columns that don't have a description yet." />
                  </span>
                }
                description="Skip tables and columns that already have a comment."
              />
              <Switch
                checked={autoApply}
                onChange={(e) => setAutoApply(e.target.checked)}
                label={
                  <span className="inline-flex items-center gap-1">
                    Auto-apply on success
                    <InfoHint text="Skip review; write generated descriptions straight to the database." />
                  </span>
                }
                description="Write approved descriptions to the live DB without a separate Apply step."
              />
              <Switch
                checked={batchMode && supportsBatch}
                onChange={(e) => setBatchMode(e.target.checked)}
                disabled={!supportsBatch}
                label={
                  <span className="inline-flex items-center gap-1">
                    Batch mode
                    <InfoHint
                      text={
                        supportsBatch
                          ? "Submit every request in one async batch — ~50% cheaper, returns in minutes to hours instead of streaming live."
                          : `Provider${llmProvider ? ` "${llmProvider}"` : ""} has no batch implementation. Switch to OpenAI or Anthropic to enable.`
                      }
                    />
                  </span>
                }
                description={
                  supportsBatch
                    ? "Best for large scopes where you can wait. Streams a polling progress indicator instead of per-table updates."
                    : "Only OpenAI and Anthropic providers support batch."
                }
              />
              <hr className="border-border" />
              <dl className="grid grid-cols-2 gap-y-1.5 text-xs">
                <dt className="text-ink-dim">Schemas</dt>
                <dd className="text-right font-mono tabular-nums text-ink">
                  {picked.length}
                </dd>
                <dt className="text-ink-dim">Asset slots</dt>
                <dd className="text-right font-mono tabular-nums text-ink">
                  {totalAssets}
                </dd>
                <dt className="text-ink-dim">Mode</dt>
                <dd className="text-right font-mono text-ink">
                  {batchMode && supportsBatch ? "batch" : "chat"}
                </dd>
              </dl>
              {conflicts.length > 0 && (
                <div
                  className="flex gap-2 rounded-md border border-warning/40 bg-warning-soft/20 px-3 py-2 text-xs"
                  role="alert"
                >
                  <AlertTriangle
                    size={14}
                    className="mt-0.5 flex-none text-warning"
                    aria-hidden="true"
                  />
                  <div className="space-y-1">
                    <p className="font-medium text-ink">
                      {conflicts.length} of the picked assets were last
                      applied by someone else.
                    </p>
                    <ul className="space-y-0.5 text-ink-muted">
                      {conflicts.slice(0, 5).map((c) => {
                        const asset = c.column_name
                          ? `${c.schema_name}.${c.table_name}.${c.column_name}`
                          : `${c.schema_name}.${c.table_name}`;
                        return (
                          <li key={c.applied_at + asset} className="font-mono">
                            <span className="text-ink">{asset}</span>{" "}
                            <span className="text-ink-dim">·</span>{" "}
                            <span>{c.applied_by || "unknown"}</span>
                          </li>
                        );
                      })}
                      {conflicts.length > 5 && (
                        <li className="text-ink-dim">
                          + {conflicts.length - 5} more
                        </li>
                      )}
                    </ul>
                    <p className="text-ink-dim">
                      Apply will overwrite their comments. Confirm before
                      submitting if you intend to.
                    </p>
                  </div>
                </div>
              )}
              <Button
                type="button"
                onClick={() => {
                  if (conflicts.length > 0) {
                    setConfirmOverwriteOpen(true);
                  } else {
                    submit.mutate();
                  }
                }}
                disabled={picked.length === 0}
                loading={submit.isPending}
                variant="primary"
                size="lg"
                fullWidth
                leadingIcon={<PlayCircle size={14} />}
              >
                {submit.isPending ? "Starting…" : "Start run"}
              </Button>
              {picked.length === 0 && (
                <p className="text-[11px] text-ink-dim">
                  Pick at least one schema to enable the start button.
                </p>
              )}
              <AlertDialog
                open={confirmOverwriteOpen}
                onClose={() => setConfirmOverwriteOpen(false)}
                onConfirm={() => {
                  setConfirmOverwriteOpen(false);
                  submit.mutate();
                }}
                loading={submit.isPending}
                title={`Run on ${conflicts.length} asset${conflicts.length === 1 ? "" : "s"} someone else applied?`}
                description={
                  conflicts.length === 1
                    ? `${conflicts[0].applied_by || "another user"} last applied ${conflicts[0].schema_name}.${conflicts[0].table_name}${conflicts[0].column_name ? "." + conflicts[0].column_name : ""}. The run regenerates suggestions; the live comment only changes when you Apply afterwards.`
                    : `Multiple teammates' work is in scope. The run regenerates suggestions; the live comments only change when you Apply afterwards. Cancel here to narrow the picked tables first.`
                }
                confirmLabel="Run anyway"
              />
            </CardBody>
          </Card>
        </div>
      )}
    </>
  );
}

function SchemaTablePicker({
  schema,
  selected,
  onChange,
  attribution,
  currentUser,
  currentHost,
}: {
  schema: string;
  selected: string[];
  onChange: (tables: string[]) => void;
  /** ``schema.table`` -> latest apply event lookup. Empty when the
   *  history store has no events for the active profile. */
  attribution?: Map<string, ApplyAttribution>;
  currentUser?: string | null;
  currentHost?: string | null;
}) {
  const scope = useRunScope();
  const assets = useQuery({
    queryKey: [
      "live-assets",
      scope?.profile ?? "",
      scope?.database ?? "",
      scope?.catalog ?? "",
      schema,
    ],
    queryFn: () => api.liveAssets(scope!, schema),
    enabled: !!scope,
  });
  if (assets.isLoading) {
    return (
      <div className="space-y-1.5 px-8 pb-3">
        <Skeleton className="h-3 w-1/4" />
        <div className="flex flex-wrap gap-1.5">
          {Array.from({ length: 6 }).map((_, i) => (
            <Skeleton key={i} className="h-5 w-20" />
          ))}
        </div>
      </div>
    );
  }
  if (assets.error) {
    return (
      <div className="px-8 pb-3 text-xs text-critical">
        {(assets.error as Error).message}
      </div>
    );
  }
  if (!assets.data?.assets?.length) {
    return <div className="px-8 pb-3 text-xs text-ink-dim">(empty)</div>;
  }

  function toggle(name: string) {
    if (selected.includes(name)) {
      onChange(selected.filter((t) => t !== name));
    } else {
      onChange([...selected, name]);
    }
  }
  function selectAll() {
    onChange([]);
  }

  return (
    <div className="space-y-2 px-8 pb-3">
      <div className="flex items-center gap-2 text-[11px] text-ink-dim">
        <button
          type="button"
          onClick={selectAll}
          className="rounded border border-border px-1.5 py-0.5 hover:bg-surface-subtle"
        >
          all tables
        </button>
        <span>
          {selected.length === 0
            ? `every table (${assets.data.assets.length})`
            : `${selected.length} of ${assets.data.assets.length} selected`}
        </span>
      </div>
      <div className="flex flex-wrap gap-1.5">
        {assets.data.assets.map((asset) => {
          const on = selected.length === 0 || selected.includes(asset.name);
          const attr = attribution?.get(`${schema}.${asset.name}`);
          const hasComment = !!asset.comment?.trim();
          // Conflict iff a foreign user last applied this asset.
          // Same user + same host (when known) reads as "yours" so
          // re-running your own previous output doesn't fire the
          // warning chip.
          const isMine =
            !!attr &&
            !!currentUser &&
            attr.applied_by === currentUser &&
            (!currentHost || !attr.hostname || attr.hostname === currentHost);
          const isConflict = !!attr && !isMine;
          return (
            <button
              key={`${schema}.${asset.name}`}
              type="button"
              onClick={() => toggle(asset.name)}
              title={
                attr
                  ? isMine
                    ? `You applied this on ${new Date(attr.applied_at * 1000).toLocaleDateString()}.`
                    : `${attr.applied_by || "another user"} last applied this on ${new Date(attr.applied_at * 1000).toLocaleDateString()} -- run will offer to overwrite.`
                  : hasComment
                    ? "Already has a live-DB comment from outside AMX."
                    : "No comment yet on this asset."
              }
              className={cn(
                "inline-flex items-center gap-1 rounded-md border px-2 py-0.5 font-mono text-[11px] transition-colors duration-fast",
                on
                  ? "border-accent/40 bg-accent-soft/40 text-ink"
                  : "border-border text-ink-dim hover:border-accent/40 hover:text-ink",
                isConflict && on && "border-warning/60 bg-warning-soft/30",
              )}
            >
              <span>{asset.name}</span>
              {isConflict && (
                <Badge tone="warning">{attr?.applied_by || "other"}</Badge>
              )}
              {!isConflict && isMine && (
                <Badge tone="info">you</Badge>
              )}
              {!attr && hasComment && (
                <Badge tone="neutral">commented</Badge>
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
}

interface DbProfileSummary {
  name: string;
  backend: string;
  database: string;
  catalog: string;
}

function ScopePicker() {
  // Inline picker shown when the user lands on /runs/new without a
  // scope query string (e.g. via the "+ New run" button on the Runs
  // list, or by typing the URL). Without this, the page used to dump
  // a "scope is encoded in the URL" message that didn't help anyone
  // who didn't know which sidebar link to follow.
  const navigate = useNavigate();
  const [profile, setProfile] = useState<string>("");

  const profiles = useQuery({
    queryKey: ["profiles", "db"],
    queryFn: () =>
      apiFetch<{ profiles: DbProfileSummary[]; active: string | null }>(
        "/api/profiles/db",
      ),
    retry: false,
  });

  const selected = useMemo(
    () => profiles.data?.profiles?.find((p) => p.name === profile) ?? null,
    [profiles.data, profile],
  );
  // Catalog backends (Databricks, BigQuery) live under /api/live/catalogs;
  // every other backend uses /api/live/databases. Switching on backend
  // up front avoids a wasted round trip.
  const isCatalogBackend =
    selected != null &&
    ["databricks", "bigquery"].includes(selected.backend);

  const catalogs = useQuery({
    queryKey: ["live", "catalogs", profile],
    queryFn: () =>
      apiFetch<{ catalogs: string[]; active_catalog: string | null }>(
        `/api/live/catalogs?profile=${encodeURIComponent(profile)}`,
      ),
    enabled: !!profile && isCatalogBackend,
    retry: false,
  });
  const databases = useQuery({
    queryKey: ["live", "databases", profile],
    queryFn: () =>
      apiFetch<{ databases: string[]; active_database: string | null }>(
        `/api/live/databases?profile=${encodeURIComponent(profile)}`,
      ),
    enabled: !!profile && !isCatalogBackend,
    retry: false,
  });

  const goto = (target: string) => {
    const param = isCatalogBackend ? "catalog" : "database";
    navigate(
      `/runs/new?profile=${encodeURIComponent(profile)}&${param}=${encodeURIComponent(target)}`,
    );
  };

  const profileList = profiles.data?.profiles ?? [];
  const items = isCatalogBackend
    ? catalogs.data?.catalogs ?? []
    : databases.data?.databases ?? [];
  const itemsLoading = isCatalogBackend
    ? catalogs.isLoading
    : databases.isLoading;
  const itemsError = isCatalogBackend ? catalogs.error : databases.error;

  return (
    <Card>
      <CardHeader
        title="Pick a scope to start a run"
        description="A run analyses one database or catalog at a time. Pick a DB profile, then the database / catalog under it — schemas to enumerate appear next."
      />
      <CardBody className="space-y-4">
        <div>
          <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-ink-dim">
            DB profile
          </label>
          {profiles.isLoading ? (
            <Skeleton className="h-9 w-full" />
          ) : profileList.length === 0 ? (
            <p className="text-sm text-ink-muted">
              No DB profiles configured.{" "}
              <Link to="/settings?tab=db" className="text-accent underline">
                Add one in Settings →
              </Link>
            </p>
          ) : (
            <div className="flex flex-wrap gap-1.5">
              {profileList.map((p) => (
                <button
                  key={p.name}
                  type="button"
                  onClick={() => setProfile(p.name)}
                  className={
                    "rounded-md border px-2.5 py-1 text-xs font-mono transition " +
                    (p.name === profile
                      ? "border-accent bg-accent-soft text-accent-ink"
                      : "border-surface-border bg-surface text-ink-muted hover:border-accent/40")
                  }
                  title={`${p.backend} · ${p.database || p.catalog || "no default db"}`}
                >
                  {p.name}
                  <span className="ml-1.5 text-[10px] text-ink-dim">
                    {p.backend}
                  </span>
                </button>
              ))}
            </div>
          )}
        </div>

        {profile && (
          <div>
            <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-ink-dim">
              {isCatalogBackend ? "Catalog" : "Database"}
            </label>
            {itemsLoading ? (
              <Skeleton className="h-9 w-full" />
            ) : itemsError ? (
              <p className="text-sm text-critical">
                {(itemsError as Error).message}
              </p>
            ) : items.length === 0 ? (
              <p className="text-sm text-ink-muted">
                No {isCatalogBackend ? "catalogs" : "databases"} reachable on{" "}
                <span className="font-mono">{profile}</span>. Check the
                connection in Settings →&nbsp;DB.
              </p>
            ) : (
              <div className="flex flex-wrap gap-1.5">
                {items.map((item) => (
                  <button
                    key={item}
                    type="button"
                    onClick={() => goto(item)}
                    className="rounded-md border border-surface-border bg-surface px-2.5 py-1 text-xs font-mono text-ink-muted transition hover:border-accent hover:bg-accent-soft hover:text-accent-ink"
                  >
                    {item}
                  </button>
                ))}
              </div>
            )}
          </div>
        )}
      </CardBody>
    </Card>
  );
}
