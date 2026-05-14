import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { PlayCircle, Settings as SettingsIcon } from "lucide-react";

import { api, apiFetch } from "../lib/api";
import type { Scope } from "../lib/scope";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import { Button, InfoHint, Skeleton, Switch, useToast } from "../components/ui";
import AdvancedLLMOverrides, {
  EMPTY_OVERRIDES,
  buildOverridesPayload,
  seedFromDefaults,
  type OverrideFormState,
} from "../components/AdvancedLLMOverrides";

import ScopeTree, { type SchemaPick } from "../components/ScopeTree";

const CATALOG_BACKENDS = new Set(["databricks", "bigquery"]);

/** One profile selection block — Profile + Database + tree of picks.
 *  A run can stack any number of these; the SPA fans them out to
 *  N parallel ``/api/runs`` calls on submit, so one "Start run"
 *  click can spread metadata work across multiple databases. */
interface ProfileSelection {
  /** Local-only stable key for React. */
  id: string;
  profile: string;
  /** Database or catalog name, depending on backend. */
  database: string;
  isCatalogBackend: boolean;
  picks: SchemaPick[];
}

function freshSelection(seed?: {
  profile?: string;
  database?: string;
  isCatalogBackend?: boolean;
}): ProfileSelection {
  return {
    id: Math.random().toString(36).slice(2, 10),
    profile: seed?.profile ?? "",
    database: seed?.database ?? "",
    isCatalogBackend: Boolean(seed?.isCatalogBackend),
    picks: [],
  };
}

interface SelectionBlockProps {
  selection: ProfileSelection;
  index: number;
  canRemove: boolean;
  profiles: DbProfileSummary[];
  profileByName: Map<string, DbProfileSummary>;
  onUpdate: (patch: Partial<ProfileSelection>) => void;
  onRemove: () => void;
}

function SelectionBlock({
  selection,
  index,
  canRemove,
  profiles,
  profileByName,
  onUpdate,
  onRemove,
}: SelectionBlockProps) {
  const selectedProfile = profileByName.get(selection.profile);
  const isCatalog = Boolean(
    selectedProfile && CATALOG_BACKENDS.has(selectedProfile.backend),
  );

  // Live databases / catalogs for the picked profile. Drives the
  // second dropdown. Auto-picks the only available option to spare
  // the user a click on single-DB profiles like Postgres.
  const databasesQ = useQuery({
    queryKey: ["run-sel-dbs", selection.profile, isCatalog],
    queryFn: () => {
      const path = isCatalog
        ? `/api/live/catalogs?profile=${encodeURIComponent(selection.profile)}`
        : `/api/live/databases?profile=${encodeURIComponent(selection.profile)}`;
      return apiFetch<{ databases?: string[]; catalogs?: string[] }>(path);
    },
    enabled: Boolean(selection.profile),
    // Errors render inline as a chip below the select.
    meta: { silentError: true },
  });
  const options = isCatalog
    ? databasesQ.data?.catalogs ?? []
    : databasesQ.data?.databases ?? [];

  // Auto-pick the only available DB when the profile changes.
  useEffect(() => {
    if (!selection.database && options.length === 1) {
      onUpdate({ database: options[0], isCatalogBackend: isCatalog });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [options.length]);

  return (
    <div className="rounded-md border border-border bg-surface-muted/40 p-3">
      <div className="mb-2 flex items-center justify-between">
        <span className="text-xs font-medium uppercase tracking-wider text-ink-dim">
          Selection {index + 1}
        </span>
        {canRemove && (
          <button
            type="button"
            onClick={onRemove}
            className="rounded border border-border px-2 py-0.5 text-xs text-ink-dim hover:border-critical/40 hover:text-critical"
          >
            Remove
          </button>
        )}
      </div>
      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
        <label className="flex flex-col gap-1 text-xs">
          <span className="text-ink-dim">DB profile</span>
          <select
            className="h-8 rounded-md border border-border bg-surface-raised px-2 text-sm text-ink"
            value={selection.profile}
            onChange={(e) => {
              const next = profileByName.get(e.target.value);
              const nextIsCatalog = Boolean(
                next && CATALOG_BACKENDS.has(next.backend),
              );
              onUpdate({
                profile: e.target.value,
                database: "",
                isCatalogBackend: nextIsCatalog,
                picks: [],
              });
            }}
          >
            <option value="">— pick a profile —</option>
            {profiles.map((p) => (
              <option key={p.name} value={p.name}>
                {p.name} · {p.backend}
              </option>
            ))}
          </select>
        </label>
        <label className="flex flex-col gap-1 text-xs">
          <span className="text-ink-dim">{isCatalog ? "Catalog" : "Database"}</span>
          <select
            className="h-8 rounded-md border border-border bg-surface-raised px-2 text-sm text-ink disabled:opacity-50"
            value={selection.database}
            onChange={(e) =>
              onUpdate({
                database: e.target.value,
                isCatalogBackend: isCatalog,
                picks: [],
              })
            }
            disabled={!selection.profile || databasesQ.isLoading}
          >
            {!selection.profile && <option value="">pick a profile first</option>}
            {selection.profile && databasesQ.isLoading && (
              <option value="">loading…</option>
            )}
            {selection.profile && !databasesQ.isLoading && options.length === 0 && (
              <option value="">(none visible)</option>
            )}
            {options.map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
          {databasesQ.isError && (
            <span className="mt-1 flex items-center gap-1 text-[11px] text-critical">
              <span className="truncate">
                {databasesQ.error instanceof Error
                  ? databasesQ.error.message
                  : `Couldn't load ${isCatalog ? "catalogs" : "databases"}.`}
              </span>
              <button
                type="button"
                onClick={() => databasesQ.refetch()}
                className="ml-auto rounded border border-critical/40 px-1.5 py-0.5 text-[10px] text-critical hover:bg-critical/10"
              >
                Retry
              </button>
            </span>
          )}
        </label>
      </div>
      <div className="mt-3">
        <ScopeTree
          dbProfile={selection.profile}
          database={selection.database}
          isCatalogBackend={selection.isCatalogBackend || isCatalog}
          picks={selection.picks}
          onChange={(picks) => onUpdate({ picks })}
        />
      </div>
    </div>
  );
}

/** Build the ``column_overrides`` dict the /api/runs body now accepts
 * from a tree of {schema, tables: [{table, columns}]} picks. Returns
 * undefined when no column-level restriction is present, so callers
 * can ``column_overrides: buildColumnOverrides(picks) ?? undefined``.
 */
function buildColumnOverrides(
  picks: SchemaPick[],
): Record<string, string[]> | undefined {
  const out: Record<string, string[]> = {};
  for (const p of picks) {
    for (const t of p.tables) {
      if (t.columns.length === 0) continue;
      out[`${p.schema}.${t.table}`] = t.columns;
    }
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/** Build the legacy ``scope: {schema: [tables]}`` dict the run worker
 * still expects. Column-level picks collapse to their owning table
 * (``[t1, t2]`` rather than every table in the schema), and a schema
 * with no specific tables picked round-trips as ``[]`` (= every
 * reachable table under that schema). */
function buildScopeDict(picks: SchemaPick[]): Record<string, string[]> {
  const out: Record<string, string[]> = {};
  for (const p of picks) {
    out[p.schema] = p.tables.map((t) => t.table);
  }
  return out;
}

// OverrideFormState, EMPTY_OVERRIDES, seedFromDefaults, pickNumber,
// pickString, buildOverridesPayload, and the AdvancedLLMOverrides
// component itself now live in
// ``frontend/src/components/AdvancedLLMOverrides.tsx`` so the Re-Run
// modal can mount the same form block. RunNew imports them at the
// top of this file.

interface DbProfileSummary {
  name: string;
  backend: string;
  host: string;
  database: string;
  catalog: string;
  is_active: boolean;
}

interface DbProfilesListResponse {
  profiles: DbProfileSummary[];
  count: number;
}

/** Read scope from `?profile=…&database=…` (or `&catalog=…`) — the
 * Database/Schema/Table pages link here with the scope encoded.
 *
 * When the user opens ``/runs/new`` directly (top-nav, bookmark) the
 * URL carries no scope. In that case we derive scope from the active
 * profile's pinned catalog / database so the page is immediately
 * usable AND the query keys match what the Sidebar already populated
 * (sharing the live-schemas cache, no second fetch).
 */
function useRunScope(): Scope | null {
  const [params] = useSearchParams();
  const profile = params.get("profile") || "";
  const database = params.get("database") || "";
  const catalog = params.get("catalog") || "";

  // Fetch the profiles list only when the URL has no profile param.
  // Otherwise we trust the URL — the user clicked a sidebar entry and
  // we should honour that scope verbatim, even if it differs from the
  // active profile's pin.
  const fallback = useQuery({
    queryKey: ["profiles", "db"],
    queryFn: () => apiFetch<DbProfilesListResponse>("/api/profiles/db"),
    retry: false,
    enabled: !profile,
  });

  if (profile) {
    if (catalog) return { profile, catalog, kind: "catalog" };
    if (database) return { profile, database, kind: "database" };
    return null;
  }

  const active = fallback.data?.profiles.find((p) => p.is_active);
  if (!active) return null;
  if (active.catalog) {
    return { profile: active.name, catalog: active.catalog, kind: "catalog" };
  }
  if (active.database) {
    return { profile: active.name, database: active.database, kind: "database" };
  }
  // Active profile has no pinned catalog or database. Returning a
  // half-formed scope here (the old behaviour) sent ``catalog=None``
  // to ``/api/live/schemas/.../assets`` and produced a Databricks
  // ``[NO_SUCH_CATALOG_EXCEPTION] Catalog 'none' was not found`` on
  // every asset expand. Returning ``null`` instead surfaces the
  // inline ``ScopePicker`` below so the user explicitly picks a
  // profile + catalog / database before any live-DB call fires.
  return null;
}

export default function RunNew() {
  const navigate = useNavigate();
  const toast = useToast();
  const scope = useRunScope();
  // Multi-profile selections. The first entry seeds itself from the
  // URL scope (``?profile=…&database=…``) so existing deep-links from
  // sidebar / Browse pages keep working. The user can append further
  // selections via "Add profile"; each fires its own /api/runs call
  // on submit, so one click can spread metadata work across N DBs.
  const [selections, setSelections] = useState<ProfileSelection[]>(() => {
    if (scope) {
      return [
        freshSelection({
          profile: scope.profile,
          database: scope.database ?? scope.catalog ?? "",
          isCatalogBackend: scope.kind === "catalog",
        }),
      ];
    }
    return [freshSelection()];
  });
  // RunNew stays mounted across the inline-picker → scoped-form transition
  // (same /runs/new route, only the query string changes), so the useState
  // initializer above only sees the URL state at first mount. When the
  // user picks a profile + database via ScopePicker, navigate() updates
  // the URL params but selections is still the empty placeholder from
  // first mount — leaving them looking at a fresh "pick a profile" form.
  // Seed the first selection once the URL scope materialises.
  const scopeSeededRef = useRef(false);
  const scopeProfile = scope?.profile ?? "";
  const scopeDatabase = scope?.database ?? scope?.catalog ?? "";
  const scopeIsCatalog = scope?.kind === "catalog";
  useEffect(() => {
    if (scopeSeededRef.current) return;
    if (!scopeProfile) return;
    scopeSeededRef.current = true;
    setSelections((curr) => {
      const first = curr[0];
      if (first && (first.profile || first.database)) return curr;
      const seeded = freshSelection({
        profile: scopeProfile,
        database: scopeDatabase,
        isCatalogBackend: scopeIsCatalog,
      });
      return curr.length === 0 ? [seeded] : [seeded, ...curr.slice(1)];
    });
  }, [scopeProfile, scopeDatabase, scopeIsCatalog]);
  const [missingOnly, setMissingOnly] = useState(false);
  const [autoApply, setAutoApply] = useState(false);
  const [batchMode, setBatchMode] = useState(false);
  const [overrides, setOverrides] = useState<OverrideFormState>(EMPTY_OVERRIDES);
  const [advancedOpen, setAdvancedOpen] = useState(false);

  const ctx = useQuery({ queryKey: ["context"], queryFn: () => api.context() });
  const supportsBatch = !!ctx.data?.llm_supports_batch;
  const llmProvider = ctx.data?.llm_provider ?? null;
  const profileDefaults = ctx.data?.llm_profile_defaults ?? null;
  const llmModel = ctx.data?.llm_model ?? null;
  const activeLlmProfile = ctx.data?.active_llm_profile ?? null;
  // Resolve the live (provider, model) price so the "Cost overrides"
  // section can show the auto-detected LiteLLM/OpenRouter rate as the
  // default badge instead of "—". The profile's stored
  // ``custom_*_cost_per_mtok`` still wins when set; this is the
  // fallback for the common "no override" case.
  const livePrice = useQuery({
    queryKey: ["pricing", "model", llmProvider, llmModel, activeLlmProfile],
    queryFn: () =>
      api.lookupPrice(
        llmProvider!,
        llmModel!,
        activeLlmProfile ?? undefined,
      ),
    enabled: !!llmProvider && !!llmModel,
    staleTime: 5 * 60_000, // 5 min — matches Settings.tsx behaviour
  });

  // Seed the override form with the active profile's values the
  // first time they arrive so every input shows a real starting
  // point ("0.20", "16384", "standard") instead of a row of empty
  // boxes. Subsequent profile-defaults reads are ignored —
  // a React Query refetch must not stomp the user's already-typed
  // overrides mid-session.
  const seededRef = useRef(false);
  useEffect(() => {
    if (seededRef.current || !profileDefaults) return;
    seededRef.current = true;
    setOverrides(seedFromDefaults(profileDefaults));
  }, [profileDefaults]);
  // Pre-flight gate: the worker fails fast in _run_worker_body when
  // cfg.llm.provider/model are missing, but the SPA shouldn't even
  // accept the click — the user only sees the error after the job
  // status flips to failed in the run detail. Surfacing the gap on
  // RunNew keeps the bad path out of history altogether.
  const llmReady = !!(
    ctx.data?.llm_provider && ctx.data?.llm_model && ctx.data?.active_llm_profile
  );


  const scopeUnavailable = !scope;

  // When the user types in the Scope search box we want to match
  // against asset names too, not just schema names. Tables are loaded
  // per-schema lazily, so kick off parallel asset fetches for every
  // schema only while the search is active. Results are cached so
  // subsequent keystrokes hit memory.
  // ScopeTree owns its own schema/table/column fetches now, so the
  // old per-schema asset-search machinery is gone. ``schemas`` is
  // still kept around for the "no schemas reachable" guard above.

  // DB profile catalog drives every per-selection Profile dropdown.
  const dbProfilesQ = useQuery({
    queryKey: ["profiles", "db"],
    queryFn: () => apiFetch<DbProfilesListResponse>("/api/profiles/db"),
  });
  const profileByName = useMemo(() => {
    const m = new Map<string, DbProfileSummary>();
    for (const p of dbProfilesQ.data?.profiles ?? []) m.set(p.name, p);
    return m;
  }, [dbProfilesQ.data]);

  function updateSelection(id: string, patch: Partial<ProfileSelection>) {
    setSelections((curr) =>
      curr.map((s) => (s.id === id ? { ...s, ...patch } : s)),
    );
  }

  function addSelection() {
    setSelections((curr) => [...curr, freshSelection()]);
  }

  function removeSelection(id: string) {
    setSelections((curr) =>
      curr.length <= 1 ? curr : curr.filter((s) => s.id !== id),
    );
  }

  const totalPicks = selections.reduce((n, s) => n + s.picks.length, 0);
  const canSubmit =
    llmReady &&
    selections.every((s) => s.profile && s.database) &&
    totalPicks > 0;

  const submit = useMutation({
    mutationFn: async () => {
      // Fan-out: fire one /api/runs per selection, parallel. Whichever
      // job_id comes back first wins the navigate(); the others still
      // exist as separate Runs rows so the user can see all N.
      const results = await Promise.all(
        selections.map((sel) =>
          api.submitRun({
            scope: buildScopeDict(sel.picks),
            column_overrides: buildColumnOverrides(sel.picks),
            apply: autoApply,
            missing_only: missingOnly,
            batch_mode: batchMode && supportsBatch,
            db_profile: sel.profile,
            database: sel.isCatalogBackend ? undefined : sel.database,
            catalog: sel.isCatalogBackend ? sel.database : undefined,
            llm_overrides: buildOverridesPayload(overrides, profileDefaults),
          }),
        ),
      );
      return results;
    },
    onSuccess: (results) => {
      toast.push({
        title:
          results.length === 1
            ? "Run started"
            : `${results.length} runs started`,
        description:
          results.length === 1
            ? `${totalPicks} ${totalPicks === 1 ? "schema" : "schemas"} queued.`
            : `${selections.length} profiles queued in parallel.`,
        tone: "success",
        duration: 2200,
      });
      navigate(`/runs/new-${results[0].job_id}`);
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
      selections.reduce(
        (acc, sel) =>
          acc +
          sel.picks.reduce(
            (n, p) => n + (p.tables.length === 0 ? 1 : p.tables.length),
            0,
          ),
        0,
      ),
    [selections],
  );

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
          <Card className="lg:flex lg:h-full lg:flex-col lg:self-stretch">
            <CardHeader
              title="Scope"
              description="Pick a DB profile, then drill into schemas / tables / columns. Add more profiles to spread a single run across multiple databases."
            />
            <CardBody className="space-y-4 p-3 lg:flex lg:min-h-0 lg:flex-1 lg:flex-col lg:overflow-y-auto">
              {selections.map((sel, idx) => (
                <SelectionBlock
                  key={sel.id}
                  selection={sel}
                  index={idx}
                  canRemove={selections.length > 1}
                  profiles={dbProfilesQ.data?.profiles ?? []}
                  profileByName={profileByName}
                  onUpdate={(patch) => updateSelection(sel.id, patch)}
                  onRemove={() => removeSelection(sel.id)}
                />
              ))}
              <button
                type="button"
                onClick={addSelection}
                className="inline-flex w-full items-center justify-center gap-2 rounded-md border border-dashed border-border px-3 py-2 text-sm text-ink-dim hover:border-accent/40 hover:text-ink"
              >
                + Add another profile
              </button>
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
              <AdvancedLLMOverrides
                open={advancedOpen}
                onToggle={() => setAdvancedOpen((v) => !v)}
                form={overrides}
                onChange={setOverrides}
                defaults={profileDefaults}
                profileName={ctx.data?.active_llm_profile ?? null}
                livePrice={livePrice.data ?? null}
                livePriceLoading={livePrice.isFetching}
              />
              <hr className="border-border" />
              <dl className="grid grid-cols-2 gap-y-1.5 text-xs">
                <dt className="text-ink-dim">Schemas</dt>
                <dd className="text-right font-mono tabular-nums text-ink">
                  {totalPicks}
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
              <Button
                type="button"
                onClick={() => submit.mutate()}
                disabled={!canSubmit}
                loading={submit.isPending}
                variant="primary"
                size="lg"
                fullWidth
                leadingIcon={<PlayCircle size={14} />}
              >
                {submit.isPending ? "Starting…" : "Start run"}
              </Button>
              {totalPicks === 0 && (
                <p className="text-[11px] text-ink-dim">
                  Pick at least one schema to enable the start button.
                </p>
              )}
            </CardBody>
          </Card>
        </div>
      )}
    </>
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

