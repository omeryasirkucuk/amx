import { useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { ChevronDown, PlayCircle, Settings as SettingsIcon } from "lucide-react";

import { api, apiFetch } from "../lib/api";
import type { LLMOverrides, LLMProfileDefaults } from "../lib/api";
import { cn } from "../lib/cn";
import type { Scope } from "../lib/scope";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import { Button, Field, InfoHint, Skeleton, Switch, useToast } from "../components/ui";

interface SchemaPickState {
  schema: string;
  tables: string[]; // empty = "every reachable table"
}

/** Mutable form-state shape for the "Advanced LLM settings" disclosure.
 *  Strings everywhere because every input is uncontrolled-friendly:
 *  the user can clear a field to empty, and we only forward the
 *  parsed numeric value when it actually differs from the saved
 *  profile default. Empty / unparseable / unchanged ⇒ no override. */
interface OverrideFormState {
  temperature: string;
  maxTokens: string;
  nAlternatives: string;
  columnBatchSize: string;
  promptDetail: string;
  descriptionVerbosity: string;
  thinkingBudget: string;
  logprobHigh: string;
  logprobMedium: string;
  customInputCost: string;
  customOutputCost: string;
}

const EMPTY_OVERRIDES: OverrideFormState = {
  temperature: "",
  maxTokens: "",
  nAlternatives: "",
  columnBatchSize: "",
  promptDetail: "",
  descriptionVerbosity: "",
  thinkingBudget: "",
  logprobHigh: "",
  logprobMedium: "",
  customInputCost: "",
  customOutputCost: "",
};

/** Coerce a stringy form value into a numeric override only when the
 *  user actually typed something new. Returns ``undefined`` to mean
 *  "no override" (use the saved profile's value). */
function pickNumber(raw: string, profileValue: number | null | undefined): number | undefined {
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed)) return undefined;
  if (profileValue !== null && profileValue !== undefined && parsed === profileValue) {
    return undefined;
  }
  return parsed;
}

function pickString(raw: string, profileValue: string | null | undefined): string | undefined {
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  if (profileValue && trimmed === profileValue) return undefined;
  return trimmed;
}

/** Build the ``llm_overrides`` payload from the form state. Returns
 *  ``undefined`` when the user has not changed any field, so the
 *  request body stays byte-identical to the pre-overrides shape in
 *  the common case. */
function buildOverridesPayload(
  form: OverrideFormState,
  defaults: LLMProfileDefaults | null,
): LLMOverrides | undefined {
  const out: LLMOverrides = {};
  const temperature = pickNumber(form.temperature, defaults?.temperature);
  if (temperature !== undefined) out.temperature = temperature;
  const maxTokens = pickNumber(form.maxTokens, defaults?.max_tokens);
  if (maxTokens !== undefined) out.max_tokens = maxTokens;
  const nAlt = pickNumber(form.nAlternatives, defaults?.n_alternatives);
  if (nAlt !== undefined) out.n_alternatives = nAlt;
  const columnBatch = pickNumber(form.columnBatchSize, defaults?.column_batch_size);
  if (columnBatch !== undefined) out.column_batch_size = columnBatch;
  const promptDetail = pickString(form.promptDetail, defaults?.prompt_detail);
  if (promptDetail !== undefined) out.prompt_detail = promptDetail;
  const verbosity = pickString(form.descriptionVerbosity, defaults?.description_verbosity);
  if (verbosity !== undefined) out.description_verbosity = verbosity;
  const thinking = pickNumber(form.thinkingBudget, defaults?.thinking_budget);
  if (thinking !== undefined) out.thinking_budget = thinking;
  const high = pickNumber(form.logprobHigh, defaults?.logprob_high);
  if (high !== undefined) out.logprob_high = high;
  const medium = pickNumber(form.logprobMedium, defaults?.logprob_medium);
  if (medium !== undefined) out.logprob_medium = medium;
  const inputCost = pickNumber(form.customInputCost, defaults?.custom_input_cost_per_mtok);
  if (inputCost !== undefined) out.custom_input_cost_per_mtok = inputCost;
  const outputCost = pickNumber(form.customOutputCost, defaults?.custom_output_cost_per_mtok);
  if (outputCost !== undefined) out.custom_output_cost_per_mtok = outputCost;
  return Object.keys(out).length === 0 ? undefined : out;
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
  const [overrides, setOverrides] = useState<OverrideFormState>(EMPTY_OVERRIDES);
  const [advancedOpen, setAdvancedOpen] = useState(false);

  const ctx = useQuery({ queryKey: ["context"], queryFn: () => api.context() });
  const supportsBatch = !!ctx.data?.llm_supports_batch;
  const llmProvider = ctx.data?.llm_provider ?? null;
  const profileDefaults = ctx.data?.llm_profile_defaults ?? null;
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
        llm_overrides: buildOverridesPayload(overrides, profileDefaults),
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
              <AdvancedLLMOverrides
                open={advancedOpen}
                onToggle={() => setAdvancedOpen((v) => !v)}
                form={overrides}
                onChange={setOverrides}
                defaults={profileDefaults}
                profileName={ctx.data?.active_llm_profile ?? null}
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
              <Button
                type="button"
                onClick={() => submit.mutate()}
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
}: {
  schema: string;
  selected: string[];
  onChange: (tables: string[]) => void;
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
          return (
            <button
              key={`${schema}.${asset.name}`}
              type="button"
              onClick={() => toggle(asset.name)}
              className={cn(
                "rounded-md border px-2 py-0.5 font-mono text-[11px] transition-colors duration-fast",
                on
                  ? "border-accent/40 bg-accent-soft/40 text-ink"
                  : "border-border text-ink-dim hover:border-accent/40 hover:text-ink",
              )}
            >
              {asset.name}
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

interface AdvancedLLMOverridesProps {
  open: boolean;
  onToggle: () => void;
  form: OverrideFormState;
  onChange: (next: OverrideFormState) => void;
  defaults: LLMProfileDefaults | null;
  profileName: string | null;
}

/** Disclosure block on /runs/new exposing every LLM-profile tuning
 *  knob as a per-run override. Profile defaults pre-fill via the
 *  `Profile: …` hint next to each input. Empty input = use profile
 *  default; the parent only forwards changed values to the backend.
 *  The saved profile is never mutated. */
function AdvancedLLMOverrides({
  open,
  onToggle,
  form,
  onChange,
  defaults,
  profileName,
}: AdvancedLLMOverridesProps) {
  const update = (patch: Partial<OverrideFormState>) =>
    onChange({ ...form, ...patch });
  const overrideCount = useMemo(() => {
    let n = 0;
    if (pickNumber(form.temperature, defaults?.temperature) !== undefined) n++;
    if (pickNumber(form.maxTokens, defaults?.max_tokens) !== undefined) n++;
    if (pickNumber(form.nAlternatives, defaults?.n_alternatives) !== undefined) n++;
    if (pickNumber(form.columnBatchSize, defaults?.column_batch_size) !== undefined) n++;
    if (pickString(form.promptDetail, defaults?.prompt_detail) !== undefined) n++;
    if (pickString(form.descriptionVerbosity, defaults?.description_verbosity) !== undefined) n++;
    if (pickNumber(form.thinkingBudget, defaults?.thinking_budget) !== undefined) n++;
    if (pickNumber(form.logprobHigh, defaults?.logprob_high) !== undefined) n++;
    if (pickNumber(form.logprobMedium, defaults?.logprob_medium) !== undefined) n++;
    if (pickNumber(form.customInputCost, defaults?.custom_input_cost_per_mtok) !== undefined) n++;
    if (pickNumber(form.customOutputCost, defaults?.custom_output_cost_per_mtok) !== undefined) n++;
    return n;
  }, [form, defaults]);
  const profileBadge = (value: number | string | null | undefined): string =>
    value === null || value === undefined || value === "" ? "—" : String(value);

  return (
    <div className="rounded-md border border-border bg-surface-subtle/30">
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full items-center justify-between gap-2 px-3 py-2 text-left text-xs font-medium text-ink-muted hover:bg-surface-subtle/60"
        aria-expanded={open}
      >
        <span className="inline-flex items-center gap-1.5">
          <ChevronDown
            size={12}
            className={cn(
              "transition-transform duration-fast",
              !open && "-rotate-90",
            )}
            aria-hidden="true"
          />
          Advanced LLM settings
          <InfoHint text="Override the active LLM profile's tuning knobs for this run only. The saved profile is not mutated." />
        </span>
        <span className="text-[10.5px] uppercase tracking-wider text-ink-dim">
          {overrideCount > 0
            ? `${overrideCount} override${overrideCount > 1 ? "s" : ""}`
            : "profile defaults"}
        </span>
      </button>
      {open && (
        <div className="space-y-4 border-t border-border px-3 py-3">
          <p className="text-[11px] text-ink-dim">
            Profile{" "}
            <span className="font-mono text-ink-muted">
              {profileName ?? "—"}
            </span>{" "}
            is the source of truth — empty fields fall back to its
            current values. Type a new value to override for this run
            only.
          </p>

          <h4 className="text-[10.5px] font-semibold uppercase tracking-wider text-ink-dim">
            Generation
          </h4>
          <div className="grid grid-cols-2 gap-3">
            <Field
              label="Temperature (0.0–2.0)"
              hint={`Profile: ${profileBadge(defaults?.temperature)}`}
              description="Creativity: low = consistent, high = varied (0.1–0.3 recommended)."
            >
              <input
                type="number"
                min={0}
                max={2}
                step={0.05}
                placeholder={profileBadge(defaults?.temperature)}
                value={form.temperature}
                onChange={(e) => update({ temperature: e.target.value })}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
            <Field
              label="Max output tokens"
              hint={`Profile: ${profileBadge(defaults?.max_tokens)}`}
              description="Output budget per LLM call. Reasoning models auto-tune a 32k floor on top. Higher = bigger answers + higher cost."
            >
              <input
                type="number"
                min={256}
                max={262_144}
                step={1024}
                placeholder={profileBadge(defaults?.max_tokens)}
                value={form.maxTokens}
                onChange={(e) => update({ maxTokens: e.target.value })}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
            <Field
              label="Alternatives per column (1–5)"
              hint={`Profile: ${profileBadge(defaults?.n_alternatives)}`}
              description="How many alternative description proposals to generate per column."
            >
              <input
                type="number"
                min={1}
                max={5}
                step={1}
                placeholder={profileBadge(defaults?.n_alternatives)}
                value={form.nAlternatives}
                onChange={(e) => update({ nAlternatives: e.target.value })}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
            <Field
              label="Column batch size"
              hint={`Profile: ${profileBadge(defaults?.column_batch_size)}`}
              description="Columns processed per LLM call. Higher = cheaper; lower = more stable."
            >
              <input
                type="number"
                min={1}
                max={200}
                step={1}
                placeholder={profileBadge(defaults?.column_batch_size)}
                value={form.columnBatchSize}
                onChange={(e) => update({ columnBatchSize: e.target.value })}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
            <Field
              label="Prompt detail"
              hint={`Profile: ${profileBadge(defaults?.prompt_detail)}`}
              description="How much context the model receives. More = accurate; less = fast/cheap."
            >
              <select
                value={form.promptDetail || (defaults?.prompt_detail ?? "")}
                onChange={(e) => update({ promptDetail: e.target.value })}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm"
              >
                {["minimal", "standard", "detailed", "full"].map((v) => (
                  <option key={v} value={v}>
                    {v}
                  </option>
                ))}
              </select>
            </Field>
            <Field
              label="Description verbosity"
              hint={`Profile: ${profileBadge(defaults?.description_verbosity)}`}
              description="Output length: brief = one sentence, exhaustive = detailed."
            >
              <select
                value={
                  form.descriptionVerbosity ||
                  (defaults?.description_verbosity ?? "")
                }
                onChange={(e) =>
                  update({ descriptionVerbosity: e.target.value })
                }
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm"
              >
                {["brief", "detailed", "comprehensive", "exhaustive"].map((v) => (
                  <option key={v} value={v}>
                    {v}
                  </option>
                ))}
              </select>
            </Field>
            <Field
              label="Thinking budget"
              hint={`Profile: ${profileBadge(defaults?.thinking_budget)}`}
              description="Token budget for the model's internal reasoning (Anthropic extended thinking + similar). 0 = off."
            >
              <input
                type="number"
                min={0}
                max={64_000}
                step={256}
                placeholder={profileBadge(defaults?.thinking_budget)}
                value={form.thinkingBudget}
                onChange={(e) => update({ thinkingBudget: e.target.value })}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
          </div>

          <h4 className="border-t border-border pt-3 text-[10.5px] font-semibold uppercase tracking-wider text-ink-dim">
            Confidence thresholds
          </h4>
          <div className="grid grid-cols-2 gap-3">
            <Field
              label="High (≥)"
              hint={`Profile: ${profileBadge(defaults?.logprob_high)}`}
              description="Predictions above this score are flagged 'high confidence'."
            >
              <input
                type="number"
                min={0}
                max={1}
                step={0.05}
                placeholder={profileBadge(defaults?.logprob_high)}
                value={form.logprobHigh}
                onChange={(e) => update({ logprobHigh: e.target.value })}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
            <Field
              label="Medium (≥)"
              hint={`Profile: ${profileBadge(defaults?.logprob_medium)}`}
              description="Above this is 'medium confidence'; below counts as 'low'."
            >
              <input
                type="number"
                min={0}
                max={1}
                step={0.05}
                placeholder={profileBadge(defaults?.logprob_medium)}
                value={form.logprobMedium}
                onChange={(e) => update({ logprobMedium: e.target.value })}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
          </div>

          <h4 className="border-t border-border pt-3 text-[10.5px] font-semibold uppercase tracking-wider text-ink-dim">
            Cost overrides
          </h4>
          <p className="text-[11px] text-ink-dim">
            Reporting only — does not affect the LLM call. Both rates
            must be set together, or both blank.
          </p>
          <div className="grid grid-cols-2 gap-3">
            <Field
              label="Custom input cost (USD / 1M tokens)"
              hint={`Profile: ${profileBadge(defaults?.custom_input_cost_per_mtok)}`}
            >
              <input
                type="number"
                min={0}
                step={0.01}
                placeholder={profileBadge(defaults?.custom_input_cost_per_mtok)}
                value={form.customInputCost}
                onChange={(e) => update({ customInputCost: e.target.value })}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
            <Field
              label="Custom output cost (USD / 1M tokens)"
              hint={`Profile: ${profileBadge(defaults?.custom_output_cost_per_mtok)}`}
            >
              <input
                type="number"
                min={0}
                step={0.01}
                placeholder={profileBadge(defaults?.custom_output_cost_per_mtok)}
                value={form.customOutputCost}
                onChange={(e) => update({ customOutputCost: e.target.value })}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
          </div>

          {overrideCount > 0 && (
            <button
              type="button"
              onClick={() => onChange(EMPTY_OVERRIDES)}
              className="text-[11px] text-accent underline-offset-2 hover:underline"
            >
              Reset all overrides
            </button>
          )}
        </div>
      )}
    </div>
  );
}
