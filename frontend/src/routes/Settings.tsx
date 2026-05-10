import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertCircle,
  CheckCircle,
  Code as CodeIcon,
  Database,
  FileText,
  Loader2,
  Pencil,
  Plus,
  Sparkles,
  Trash2,
} from "lucide-react";

import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import StatusPill from "../components/StatusPill";
import EmptyState from "../components/EmptyState";
import Modal from "../components/Modal";
import JobProgress from "../components/JobProgress";
import { api, apiFetch } from "../lib/api";
import { cn } from "../lib/cn";
import { InfoHint, Tabs, TabsList, Tab as TabTrigger, TabPanel } from "../components/ui";

type Tab = "db" | "llm" | "docs" | "code";

interface DbProfileSummary {
  name: string;
  backend: string;
  host: string;
  database: string;
  catalog: string;
  is_active: boolean;
}

interface LlmProfileSummary {
  name: string;
  provider: string;
  model: string;
  is_active: boolean;
}

interface DocProfile {
  name: string;
  paths: string[];
  is_active: boolean;
  linked_db_profiles?: string[];
}

interface CodeProfile {
  name: string;
  path: string;
  is_active: boolean;
  linked_db_profiles?: string[];
}

interface DbBackend {
  id: string;
  label: string;
  fields: string[];
  default_port?: number;
  supports_catalog?: boolean;
}

interface LlmProvider {
  id: string;
  label: string;
  needs_key: boolean;
  needs_base: boolean;
}

const TABS: Array<{ id: Tab; label: string; icon: typeof Database }> = [
  { id: "db", label: "Database", icon: Database },
  { id: "llm", label: "LLM", icon: Sparkles },
  { id: "docs", label: "Docs", icon: FileText },
  { id: "code", label: "Code", icon: CodeIcon },
];

const TAB_IDS: readonly Tab[] = ["db", "llm", "docs", "code"];

function isTab(value: string | null): value is Tab {
  return value !== null && (TAB_IDS as readonly string[]).includes(value);
}

export default function Settings() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = searchParams.get("tab");
  const tab: Tab = isTab(tabParam) ? tabParam : "db";
  const setTab = (next: Tab) => {
    setSearchParams(
      (prev) => {
        const sp = new URLSearchParams(prev);
        sp.set("tab", next);
        return sp;
      },
      { replace: true },
    );
  };
  return (
    <>
      <PageHeader
        title="Settings"
        breadcrumbs={[{ label: "Settings" }]}
      />
      <Tabs value={tab} onValueChange={(v) => setTab(v as Tab)}>
        <TabsList>
          {TABS.map((t) => (
            <TabTrigger
              key={t.id}
              value={t.id}
              icon={<t.icon size={13} />}
            >
              {t.label}
            </TabTrigger>
          ))}
        </TabsList>
        <TabPanel value="db">
          <DbProfilesSection />
        </TabPanel>
        <TabPanel value="llm">
          <LlmProfilesSection />
        </TabPanel>
        <TabPanel value="docs">
          <DocProfilesSection />
        </TabPanel>
        <TabPanel value="code">
          <CodeProfilesSection />
        </TabPanel>
      </Tabs>
    </>
  );
}

// ── DB profiles ───────────────────────────────────────────────────────

function DbProfilesSection() {
  const qc = useQueryClient();
  const [editing, setEditing] = useState<{ name: string | null } | null>(null);
  const [testResult, setTestResult] = useState<Record<string, { ok: boolean; message: string }>>({});

  const profiles = useQuery({
    queryKey: ["profiles", "db"],
    queryFn: () =>
      apiFetch<{ profiles: DbProfileSummary[]; count: number }>(
        "/api/profiles/db",
      ),
    retry: false,
  });

  // DB profile activation was retired in 0.13: every defined profile
  // is selectable from Run / Ask / Browse directly, so the list page
  // no longer renders an Active badge or an Activate button. The
  // mutation hook + ``is_active`` typings stay deleted here -- the
  // server still answers the legacy ``/activate`` route with 410 Gone
  // for any older bundle that races a refresh.

  const remove = useMutation({
    mutationFn: (name: string) =>
      apiFetch(`/api/profiles/db/${encodeURIComponent(name)}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["profiles", "db"] }),
  });

  const test = useMutation({
    mutationFn: async (name: string) => {
      const result = await apiFetch<{ ok: boolean; message: string }>(
        `/api/profiles/db/${encodeURIComponent(name)}/test`,
        { method: "POST" },
      );
      return { name, ...result };
    },
    onSuccess: (r) =>
      setTestResult((curr) => ({ ...curr, [r.name]: { ok: r.ok, message: r.message } })),
  });

  return (
    <>
      <Card>
        <CardHeader
          title={`${profiles.data?.count ?? 0} DB profile${profiles.data?.count === 1 ? "" : "s"}`}
          description="Every defined profile is selectable from Run, Ask, and Browse. There's no separate Activate step."
          actions={
            <button
              type="button"
              onClick={() => setEditing({ name: null })}
              className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90"
            >
              <Plus size={14} />
              Add
            </button>
          }
        />
        <CardBody className="p-0">
          {profiles.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading…</div>
          ) : profiles.data?.profiles.length ? (
            <ul className="divide-y divide-surface-border">
              {profiles.data.profiles.map((p) => {
                const result = testResult[p.name];
                return (
                  <li key={p.name} className="px-5 py-3 text-sm">
                    <div className="flex items-center justify-between gap-3">
                      <div className="min-w-0">
                        <div className="font-medium">{p.name}</div>
                        <div className="text-xs text-ink-dim">
                          <span className="font-mono">{p.backend}</span>
                          {p.host && <> · {p.host}</>}
                          {p.database && <> · {p.database}</>}
                          {p.catalog && <> · {p.catalog}</>}
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        {result && (
                          <div
                            title={result.message}
                            className={cn(
                              "inline-flex max-w-[260px] items-center gap-1.5 rounded-md px-2 py-1 text-[11px]",
                              result.ok
                                ? "bg-positive/10 text-positive"
                                : "bg-critical/10 text-critical",
                            )}
                          >
                            {result.ok ? (
                              <CheckCircle size={12} className="shrink-0" />
                            ) : (
                              <AlertCircle size={12} className="shrink-0" />
                            )}
                            <span className="truncate">
                              {result.message || (result.ok ? "OK" : "Failed")}
                            </span>
                          </div>
                        )}
                        <button
                          type="button"
                          onClick={() => test.mutate(p.name)}
                          disabled={test.isPending && test.variables === p.name}
                          className="inline-flex items-center gap-1.5 rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-surface-border disabled:opacity-60"
                        >
                          {test.isPending && test.variables === p.name ? (
                            <>
                              <Loader2 size={12} className="animate-spin" />
                              Testing…
                            </>
                          ) : (
                            "Connection test"
                          )}
                        </button>
                        <button
                          type="button"
                          onClick={() => setEditing({ name: p.name })}
                          className="rounded-md p-1 text-ink-dim hover:bg-surface-subtle hover:text-ink"
                          title="Edit"
                        >
                          <Pencil size={14} />
                        </button>
                        {/* Delete is unconditional now: there's no Active
                            profile to protect, and the API already
                            handles "last profile" + "first profile"
                            cases gracefully (the next defined profile
                            becomes the new CLI default-fallback). */}
                        {true && (
                          <button
                            type="button"
                            onClick={() => {
                              if (confirm(`Delete DB profile '${p.name}'?`)) remove.mutate(p.name);
                            }}
                            className="rounded-md p-1 text-ink-dim hover:bg-critical/10 hover:text-critical"
                            title="Delete"
                          >
                            <Trash2 size={14} />
                          </button>
                        )}
                      </div>
                    </div>
                  </li>
                );
              })}
            </ul>
          ) : (
            <EmptyState
              icon={Database}
              title="No DB profiles yet"
              description="Click Add to create your first one. The same wizard the CLI's /add-db-profile uses."
            />
          )}
        </CardBody>
      </Card>
      {editing && (
        <DbProfileWizard
          open
          editingName={editing.name}
          onClose={() => setEditing(null)}
        />
      )}
    </>
  );
}

const DB_FIELD_LABELS: Record<string, string> = {
  host: "Host",
  port: "Port",
  user: "User",
  password: "Password",
  database: "Database",
  catalog: "Catalog",
  account: "Snowflake account",
  warehouse: "Warehouse",
  role: "Role",
  http_path: "HTTP path",
  access_token: "Access token",
  project: "GCP project",
  dataset: "BigQuery dataset",
  credentials_path: "Credentials JSON path",
  service_name: "Oracle service name",
  driver: "ODBC driver",
  cluster_identifier: "Redshift cluster id",
  secure: "Use HTTPS",
};

const DB_SECRET_FIELDS = new Set(["password", "access_token"]);

function DbProfileWizard({
  open,
  onClose,
  editingName,
}: {
  open: boolean;
  onClose: () => void;
  editingName: string | null;
}) {
  const qc = useQueryClient();
  const isEdit = editingName != null;

  const backends = useQuery({
    queryKey: ["profiles", "db-backends"],
    queryFn: () => apiFetch<{ backends: DbBackend[] }>("/api/profiles/db/backends"),
  });
  const existing = useQuery({
    queryKey: ["profiles", "db", editingName],
    queryFn: () =>
      apiFetch<Record<string, unknown>>(`/api/profiles/db/${encodeURIComponent(editingName!)}`),
    enabled: isEdit,
  });

  const [name, setName] = useState(editingName ?? "");
  const [backend, setBackend] = useState<string>("postgresql");
  const [values, setValues] = useState<Record<string, string>>({});
  const [hydratedFor, setHydratedFor] = useState<string | null>(null);

  useEffect(() => {
    if (!isEdit || !existing.data) return;
    const data = existing.data as Record<string, unknown>;
    const dataName = typeof data.name === "string" ? data.name : null;
    if (dataName !== editingName) return;
    if (hydratedFor === editingName) return;
    setBackend(String(data.backend || "postgresql"));
    const next: Record<string, string> = {};
    for (const [k, v] of Object.entries(data)) {
      if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
        next[k] = String(v);
      }
    }
    setValues(next);
    setHydratedFor(editingName);
  }, [isEdit, existing.data, editingName, hydratedFor]);

  const chosenBackend = backends.data?.backends.find((b) => b.id === backend);
  const fields = chosenBackend?.fields ?? ["host", "port", "user", "password", "database"];

  const save = useMutation({
    mutationFn: () => {
      const body: Record<string, unknown> = { backend };
      for (const f of fields) {
        const raw = values[f];
        if (raw === undefined) continue;
        if (DB_SECRET_FIELDS.has(f) && raw === "********") continue;
        body[f] = coerceValue(f, raw);
      }
      return apiFetch(`/api/profiles/db/${encodeURIComponent(name)}`, {
        method: "PUT",
        body: JSON.stringify(body),
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["profiles", "db"] });
      qc.invalidateQueries({ queryKey: ["profiles", "db", name] });
      onClose();
    },
  });

  return (
    <Modal
      open={open}
      onClose={onClose}
      size="lg"
      title={isEdit ? `Edit ${editingName}` : "Add DB profile"}
      description={
        isEdit
          ? "Tweak fields and save. Secrets stay unchanged unless you replace the masked placeholder."
          : "Pick a backend and fill the connection details. Same fields the CLI's /add-db-profile asks for."
      }
      footer={
        <>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md bg-surface-subtle px-3 py-1.5 text-sm text-ink-muted hover:bg-surface-border"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => save.mutate()}
            disabled={!name.trim() || save.isPending}
            className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
          >
            {save.isPending ? "Saving…" : "Save"}
          </button>
        </>
      }
    >
      <div className="space-y-4">
        <Field label="Profile name">
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={isEdit}
            placeholder="e.g. local-postgre"
            className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20 disabled:opacity-60"
          />
        </Field>
        <Field
          label="Backend"
          hint="Which database engine you're connecting to (Postgres, Snowflake, Databricks…)."
        >
          <select
            value={backend}
            onChange={(e) => setBackend(e.target.value)}
            className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm focus:border-accent focus:outline-none"
          >
            {(backends.data?.backends ?? []).map((b) => (
              <option key={b.id} value={b.id}>
                {b.label}
              </option>
            ))}
          </select>
        </Field>
        <div className="grid grid-cols-2 gap-3">
          {fields.map((f) => (
            <Field key={f} label={DB_FIELD_LABELS[f] ?? f} narrow={f === "port"}>
              {f === "secure" || f === "encrypt" || f === "trust_server_certificate" ? (
                <input
                  type="checkbox"
                  checked={values[f] === "true"}
                  onChange={(e) =>
                    setValues({ ...values, [f]: e.target.checked ? "true" : "false" })
                  }
                  className="h-4 w-4 cursor-pointer accent-current"
                />
              ) : (
                <input
                  type={DB_SECRET_FIELDS.has(f) ? "password" : "text"}
                  value={values[f] ?? ""}
                  onChange={(e) => setValues({ ...values, [f]: e.target.value })}
                  placeholder={
                    f === "port" && chosenBackend?.default_port
                      ? String(chosenBackend.default_port)
                      : ""
                  }
                  className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20"
                />
              )}
            </Field>
          ))}
        </div>
        {save.isError && (
          <div className="rounded-md border border-critical/40 bg-critical/5 px-3 py-2 text-xs text-critical">
            {save.error instanceof Error ? save.error.message : "Save failed."}
          </div>
        )}
      </div>
    </Modal>
  );
}

function coerceValue(field: string, raw: string): unknown {
  if (field === "port") {
    const n = Number(raw);
    return Number.isFinite(n) ? n : raw;
  }
  if (raw === "true") return true;
  if (raw === "false") return false;
  return raw;
}

// ── LLM profiles ──────────────────────────────────────────────────────

/** Per-row "$X / $Y per 1M (source)" hint under each LLM profile.
 *
 * Reuses the same ``api.lookupPrice`` query the cost-override editor
 * already uses, so a refresh of the price cache propagates through
 * every row at once via the shared TanStack Query key. Profile rows
 * with a missing provider/model render nothing — falls back to the
 * provider · model line above with no awkward "—" placeholder. */
function LlmProfilePriceLine({
  provider,
  model,
  isActive,
}: {
  provider: string;
  model: string;
  isActive: boolean;
}) {
  const enabled = Boolean(provider && model);
  const price = useQuery({
    queryKey: ["pricing", "model", provider, model],
    queryFn: () => api.lookupPrice(provider, model),
    enabled,
    refetchOnWindowFocus: false,
    retry: false,
  });
  if (!enabled) return null;
  if (price.isLoading) {
    return (
      <div className={cn("text-[11px]", isActive ? "text-ink-muted" : "text-ink-dim")}>
        Loading price…
      </div>
    );
  }
  const data = price.data;
  if (!data || data.source === "unknown") {
    return (
      <div className={cn("text-[11px]", "text-ink-dim")}>
        no price data — refresh in topbar
      </div>
    );
  }
  return (
    <div className={cn("text-[11px]", isActive ? "text-ink" : "text-ink-muted")}>
      ${data.input_per_mtok.toFixed(4)} / ${data.output_per_mtok.toFixed(4)} per 1M{" "}
      <span className="text-ink-dim">({data.source})</span>
    </div>
  );
}

function LlmProfilesSection() {
  const qc = useQueryClient();
  const [editing, setEditing] = useState<{ name: string | null } | null>(null);

  const profiles = useQuery({
    queryKey: ["profiles", "llm"],
    queryFn: () =>
      apiFetch<{ profiles: LlmProfileSummary[]; active: string | null; count: number }>(
        "/api/profiles/llm",
      ),
    retry: false,
  });
  const activate = useMutation({
    mutationFn: (name: string) =>
      apiFetch(`/api/profiles/llm/${encodeURIComponent(name)}/activate`, { method: "POST" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["profiles", "llm"] }),
  });
  const remove = useMutation({
    mutationFn: (name: string) =>
      apiFetch(`/api/profiles/llm/${encodeURIComponent(name)}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["profiles", "llm"] }),
  });

  return (
    <>
      <Card>
        <CardHeader
          title={`${profiles.data?.count ?? 0} LLM profile${profiles.data?.count === 1 ? "" : "s"}`}
          description={`Active: ${profiles.data?.active ?? "—"}`}
          actions={
            <button
              type="button"
              onClick={() => setEditing({ name: null })}
              className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90"
            >
              <Plus size={14} />
              Add
            </button>
          }
        />
        <CardBody className="p-0">
          {profiles.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading…</div>
          ) : profiles.data?.profiles.length ? (
            <ul className="divide-y divide-surface-border">
              {profiles.data.profiles.map((p) => (
                <li key={p.name} className="flex items-center justify-between gap-3 px-5 py-3 text-sm">
                  <div>
                    <div className="font-medium">{p.name}</div>
                    <div className="text-xs text-ink-dim">
                      <span className="font-mono">{p.provider || "—"}</span> ·{" "}
                      <span className="font-mono">{p.model || "—"}</span>
                    </div>
                    <LlmProfilePriceLine
                      provider={p.provider}
                      model={p.model}
                      isActive={p.is_active}
                    />
                  </div>
                  <div className="flex items-center gap-2">
                    {p.is_active ? (
                      <StatusPill tone="positive">Active</StatusPill>
                    ) : (
                      <button
                        type="button"
                        onClick={() => activate.mutate(p.name)}
                        className="rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-accent-soft hover:text-accent-ink"
                      >
                        Activate
                      </button>
                    )}
                    <button
                      type="button"
                      onClick={() => setEditing({ name: p.name })}
                      className="rounded-md p-1 text-ink-dim hover:bg-surface-subtle hover:text-ink"
                      title="Edit"
                    >
                      <Pencil size={14} />
                    </button>
                    {!p.is_active && (
                      <button
                        type="button"
                        onClick={() => {
                          if (confirm(`Delete LLM profile '${p.name}'?`)) remove.mutate(p.name);
                        }}
                        className="rounded-md p-1 text-ink-dim hover:bg-critical/10 hover:text-critical"
                        title="Delete"
                      >
                        <Trash2 size={14} />
                      </button>
                    )}
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <EmptyState
              icon={Sparkles}
              title="No LLM profiles yet"
              description="Click Add to wire up an OpenAI / Anthropic / Gemini / OpenRouter / local provider."
            />
          )}
        </CardBody>
      </Card>
      {editing && (
        <LlmProfileWizard open editingName={editing.name} onClose={() => setEditing(null)} />
      )}
    </>
  );
}

function LlmProfileWizard({
  open,
  onClose,
  editingName,
}: {
  open: boolean;
  onClose: () => void;
  editingName: string | null;
}) {
  const qc = useQueryClient();
  const isEdit = editingName != null;

  const providers = useQuery({
    queryKey: ["profiles", "llm-providers"],
    queryFn: () => apiFetch<{ providers: LlmProvider[] }>("/api/profiles/llm/providers"),
  });
  const existing = useQuery({
    queryKey: ["profiles", "llm", editingName],
    queryFn: () =>
      apiFetch<Record<string, unknown>>(`/api/profiles/llm/${encodeURIComponent(editingName!)}`),
    enabled: isEdit,
  });

  const [name, setName] = useState(editingName ?? "");
  const [provider, setProvider] = useState("openrouter");
  const [model, setModel] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [apiBase, setApiBase] = useState("");
  const [temperature, setTemperature] = useState(0.2);
  const [maxTokens, setMaxTokens] = useState(16_384);
  // Custom cost overrides (USD per 1M tokens). Stored as strings so
  // the user can clear them by deleting the input — an empty string
  // round-trips to ``null`` on the backend, which the resolution path
  // treats as "no override". Half-overrides (one set, one blank) are
  // also rejected by the backend, matching the CLI ``/cost`` command.
  const [customInputCost, setCustomInputCost] = useState("");
  const [customOutputCost, setCustomOutputCost] = useState("");
  const [nAlternatives, setNAlternatives] = useState(3);
  const [columnBatchSize, setColumnBatchSize] = useState(10);
  const [promptDetail, setPromptDetail] = useState("standard");
  const [descriptionVerbosity, setDescriptionVerbosity] = useState("brief");
  const [logprobHigh, setLogprobHigh] = useState(0.85);
  const [logprobMedium, setLogprobMedium] = useState(0.5);
  const [hydratedFor, setHydratedFor] = useState<string | null>(null);

  useEffect(() => {
    if (!isEdit || !existing.data) return;
    const d = existing.data as Record<string, unknown>;
    const dataName = typeof d.name === "string" ? d.name : null;
    if (dataName !== editingName) return;
    if (hydratedFor === editingName) return;
    setProvider(String(d.provider || "openrouter"));
    setModel(String(d.model || ""));
    setApiKey(String(d.api_key || ""));
    setApiBase(String(d.api_base ?? "") || "");
    setTemperature(Number(d.temperature ?? 0.2));
    setMaxTokens(Number(d.max_tokens ?? 16384));
    setCustomInputCost(
      d.custom_input_cost_per_mtok != null
        ? String(d.custom_input_cost_per_mtok)
        : "",
    );
    setCustomOutputCost(
      d.custom_output_cost_per_mtok != null
        ? String(d.custom_output_cost_per_mtok)
        : "",
    );
    setNAlternatives(Number(d.n_alternatives ?? 3));
    setColumnBatchSize(Number(d.column_batch_size ?? 10));
    setPromptDetail(String(d.prompt_detail || "standard"));
    setDescriptionVerbosity(String(d.description_verbosity || "brief"));
    setLogprobHigh(Number(d.logprob_high ?? 0.85));
    setLogprobMedium(Number(d.logprob_medium ?? 0.5));
    setHydratedFor(editingName);
  }, [isEdit, existing.data, editingName, hydratedFor]);

  const chosenProvider = providers.data?.providers.find((p) => p.id === provider);
  const showApiBase = !!chosenProvider?.needs_base;
  const showApiKey = !!chosenProvider?.needs_key;

  // Live "what would AMX bill if we did NOT have a custom override?"
  // hint, fetched as the user types into the model field. Shown above
  // the custom-cost inputs so users see exactly what their override
  // is replacing — a profile pointed at "openai/gpt-4o" with no
  // override should render "Auto-detected: $2.50 / $10.00 per 1M
  // (litellm)" so the price source is never a black box.
  // Pricing lookup is forced to bypass the user override (we want the
  // *fetched* rate, not whatever the user just typed) by passing no
  // ``profile_name`` — the backend then walks the resolution chain
  // skipping the override layer for that profile.
  const liveModelPrice = useQuery({
    queryKey: ["pricing", "model", provider, model],
    queryFn: () => api.lookupPrice(provider, model),
    enabled: Boolean(provider && model.trim().length > 0),
    retry: false,
    staleTime: 60_000,
  });

  const save = useMutation({
    mutationFn: () => {
      const body: Record<string, unknown> = {
        provider,
        model,
        temperature,
        max_tokens: maxTokens,
        // Empty string = "clear the override". Backend treats null /
        // negative / non-numeric as "no override". The conversion is
        // explicit here so a future schema change does not silently
        // start sending ``"" -> 0`` (which would bill input at zero).
        custom_input_cost_per_mtok:
          customInputCost.trim() === "" ? null : Number(customInputCost),
        custom_output_cost_per_mtok:
          customOutputCost.trim() === "" ? null : Number(customOutputCost),
        n_alternatives: nAlternatives,
        column_batch_size: columnBatchSize,
        prompt_detail: promptDetail,
        description_verbosity: descriptionVerbosity,
        logprob_high: logprobHigh,
        logprob_medium: logprobMedium,
      };
      if (showApiKey && apiKey !== "********") body.api_key = apiKey;
      if (showApiBase) body.api_base = apiBase;
      return apiFetch(`/api/profiles/llm/${encodeURIComponent(name)}`, {
        method: "PUT",
        body: JSON.stringify(body),
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["profiles", "llm"] });
      qc.invalidateQueries({ queryKey: ["profiles", "llm", name] });
      onClose();
    },
  });

  return (
    <Modal
      open={open}
      onClose={onClose}
      size="lg"
      title={isEdit ? `Edit ${editingName}` : "Add LLM profile"}
      description="Provider, model, and the run-time knobs that affect every /run + /ask."
      footer={
        <>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md bg-surface-subtle px-3 py-1.5 text-sm text-ink-muted hover:bg-surface-border"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => save.mutate()}
            disabled={!name.trim() || !model.trim() || save.isPending}
            className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
          >
            {save.isPending ? "Saving…" : "Save"}
          </button>
        </>
      }
    >
      <div className="space-y-4">
        <div className="grid grid-cols-2 gap-3">
          <Field label="Profile name">
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              disabled={isEdit}
              placeholder="e.g. or-kimi"
              className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm disabled:opacity-60"
            />
          </Field>
          <Field label="Provider">
            <select
              value={provider}
              onChange={(e) => setProvider(e.target.value)}
              className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm"
            >
              {(providers.data?.providers ?? []).map((p) => (
                <option key={p.id} value={p.id}>
                  {p.label}
                </option>
              ))}
            </select>
          </Field>
        </div>
        <Field label="Model">
          <input
            type="text"
            value={model}
            onChange={(e) => setModel(e.target.value)}
            placeholder="e.g. moonshotai/kimi-k2-instruct"
            className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
          />
        </Field>
        <div className="grid grid-cols-2 gap-3">
          {showApiKey && (
            <Field label="API key">
              <input
                type="password"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
          )}
          {showApiBase && (
            <Field label="API base URL">
              <input
                type="text"
                value={apiBase}
                onChange={(e) => setApiBase(e.target.value)}
                placeholder="https://…/v1"
                className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
              />
            </Field>
          )}
        </div>

        <h3 className="border-t border-surface-border pt-4 text-[11px] font-semibold uppercase tracking-wider text-ink-dim">
          Generation knobs
        </h3>
        <div className="grid grid-cols-2 gap-3">
          <Field
            label={`Temperature (${temperature.toFixed(2)})`}
            hint="Creativity: low = consistent, high = varied (0.1–0.3 recommended)."
          >
            <input
              type="range"
              min={0}
              max={2}
              step={0.05}
              value={temperature}
              onChange={(e) => setTemperature(Number(e.target.value))}
              className="w-full"
            />
          </Field>
          <Field
            label={`Max output tokens (${maxTokens.toLocaleString()})`}
            hint="Output budget per LLM call. Reasoning models (Kimi K2.x, Claude extended-thinking, GPT-5/o-series, deepseek-reasoner) automatically get a 32k floor on top — this value is the floor for non-reasoning models. Higher = bigger answers + higher cost."
          >
            <input
              type="number"
              min={256}
              max={262_144}
              step={1024}
              value={maxTokens}
              onChange={(e) => setMaxTokens(Math.max(256, Number(e.target.value) || 0))}
              className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
            />
          </Field>
          <div className="col-span-2">
            <PriceHint
              loading={liveModelPrice.isFetching}
              data={liveModelPrice.data}
              error={liveModelPrice.error as Error | null}
              hasModel={!!model.trim()}
            />
          </div>
          <Field
            label="Custom input cost (USD / 1M tokens)"
            hint="Override the auto-detected price. Leave blank to use LiteLLM / OpenRouter / bundled prices. Both rates must be set together — a half-override is treated as no override."
          >
            <input
              type="number"
              min={0}
              step={0.01}
              value={customInputCost}
              placeholder="auto"
              onChange={(e) => setCustomInputCost(e.target.value)}
              className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
            />
          </Field>
          <Field
            label="Custom output cost (USD / 1M tokens)"
            hint="Same rules as the input rate — both must be set or both blank."
          >
            <input
              type="number"
              min={0}
              step={0.01}
              value={customOutputCost}
              placeholder="auto"
              onChange={(e) => setCustomOutputCost(e.target.value)}
              className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
            />
          </Field>
          <Field
            label={`Alternatives per column (${nAlternatives})`}
            hint="How many alternative description proposals to generate per column."
          >
            <input
              type="range"
              min={1}
              max={5}
              step={1}
              value={nAlternatives}
              onChange={(e) => setNAlternatives(Number(e.target.value))}
              className="w-full"
            />
          </Field>
          <Field
            label={`Column batch size (${columnBatchSize})`}
            hint="Columns processed per LLM call. Higher = cheaper; lower = more stable."
          >
            <input
              type="number"
              min={1}
              max={100}
              value={columnBatchSize}
              onChange={(e) => setColumnBatchSize(Number(e.target.value))}
              className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
            />
          </Field>
          <Field
            label="Prompt detail"
            hint="How much context the model receives. More = accurate; less = fast/cheap."
          >
            <select
              value={promptDetail}
              onChange={(e) => setPromptDetail(e.target.value)}
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
            hint="Output length: brief = one sentence, exhaustive = detailed."
          >
            <select
              value={descriptionVerbosity}
              onChange={(e) => setDescriptionVerbosity(e.target.value)}
              className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm"
            >
              {["brief", "detailed", "comprehensive", "exhaustive"].map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </Field>
        </div>

        <h3 className="border-t border-surface-border pt-4 text-[11px] font-semibold uppercase tracking-wider text-ink-dim">
          Confidence thresholds
        </h3>
        <div className="grid grid-cols-2 gap-3">
          <Field
            label={`High (≥ ${logprobHigh.toFixed(2)})`}
            hint="Predictions above this score are flagged 'high confidence'."
          >
            <input
              type="range"
              min={0}
              max={1}
              step={0.05}
              value={logprobHigh}
              onChange={(e) => setLogprobHigh(Number(e.target.value))}
              className="w-full"
            />
          </Field>
          <Field
            label={`Medium (≥ ${logprobMedium.toFixed(2)})`}
            hint="Above this is 'medium confidence'; below counts as 'low'."
          >
            <input
              type="range"
              min={0}
              max={1}
              step={0.05}
              value={logprobMedium}
              onChange={(e) => setLogprobMedium(Number(e.target.value))}
              className="w-full"
            />
          </Field>
        </div>

        {save.isError && (
          <div className="rounded-md border border-critical/40 bg-critical/5 px-3 py-2 text-xs text-critical">
            {save.error instanceof Error ? save.error.message : "Save failed."}
          </div>
        )}
      </div>
    </Modal>
  );
}

/** Live "auto-detected price" hint above the custom-cost inputs.
 *
 * Renders the pricing engine's resolution result for the (provider,
 * model) the user is editing — *without* their custom override layered
 * on top — so the user always sees the rate their override is replacing.
 *
 * Five visual states:
 *  - no model typed yet -> a soft instruction to fill the model field
 *  - loading            -> dim placeholder while the API call is in flight
 *  - error              -> compact red strip + the API's detail message
 *  - source != unknown  -> "Auto-detected: $X / $Y per 1M (litellm)"
 *  - source == unknown  -> warning tone + nudge to /refresh-prices or set custom
 */
function PriceHint({
  loading,
  data,
  error,
  hasModel,
}: {
  loading: boolean;
  data: { input_per_mtok: number; output_per_mtok: number; source: string } | undefined;
  error: Error | null;
  hasModel: boolean;
}) {
  if (!hasModel) {
    return (
      <div className="rounded-md border border-dashed border-surface-border px-3 py-2 text-[11px] text-ink-dim">
        Fill the model field above to see the auto-detected price.
      </div>
    );
  }
  if (loading) {
    return (
      <div className="rounded-md border border-surface-border bg-surface-subtle/40 px-3 py-2 text-[11px] text-ink-dim">
        Looking up auto-detected price…
      </div>
    );
  }
  if (error) {
    return (
      <div className="rounded-md border border-critical/40 bg-critical-soft/40 px-3 py-2 text-[11px] text-critical">
        Could not look up price: {error.message}
      </div>
    );
  }
  if (!data) return null;
  if (data.source === "unknown") {
    return (
      <div className="rounded-md border border-warning/40 bg-warning-soft/40 px-3 py-2 text-[11px] text-warning">
        No price entry found for this model. Run <code className="font-mono">/refresh-prices</code>{" "}
        from the CLI (or click ↻ in the TopBar pricing badge) to re-pull the LiteLLM /
        OpenRouter tables, or set both custom rates below to bill it explicitly.
      </div>
    );
  }
  const sourceLabel =
    data.source === "user_override"
      ? "your override"
      : data.source;
  return (
    <div className="rounded-md border border-info/40 bg-info-soft/40 px-3 py-2 text-[11px] text-info">
      Auto-detected: <span className="font-mono">${data.input_per_mtok.toFixed(4)}</span> /{" "}
      <span className="font-mono">${data.output_per_mtok.toFixed(4)}</span> per 1M tokens (
      <span className="font-medium">{sourceLabel}</span>). Set both inputs below to override.
    </div>
  );
}

// ── Doc + Code profiles ───────────────────────────────────────────────

function DocUploadDropZone({
  profile,
  onJobStarted,
}: {
  profile: string;
  onJobStarted: (jobId: string, label: string) => void;
}) {
  const qc = useQueryClient();
  const [dragOver, setDragOver] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const upload = async (files: FileList) => {
    setError(null);
    if (!profile) {
      setError("Pick a doc profile first.");
      return;
    }
    setBusy(true);
    try {
      const fd = new FormData();
      fd.append("profile", profile);
      fd.append("ingest", "true");
      for (let i = 0; i < files.length; i += 1) {
        fd.append("files", files[i]);
      }
      const resp = await fetch("/api/docs/upload", {
        method: "POST",
        body: fd,
        credentials: "include",
      });
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `Upload failed: HTTP ${resp.status}`);
      }
      const body = (await resp.json()) as {
        saved: Array<{ name: string; duplicate: boolean }>;
        job_id?: string;
      };
      qc.invalidateQueries({ queryKey: ["profiles", "docs"] });
      if (body.job_id) {
        const fresh = body.saved.filter((s) => !s.duplicate).length;
        onJobStarted(
          body.job_id,
          `Ingesting ${fresh} new file${fresh === 1 ? "" : "s"} in ${profile}`,
        );
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="px-5 pb-3">
      <div
        onDragOver={(e) => {
          e.preventDefault();
          setDragOver(true);
        }}
        onDragLeave={() => setDragOver(false)}
        onDrop={(e) => {
          e.preventDefault();
          setDragOver(false);
          if (e.dataTransfer.files?.length) {
            upload(e.dataTransfer.files);
          }
        }}
        className={
          "rounded-md border border-dashed px-3 py-2 text-xs transition " +
          (dragOver
            ? "border-accent bg-accent-soft text-accent-ink"
            : "border-surface-border text-ink-dim hover:border-accent/40")
        }
      >
        <label className="flex cursor-pointer items-center justify-between gap-2">
          <span>
            {busy
              ? "Uploading…"
              : `Drag-drop files here to add to ${profile || "(pick a profile)"} — or click to browse.`}
          </span>
          <input
            type="file"
            multiple
            disabled={busy || !profile}
            onChange={(e) => {
              if (e.target.files?.length) upload(e.target.files);
              e.target.value = "";
            }}
            className="hidden"
            accept=".md,.markdown,.txt,.pdf,.docx,.doc,.csv,.tsv,.html,.htm,.rst,.rtf,.json,.yaml,.yml"
          />
          <span className="rounded bg-surface-subtle px-2 py-0.5 text-[10px]">
            Browse
          </span>
        </label>
      </div>
      {error && (
        <div className="mt-1 text-[11px] text-critical">{error}</div>
      )}
    </div>
  );
}

function DocProfilesSection() {
  const qc = useQueryClient();
  const [editing, setEditing] = useState<{ name: string | null } | null>(null);
  const [activeOp, setActiveOp] = useState<{ jobId: string; label: string } | null>(null);

  const profiles = useQuery({
    queryKey: ["profiles", "docs"],
    queryFn: () =>
      apiFetch<{ profiles: DocProfile[]; active: string | null }>("/api/profiles/docs"),
    retry: false,
  });
  const activate = useMutation({
    mutationFn: (name: string) =>
      apiFetch(`/api/profiles/docs/${encodeURIComponent(name)}/activate`, { method: "POST" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["profiles", "docs"] }),
  });
  const remove = useMutation({
    mutationFn: (name: string) =>
      apiFetch(`/api/profiles/docs/${encodeURIComponent(name)}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["profiles", "docs"] }),
  });
  const scan = useMutation({
    mutationFn: (profile: string) =>
      apiFetch<{ job_id: string }>("/api/docs/scan", {
        method: "POST",
        body: JSON.stringify({ profile }),
      }),
    onSuccess: (r, profile) => setActiveOp({ jobId: r.job_id, label: `Scanning ${profile}` }),
  });
  const ingest = useMutation({
    mutationFn: (vars: { profile: string; refresh: boolean }) =>
      apiFetch<{ job_id: string }>("/api/docs/ingest", {
        method: "POST",
        body: JSON.stringify({ profile: vars.profile, refresh: vars.refresh }),
      }),
    onSuccess: (r, vars) =>
      setActiveOp({
        jobId: r.job_id,
        label: `Ingesting ${vars.profile}${vars.refresh ? " (refresh)" : ""}`,
      }),
  });

  return (
    <>
      <SearchDocsBox />
      {activeOp && (
        <div className="mb-4">
          <JobProgress
            jobId={activeOp.jobId}
            kind="docs/scan"
            onTerminal={() => setActiveOp(null)}
          />
          <p className="mt-1 text-xs text-ink-dim">{activeOp.label}</p>
        </div>
      )}
      <Card>
        <CardHeader
          title={`${profiles.data?.profiles?.length ?? 0} doc profile${
            profiles.data?.profiles?.length === 1 ? "" : "s"
          }`}
          description={`Active: ${profiles.data?.active ?? "—"} · Each profile groups one or more directories / URLs / S3 prefixes that AMX feeds to the doc RAG agent.`}
          actions={
            <button
              type="button"
              onClick={() => setEditing({ name: null })}
              className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90"
            >
              <Plus size={14} />
              Add
            </button>
          }
        />
        <CardBody className="p-0">
          {profiles.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading…</div>
          ) : profiles.data?.profiles?.length ? (
            <ul className="divide-y divide-surface-border">
              {profiles.data.profiles.map((p) => (
                <li key={p.name} className="px-5 py-3 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <div className="min-w-0">
                      <div className="font-medium">{p.name}</div>
                      <div className="text-xs text-ink-dim">
                        {p.paths.length} path{p.paths.length === 1 ? "" : "s"}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      {p.is_active ? (
                        <StatusPill tone="positive">Active</StatusPill>
                      ) : (
                        <button
                          type="button"
                          onClick={() => activate.mutate(p.name)}
                          className="rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-accent-soft hover:text-accent-ink"
                        >
                          Activate
                        </button>
                      )}
                      <button
                        type="button"
                        onClick={() => scan.mutate(p.name)}
                        disabled={scan.isPending || !!activeOp}
                        className="rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-surface-border disabled:opacity-50"
                        title="Scan: list what would be ingested"
                      >
                        Scan
                      </button>
                      <button
                        type="button"
                        onClick={() =>
                          ingest.mutate({ profile: p.name, refresh: false })
                        }
                        disabled={ingest.isPending || !!activeOp}
                        className="rounded-md bg-accent-soft px-2 py-1 text-xs text-accent-ink hover:opacity-90 disabled:opacity-50"
                        title="Ingest into Chroma RAG store"
                      >
                        Ingest
                      </button>
                      <button
                        type="button"
                        onClick={() =>
                          ingest.mutate({ profile: p.name, refresh: true })
                        }
                        disabled={ingest.isPending || !!activeOp}
                        className="rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-surface-border disabled:opacity-50"
                        title="Refresh: drop existing chunks for these sources before re-ingesting"
                      >
                        Re-ingest
                      </button>
                      <button
                        type="button"
                        onClick={() => setEditing({ name: p.name })}
                        className="rounded-md p-1 text-ink-dim hover:bg-surface-subtle hover:text-ink"
                        title="Edit"
                      >
                        <Pencil size={14} />
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          if (confirm(`Delete doc profile '${p.name}'?`)) remove.mutate(p.name);
                        }}
                        className="rounded-md p-1 text-ink-dim hover:bg-critical/10 hover:text-critical"
                        title="Delete"
                      >
                        <Trash2 size={14} />
                      </button>
                    </div>
                  </div>
                  {p.paths.length > 0 && (
                    <ul className="mt-2 space-y-0.5 pl-3 text-[11px] text-ink-muted">
                      {p.paths.map((path, idx) => (
                        <li key={idx} className="truncate font-mono">
                          {path}
                        </li>
                      ))}
                    </ul>
                  )}
                  {(p.linked_db_profiles?.length ?? 0) > 0 && (
                    <div className="mt-1.5 flex flex-wrap items-center gap-1 pl-3 text-[10px] text-ink-dim">
                      <span>Links:</span>
                      {p.linked_db_profiles!.map((db) => (
                        <span
                          key={db}
                          className="rounded bg-surface-subtle px-1.5 py-0.5 font-mono text-accent-ink"
                        >
                          {db}
                        </span>
                      ))}
                    </div>
                  )}
                  <div className="mt-2">
                    <DocUploadDropZone
                      profile={p.name}
                      onJobStarted={(jobId, label) =>
                        setActiveOp({ jobId, label })
                      }
                    />
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <EmptyState
              icon={FileText}
              title="No doc profiles yet"
              description="Bind a folder of design docs / data dictionaries so the RAG agent can ground its descriptions."
            />
          )}
        </CardBody>
      </Card>
      {editing && (
        <DocProfileWizard
          open
          editingName={editing.name}
          existingPaths={
            editing.name
              ? profiles.data?.profiles?.find((p) => p.name === editing.name)?.paths ?? []
              : []
          }
          existingLinkedDbs={
            editing.name
              ? profiles.data?.profiles?.find((p) => p.name === editing.name)
                  ?.linked_db_profiles ?? []
              : []
          }
          onClose={() => setEditing(null)}
        />
      )}
    </>
  );
}

function DocProfileWizard({
  open,
  onClose,
  editingName,
  existingPaths,
  existingLinkedDbs,
}: {
  open: boolean;
  onClose: () => void;
  editingName: string | null;
  existingPaths: string[];
  existingLinkedDbs: string[];
}) {
  const qc = useQueryClient();
  const isEdit = editingName != null;
  const [name, setName] = useState(editingName ?? "");
  const [text, setText] = useState(existingPaths.join("\n"));
  const [linkedDbs, setLinkedDbs] = useState<string[]>(existingLinkedDbs);
  // Buffered drag-drop files. Held client-side until Save so the user
  // can stage uploads without committing a half-built profile, and so
  // the same flow works for both create (no profile yet) and edit.
  // After PUT succeeds we POST these to /api/docs/upload, which
  // auto-creates the profile if it didn't exist on the backend.
  const [pendingFiles, setPendingFiles] = useState<File[]>([]);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const [uploadingNote, setUploadingNote] = useState<string | null>(null);

  const addFiles = (incoming: FileList | File[]) => {
    const next: File[] = [...pendingFiles];
    for (let i = 0; i < (incoming as FileList).length; i += 1) {
      const f = (incoming as FileList)[i] ?? (incoming as File[])[i];
      if (f && !next.some((p) => p.name === f.name && p.size === f.size)) {
        next.push(f);
      }
    }
    setPendingFiles(next);
  };

  const save = useMutation({
    mutationFn: async () => {
      await apiFetch(`/api/profiles/docs/${encodeURIComponent(name)}`, {
        method: "PUT",
        body: JSON.stringify({
          paths: text
            .split("\n")
            .map((s) => s.trim())
            .filter(Boolean),
          linked_db_profiles: linkedDbs,
        }),
      });
      if (pendingFiles.length === 0) return;
      setUploadingNote(
        `Uploading ${pendingFiles.length} file${
          pendingFiles.length === 1 ? "" : "s"
        }…`,
      );
      const fd = new FormData();
      fd.append("profile", name);
      fd.append("ingest", "true");
      for (const f of pendingFiles) fd.append("files", f);
      const resp = await fetch("/api/docs/upload", {
        method: "POST",
        body: fd,
        credentials: "include",
      });
      if (!resp.ok) {
        const errText = await resp.text();
        throw new Error(errText || `Upload failed: HTTP ${resp.status}`);
      }
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["profiles", "docs"] });
      setUploadingNote(null);
      onClose();
    },
    onError: (e) => {
      setUploadError(e instanceof Error ? e.message : String(e));
      setUploadingNote(null);
    },
  });

  return (
    <Modal
      open={open}
      onClose={onClose}
      title={isEdit ? `Edit ${editingName}` : "Add doc profile"}
      description="One path per line. Local directories, file:// URIs, and remote URLs (https / s3 / gcs) all work."
      footer={
        <>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md bg-surface-subtle px-3 py-1.5 text-sm text-ink-muted hover:bg-surface-border"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => save.mutate()}
            disabled={!name.trim() || save.isPending}
            className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
          >
            {save.isPending ? "Saving…" : "Save"}
          </button>
        </>
      }
    >
      <div className="space-y-4">
        <Field label="Profile name">
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={isEdit}
            placeholder="e.g. design-docs"
            className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm disabled:opacity-60"
          />
        </Field>
        <Field label="Paths (one per line)">
          <textarea
            value={text}
            onChange={(e) => setText(e.target.value)}
            rows={6}
            placeholder={"/abs/path/to/docs\nhttps://example.com/handbook.pdf\ns3://bucket/key/"}
            className="w-full rounded-md border border-surface-border bg-surface px-3 py-2 font-mono text-xs"
          />
        </Field>
        <Field label="Or drop files (optional)">
          <p className="mb-2 text-[11px] text-ink-dim">
            PDF, DOCX, MD, TXT, CSV, HTML, RST, JSON, YAML accepted. Files
            land under <code>~/.amx/uploads/{name || "<profile>"}/</code> and
            ingest immediately when you save the profile.
          </p>
          <div
            onDragOver={(e) => {
              e.preventDefault();
              setDragOver(true);
            }}
            onDragLeave={() => setDragOver(false)}
            onDrop={(e) => {
              e.preventDefault();
              setDragOver(false);
              if (e.dataTransfer.files?.length) addFiles(e.dataTransfer.files);
            }}
            className={
              "rounded-md border border-dashed px-3 py-3 text-xs transition " +
              (dragOver
                ? "border-accent bg-accent-soft text-accent-ink"
                : "border-surface-border text-ink-dim hover:border-accent/40")
            }
          >
            <label className="flex cursor-pointer items-center justify-between gap-2">
              <span>
                {pendingFiles.length === 0
                  ? "Drag-drop files here, or click to browse."
                  : `${pendingFiles.length} file${
                      pendingFiles.length === 1 ? "" : "s"
                    } staged — drop more or click to add.`}
              </span>
              <input
                type="file"
                multiple
                onChange={(e) => {
                  if (e.target.files?.length) addFiles(e.target.files);
                  e.target.value = "";
                }}
                className="hidden"
                accept=".md,.markdown,.txt,.pdf,.docx,.doc,.csv,.tsv,.html,.htm,.rst,.rtf,.json,.yaml,.yml"
              />
              <span className="rounded bg-surface-subtle px-2 py-0.5 text-[10px]">
                Browse
              </span>
            </label>
          </div>
          {pendingFiles.length > 0 && (
            <ul className="mt-2 space-y-0.5 text-[11px] text-ink-muted">
              {pendingFiles.map((f, idx) => (
                <li
                  key={`${f.name}-${idx}`}
                  className="flex items-center justify-between gap-2 rounded bg-surface-subtle px-2 py-1"
                >
                  <span className="truncate font-mono">
                    {f.name}{" "}
                    <span className="text-ink-dim">
                      ({(f.size / 1024).toFixed(1)} KB)
                    </span>
                  </span>
                  <button
                    type="button"
                    onClick={() =>
                      setPendingFiles(pendingFiles.filter((_, i) => i !== idx))
                    }
                    className="rounded p-0.5 text-ink-dim hover:bg-critical/10 hover:text-critical"
                    title="Remove"
                  >
                    <Trash2 size={11} />
                  </button>
                </li>
              ))}
            </ul>
          )}
          {uploadingNote && (
            <p className="mt-1 text-[11px] text-ink-muted">{uploadingNote}</p>
          )}
          {uploadError && (
            <p className="mt-1 text-[11px] text-critical">{uploadError}</p>
          )}
        </Field>
        <LinkedDbsField selected={linkedDbs} onChange={setLinkedDbs} kind="doc" />
        {save.isError && (
          <div className="rounded-md border border-critical/40 bg-critical/5 px-3 py-2 text-xs text-critical">
            {save.error instanceof Error ? save.error.message : "Save failed."}
          </div>
        )}
      </div>
    </Modal>
  );
}

function LinkedDbsField({
  selected,
  onChange,
  kind,
}: {
  selected: string[];
  onChange: (next: string[]) => void;
  kind: "doc" | "code";
}) {
  // Multi-select chips driven by the canonical DB profile list. An
  // empty selection means "global" — the doc/code profile shows up in
  // every /ask scope. Linking it to one or more DB profiles narrows
  // /ask retrieval to questions running against those DBs.
  const dbs = useQuery({
    queryKey: ["profiles", "db", "names-only"],
    queryFn: () =>
      apiFetch<{ profiles: DbProfileSummary[] }>("/api/profiles/db"),
    retry: false,
  });
  const profiles = dbs.data?.profiles ?? [];
  const toggle = (name: string) => {
    if (selected.includes(name)) {
      onChange(selected.filter((n) => n !== name));
    } else {
      onChange([...selected, name]);
    }
  };
  return (
    <Field label="Linked DB profiles (optional)">
      <p className="mb-2 text-[11px] text-ink-dim">
        Pick which DB profiles this {kind} documents. Empty = global (in
        scope for every /ask). When set, /ask only pulls from this {kind}{" "}
        profile when at least one of the selected DBs is in the question's
        scope.
      </p>
      {profiles.length === 0 ? (
        <p className="text-xs text-ink-dim">
          No DB profiles configured yet — leave empty to keep this {kind}{" "}
          profile global.
        </p>
      ) : (
        <div className="flex flex-wrap gap-1.5">
          {profiles.map((db) => {
            const active = selected.includes(db.name);
            return (
              <button
                key={db.name}
                type="button"
                onClick={() => toggle(db.name)}
                className={
                  "rounded-md border px-2 py-1 text-xs font-mono transition " +
                  (active
                    ? "border-accent bg-accent-soft text-accent-ink"
                    : "border-surface-border bg-surface text-ink-muted hover:border-accent/40")
                }
              >
                {db.name}
              </button>
            );
          })}
        </div>
      )}
    </Field>
  );
}

interface SearchHit {
  source: string;
  distance: number;
  preview: string;
}

interface SearchResponse {
  hits: SearchHit[];
  count: number;
  message?: string;
}

function SearchDocsBox() {
  const [query, setQuery] = useState("");
  const [submitted, setSubmitted] = useState("");

  const search = useQuery({
    queryKey: ["docs-search", submitted],
    queryFn: () =>
      apiFetch<SearchResponse>(
        `/api/docs/search?q=${encodeURIComponent(submitted)}&n=8`,
      ),
    enabled: submitted.length > 0,
    retry: false,
  });

  return (
    <Card className="mb-4">
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <FileText size={14} className="text-accent" />
            Search docs
          </span>
        }
        description="Embedding-only Chroma similarity over every chunk you've ingested. No LLM call — instant."
      />
      <CardBody className="space-y-3">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            setSubmitted(query.trim());
          }}
          className="flex gap-2"
        >
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="e.g. customer master, order pipeline, GDPR…"
            className="flex-1 rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20"
          />
          <button
            type="submit"
            disabled={!query.trim() || search.isFetching}
            className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
          >
            {search.isFetching ? "Searching…" : "Search"}
          </button>
        </form>
        {search.error && (
          <div className="rounded-md border border-critical/40 bg-critical/5 px-3 py-2 text-xs text-critical">
            {(search.error as Error).message}
          </div>
        )}
        {search.data && (
          <div className="space-y-2">
            {search.data.message && (
              <p className="text-xs text-warning">{search.data.message}</p>
            )}
            {search.data.hits.length === 0 ? (
              <p className="text-xs text-ink-dim">No matches.</p>
            ) : (
              <ul className="space-y-2">
                {search.data.hits.map((hit, idx) => (
                  <li
                    key={`${hit.source}-${idx}`}
                    className="rounded-md border border-surface-border bg-surface px-3 py-2"
                  >
                    <div className="flex items-center justify-between gap-2 text-[11px]">
                      <span className="truncate font-mono text-ink-muted">
                        {hit.source}
                      </span>
                      <span className="font-mono text-ink-dim">
                        d={hit.distance.toFixed(3)}
                      </span>
                    </div>
                    <p className="mt-1 line-clamp-3 text-xs text-ink-muted">
                      {hit.preview}
                    </p>
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}
      </CardBody>
    </Card>
  );
}

interface CodeSearchHit {
  source: string;
  rel_path: string;
  symbol: string;
  distance: number;
  preview: string;
}

interface CodeSearchResponse {
  hits: CodeSearchHit[];
  count: number;
  message?: string;
}

function SearchCodeBox() {
  const [query, setQuery] = useState("");
  const [submitted, setSubmitted] = useState("");

  const search = useQuery({
    queryKey: ["code-search", submitted],
    queryFn: () =>
      apiFetch<CodeSearchResponse>(
        `/api/code/search?q=${encodeURIComponent(submitted)}&n=8`,
      ),
    enabled: submitted.length > 0,
    retry: false,
  });

  return (
    <Card className="mb-4">
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <CodeIcon size={14} className="text-accent" />
            Search code
          </span>
        }
        description="Embedding-only Chroma similarity over every snippet you've indexed via /code-scan. No LLM call — instant. For a comprehensive table-level review, use Code Analyze (CLI: /code-analyze)."
      />
      <CardBody className="space-y-3">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            setSubmitted(query.trim());
          }}
          className="flex gap-2"
        >
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="e.g. where is customers written, ETL job for orders…"
            className="flex-1 rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20"
          />
          <button
            type="submit"
            disabled={!query.trim() || search.isFetching}
            className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
          >
            {search.isFetching ? "Searching…" : "Search"}
          </button>
        </form>
        {search.error && (
          <div className="rounded-md border border-critical/40 bg-critical/5 px-3 py-2 text-xs text-critical">
            {(search.error as Error).message}
          </div>
        )}
        {search.data && (
          <div className="space-y-2">
            {search.data.message && (
              <p className="text-xs text-warning">{search.data.message}</p>
            )}
            {search.data.hits.length === 0 ? (
              <p className="text-xs text-ink-dim">No matches.</p>
            ) : (
              <ul className="space-y-2">
                {search.data.hits.map((hit, idx) => (
                  <li
                    key={`${hit.source}-${idx}`}
                    className="rounded-md border border-surface-border bg-surface px-3 py-2"
                  >
                    <div className="flex items-center justify-between gap-2 text-[11px]">
                      <span className="truncate font-mono text-ink-muted">
                        {hit.source}
                        {hit.symbol ? (
                          <span className="ml-2 text-accent-ink">
                            · {hit.symbol}
                          </span>
                        ) : null}
                      </span>
                      <span className="font-mono text-ink-dim">
                        d={hit.distance.toFixed(3)}
                      </span>
                    </div>
                    <p className="mt-1 line-clamp-3 whitespace-pre-wrap text-xs text-ink-muted">
                      {hit.preview}
                    </p>
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}
      </CardBody>
    </Card>
  );
}

function CodeAnalyzeModal({
  open,
  onClose,
  codeProfile,
  onJobStarted,
}: {
  open: boolean;
  onClose: () => void;
  codeProfile: string;
  onJobStarted: (jobId: string, label: string) => void;
}) {
  const [schema, setSchema] = useState("");
  const [tablesText, setTablesText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const run = useMutation({
    mutationFn: () => {
      const tables = tablesText
        .split(/[\n,]/)
        .map((s) => s.trim())
        .filter(Boolean);
      return apiFetch<{ job_id: string; tables: string[] }>(
        "/api/code/analyze",
        {
          method: "POST",
          body: JSON.stringify({
            schema,
            tables,
            code_profile: codeProfile,
          }),
        },
      );
    },
    onSuccess: (resp) => {
      onJobStarted(
        resp.job_id,
        `Code Analyze ${codeProfile} (${resp.tables.length} table${
          resp.tables.length === 1 ? "" : "s"
        })`,
      );
      onClose();
    },
    onError: (e) => setError(e instanceof Error ? e.message : String(e)),
  });

  return (
    <Modal
      open={open}
      onClose={onClose}
      title={`Code Analyze — ${codeProfile}`}
      description="Run the Code Agent against the cached /code-scan for the listed tables. Results write to ~/.amx/code_agent_results.json — the next /run will pick them up automatically."
      footer={
        <>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md bg-surface-subtle px-3 py-1.5 text-sm text-ink-muted hover:bg-surface-border"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => {
              setError(null);
              run.mutate();
            }}
            disabled={!schema.trim() || !tablesText.trim() || run.isPending}
            className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
          >
            {run.isPending ? "Starting…" : "Run analyze"}
          </button>
        </>
      }
    >
      <div className="space-y-4">
        <Field label="Schema">
          <input
            type="text"
            value={schema}
            onChange={(e) => setSchema(e.target.value)}
            placeholder="e.g. sales"
            className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
          />
        </Field>
        <Field label="Tables (comma- or newline-separated)">
          <textarea
            value={tablesText}
            onChange={(e) => setTablesText(e.target.value)}
            rows={4}
            placeholder={"orders\ncustomers\n..."}
            className="w-full rounded-md border border-surface-border bg-surface px-3 py-2 font-mono text-xs"
          />
        </Field>
        {error && (
          <div className="rounded-md border border-critical/40 bg-critical/5 px-3 py-2 text-xs text-critical">
            {error}
          </div>
        )}
      </div>
    </Modal>
  );
}

function CodeProfilesSection() {
  const qc = useQueryClient();
  const [editing, setEditing] = useState<{ name: string | null } | null>(null);
  const [analyzing, setAnalyzing] = useState<string | null>(null);
  const [activeOp, setActiveOp] = useState<{ jobId: string; label: string } | null>(null);

  const profiles = useQuery({
    queryKey: ["profiles", "code"],
    queryFn: () =>
      apiFetch<{ profiles: CodeProfile[]; active: string | null }>("/api/profiles/code"),
    retry: false,
  });
  const activate = useMutation({
    mutationFn: (name: string) =>
      apiFetch(`/api/profiles/code/${encodeURIComponent(name)}/activate`, { method: "POST" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["profiles", "code"] }),
  });
  const remove = useMutation({
    mutationFn: (name: string) =>
      apiFetch(`/api/profiles/code/${encodeURIComponent(name)}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["profiles", "code"] }),
  });
  const scan = useMutation({
    mutationFn: (vars: { profile: string; column_scan: boolean }) =>
      apiFetch<{ job_id: string }>("/api/code/scan", {
        method: "POST",
        body: JSON.stringify({ profile: vars.profile, column_scan: vars.column_scan }),
      }),
    onSuccess: (r, vars) =>
      setActiveOp({
        jobId: r.job_id,
        label: `Scanning ${vars.profile}${vars.column_scan ? " (incl. columns)" : ""}`,
      }),
  });

  return (
    <>
      <SearchCodeBox />
      {activeOp && (
        <div className="mb-4">
          <JobProgress
            jobId={activeOp.jobId}
            kind="docs/scan"
            onTerminal={() => setActiveOp(null)}
          />
          <p className="mt-1 text-xs text-ink-dim">{activeOp.label}</p>
        </div>
      )}
      <Card>
        <CardHeader
          title={`${profiles.data?.profiles?.length ?? 0} code profile${
            profiles.data?.profiles?.length === 1 ? "" : "s"
          }`}
          description={`Active: ${profiles.data?.active ?? "—"} · A code profile points at a single codebase (local dir or Git URL) that the Code agent scans for table/column references.`}
          actions={
            <button
              type="button"
              onClick={() => setEditing({ name: null })}
              className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90"
            >
              <Plus size={14} />
              Add
            </button>
          }
        />
        <CardBody className="p-0">
          {profiles.isLoading ? (
            <div className="px-5 py-6 text-sm text-ink-dim">Loading…</div>
          ) : profiles.data?.profiles?.length ? (
            <ul className="divide-y divide-surface-border">
              {profiles.data.profiles.map((p) => (
                <li key={p.name} className="flex items-center justify-between gap-3 px-5 py-3 text-sm">
                  <div className="min-w-0">
                    <div className="font-medium">{p.name}</div>
                    <div className="truncate font-mono text-xs text-ink-dim">{p.path}</div>
                    {(p.linked_db_profiles?.length ?? 0) > 0 && (
                      <div className="mt-1 flex flex-wrap items-center gap-1 text-[10px] text-ink-dim">
                        <span>Links:</span>
                        {p.linked_db_profiles!.map((db) => (
                          <span
                            key={db}
                            className="rounded bg-surface-subtle px-1.5 py-0.5 font-mono text-accent-ink"
                          >
                            {db}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    {p.is_active ? (
                      <StatusPill tone="positive">Active</StatusPill>
                    ) : (
                      <button
                        type="button"
                        onClick={() => activate.mutate(p.name)}
                        className="rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-accent-soft hover:text-accent-ink"
                      >
                        Activate
                      </button>
                    )}
                    <button
                      type="button"
                      onClick={() => scan.mutate({ profile: p.name, column_scan: false })}
                      disabled={scan.isPending || !!activeOp}
                      className="rounded-md bg-accent-soft px-2 py-1 text-xs text-accent-ink hover:opacity-90 disabled:opacity-50"
                      title="Scan: walk source files for table references"
                    >
                      Scan
                    </button>
                    <button
                      type="button"
                      onClick={() => scan.mutate({ profile: p.name, column_scan: true })}
                      disabled={scan.isPending || !!activeOp}
                      className="rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-surface-border disabled:opacity-50"
                      title="Scan + columns: also pick up column-name references (slower)"
                    >
                      +Cols
                    </button>
                    <button
                      type="button"
                      onClick={() => setAnalyzing(p.name)}
                      disabled={!!activeOp}
                      className="rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-accent-soft hover:text-accent-ink disabled:opacity-50"
                      title="Run the Code Agent against /code-scan output for selected tables"
                    >
                      Analyze
                    </button>
                    <button
                      type="button"
                      onClick={() => setEditing({ name: p.name })}
                      className="rounded-md p-1 text-ink-dim hover:bg-surface-subtle hover:text-ink"
                      title="Edit"
                    >
                      <Pencil size={14} />
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        if (confirm(`Delete code profile '${p.name}'?`)) remove.mutate(p.name);
                      }}
                      className="rounded-md p-1 text-ink-dim hover:bg-critical/10 hover:text-critical"
                      title="Delete"
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <EmptyState
              icon={CodeIcon}
              title="No code profiles yet"
              description="Bind a repository so the Code agent can pick up SQL/Python references when generating descriptions."
            />
          )}
        </CardBody>
      </Card>
      {editing && (
        <CodeProfileWizard
          open
          editingName={editing.name}
          existingPath={
            editing.name
              ? profiles.data?.profiles?.find((p) => p.name === editing.name)?.path ?? ""
              : ""
          }
          existingLinkedDbs={
            editing.name
              ? profiles.data?.profiles?.find((p) => p.name === editing.name)
                  ?.linked_db_profiles ?? []
              : []
          }
          onClose={() => setEditing(null)}
        />
      )}
      {analyzing && (
        <CodeAnalyzeModal
          open
          codeProfile={analyzing}
          onClose={() => setAnalyzing(null)}
          onJobStarted={(jobId, label) => setActiveOp({ jobId, label })}
        />
      )}
    </>
  );
}

function CodeProfileWizard({
  open,
  onClose,
  editingName,
  existingPath,
  existingLinkedDbs,
}: {
  open: boolean;
  onClose: () => void;
  editingName: string | null;
  existingPath: string;
  existingLinkedDbs: string[];
}) {
  const qc = useQueryClient();
  const isEdit = editingName != null;
  const [name, setName] = useState(editingName ?? "");
  const [path, setPath] = useState(existingPath);
  const [linkedDbs, setLinkedDbs] = useState<string[]>(existingLinkedDbs);

  const save = useMutation({
    mutationFn: () =>
      apiFetch(`/api/profiles/code/${encodeURIComponent(name)}`, {
        method: "PUT",
        body: JSON.stringify({ path, linked_db_profiles: linkedDbs }),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["profiles", "code"] });
      onClose();
    },
  });

  return (
    <Modal
      open={open}
      onClose={onClose}
      title={isEdit ? `Edit ${editingName}` : "Add code profile"}
      description="A single repository or local directory the Code agent walks for SQL/Python references."
      footer={
        <>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md bg-surface-subtle px-3 py-1.5 text-sm text-ink-muted hover:bg-surface-border"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => save.mutate()}
            disabled={!name.trim() || !path.trim() || save.isPending}
            className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-40"
          >
            {save.isPending ? "Saving…" : "Save"}
          </button>
        </>
      }
    >
      <div className="space-y-4">
        <Field label="Profile name">
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={isEdit}
            placeholder="e.g. main-repo"
            className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm disabled:opacity-60"
          />
        </Field>
        <Field label="Path or Git URL">
          <input
            type="text"
            value={path}
            onChange={(e) => setPath(e.target.value)}
            placeholder="/abs/path/to/repo or https://github.com/org/repo"
            className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
          />
        </Field>
        <LinkedDbsField selected={linkedDbs} onChange={setLinkedDbs} kind="code" />
        {save.isError && (
          <div className="rounded-md border border-critical/40 bg-critical/5 px-3 py-2 text-xs text-critical">
            {save.error instanceof Error ? save.error.message : "Save failed."}
          </div>
        )}
      </div>
    </Modal>
  );
}

// ── Shared ────────────────────────────────────────────────────────────

function Field({
  label,
  children,
  narrow,
  hint,
}: {
  label: string;
  children: React.ReactNode;
  narrow?: boolean;
  hint?: string;
}) {
  return (
    <label className={cn("block", narrow && "max-w-[160px]")}>
      <span className="mb-1 flex items-center gap-1 text-[11px] font-semibold uppercase tracking-wider text-ink-dim">
        {label}
        {hint && <InfoHint text={hint} />}
      </span>
      {children}
    </label>
  );
}
