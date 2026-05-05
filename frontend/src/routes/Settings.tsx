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
import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";
import { Tabs, TabsList, Tab as TabTrigger, TabPanel } from "../components/ui";

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
}

interface CodeProfile {
  name: string;
  path: string;
  is_active: boolean;
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
      apiFetch<{ profiles: DbProfileSummary[]; active: string | null; count: number }>(
        "/api/profiles/db",
      ),
    retry: false,
  });

  const activate = useMutation({
    mutationFn: (name: string) =>
      apiFetch(`/api/profiles/db/${encodeURIComponent(name)}/activate`, { method: "POST" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["profiles", "db"] }),
  });

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
                        {!p.is_active && (
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
        <Field label="Backend">
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

  const save = useMutation({
    mutationFn: () => {
      const body: Record<string, unknown> = {
        provider,
        model,
        temperature,
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
          <Field label={`Temperature (${temperature.toFixed(2)})`}>
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
          <Field label={`Alternatives per column (${nAlternatives})`}>
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
          <Field label={`Column batch size (${columnBatchSize})`}>
            <input
              type="number"
              min={1}
              max={100}
              value={columnBatchSize}
              onChange={(e) => setColumnBatchSize(Number(e.target.value))}
              className="w-full rounded-md border border-surface-border bg-surface px-3 py-1.5 font-mono text-sm"
            />
          </Field>
          <Field label="Prompt detail">
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
          <Field label="Description verbosity">
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
          <Field label={`High (≥ ${logprobHigh.toFixed(2)})`}>
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
          <Field label={`Medium (≥ ${logprobMedium.toFixed(2)})`}>
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

// ── Doc + Code profiles ───────────────────────────────────────────────

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
}: {
  open: boolean;
  onClose: () => void;
  editingName: string | null;
  existingPaths: string[];
}) {
  const qc = useQueryClient();
  const isEdit = editingName != null;
  const [name, setName] = useState(editingName ?? "");
  const [text, setText] = useState(existingPaths.join("\n"));

  const save = useMutation({
    mutationFn: () =>
      apiFetch(`/api/profiles/docs/${encodeURIComponent(name)}`, {
        method: "PUT",
        body: JSON.stringify({
          paths: text
            .split("\n")
            .map((s) => s.trim())
            .filter(Boolean),
        }),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["profiles", "docs"] });
      onClose();
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
            rows={8}
            placeholder={"/abs/path/to/docs\nhttps://example.com/handbook.pdf\ns3://bucket/key/"}
            className="w-full rounded-md border border-surface-border bg-surface px-3 py-2 font-mono text-xs"
          />
        </Field>
        {save.isError && (
          <div className="rounded-md border border-critical/40 bg-critical/5 px-3 py-2 text-xs text-critical">
            {save.error instanceof Error ? save.error.message : "Save failed."}
          </div>
        )}
      </div>
    </Modal>
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

function CodeProfilesSection() {
  const qc = useQueryClient();
  const [editing, setEditing] = useState<{ name: string | null } | null>(null);
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
          onClose={() => setEditing(null)}
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
}: {
  open: boolean;
  onClose: () => void;
  editingName: string | null;
  existingPath: string;
}) {
  const qc = useQueryClient();
  const isEdit = editingName != null;
  const [name, setName] = useState(editingName ?? "");
  const [path, setPath] = useState(existingPath);

  const save = useMutation({
    mutationFn: () =>
      apiFetch(`/api/profiles/code/${encodeURIComponent(name)}`, {
        method: "PUT",
        body: JSON.stringify({ path }),
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
}: {
  label: string;
  children: React.ReactNode;
  narrow?: boolean;
}) {
  return (
    <label className={cn("block", narrow && "max-w-[160px]")}>
      <span className="mb-1 block text-[11px] font-semibold uppercase tracking-wider text-ink-dim">
        {label}
      </span>
      {children}
    </label>
  );
}
