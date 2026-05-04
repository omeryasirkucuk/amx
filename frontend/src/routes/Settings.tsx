import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle, Database, Sparkles, AlertCircle } from "lucide-react";

import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import StatusPill from "../components/StatusPill";
import EmptyState from "../components/EmptyState";
import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";

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

export default function Settings() {
  return (
    <>
      <PageHeader
        eyebrow="Configuration"
        title="Settings"
        description="DB and LLM profile management. Mutations here update ~/.amx/config.yml — the same file the CLI wizards write."
      />

      <div className="grid gap-4 lg:grid-cols-2">
        <DbProfilesCard />
        <LlmProfilesCard />
      </div>
    </>
  );
}

function DbProfilesCard() {
  const qc = useQueryClient();
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

  const test = useMutation({
    mutationFn: async (name: string) => {
      const result = await apiFetch<{ ok: boolean; message: string }>(
        `/api/profiles/db/${encodeURIComponent(name)}/test`,
        { method: "POST" },
      );
      return { name, ...result };
    },
    onSuccess: (r) => setTestResult((current) => ({ ...current, [r.name]: { ok: r.ok, message: r.message } })),
  });

  return (
    <Card>
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Database size={16} className="text-accent" />
            Database profiles
          </span>
        }
        description={`${profiles.data?.count ?? 0} configured · active: ${
          profiles.data?.active ?? "—"
        }`}
      />
      <CardBody className="p-0">
        {profiles.data?.profiles.length ? (
          <ul className="divide-y divide-surface-border">
            {profiles.data.profiles.map((p) => {
              const result = testResult[p.name];
              return (
                <li key={p.name} className="px-5 py-3 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="font-medium">{p.name}</div>
                      <div className="text-xs text-ink-dim">
                        {p.backend} · {p.host || "(no host)"}
                        {p.database ? ` · ${p.database}` : ""}
                        {p.catalog ? ` · ${p.catalog}` : ""}
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
                        onClick={() => test.mutate(p.name)}
                        className="rounded-md bg-surface-subtle px-2 py-1 text-xs text-ink-muted hover:bg-surface-border"
                      >
                        Test
                      </button>
                    </div>
                  </div>
                  {result && (
                    <div
                      className={cn(
                        "mt-2 inline-flex items-center gap-1.5 rounded-md px-2 py-1 text-[11px]",
                        result.ok
                          ? "bg-positive/10 text-positive"
                          : "bg-critical/10 text-critical",
                      )}
                    >
                      {result.ok ? <CheckCircle size={12} /> : <AlertCircle size={12} />}
                      {result.message || (result.ok ? "OK" : "Failed")}
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        ) : (
          <EmptyState
            icon={Database}
            title="No DB profiles yet"
            description="Run /add-db-profile from the CLI; the editor lands in a follow-up."
          />
        )}
      </CardBody>
    </Card>
  );
}

function LlmProfilesCard() {
  const qc = useQueryClient();

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

  return (
    <Card>
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Sparkles size={16} className="text-accent" />
            LLM profiles
          </span>
        }
        description={`${profiles.data?.count ?? 0} configured · active: ${
          profiles.data?.active ?? "—"
        }`}
      />
      <CardBody className="p-0">
        {profiles.data?.profiles.length ? (
          <ul className="divide-y divide-surface-border">
            {profiles.data.profiles.map((p) => (
              <li key={p.name} className="flex items-center justify-between gap-3 px-5 py-3 text-sm">
                <div>
                  <div className="font-medium">{p.name}</div>
                  <div className="text-xs text-ink-dim">
                    {p.provider || "—"} · {p.model || "—"}
                  </div>
                </div>
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
              </li>
            ))}
          </ul>
        ) : (
          <EmptyState
            icon={Sparkles}
            title="No LLM profiles yet"
            description="Run /add-llm-profile from the CLI; the editor lands in a follow-up."
          />
        )}
      </CardBody>
    </Card>
  );
}
