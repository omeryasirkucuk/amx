/**
 * HistoryStoreCard — enable / disable the shared team history store.
 *
 * Extracted from System.tsx so it can be reused in the Settings
 * "Team workspace" tab without pulling the entire System page into scope.
 */

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { RefreshCw, Users } from "lucide-react";

import { Card, CardBody, CardHeader } from "./Card";
import StatusPill from "./StatusPill";
import { api, apiFetch } from "../lib/api";
import { Button, InfoHint, useToast } from "./ui";

interface HistoryStoreStatus {
  enabled: boolean;
  profile: string;
  schema: string;
  outbox_pending: number;
}

export default function HistoryStoreCard() {
  const qc = useQueryClient();
  const toast = useToast();
  const status = useQuery({
    queryKey: ["history-store-status"],
    queryFn: () =>
      apiFetch<HistoryStoreStatus>("/api/admin/history-store-status"),
    retry: false,
  });
  const dbProfiles = useQuery({
    queryKey: ["profiles", "db", "list"],
    queryFn: () =>
      apiFetch<{ profiles: Array<{ name: string }> }>("/api/profiles/db"),
    retry: false,
  });

  const [draftProfile, setDraftProfile] = useState("");
  const [draftSchema, setDraftSchema] = useState("AMX");

  const enable = useMutation({
    mutationFn: (vars: { profile: string; schema: string }) =>
      api.enableHistoryStore({ profile: vars.profile, schema: vars.schema }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["history-store-status"] });
      toast.push({
        title: "Team history store enabled",
        description: "Future runs will dual-write to the team schema.",
        tone: "success",
      });
      setDraftProfile("");
    },
    onError: (e: Error) =>
      toast.push({
        title: "Could not enable",
        description: e.message,
        tone: "error",
      }),
  });

  const disable = useMutation({
    mutationFn: () => api.disableHistoryStore(),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["history-store-status"] });
      toast.push({
        title: "Team history store disabled",
        description: "Runs revert to local-only writes.",
        tone: "info",
      });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Could not disable",
        description: e.message,
        tone: "error",
      }),
  });

  return (
    <Card>
      <CardHeader
        title={
          <span className="inline-flex items-center gap-2">
            <Users size={16} className="text-accent" />
            Team history store
            <InfoHint text="When enabled, every /run dual-writes to a shared team database so colleagues can replay each other's runs. Configure from CLI with `/team enable`; this card is read-only." />
          </span>
        }
        actions={
          <button
            type="button"
            onClick={() => status.refetch()}
            className="inline-flex items-center gap-1 rounded-md bg-surface-subtle px-2.5 py-1 text-xs text-ink-muted hover:bg-surface-border"
          >
            <RefreshCw size={12} />
            Refresh
          </button>
        }
      />
      <CardBody>
        {status.isLoading ? (
          <div className="text-sm text-ink-dim">Loading…</div>
        ) : status.error ? (
          <div className="text-sm text-critical">
            {(status.error as Error).message}
          </div>
        ) : status.data ? (
          <div className="space-y-3 text-sm">
            <div className="flex flex-wrap items-center gap-2">
              <StatusPill tone={status.data.enabled ? "positive" : "neutral"}>
                {status.data.enabled ? "Active" : "Disabled"}
              </StatusPill>
              {status.data.outbox_pending > 0 && (
                <StatusPill tone="warning">
                  {status.data.outbox_pending} retry queue
                </StatusPill>
              )}
              {status.data.enabled && status.data.outbox_pending === 0 && (
                <StatusPill tone="positive">in sync</StatusPill>
              )}
            </div>
            <p className="text-xs text-ink-muted">
              When enabled, every <code className="font-mono">/run</code>{" "}
              dual-writes its history into a Postgres/MySQL schema your
              teammates also point at, so anyone can replay each other&apos;s
              runs. Reads stay local; only writes are mirrored.
            </p>
            <dl className="grid grid-cols-1 gap-2 text-xs sm:grid-cols-2">
              <div className="rounded-md border border-border bg-surface-subtle/40 px-3 py-2">
                <dt className="text-[10px] uppercase tracking-wider text-ink-dim">
                  Target profile
                </dt>
                <dd className="mt-0.5 font-mono text-ink">
                  {status.data.profile || "—"}
                </dd>
              </div>
              <div className="rounded-md border border-border bg-surface-subtle/40 px-3 py-2">
                <dt className="text-[10px] uppercase tracking-wider text-ink-dim">
                  Target schema
                </dt>
                <dd className="mt-0.5 font-mono text-ink">
                  {status.data.schema || "—"}
                </dd>
              </div>
              <div className="rounded-md border border-border bg-surface-subtle/40 px-3 py-2">
                <dt className="text-[10px] uppercase tracking-wider text-ink-dim">
                  Outbox depth
                </dt>
                <dd className="mt-0.5 font-mono tabular-nums text-ink">
                  {status.data.outbox_pending}
                </dd>
              </div>
              <div className="rounded-md border border-border bg-surface-subtle/40 px-3 py-2">
                <dt className="text-[10px] uppercase tracking-wider text-ink-dim">
                  Mode
                </dt>
                <dd className="mt-0.5 font-mono text-ink">
                  {status.data.enabled ? "dual-write" : "local-only"}
                </dd>
              </div>
            </dl>
            {!status.data.enabled ? (
              <div className="space-y-2 rounded-md border border-border bg-surface-subtle/30 px-3 py-2.5 text-xs text-ink-muted">
                <p className="font-medium text-ink">Enable team history store</p>
                <p className="text-[11px] text-ink-dim">
                  Pick a DB profile to dual-write into. The schema is
                  bootstrapped lazily on the next run if it doesn&apos;t
                  exist yet.
                </p>
                <div className="grid grid-cols-1 gap-2 sm:grid-cols-[2fr_1fr_auto]">
                  <select
                    value={draftProfile}
                    onChange={(e) => setDraftProfile(e.target.value)}
                    className="h-8 rounded-md border border-border bg-surface-raised px-2 text-xs text-ink focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20"
                  >
                    <option value="">Select target profile…</option>
                    {(dbProfiles.data?.profiles ?? []).map((p) => (
                      <option key={p.name} value={p.name}>
                        {p.name}
                      </option>
                    ))}
                  </select>
                  <input
                    type="text"
                    value={draftSchema}
                    onChange={(e) => setDraftSchema(e.target.value)}
                    placeholder="Schema name"
                    className="h-8 rounded-md border border-border bg-surface-raised px-2 font-mono text-xs text-ink focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20"
                  />
                  <Button
                    variant="primary"
                    size="sm"
                    loading={enable.isPending}
                    disabled={!draftProfile || !draftSchema || enable.isPending}
                    onClick={() =>
                      enable.mutate({
                        profile: draftProfile,
                        schema: draftSchema,
                      })
                    }
                  >
                    Enable
                  </Button>
                </div>
                <p className="text-[10.5px] text-ink-dim">
                  Or run <code className="font-mono">/history-store enable</code> in the CLI for the
                  full guided wizard (creates DB roles, validates connectivity, etc.).
                </p>
              </div>
            ) : (
              <div className="flex flex-wrap items-center justify-between gap-2 rounded-md border border-border bg-surface-subtle/30 px-3 py-2 text-xs">
                <span className="text-ink-muted">
                  Dual-write is on. Switch back to local-only with the button.
                </span>
                <Button
                  variant="subtle"
                  size="sm"
                  loading={disable.isPending}
                  onClick={() => disable.mutate()}
                >
                  Disable
                </Button>
              </div>
            )}
            {status.data.enabled && status.data.outbox_pending > 0 && (
              <div className="rounded-md border border-warning/40 bg-warning-soft px-3 py-2 text-xs text-warning">
                <span className="font-medium">
                  {status.data.outbox_pending} dual-writes queued for retry.
                </span>{" "}
                Run{" "}
                <code className="font-mono">/history-store flush-pending</code>{" "}
                to drain the outbox.
              </div>
            )}
          </div>
        ) : null}
      </CardBody>
    </Card>
  );
}
