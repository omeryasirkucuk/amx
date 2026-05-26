import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, ChevronDown, ChevronRight, Copy, Loader2, Plug, Unplug } from "lucide-react";

import { Card, CardBody, CardHeader } from "../../components/Card";
import { Badge, Button, Checkbox, useToast } from "../../components/ui";
import { apiFetch } from "../../lib/api";

interface IdeStatus {
  ide: string;
  label: string;
  config_path: string;
  connected: boolean;
  drifted: boolean;
  profiles: string[];
  error: string | null;
}

interface McpStatus {
  ides: IdeStatus[];
  tool_count: number;
  available_profiles: string[];
  active_profiles: string[];
}

interface ConnectResult {
  ok: boolean;
  label: string;
  config_path: string;
  post_connect_steps: string[];
  status: IdeStatus;
}

interface ToolInfo {
  name: string;
  description: string;
}

export default function McpTab() {
  const qc = useQueryClient();
  const toast = useToast();
  const statusQuery = useQuery<McpStatus>({
    queryKey: ["mcp", "status"],
    queryFn: () => apiFetch<McpStatus>("/api/mcp/status"),
  });

  if (statusQuery.isPending) {
    return (
      <div className="flex items-center gap-2 py-8 text-sm text-ink-dim">
        <Loader2 size={14} className="animate-spin" /> Loading MCP settings…
      </div>
    );
  }
  if (statusQuery.error || !statusQuery.data) {
    return (
      <div className="rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-sm text-critical">
        Failed to load MCP settings.
      </div>
    );
  }

  const data = statusQuery.data;
  const refresh = () => qc.invalidateQueries({ queryKey: ["mcp"] });

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader
          title="Connect AMX to your IDE"
          description="Expose AMX's catalog to IDE code agents over the Model Context Protocol (MCP)."
        />
        <CardBody>
          <p className="text-sm text-ink-dim">
            Once connected, your IDE's AI agent can read AMX's schemas, descriptions, join keys,
            and lineage — so it writes data code grounded in your real catalog. AMX exposes{" "}
            <span className="font-medium text-ink">{data.tool_count} read-only tools</span> (cached
            catalog only — no live database access, no credentials needed).
          </p>
        </CardBody>
      </Card>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        {data.ides.map((ide) => (
          <IdeCard
            key={ide.ide}
            ide={ide}
            availableProfiles={data.available_profiles}
            onChanged={refresh}
            toast={toast}
          />
        ))}
      </div>

      <ExposedToolsCard />
    </div>
  );
}

function statusBadge(ide: IdeStatus) {
  if (ide.error) return <Badge tone="critical">config error</Badge>;
  if (!ide.connected) return <Badge tone="neutral">not connected</Badge>;
  if (ide.drifted) return <Badge tone="warning">needs repair</Badge>;
  return (
    <Badge tone="positive" dot>
      connected
    </Badge>
  );
}

function IdeCard({
  ide,
  availableProfiles,
  onChanged,
  toast,
}: {
  ide: IdeStatus;
  availableProfiles: string[];
  onChanged: () => void;
  toast: ReturnType<typeof useToast>;
}) {
  const [customScope, setCustomScope] = useState(false);
  const [selected, setSelected] = useState<string[]>([]);
  const [steps, setSteps] = useState<string[] | null>(null);
  const [snippet, setSnippet] = useState<string | null>(null);

  const connect = useMutation({
    mutationFn: () =>
      apiFetch<ConnectResult>("/api/mcp/connect", {
        method: "POST",
        body: JSON.stringify({
          ide: ide.ide,
          profiles: customScope && selected.length > 0 ? selected : null,
        }),
      }),
    onSuccess: (res) => {
      setSteps(res.post_connect_steps);
      toast.push({ title: `Connected to ${res.label}`, tone: "success" });
      onChanged();
    },
    onError: (err: unknown) => {
      toast.push({ title: `Connect failed: ${(err as Error).message}`, tone: "error" });
    },
  });

  const disconnect = useMutation({
    mutationFn: () =>
      apiFetch<{ ok: boolean }>("/api/mcp/disconnect", {
        method: "POST",
        body: JSON.stringify({ ide: ide.ide }),
      }),
    onSuccess: () => {
      setSteps(null);
      toast.push({ title: `Removed AMX from ${ide.label}`, tone: "info" });
      onChanged();
    },
    onError: (err: unknown) => {
      toast.push({ title: `Disconnect failed: ${(err as Error).message}`, tone: "error" });
    },
  });

  const loadSnippet = async () => {
    if (snippet !== null) {
      setSnippet(null);
      return;
    }
    const scope = customScope && selected.length > 0 ? `&profiles=${selected.join(",")}` : "";
    const res = await apiFetch<{ snippet: string }>(`/api/mcp/snippet?ide=${ide.ide}${scope}`);
    setSnippet(res.snippet);
  };

  const toggleProfile = (name: string) =>
    setSelected((prev) =>
      prev.includes(name) ? prev.filter((p) => p !== name) : [...prev, name],
    );

  return (
    <Card>
      <CardHeader
        title={
          <span className="flex items-center justify-between gap-2">
            <span>{ide.label}</span>
            {statusBadge(ide)}
          </span>
        }
      />
      <CardBody className="space-y-3">
        <p className="break-all text-xs text-ink-muted">{ide.config_path}</p>

        {ide.connected && (
          <p className="text-xs text-ink-dim">
            Scope: {ide.profiles.length > 0 ? ide.profiles.join(", ") : "active profiles"}
            {ide.drifted && (
              <span className="text-warning">
                {" "}
                · interpreter path changed — reconnect to repair
              </span>
            )}
          </p>
        )}

        {availableProfiles.length > 0 && (
          <div className="space-y-2">
            <Checkbox
              checked={customScope}
              onChange={(e) => setCustomScope(e.currentTarget.checked)}
              label="Customize profile scope"
              description="Default exposes your active profiles."
            />
            {customScope && (
              <div className="grid grid-cols-1 gap-1.5 pl-6 sm:grid-cols-2">
                {availableProfiles.map((p) => (
                  <Checkbox
                    key={p}
                    checked={selected.includes(p)}
                    onChange={() => toggleProfile(p)}
                    label={p}
                  />
                ))}
              </div>
            )}
          </div>
        )}

        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="primary"
            size="sm"
            onClick={() => connect.mutate()}
            disabled={connect.isPending}
          >
            {connect.isPending ? (
              <Loader2 size={13} className="animate-spin" />
            ) : (
              <Plug size={13} />
            )}
            {ide.connected ? "Reconnect" : "Connect"}
          </Button>
          {ide.connected && (
            <Button
              variant="danger"
              size="sm"
              onClick={() => disconnect.mutate()}
              disabled={disconnect.isPending}
            >
              <Unplug size={13} /> Disconnect
            </Button>
          )}
          <Button variant="ghost" size="sm" onClick={loadSnippet}>
            {snippet !== null ? "Hide" : "Show"} snippet
          </Button>
        </div>

        {snippet !== null && <SnippetBlock snippet={snippet} toast={toast} />}

        {steps && (
          <div className="rounded-md border border-positive/30 bg-positive/10 px-3 py-2 text-xs text-ink">
            <p className="mb-1 flex items-center gap-1.5 font-medium text-positive">
              <CheckCircle2 size={13} /> Next steps
            </p>
            <ol className="list-decimal space-y-0.5 pl-4 text-ink-dim">
              {steps.map((s, i) => (
                <li key={i}>{s}</li>
              ))}
            </ol>
          </div>
        )}
      </CardBody>
    </Card>
  );
}

function SnippetBlock({
  snippet,
  toast,
}: {
  snippet: string;
  toast: ReturnType<typeof useToast>;
}) {
  return (
    <div className="relative">
      <pre className="max-h-64 overflow-auto rounded-md border border-surface-border bg-surface-subtle px-3 py-2 text-xs text-ink-dim">
        {snippet}
      </pre>
      <Button
        variant="subtle"
        size="sm"
        className="absolute right-2 top-2"
        onClick={() => {
          void navigator.clipboard?.writeText(snippet);
          toast.push({ title: "Snippet copied", tone: "success" });
        }}
      >
        <Copy size={12} /> Copy
      </Button>
    </div>
  );
}

function ExposedToolsCard() {
  const [open, setOpen] = useState(false);
  const toolsQuery = useQuery<{ tools: ToolInfo[]; count: number }>({
    queryKey: ["mcp", "tools"],
    queryFn: () => apiFetch<{ tools: ToolInfo[]; count: number }>("/api/mcp/tools"),
    enabled: open,
  });

  return (
    <Card>
      <button
        type="button"
        className="flex w-full items-center gap-2 px-4 py-3 text-left text-sm font-medium text-ink"
        onClick={() => setOpen((v) => !v)}
      >
        {open ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
        Exposed tools (read-only)
      </button>
      {open && (
        <CardBody className="border-t border-surface-border">
          {toolsQuery.isPending ? (
            <div className="flex items-center gap-2 text-sm text-ink-dim">
              <Loader2 size={14} className="animate-spin" /> Loading…
            </div>
          ) : (
            <ul className="space-y-2">
              {toolsQuery.data?.tools.map((t) => (
                <li key={t.name} className="text-sm">
                  <code className="text-accent">{t.name}</code>
                  <p className="text-xs text-ink-muted">{t.description}</p>
                </li>
              ))}
            </ul>
          )}
        </CardBody>
      )}
    </Card>
  );
}
