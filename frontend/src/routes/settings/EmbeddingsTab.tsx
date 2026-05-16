import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, CheckCircle2, Loader2, RefreshCw, XCircle } from "lucide-react";

import { Card, CardBody, CardHeader } from "../../components/Card";
import {
  AlertDialog,
  Badge,
  Button,
  Field,
  Input,
  Select,
} from "../../components/ui";
import { apiFetch } from "../../lib/api";

type Side = "docs" | "code";

interface KindDescriptor {
  id: string;
  label: string;
  needs_model: boolean;
  needs_key: boolean;
  needs_base: boolean;
  hint: string;
  available: boolean;
}

interface Preset {
  id: string;
  label: string;
  base_url: string;
}

interface KindsPayload {
  kinds: KindDescriptor[];
  presets: Preset[];
  sides: Side[];
  supported_kinds: string[];
}

interface EmbeddingState {
  kind: string;
  model: string;
  api_key: string;
  base_url: string;
  is_configured: boolean;
}

interface CollectionStatus {
  name: string;
  count: number;
  embedding_provider: string;
  embedding_model: string;
  stale: boolean;
}

interface SideStatus {
  collections: CollectionStatus[];
  stale: boolean;
  current_provider?: string;
  current_model?: string;
  error?: string;
}

const SIDE_TITLES: Record<Side, string> = {
  docs: "Docs RAG embedding",
  code: "Code RAG embedding",
};

const SIDE_BLURBS: Record<Side, string> = {
  docs: "Vectorises documentation chunks for /search and /ask retrieval.",
  code: "Vectorises code snippets for /code search and the code agent.",
};

const SECRET_PLACEHOLDER = "********";

export default function EmbeddingsTab() {
  const kindsQuery = useQuery<KindsPayload>({
    queryKey: ["embedding", "kinds"],
    queryFn: () => apiFetch<KindsPayload>("/api/profiles/embedding/kinds"),
    staleTime: 1000 * 60 * 10,
  });
  const settingsQuery = useQuery<Record<Side, EmbeddingState>>({
    queryKey: ["embedding"],
    queryFn: () => apiFetch<Record<Side, EmbeddingState>>("/api/profiles/embedding"),
  });
  const statusQuery = useQuery<Record<Side, SideStatus>>({
    queryKey: ["embedding", "status"],
    queryFn: () => apiFetch<Record<Side, SideStatus>>("/api/profiles/embedding/status"),
  });

  if (kindsQuery.isPending || settingsQuery.isPending) {
    return (
      <div className="flex items-center gap-2 py-8 text-sm text-ink-dim">
        <Loader2 size={14} className="animate-spin" /> Loading embedding settings…
      </div>
    );
  }
  if (kindsQuery.error || settingsQuery.error) {
    return (
      <div className="rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-sm text-critical">
        Failed to load embedding settings.
      </div>
    );
  }

  const kinds = kindsQuery.data!.kinds;
  const presets = kindsQuery.data!.presets;
  const settings = settingsQuery.data!;
  const status = statusQuery.data;

  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
      {(Object.keys(SIDE_TITLES) as Side[]).map((side) => (
        <EmbeddingPanel
          key={side}
          side={side}
          kinds={kinds}
          presets={presets}
          initial={settings[side]}
          status={status?.[side]}
        />
      ))}
    </div>
  );
}

interface PanelProps {
  side: Side;
  kinds: KindDescriptor[];
  presets: Preset[];
  initial: EmbeddingState;
  status?: SideStatus;
}

function EmbeddingPanel({ side, kinds, presets, initial, status }: PanelProps) {
  const qc = useQueryClient();
  const [kind, setKind] = useState<string>(initial.kind || "minilm");
  const [model, setModel] = useState<string>(initial.model || "");
  const [baseUrl, setBaseUrl] = useState<string>(initial.base_url || "");
  // API key starts as the masked placeholder when a secret is on file —
  // sending the placeholder back means "leave unchanged"; clearing it
  // means "clear the secret".
  const [apiKey, setApiKey] = useState<string>(initial.api_key || "");
  const [testResult, setTestResult] = useState<{ ok: boolean; message: string; dim?: number } | null>(null);
  const [rebuildConfirm, setRebuildConfirm] = useState(false);

  // Keep local state in sync if the upstream query refreshes (e.g.
  // after another user / panel edits this side via the same SPA).
  useEffect(() => {
    setKind(initial.kind || "minilm");
    setModel(initial.model || "");
    setBaseUrl(initial.base_url || "");
    setApiKey(initial.api_key || "");
  }, [initial.kind, initial.model, initial.base_url, initial.api_key]);

  const kindDescriptor = kinds.find((k) => k.id === kind) ?? kinds[0];
  const needsModel = kindDescriptor?.needs_model ?? false;
  const needsKey = kindDescriptor?.needs_key ?? false;
  const needsBase = kindDescriptor?.needs_base ?? false;

  const saveMutation = useMutation({
    mutationFn: () =>
      apiFetch<EmbeddingState>(`/api/profiles/embedding/${side}`, {
        method: "PUT",
        body: JSON.stringify({
          kind,
          model: needsModel ? model : "",
          base_url: needsBase ? baseUrl : "",
          api_key: needsKey ? apiKey : "",
        }),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["embedding"] });
      qc.invalidateQueries({ queryKey: ["embedding", "status"] });
    },
  });

  const testMutation = useMutation({
    mutationFn: () =>
      apiFetch<{ ok: boolean; message: string; dim?: number }>(
        `/api/profiles/embedding/${side}/test`,
        {
          method: "POST",
          body: JSON.stringify({
            kind,
            model: needsModel ? model : "",
            base_url: needsBase ? baseUrl : "",
            api_key: needsKey ? apiKey : "",
          }),
        },
      ),
    onSuccess: (result) => setTestResult(result),
    onError: (error: Error) =>
      setTestResult({ ok: false, message: error.message || "Test failed" }),
  });

  const rebuildMutation = useMutation({
    mutationFn: () =>
      apiFetch<{ ok: boolean; message: string }>(
        `/api/profiles/embedding/${side}/rebuild`,
        { method: "POST", body: JSON.stringify({}) },
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["embedding", "status"] });
      if (side === "docs") qc.invalidateQueries({ queryKey: ["profiles", "docs"] });
      if (side === "code") qc.invalidateQueries({ queryKey: ["profiles", "code"] });
    },
    onSettled: () => setRebuildConfirm(false),
  });

  const onPickPreset = (presetId: string) => {
    const preset = presets.find((p) => p.id === presetId);
    if (preset) setBaseUrl(preset.base_url);
  };

  const ready = (() => {
    if (kind === "minilm") return true;
    if (needsModel && !model.trim()) return false;
    return true;
  })();

  const totalChunks = (status?.collections ?? []).reduce((acc, c) => acc + (c.count || 0), 0);
  const collectionCount = status?.collections?.length ?? 0;

  return (
    <Card>
      <CardHeader title={SIDE_TITLES[side]} description={SIDE_BLURBS[side]} />
      <CardBody className="flex flex-col gap-4">
        <fieldset className="flex flex-col gap-2">
          <legend className="text-xs font-medium text-ink-muted">Provider</legend>
          <div className="flex flex-col gap-1.5">
            {kinds.map((k) => {
              const disabled = !k.available;
              return (
                <label
                  key={k.id}
                  className={
                    "flex cursor-pointer items-start gap-2 rounded-md border border-surface-border px-3 py-2 text-sm transition hover:border-accent " +
                    (kind === k.id ? "border-accent bg-accent/5" : "") +
                    (disabled ? " cursor-not-allowed opacity-50 hover:border-surface-border" : "")
                  }
                >
                  <input
                    type="radio"
                    name={`kind-${side}`}
                    value={k.id}
                    checked={kind === k.id}
                    disabled={disabled}
                    onChange={() => setKind(k.id)}
                    className="mt-1"
                  />
                  <div className="flex flex-col">
                    <span className="font-medium">{k.label}</span>
                    <span className="text-xs text-ink-dim">{k.hint}</span>
                  </div>
                </label>
              );
            })}
          </div>
        </fieldset>

        {kind === "openai_compatible" && (
          <Field label="Preset (optional)">
            <Select
              value=""
              onChange={(e) => onPickPreset((e.target as HTMLSelectElement).value)}
            >
              <option value="">Pick a preset…</option>
              {presets.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.label}
                </option>
              ))}
            </Select>
          </Field>
        )}

        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          {needsModel && (
            <Field
              label="Model"
              required
              description={
                kind === "openai_compatible"
                  ? "e.g. text-embedding-3-small"
                  : "HuggingFace model id, e.g. BAAI/bge-large-en-v1.5"
              }
            >
              <Input
                value={model}
                onChange={(e) => setModel(e.target.value)}
                placeholder="model-id"
              />
            </Field>
          )}
          {needsBase && (
            <Field label="Base URL" description="The /embeddings endpoint.">
              <Input
                value={baseUrl}
                onChange={(e) => setBaseUrl(e.target.value)}
                placeholder="https://api.openai.com/v1"
              />
            </Field>
          )}
          {needsKey && (
            <Field
              label="API key"
              description="Stored in the OS keyring, never in YAML."
            >
              <Input
                type="password"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder={SECRET_PLACEHOLDER}
              />
            </Field>
          )}
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="secondary"
            disabled={!ready || testMutation.isPending}
            onClick={() => testMutation.mutate()}
          >
            {testMutation.isPending ? (
              <>
                <Loader2 size={13} className="mr-1 animate-spin" /> Testing…
              </>
            ) : (
              "Test connection"
            )}
          </Button>
          <Button
            variant="primary"
            disabled={!ready || saveMutation.isPending}
            onClick={() => saveMutation.mutate()}
          >
            {saveMutation.isPending ? (
              <>
                <Loader2 size={13} className="mr-1 animate-spin" /> Saving…
              </>
            ) : (
              "Save"
            )}
          </Button>
          {testResult && (
            <Badge tone={testResult.ok ? "positive" : "critical"}>
              {testResult.ok ? <CheckCircle2 size={11} /> : <XCircle size={11} />}{" "}
              {testResult.ok
                ? `OK${testResult.dim ? ` · dim ${testResult.dim}` : ""}`
                : testResult.message}
            </Badge>
          )}
          {saveMutation.isSuccess && !saveMutation.isPending && (
            <Badge tone="positive">Saved</Badge>
          )}
          {saveMutation.error && (
            <Badge tone="critical">{(saveMutation.error as Error).message}</Badge>
          )}
        </div>

        {status?.stale && (
          <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-sm">
            <div className="flex items-start gap-2">
              <AlertTriangle size={14} className="mt-0.5 text-amber-500" />
              <div className="flex-1">
                <div className="font-medium">Vectors are stale</div>
                <div className="text-xs text-ink-dim">
                  {collectionCount} collection
                  {collectionCount === 1 ? "" : "s"} · {totalChunks.toLocaleString()} chunks
                  embedded with a different provider. Rebuild to re-embed with the
                  active settings.
                </div>
                <div className="mt-2">
                  <Button
                    variant="secondary"
                    onClick={() => setRebuildConfirm(true)}
                    disabled={rebuildMutation.isPending}
                  >
                    {rebuildMutation.isPending ? (
                      <>
                        <Loader2 size={13} className="mr-1 animate-spin" /> Rebuilding…
                      </>
                    ) : (
                      <>
                        <RefreshCw size={13} className="mr-1" /> Rebuild{" "}
                        {side === "docs" ? "docs" : "code"}
                      </>
                    )}
                  </Button>
                </div>
              </div>
            </div>
          </div>
        )}

        <AlertDialog
          open={rebuildConfirm}
          title={`Rebuild ${side} embeddings?`}
          description={
            `This clears ${collectionCount} collection${collectionCount === 1 ? "" : "s"} ` +
            `(${totalChunks.toLocaleString()} chunks) and re-embeds them with the active provider. ` +
            `Existing data is preserved; only the vector index is rebuilt.`
          }
          confirmLabel="Rebuild"
          onConfirm={() => rebuildMutation.mutate()}
          onClose={() => setRebuildConfirm(false)}
          loading={rebuildMutation.isPending}
          tone="danger"
        />
      </CardBody>
    </Card>
  );
}
