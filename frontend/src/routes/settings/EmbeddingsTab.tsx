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

type Side = "docs" | "code" | "assets";

const SIDES: Side[] = ["docs", "code", "assets"];

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
    <EmbeddingPanel
      kinds={kinds}
      presets={presets}
      initial={settings}
      status={status}
    />
  );
}

interface PanelProps {
  kinds: KindDescriptor[];
  presets: Preset[];
  initial: Record<Side, EmbeddingState>;
  status?: Record<Side, SideStatus>;
}

function EmbeddingPanel({ kinds, presets, initial, status }: PanelProps) {
  const qc = useQueryClient();
  // Single embedding form drives all three sides (docs / code / assets).
  // Seed from the docs side; the other two are assumed to be in sync
  // (the save handler fans the same payload out to every side, so they
  // converge on the next click). If a user has edited config.yml by
  // hand to split them, the next Save here re-aligns them — that is
  // the documented expectation in the AMX docs ("Studio embedding
  // settings apply to every RAG store").
  const seed = initial.docs;
  const [kind, setKind] = useState<string>(seed.kind || "minilm");
  const [model, setModel] = useState<string>(seed.model || "");
  const [baseUrl, setBaseUrl] = useState<string>(seed.base_url || "");
  const [apiKey, setApiKey] = useState<string>(seed.api_key || "");
  const [testResult, setTestResult] = useState<
    { ok: boolean; message: string; dim?: number } | null
  >(null);
  const [rebuildConfirm, setRebuildConfirm] = useState(false);

  // Keep local state in sync when the upstream query refreshes.
  useEffect(() => {
    setKind(seed.kind || "minilm");
    setModel(seed.model || "");
    setBaseUrl(seed.base_url || "");
    setApiKey(seed.api_key || "");
  }, [seed.kind, seed.model, seed.base_url, seed.api_key]);

  const kindDescriptor = kinds.find((k) => k.id === kind) ?? kinds[0];
  const needsModel = kindDescriptor?.needs_model ?? false;
  const needsKey = kindDescriptor?.needs_key ?? false;
  const needsBase = kindDescriptor?.needs_base ?? false;

  const payload = () => ({
    kind,
    model: needsModel ? model : "",
    base_url: needsBase ? baseUrl : "",
    api_key: needsKey ? apiKey : "",
  });

  const saveMutation = useMutation({
    mutationFn: async () => {
      const body = payload();
      // Fan-out to all three sides in parallel so a single Save
      // converges docs/code/assets onto the same model.
      const results = await Promise.all(
        SIDES.map((side) =>
          apiFetch<EmbeddingState>(`/api/profiles/embedding/${side}`, {
            method: "PUT",
            body: JSON.stringify(body),
          }),
        ),
      );
      return results;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["embedding"] });
      qc.invalidateQueries({ queryKey: ["embedding", "status"] });
    },
  });

  const testMutation = useMutation({
    mutationFn: () =>
      apiFetch<{ ok: boolean; message: string; dim?: number }>(
        // Use the docs side as the canary — the test endpoint exercises
        // the embedding function in-process so the result is identical
        // for every side.
        `/api/profiles/embedding/docs/test`,
        { method: "POST", body: JSON.stringify(payload()) },
      ),
    onSuccess: (result) => setTestResult(result),
    onError: (error: Error) =>
      setTestResult({ ok: false, message: error.message || "Test failed" }),
  });

  const rebuildMutation = useMutation({
    mutationFn: async () => {
      const results = await Promise.all(
        SIDES.map((side) =>
          apiFetch<{ ok: boolean; message: string }>(
            `/api/profiles/embedding/${side}/rebuild`,
            { method: "POST", body: JSON.stringify({}) },
          ).catch((err: Error) => ({
            ok: false,
            message: `${side}: ${err.message}`,
          })),
        ),
      );
      return results;
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["embedding", "status"] });
      qc.invalidateQueries({ queryKey: ["profiles", "docs"] });
      qc.invalidateQueries({ queryKey: ["profiles", "code"] });
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

  // Aggregate status across all sides: total chunks across every
  // collection + stale flag if ANY side is mis-aligned with the
  // active embedding triple.
  const allCollections = SIDES.flatMap((side) => status?.[side]?.collections ?? []);
  const totalChunks = allCollections.reduce((acc, c) => acc + (c.count || 0), 0);
  const collectionCount = allCollections.length;
  const isStale = SIDES.some((side) => status?.[side]?.stale);

  return (
    <Card className="max-w-2xl">
      <CardHeader
        title="Embedding model"
        description="One model powers every RAG store — docs, code, and ingested assets. Save applies the same settings to all three."
      />
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
                    (disabled
                      ? " cursor-not-allowed opacity-50 hover:border-surface-border"
                      : "")
                  }
                >
                  <input
                    type="radio"
                    name="embedding-kind"
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
            <Badge tone="positive">Saved to docs, code, and assets</Badge>
          )}
          {saveMutation.error && (
            <Badge tone="critical">{(saveMutation.error as Error).message}</Badge>
          )}
        </div>

        {isStale && (
          <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-sm">
            <div className="flex items-start gap-2">
              <AlertTriangle size={14} className="mt-0.5 text-amber-500" />
              <div className="flex-1">
                <div className="font-medium">Vectors are stale</div>
                <div className="text-xs text-ink-dim">
                  {collectionCount} collection
                  {collectionCount === 1 ? "" : "s"} ·{" "}
                  {totalChunks.toLocaleString()} chunks embedded with a different
                  provider. Rebuild to re-embed every RAG store under the active
                  settings.
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
                        <RefreshCw size={13} className="mr-1" /> Rebuild all
                      </>
                    )}
                  </Button>
                </div>
              </div>
            </div>
          </div>
        )}
      </CardBody>

      <AlertDialog
        open={rebuildConfirm}
        title="Rebuild every RAG store?"
        description={
          <span>
            Drops the existing Chroma collections (docs, code, and assets) so
            the next ingest / query / sync re-embeds with the active provider.
            Existing chunks are deleted; re-ingestion is required. Tens of
            seconds for catalogs of a few thousand chunks.
          </span>
        }
        tone="danger"
        confirmLabel="Rebuild all"
        loading={rebuildMutation.isPending}
        onConfirm={() => rebuildMutation.mutate()}
        onClose={() => setRebuildConfirm(false)}
      />
    </Card>
  );
}
