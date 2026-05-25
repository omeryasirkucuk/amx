import { useMutation, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, CheckCircle2, Loader2, PackageX, RefreshCw } from "lucide-react";
import { useState } from "react";

import { Card, CardBody, CardHeader } from "../../components/Card";
import { AlertDialog, Badge, Button } from "../../components/ui";
import { apiFetch } from "../../lib/api";

export type Side = "docs" | "code" | "assets";

export const SIDES: Side[] = ["docs", "code", "assets"];

const SIDE_LABEL: Record<Side, string> = {
  docs: "Catalog / docs",
  code: "Code",
  assets: "Assets",
};

export interface CollectionStatus {
  name: string;
  count: number;
  embedding_provider: string;
  embedding_model: string;
  stale: boolean;
}

export interface SideStatus {
  collections: CollectionStatus[];
  stale: boolean;
  current_provider?: string;
  current_model?: string;
  // Enriched (unified embedding management).
  configured_provider?: string;
  configured_model?: string;
  fell_back?: boolean;
  fallback_reason?: string | null;
  dependency_available?: boolean;
  needs_rebuild?: boolean;
  error?: string;
}

function modelLabel(provider?: string, model?: string): string {
  const p = (provider || "").trim();
  const m = (model || "").trim();
  if (!p && !m) return "—";
  if (!m || m === p) return p;
  return `${p} · ${m}`;
}

function chunkCount(s: SideStatus): number {
  return (s.collections ?? []).reduce((acc, c) => acc + (c.count || 0), 0);
}

/** A side is "fell back" when its configured model couldn't build and it
 * silently runs a default instead — rebuilding does NOT fix this (the
 * dependency is still missing); the config/dependency must change first. */
function sideTone(s: SideStatus): { tone: "positive" | "warning" | "critical"; label: string } {
  if (s.fell_back) return { tone: "critical", label: "Fallback" };
  if (s.stale) return { tone: "warning", label: "Stale" };
  return { tone: "positive", label: "OK" };
}

interface Props {
  status?: Record<Side, SideStatus>;
  loading?: boolean;
}

export default function EmbeddingHealth({ status, loading }: Props) {
  const qc = useQueryClient();
  const [rebuildConfirm, setRebuildConfirm] = useState(false);

  const rebuildAll = useMutation({
    mutationFn: () =>
      apiFetch<{ ok: boolean; message: string; failed: string[] }>(
        "/api/profiles/embedding/rebuild",
        { method: "POST", body: "{}" },
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["embedding", "status"] });
      qc.invalidateQueries({ queryKey: ["profiles", "docs"] });
      qc.invalidateQueries({ queryKey: ["profiles", "code"] });
    },
    onSettled: () => setRebuildConfirm(false),
  });

  const sides = status ? SIDES.filter((s) => status[s]) : [];
  const anyStale = sides.some((s) => status?.[s]?.stale);
  const fellBackSides = sides.filter((s) => status?.[s]?.fell_back);
  // The reason is identical across sides (same missing dependency), so
  // surface the first one we find.
  const fallbackReason =
    fellBackSides.map((s) => status?.[s]?.fallback_reason).find(Boolean) || "";

  return (
    <Card className="max-w-2xl">
      <CardHeader
        title="Embedding health"
        description="What each RAG store is configured to use versus what it is actually running, and whether its vectors are current."
      />
      <CardBody className="flex flex-col gap-4">
        {loading && !status ? (
          <div className="flex items-center gap-2 py-4 text-sm text-ink-dim">
            <Loader2 size={14} className="animate-spin" /> Inspecting collections…
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-surface-border text-left text-xs text-ink-muted">
                  <th className="py-1.5 pr-3 font-medium">Store</th>
                  <th className="py-1.5 pr-3 font-medium">Configured</th>
                  <th className="py-1.5 pr-3 font-medium">Running</th>
                  <th className="py-1.5 pr-3 text-right font-medium">Chunks</th>
                  <th className="py-1.5 font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {sides.map((side) => {
                  const s = status![side];
                  const { tone, label } = sideTone(s);
                  const configured = modelLabel(s.configured_provider, s.configured_model);
                  const running = modelLabel(s.current_provider, s.current_model);
                  const mismatch = s.fell_back && configured !== running;
                  return (
                    <tr key={side} className="border-b border-surface-border/60">
                      <td className="py-1.5 pr-3 font-medium">{SIDE_LABEL[side]}</td>
                      <td className="py-1.5 pr-3 text-ink-dim">{configured}</td>
                      <td className="py-1.5 pr-3">
                        <span className={mismatch ? "text-warning" : "text-ink-dim"}>
                          {running}
                        </span>
                      </td>
                      <td className="py-1.5 pr-3 text-right tabular-nums text-ink-dim">
                        {chunkCount(s).toLocaleString()}
                      </td>
                      <td className="py-1.5">
                        {s.error ? (
                          <Badge tone="neutral">{s.error}</Badge>
                        ) : (
                          <Badge tone={tone} dot>
                            {label}
                          </Badge>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {fellBackSides.length > 0 && (
          <div className="rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-sm">
            <div className="flex items-start gap-2">
              <PackageX size={14} className="mt-0.5 text-critical" />
              <div className="flex-1">
                <div className="font-medium">
                  Configured model isn’t running ({fellBackSides.map((s) => SIDE_LABEL[s]).join(", ")})
                </div>
                <div className="text-xs text-ink-dim">
                  The configured model couldn’t be loaded, so these stores fell
                  back to the bundled default. Rebuilding won’t help — it would
                  just re-embed under the fallback. Install the dependency or
                  pick an available model above, then rebuild.
                  {fallbackReason && (
                    <div className="mt-1 font-mono text-[11px] text-critical">
                      {fallbackReason}
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}

        {anyStale && (
          <div className="rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-sm">
            <div className="flex items-start gap-2">
              <AlertTriangle size={14} className="mt-0.5 text-warning" />
              <div className="flex-1">
                <div className="font-medium">Vectors are stale</div>
                <div className="text-xs text-ink-dim">
                  One or more stores were embedded with a different model than
                  the active one. Rebuild to re-embed every store under the
                  current settings.
                </div>
              </div>
            </div>
          </div>
        )}

        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant={anyStale ? "primary" : "secondary"}
            leadingIcon={<RefreshCw size={13} />}
            loading={rebuildAll.isPending}
            onClick={() => setRebuildConfirm(true)}
          >
            {rebuildAll.isPending ? "Rebuilding…" : "Rebuild all stores"}
          </Button>
          {rebuildAll.isSuccess && !rebuildAll.isPending && (
            <Badge tone={rebuildAll.data?.failed?.length ? "warning" : "positive"}>
              <CheckCircle2 size={11} /> {rebuildAll.data?.message}
            </Badge>
          )}
          {rebuildAll.error && (
            <Badge tone="critical">{(rebuildAll.error as Error).message}</Badge>
          )}
        </div>
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
        loading={rebuildAll.isPending}
        onConfirm={() => rebuildAll.mutate()}
        onClose={() => setRebuildConfirm(false)}
      />
    </Card>
  );
}
