/**
 * Assets — remote code-asset browser.
 *
 * Six tabs: Notebooks / Jobs / Pipelines / Streamlit / Streams / Queries.
 * Profile picker at the top lets the user scope to any configured DB
 * profile. Row click opens the detail drawer. "Ingest assets" button
 * opens the ingest dialog.
 */

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Download, Loader2, Scissors, Search, Trash2 } from "lucide-react";

import PageHeader from "../components/PageHeader";
import { Button } from "../components/ui";
import AlertDialog from "../components/ui/AlertDialog";
import Dialog from "../components/ui/Dialog";
import { api, apiFetch, type RemoteAssetKind, type RemoteAssetRow } from "../lib/api";
import IngestDialog from "../components/assets/IngestDialog";
import AssetDetailDrawer from "../components/assets/AssetDetailDrawer";

interface Tab {
  id: RemoteAssetKind;
  label: string;
}

const TABS: Tab[] = [
  { id: "notebook", label: "Notebooks" },
  { id: "job", label: "Jobs" },
  { id: "pipeline", label: "Pipelines" },
  { id: "streamlit", label: "Streamlit" },
  { id: "stream", label: "Streams" },
  { id: "query", label: "Queries" },
];

function formatValue(v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function formatDate(val: unknown): string {
  if (!val) return "—";
  const s = String(val);
  // Try to format ISO strings nicely; fall back to raw
  try {
    const d = new Date(s);
    if (!isNaN(d.getTime())) return d.toLocaleDateString();
  } catch {
    // keep raw
  }
  return s;
}

interface AssetTableProps {
  profile: string;
  kind: RemoteAssetKind;
  onRowClick: (row: RemoteAssetRow) => void;
  onDeleteClick: (row: RemoteAssetRow) => void;
  onChunkClick: (row: RemoteAssetRow) => void;
  pendingDeleteId: string | null;
}

const _CHUNKABLE_KINDS = new Set<RemoteAssetKind>(["notebook", "query", "pipeline"]);

function AssetTable({
  profile,
  kind,
  onRowClick,
  onDeleteClick,
  onChunkClick,
  pendingDeleteId,
}: AssetTableProps) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["remote-assets", profile, kind],
    queryFn: () => api.listRemoteAssets(profile, kind),
    enabled: !!profile,
    staleTime: 60_000,
  });

  if (!profile) {
    return (
      <p className="py-8 text-center text-sm text-ink-dim">
        Select a profile above to view assets.
      </p>
    );
  }

  if (isLoading) {
    return (
      <p className="py-8 text-center text-sm text-ink-dim">Loading…</p>
    );
  }

  if (error) {
    return (
      <p className="py-4 text-sm text-critical">
        {(error as Error).message}
      </p>
    );
  }

  const items = data?.items ?? [];

  if (items.length === 0) {
    return (
      <p className="py-8 text-center text-sm text-ink-dim">
        No {kind}s found for this profile. Run ingestion to populate.
      </p>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-left text-xs font-medium uppercase tracking-wide text-ink-dim">
            <th className="pb-2 pr-4 pt-1">Name</th>
            <th className="hidden pb-2 pr-4 pt-1 sm:table-cell">Platform</th>
            <th className="hidden pb-2 pr-4 pt-1 md:table-cell">Owner</th>
            <th className="hidden pb-2 pr-4 pt-1 lg:table-cell">Last modified</th>
            <th className="w-10 pb-2 pt-1" aria-label="Actions" />
          </tr>
        </thead>
        <tbody className="divide-y divide-border/50">
          {items.map((row) => {
            const isDeleting = pendingDeleteId === String(row.id);
            return (
              <tr
                key={String(row.id)}
                onClick={() => onRowClick(row)}
                className="cursor-pointer transition-colors hover:bg-surface-subtle"
              >
                <td className="py-2 pr-4">
                  <span className="font-medium text-ink">{row.name ?? "—"}</span>
                </td>
                <td className="hidden py-2 pr-4 text-ink-muted sm:table-cell">
                  {formatValue(row.platform)}
                </td>
                <td className="hidden py-2 pr-4 text-ink-muted md:table-cell">
                  {formatValue(row.owner)}
                </td>
                <td className="hidden py-2 pr-4 text-ink-muted lg:table-cell">
                  {formatDate(row.last_modified_at)}
                </td>
                <td className="py-2 text-right">
                  <div className="inline-flex items-center gap-0.5">
                    {_CHUNKABLE_KINDS.has(kind) && (
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          onChunkClick(row);
                        }}
                        aria-label={`Configure chunking for ${row.name ?? kind}`}
                        title="Configure chunking strategy"
                        className="rounded p-1 text-ink-dim hover:bg-accent-soft hover:text-accent-ink"
                      >
                        <Scissors size={14} />
                      </button>
                    )}
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        onDeleteClick(row);
                      }}
                      disabled={isDeleting}
                      aria-label={`Delete ${row.name ?? kind}`}
                      title="Delete asset"
                      className="rounded p-1 text-ink-dim hover:bg-critical/10 hover:text-critical disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {isDeleting ? (
                        <Loader2 size={14} className="animate-spin" />
                      ) : (
                        <Trash2 size={14} />
                      )}
                    </button>
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <p className="mt-2 text-right text-[11px] text-ink-dim">
        {data?.count ?? items.length} {kind}(s)
      </p>
    </div>
  );
}

interface AssetSearchHit {
  chunk_id: string;
  kind: RemoteAssetKind;
  profile: string;
  remote_id: number;
  name: string;
  score: number;
  matched_text: string;
  metadata: Record<string, unknown>;
}

interface AssetSearchResponse {
  items: AssetSearchHit[];
  rag_available: boolean;
  count?: number;
  reason?: string;
}

export default function Assets() {
  const [activeTab, setActiveTab] = useState<RemoteAssetKind>("notebook");
  const [profile, setProfile] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [ingestOpen, setIngestOpen] = useState(false);
  const [drawerKind, setDrawerKind] = useState<RemoteAssetKind>("notebook");
  const [drawerAssetId, setDrawerAssetId] = useState<string>("");
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [pendingDelete, setPendingDelete] = useState<{
    kind: RemoteAssetKind;
    row: RemoteAssetRow;
  } | null>(null);
  const [chunkingTarget, setChunkingTarget] = useState<{
    kind: RemoteAssetKind;
    row: RemoteAssetRow;
  } | null>(null);

  const queryClient = useQueryClient();
  const deleteMutation = useMutation({
    mutationFn: ({ kind, id }: { kind: RemoteAssetKind; id: string }) =>
      api.deleteRemoteAsset(kind, id),
    onSuccess: (_data, { kind, id }) => {
      queryClient.invalidateQueries({ queryKey: ["remote-assets"] });
      queryClient.removeQueries({ queryKey: ["remote-asset", kind, id] });
      setPendingDelete(null);
    },
  });

  const profilesQ = useQuery({
    queryKey: ["db-profiles", "list"],
    queryFn: () => api.listDbProfiles(),
    staleTime: 60_000,
  });

  const profiles = profilesQ.data?.profiles ?? [];

  function openDrawer(kind: RemoteAssetKind, row: RemoteAssetRow) {
    setDrawerKind(kind);
    setDrawerAssetId(String(row.id));
    setDrawerOpen(true);
  }

  return (
    <div>
      <PageHeader
        title="Assets"
        actions={
          <Button
            variant="primary"
            leadingIcon={<Download size={14} />}
            onClick={() => setIngestOpen(true)}
            disabled={!profile}
          >
            Ingest assets
          </Button>
        }
      />

      {/* Profile picker + semantic search box — stack vertically on mobile */}
      <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:gap-4">
        <div className="flex items-center gap-2">
          <label
            htmlFor="assets-profile-picker"
            className="shrink-0 text-sm text-ink-muted"
          >
            Profile
          </label>
          <select
            id="assets-profile-picker"
            value={profile}
            onChange={(e) => setProfile(e.target.value)}
            className="rounded-md border border-border bg-surface px-2.5 py-1.5 text-sm text-ink focus:outline-none focus:ring-2 focus:ring-accent/40"
          >
            <option value="">— pick a profile —</option>
            {profiles.map((p) => (
              <option key={p.name} value={p.name}>
                {p.name}
                {p.backend ? ` (${p.backend})` : ""}
              </option>
            ))}
          </select>
        </div>
        <div className="flex flex-1 items-center gap-2 sm:max-w-md">
          <Search size={14} className="text-ink-dim" />
          <input
            type="search"
            placeholder="Semantic search across ingested assets…"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="flex-1 rounded-md border border-border bg-surface px-2.5 py-1.5 text-sm text-ink focus:outline-none focus:ring-2 focus:ring-accent/40"
          />
        </div>
      </div>

      {/* Tab bar — horizontally scrollable on overflow */}
      <div className="mb-4 -mx-1 overflow-x-auto">
        <div className="flex min-w-max gap-0.5 px-1">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              type="button"
              onClick={() => setActiveTab(tab.id)}
              className={
                activeTab === tab.id
                  ? "rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-white"
                  : "rounded-md px-3 py-1.5 text-sm font-medium text-ink-muted hover:bg-surface-subtle hover:text-ink"
              }
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Asset table — or semantic search results when a query is typed. */}
      <div className="rounded-lg border border-border bg-surface-raised px-4 py-3">
        {searchQuery.trim() ? (
          <AssetSearchResults
            profile={profile}
            kind={activeTab}
            query={searchQuery.trim()}
            onRowClick={(hit) =>
              openDrawer(hit.kind, {
                id: String(hit.remote_id),
                name: hit.name,
              } as RemoteAssetRow)
            }
          />
        ) : (
          <AssetTable
            profile={profile}
            kind={activeTab}
            onRowClick={(row) => openDrawer(activeTab, row)}
            onDeleteClick={(row) => setPendingDelete({ kind: activeTab, row })}
            onChunkClick={(row) => setChunkingTarget({ kind: activeTab, row })}
            pendingDeleteId={
              deleteMutation.isPending && pendingDelete
                ? String(pendingDelete.row.id)
                : null
            }
          />
        )}
      </div>

      <AlertDialog
        open={pendingDelete !== null}
        onClose={() => setPendingDelete(null)}
        onConfirm={() =>
          pendingDelete &&
          deleteMutation.mutate({
            kind: pendingDelete.kind,
            id: String(pendingDelete.row.id),
          })
        }
        tone="danger"
        loading={deleteMutation.isPending}
        confirmLabel="Delete"
        title={
          pendingDelete
            ? `Delete ${pendingDelete.kind} "${pendingDelete.row.name ?? pendingDelete.row.id}"`
            : "Delete asset"
        }
        description={
          <span>
            Removes the row from AMX's catalog. The source on the platform is{" "}
            <strong>untouched</strong>. Lineage edges that reference this asset
            are also removed.
          </span>
        }
      />

      {/* Ingest dialog */}
      <IngestDialog
        open={ingestOpen}
        onClose={() => setIngestOpen(false)}
        profile={profile}
      />

      {/* Detail drawer */}
      <AssetDetailDrawer
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        kind={drawerKind}
        assetId={drawerAssetId}
        profile={profile}
        onOpenAsset={(kind, id) => {
          // Swap the drawer in place (e.g. a Job task's target opens its
          // linked notebook). Keep the underlying tab where it is so the
          // user can close the drawer and return to the job list.
          setDrawerKind(kind);
          setDrawerAssetId(String(id));
        }}
      />

      {/* Per-row chunking override modal */}
      {chunkingTarget && (
        <ChunkingDialog
          open={true}
          onClose={() => setChunkingTarget(null)}
          profile={profile}
          kind={chunkingTarget.kind}
          row={chunkingTarget.row}
        />
      )}
    </div>
  );
}

interface AssetSearchResultsProps {
  profile: string;
  kind: RemoteAssetKind;
  query: string;
  onRowClick: (hit: AssetSearchHit) => void;
}

function AssetSearchResults({
  profile,
  kind,
  query,
  onRowClick,
}: AssetSearchResultsProps) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["assets-search", profile, kind, query],
    queryFn: () => {
      const params = new URLSearchParams();
      params.set("q", query);
      if (profile) params.set("profile", profile);
      if (kind) params.set("kind", kind);
      return apiFetch<AssetSearchResponse>(
        `/api/assets/search?${params.toString()}`,
      );
    },
    enabled: Boolean(query),
    staleTime: 5_000,
  });

  if (isLoading) {
    return (
      <p className="py-8 text-center text-sm text-ink-dim">
        Searching ingested assets…
      </p>
    );
  }

  if (error) {
    return (
      <p className="py-4 text-sm text-critical">{(error as Error).message}</p>
    );
  }

  const payload = data ?? { items: [], rag_available: true };

  if (!payload.rag_available) {
    return (
      <div className="space-y-1 py-4 text-sm">
        <p className="text-warn">
          Semantic search is not available yet — run{" "}
          <code className="rounded bg-surface-subtle px-1">
            /db ingest-assets
          </code>{" "}
          to build the asset index, or{" "}
          <code className="rounded bg-surface-subtle px-1">
            /db assets reindex
          </code>{" "}
          if you just switched embedding models.
        </p>
        {payload.reason && (
          <p className="text-ink-dim">{payload.reason}</p>
        )}
      </div>
    );
  }

  const items = payload.items ?? [];

  if (items.length === 0) {
    return (
      <p className="py-8 text-center text-sm text-ink-dim">
        No semantic matches for{" "}
        <span className="font-mono">{query}</span> in profile{" "}
        <span className="font-mono">{profile || "(any)"}</span>.
      </p>
    );
  }

  return (
    <ul className="divide-y divide-border/50">
      {items.map((hit) => (
        <li
          key={hit.chunk_id}
          className="cursor-pointer py-2 transition-colors hover:bg-surface-subtle"
          onClick={() => onRowClick(hit)}
        >
          <div className="flex items-center justify-between gap-2">
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2 text-sm">
                <span className="rounded bg-accent-soft px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-accent-ink">
                  {hit.kind}
                </span>
                <span className="truncate font-medium text-ink">
                  {hit.name || `#${hit.remote_id}`}
                </span>
              </div>
              <p className="mt-1 line-clamp-2 text-xs text-ink-muted">
                {hit.matched_text}
              </p>
            </div>
            <span className="shrink-0 font-mono text-[11px] tabular-nums text-ink-dim">
              {hit.score.toFixed(2)}
            </span>
          </div>
        </li>
      ))}
    </ul>
  );
}

interface ChunkingPayload {
  kind: string;
  profile: string;
  remote_id: number;
  has_override: boolean;
  effective: {
    strategy: string;
    chunk_chars?: number;
    chunk_overlap?: number;
  };
  default: {
    strategy: string;
    chunk_chars?: number;
    chunk_overlap?: number;
  };
  override?: {
    strategy: string;
    chunk_chars?: number | null;
    chunk_overlap?: number | null;
  };
}

const _STRATEGY_CHOICES: Record<RemoteAssetKind, string[]> = {
  notebook: ["whole", "cell", "char_window"],
  query: ["whole", "statement", "char_window"],
  pipeline: ["metadata", "whole"],
  // The remaining kinds aren't reachable via the Chunk button — the
  // table only wires the action up for the entries in _CHUNKABLE_KINDS
  // — but TypeScript wants exhaustive keys on the Record.
  stream: [],
  streamlit: [],
  job: [],
};

interface ChunkingDialogProps {
  open: boolean;
  onClose: () => void;
  profile: string;
  kind: RemoteAssetKind;
  row: RemoteAssetRow;
}

function ChunkingDialog({ open, onClose, profile, kind, row }: ChunkingDialogProps) {
  const qc = useQueryClient();
  const assetId = String(row.id);
  const choices = _STRATEGY_CHOICES[kind] ?? [];

  // Strategy / chunk_chars / chunk_overlap form state.
  const [strategy, setStrategy] = useState<string>("");
  const [chunkChars, setChunkChars] = useState<string>("");
  const [chunkOverlap, setChunkOverlap] = useState<string>("");
  const [seeded, setSeeded] = useState(false);

  const dataQuery = useQuery({
    queryKey: ["asset-chunking", profile, kind, assetId],
    queryFn: () =>
      apiFetch<ChunkingPayload>(
        `/api/assets/${kind}/${assetId}/chunking?profile=${encodeURIComponent(profile)}`,
      ),
    enabled: open,
    staleTime: 0,
  });

  // Seed the form from the effective config once the GET lands. We
  // only seed once per open so user edits aren't clobbered if the
  // user types while the query refetches.
  useEffect(() => {
    if (!open) {
      setSeeded(false);
      return;
    }
    if (seeded || !dataQuery.data) return;
    const eff = dataQuery.data.effective;
    setStrategy(eff.strategy);
    setChunkChars(eff.chunk_chars != null ? String(eff.chunk_chars) : "");
    setChunkOverlap(eff.chunk_overlap != null ? String(eff.chunk_overlap) : "");
    setSeeded(true);
  }, [open, seeded, dataQuery.data]);

  const saveMutation = useMutation({
    mutationFn: () =>
      apiFetch<{ ok: boolean }>(
        `/api/assets/${kind}/${assetId}/chunking?profile=${encodeURIComponent(profile)}`,
        {
          method: "PUT",
          body: JSON.stringify({
            strategy,
            chunk_chars: chunkChars ? Number(chunkChars) : null,
            chunk_overlap: chunkOverlap ? Number(chunkOverlap) : null,
          }),
        },
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["asset-chunking"] });
      onClose();
    },
  });

  const resetMutation = useMutation({
    mutationFn: () =>
      apiFetch<{ ok: boolean }>(
        `/api/assets/${kind}/${assetId}/chunking?profile=${encodeURIComponent(profile)}`,
        { method: "DELETE" },
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["asset-chunking"] });
      onClose();
    },
  });

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={`Chunking · ${row.name ?? `${kind} #${assetId}`}`}
      description={
        <span>
          Override the chunking strategy for this single asset. Other assets
          continue to use the global default in Settings → Embeddings.
        </span>
      }
      size="md"
      footer={
        <div className="flex w-full items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            {dataQuery.data?.has_override && (
              <Button
                variant="ghost"
                onClick={() => resetMutation.mutate()}
                disabled={resetMutation.isPending}
              >
                Reset to default
              </Button>
            )}
          </div>
          <div className="flex items-center gap-2">
            <Button variant="secondary" onClick={onClose}>
              Cancel
            </Button>
            <Button
              variant="primary"
              onClick={() => saveMutation.mutate()}
              disabled={!strategy || saveMutation.isPending}
            >
              {saveMutation.isPending ? (
                <>
                  <Loader2 size={12} className="mr-1 animate-spin" /> Saving…
                </>
              ) : (
                "Save & re-embed"
              )}
            </Button>
          </div>
        </div>
      }
    >
      {dataQuery.isPending ? (
        <p className="text-sm text-ink-dim">Loading current config…</p>
      ) : dataQuery.error ? (
        <p className="text-sm text-critical">
          {(dataQuery.error as Error).message}
        </p>
      ) : (
        <div className="space-y-4">
          <div>
            <label className="text-xs font-medium text-ink-muted">
              Strategy
            </label>
            <div className="mt-1 flex flex-wrap gap-1.5">
              {choices.map((opt) => {
                const active = strategy === opt;
                return (
                  <button
                    key={opt}
                    type="button"
                    onClick={() => setStrategy(opt)}
                    className={
                      "rounded-md border px-2.5 py-1 text-xs font-medium " +
                      (active
                        ? "border-accent bg-accent-soft text-accent-ink"
                        : "border-surface-border bg-surface text-ink-dim hover:border-accent/40 hover:text-ink")
                    }
                  >
                    {opt}
                  </button>
                );
              })}
            </div>
            <p className="mt-1 text-[11px] text-ink-dim">
              Global default for {kind}:{" "}
              <span className="font-mono">{dataQuery.data?.default.strategy}</span>
            </p>
          </div>

          {kind !== "pipeline" && (
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-xs font-medium text-ink-muted">
                  Chunk chars
                </label>
                <input
                  type="number"
                  min={200}
                  value={chunkChars}
                  onChange={(e) => setChunkChars(e.target.value)}
                  placeholder={String(dataQuery.data?.default.chunk_chars ?? "")}
                  className="mt-1 w-full rounded-md border border-surface-border bg-surface px-2.5 py-1.5 text-sm text-ink"
                />
                <p className="mt-1 text-[11px] text-ink-dim">
                  Blank → inherit global default.
                </p>
              </div>
              <div>
                <label className="text-xs font-medium text-ink-muted">
                  Chunk overlap
                </label>
                <input
                  type="number"
                  min={0}
                  value={chunkOverlap}
                  onChange={(e) => setChunkOverlap(e.target.value)}
                  placeholder={String(dataQuery.data?.default.chunk_overlap ?? "")}
                  className="mt-1 w-full rounded-md border border-surface-border bg-surface px-2.5 py-1.5 text-sm text-ink"
                />
              </div>
            </div>
          )}

          {dataQuery.data?.has_override && (
            <div className="rounded-md border border-accent/30 bg-accent-soft/30 px-3 py-2 text-[11px] text-ink-muted">
              This asset currently has a per-row override. Save to update,
              or "Reset to default" to remove and fall back to the global
              setting.
            </div>
          )}

          {(saveMutation.error || resetMutation.error) && (
            <p className="text-sm text-critical">
              {((saveMutation.error || resetMutation.error) as Error).message}
            </p>
          )}
        </div>
      )}
    </Dialog>
  );
}
