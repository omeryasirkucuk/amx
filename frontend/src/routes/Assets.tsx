/**
 * Assets — remote code-asset browser.
 *
 * Six tabs: Notebooks / Jobs / Pipelines / Streamlit / Streams / Queries.
 * Profile picker at the top lets the user scope to any configured DB
 * profile. Row click opens the detail drawer. "Ingest assets" button
 * opens the ingest dialog.
 */

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Download } from "lucide-react";

import PageHeader from "../components/PageHeader";
import { Button } from "../components/ui";
import { api, type RemoteAssetKind, type RemoteAssetRow } from "../lib/api";
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
}

function AssetTable({ profile, kind, onRowClick }: AssetTableProps) {
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
            <th className="hidden pb-2 pt-1 lg:table-cell">Last modified</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/50">
          {items.map((row) => (
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
              <td className="hidden py-2 text-ink-muted lg:table-cell">
                {formatDate(row.last_modified_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mt-2 text-right text-[11px] text-ink-dim">
        {data?.count ?? items.length} {kind}(s)
      </p>
    </div>
  );
}

export default function Assets() {
  const [activeTab, setActiveTab] = useState<RemoteAssetKind>("notebook");
  const [profile, setProfile] = useState("");
  const [ingestOpen, setIngestOpen] = useState(false);
  const [drawerKind, setDrawerKind] = useState<RemoteAssetKind>("notebook");
  const [drawerAssetId, setDrawerAssetId] = useState<string>("");
  const [drawerOpen, setDrawerOpen] = useState(false);

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

      {/* Profile picker + tab bar — stack vertically on mobile */}
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

      {/* Asset table */}
      <div className="rounded-lg border border-border bg-surface-raised px-4 py-3">
        <AssetTable
          profile={profile}
          kind={activeTab}
          onRowClick={(row) => openDrawer(activeTab, row)}
        />
      </div>

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
    </div>
  );
}
