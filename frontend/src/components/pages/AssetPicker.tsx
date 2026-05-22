// Three-tab asset picker for the Documentation Pages wizard.
// Lets the user attach DB assets (drill from profile down to column),
// doc profiles, and lineage artifacts as generation context for a
// page.

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Check, Database, FileText, Package, Workflow } from "lucide-react";

import { apiFetch, lineageList } from "../../lib/api";
import { cn } from "../../lib/cn";
import type { PageAssetRef } from "../../hooks/usePages";
import DbAssetTree from "./DbAssetTree";

interface DbProfileSummary {
  name: string;
  backend?: string;
}
interface DbProfilesResponse {
  profiles: DbProfileSummary[];
}

interface IngestedAssetOption {
  kind: string;
  ref: string;
  name: string;
  location: string;
  ingested_at: string;
}

const INGESTED_KINDS: Array<{ kind: string; label: string }> = [
  { kind: "asset_notebook", label: "Notebooks" },
  { kind: "asset_job", label: "Jobs" },
  { kind: "asset_pipeline", label: "Pipelines" },
  { kind: "asset_query", label: "Queries" },
  { kind: "asset_stream", label: "Streams" },
  { kind: "asset_streamlit", label: "Streamlit apps" },
];

interface DocProfileSummary {
  name: string;
  paths?: string[];
}
interface DocProfilesResponse {
  profiles: DocProfileSummary[];
}

interface Props {
  value: PageAssetRef[];
  onChange: (next: PageAssetRef[]) => void;
}

type TabId = "db" | "docs" | "lineage" | "ingested";

const TABS: Array<{ id: TabId; label: string; icon: typeof Database }> = [
  { id: "db", label: "DB assets", icon: Database },
  { id: "docs", label: "Doc profiles", icon: FileText },
  { id: "lineage", label: "Lineage", icon: Workflow },
  { id: "ingested", label: "Ingested assets", icon: Package },
];

export default function AssetPicker({ value, onChange }: Props) {
  const [active, setActive] = useState<TabId>("db");

  const countsByTab: Record<TabId, number> = {
    db: value.filter((a) => a.kind.startsWith("db_")).length,
    docs: value.filter((a) => a.kind === "doc_profile").length,
    lineage: value.filter((a) => a.kind === "lineage_artifact").length,
    ingested: value.filter((a) => a.kind.startsWith("asset_")).length,
  };

  function toggle(asset: PageAssetRef) {
    const exists = value.some(
      (a) => a.kind === asset.kind && a.ref === asset.ref,
    );
    if (exists) {
      onChange(value.filter((a) => !(a.kind === asset.kind && a.ref === asset.ref)));
    } else {
      onChange([...value, asset]);
    }
  }

  function isSelected(kind: string, ref: string) {
    return value.some((a) => a.kind === kind && a.ref === ref);
  }

  return (
    <div className="space-y-3">
      {/* Tabs (horizontal on md+, accordion on sm) */}
      <div
        role="tablist"
        aria-label="Asset kind"
        className="hidden md:flex items-center gap-1 border-b border-border"
      >
        {TABS.map((t) => {
          const Icon = t.icon;
          const isActive = active === t.id;
          return (
            <button
              key={t.id}
              role="tab"
              aria-selected={isActive}
              type="button"
              onClick={() => setActive(t.id)}
              className={cn(
                "inline-flex items-center gap-1.5 border-b-2 px-3 py-2 text-sm font-medium transition-colors",
                isActive
                  ? "border-accent text-ink"
                  : "border-transparent text-ink-muted hover:text-ink",
              )}
            >
              <Icon size={14} />
              {t.label}
              {countsByTab[t.id] > 0 && (
                <span className="ml-1 rounded-full bg-accent-soft px-1.5 text-[10px] font-semibold text-accent-ink">
                  {countsByTab[t.id]}
                </span>
              )}
            </button>
          );
        })}
      </div>

      {/* md+ panel */}
      <div className="hidden md:block">
        {active === "db" && (
          <DbAssetTree value={value} onChange={onChange} />
        )}
        {active === "docs" && (
          <DocProfileTab isSelected={isSelected} onToggle={toggle} />
        )}
        {active === "lineage" && (
          <LineageTab isSelected={isSelected} onToggle={toggle} />
        )}
        {active === "ingested" && (
          <IngestedAssetsTab isSelected={isSelected} onToggle={toggle} />
        )}
      </div>

      {/* sm accordion */}
      <div className="md:hidden space-y-2">
        {TABS.map((t) => {
          const Icon = t.icon;
          const expanded = active === t.id;
          return (
            <div
              key={t.id}
              className="rounded-md border border-border bg-surface"
            >
              <button
                type="button"
                onClick={() => setActive(expanded ? ("" as TabId) : t.id)}
                aria-expanded={expanded}
                className="flex w-full items-center justify-between gap-2 px-3 py-2 text-sm"
              >
                <span className="inline-flex items-center gap-1.5 font-medium text-ink">
                  <Icon size={14} />
                  {t.label}
                </span>
                {countsByTab[t.id] > 0 && (
                  <span className="rounded-full bg-accent-soft px-1.5 text-[10px] font-semibold text-accent-ink">
                    {countsByTab[t.id]}
                  </span>
                )}
              </button>
              {expanded && (
                <div className="border-t border-border p-3">
                  {t.id === "db" && (
                    <DbAssetTree value={value} onChange={onChange} />
                  )}
                  {t.id === "docs" && (
                    <DocProfileTab isSelected={isSelected} onToggle={toggle} />
                  )}
                  {t.id === "lineage" && (
                    <LineageTab isSelected={isSelected} onToggle={toggle} />
                  )}
                  {t.id === "ingested" && (
                    <IngestedAssetsTab isSelected={isSelected} onToggle={toggle} />
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

interface TabProps {
  isSelected: (kind: string, ref: string) => boolean;
  onToggle: (asset: PageAssetRef) => void;
}

function DocProfileTab({ isSelected, onToggle }: TabProps) {
  const q = useQuery({
    queryKey: ["pages", "asset-picker", "doc-profiles"],
    queryFn: () => apiFetch<DocProfilesResponse>("/api/profiles/docs"),
    staleTime: 30_000,
  });
  if (q.isLoading)
    return <div className="text-xs text-ink-dim">Loading doc profiles...</div>;
  if (q.error)
    return (
      <div className="text-xs text-critical">{(q.error as Error).message}</div>
    );
  const list = q.data?.profiles ?? [];
  if (list.length === 0)
    return (
      <div className="text-xs text-ink-dim">
        No doc profiles configured. Add one from Settings.
      </div>
    );
  return (
    <div className="grid gap-1.5 sm:grid-cols-2 lg:grid-cols-3">
      {list.map((p) => {
        const ref = `doc:${p.name}`;
        return (
          <AssetRow
            key={p.name}
            label={p.name}
            sublabel={
              p.paths && p.paths.length > 0
                ? `${p.paths.length} path${p.paths.length === 1 ? "" : "s"}`
                : undefined
            }
            selected={isSelected("doc_profile", ref)}
            onClick={() => onToggle({ kind: "doc_profile", ref })}
          />
        );
      })}
    </div>
  );
}

function LineageTab({ isSelected, onToggle }: TabProps) {
  const q = useQuery({
    queryKey: ["pages", "asset-picker", "lineage-artifacts"],
    queryFn: () => lineageList(),
    staleTime: 30_000,
  });
  if (q.isLoading)
    return (
      <div className="text-xs text-ink-dim">Loading lineage artifacts...</div>
    );
  if (q.error)
    return (
      <div className="text-xs text-critical">{(q.error as Error).message}</div>
    );
  const list = q.data?.artifacts ?? [];
  if (list.length === 0)
    return (
      <div className="text-xs text-ink-dim">
        No saved lineage artifacts. Build one from the Lineage page first.
      </div>
    );
  return (
    <div className="grid gap-1.5 sm:grid-cols-2">
      {list.map((a) => {
        const ref = `lineage:${a.id}`;
        const label = a.name || `${a.anchor_schema ?? ""}.${a.anchor_table ?? ""}`;
        return (
          <AssetRow
            key={a.id}
            label={label}
            sublabel={`${a.db_profile} · ${a.node_count} nodes · ${a.edge_count} edges`}
            selected={isSelected("lineage_artifact", ref)}
            onClick={() => onToggle({ kind: "lineage_artifact", ref })}
          />
        );
      })}
    </div>
  );
}

function IngestedAssetsTab({ isSelected, onToggle }: TabProps) {
  const profilesQ = useQuery({
    queryKey: ["pages", "asset-picker", "db-profiles"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
    staleTime: 30_000,
  });

  const profiles = profilesQ.data?.profiles ?? [];
  const [profile, setProfile] = useState<string>("");
  const [kind, setKind] = useState<string>(INGESTED_KINDS[0].kind);

  const effectiveProfile = profile || profiles[0]?.name || "";

  const assetsQ = useQuery({
    queryKey: ["pages", "asset-picker", "ingested", effectiveProfile, kind],
    queryFn: () =>
      apiFetch<IngestedAssetOption[]>(
        `/api/pages/asset-options?kind=${encodeURIComponent(kind)}&profile=${encodeURIComponent(effectiveProfile)}`,
      ),
    enabled: Boolean(effectiveProfile),
    staleTime: 15_000,
  });

  if (profilesQ.isLoading)
    return <div className="text-xs text-ink-dim">Loading DB profiles...</div>;
  if (profiles.length === 0)
    return (
      <div className="text-xs text-ink-dim">
        No DB profiles configured. Add one from Settings and run
        <code className="mx-1 rounded bg-surface-subtle px-1">
          /db ingest-assets
        </code>
        to populate this tab.
      </div>
    );

  return (
    <div className="space-y-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
        <label className="flex items-center gap-2 text-xs text-ink-dim">
          Profile
          <select
            value={effectiveProfile}
            onChange={(e) => setProfile(e.target.value)}
            className="rounded border border-border bg-surface px-2 py-1 text-xs text-ink"
          >
            {profiles.map((p) => (
              <option key={p.name} value={p.name}>
                {p.name}
              </option>
            ))}
          </select>
        </label>
        <div className="flex flex-wrap gap-1">
          {INGESTED_KINDS.map((k) => {
            const isActive = kind === k.kind;
            return (
              <button
                key={k.kind}
                type="button"
                onClick={() => setKind(k.kind)}
                className={cn(
                  "rounded-md border px-2 py-1 text-xs font-medium transition-colors",
                  isActive
                    ? "border-accent bg-accent-soft text-accent-ink"
                    : "border-border bg-surface text-ink-dim hover:border-accent/40 hover:text-ink",
                )}
              >
                {k.label}
              </button>
            );
          })}
        </div>
      </div>

      {assetsQ.isLoading && (
        <div className="text-xs text-ink-dim">Loading ingested assets...</div>
      )}
      {assetsQ.error && (
        <div className="text-xs text-critical">
          {(assetsQ.error as Error).message}
        </div>
      )}
      {assetsQ.data && assetsQ.data.length === 0 && (
        <div className="text-xs text-ink-dim">
          No ingested{" "}
          {INGESTED_KINDS.find((k) => k.kind === kind)?.label.toLowerCase() ??
            kind}{" "}
          for profile <span className="font-mono">{effectiveProfile}</span>.
          Run <code className="rounded bg-surface-subtle px-1">/db ingest-assets</code>{" "}
          first.
        </div>
      )}
      {assetsQ.data && assetsQ.data.length > 0 && (
        <div className="grid gap-1.5 sm:grid-cols-2 lg:grid-cols-3">
          {assetsQ.data.map((row) => (
            <AssetRow
              key={`${row.kind}::${row.ref}`}
              label={row.name}
              sublabel={row.location || row.ingested_at}
              selected={isSelected(row.kind, row.ref)}
              onClick={() => onToggle({ kind: row.kind, ref: row.ref })}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function AssetRow({
  label,
  sublabel,
  selected,
  onClick,
}: {
  label: string;
  sublabel?: string;
  selected: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={selected}
      className={cn(
        "flex w-full items-center justify-between gap-2 rounded-md border px-3 py-2 text-left transition-colors",
        selected
          ? "border-accent/40 bg-accent-soft text-accent-ink"
          : "border-border bg-surface hover:border-accent/40 hover:bg-surface-subtle",
      )}
    >
      <div className="min-w-0 flex-1">
        <div className="truncate text-sm font-medium">{label}</div>
        {sublabel && (
          <div className="truncate text-[11px] text-ink-dim">{sublabel}</div>
        )}
      </div>
      <span
        className={cn(
          "inline-flex h-4 w-4 items-center justify-center rounded border",
          selected ? "border-accent bg-accent text-white" : "border-border bg-surface",
        )}
        aria-hidden="true"
      >
        {selected && <Check size={10} />}
      </span>
    </button>
  );
}
