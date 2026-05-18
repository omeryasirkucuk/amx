// Three-tab asset picker for the Documentation Pages wizard.
// Lets the user attach DB profiles, doc profiles, and lineage
// artifacts as generation context for a page.

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Check, Database, FileText, Workflow } from "lucide-react";

import { apiFetch, lineageList } from "../../lib/api";
import { cn } from "../../lib/cn";
import type { PageAssetRef } from "../../hooks/usePages";

interface DbProfileSummary {
  name: string;
  backend?: string;
}
interface DbProfilesResponse {
  profiles: DbProfileSummary[];
}

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

type TabId = "db" | "docs" | "lineage";

const TABS: Array<{ id: TabId; label: string; icon: typeof Database }> = [
  { id: "db", label: "DB profiles", icon: Database },
  { id: "docs", label: "Doc profiles", icon: FileText },
  { id: "lineage", label: "Lineage", icon: Workflow },
];

export default function AssetPicker({ value, onChange }: Props) {
  const [active, setActive] = useState<TabId>("db");

  const countsByTab: Record<TabId, number> = {
    db: value.filter((a) => a.kind === "db_profile").length,
    docs: value.filter((a) => a.kind === "doc_profile").length,
    lineage: value.filter((a) => a.kind === "lineage_artifact").length,
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
          <DbProfileTab isSelected={isSelected} onToggle={toggle} />
        )}
        {active === "docs" && (
          <DocProfileTab isSelected={isSelected} onToggle={toggle} />
        )}
        {active === "lineage" && (
          <LineageTab isSelected={isSelected} onToggle={toggle} />
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
                    <DbProfileTab isSelected={isSelected} onToggle={toggle} />
                  )}
                  {t.id === "docs" && (
                    <DocProfileTab isSelected={isSelected} onToggle={toggle} />
                  )}
                  {t.id === "lineage" && (
                    <LineageTab isSelected={isSelected} onToggle={toggle} />
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

function DbProfileTab({ isSelected, onToggle }: TabProps) {
  const q = useQuery({
    queryKey: ["pages", "asset-picker", "db-profiles"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
    staleTime: 30_000,
  });
  if (q.isLoading)
    return <div className="text-xs text-ink-dim">Loading DB profiles...</div>;
  if (q.error)
    return (
      <div className="text-xs text-critical">{(q.error as Error).message}</div>
    );
  const list = q.data?.profiles ?? [];
  if (list.length === 0)
    return (
      <div className="text-xs text-ink-dim">
        No DB profiles configured. Add one from Settings.
      </div>
    );
  return (
    <div className="grid gap-1.5 sm:grid-cols-2 lg:grid-cols-3">
      {list.map((p) => (
        <AssetRow
          key={p.name}
          label={p.name}
          sublabel={p.backend}
          selected={isSelected("db_profile", p.name)}
          onClick={() => onToggle({ kind: "db_profile", ref: p.name })}
        />
      ))}
    </div>
  );
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
