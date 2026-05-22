/**
 * AssetBrowsePicker — PR-E lazy tree.
 *
 * Talks to ``/api/assets/discover/tree``: root loads instantly
 * from cache, each folder expand fires one /tree call for its
 * immediate children, per-folder refresh re-fetches just that
 * level. Search filters loaded rows when the cache has data;
 * empty cache offers an explicit "walk workspace" fallback.
 */

import {
  ChevronDown,
  ChevronRight,
  Loader2,
  RefreshCw,
  Search,
} from "lucide-react";
import { useCallback, useEffect, useState } from "react";

import { api, type DiscoverTreeNode } from "../../lib/api";

const PICKABLE_KINDS: Array<{ id: string; label: string; kindParam: string }> = [
  { id: "notebooks", label: "Notebooks", kindParam: "notebook" },
  // Jobs + pipelines are flat (no folder hierarchy) — the same tree
  // component renders them with zero indentation.
  { id: "jobs", label: "Jobs", kindParam: "job" },
  { id: "pipelines", label: "Pipelines", kindParam: "pipeline" },
];

interface Props {
  profile: string;
  enabledKinds: string[];
  selection: Record<string, Set<string>>;
  onSelectionChange: (next: Record<string, Set<string>>) => void;
  disabled?: boolean;
}

interface NodeState {
  expanded: boolean;
  loading: boolean;
  error: string | null;
  children: DiscoverTreeNode[] | null;
}

export default function AssetBrowsePicker({
  profile,
  enabledKinds,
  selection,
  onSelectionChange,
  disabled,
}: Props) {
  const tabs = PICKABLE_KINDS.filter((k) => enabledKinds.includes(k.id));
  const [activeTabId, setActiveTabId] = useState<string>(
    tabs[0]?.id ?? "notebooks",
  );
  const activeTab =
    tabs.find((t) => t.id === activeTabId) ?? tabs[0] ?? PICKABLE_KINDS[0];

  const [nodes, setNodes] = useState<Record<string, NodeState>>({});
  const [rootLoading, setRootLoading] = useState(false);
  const [rootError, setRootError] = useState<string | null>(null);
  const [rootChildren, setRootChildren] = useState<DiscoverTreeNode[] | null>(
    null,
  );
  const [rootRefreshing, setRootRefreshing] = useState(false);

  const [filter, setFilter] = useState("");
  const [debouncedFilter, setDebouncedFilter] = useState("");
  const [walking, setWalking] = useState(false);
  const [walkError, setWalkError] = useState<string | null>(null);

  // Reset all state when profile/kind changes.
  useEffect(() => {
    setNodes({});
    setRootChildren(null);
    setRootError(null);
    setFilter("");
    setDebouncedFilter("");
  }, [profile, activeTab.kindParam]);

  useEffect(() => {
    const t = setTimeout(() => setDebouncedFilter(filter.trim()), 200);
    return () => clearTimeout(t);
  }, [filter]);

  const fetchRoot = useCallback(
    async (force: boolean) => {
      if (force) setRootRefreshing(true);
      else setRootLoading(true);
      setRootError(null);
      try {
        const fn = force ? api.refreshDiscoverTree : api.discoverTree;
        const res = await fn({
          profile,
          kind: activeTab.kindParam,
          parent: "",
        });
        setRootChildren(res.items);
      } catch (err) {
        setRootError((err as Error).message || "Failed to load workspace.");
      } finally {
        setRootLoading(false);
        setRootRefreshing(false);
      }
    },
    [profile, activeTab.kindParam],
  );

  useEffect(() => {
    if (!profile) return;
    if (rootChildren !== null) return;
    fetchRoot(false);
  }, [profile, rootChildren, fetchRoot]);

  const fetchChildren = useCallback(
    async (parentPath: string, force: boolean) => {
      setNodes((prev) => ({
        ...prev,
        [parentPath]: {
          ...(prev[parentPath] ?? { expanded: true, children: null }),
          loading: true,
          error: null,
          expanded: true,
        },
      }));
      try {
        const fn = force ? api.refreshDiscoverTree : api.discoverTree;
        const res = await fn({
          profile,
          kind: activeTab.kindParam,
          parent: parentPath,
        });
        setNodes((prev) => ({
          ...prev,
          [parentPath]: {
            ...(prev[parentPath] ?? { expanded: true }),
            loading: false,
            error: null,
            children: res.items,
            expanded: true,
          },
        }));
      } catch (err) {
        setNodes((prev) => ({
          ...prev,
          [parentPath]: {
            ...(prev[parentPath] ?? { expanded: true, children: null }),
            loading: false,
            error: (err as Error).message ?? "Fetch failed.",
            expanded: true,
          },
        }));
      }
    },
    [profile, activeTab.kindParam],
  );

  const toggleFolder = useCallback(
    (folder: DiscoverTreeNode) => {
      const state = nodes[folder.path];
      if (state?.expanded) {
        setNodes((prev) => ({
          ...prev,
          [folder.path]: { ...prev[folder.path]!, expanded: false },
        }));
        return;
      }
      if (state?.children) {
        setNodes((prev) => ({
          ...prev,
          [folder.path]: { ...prev[folder.path]!, expanded: true },
        }));
        return;
      }
      void fetchChildren(folder.path, false);
    },
    [nodes, fetchChildren],
  );

  const selectedSet = selection[activeTab.id] ?? new Set<string>();

  const toggleLeaf = (leaf: DiscoverTreeNode) => {
    if (!leaf.external_id) return;
    const next = new Set(selectedSet);
    if (next.has(leaf.external_id)) next.delete(leaf.external_id);
    else next.add(leaf.external_id);
    onSelectionChange({ ...selection, [activeTab.id]: next });
  };

  const allLoadedLeaves = useCallback((): DiscoverTreeNode[] => {
    const acc: DiscoverTreeNode[] = [];
    const pushLeaf = (n: DiscoverTreeNode) => {
      if (!isDir(n)) acc.push(n);
    };
    (rootChildren ?? []).forEach(pushLeaf);
    Object.values(nodes).forEach((s) => {
      (s.children ?? []).forEach(pushLeaf);
    });
    return acc;
  }, [rootChildren, nodes]);

  const cacheHasAnyRow = (rootChildren?.length ?? 0) > 0;

  const onWalk = async () => {
    setWalking(true);
    setWalkError(null);
    try {
      await api.walkDiscoverTree({ profile, kind: activeTab.kindParam });
      await fetchRoot(false);
    } catch (err) {
      setWalkError((err as Error).message ?? "Walk failed.");
    } finally {
      setWalking(false);
    }
  };

  const matched = (() => {
    if (!debouncedFilter) return null;
    const needle = debouncedFilter.toLowerCase();
    return allLoadedLeaves().filter((l) => {
      const hay = `${l.name} ${l.path} ${l.owner ?? ""}`.toLowerCase();
      return hay.includes(needle);
    });
  })();

  if (tabs.length === 0) {
    return (
      <p className="rounded-md border border-dashed border-border px-3 py-4 text-center text-sm text-ink-muted">
        Pick at least one of Notebooks / Jobs / Pipelines above to browse
        individual assets. Other kinds (queries, task dependencies, streams,
        streamlit apps) are time-windowed or have no folder hierarchy worth
        a picker.
      </p>
    );
  }

  return (
    <div className="space-y-2">
      <div role="tablist" className="flex flex-wrap gap-1 border-b border-border">
        {tabs.map((tab) => {
          const isActive = tab.id === activeTabId;
          const count = (selection[tab.id] ?? new Set()).size;
          return (
            <button
              key={tab.id}
              role="tab"
              type="button"
              onClick={() => setActiveTabId(tab.id)}
              disabled={disabled}
              className={`-mb-px border-b-2 px-3 py-1.5 text-xs font-medium transition-colors ${
                isActive
                  ? "border-accent text-accent"
                  : "border-transparent text-ink-muted hover:text-ink"
              }`}
            >
              {tab.label}
              {count > 0 && (
                <span className="ml-1.5 rounded-full bg-accent/15 px-1.5 py-0.5 text-[10px] text-accent">
                  {count}
                </span>
              )}
            </button>
          );
        })}
      </div>

      <div className="flex items-center gap-2">
        <div className="relative flex-1">
          <Search
            size={14}
            className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-ink-dim"
          />
          <input
            type="search"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Search by name, path, or owner…"
            disabled={disabled}
            className="w-full rounded-md border border-border bg-surface-raised py-1.5 pl-7 pr-2 text-sm placeholder:text-ink-dim disabled:cursor-not-allowed disabled:opacity-50"
          />
        </div>
        <button
          type="button"
          title="Refresh root folder"
          onClick={() => fetchRoot(true)}
          disabled={disabled || rootRefreshing || rootLoading}
          className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-border hover:bg-surface-subtle disabled:cursor-not-allowed disabled:opacity-50"
          aria-label="Refresh root level"
        >
          {rootRefreshing ? (
            <Loader2 size={14} className="animate-spin" />
          ) : (
            <RefreshCw size={14} />
          )}
        </button>
      </div>

      <div className="flex items-center justify-between gap-2 text-xs text-ink-muted">
        <span>
          {selectedSet.size > 0
            ? `${selectedSet.size} selected`
            : "None selected — every shown row will be skipped at submit."}
        </span>
        {selectedSet.size > 0 && (
          <button
            type="button"
            onClick={() =>
              onSelectionChange({ ...selection, [activeTab.id]: new Set() })
            }
            className="text-ink-muted hover:underline"
          >
            Clear
          </button>
        )}
      </div>

      <div className="max-h-[55vh] overflow-y-auto rounded-md border border-border">
        {rootLoading ? (
          <div className="px-3 py-6 text-center text-xs text-ink-dim">
            Loading workspace root…
          </div>
        ) : rootError ? (
          <div className="px-3 py-4 text-xs text-critical">{rootError}</div>
        ) : matched ? (
          <SearchResults
            matched={matched}
            cacheHasAnyRow={cacheHasAnyRow}
            walking={walking}
            walkError={walkError}
            onWalk={onWalk}
            selectedSet={selectedSet}
            onToggleLeaf={toggleLeaf}
            disabled={!!disabled}
          />
        ) : (
          <TreeList
            level={(rootChildren ?? []).filter(distinctByPath())}
            nodes={nodes}
            depth={0}
            onToggleFolder={toggleFolder}
            onRefreshFolder={(p) => fetchChildren(p, true)}
            onToggleLeaf={toggleLeaf}
            selectedSet={selectedSet}
            disabled={!!disabled}
          />
        )}
      </div>
    </div>
  );
}

function isDir(n: DiscoverTreeNode): boolean {
  return Boolean(n.is_directory);
}

function distinctByPath() {
  const seen = new Set<string>();
  return (n: DiscoverTreeNode) => {
    if (seen.has(n.path)) return false;
    seen.add(n.path);
    return true;
  };
}

interface TreeListProps {
  level: DiscoverTreeNode[];
  nodes: Record<string, NodeState>;
  depth: number;
  onToggleFolder: (folder: DiscoverTreeNode) => void;
  onRefreshFolder: (path: string) => void;
  onToggleLeaf: (leaf: DiscoverTreeNode) => void;
  selectedSet: Set<string>;
  disabled: boolean;
}

function TreeList({
  level,
  nodes,
  depth,
  onToggleFolder,
  onRefreshFolder,
  onToggleLeaf,
  selectedSet,
  disabled,
}: TreeListProps) {
  if (level.length === 0) {
    return (
      <div className="px-3 py-4 text-center text-xs text-ink-muted">
        Empty.
      </div>
    );
  }
  return (
    <ul className="divide-y divide-border/60">
      {level.map((entry) =>
        isDir(entry) ? (
          <FolderRow
            key={entry.path}
            folder={entry}
            state={nodes[entry.path]}
            depth={depth}
            onToggleFolder={onToggleFolder}
            onRefreshFolder={onRefreshFolder}
            onToggleLeaf={onToggleLeaf}
            selectedSet={selectedSet}
            disabled={disabled}
            nodes={nodes}
          />
        ) : (
          <LeafRow
            key={entry.path}
            leaf={entry}
            depth={depth}
            selectedSet={selectedSet}
            onToggleLeaf={onToggleLeaf}
            disabled={disabled}
          />
        ),
      )}
    </ul>
  );
}

function FolderRow({
  folder,
  state,
  depth,
  onToggleFolder,
  onRefreshFolder,
  onToggleLeaf,
  selectedSet,
  disabled,
  nodes,
}: {
  folder: DiscoverTreeNode;
  state: NodeState | undefined;
  depth: number;
  onToggleFolder: (folder: DiscoverTreeNode) => void;
  onRefreshFolder: (path: string) => void;
  onToggleLeaf: (leaf: DiscoverTreeNode) => void;
  selectedSet: Set<string>;
  disabled: boolean;
  nodes: Record<string, NodeState>;
}) {
  const expanded = state?.expanded ?? false;
  const loading = state?.loading ?? false;
  const error = state?.error ?? null;
  const children = state?.children ?? null;
  const [refreshing, setRefreshing] = useState(false);

  const handleRefresh = async (e: React.MouseEvent) => {
    e.stopPropagation();
    setRefreshing(true);
    try {
      await Promise.resolve(onRefreshFolder(folder.path));
    } finally {
      setRefreshing(false);
    }
  };

  return (
    <li>
      <div
        role="button"
        tabIndex={0}
        onClick={() => onToggleFolder(folder)}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onToggleFolder(folder);
          }
        }}
        style={{ paddingLeft: 8 + depth * 16 }}
        className="flex cursor-pointer items-center gap-2 py-1.5 pr-2 text-sm hover:bg-surface-subtle"
      >
        {loading ? (
          <Loader2 size={14} className="shrink-0 animate-spin text-ink-dim" />
        ) : expanded ? (
          <ChevronDown size={14} className="shrink-0 text-ink-dim" />
        ) : (
          <ChevronRight size={14} className="shrink-0 text-ink-dim" />
        )}
        <span className="flex-1 truncate font-mono text-xs text-ink">
          {folder.path || "/"}
        </span>
        {children && (
          <span className="shrink-0 text-[11px] text-ink-dim">
            {children.length} items
          </span>
        )}
        <button
          type="button"
          title="Refresh this folder"
          onClick={handleRefresh}
          disabled={disabled || refreshing || loading}
          className="shrink-0 rounded p-0.5 text-ink-dim hover:bg-surface-raised disabled:cursor-not-allowed disabled:opacity-40"
          aria-label={`Refresh ${folder.path}`}
        >
          {refreshing ? (
            <Loader2 size={12} className="animate-spin" />
          ) : (
            <RefreshCw size={12} />
          )}
        </button>
      </div>
      {expanded && (
        <>
          {error && (
            <div
              style={{ paddingLeft: 24 + depth * 16 }}
              className="py-1 text-[11px] text-critical"
            >
              {error}
            </div>
          )}
          {children && (
            <TreeList
              level={children.filter(distinctByPath())}
              nodes={nodes}
              depth={depth + 1}
              onToggleFolder={onToggleFolder}
              onRefreshFolder={onRefreshFolder}
              onToggleLeaf={onToggleLeaf}
              selectedSet={selectedSet}
              disabled={disabled}
            />
          )}
        </>
      )}
    </li>
  );
}

function LeafRow({
  leaf,
  depth,
  selectedSet,
  onToggleLeaf,
  disabled,
}: {
  leaf: DiscoverTreeNode;
  depth: number;
  selectedSet: Set<string>;
  onToggleLeaf: (leaf: DiscoverTreeNode) => void;
  disabled: boolean;
}) {
  const checked = leaf.external_id ? selectedSet.has(leaf.external_id) : false;
  return (
    <li
      style={{ paddingLeft: 24 + depth * 16 }}
      className="flex items-center gap-2 py-1 pr-2 text-sm hover:bg-surface-subtle"
    >
      <input
        type="checkbox"
        checked={checked}
        onChange={() => onToggleLeaf(leaf)}
        disabled={disabled || !leaf.external_id}
        className="h-3.5 w-3.5 shrink-0 accent-accent"
        aria-label={`Select ${leaf.name}`}
      />
      <span className="flex-1 truncate text-ink">{leaf.name}</span>
      {leaf.owner && (
        <span className="shrink-0 text-[11px] text-ink-dim">{leaf.owner}</span>
      )}
    </li>
  );
}

function SearchResults({
  matched,
  cacheHasAnyRow,
  walking,
  walkError,
  onWalk,
  selectedSet,
  onToggleLeaf,
  disabled,
}: {
  matched: DiscoverTreeNode[];
  cacheHasAnyRow: boolean;
  walking: boolean;
  walkError: string | null;
  onWalk: () => void;
  selectedSet: Set<string>;
  onToggleLeaf: (leaf: DiscoverTreeNode) => void;
  disabled: boolean;
}) {
  if (!cacheHasAnyRow) {
    return (
      <div className="space-y-2 px-3 py-4 text-xs text-ink-muted">
        <p>
          The cache is empty. Run a full workspace walk once to enable search
          across every folder. Subsequent searches are instant.
        </p>
        <button
          type="button"
          onClick={onWalk}
          disabled={walking}
          className="inline-flex items-center gap-1.5 rounded border border-border px-2.5 py-1 text-xs font-medium hover:bg-surface-subtle disabled:cursor-not-allowed disabled:opacity-50"
        >
          {walking ? (
            <Loader2 size={12} className="animate-spin" />
          ) : (
            <RefreshCw size={12} />
          )}
          {walking ? "Walking workspace…" : "Walk workspace + search"}
        </button>
        {walkError && <p className="text-critical">{walkError}</p>}
      </div>
    );
  }
  if (matched.length === 0) {
    return (
      <div className="px-3 py-6 text-center text-xs text-ink-muted">
        No matches in the loaded folders. Tip: expand more folders or hit{" "}
        <button
          type="button"
          onClick={onWalk}
          className="underline hover:text-accent"
          disabled={walking}
        >
          walk workspace
        </button>{" "}
        to search the entire tree.
      </div>
    );
  }
  return (
    <ul className="divide-y divide-border/60">
      {matched.map((leaf) => (
        <li
          key={leaf.path}
          className="flex items-center gap-2 px-3 py-1 text-sm hover:bg-surface-subtle"
        >
          <input
            type="checkbox"
            checked={
              leaf.external_id ? selectedSet.has(leaf.external_id) : false
            }
            onChange={() => onToggleLeaf(leaf)}
            disabled={disabled || !leaf.external_id}
            className="h-3.5 w-3.5 shrink-0 accent-accent"
          />
          <span className="flex-1 truncate text-ink">{leaf.name}</span>
          <span className="shrink-0 truncate font-mono text-[11px] text-ink-dim">
            {leaf.path}
          </span>
        </li>
      ))}
    </ul>
  );
}
