/**
 * AssetBrowsePicker — browse-then-pick step for IngestDialog.
 *
 * Renders one tab per pickable asset kind (notebooks / jobs /
 * pipelines / streamlit_apps / streams). Each tab hits
 * GET /api/assets/discover for cheap identity rows (no source
 * content) and shows a DataTable with a checkbox column so the
 * user can cherry-pick individual external_ids before submitting
 * the ingest job. ``queries`` and ``task_dependencies`` are
 * intentionally not pickable — they're time-windowed aggregates
 * filtered by ``history_days`` / ``query_history_limit``, not
 * per-asset rows.
 *
 * Selection state lives in the parent (IngestDialog) so the
 * submit handler can fold it into the ``selection`` field on
 * /api/assets/ingest.
 */

import { useEffect, useMemo, useState } from "react";

import { api, type RemoteAssetMetadata } from "../../lib/api";
import { DataTable, type DataTableColumn } from "../ui";

/**
 * Kinds the discover endpoint serves. Order is the tab order; pick
 * "notebooks" first because it's the most common entry point.
 */
const PICKABLE_KINDS: Array<{ id: string; label: string }> = [
  { id: "notebooks", label: "Notebooks" },
  { id: "jobs", label: "Jobs" },
  { id: "pipelines", label: "Pipelines" },
  { id: "streamlit_apps", label: "Streamlit apps" },
  { id: "streams", label: "Streams" },
];

interface Props {
  profile: string;
  /** Kinds the user has ticked in the type picker — only these tabs render. */
  enabledKinds: string[];
  /** kind → Set of selected external_ids. Owned by parent. */
  selection: Record<string, Set<string>>;
  /** Parent updates the per-kind Set in immutable fashion. */
  onSelectionChange: (next: Record<string, Set<string>>) => void;
  /** Disable interactions during submission. */
  disabled?: boolean;
}

interface KindState {
  loading: boolean;
  error: string | null;
  items: RemoteAssetMetadata[];
}

export default function AssetBrowsePicker({
  profile,
  enabledKinds,
  selection,
  onSelectionChange,
  disabled,
}: Props) {
  const tabs = useMemo(
    () => PICKABLE_KINDS.filter((k) => enabledKinds.includes(k.id)),
    [enabledKinds],
  );
  const [activeKind, setActiveKind] = useState<string>(tabs[0]?.id ?? "");
  const [cache, setCache] = useState<Record<string, KindState>>({});

  // When the enabled set shrinks past the current active tab,
  // re-anchor to the first remaining tab.
  useEffect(() => {
    if (tabs.length === 0) {
      setActiveKind("");
      return;
    }
    if (!tabs.some((t) => t.id === activeKind)) {
      setActiveKind(tabs[0].id);
    }
  }, [tabs, activeKind]);

  // Lazy-load each kind's metadata the first time its tab is opened.
  // Mounting all tabs at once would fire one /discover call per kind
  // on dialog open, which is wasteful for the common path where the
  // user only browses one kind.
  useEffect(() => {
    if (!activeKind || cache[activeKind]) return;
    let cancelled = false;
    setCache((prev) => ({
      ...prev,
      [activeKind]: { loading: true, error: null, items: [] },
    }));
    api
      .discoverAssets({ profile, kind: activeKind })
      .then((res) => {
        if (cancelled) return;
        setCache((prev) => ({
          ...prev,
          [activeKind]: { loading: false, error: null, items: res.items },
        }));
      })
      .catch((err: Error) => {
        if (cancelled) return;
        setCache((prev) => ({
          ...prev,
          [activeKind]: {
            loading: false,
            error: err.message ?? "Failed to load assets.",
            items: [],
          },
        }));
      });
    return () => {
      cancelled = true;
    };
  }, [activeKind, profile, cache]);

  function toggle(kind: string, id: string) {
    const current = selection[kind] ?? new Set<string>();
    const next = new Set(current);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    onSelectionChange({ ...selection, [kind]: next });
  }

  function selectAllVisible(kind: string, ids: string[]) {
    const current = selection[kind] ?? new Set<string>();
    const next = new Set(current);
    for (const id of ids) next.add(id);
    onSelectionChange({ ...selection, [kind]: next });
  }

  function clearKind(kind: string) {
    onSelectionChange({ ...selection, [kind]: new Set() });
  }

  if (tabs.length === 0) {
    return (
      <p className="rounded-md border border-dashed border-border px-3 py-4 text-center text-sm text-ink-muted">
        Pick at least one type above (notebooks, jobs, pipelines, streamlit
        apps, or streams) to browse individual assets. Queries and task
        dependencies are time-windowed and can't be cherry-picked.
      </p>
    );
  }

  const state = cache[activeKind];
  const selectedForKind = selection[activeKind] ?? new Set<string>();

  const columns: DataTableColumn<RemoteAssetMetadata>[] = [
    {
      id: "_pick",
      header: "",
      width: "w-10",
      cell: (row) => (
        <input
          type="checkbox"
          checked={selectedForKind.has(row.external_id)}
          onChange={() => toggle(activeKind, row.external_id)}
          disabled={disabled}
          className="h-3.5 w-3.5 accent-accent"
          aria-label={`Select ${row.name}`}
        />
      ),
    },
    {
      id: "name",
      header: "Name",
      sortValue: (row) => row.name,
      cell: (row) => <span className="font-medium">{row.name}</span>,
    },
    {
      id: "path",
      header: "Path",
      sortValue: (row) => row.path,
      hideOnMobile: true,
      mono: true,
      cell: (row) => (
        <span className="break-all text-xs text-ink-muted">
          {row.path || "—"}
        </span>
      ),
    },
    {
      id: "owner",
      header: "Owner",
      sortValue: (row) => row.owner ?? "",
      hideOnMobile: true,
      cell: (row) => row.owner ?? "—",
    },
  ];

  return (
    <div className="space-y-2">
      {/* Tab strip — flex-wrap keeps it usable on narrow screens. */}
      <div
        role="tablist"
        className="flex flex-wrap gap-1 border-b border-border"
      >
        {tabs.map((tab) => {
          const isActive = tab.id === activeKind;
          const count = (selection[tab.id] ?? new Set()).size;
          return (
            <button
              key={tab.id}
              role="tab"
              type="button"
              onClick={() => setActiveKind(tab.id)}
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

      {/* Per-tab toolbar — bulk actions for the currently visible set. */}
      <div className="flex items-center justify-between gap-2 text-xs text-ink-muted">
        <span>
          {selectedForKind.size > 0
            ? `${selectedForKind.size} selected`
            : "None selected — every shown row will be skipped at submit."}
        </span>
        <div className="flex gap-2">
          <button
            type="button"
            disabled={disabled || !state || state.items.length === 0}
            onClick={() =>
              selectAllVisible(
                activeKind,
                (state?.items ?? []).map((r) => r.external_id),
              )
            }
            className="text-accent hover:underline disabled:cursor-not-allowed disabled:opacity-50"
          >
            Select all
          </button>
          <button
            type="button"
            disabled={disabled || selectedForKind.size === 0}
            onClick={() => clearKind(activeKind)}
            className="text-ink-muted hover:underline disabled:cursor-not-allowed disabled:opacity-50"
          >
            Clear
          </button>
        </div>
      </div>

      <DataTable
        columns={columns}
        rows={state?.items ?? []}
        rowKey={(row) => row.external_id}
        searchable
        searchPlaceholder={`Search ${activeKind.replace("_", " ")}…`}
        searchAccessor={(row) =>
          `${row.name} ${row.path} ${row.owner ?? ""}`.toLowerCase()
        }
        isLoading={state?.loading ?? false}
        error={state?.error ?? null}
        pageSize={25}
        emptyState={
          <span className="text-ink-muted">
            No {activeKind.replace("_", " ")} found in this profile.
          </span>
        }
      />
    </div>
  );
}
