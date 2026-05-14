import { useEffect, useState, useSyncExternalStore } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { QueryClient } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router-dom";
import {
  ChevronRight,
  ChevronDown,
  Database,
  FolderTree,
  HardDrive,
  Layers,
  Loader2,
  RefreshCw,
  Search,
  X,
} from "lucide-react";

import { ApiError, api, apiFetch } from "../lib/api";
import type { DbCacheSearchResult } from "../lib/api";
import { cn } from "../lib/cn";
import type { Scope } from "../lib/scope";
import { scopePath } from "../lib/scope";
import { useUi } from "../lib/store";
import { InfoHint } from "./ui";
import LlmProfilePriceLine from "./LlmProfilePriceLine";
import ProfilePicker from "./topbar/ProfilePicker";

interface Props {
  collapsed: boolean;
}

interface DbProfileSummary {
  name: string;
  /** Always ``true`` since 0.13: DB profile activation was retired and
   *  every defined profile is selectable from every Studio surface.
   *  The field is retained for back-compat with older bundles that
   *  still cached this shape; new code should ignore it. */
  is_active: boolean;
  backend?: string;
  host?: string;
  database?: string;
  catalog?: string;
}

interface DbProfilesResponse {
  profiles: DbProfileSummary[];
}

/**
 * Live-DB asset tree, multi-profile shape:
 *
 *   profile  →  database (or catalog)  →  schema  →  table
 *
 * Every saved DB profile is a top-level row that the user can expand
 * independently. There is no "active" / "switch" concept anymore —
 * scope is per-URL, so two browser tabs on different profiles never
 * fight over a global state. Visual hierarchy is encoded with both
 * indent and typography (uppercase/bold profile, normal db/catalog,
 * dim small schema, dimmer extra-small table) so it stays scannable
 * even with several profiles expanded at once.
 */
export default function Sidebar({ collapsed }: Props) {
  if (collapsed) {
    return <CollapsedRail />;
  }
  return (
    <div className="flex h-full flex-col">
      <div className="px-3 pt-3">
        <SectionTitle hint="Active LLM profile. Manage from Settings.">
          LLM Profile
        </SectionTitle>
        <LlmSection />
        <SectionTitle
          className="mt-4"
          hint="Every saved DB profile, expandable independently. Click any node to navigate."
        >
          DB Profiles
        </SectionTitle>
        <Link
          to="/db-cache"
          title="Inspect and flush AMX's DB metadata caches"
          className={cn(
            "mb-1 mt-1 flex items-center gap-1.5 rounded px-2 py-1 text-[12px]",
            "text-ink-dim transition-colors duration-fast hover:bg-surface-subtle hover:text-ink",
          )}
        >
          <HardDrive size={12} />
          <span>DB cache</span>
        </Link>
        <ProfileSearchInput />
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto px-3 pb-3">
        <ProfilesTree />
      </div>
    </div>
  );
}

/** Vertical rail rendered when the sidebar is collapsed.
 *
 * Each icon is a button bound to ``toggleSidebar`` so the user can
 * pop the panel back open by clicking any of the section glyphs —
 * the topbar's ``PanelLeft`` chevron is the formal toggle, but
 * users instinctively reach for the visible icons first and the
 * earlier pass left them as inert SVGs. The semantic ``aria-label``s
 * and ``aria-expanded={false}`` let screen-reader users navigate
 * the same way.
 */
function CollapsedRail() {
  const toggleSidebar = useUi((s) => s.toggleSidebar);
  const railIcons: Array<{ Icon: typeof Database; label: string }> = [
    { Icon: Database, label: "Expand sidebar — DB profiles" },
    { Icon: FolderTree, label: "Expand sidebar — profile tree" },
    { Icon: Layers, label: "Expand sidebar — LLM profile" },
  ];
  return (
    <div className="flex h-full flex-col items-center gap-1 py-3 text-ink-dim">
      {railIcons.map(({ Icon, label }) => (
        <button
          key={label}
          type="button"
          onClick={toggleSidebar}
          aria-label={label}
          aria-expanded={false}
          className="flex h-8 w-8 items-center justify-center rounded-md transition-colors duration-fast hover:bg-surface-subtle hover:text-ink focus:outline-none focus-visible:ring-2 focus-visible:ring-accent/40"
        >
          <Icon size={16} />
        </button>
      ))}
    </div>
  );
}

/** Sidebar search box for the DB Profiles section. Filters
 *  profile names + their database / catalog children by simple
 *  case-insensitive substring; no fuzzy matching, no regex --
 *  just type the few letters you remember. The query is read by
 *  ``ProfilesTree`` via the module-local ``useProfileSearch``
 *  hook so the input lives next to the section title without
 *  prop-drilling through the tree. */
function ProfileSearchInput() {
  const { query, setQuery } = useProfileSearch();
  return (
    <div className="mb-1.5 mt-1 flex items-center gap-1.5 rounded-md border border-surface-border bg-surface px-2 py-1 focus-within:border-accent/40">
      <Search size={12} className="shrink-0 text-ink-dim" />
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Search schemas, tables, columns…"
        aria-label="Search DB profiles, schemas, tables, columns"
        className="w-full bg-transparent text-xs text-ink outline-none placeholder:text-ink-dim"
      />
      {query && (
        <button
          type="button"
          onClick={() => setQuery("")}
          aria-label="Clear search"
          className="shrink-0 rounded p-0.5 text-ink-dim hover:bg-surface-subtle hover:text-ink"
        >
          <X size={11} />
        </button>
      )}
    </div>
  );
}

// Module-local search-state singleton. Backs a useSyncExternalStore
// hook so every sub-tree of the sidebar (the input + every profile
// row + every database/catalog child) reacts to the same query
// without prop-drilling. Reset to the empty string on every page
// load -- search state is intentionally session-only because the
// query is "what the user is hunting for right now", not a
// long-lived preference.
const _searchListeners = new Set<() => void>();
let _searchValue = "";
function _subscribeProfileSearch(listener: () => void): () => void {
  _searchListeners.add(listener);
  return () => _searchListeners.delete(listener);
}
function _getProfileSearch(): string {
  return _searchValue;
}
function _setProfileSearch(next: string): void {
  if (_searchValue === next) return;
  _searchValue = next;
  _searchListeners.forEach((fn) => fn());
}

function useProfileSearch(): { query: string; setQuery: (next: string) => void } {
  const query = useSyncExternalStore(
    _subscribeProfileSearch,
    _getProfileSearch,
    _getProfileSearch,
  );
  return { query, setQuery: _setProfileSearch };
}

/** Case-insensitive substring match. Empty needle matches
 *  everything so the unfiltered tree renders normally. */
function matchesSearch(haystack: string, needle: string): boolean {
  if (!needle) return true;
  return haystack.toLowerCase().includes(needle.toLowerCase());
}

interface CatalogsCache {
  supports_catalogs: boolean;
  catalogs: string[];
  active_catalog?: string | null;
  /** BigQuery's project — same scope role as a catalog. */
  active_project?: string | null;
}
interface DatabasesCache {
  databases: string[];
  active_database?: string | null;
}
interface AssetsCache {
  assets: { name: string; kind?: string }[];
}

/** Look up cached database/catalog children for a profile and
 *  return true if any of their names match the search query.
 *  Used so a profile row stays visible when the user types the
 *  name of a database that lives below it -- but only when that
 *  list is already in the query cache (we don't kick off N
 *  fetches just for the search). */
function profileHasMatchingChildInCache(
  qc: QueryClient,
  profileName: string,
  query: string,
): boolean {
  if (!query) return false;
  const cats = qc.getQueryData<CatalogsCache>(["live-catalogs", profileName]);
  if (cats?.supports_catalogs) {
    if (cats.catalogs.some((c) => matchesSearch(c, query))) return true;
  }
  const dbs = qc.getQueryData<DatabasesCache>(["live-databases", profileName]);
  if (dbs?.databases?.some((d) => matchesSearch(d, query))) return true;
  return false;
}

/** Return true when a schema's cached asset list (tables / views)
 *  contains a name that matches the search query. Used to keep a
 *  schema row visible when the user types a table name -- but only
 *  if that asset list is already in the React Query cache. */
function schemaHasMatchingAssetInCache(
  qc: QueryClient,
  scope: Scope,
  schema: string,
  query: string,
): boolean {
  if (!query) return false;
  const cached = qc.getQueryData<AssetsCache>([
    "live-assets",
    scope.profile,
    scope.database ?? "",
    scope.catalog ?? "",
    schema,
  ]);
  return !!cached?.assets?.some((a) => matchesSearch(a.name, query));
}

function SectionTitle({
  children,
  className,
  hint,
}: {
  children: React.ReactNode;
  className?: string;
  hint?: string;
}) {
  return (
    <div
      className={cn(
        "flex items-center gap-1 px-2 pb-1 text-[10px] font-semibold uppercase tracking-[0.08em] text-ink-dim",
        className,
      )}
    >
      {children}
      {hint && <InfoHint text={hint} />}
    </div>
  );
}

function LlmSection() {
  const { data } = useQuery({ queryKey: ["context"], queryFn: () => api.context() });
  const provider = data?.llm_provider ?? "";
  const model = data?.llm_model ?? "";
  return (
    <div className="space-y-0.5 text-sm">
      <ProfilePicker
        kind="llm"
        label="LLM"
        variant="row"
        activeName={data?.active_llm_profile ?? null}
        tooltip={data?.llm_model ?? undefined}
      />
      {provider && model && (
        <LlmProfilePriceLine
          provider={provider}
          model={model}
          isActive
          density="compact"
          className="px-1.5 pt-0.5"
        />
      )}
    </div>
  );
}

/** Minimum query length before we issue a cache-search request.
 *  Single characters would degrade into a near-no-op scan that
 *  returns the entire catalog; two characters is the same gate the
 *  backend enforces. */
const SEARCH_MIN_CHARS = 2;
/** Debounce window between keystrokes — long enough that fast
 *  typing collapses into one request, short enough that the user
 *  sees results almost immediately after they pause. */
const SEARCH_DEBOUNCE_MS = 200;

function useDebouncedValue<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = window.setTimeout(() => setDebounced(value), delay);
    return () => window.clearTimeout(id);
  }, [value, delay]);
  return debounced;
}

function ProfilesTree() {
  const qc = useQueryClient();
  const { query } = useProfileSearch();
  const profiles = useQuery({
    queryKey: ["db-profiles", "list"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
    retry: false,
  });

  if (profiles.isLoading) {
    return <div className="px-2 py-1 text-xs text-ink-dim">Loading profiles…</div>;
  }
  if (profiles.error) {
    return (
      <div className="px-2 py-1 text-xs text-critical">
        {(profiles.error as Error).message}
      </div>
    );
  }
  const list = profiles.data?.profiles ?? [];
  if (list.length === 0) {
    return (
      <div className="px-2 py-1 text-xs text-ink-dim">
        No DB profiles yet — add one under Settings.
      </div>
    );
  }
  // A query of two or more characters switches the sidebar into
  // catalog-cache search mode: instead of filtering the tree by
  // already-cached child names, we ask the backend to walk
  // catalog_entities (schemas + tables + columns) and render the
  // hits as a flat ranked list. Tree mode handles empty / 1-char
  // queries as before.
  if (query.length >= SEARCH_MIN_CHARS) {
    return <SearchResultsList query={query} profiles={list} />;
  }
  // When the search box is empty, render the whole tree. When
  // it's set, decide row-by-row whether to show each profile.
  // A profile passes the filter if its name or backend matches,
  // or if a database/catalog already in the React Query cache
  // for it matches -- the latter lets a hit on a child name keep
  // the parent visible after the user has expanded that profile
  // at least once. We don't fetch lazily-loaded children just to
  // search; the user can expand profiles to broaden the surface.
  const filtered = query
    ? list.filter((p) => {
        const nameMatch =
          matchesSearch(p.name, query) || matchesSearch(p.backend ?? "", query);
        if (nameMatch) return true;
        return profileHasMatchingChildInCache(qc, p.name, query);
      })
    : list;
  if (filtered.length === 0) {
    return (
      <div className="px-2 py-1 text-xs text-ink-dim">
        No profiles match “{query}”.
      </div>
    );
  }
  return (
    <div className="space-y-0.5">
      {filtered.map((p) => {
        const profileNameMatched =
          !!query &&
          (matchesSearch(p.name, query) ||
            matchesSearch(p.backend ?? "", query));
        return (
          <ProfileNode
            key={p.name}
            profile={p}
            query={query}
            profileNameMatched={profileNameMatched}
          />
        );
      })}
    </div>
  );
}

/** Catalog-cache search results. Renders when the sidebar search
 *  query is two or more characters — replaces the tree with a
 *  flat ranked list of schema / table / column matches pulled from
 *  catalog_entities via /api/db/cache/search.
 *
 *  Multi-match UX: a query like "id" can resolve to hundreds of
 *  columns. We cap at 50 server-side and surface a "+N more" hint
 *  rather than auto-expanding the tree (which would unfold every
 *  schema at once and obscure the matches). Refining the query is
 *  the natural narrowing affordance.
 */
function SearchResultsList({
  query,
  profiles,
}: {
  query: string;
  profiles: DbProfileSummary[];
}) {
  const qc = useQueryClient();
  const navigate = useNavigate();
  const { setQuery } = useProfileSearch();
  const debouncedQuery = useDebouncedValue(query, SEARCH_DEBOUNCE_MS);
  const search = useQuery({
    queryKey: ["db-cache-search", debouncedQuery],
    queryFn: () => api.dbCacheSearch(debouncedQuery),
    enabled: debouncedQuery.length >= SEARCH_MIN_CHARS,
    retry: false,
    staleTime: 30_000,
  });

  if (search.isLoading || debouncedQuery !== query) {
    return <div className="px-2 py-1 text-[11px] text-ink-dim">Searching…</div>;
  }
  if (search.error) {
    return (
      <div className="px-2 py-1 text-[11px] text-critical">
        {(search.error as Error).message}
      </div>
    );
  }
  const results = search.data?.results ?? [];
  if (results.length === 0) {
    // The most common reason for an empty result set is that no
    // profile has been catalog-synced yet — without ``state='done'``
    // on catalog_profile_state the backend has nothing to search.
    // Surface that hint so the user has a single next step.
    const anySynced = profiles.length > 0;
    return (
      <div className="space-y-1 px-2 py-1 text-[11px] text-ink-dim">
        <div>No matches for “{query}”.</div>
        {anySynced && (
          <div className="text-ink-dim">
            Tip: column search requires{" "}
            <Link to="/db-cache" className="underline hover:text-ink">
              a synced catalog
            </Link>
            .
          </div>
        )}
      </div>
    );
  }

  // Group rows by profile so cross-profile noise is easy to scan.
  // Maintain insertion order — the server already ranked schema →
  // table → column then alphabetical, so we just preserve it.
  const byProfile = new Map<string, DbCacheSearchResult[]>();
  for (const r of results) {
    const bucket = byProfile.get(r.profile);
    if (bucket) bucket.push(r);
    else byProfile.set(r.profile, [r]);
  }

  // 3-level backends (Databricks Unity Catalog, BigQuery) store
  // the catalog/project in ``database_name``; clicking through must
  // route them as a catalog URL or the page won't resolve. The
  // sidebar's catalogs query is already cached for any profile the
  // user has touched, so we read ``supports_catalogs`` from there.
  const profileSupportsCatalogs = (profileName: string): boolean => {
    const cats = qc.getQueryData<CatalogsCache>(["live-catalogs", profileName]);
    return !!cats?.supports_catalogs;
  };

  const onResultClick = (r: DbCacheSearchResult) => {
    const usesCatalog = profileSupportsCatalogs(r.profile);
    const scope = {
      profile: r.profile,
      database: usesCatalog ? undefined : r.database,
      catalog: usesCatalog ? r.database : undefined,
    };
    const targetPath = r.table
      ? scopePath(scope, r.schema, r.table)
      : scopePath(scope, r.schema);
    setQuery("");
    navigate(targetPath);
  };

  return (
    <div className="space-y-2">
      {Array.from(byProfile.entries()).map(([profileName, rows]) => (
        <div key={profileName}>
          <div className="px-2 pb-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-ink-dim">
            {profileName}
          </div>
          <div className="space-y-0.5">
            {rows.map((r, idx) => (
              <SearchResultRow
                key={`${profileName}-${idx}-${r.schema}-${r.table ?? ""}-${r.column ?? ""}`}
                row={r}
                query={query}
                onSelect={() => onResultClick(r)}
              />
            ))}
          </div>
        </div>
      ))}
      {search.data?.truncated && (
        <div className="px-2 pt-1 text-[10px] italic text-ink-dim">
          Showing first {results.length} matches — refine your search to narrow.
        </div>
      )}
    </div>
  );
}

/** One row in ``SearchResultsList``. Renders the breadcrumb path
 *  (schema › table › column) with the matching segment bolded so
 *  the hit is visually unambiguous. */
function SearchResultRow({
  row,
  query,
  onSelect,
}: {
  row: DbCacheSearchResult;
  query: string;
  onSelect: () => void;
}) {
  const leaf =
    row.match_field === "column"
      ? row.column ?? ""
      : row.match_field === "table"
        ? row.table ?? ""
        : row.schema;
  return (
    <button
      type="button"
      onClick={onSelect}
      title={`${row.profile} · ${row.database} · ${row.schema}${
        row.table ? " · " + row.table : ""
      }${row.column ? " · " + row.column : ""}`}
      className={cn(
        "flex w-full min-w-0 flex-col items-start gap-0.5 rounded px-2 py-1 text-left",
        "text-[11px] text-ink hover:bg-surface-subtle",
      )}
    >
      <div className="flex w-full min-w-0 items-center gap-1">
        <span className="truncate font-mono text-ink">
          <Highlight text={leaf} needle={query} />
        </span>
        <span className="ml-auto shrink-0 rounded bg-surface-subtle px-1 py-px text-[9px] uppercase tracking-wide text-ink-dim">
          {row.match_field}
        </span>
      </div>
      <div className="w-full truncate text-[10px] text-ink-dim">
        {row.schema}
        {row.table && (
          <>
            <span className="px-0.5">›</span>
            {row.table}
          </>
        )}
        {row.column && (
          <>
            <span className="px-0.5">›</span>
            {row.column}
          </>
        )}
      </div>
    </button>
  );
}

/** Bold the matching substring inside a result label. Case-
 *  insensitive; first occurrence only — keeps the visual noise
 *  low for column names that repeat the query several times. */
function Highlight({ text, needle }: { text: string; needle: string }) {
  if (!needle) return <>{text}</>;
  const lowerText = text.toLowerCase();
  const lowerNeedle = needle.toLowerCase();
  const idx = lowerText.indexOf(lowerNeedle);
  if (idx < 0) return <>{text}</>;
  return (
    <>
      {text.slice(0, idx)}
      <span className="font-semibold text-accent-ink">
        {text.slice(idx, idx + needle.length)}
      </span>
      {text.slice(idx + needle.length)}
    </>
  );
}

/**
 * Tiny refresh icon shown next to a profile row or a database / catalog
 * row. Posts ``/api/catalog/sync`` scoped to the row -- profile-only at
 * the top, ``?profile=…&database=…`` at the database/catalog level so a
 * click on one container doesn't drag every other container under the
 * same profile through a re-walk. On settle we invalidate the live
 * schema/asset queries under the row so the tree re-fetches the fresh
 * data without a manual page reload.
 */
function CatalogSyncIconButton({
  profile,
  database,
  invalidateKeys,
  title,
}: {
  profile: string;
  database?: string;
  invalidateKeys: ReadonlyArray<ReadonlyArray<unknown>>;
  title: string;
}) {
  const qc = useQueryClient();
  const sync = useMutation({
    mutationFn: () => {
      const params = new URLSearchParams({ profile });
      if (database) params.set("database", database);
      return apiFetch(`/api/catalog/sync?${params.toString()}`, { method: "POST" });
    },
    onSettled: () => {
      qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
      for (const key of invalidateKeys) {
        qc.invalidateQueries({ queryKey: key as unknown[] });
      }
      // Second pass for small catalogs that finish before the first
      // invalidation's refetch reaches the server.
      window.setTimeout(() => {
        qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
        for (const key of invalidateKeys) {
          qc.invalidateQueries({ queryKey: key as unknown[] });
        }
      }, 3000);
    },
  });
  return (
    <button
      type="button"
      aria-label={title}
      title={title}
      onClick={(event) => {
        // The row itself is a button bound to expand / navigate; we
        // must not let the refresh click bubble through and trigger
        // those side effects.
        event.stopPropagation();
        if (!sync.isPending) sync.mutate();
      }}
      className={cn(
        "ml-1 inline-flex h-5 w-5 items-center justify-center rounded text-ink-dim",
        "hover:bg-surface-subtle hover:text-ink",
        sync.isPending && "text-ink",
      )}
    >
      {sync.isPending ? (
        <Loader2 size={12} className="animate-spin" />
      ) : (
        <RefreshCw size={12} />
      )}
    </button>
  );
}

function ProfileNode({
  profile,
  query,
  profileNameMatched,
}: {
  profile: DbProfileSummary;
  query: string;
  profileNameMatched: boolean;
}) {
  const params = useParams();
  // Collapsed by default so the tree doesn't fire one fetch per
  // profile on first render. Expand sticky if the user is currently
  // looking at this profile, OR while a search query is active --
  // a search hit is worthless if the children stay hidden behind
  // a chevron the user has to click.
  const [open, setOpen] = useState<boolean>(params.profile === profile.name);
  const effectiveOpen = open || !!query;

  return (
    <div>
      <div className="flex w-full items-center">
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          title={profile.backend || ""}
          className={cn(
            "flex min-w-0 flex-1 items-center gap-1 rounded px-2 py-1 text-left transition-colors duration-fast",
            "text-[13px] font-bold uppercase tracking-wide",
            params.profile === profile.name
              ? "bg-accent-soft text-accent-ink"
              : "text-ink hover:bg-surface-subtle",
          )}
        >
          {effectiveOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          <span className="truncate">{profile.name}</span>
          {profile.backend && (
            <span className="ml-auto text-[9px] font-normal normal-case tracking-normal text-ink-dim">
              {profile.backend}
            </span>
          )}
        </button>
        <CatalogSyncIconButton
          profile={profile.name}
          title={`Refresh catalog for ${profile.name}`}
          invalidateKeys={[
            ["live-catalogs", profile.name],
            ["live-databases", profile.name],
            ["live-schemas", profile.name],
          ]}
        />
      </div>
      {effectiveOpen && (
        <div className="ml-3 mt-0.5 border-l border-border pl-2">
          <ProfileScopeChildren
            profile={profile.name}
            query={query}
            parentMatched={profileNameMatched}
          />
        </div>
      )}
    </div>
  );
}

/**
 * Below a profile row: list its catalogs (3-level) or databases
 * (2-level). Probes /api/live/catalogs first; if the backend is
 * 2-level we fall through to /api/live/databases.
 */
function ProfileScopeChildren({
  profile,
  query,
  parentMatched,
}: {
  profile: string;
  query: string;
  parentMatched: boolean;
}) {
  const catalogs = useQuery({
    queryKey: ["live-catalogs", profile],
    queryFn: () => api.liveCatalogs({ profile }),
    retry: false,
  });
  const databases = useQuery({
    queryKey: ["live-databases", profile],
    queryFn: () => api.liveDatabases({ profile }),
    retry: false,
    enabled: catalogs.data ? !catalogs.data.supports_catalogs : false,
  });

  // When the parent profile name matched the query, the user
  // already found their target -- show every child unfiltered so
  // they can drill in. Only filter children when the search
  // narrows past the profile level.
  const childFilter = (name: string): boolean =>
    !query || parentMatched || matchesSearch(name, query);

  if (catalogs.isLoading) {
    return <div className="px-2 py-1 text-[11px] text-ink-dim">Loading…</div>;
  }
  if (catalogs.error) {
    return (
      <div className="px-2 py-1 text-[11px] text-critical">
        {(catalogs.error as Error).message}
      </div>
    );
  }
  if (catalogs.data?.supports_catalogs) {
    // Honour the profile-level pin: when the user chose a catalog at
    // /db profile creation we render only that one, regardless of
    // what the role can see on the workspace. The backend still
    // returns the full list in ``catalogs`` so the user can switch
    // by editing the profile, but the sidebar respects the pin.
    //
    // ``active_project`` is BigQuery's equivalent of a Databricks
    // catalog (the wizard pins one or the other depending on
    // backend); the rule is identical for both.
    const pinned =
      (catalogs.data.active_catalog || catalogs.data.active_project || "").trim();
    const pinnedMissing =
      pinned && !catalogs.data.catalogs.includes(pinned);
    const effective = pinned && !pinnedMissing
      ? catalogs.data.catalogs.filter((c) => c === pinned)
      : catalogs.data.catalogs;
    const list = effective.filter(childFilter);
    if (catalogs.data.catalogs.length === 0) {
      return (
        <div className="px-2 py-1 text-[11px] text-ink-dim">(no catalogs visible)</div>
      );
    }
    if (pinnedMissing) {
      // Pinned catalog is gone server-side; rather than silently
      // showing the whole workspace (which would mask the misconfig)
      // surface a hint and still render the visible list so the user
      // can navigate.
      return (
        <div className="space-y-0.5">
          <div className="px-2 py-1 text-[11px] text-warning">
            Pinned catalog &quot;{pinned}&quot; not visible. Edit the
            profile via /db to re-pin.
          </div>
          {list.map((name) => (
            <ScopeNode
              key={name}
              scope={{ profile, catalog: name, kind: "catalog" }}
              label={name}
              query={query}
              parentMatched={parentMatched || matchesSearch(name, query)}
            />
          ))}
        </div>
      );
    }
    if (list.length === 0) {
      return (
        <div className="px-2 py-1 text-[11px] text-ink-dim">
          No catalogs match.
        </div>
      );
    }
    return (
      <div className="space-y-0.5">
        {list.map((name) => (
          <ScopeNode
            key={name}
            scope={{ profile, catalog: name, kind: "catalog" }}
            label={name}
            query={query}
            parentMatched={parentMatched || matchesSearch(name, query)}
          />
        ))}
      </div>
    );
  }
  if (databases.isLoading) {
    return <div className="px-2 py-1 text-[11px] text-ink-dim">Loading databases…</div>;
  }
  if (databases.error) {
    return (
      <div className="px-2 py-1 text-[11px] text-critical">
        {(databases.error as Error).message}
      </div>
    );
  }
  // Same pin-honouring rule as the catalogs branch above: if the
  // profile carries a pinned ``cfg.database`` we render only that
  // database, even when the role can see more on the server.
  const allDbListRaw = databases.data?.databases ?? [];
  const pinnedDb = (databases.data?.active_database || "").trim();
  const pinnedDbMissing = pinnedDb && !allDbListRaw.includes(pinnedDb);
  const allDbList = pinnedDb && !pinnedDbMissing
    ? allDbListRaw.filter((d) => d === pinnedDb)
    : allDbListRaw;
  const dbList = allDbList.filter(childFilter);
  if (pinnedDbMissing) {
    return (
      <div className="space-y-0.5">
        <div className="px-2 py-1 text-[11px] text-warning">
          Pinned database &quot;{pinnedDb}&quot; not visible. Edit the
          profile via /db to re-pin.
        </div>
        {dbList.map((name) => (
          <ScopeNode
            key={name}
            scope={{ profile, database: name, kind: "database" }}
            label={name}
            query={query}
            parentMatched={parentMatched || matchesSearch(name, query)}
          />
        ))}
      </div>
    );
  }
  if (allDbList.length === 0) {
    return (
      <div className="px-2 py-1 text-[11px] text-ink-dim">
        (no databases reachable)
      </div>
    );
  }
  if (dbList.length === 0) {
    return (
      <div className="px-2 py-1 text-[11px] text-ink-dim">No databases match.</div>
    );
  }
  return (
    <div className="space-y-0.5">
      {dbList.map((name) => (
        <ScopeNode
          key={name}
          scope={{ profile, database: name, kind: "database" }}
          label={name}
          query={query}
          parentMatched={parentMatched || matchesSearch(name, query)}
        />
      ))}
    </div>
  );
}

function ScopeNode({
  scope,
  label,
  query,
  parentMatched,
}: {
  scope: Scope;
  label: string;
  query: string;
  parentMatched: boolean;
}) {
  const params = useParams();
  const navigate = useNavigate();
  const isOnThis =
    params.profile === scope.profile &&
    (scope.database ? params.database === scope.database : params.catalog === scope.catalog);
  const [open, setOpen] = useState<boolean>(isOnThis);
  // While the user is typing, force every level open so the
  // search query reaches schemas + tables. Without this the tree
  // would only filter the rows the user has already drilled into.
  const effectiveOpen = open || !!query;

  // The catalog stamp is the universal container key on both 2-level
  // (database) and 3-level (catalog) backends -- the backend stores
  // both into the same ``catalog_entities.database_name`` column. Pass
  // it through unchanged so the per-container refresh hits the right
  // scope on either shape.
  const containerName = scope.database ?? scope.catalog ?? "";
  return (
    <div>
      <div className="flex w-full items-center">
        <button
          type="button"
          onClick={() => {
            setOpen((v) => !v);
            navigate(scopePath(scope));
          }}
          className={cn(
            "flex min-w-0 flex-1 items-center gap-1 rounded px-2 py-1 text-left text-[13px] transition-colors duration-fast",
            isOnThis
              ? "bg-accent-soft text-accent-ink"
              : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
          )}
        >
          {effectiveOpen ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
          <span className="truncate">{label}</span>
        </button>
        {containerName && (
          <CatalogSyncIconButton
            profile={scope.profile}
            database={containerName}
            title={`Refresh catalog for ${label}`}
            invalidateKeys={[
              ["live-schemas", scope.profile, scope.database ?? "", scope.catalog ?? ""],
            ]}
          />
        )}
      </div>
      {effectiveOpen && (
        <div className="ml-3 mt-0.5 border-l border-border pl-2">
          <SchemasUnderScope
            scope={scope}
            query={query}
            parentMatched={parentMatched}
          />
        </div>
      )}
    </div>
  );
}

function SchemasUnderScope({
  scope,
  query,
  parentMatched,
}: {
  scope: Scope;
  query: string;
  parentMatched: boolean;
}) {
  const qc = useQueryClient();
  const { data, error, isLoading } = useQuery({
    queryKey: [
      "live-schemas",
      scope.profile,
      scope.database ?? "",
      scope.catalog ?? "",
    ],
    queryFn: () => api.liveSchemas(scope),
    retry: false,
  });
  if (isLoading) {
    return <div className="px-2 py-1 text-[11px] text-ink-dim">Loading schemas…</div>;
  }
  if (error instanceof ApiError) {
    return (
      <div className="px-2 py-1 text-[11px] text-critical">{error.message}</div>
    );
  }
  if (error) {
    return (
      <div className="px-2 py-1 text-[11px] text-critical">
        {(error as Error).message}
      </div>
    );
  }
  if (!data || data.schemas.length === 0) {
    return <div className="px-2 py-1 text-[11px] text-ink-dim">(no schemas)</div>;
  }
  // Honour the wizard's schema-level pin: Databricks pins via
  // ``cfg.database`` (the wizard prompt literally reads "Schema /
  // database (optional)") and BigQuery pins via ``cfg.dataset``.
  // When the pin is set AND visible in the live list, render only
  // that one. When the pin is set but missing (dropped server-side
  // or permissions lost) fall through to the full list so the
  // sidebar's pinned-but-missing warning surfaces — never fabricate
  // a phantom row.
  const pinnedSchema =
    (data.active_schema || data.active_dataset || "").trim();
  const pinnedSchemaMissing =
    pinnedSchema && !data.schemas.includes(pinnedSchema);
  const visibleSchemas =
    pinnedSchema && !pinnedSchemaMissing
      ? data.schemas.filter((s) => s === pinnedSchema)
      : data.schemas;

  // When parentMatched is true, the user already hit on something
  // higher up the chain (a profile / db / catalog) -- show every
  // schema unfiltered. Otherwise narrow to schemas whose name
  // matches OR that have a cached asset list with a table name
  // match (so a typed table name keeps its parent schema visible
  // even before the user clicks into it).
  const schemas = query
    ? visibleSchemas.filter(
        (s) =>
          parentMatched ||
          matchesSearch(s, query) ||
          schemaHasMatchingAssetInCache(qc, scope, s, query),
      )
    : visibleSchemas;
  if (schemas.length === 0) {
    return <div className="px-2 py-1 text-[11px] text-ink-dim">No schemas match.</div>;
  }
  return (
    <div className="space-y-0.5">
      {pinnedSchemaMissing && (
        <div className="px-2 py-1 text-[11px] text-warning">
          Pinned schema &quot;{pinnedSchema}&quot; not visible. Edit
          the profile via /db to re-pin.
        </div>
      )}
      {schemas.map((schema) => (
        <SchemaNode
          key={schema}
          scope={scope}
          schema={schema}
          query={query}
          parentMatched={parentMatched || matchesSearch(schema, query)}
        />
      ))}
    </div>
  );
}

function SchemaNode({
  scope,
  schema,
  query,
  parentMatched,
}: {
  scope: Scope;
  schema: string;
  query: string;
  parentMatched: boolean;
}) {
  const params = useParams();
  const navigate = useNavigate();
  const isOnThis =
    params.profile === scope.profile &&
    (scope.database ? params.database === scope.database : params.catalog === scope.catalog) &&
    params.schema === schema;
  const [open, setOpen] = useState<boolean>(isOnThis);
  // Force the assets list open while the user has a search query
  // active so a table-name match pulls its parent schema's assets
  // into view without an extra click.
  const effectiveOpen = open || !!query;

  const queryKey = [
    "live-assets",
    scope.profile,
    scope.database ?? "",
    scope.catalog ?? "",
    schema,
  ];
  const { data: assets } = useQuery({
    queryKey,
    queryFn: () => api.liveAssets(scope, schema),
    enabled: effectiveOpen,
  });

  // Manual cache-busting refresh: hits POST /api/live/schemas/{s}/refresh
  // which clears the column-comments cache on the backend and returns
  // the fresh asset list. The result is fed back into the same TanStack
  // Query so the sidebar swaps in-place without a flicker. Useful when
  // a DBA edited descriptions outside AMX — AMX-internal writes already
  // invalidate the cache before their HTTP response returns.
  const qcLocal = useQueryClient();
  const refresh = useMutation({
    mutationFn: () => api.refreshSchemaMetadata(scope, schema),
    onSuccess: (data) => {
      qcLocal.setQueryData(queryKey, data);
    },
  });

  const filteredAssets =
    assets?.assets && query && !parentMatched
      ? assets.assets.filter((a) => matchesSearch(a.name, query))
      : assets?.assets;

  return (
    <div className="group/row">
      <div className="flex w-full items-center">
        <button
          type="button"
          onClick={() => {
            setOpen((v) => !v);
            navigate(scopePath(scope, schema));
          }}
          className={cn(
            "flex min-w-0 flex-1 items-center gap-1 rounded px-2 py-1 text-left text-[12px] transition-colors duration-fast",
            isOnThis
              ? "bg-accent-soft text-accent-ink"
              : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
          )}
        >
          {effectiveOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          <span className="truncate">{schema}</span>
        </button>
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            refresh.mutate();
          }}
          disabled={refresh.isPending}
          aria-label={`Refresh ${schema}`}
          title="Refresh column descriptions for this schema"
          className={cn(
            "ml-1 shrink-0 rounded p-1 text-ink-dim transition-opacity",
            // Hidden by default, revealed on row hover. Always visible
            // while the refresh is in flight so the user has a visible
            // spinner anchor even if their cursor leaves.
            refresh.isPending
              ? "opacity-100"
              : "opacity-0 group-hover/row:opacity-100",
            "hover:bg-surface-subtle hover:text-ink",
          )}
        >
          {refresh.isPending ? (
            <Loader2 size={11} className="animate-spin" />
          ) : (
            <RefreshCw size={11} />
          )}
        </button>
      </div>
      {effectiveOpen && (
        <div className="ml-3 border-l border-border pl-2">
          {filteredAssets?.length ? (
            filteredAssets.map((asset) => (
              <button
                key={`${schema}.${asset.name}`}
                type="button"
                onClick={() => navigate(scopePath(scope, schema, asset.name))}
                className={cn(
                  "block w-full truncate rounded px-2 py-0.5 text-left text-[11px] transition-colors duration-fast",
                  params.table === asset.name &&
                    params.schema === schema &&
                    params.profile === scope.profile
                    ? "bg-accent-soft text-accent-ink"
                    : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
                )}
                title={asset.kind}
              >
                {asset.name}
              </button>
            ))
          ) : (
            <div className="px-2 py-1 text-[11px] text-ink-dim">
              {assets
                ? query && !parentMatched
                  ? "No tables match."
                  : "(empty)"
                : "Loading…"}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
