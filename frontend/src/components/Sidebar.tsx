import { useState, useSyncExternalStore } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import type { QueryClient } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";
import {
  ChevronRight,
  ChevronDown,
  Database,
  FolderTree,
  Layers,
  Search,
  X,
} from "lucide-react";

import { ApiError, api, apiFetch } from "../lib/api";
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
        placeholder="Search profiles, databases…"
        aria-label="Search DB profiles"
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
}
interface DatabasesCache {
  databases: string[];
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
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        title={profile.backend || ""}
        className={cn(
          "flex w-full items-center gap-1 rounded px-2 py-1 text-left transition-colors duration-fast",
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
    const list = catalogs.data.catalogs.filter(childFilter);
    if (catalogs.data.catalogs.length === 0) {
      return (
        <div className="px-2 py-1 text-[11px] text-ink-dim">(no catalogs visible)</div>
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
  const allDbList = databases.data?.databases ?? [];
  const dbList = allDbList.filter(childFilter);
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

  return (
    <div>
      <button
        type="button"
        onClick={() => {
          setOpen((v) => !v);
          navigate(scopePath(scope));
        }}
        className={cn(
          "flex w-full items-center gap-1 rounded px-2 py-1 text-left text-[13px] transition-colors duration-fast",
          isOnThis
            ? "bg-accent-soft text-accent-ink"
            : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
        )}
      >
        {effectiveOpen ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        <span className="truncate">{label}</span>
      </button>
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
  // When parentMatched is true, the user already hit on something
  // higher up the chain (a profile / db / catalog) -- show every
  // schema unfiltered. Otherwise narrow to schemas whose name
  // matches OR that have a cached asset list with a table name
  // match (so a typed table name keeps its parent schema visible
  // even before the user clicks into it).
  const schemas = query
    ? data.schemas.filter(
        (s) =>
          parentMatched ||
          matchesSearch(s, query) ||
          schemaHasMatchingAssetInCache(qc, scope, s, query),
      )
    : data.schemas;
  if (schemas.length === 0) {
    return <div className="px-2 py-1 text-[11px] text-ink-dim">No schemas match.</div>;
  }
  return (
    <div className="space-y-0.5">
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

  const { data: assets } = useQuery({
    queryKey: [
      "live-assets",
      scope.profile,
      scope.database ?? "",
      scope.catalog ?? "",
      schema,
    ],
    queryFn: () => api.liveAssets(scope, schema),
    enabled: effectiveOpen,
  });

  const filteredAssets =
    assets?.assets && query && !parentMatched
      ? assets.assets.filter((a) => matchesSearch(a.name, query))
      : assets?.assets;

  return (
    <div>
      <button
        type="button"
        onClick={() => {
          setOpen((v) => !v);
          navigate(scopePath(scope, schema));
        }}
        className={cn(
          "flex w-full items-center gap-1 rounded px-2 py-1 text-left text-[12px] transition-colors duration-fast",
          isOnThis
            ? "bg-accent-soft text-accent-ink"
            : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
        )}
      >
        {effectiveOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        <span className="truncate">{schema}</span>
      </button>
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
