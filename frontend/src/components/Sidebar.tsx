import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";
import { ChevronRight, ChevronDown, Database, FolderTree, Layers } from "lucide-react";

import { ApiError, api, apiFetch } from "../lib/api";
import { cn } from "../lib/cn";
import type { Scope } from "../lib/scope";
import { scopePath } from "../lib/scope";
import { InfoHint } from "./ui";
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
    return (
      <div className="flex h-full flex-col items-center gap-3 py-3 text-ink-dim">
        <Database size={16} />
        <FolderTree size={16} />
        <Layers size={16} />
      </div>
    );
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
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto px-3 pb-3">
        <ProfilesTree />
      </div>
    </div>
  );
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
  return (
    <div className="space-y-0.5 text-sm">
      <ProfilePicker
        kind="llm"
        label="LLM"
        variant="row"
        activeName={data?.active_llm_profile ?? null}
        tooltip={data?.llm_model ?? undefined}
      />
    </div>
  );
}

function ProfilesTree() {
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
  return (
    <div className="space-y-0.5">
      {list.map((p) => (
        <ProfileNode key={p.name} profile={p} />
      ))}
    </div>
  );
}

function ProfileNode({ profile }: { profile: DbProfileSummary }) {
  const params = useParams();
  // Collapsed by default so the tree doesn't fire one fetch per
  // profile on first render. Expand sticky if the user is currently
  // looking at this profile.
  const [open, setOpen] = useState<boolean>(params.profile === profile.name);

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
        {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        <span className="truncate">{profile.name}</span>
        {profile.backend && (
          <span className="ml-auto text-[9px] font-normal normal-case tracking-normal text-ink-dim">
            {profile.backend}
          </span>
        )}
      </button>
      {open && (
        <div className="ml-3 mt-0.5 border-l border-border pl-2">
          <ProfileScopeChildren profile={profile.name} />
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
function ProfileScopeChildren({ profile }: { profile: string }) {
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
    const list = catalogs.data.catalogs;
    if (list.length === 0) {
      return (
        <div className="px-2 py-1 text-[11px] text-ink-dim">(no catalogs visible)</div>
      );
    }
    return (
      <div className="space-y-0.5">
        {list.map((name) => (
          <ScopeNode
            key={name}
            scope={{ profile, catalog: name, kind: "catalog" }}
            label={name}
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
  const dbList = databases.data?.databases ?? [];
  if (dbList.length === 0) {
    return (
      <div className="px-2 py-1 text-[11px] text-ink-dim">
        (no databases reachable)
      </div>
    );
  }
  return (
    <div className="space-y-0.5">
      {dbList.map((name) => (
        <ScopeNode
          key={name}
          scope={{ profile, database: name, kind: "database" }}
          label={name}
        />
      ))}
    </div>
  );
}

function ScopeNode({ scope, label }: { scope: Scope; label: string }) {
  const params = useParams();
  const navigate = useNavigate();
  const isOnThis =
    params.profile === scope.profile &&
    (scope.database ? params.database === scope.database : params.catalog === scope.catalog);
  const [open, setOpen] = useState<boolean>(isOnThis);

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
        {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        <span className="truncate">{label}</span>
      </button>
      {open && (
        <div className="ml-3 mt-0.5 border-l border-border pl-2">
          <SchemasUnderScope scope={scope} />
        </div>
      )}
    </div>
  );
}

function SchemasUnderScope({ scope }: { scope: Scope }) {
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
  return (
    <div className="space-y-0.5">
      {data.schemas.map((schema) => (
        <SchemaNode key={schema} scope={scope} schema={schema} />
      ))}
    </div>
  );
}

function SchemaNode({ scope, schema }: { scope: Scope; schema: string }) {
  const params = useParams();
  const navigate = useNavigate();
  const isOnThis =
    params.profile === scope.profile &&
    (scope.database ? params.database === scope.database : params.catalog === scope.catalog) &&
    params.schema === schema;
  const [open, setOpen] = useState<boolean>(isOnThis);

  const { data: assets } = useQuery({
    queryKey: [
      "live-assets",
      scope.profile,
      scope.database ?? "",
      scope.catalog ?? "",
      schema,
    ],
    queryFn: () => api.liveAssets(scope, schema),
    enabled: open,
  });

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
        {open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        <span className="truncate">{schema}</span>
      </button>
      {open && (
        <div className="ml-3 border-l border-border pl-2">
          {assets?.assets?.length ? (
            assets.assets.map((asset) => (
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
              {assets ? "(empty)" : "Loading…"}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
