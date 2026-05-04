import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";
import { ChevronRight, ChevronDown, Database, FolderTree, Layers } from "lucide-react";

import { ApiError, api } from "../lib/api";
import { cn } from "../lib/cn";

interface Props {
  collapsed: boolean;
}

// Live-DB asset tree: database (or catalog) → schema → table.
// Lazy loads each level on expand so a workspace with thousands of
// schemas doesn't pay the cost up front. PR-F adds virtualization.
export default function Sidebar({ collapsed }: Props) {
  if (collapsed) {
    return (
      <div className="flex h-full flex-col items-center gap-3 py-4 text-ink-dim">
        <Database size={18} />
        <FolderTree size={18} />
        <Layers size={18} />
      </div>
    );
  }
  return (
    <div className="h-full overflow-y-auto px-3 py-4">
      <SectionTitle>Profiles</SectionTitle>
      <ProfilesSection />
      <SectionTitle className="mt-5">Live database</SectionTitle>
      <LiveDbTree />
    </div>
  );
}

function SectionTitle({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "px-2 pb-1 text-[10px] font-semibold uppercase tracking-[0.08em] text-ink-dim",
        className,
      )}
    >
      {children}
    </div>
  );
}

function ProfilesSection() {
  const { data } = useQuery({ queryKey: ["context"], queryFn: () => api.context() });
  return (
    <div className="space-y-0.5 text-sm">
      <ProfileRow label="DB profile" value={data?.active_db_profile ?? "(none)"} />
      <ProfileRow label="LLM profile" value={data?.active_llm_profile ?? "(none)"} />
    </div>
  );
}

function ProfileRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between rounded-md px-2 py-1 text-ink-muted">
      <span className="text-[11px] text-ink-dim">{label}</span>
      <span className="truncate font-mono text-xs text-ink">{value}</span>
    </div>
  );
}

// Top of the live tree. Decides which axis to render — catalogs for
// 3-level backends (Databricks, BigQuery), databases for 2-level
// backends (Postgres, MySQL, …). Each top-level node is collapsible
// and clicking an inactive one switches the active scope before
// loading its schemas, mirroring the CLI's /connect flow.
function LiveDbTree() {
  const catalogs = useQuery({
    queryKey: ["live-catalogs"],
    queryFn: () => api.liveCatalogs(),
    retry: false,
  });
  const databases = useQuery({
    queryKey: ["live-databases"],
    queryFn: () => api.liveDatabases(),
    retry: false,
    enabled: catalogs.data ? !catalogs.data.supports_catalogs : false,
  });

  if (catalogs.isLoading) {
    return <div className="px-2 py-1 text-xs text-ink-dim">Loading…</div>;
  }
  if (catalogs.error) {
    return (
      <div className="px-2 py-1 text-xs text-critical">
        {(catalogs.error as Error).message}
      </div>
    );
  }
  if (catalogs.data?.supports_catalogs) {
    const list = catalogs.data.catalogs;
    if (list.length === 0) {
      return (
        <div className="px-2 py-1 text-xs text-ink-dim">
          No catalogs visible — check your DB profile credentials.
        </div>
      );
    }
    return (
      <div className="space-y-0.5">
        {list.map((name) => (
          <ScopeNode
            key={name}
            kind="catalog"
            name={name}
            isActive={catalogs.data.active_catalog === name}
          />
        ))}
      </div>
    );
  }

  // 2-level path
  if (databases.isLoading) {
    return <div className="px-2 py-1 text-xs text-ink-dim">Loading databases…</div>;
  }
  if (databases.error) {
    return (
      <div className="px-2 py-1 text-xs text-critical">
        {(databases.error as Error).message}
      </div>
    );
  }
  const dbList = databases.data?.databases ?? [];
  if (dbList.length === 0) {
    return (
      <div className="px-2 py-1 text-xs text-ink-dim">
        No databases reachable yet — activate a DB profile under Settings.
      </div>
    );
  }
  return (
    <div className="space-y-0.5">
      {dbList.map((name) => (
        <ScopeNode
          key={name}
          kind="database"
          name={name}
          isActive={databases.data?.active_database === name}
        />
      ))}
    </div>
  );
}

// One database (Postgres) or catalog (Databricks) row.
//
// UX rules:
//   * The currently active scope is highlighted and auto-expanded;
//     clicking its chevron just toggles the schema sub-tree.
//   * An inactive scope click triggers activation. When the mutation
//     succeeds, query invalidation re-renders this node with
//     ``isActive=true`` and the schema list shows up automatically.
//   * No schemas are loaded for inactive scopes — the connector cache
//     and the live SQL session are bound to a single active scope at
//     a time, exactly like the CLI.
function ScopeNode({
  kind,
  name,
  isActive,
}: {
  kind: "database" | "catalog";
  name: string;
  isActive: boolean;
}) {
  const [collapsed, setCollapsed] = useState<boolean>(false);
  const queryClient = useQueryClient();

  const activate = useMutation<unknown, Error, string>({
    mutationFn: (chosen: string) =>
      kind === "catalog"
        ? api.activateCatalog(chosen, true)
        : api.activateDatabase(chosen, true),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["live-catalogs"] });
      queryClient.invalidateQueries({ queryKey: ["live-databases"] });
      queryClient.invalidateQueries({ queryKey: ["live-schemas"] });
      queryClient.invalidateQueries({ queryKey: ["live-assets"] });
      queryClient.invalidateQueries({ queryKey: ["context"] });
    },
  });

  function handleClick() {
    if (!isActive) {
      activate.mutate(name);
      setCollapsed(false);
      return;
    }
    setCollapsed((v) => !v);
  }

  const expanded = isActive && !collapsed;

  return (
    <div>
      <button
        type="button"
        onClick={handleClick}
        disabled={activate.isPending}
        title={isActive ? `Active ${kind}` : `Switch to ${name}`}
        className={cn(
          "flex w-full items-center gap-1 rounded-md px-2 py-1 text-left text-sm",
          isActive
            ? "bg-accent-soft text-accent-ink"
            : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
          activate.isPending && "opacity-60",
        )}
      >
        {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        <span className="truncate font-medium">{name}</span>
        {!isActive && (
          <span className="ml-auto rounded-full bg-surface-subtle px-1.5 py-px text-[9px] uppercase tracking-wider text-ink-dim">
            {activate.isPending ? "switching" : "switch"}
          </span>
        )}
      </button>
      {expanded && (
        <div className="ml-4 mt-0.5 border-l border-surface-border pl-2">
          <SchemasUnderActiveScope />
        </div>
      )}
      {activate.isError && (
        <div className="ml-6 py-1 text-[11px] text-critical">
          {activate.error instanceof Error
            ? activate.error.message
            : "Switch failed."}
        </div>
      )}
    </div>
  );
}

// The schema list of the *currently active* scope. Lives one level
// inside ScopeNode so when the user switches catalogs/databases
// only this subtree refetches.
function SchemasUnderActiveScope() {
  const { data, error, isLoading } = useQuery({
    queryKey: ["live-schemas"],
    queryFn: () => api.liveSchemas(),
    retry: false,
  });
  if (isLoading) {
    return <div className="px-2 py-1 text-xs text-ink-dim">Loading schemas…</div>;
  }
  if (error instanceof ApiError && error.hint === "select-catalog") {
    return (
      <div className="px-2 py-1 text-xs text-warning">
        Catalog not yet selected.
      </div>
    );
  }
  if (error instanceof ApiError && error.hint === "select-database") {
    return (
      <div className="px-2 py-1 text-xs text-warning">
        Database not yet selected.
      </div>
    );
  }
  if (error) {
    return (
      <div className="px-2 py-1 text-xs text-critical">
        {(error as Error).message}
      </div>
    );
  }
  if (!data || data.schemas.length === 0) {
    return <div className="px-2 py-1 text-xs text-ink-dim">(no schemas)</div>;
  }
  return (
    <div className="space-y-0.5">
      {data.schemas.map((schema) => (
        <SchemaNode key={schema} schema={schema} />
      ))}
    </div>
  );
}

function SchemaNode({ schema }: { schema: string }) {
  const params = useParams();
  const isActiveSchema = params.schema === schema;
  const [open, setOpen] = useState<boolean>(isActiveSchema);
  const navigate = useNavigate();
  const profile = params.profile || "active";

  const { data: assets } = useQuery({
    queryKey: ["live-assets", schema],
    queryFn: () => api.liveAssets(schema),
    enabled: open,
  });

  return (
    <div>
      <button
        type="button"
        onClick={() => {
          setOpen((v) => !v);
          navigate(`/db/${profile}/${schema}`);
        }}
        className={cn(
          "flex w-full items-center gap-1 rounded-md px-2 py-1 text-left text-sm",
          isActiveSchema
            ? "bg-accent-soft text-accent-ink"
            : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
        )}
      >
        {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        <span className="truncate font-medium">{schema}</span>
      </button>
      {open && (
        <div className="ml-4 border-l border-surface-border pl-2">
          {assets?.assets?.length ? (
            assets.assets.map((asset) => (
              <button
                key={`${schema}.${asset.name}`}
                type="button"
                onClick={() => navigate(`/db/${profile}/${schema}/${asset.name}`)}
                className={cn(
                  "block w-full truncate rounded-md px-2 py-0.5 text-left text-xs",
                  params.table === asset.name
                    ? "bg-accent-soft text-accent-ink"
                    : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
                )}
                title={`${asset.kind}`}
              >
                {asset.name}
              </button>
            ))
          ) : (
            <div className="px-2 py-1 text-xs text-ink-dim">
              {assets ? "(empty)" : "Loading…"}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
