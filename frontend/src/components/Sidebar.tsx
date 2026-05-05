import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";
import { ChevronRight, ChevronDown, Database, FolderTree, Layers } from "lucide-react";

import { ApiError, api } from "../lib/api";
import { cn } from "../lib/cn";
import { InfoHint } from "./ui";
import ProfilePicker from "./topbar/ProfilePicker";

interface Props {
  collapsed: boolean;
}

/**
 * Live-DB asset tree: database (or catalog) → schema → table.
 * Lazy loads each level on expand. Acts as the secondary navigation
 * for the Browse experience while the TopBar carries primary routes.
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
        <SectionTitle hint="Currently active DB and LLM profiles. Switch from Settings.">
          Profiles
        </SectionTitle>
        <ProfilesSection />
        <SectionTitle
          className="mt-4"
          hint="Schema and table tree read live from the active profile."
        >
          Live database
        </SectionTitle>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto px-3 pb-3">
        <LiveDbTree />
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

function ProfilesSection() {
  const { data } = useQuery({ queryKey: ["context"], queryFn: () => api.context() });
  return (
    <div className="space-y-0.5 text-sm">
      <ProfilePicker
        kind="db"
        label="DB"
        variant="row"
        activeName={data?.active_db_profile ?? null}
        tooltip={data?.db_backend ?? undefined}
      />
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
  const navigate = useNavigate();

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
      // Land on the catalog/database page so the user can edit its
      // own description and see the schema list at full width.
      navigate("/db/active");
    },
  });

  function handleClick() {
    if (!isActive) {
      activate.mutate(name);
      setCollapsed(false);
      return;
    }
    setCollapsed((v) => !v);
    // Already-active scope: clicking the row also opens its page so
    // the user can reach the description editor without expanding.
    navigate("/db/active");
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
          "relative flex w-full items-center gap-1 rounded px-2 py-1 text-left text-sm transition-colors duration-fast",
          isActive
            ? "bg-accent-soft text-accent-ink"
            : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
          activate.isPending && "opacity-60",
        )}
      >
        {isActive && (
          <span
            className="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-r bg-accent"
            aria-hidden="true"
          />
        )}
        {expanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        <span className="truncate font-medium">{name}</span>
        {!isActive && (
          <span className="ml-auto rounded bg-surface-subtle px-1.5 py-px text-[9px] uppercase tracking-wider text-ink-dim">
            {activate.isPending ? "switching" : "switch"}
          </span>
        )}
      </button>
      {expanded && (
        <div className="ml-4 mt-0.5 border-l border-border pl-2">
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
          "relative flex w-full items-center gap-1 rounded px-2 py-1 text-left text-sm transition-colors duration-fast",
          isActiveSchema
            ? "bg-accent-soft text-accent-ink"
            : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
        )}
      >
        {isActiveSchema && (
          <span
            className="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-r bg-accent"
            aria-hidden="true"
          />
        )}
        {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        <span className="truncate font-medium">{schema}</span>
      </button>
      {open && (
        <div className="ml-4 border-l border-border pl-2">
          {assets?.assets?.length ? (
            assets.assets.map((asset) => (
              <button
                key={`${schema}.${asset.name}`}
                type="button"
                onClick={() => navigate(`/db/${profile}/${schema}/${asset.name}`)}
                className={cn(
                  "block w-full truncate rounded px-2 py-0.5 text-left text-xs transition-colors duration-fast",
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
