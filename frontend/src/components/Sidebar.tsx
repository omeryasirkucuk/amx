import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate, useParams } from "react-router-dom";
import { ChevronRight, ChevronDown, Database, FolderTree, Layers } from "lucide-react";

import { api } from "../lib/api";
import { cn } from "../lib/cn";

interface Props {
  collapsed: boolean;
}

// Live-DB asset tree: catalog → schema → table.
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

function LiveDbTree() {
  const { data: schemas } = useQuery({
    queryKey: ["live-schemas"],
    queryFn: () => api.liveSchemas(),
  });
  if (!schemas) {
    return (
      <div className="px-2 py-1 text-xs text-ink-dim">
        Loading schemas…
      </div>
    );
  }
  if (schemas.schemas.length === 0) {
    return (
      <div className="px-2 py-1 text-xs text-ink-dim">
        No schemas reachable yet — activate a DB profile under Settings.
      </div>
    );
  }
  return (
    <div className="space-y-0.5">
      {schemas.schemas.map((schema) => (
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
