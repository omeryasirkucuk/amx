/**
 * LineageCreateModal — wizard for creating a fresh lineage artifact
 * from the browse page. Walks the same picker chain the CLI wizard
 * follows: profile → catalog (when supported) → database → schema →
 * table. On submit it POSTs `/refresh` and navigates the user into
 * the resulting canvas in one continuous flow.
 *
 * Reuses `liveCatalogs / liveDatabases / liveSchemas / liveAssets`
 * which are already cache-aware via TanStack Query.
 */

import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { Workflow } from "lucide-react";

import { api, apiFetch, lineageRefresh } from "../lib/api";
import Modal from "./Modal";
import { Button } from "./ui";

interface DbProfileSummary {
  name: string;
  backend?: string;
  database?: string;
  catalog?: string;
}

interface DbProfilesResponse {
  profiles: DbProfileSummary[];
}

interface Props {
  open: boolean;
  onClose: () => void;
}

const CATALOG_BACKENDS = new Set(["databricks", "bigquery", "snowflake"]);

export default function LineageCreateModal({ open, onClose }: Props) {
  const navigate = useNavigate();
  const [profile, setProfile] = useState<string>("");
  const [catalog, setCatalog] = useState<string>("");
  const [database, setDatabase] = useState<string>("");
  const [schema, setSchema] = useState<string>("");
  const [table, setTable] = useState<string>("");

  // Reset wizard state every time the modal opens so closing mid-flow
  // doesn't carry stale selections into the next session.
  useEffect(() => {
    if (open) {
      setProfile("");
      setCatalog("");
      setDatabase("");
      setSchema("");
      setTable("");
    }
  }, [open]);

  const profiles = useQuery({
    queryKey: ["db-profiles", "list"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
    enabled: open,
  });
  const profileList = useMemo(() => profiles.data?.profiles ?? [], [profiles.data]);
  const profileMeta = useMemo(
    () => profileList.find((p) => p.name === profile),
    [profileList, profile],
  );
  const supportsCatalogs = profileMeta
    ? CATALOG_BACKENDS.has(String(profileMeta.backend || "").toLowerCase())
    : false;

  // Auto-pick when only one profile is configured so the wizard skips
  // an unnecessary click.
  useEffect(() => {
    if (open && !profile && profileList.length === 1) {
      setProfile(profileList[0].name);
    }
  }, [open, profile, profileList]);

  const catalogs = useQuery({
    queryKey: ["live-catalogs", profile],
    queryFn: () => api.liveCatalogs({ profile }),
    enabled: open && !!profile && supportsCatalogs,
  });
  useEffect(() => {
    if (open && supportsCatalogs && !catalog) {
      const list = catalogs.data?.catalogs ?? [];
      const active = catalogs.data?.active_catalog ?? catalogs.data?.active_project ?? "";
      if (active && list.includes(active)) setCatalog(active);
      else if (list.length === 1) setCatalog(list[0]);
    }
  }, [open, supportsCatalogs, catalog, catalogs.data]);

  const databases = useQuery({
    queryKey: ["live-databases", profile],
    queryFn: () => api.liveDatabases({ profile }),
    enabled: open && !!profile && !supportsCatalogs,
  });
  useEffect(() => {
    if (open && !supportsCatalogs && !database) {
      const list = databases.data?.databases ?? [];
      const active = databases.data?.active_database ?? "";
      if (active && list.includes(active)) setDatabase(active);
      else if (list.length === 1) setDatabase(list[0]);
    }
  }, [open, supportsCatalogs, database, databases.data]);

  const schemasScope = useMemo(
    () => ({
      profile,
      database: supportsCatalogs ? "" : database,
      catalog: supportsCatalogs ? catalog : "",
      kind: (supportsCatalogs ? "catalog" : "database") as "catalog" | "database",
    }),
    [profile, supportsCatalogs, database, catalog],
  );
  const schemas = useQuery({
    queryKey: [
      "live-schemas",
      schemasScope.profile,
      schemasScope.database,
      schemasScope.catalog,
    ],
    queryFn: () => api.liveSchemas(schemasScope),
    enabled:
      open && !!profile && (supportsCatalogs ? !!catalog : !!database || !supportsCatalogs),
  });
  const schemaList = schemas.data?.schemas ?? [];

  const assets = useQuery({
    queryKey: [
      "live-assets",
      schemasScope.profile,
      schemasScope.database,
      schemasScope.catalog,
      schema,
    ],
    queryFn: () => api.liveAssets(schemasScope, schema),
    enabled: open && !!profile && !!schema,
  });
  const tableList = useMemo(
    () => (assets.data?.assets ?? []).filter((a) => a.kind === "table" || a.kind === "view"),
    [assets.data],
  );

  const create = useMutation({
    mutationFn: async () => {
      const anchorPath = [schema, table].filter(Boolean).join(".");
      await lineageRefresh(anchorPath, { profile });
      const slug = `${schema}-${table}`.replace(/[^A-Za-z0-9_-]+/g, "_");
      return { profile, slug };
    },
    onSuccess: (out) => {
      onClose();
      navigate(`/lineage/${encodeURIComponent(out.profile)}/${encodeURIComponent(out.slug)}`);
    },
  });

  const canSubmit = !!profile && !!schema && !!table && !create.isPending;

  return (
    <Modal
      open={open}
      onClose={onClose}
      size="md"
      title={
        <span className="inline-flex items-center gap-2">
          <Workflow className="h-4 w-4" />
          Create new lineage
        </span>
      }
      description="Pick a table to render its column-level lineage. AMX fills the canvas with everything it already knows from cache — no live DB call by default."
      footer={
        <div className="flex justify-end gap-2">
          <Button variant="secondary" size="md" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="primary"
            size="md"
            disabled={!canSubmit}
            loading={create.isPending}
            onClick={() => create.mutate()}
          >
            Open canvas →
          </Button>
        </div>
      }
    >
      <div className="space-y-4 text-sm">
        <Picker
          label="DB profile"
          value={profile}
          onChange={(v) => {
            setProfile(v);
            setCatalog("");
            setDatabase("");
            setSchema("");
            setTable("");
          }}
          options={profileList.map((p) => p.name)}
          loading={profiles.isLoading}
          empty="No DB profiles configured — add one in Settings."
        />
        {profile && supportsCatalogs && (
          <Picker
            label="Catalog / project"
            value={catalog}
            onChange={(v) => {
              setCatalog(v);
              setSchema("");
              setTable("");
            }}
            options={catalogs.data?.catalogs ?? []}
            loading={catalogs.isLoading}
            empty="No catalogs returned for this profile."
          />
        )}
        {profile && !supportsCatalogs && (
          <Picker
            label="Database"
            value={database}
            onChange={(v) => {
              setDatabase(v);
              setSchema("");
              setTable("");
            }}
            options={databases.data?.databases ?? []}
            loading={databases.isLoading}
            empty="No databases returned for this profile."
          />
        )}
        {profile && (supportsCatalogs ? catalog : database) && (
          <Picker
            label="Schema"
            value={schema}
            onChange={(v) => {
              setSchema(v);
              setTable("");
            }}
            options={schemaList}
            loading={schemas.isLoading}
            empty="No schemas in this database — run Sync first."
          />
        )}
        {schema && (
          <Picker
            label="Table"
            value={table}
            onChange={setTable}
            options={tableList.map((a) => a.name)}
            loading={assets.isLoading}
            empty="No tables in this schema — run Sync first."
          />
        )}
        {create.error && (
          <p className="text-sm text-critical">
            Could not create lineage: {(create.error as Error).message}
          </p>
        )}
      </div>
    </Modal>
  );
}

function Picker({
  label,
  value,
  onChange,
  options,
  loading,
  empty,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  options: string[];
  loading?: boolean;
  empty?: string;
}) {
  return (
    <label className="block space-y-1">
      <span className="text-xs uppercase tracking-wide text-fg-muted">{label}</span>
      {loading ? (
        <div className="rounded-md border border-surface-border bg-surface-muted px-3 py-2 text-xs text-fg-muted">
          Loading…
        </div>
      ) : options.length === 0 ? (
        <div className="rounded-md border border-dashed border-surface-border bg-surface-muted px-3 py-2 text-xs text-fg-muted">
          {empty ?? "Nothing to choose."}
        </div>
      ) : (
        <select
          className="block w-full rounded-md border border-surface-border bg-surface-raised px-3 py-2 text-sm focus:border-accent-default focus:outline-none focus:ring-1 focus:ring-accent-default"
          value={value}
          onChange={(e) => onChange(e.target.value)}
        >
          <option value="">— pick {label.toLowerCase()} —</option>
          {options.map((opt) => (
            <option key={opt} value={opt}>
              {opt}
            </option>
          ))}
        </select>
      )}
    </label>
  );
}
