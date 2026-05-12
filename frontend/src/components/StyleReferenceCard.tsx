import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api, apiFetch } from "../lib/api";
import { Card, CardBody, CardHeader } from "./Card";
import { AlertDialog, Select } from "./ui";

interface DbProfileSummary {
  name: string;
  backend: string;
  catalog?: string;
  database?: string;
}

interface DbProfilesResponse {
  profiles: DbProfileSummary[];
}

type StoredStyleProfile = {
  llm_profile: string;
  source_ref: string;
  source_db_kind: string;
  enabled: boolean;
  sample_count: number;
  profile: {
    language: string;
    tone: string;
    avg_length_words: number;
    length_range: [number, number];
    person: string;
    capitalization: string;
    ends_with_period: boolean;
    structural_patterns: string[];
    vocabulary_register: string;
    redacted_examples: string[];
  };
  created_at: number;
  updated_at: number;
};

export function StyleReferenceCard({ llmProfile }: { llmProfile: string | null }) {
  const qc = useQueryClient();
  const [dbProfile, setDbProfile] = useState<string>("");
  const [catalogOrDb, setCatalogOrDb] = useState<string>("");
  const [schema, setSchema] = useState<string>("");
  const [table, setTable] = useState<string>("");
  const [confirmClearOpen, setConfirmClearOpen] = useState(false);

  const profileName = llmProfile ?? "";
  const enabled = Boolean(profileName);

  const dbProfiles = useQuery({
    queryKey: ["profiles", "db", "for-style"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
    retry: false,
  });

  // Default the DB profile selector to the first available one so the
  // user lands on the picker with the first dropdown pre-populated.
  useEffect(() => {
    if (!dbProfile && dbProfiles.data?.profiles?.length) {
      setDbProfile(dbProfiles.data.profiles[0].name);
    }
  }, [dbProfile, dbProfiles.data]);

  const catalogs = useQuery({
    queryKey: ["live-catalogs", dbProfile, "for-style"],
    queryFn: () => api.liveCatalogs({ profile: dbProfile }),
    enabled: Boolean(dbProfile),
    retry: false,
  });

  const supportsCatalogs = catalogs.data?.supports_catalogs ?? false;

  const databases = useQuery({
    queryKey: ["live-databases", dbProfile, "for-style"],
    queryFn: () => api.liveDatabases({ profile: dbProfile }),
    enabled: Boolean(dbProfile) && catalogs.data ? !supportsCatalogs : false,
    retry: false,
  });

  const catOrDbOptions = useMemo<string[]>(() => {
    if (supportsCatalogs) return catalogs.data?.catalogs ?? [];
    return databases.data?.databases ?? [];
  }, [supportsCatalogs, catalogs.data, databases.data]);

  // Reset downstream selections whenever an upstream pick changes —
  // a stale schema/table from a previous catalog would otherwise leak
  // into the new context and produce a 502 from the extract endpoint.
  useEffect(() => {
    setCatalogOrDb("");
    setSchema("");
    setTable("");
  }, [dbProfile]);

  useEffect(() => {
    setSchema("");
    setTable("");
  }, [catalogOrDb]);

  useEffect(() => {
    setTable("");
  }, [schema]);

  const schemasQuery = useQuery({
    queryKey: ["live-schemas", dbProfile, supportsCatalogs ? "cat" : "db", catalogOrDb, "for-style"],
    queryFn: () =>
      api.liveSchemas({
        profile: dbProfile,
        kind: supportsCatalogs ? "catalog" : "database",
        catalog: supportsCatalogs ? catalogOrDb : undefined,
        database: supportsCatalogs ? undefined : catalogOrDb,
      }),
    enabled: Boolean(dbProfile && catalogOrDb),
    retry: false,
  });

  const tablesQuery = useQuery({
    queryKey: ["live-assets", dbProfile, supportsCatalogs ? "cat" : "db", catalogOrDb, schema, "for-style"],
    queryFn: () =>
      api.liveAssets(
        {
          profile: dbProfile,
          kind: supportsCatalogs ? "catalog" : "database",
          catalog: supportsCatalogs ? catalogOrDb : undefined,
          database: supportsCatalogs ? undefined : catalogOrDb,
        },
        schema,
      ),
    enabled: Boolean(dbProfile && catalogOrDb && schema),
    retry: false,
  });

  const sourceRef =
    dbProfile && catalogOrDb && schema && table
      ? `${catalogOrDb}.${schema}.${table}`
      : "";

  const styleQuery = useQuery<StoredStyleProfile | null>({
    queryKey: ["style", profileName],
    queryFn: async () => {
      try {
        return await apiFetch<StoredStyleProfile>(
          `/api/llm-profiles/${encodeURIComponent(profileName)}/style`,
        );
      } catch (e) {
        const status = (e as { status?: number }).status;
        if (status === 404) return null;
        throw e;
      }
    },
    enabled,
    retry: false,
  });

  const extract = useMutation({
    mutationFn: (body: { source_ref: string; db_profile: string }) =>
      apiFetch(`/api/llm-profiles/${encodeURIComponent(profileName)}/style/extract`, {
        method: "POST",
        body: JSON.stringify(body),
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["style", profileName] }),
  });

  const toggle = useMutation({
    mutationFn: (next: boolean) =>
      apiFetch(`/api/llm-profiles/${encodeURIComponent(profileName)}/style`, {
        method: "PATCH",
        body: JSON.stringify({ enabled: next }),
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["style", profileName] }),
  });

  const clear = useMutation({
    mutationFn: () =>
      apiFetch(`/api/llm-profiles/${encodeURIComponent(profileName)}/style`, {
        method: "DELETE",
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["style", profileName] }),
  });

  if (!profileName) {
    return (
      <Card>
        <CardHeader
          title="Writing style reference"
          description="Activate an LLM profile to configure a style reference."
        />
      </Card>
    );
  }

  const row = styleQuery.data;
  const isBusy = extract.isPending || toggle.isPending || clear.isPending;

  return (
    <Card>
      <CardHeader
        title="Writing style reference"
        description={
          row
            ? `Attached to '${profileName}'. AMX matches this style on runs.`
            : `Attach a reference table so AMX matches your description style for '${profileName}'.`
        }
      />
      <CardBody>
        {extract.isPending ? (
          <div className="text-sm text-ink-dim">Extracting style…</div>
        ) : row ? (
          <div className="space-y-3 text-sm">
            <div>
              Source: <code className="font-mono">{row.source_ref}</code> ·{" "}
              backend: <span className="font-mono">{row.source_db_kind}</span>
            </div>
            <div className="grid grid-cols-2 gap-2 text-ink-muted">
              <div>Language: {row.profile.language}</div>
              <div>Tone: {row.profile.tone}</div>
              <div>
                Length: {row.profile.length_range[0]}–{row.profile.length_range[1]} words
              </div>
              <div>Samples: {row.sample_count}</div>
            </div>
            <details>
              <summary className="cursor-pointer text-xs text-ink-dim">
                Raw profile JSON
              </summary>
              <pre className="mt-2 max-h-72 overflow-auto rounded-md bg-surface-subtle p-3 text-xs">
                {JSON.stringify(row.profile, null, 2)}
              </pre>
            </details>
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={row.enabled}
                disabled={isBusy}
                onChange={(e) => toggle.mutate(e.target.checked)}
              />
              Use this style on runs
            </label>
            <SourceRefPicker
              dbProfiles={dbProfiles.data?.profiles ?? []}
              dbProfile={dbProfile}
              onDbProfileChange={setDbProfile}
              supportsCatalogs={supportsCatalogs}
              catOrDbOptions={catOrDbOptions}
              catOrDbLoading={catalogs.isLoading || databases.isLoading}
              catalogOrDb={catalogOrDb}
              onCatOrDbChange={setCatalogOrDb}
              schemaOptions={schemasQuery.data?.schemas ?? []}
              schemaLoading={schemasQuery.isLoading}
              schema={schema}
              onSchemaChange={setSchema}
              tableOptions={(tablesQuery.data?.assets ?? []).map((a) => a.name)}
              tableLoading={tablesQuery.isLoading}
              table={table}
              onTableChange={setTable}
              busy={isBusy}
            />
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                disabled={!sourceRef || isBusy}
                onClick={() => extract.mutate({ source_ref: sourceRef, db_profile: dbProfile })}
                className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-50"
              >
                Re-extract
              </button>
              <button
                type="button"
                disabled={isBusy}
                onClick={() => setConfirmClearOpen(true)}
                className="rounded-md border border-surface-border px-3 py-1.5 text-sm text-ink-muted hover:bg-surface-subtle"
              >
                Clear
              </button>
            </div>
          </div>
        ) : (
          <div className="space-y-3 text-sm">
            <p className="text-ink-muted">
              Provide a reference table whose column comments show your team's
              writing style. AMX reads only column comments — never row data —
              and distills a style profile used on future runs. Domain terms
              from the reference table are never leaked into other tables'
              descriptions.
            </p>
            <SourceRefPicker
              dbProfiles={dbProfiles.data?.profiles ?? []}
              dbProfile={dbProfile}
              onDbProfileChange={setDbProfile}
              supportsCatalogs={supportsCatalogs}
              catOrDbOptions={catOrDbOptions}
              catOrDbLoading={catalogs.isLoading || databases.isLoading}
              catalogOrDb={catalogOrDb}
              onCatOrDbChange={setCatalogOrDb}
              schemaOptions={schemasQuery.data?.schemas ?? []}
              schemaLoading={schemasQuery.isLoading}
              schema={schema}
              onSchemaChange={setSchema}
              tableOptions={(tablesQuery.data?.assets ?? []).map((a) => a.name)}
              tableLoading={tablesQuery.isLoading}
              table={table}
              onTableChange={setTable}
              busy={isBusy}
            />
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                disabled={!sourceRef || isBusy}
                onClick={() => extract.mutate({ source_ref: sourceRef, db_profile: dbProfile })}
                className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-50"
              >
                Extract style
              </button>
            </div>
          </div>
        )}
        {extract.isError && (
          <div className="mt-3 rounded-md bg-critical/10 px-3 py-2 text-sm text-critical">
            Extract failed: {(extract.error as Error).message}
          </div>
        )}
      </CardBody>
      <AlertDialog
        open={confirmClearOpen}
        onClose={() => {
          if (!clear.isPending) setConfirmClearOpen(false);
        }}
        onConfirm={() =>
          clear.mutate(undefined, {
            onSettled: () => setConfirmClearOpen(false),
          })
        }
        title={`Clear style reference for '${profileName}'?`}
        description="The extracted style profile is removed from this LLM. Future runs fall back to the default tone until you attach a new reference."
        confirmLabel="Clear"
        loading={clear.isPending}
      />
    </Card>
  );
}

interface PickerProps {
  dbProfiles: DbProfileSummary[];
  dbProfile: string;
  onDbProfileChange: (value: string) => void;
  supportsCatalogs: boolean;
  catOrDbOptions: string[];
  catOrDbLoading: boolean;
  catalogOrDb: string;
  onCatOrDbChange: (value: string) => void;
  schemaOptions: string[];
  schemaLoading: boolean;
  schema: string;
  onSchemaChange: (value: string) => void;
  tableOptions: string[];
  tableLoading: boolean;
  table: string;
  onTableChange: (value: string) => void;
  busy: boolean;
}

function PickerField({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="flex flex-col gap-1 text-xs text-ink-dim">
      <span className="flex items-center gap-2">
        <span className="font-medium uppercase tracking-wide">{label}</span>
        {hint ? <span className="text-ink-dim/80 normal-case">{hint}</span> : null}
      </span>
      {children}
    </label>
  );
}

function SourceRefPicker({
  dbProfiles,
  dbProfile,
  onDbProfileChange,
  supportsCatalogs,
  catOrDbOptions,
  catOrDbLoading,
  catalogOrDb,
  onCatOrDbChange,
  schemaOptions,
  schemaLoading,
  schema,
  onSchemaChange,
  tableOptions,
  tableLoading,
  table,
  onTableChange,
  busy,
}: PickerProps) {
  if (dbProfiles.length === 0) {
    return (
      <div className="rounded-md border border-dashed border-surface-border bg-surface-subtle px-3 py-2 text-sm text-ink-muted">
        Add a DB profile from the Database tab to pick a reference table.
      </div>
    );
  }
  const catLabel = supportsCatalogs ? "Catalog" : "Database";
  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
      <PickerField label="DB profile">
        <Select
          value={dbProfile}
          disabled={busy}
          onChange={(e) => onDbProfileChange(e.target.value)}
        >
          {dbProfiles.map((p) => (
            <option key={p.name} value={p.name}>
              {p.name} ({p.backend})
            </option>
          ))}
        </Select>
      </PickerField>
      <PickerField
        label={catLabel}
        hint={dbProfile && catOrDbLoading ? "loading…" : undefined}
      >
        <Select
          value={catalogOrDb}
          disabled={busy || !dbProfile || catOrDbLoading || catOrDbOptions.length === 0}
          onChange={(e) => onCatOrDbChange(e.target.value)}
        >
          <option value="">
            {catOrDbOptions.length === 0 && !catOrDbLoading
              ? `(no ${catLabel.toLowerCase()}s visible)`
              : `Select a ${catLabel.toLowerCase()}…`}
          </option>
          {catOrDbOptions.map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </Select>
      </PickerField>
      <PickerField
        label="Schema"
        hint={catalogOrDb && schemaLoading ? "loading…" : undefined}
      >
        <Select
          value={schema}
          disabled={busy || !catalogOrDb || schemaLoading || schemaOptions.length === 0}
          onChange={(e) => onSchemaChange(e.target.value)}
        >
          <option value="">
            {schemaOptions.length === 0 && !schemaLoading && catalogOrDb
              ? "(no schemas visible)"
              : "Select a schema…"}
          </option>
          {schemaOptions.map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </Select>
      </PickerField>
      <PickerField label="Table" hint={schema && tableLoading ? "loading…" : undefined}>
        <Select
          value={table}
          disabled={busy || !schema || tableLoading || tableOptions.length === 0}
          onChange={(e) => onTableChange(e.target.value)}
        >
          <option value="">
            {tableOptions.length === 0 && !tableLoading && schema
              ? "(no tables visible)"
              : "Select a table…"}
          </option>
          {tableOptions.map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </Select>
      </PickerField>
    </div>
  );
}
