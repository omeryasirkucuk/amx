/**
 * SyncScopeDialog — pick a scope and fire an immediate cache refresh.
 *
 * The Catalog cache page exposes this dialog as a "Sync scope…"
 * affordance so power users don't have to leave the page to refresh
 * an arbitrary schema / table / column subset (the left sidebar
 * carries the same per-item icons, but only down to schema). The
 * dialog reuses ScopeTree so the picker UX matches the scheduled
 * refresh dialog, but removes everything related to scheduling —
 * no datetime, no recurrence, no cron — because the action is
 * "do it right now" and persisting a one-off schedule row would be
 * noise. Submits to ``POST /api/catalog/refresh`` which runs the
 * same ``cache_refresh_executor`` synchronously.
 */

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { Button, Dialog, Field, Select, useToast } from "./ui";
import {
  api,
  apiFetch,
  type CatalogScopeRefreshPayload,
} from "../lib/api";
import ScopeTree, { picksToScopeJson, type SchemaPick } from "./ScopeTree";

interface DbProfileSummary {
  name: string;
  backend: string;
  database: string;
  catalog?: string;
}

const CATALOG_BACKENDS = new Set(["databricks", "bigquery"]);

export interface SyncScopeDialogProps {
  open: boolean;
  onClose: () => void;
  /**
   * Optional initial profile pick — when the dialog is launched
   * with a specific profile in mind (sidebar context menu in a
   * future iteration, for instance) we can pre-select it.
   */
  initialProfile?: string | null;
}

export default function SyncScopeDialog({
  open,
  onClose,
  initialProfile,
}: SyncScopeDialogProps) {
  const toast = useToast();
  const qc = useQueryClient();

  const [dbProfile, setDbProfile] = useState(initialProfile ?? "");
  const [database, setDatabase] = useState("");
  const [scopePicks, setScopePicks] = useState<SchemaPick[]>([]);
  const [error, setError] = useState<string | null>(null);

  const dbProfilesQ = useQuery({
    queryKey: ["profiles", "db"],
    queryFn: () =>
      apiFetch<{ profiles: DbProfileSummary[] }>("/api/profiles/db"),
    enabled: open,
  });

  const selectedProfile = dbProfilesQ.data?.profiles.find(
    (p) => p.name === dbProfile,
  );
  const isCatalogBackend = Boolean(
    selectedProfile && CATALOG_BACKENDS.has(selectedProfile.backend),
  );

  const databasesQ = useQuery({
    queryKey: ["scope-tree-dbs", dbProfile, isCatalogBackend],
    queryFn: () => {
      const path = isCatalogBackend
        ? `/api/live/catalogs?profile=${encodeURIComponent(dbProfile)}`
        : `/api/live/databases?profile=${encodeURIComponent(dbProfile)}`;
      return apiFetch<{
        databases?: string[];
        catalogs?: string[];
      }>(path);
    },
    enabled: open && Boolean(dbProfile),
  });
  const databaseOptions = isCatalogBackend
    ? databasesQ.data?.catalogs ?? []
    : databasesQ.data?.databases ?? [];

  if (open && dbProfile && !database && databaseOptions.length) {
    setDatabase(databaseOptions[0]);
  }
  if (open && !dbProfile && dbProfilesQ.data?.profiles?.length) {
    setDbProfile(dbProfilesQ.data.profiles[0].name);
  }

  const mutation = useMutation({
    mutationFn: (body: CatalogScopeRefreshPayload) => api.refreshCatalogScope(body),
    onSuccess: () => {
      toast.push({ tone: "success", title: "Cache synced" });
      // Cover every consumer that paints something derived from
      // the cache so the user's next look at any of those surfaces
      // the freshly-warmed state.
      qc.invalidateQueries({ queryKey: ["catalog-freshness"] });
      qc.invalidateQueries({ queryKey: ["db-cache", "show"] });
      qc.invalidateQueries({ queryKey: ["db-cache", "stats"] });
      qc.invalidateQueries({ queryKey: ["live-schemas"] });
      qc.invalidateQueries({ queryKey: ["live-catalogs"] });
      qc.invalidateQueries({ queryKey: ["live-databases"] });
      setError(null);
      setScopePicks([]);
      onClose();
    },
    onError: (err) =>
      setError(err instanceof Error ? err.message : String(err)),
  });

  function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    if (!dbProfile) {
      setError("Pick a DB profile.");
      return;
    }
    const scope = picksToScopeJson(scopePicks);
    mutation.mutate({
      profile: dbProfile,
      database: isCatalogBackend ? null : database || null,
      catalog: isCatalogBackend ? database || null : null,
      scope,
    });
  }

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Sync catalog cache"
      size="lg"
    >
      <form onSubmit={onSubmit} className="space-y-3 sm:space-y-4">
        <p className="text-xs text-ink-dim">
          Invalidates the catalog cache for the picked scope and
          re-populates from the live DB. Same path the scheduler uses
          for cache_refresh schedules — but fires once, right now.
        </p>
        <div className="grid grid-cols-1 items-start gap-x-3 gap-y-3 sm:gap-x-4 sm:gap-y-4 md:grid-cols-2">
          <Field label="DB profile" required>
            <Select
              value={dbProfile}
              onChange={(e) => {
                setDbProfile(e.target.value);
                setDatabase("");
                setScopePicks([]);
              }}
              required
              disabled={dbProfilesQ.isLoading}
            >
              {dbProfilesQ.isLoading && <option value="">Loading…</option>}
              {!dbProfilesQ.isLoading &&
                !dbProfilesQ.data?.profiles?.length && (
                  <option value="">(no profiles configured)</option>
                )}
              {dbProfilesQ.data?.profiles?.map((p) => (
                <option key={p.name} value={p.name}>
                  {p.name} · {p.backend}
                </option>
              ))}
            </Select>
          </Field>
          <Field label={isCatalogBackend ? "Catalog" : "Database"} required>
            <Select
              value={database}
              onChange={(e) => {
                setDatabase(e.target.value);
                setScopePicks([]);
              }}
              required
              disabled={!dbProfile || databasesQ.isLoading}
            >
              {!dbProfile && <option value="">Pick a DB profile first</option>}
              {dbProfile && databasesQ.isLoading && (
                <option value="">Loading…</option>
              )}
              {dbProfile &&
                !databasesQ.isLoading &&
                !databaseOptions.length && (
                  <option value="">(none visible)</option>
                )}
              {databaseOptions.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </Select>
          </Field>
          <Field
            label="Scope"
            description="Leave empty to sync the whole database. Tick schemas to limit; expand for tables and columns."
            className="md:col-span-2"
          >
            <ScopeTree
              dbProfile={dbProfile}
              database={database}
              isCatalogBackend={isCatalogBackend}
              picks={scopePicks}
              onChange={setScopePicks}
            />
          </Field>
        </div>
        {error && (
          <p className="rounded-md border border-critical/40 bg-critical/10 px-3 py-2 text-sm text-critical">
            {error}
          </p>
        )}
        <div className="flex flex-wrap items-center justify-end gap-2 pt-1">
          <Button variant="secondary" size="md" onClick={onClose} type="button">
            Cancel
          </Button>
          <Button
            type="submit"
            variant="primary"
            size="md"
            disabled={mutation.isPending || !dbProfile}
          >
            {mutation.isPending ? "Syncing…" : "Sync now"}
          </Button>
        </div>
      </form>
    </Dialog>
  );
}
