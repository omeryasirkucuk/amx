/**
 * Cross-profile Add Table modal.
 *
 * Profile is picked per add (not pinned canvas-wide) so a single
 * canvas can host nodes from any number of DB profiles. Schema and
 * table pickers cascade from the chosen profile. On submit the modal
 * fetches the table's column list so the new DataFrameNode lands
 * already-typed.
 */

import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Plus, Search } from "lucide-react";

import { Button } from "../../components/ui";
import Modal from "../../components/Modal";
import {
  fetchAssets,
  fetchProfiles,
  fetchSchemas,
  fetchTableColumns,
  supportsCatalogs,
  type ProfileSummary,
} from "../amx-bridge/catalog";
import type { AddTablePick } from "../types";

interface Props {
  open: boolean;
  onClose: () => void;
  defaultProfile: string;
  onPick: (pick: AddTablePick) => void;
}

export function AddTableModal({ open, onClose, defaultProfile, onPick }: Props) {
  const [profile, setProfile] = useState<string>("");
  const [database, setDatabase] = useState<string>("");
  const [catalog, setCatalog] = useState<string>("");
  const [schema, setSchema] = useState<string>("");
  const [query, setQuery] = useState<string>("");
  const [loadingPick, setLoadingPick] = useState(false);

  useEffect(() => {
    if (open) {
      setProfile(defaultProfile);
      setDatabase("");
      setCatalog("");
      setSchema("");
      setQuery("");
    }
  }, [open, defaultProfile]);

  const profilesQ = useQuery({
    queryKey: ["db-profiles", "list"],
    queryFn: fetchProfiles,
    enabled: open,
  });

  const profileMeta: ProfileSummary | undefined = profilesQ.data?.find(
    (p) => p.name === profile,
  );
  const usesCatalogs = supportsCatalogs(profileMeta);

  const dbsQ = useQuery({
    queryKey: ["live-databases", profile],
    queryFn: async () => {
      const { api } = await import("../../lib/api");
      return api.liveDatabases({ profile });
    },
    enabled: open && !!profile && !usesCatalogs,
  });
  const catalogsQ = useQuery({
    queryKey: ["live-catalogs", profile],
    queryFn: async () => {
      const { api } = await import("../../lib/api");
      return api.liveCatalogs({ profile });
    },
    enabled: open && !!profile && usesCatalogs,
  });

  useEffect(() => {
    if (!profile) return;
    if (usesCatalogs) {
      const list = catalogsQ.data?.catalogs ?? [];
      const active =
        catalogsQ.data?.active_catalog ?? catalogsQ.data?.active_project ?? "";
      if (active && list.includes(active)) setCatalog(active);
      else if (list.length === 1) setCatalog(list[0]);
    } else {
      const list = dbsQ.data?.databases ?? [];
      const active = dbsQ.data?.active_database ?? "";
      if (active && list.includes(active)) setDatabase(active);
      else if (list.length === 1) setDatabase(list[0]);
    }
  }, [profile, usesCatalogs, dbsQ.data, catalogsQ.data]);

  const schemasQ = useQuery({
    queryKey: ["live-schemas", profile, database, catalog],
    queryFn: () => fetchSchemas({ profile, database, catalog }),
    enabled: open && !!profile && (!!database || !!catalog),
  });

  const assetsQ = useQuery({
    queryKey: ["live-assets", profile, database, catalog, schema],
    queryFn: () => fetchAssets({ profile, database, catalog, schema }),
    enabled: open && !!profile && !!schema,
  });

  const matches = useMemo(() => {
    const q = query.trim().toLowerCase();
    const list = assetsQ.data ?? [];
    if (!q) return list;
    return list.filter((a) => a.name.toLowerCase().includes(q));
  }, [assetsQ.data, query]);

  async function handlePick(asset: { name: string }) {
    if (loadingPick) return;
    setLoadingPick(true);
    try {
      const cols = await fetchTableColumns({
        profile,
        database: catalog || database,
        schema,
        table: asset.name,
      });
      onPick({
        profile,
        database: catalog || database,
        schema,
        table: asset.name,
        columns: cols,
      });
      onClose();
    } finally {
      setLoadingPick(false);
    }
  }

  return (
    <Modal
      open={open}
      onClose={onClose}
      size="md"
      title={
        <span className="inline-flex items-center gap-2">
          <Plus size={14} /> Add table to canvas
        </span>
      }
      description="Pick any cached table from any DB profile. The canvas can host nodes from multiple profiles simultaneously."
    >
      <div className="space-y-3 text-sm">
        <div className="grid grid-cols-2 gap-2">
          <label className="block space-y-1">
            <span className="text-[10px] uppercase tracking-wide text-fg-muted">
              Profile
            </span>
            <select
              value={profile}
              onChange={(e) => {
                setProfile(e.target.value);
                setDatabase("");
                setCatalog("");
                setSchema("");
              }}
              className="block w-full rounded-md border border-surface-border bg-surface-raised px-2 py-1.5 text-sm"
            >
              <option value="">— pick profile —</option>
              {(profilesQ.data ?? []).map((p) => (
                <option key={p.name} value={p.name}>
                  {p.name}
                </option>
              ))}
            </select>
          </label>

          {profile && usesCatalogs ? (
            <label className="block space-y-1">
              <span className="text-[10px] uppercase tracking-wide text-fg-muted">
                Catalog
              </span>
              <select
                value={catalog}
                onChange={(e) => {
                  setCatalog(e.target.value);
                  setSchema("");
                }}
                className="block w-full rounded-md border border-surface-border bg-surface-raised px-2 py-1.5 text-sm"
              >
                <option value="">— pick catalog —</option>
                {(catalogsQ.data?.catalogs ?? []).map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            </label>
          ) : (
            profile && (
              <label className="block space-y-1">
                <span className="text-[10px] uppercase tracking-wide text-fg-muted">
                  Database
                </span>
                <select
                  value={database}
                  onChange={(e) => {
                    setDatabase(e.target.value);
                    setSchema("");
                  }}
                  className="block w-full rounded-md border border-surface-border bg-surface-raised px-2 py-1.5 text-sm"
                >
                  <option value="">— pick database —</option>
                  {(dbsQ.data?.databases ?? []).map((d) => (
                    <option key={d} value={d}>
                      {d}
                    </option>
                  ))}
                </select>
              </label>
            )
          )}
        </div>

        {profile && (database || catalog) && (
          <label className="block space-y-1">
            <span className="text-[10px] uppercase tracking-wide text-fg-muted">
              Schema
            </span>
            <select
              value={schema}
              onChange={(e) => setSchema(e.target.value)}
              className="block w-full rounded-md border border-surface-border bg-surface-raised px-2 py-1.5 text-sm"
            >
              <option value="">— pick schema —</option>
              {(schemasQ.data ?? []).map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </label>
        )}

        {schema && (
          <>
            <label className="relative block">
              <Search
                size={12}
                className="absolute left-2 top-2 text-fg-muted"
              />
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Filter tables…"
                className="block w-full rounded-md border border-surface-border bg-surface-raised pl-7 pr-2 py-1.5 text-sm"
              />
            </label>
            <div className="max-h-72 overflow-y-auto rounded-md border border-surface-border bg-surface">
              {assetsQ.isLoading ? (
                <div className="p-3 text-[11px] text-fg-muted">Loading tables…</div>
              ) : matches.length === 0 ? (
                <div className="p-3 text-[11px] text-fg-muted">No matching tables.</div>
              ) : (
                <ul className="divide-y divide-surface-border text-[12px]">
                  {matches.map((a) => (
                    <li
                      key={a.name}
                      className="flex items-center justify-between px-3 py-1.5 hover:bg-surface-raised"
                    >
                      <span className="font-mono">
                        {schema}.{a.name}
                        {a.kind === "view" && (
                          <span className="ml-2 rounded bg-surface-raised px-1 text-[9px] uppercase tracking-wide text-fg-muted">
                            view
                          </span>
                        )}
                      </span>
                      <Button
                        variant="secondary"
                        size="sm"
                        disabled={loadingPick}
                        onClick={() => handlePick(a)}
                      >
                        + Add
                      </Button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </>
        )}
      </div>
    </Modal>
  );
}
