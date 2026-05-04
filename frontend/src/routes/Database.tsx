import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { Database as DatabaseIcon, FolderTree } from "lucide-react";

import { ApiError, api } from "../lib/api";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import { InlineEditText, Skeleton, useToast } from "../components/ui";

/**
 * Database / catalog landing page. Drives the same comment-edit
 * surface as Schema and Table — `PUT /api/comments/database` is the
 * write path. The schema list is borrowed from the live-DB
 * inventory.
 */
export default function Database() {
  const params = useParams();
  const profile = params.profile || "active";
  const qc = useQueryClient();
  const toast = useToast();
  const [draftDescription, setDraftDescription] = useState("");

  const ctx = useQuery({
    queryKey: ["context"],
    queryFn: () => api.context(),
  });
  const schemas = useQuery({
    queryKey: ["live-schemas"],
    queryFn: () => api.liveSchemas(),
    retry: false,
  });
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

  const supportsCatalog = catalogs.data?.supports_catalogs ?? false;
  const activeName = supportsCatalog
    ? catalogs.data?.active_catalog
    : databases.data?.active_database;
  const headingLabel = activeName ?? profile;
  const subtitle = supportsCatalog
    ? `Catalog · ${ctx.data?.db_backend ?? "—"}`
    : `Database · ${ctx.data?.db_backend ?? "—"}`;

  async function saveDescription(next: string) {
    await api.setDatabaseComment(next);
    setDraftDescription(next);
    qc.invalidateQueries({ queryKey: ["context"] });
    toast.push({
      title: supportsCatalog ? "Catalog description saved" : "Database description saved",
      tone: "success",
      duration: 2000,
    });
  }

  const needsScope =
    schemas.error instanceof ApiError &&
    schemas.error.status === 412 &&
    (schemas.error.hint === "select-catalog" ||
      schemas.error.hint === "select-database");

  return (
    <>
      <PageHeader
        title={headingLabel}
        breadcrumbs={[{ label: "Browse", to: "/" }, { label: headingLabel }]}
        description={
          <>
            <span className="block text-[11px] uppercase tracking-wider text-ink-dim">
              {subtitle}
            </span>
            <div className="mt-1.5">
              <InlineEditText
                value={draftDescription}
                onSave={saveDescription}
                multiline
                italicEmpty
                emptyLabel={`No ${supportsCatalog ? "catalog" : "database"} description yet — click to add one.`}
              />
            </div>
          </>
        }
      />

      <Card>
        <CardHeader
          title="Schemas"
          description={
            schemas.data
              ? `${schemas.data.schemas.length} schema${schemas.data.schemas.length === 1 ? "" : "s"} reachable.`
              : undefined
          }
        />
        <CardBody className="p-0">
          {schemas.isLoading ? (
            <ul className="divide-y divide-border">
              {Array.from({ length: 4 }).map((_, i) => (
                <li key={i} className="flex items-center gap-3 px-5 py-3">
                  <Skeleton shape="circle" className="h-3.5 w-3.5" />
                  <Skeleton className="h-3 w-1/3" />
                </li>
              ))}
            </ul>
          ) : needsScope ? (
            <div className="px-5 py-6">
              <p className="text-sm font-medium text-warning">
                {schemas.error instanceof ApiError && schemas.error.hint === "select-catalog"
                  ? "No catalog selected."
                  : "No database selected."}
              </p>
              <p className="mt-1 text-xs text-ink-muted">
                Pick one from the top-bar pill so the schema inventory loads.
              </p>
            </div>
          ) : schemas.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(schemas.error as Error).message}
            </div>
          ) : schemas.data?.schemas?.length ? (
            <ul className="divide-y divide-border">
              {schemas.data.schemas.map((s) => (
                <li key={s}>
                  <Link
                    to={`/db/${profile}/${s}`}
                    className="flex items-center gap-3 px-5 py-2.5 text-sm transition-colors duration-fast hover:bg-surface-subtle/50"
                  >
                    <FolderTree size={14} className="text-accent" />
                    <span className="font-mono text-ink">{s}</span>
                  </Link>
                </li>
              ))}
            </ul>
          ) : (
            <div className="px-5 py-5">
              <EmptyState
                icon={DatabaseIcon}
                title={supportsCatalog ? "Catalog has no schemas" : "Database has no schemas"}
                description="No reachable schemas yet."
                compact
              />
            </div>
          )}
        </CardBody>
      </Card>
    </>
  );
}
