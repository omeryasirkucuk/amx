import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Database as DatabaseIcon, FolderTree, Sparkles } from "lucide-react";

import { ApiError, api } from "../lib/api";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import GenerateScopeDialog from "../components/GenerateScopeDialog";
import { Button, InlineEditText, Skeleton, useToast } from "../components/ui";

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
  const navigate = useNavigate();
  const [draftDescription, setDraftDescription] = useState("");
  const [confirmGenerate, setConfirmGenerate] = useState(false);

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

  // Two flavours of "Generate":
  // - Single-shot endpoint writes ONLY the database/catalog's own
  //   COMMENT (no schemas, no tables, one LLM call). Fast.
  // - Bulk run path spawns the full analyze.run worker scoped to
  //   every reachable schema so every table + column under the
  //   database also gets a generated description.
  const generateDbOnly = useMutation({
    mutationFn: () => api.generateDatabaseDescription(),
    onSuccess: (result) => {
      setDraftDescription(result.description);
      qc.invalidateQueries({ queryKey: ["context"] });
      toast.push({
        title: result.run_id
          ? `Queued for review (Run #${result.run_id})`
          : "Description queued for review",
        description: "Approve from the Pending page to write it to the live database.",
        tone: "success",
        duration: 3200,
      });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Generation failed",
        description: e.message,
        tone: "error",
      }),
  });

  const generateBulk = useMutation({
    mutationFn: () => {
      const list = schemas.data?.schemas ?? [];
      // Build a scope that spans every reachable schema; an empty
      // table list under each schema means "every table".
      const scope: Record<string, string[]> = {};
      for (const s of list) scope[s] = [];
      return api.submitRun({ scope, apply: false, missing_only: false });
    },
    onSuccess: (result) => {
      setConfirmGenerate(false);
      toast.push({
        title: "Bulk run queued for review",
        description:
          "Streaming activity for every schema and table; results land on the Pending page.",
        tone: "info",
        duration: 2600,
      });
      navigate(`/runs/new-${result.job_id}`);
    },
    onError: (e: Error) => {
      setConfirmGenerate(false);
      toast.push({
        title: "Could not start generation",
        description: e.message,
        tone: "error",
      });
    },
  });

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
                emptyLabel={`No ${supportsCatalog ? "catalog" : "database"} description yet — click to add one or use Generate.`}
              />
            </div>
          </>
        }
        actions={
          <Button
            variant="primary"
            size="md"
            leadingIcon={<Sparkles size={14} />}
            loading={generateDbOnly.isPending || generateBulk.isPending}
            disabled={!schemas.data && !schemas.isError}
            onClick={() => setConfirmGenerate(true)}
          >
            Generate description
          </Button>
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

      <GenerateScopeDialog
        open={confirmGenerate}
        onClose={() => setConfirmGenerate(false)}
        title={`Generate description for ${headingLabel}`}
        description={
          supportsCatalog
            ? "Pick the scope. Single-shot writes only the catalog's own COMMENT. Bulk run walks every schema, table and column under it."
            : "Pick the scope. Single-shot writes only the database's own COMMENT. Bulk run walks every schema, table and column under it."
        }
        singleOption={{
          label: supportsCatalog ? "Just this catalog" : "Just this database",
          description:
            "One fast LLM call that writes only the top-level COMMENT. Schemas and tables untouched.",
          loading: generateDbOnly.isPending,
          onClick: () => {
            generateDbOnly.mutate(undefined, {
              onSettled: () => setConfirmGenerate(false),
            });
          },
        }}
        bulkOption={{
          label: "Everything (bulk run)",
          description: schemas.data
            ? `Full analyze.run worker over ${schemas.data.schemas.length} schema${schemas.data.schemas.length === 1 ? "" : "s"} — every table and column gets a generated description, recorded in history.`
            : "Full analyze.run worker over every reachable schema and table.",
          loading: generateBulk.isPending,
          disabled: !schemas.data || schemas.data.schemas.length === 0,
          onClick: () => generateBulk.mutate(),
        }}
      />
    </>
  );
}
