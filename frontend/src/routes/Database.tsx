import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { Database as DatabaseIcon, FolderTree, Sparkles } from "lucide-react";

import { api } from "../lib/api";
import { cn } from "../lib/cn";
import { useScope, scopePath } from "../lib/scope";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import GenerateScopeDialog from "../components/GenerateScopeDialog";
import { Button, InlineEditText, Skeleton, useToast } from "../components/ui";

/**
 * Database / catalog landing page. Scope (profile + database|catalog)
 * comes from the URL — `useScope()` resolves it. Drives the same
 * comment-edit surface as Schema and Table; the write path is
 * `PUT /api/comments/database?profile=…&database=…`.
 */
export default function Database() {
  const { scope } = useScope();
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const [draftDescription, setDraftDescription] = useState("");
  const [confirmGenerate, setConfirmGenerate] = useState(false);

  // Hooks must run unconditionally; we early-return below on missing
  // scope. The queries get `enabled: !!scope` so they no-op until the
  // URL is well-formed.
  const schemas = useQuery({
    queryKey: [
      "live-schemas",
      scope?.profile ?? "",
      scope?.database ?? "",
      scope?.catalog ?? "",
    ],
    queryFn: () => api.liveSchemas(scope!),
    enabled: !!scope,
    retry: false,
  });

  useEffect(() => {
    setDraftDescription("");
  }, [scope?.profile, scope?.database, scope?.catalog]);

  async function saveDescription(next: string) {
    if (!scope) return;
    await api.setDatabaseComment(scope, next);
    setDraftDescription(next);
    qc.invalidateQueries({ queryKey: ["context"] });
    toast.push({
      title:
        scope.kind === "catalog"
          ? "Catalog description saved"
          : "Database description saved",
      tone: "success",
      duration: 2000,
    });
  }

  const generateDbOnly = useMutation({
    mutationFn: () => api.generateDatabaseDescription(scope!),
    onSuccess: (result) => {
      setDraftDescription(result.description);
      const altCount = result.alternatives_count ?? 1;
      toast.push({
        title: result.run_id
          ? `Queued for review (Run #${result.run_id})`
          : "Description queued for review",
        description:
          altCount > 1
            ? `${altCount} alternatives generated (${result.verbosity}). Pick one and approve from the Pending page.`
            : "Approve from the Pending page to write it to the live database.",
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

  const generateSchemaOne = useMutation({
    mutationFn: (schemaName: string) =>
      api.generateSchemaDescription(scope!, schemaName),
    onSuccess: (result, schemaName) => {
      qc.invalidateQueries({ queryKey: ["live-schemas"] });
      const altCount = result.alternatives_count ?? 1;
      toast.push({
        title: result.run_id
          ? `Schema queued for review (Run #${result.run_id})`
          : "Schema description queued for review",
        description:
          altCount > 1
            ? `${schemaName} — ${altCount} alternatives (${result.verbosity}); pick one in /pending.`
            : `${schemaName} — approve from /pending.`,
        tone: "success",
        duration: 3000,
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
      const reqScope: Record<string, string[]> = {};
      for (const s of list) reqScope[s] = [];
      return api.submitRun({
        scope: reqScope,
        apply: false,
        missing_only: false,
        db_profile: scope?.profile,
        database: scope?.database,
        catalog: scope?.catalog,
      });
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

  if (!scope) {
    return (
      <EmptyState
        icon={DatabaseIcon}
        title="Pick a database from the sidebar"
        description="Expand a DB profile in the left tree, then click a database or catalog."
      />
    );
  }

  const headingLabel = scope.database ?? scope.catalog ?? scope.profile;
  const subtitle = scope.kind === "catalog" ? "Catalog" : "Database";

  return (
    <>
      <PageHeader
        title={headingLabel}
        breadcrumbs={[
          { label: "Browse", to: "/" },
          { label: scope.profile },
          { label: headingLabel },
        ]}
        description={
          <>
            <span className="block text-[11px] uppercase tracking-wider text-ink-dim">
              {subtitle} · profile {scope.profile}
            </span>
            <div className="mt-1.5">
              <InlineEditText
                value={draftDescription}
                onSave={saveDescription}
                multiline
                italicEmpty
                emptyLabel={`No ${scope.kind} description yet — click to add one or use Generate.`}
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
          ) : schemas.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(schemas.error as Error).message}
            </div>
          ) : schemas.data?.items?.length ? (
            <ul className="divide-y divide-border">
              {schemas.data.items.map((item) => {
                const s = item.name;
                const isGenerating =
                  generateSchemaOne.isPending && generateSchemaOne.variables === s;
                const comment = item.comment?.trim() ?? "";
                return (
                  <li
                    key={s}
                    className="group flex items-start text-sm transition-colors duration-fast hover:bg-surface-subtle/50"
                  >
                    <Link
                      to={scopePath(scope, s)}
                      className="flex flex-1 items-start gap-3 px-5 py-2.5"
                    >
                      <FolderTree size={14} className="mt-0.5 text-accent" />
                      <span className="min-w-0 flex-1">
                        <span className="block font-mono text-ink">{s}</span>
                        <span
                          className={cn(
                            "mt-0.5 line-clamp-2 block text-xs",
                            comment ? "text-ink-muted" : "italic text-ink-dim/70",
                          )}
                          title={comment || undefined}
                        >
                          {comment || "no description yet"}
                        </span>
                      </span>
                    </Link>
                    <div className="px-3 py-2">
                      <Button
                        variant="ghost"
                        size="sm"
                        leadingIcon={<Sparkles size={11} />}
                        loading={isGenerating}
                        disabled={generateSchemaOne.isPending}
                        onClick={() => generateSchemaOne.mutate(s)}
                        title={`Generate description for ${s}`}
                      >
                        Gen
                      </Button>
                    </div>
                  </li>
                );
              })}
            </ul>
          ) : (
            <div className="px-5 py-5">
              <EmptyState
                icon={DatabaseIcon}
                title={
                  scope.kind === "catalog"
                    ? "Catalog has no schemas"
                    : "Database has no schemas"
                }
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
          scope.kind === "catalog"
            ? "Pick the scope. Single-shot writes only the catalog's own COMMENT. Bulk run walks every schema, table and column under it."
            : "Pick the scope. Single-shot writes only the database's own COMMENT. Bulk run walks every schema, table and column under it."
        }
        singleOption={{
          label: scope.kind === "catalog" ? "Just this catalog" : "Just this database",
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
