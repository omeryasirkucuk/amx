import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import {
  AlignLeft,
  Columns,
  Database,
  Hash,
  RefreshCw,
  Sparkles,
  Trash2,
  Workflow,
} from "lucide-react";

import { api, assetsForTable, lineageArtifactsForTable } from "../lib/api";
import type { LineageArtifact, LinkedAssetRow } from "../lib/api";
import { useScope, scopePath } from "../lib/scope";
import PageHeader from "../components/PageHeader";
import { Card, CardBody, CardHeader } from "../components/Card";
import EmptyState from "../components/EmptyState";
import GenerateScopeDialog from "../components/GenerateScopeDialog";
import Modal from "../components/Modal";
import StatusPill from "../components/StatusPill";
import { useUi } from "../lib/store";
import { Button, InlineEditText, Skeleton, useToast } from "../components/ui";

export default function Table() {
  const { scope } = useScope();
  const schema = scope?.schema ?? "";
  const table = scope?.table ?? "";
  const remember = useUi((s) => s.rememberOpenedTable);
  const rememberScope = useUi((s) => s.rememberOpenedScope);
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const [confirmGenerate, setConfirmGenerate] = useState(false);
  // PR E: per-run doc profile override for the bulk path.
  const [runDocProfiles, setRunDocProfiles] = useState<string[] | null>(null);
  // PR δ: parallel code-profile override forwarded to api.submitRun.
  const [runCodeProfiles, setRunCodeProfiles] = useState<string[] | null>(null);
  // Reachability pre-flight: when /api/runs/preflight reports tables the
  // live DB can't read, surface a dialog so the user explicitly opts into
  // the catalog-cache substitution instead of the bulk worker crashing on
  // ``NoSuchTableError`` (the user-confirmed "ask, don't silently fall
  // back" contract).
  const [reachabilityBlocked, setReachabilityBlocked] = useState<Array<{
    schema: string;
    table: string;
    reason: string;
  }> | null>(null);
  // Clear-reviews dialog: which of the three review categories to wipe
  // for this table. All default on so the common "reset this table's
  // reviews" case is one confirm away.
  const [clearReviewsOpen, setClearReviewsOpen] = useState(false);
  const [clearOpts, setClearOpts] = useState({
    pending: true,
    review_state: true,
    audit: true,
  });

  useEffect(() => {
    if (scope && schema && table) {
      remember(schema, table);
      rememberScope({
        profile: scope.profile,
        database: scope.database,
        catalog: scope.catalog,
        schema,
        table,
      });
    }
  }, [scope, schema, table, remember, rememberScope]);

  const snapshot = useQuery({
    queryKey: [
      "live-snapshot",
      scope?.profile ?? "",
      scope?.database ?? "",
      scope?.catalog ?? "",
      schema,
      table,
    ],
    queryFn: () => api.liveSnapshot(scope!, schema, table),
    enabled: !!scope && !!schema && !!table,
  });
  const columns = useQuery({
    queryKey: [
      "live-columns",
      scope?.profile ?? "",
      scope?.database ?? "",
      scope?.catalog ?? "",
      schema,
      table,
    ],
    queryFn: () => api.liveColumns(scope!, schema, table),
    enabled: !!scope && !!schema && !!table,
  });

  const totalCols = snapshot.data?.columns?.length ?? columns.data?.count ?? 0;
  // Synced row count from the catalog. ``null``/undefined means the
  // count was never captured by /search sync — show "—" rather than a
  // misleading "0".
  const rowCount = snapshot.data?.row_count ?? null;
  const commented =
    snapshot.data?.columns?.filter((c) => (c.comment || "").trim().length > 0).length ?? 0;
  const tableComment = snapshot.data?.table_comment ?? "";
  const pendingTableDesc = snapshot.data?.pending_description ?? null;
  const pendingColumnDescs = snapshot.data?.pending_column_descriptions ?? {};
  const pendingRunId = snapshot.data?.pending_run_id ?? null;

  async function saveTableComment(next: string) {
    if (!scope) return;
    await api.setTableComment(scope, schema, table, next);
    qc.invalidateQueries({ queryKey: ["live-snapshot"] });
    toast.push({
      title: "Description saved",
      tone: "success",
      duration: 2000,
    });
  }

  async function saveColumnComment(column: string, next: string) {
    if (!scope) return;
    await api.setColumnComment(scope, schema, table, column, next);
    qc.invalidateQueries({ queryKey: ["live-snapshot"] });
    toast.push({
      title: `Comment saved`,
      description: `${schema}.${table}.${column}`,
      tone: "success",
      duration: 2000,
    });
  }

  const generateTableOnly = useMutation({
    mutationFn: () => api.generateTableDescription(scope!, schema, table),
    onSuccess: (result) => {
      qc.invalidateQueries({ queryKey: ["live-snapshot"] });
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

  const clearReviews = useMutation({
    mutationFn: () => api.clearTableReviews(schema, table, clearOpts),
    onSuccess: (res) => {
      setClearReviewsOpen(false);
      // Refresh the snapshot (pending pills), the pending queue, and the
      // audit timeline so the cleared categories drop from the UI.
      qc.invalidateQueries({ queryKey: ["live-snapshot"] });
      qc.invalidateQueries({ queryKey: ["pending"] });
      qc.invalidateQueries({ queryKey: ["apply-events"] });
      const { pending, review_state, audit } = res.counts;
      toast.push({
        title: "Reviews cleared",
        description: `${pending} pending · ${review_state} review-state · ${audit} audit row(s) removed. Live-database comments untouched.`,
        tone: "success",
        duration: 3600,
      });
    },
    onError: (e: Error) =>
      toast.push({ title: "Clear failed", description: e.message, tone: "error" }),
  });

  const generateColumnOne = useMutation({
    mutationFn: (column: string) => api.generateColumnDescription(scope!, schema, table, column),
    onSuccess: (result, column) => {
      qc.invalidateQueries({ queryKey: ["live-snapshot"] });
      const altCount = result.alternatives_count ?? 1;
      toast.push({
        title: result.run_id
          ? `Column queued for review (Run #${result.run_id})`
          : "Column description queued for review",
        description:
          altCount > 1
            ? `${schema}.${table}.${column} — ${altCount} alternatives (${result.verbosity}); pick one in /pending.`
            : `${schema}.${table}.${column} — approve from /pending.`,
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

  // Two-phase bulk submit: pre-flight first to detect any tables the
  // live DB can't reflect, then either submit directly (everything
  // reachable) or open the reachability dialog so the user picks
  // "Use cached schema" / "Cancel". Returning ``null`` from
  // ``mutationFn`` signals "dialog opened, navigation deferred" so
  // ``onSuccess`` doesn't fire the toast prematurely.
  const generate = useMutation({
    mutationFn: async (): Promise<{ job_id: string; status: string } | null> => {
      const pre = await api.preflightRun({
        scope: { [schema]: [table] },
        db_profile: scope?.profile,
        database: scope?.database,
        catalog: scope?.catalog,
      });
      if (pre.blocked_assets.length > 0) {
        setReachabilityBlocked(pre.blocked_assets);
        return null;
      }
      return api.submitRun({
        scope: { [schema]: [table] },
        apply: false,
        missing_only: false,
        db_profile: scope?.profile,
        database: scope?.database,
        catalog: scope?.catalog,
        doc_profiles: runDocProfiles ?? undefined,
        code_profiles: runCodeProfiles ?? undefined,
      });
    },
    onSuccess: (result) => {
      if (result === null) return; // dialog open, wait for user
      setConfirmGenerate(false);
      toast.push({
        title: "Bulk run queued for review",
        description: `Streaming every column of ${schema}.${table}; results land on the Pending page.`,
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

  // Second-phase submit after the user accepts the reachability prompt.
  // ``cache_override_assets`` is the list of ``"schema.table"`` keys we
  // ask the bulk worker to substitute the catalog cache for.
  const generateWithOverride = useMutation({
    mutationFn: (overrides: string[]) =>
      api.submitRun({
        scope: { [schema]: [table] },
        apply: false,
        missing_only: false,
        db_profile: scope?.profile,
        database: scope?.database,
        catalog: scope?.catalog,
        doc_profiles: runDocProfiles ?? undefined,
        code_profiles: runCodeProfiles ?? undefined,
        cache_override_assets: overrides,
      }),
    onSuccess: (result) => {
      setReachabilityBlocked(null);
      setConfirmGenerate(false);
      toast.push({
        title: "Bulk run queued for review",
        description: `Streaming every column of ${schema}.${table} using cached schema; results land on the Pending page.`,
        tone: "info",
        duration: 2800,
      });
      navigate(`/runs/new-${result.job_id}`);
    },
    onError: (e: Error) => {
      setReachabilityBlocked(null);
      setConfirmGenerate(false);
      toast.push({
        title: "Could not start generation",
        description: e.message,
        tone: "error",
      });
    },
  });

  // "Open lineage" button on the table page. Routes by how many
  // saved canvases already contain this table:
  //   * 0   → fresh /lineage canvas seeded with the table
  //   * 1   → straight to that artifact's canvas
  //   * 2+  → small picker so the user chooses which canvas to open
  const [pickerArtifacts, setPickerArtifacts] = useState<LineageArtifact[] | null>(null);
  const openLineage = useMutation({
    mutationFn: async () => {
      if (!scope || !table) {
        throw new Error("Pick a table from the sidebar first.");
      }
      const database = scope.database || scope.catalog || "";
      const resp = await lineageArtifactsForTable({
        profile: scope.profile,
        database,
        schema,
        table,
      });
      return { artifacts: resp.artifacts, database };
    },
    onSuccess: (payload) => {
      if (!scope || !table) return;
      const { artifacts, database } = payload;
      if (artifacts.length === 0) {
        // Seed a blank canvas with the picked table so the user
        // doesn't have to re-find it in the picker.
        const seed = [
          scope.profile,
          database,
          schema,
          table,
          "", // backend left empty — logo can be picked manually
        ]
          .map(encodeURIComponent)
          .join("|");
        navigate(`/lineage?seed=${encodeURIComponent(seed)}`);
        return;
      }
      if (artifacts.length === 1) {
        navigate(`/lineage?artifact=${artifacts[0].id}`);
        return;
      }
      setPickerArtifacts(artifacts);
    },
    onError: (e: Error) => {
      toast.push({
        title: "Could not open lineage",
        description: e.message,
        tone: "error",
      });
    },
  });

  // Per-table deep sync: profile THIS table (columns + exact row count)
  // into the catalog without re-profiling the whole profile. On success
  // the snapshot query is invalidated so the Rows card refreshes.
  const deepSync = useMutation({
    mutationFn: () => api.deepSyncTable(scope!, schema, table),
    onSuccess: (res) => {
      qc.invalidateQueries({ queryKey: ["live-snapshot"] });
      qc.invalidateQueries({ queryKey: ["live-columns"] });
      toast.push({
        title: "Deep sync complete",
        description: `${table}: ${res.row_count.toLocaleString()} rows, ${res.column_count} columns.`,
        tone: "success",
      });
    },
    onError: (e: Error) => {
      toast.push({
        title: "Deep sync failed",
        description: e.message,
        tone: "error",
      });
    },
  });

  if (!scope || !schema || !table) {
    return (
      <EmptyState
        icon={Database}
        title="Pick a table from the sidebar"
        description="Expand a schema to see its tables."
      />
    );
  }

  return (
    <>
      <PageHeader
        title={table}
        breadcrumbs={[
          { label: "Browse", to: "/" },
          {
            label: scope.profile,
            to: `/${scope.kind === "catalog" ? "cat" : "db"}/${encodeURIComponent(scope.profile)}`,
          },
          {
            label: scope.database ?? scope.catalog ?? "",
            to: scopePath(scope),
          },
          { label: schema, to: scopePath(scope, schema) },
          { label: table },
        ]}
        description={
          <div className="space-y-2">
            <InlineEditText
              value={tableComment}
              onSave={saveTableComment}
              multiline
              italicEmpty
              emptyLabel="No table description yet — click to add one or use Generate."
            />
            {pendingTableDesc ? (
              <PendingDescriptionBlock
                text={pendingTableDesc}
                runId={pendingRunId}
                label="Pending review"
              />
            ) : null}
          </div>
        }
        actions={
          <div className="flex items-center gap-2">
            <Button
              variant="secondary"
              size="md"
              leadingIcon={<RefreshCw size={14} />}
              loading={deepSync.isPending}
              onClick={() => deepSync.mutate()}
              title="Profile this table from the database — fetches columns and the exact row count, then caches them"
            >
              Deep sync
            </Button>
            <Button
              variant="secondary"
              size="md"
              leadingIcon={<Workflow size={14} />}
              loading={openLineage.isPending}
              onClick={() => openLineage.mutate()}
              title="Render this table's lineage and open it in the canvas"
            >
              Open lineage
            </Button>
            <Button
              variant="secondary"
              size="md"
              leadingIcon={<Trash2 size={14} />}
              onClick={() => setClearReviewsOpen(true)}
              title="Clear this table's reviews — pending suggestions, review-state decisions, and applied-description audit"
            >
              Clear reviews
            </Button>
            <Button
              variant="primary"
              size="md"
              leadingIcon={<Sparkles size={14} />}
              loading={generateTableOnly.isPending || generate.isPending}
              onClick={() => setConfirmGenerate(true)}
            >
              Generate description
            </Button>
          </div>
        }
      />

      <div className="grid gap-4 sm:grid-cols-2 md:grid-cols-4">
        <SummaryCard label="Columns" value={String(totalCols)} icon={Columns} />
        <SummaryCard
          label="Rows"
          value={rowCount != null ? rowCount.toLocaleString() : "—"}
          icon={Hash}
        />
        <SummaryCard
          label="With comments"
          value={totalCols ? `${commented}/${totalCols}` : "—"}
          icon={AlignLeft}
        />
        <SummaryCard
          label="Coverage"
          value={totalCols ? `${Math.round((commented / totalCols) * 100)}%` : "—"}
          icon={AlignLeft}
        />
      </div>

      <Card className="mt-6">
        <CardHeader title="Columns" />
        <CardBody className="p-0 overflow-x-auto">
          {columns.isLoading ? (
            <div className="px-5 py-4 space-y-2">
              {Array.from({ length: 6 }).map((_, i) => (
                <div key={i} className="flex items-center gap-3">
                  <Skeleton className="h-3 w-1/6" />
                  <Skeleton className="h-3 w-1/12" />
                  <Skeleton className="h-3 w-1/12" />
                  <Skeleton className="h-3 flex-1" />
                </div>
              ))}
            </div>
          ) : columns.error ? (
            <div className="px-5 py-6 text-sm text-critical">
              {(columns.error as Error).message}
            </div>
          ) : columns.data && columns.data.columns.length > 0 ? (
            <table className="w-full text-sm">
              <thead className="border-b border-border bg-surface-subtle/60 text-[10.5px] uppercase tracking-wider text-ink-dim">
                <tr>
                  <th className="px-5 py-2 text-left font-semibold">Name</th>
                  <th className="px-5 py-2 text-left font-semibold">Type</th>
                  <th className="px-5 py-2 text-left font-semibold">Nullable</th>
                  <th className="px-5 py-2 text-left font-semibold">Comment</th>
                  <th className="px-2 py-2 text-right font-semibold w-20">Generate</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {columns.data.columns.map((col) => {
                  const snap = snapshot.data?.columns.find((c) => c.name === col.name);
                  const isGenerating =
                    generateColumnOne.isPending && generateColumnOne.variables === col.name;
                  return (
                    <tr key={col.name} className="align-top hover:bg-surface-subtle/40">
                      <td className="px-5 py-2 font-mono text-xs text-ink">{col.name}</td>
                      <td className="px-5 py-2 font-mono text-xs text-ink-muted">{col.dtype}</td>
                      <td className="px-5 py-2">
                        <StatusPill tone={col.nullable ? "neutral" : "accent"}>
                          {col.nullable ? "nullable" : "required"}
                        </StatusPill>
                      </td>
                      <td className="px-5 py-2 text-ink-muted">
                        <div className="space-y-1.5">
                          <InlineEditText
                            value={snap?.comment ?? ""}
                            onSave={(next) => saveColumnComment(col.name, next)}
                            multiline
                            italicEmpty
                            emptyLabel="no comment — click to add"
                          />
                          {pendingColumnDescs[col.name] ? (
                            <PendingDescriptionBlock
                              text={pendingColumnDescs[col.name]}
                              runId={pendingRunId}
                              label="Pending"
                              compact
                            />
                          ) : null}
                        </div>
                      </td>
                      <td className="px-2 py-2 text-right">
                        <Button
                          variant="ghost"
                          size="sm"
                          leadingIcon={<Sparkles size={11} />}
                          loading={isGenerating}
                          disabled={generateColumnOne.isPending}
                          onClick={() => generateColumnOne.mutate(col.name)}
                          title={`Generate description for ${col.name}`}
                        >
                          Gen
                        </Button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          ) : (
            <div className="px-5 py-6 text-sm text-ink-dim">
              This table has no introspectable columns.
            </div>
          )}
        </CardBody>
      </Card>

      <LinkedAssetsCard
        profile={scope.profile}
        database={scope.database || scope.catalog || ""}
        schema={schema}
        table={table}
      />

      <div className="mt-4">
        <Link to={scopePath(scope, schema)} className="text-xs text-ink-dim hover:text-ink">
          ← Back to schema
        </Link>
      </div>

      <GenerateScopeDialog
        open={confirmGenerate}
        onClose={() => setConfirmGenerate(false)}
        docProfiles={runDocProfiles}
        onDocProfilesChange={setRunDocProfiles}
        codeProfiles={runCodeProfiles}
        onCodeProfilesChange={setRunCodeProfiles}
        title={`Generate description for ${schema}.${table}`}
        description="Pick the scope. Just-the-table writes only the table's own COMMENT (one fast LLM call). Bulk run also generates a description for every column."
        singleOption={{
          label: "Just this table",
          description: "Writes only the table's COMMENT. One LLM call, columns untouched.",
          // Fire-and-forget: close the dialog immediately and let the
          // mutation finish in the background. The pending pill on the
          // Table page surfaces the result the moment the LLM call
          // returns, so trapping the user in a "...running" modal until
          // settle was pure friction.
          onClick: () => {
            setConfirmGenerate(false);
            generateTableOnly.mutate();
          },
        }}
        bulkOption={{
          label: "Whole table — every column too (bulk run)",
          description:
            "Spawns analyze.run for the full table; each column gets its own generated description, recorded in history.",
          // Bulk path also closes the dialog upfront. ``generate`` runs
          // pre-flight first; if blocked tables are found, the
          // reachability modal below opens — independent of whether the
          // GenerateScopeDialog is still on screen.
          onClick: () => {
            setConfirmGenerate(false);
            generate.mutate();
          },
        }}
      />
      <Modal
        open={clearReviewsOpen}
        onClose={() => setClearReviewsOpen(false)}
        size="md"
        title={
          <span>
            Clear reviews for {schema}.{table}
          </span>
        }
        description="Choose what to remove. This resets AMX's review data for the table — it never edits the descriptions already written to the live database."
      >
        <div className="space-y-3 text-[13px] text-ink">
          <label className="flex items-start gap-2">
            <input
              type="checkbox"
              className="mt-0.5 h-3.5 w-3.5 cursor-pointer accent-accent"
              checked={clearOpts.pending}
              onChange={(e) => setClearOpts((o) => ({ ...o, pending: e.target.checked }))}
            />
            <span>
              <span className="font-medium">Pending suggestions</span>
              <span className="block text-[12px] text-ink-muted">
                Unapplied generated descriptions queued for this table.
              </span>
            </span>
          </label>
          <label className="flex items-start gap-2">
            <input
              type="checkbox"
              className="mt-0.5 h-3.5 w-3.5 cursor-pointer accent-accent"
              checked={clearOpts.review_state}
              onChange={(e) => setClearOpts((o) => ({ ...o, review_state: e.target.checked }))}
            />
            <span>
              <span className="font-medium">Review-state decisions</span>
              <span className="block text-[12px] text-ink-muted">
                Accept / skip / custom choices recorded on this table's run results — reset to
                unreviewed (the generated alternatives stay).
              </span>
            </span>
          </label>
          <label className="flex items-start gap-2">
            <input
              type="checkbox"
              className="mt-0.5 h-3.5 w-3.5 cursor-pointer accent-accent"
              checked={clearOpts.audit}
              onChange={(e) => setClearOpts((o) => ({ ...o, audit: e.target.checked }))}
            />
            <span>
              <span className="font-medium">Applied-description audit</span>
              <span className="block text-[12px] text-ink-muted">
                AMX's record of descriptions previously written for this table. Clears the history
                only — the live-database comments are untouched.
              </span>
            </span>
          </label>
          <div className="flex flex-col gap-2 sm:flex-row sm:justify-end">
            <Button
              variant="ghost"
              size="md"
              onClick={() => setClearReviewsOpen(false)}
              disabled={clearReviews.isPending}
            >
              Cancel
            </Button>
            <Button
              variant="danger"
              size="md"
              loading={clearReviews.isPending}
              disabled={!clearOpts.pending && !clearOpts.review_state && !clearOpts.audit}
              onClick={() => clearReviews.mutate()}
            >
              Clear reviews
            </Button>
          </div>
        </div>
      </Modal>
      <Modal
        open={reachabilityBlocked !== null}
        onClose={() => setReachabilityBlocked(null)}
        size="md"
        title={<span>Live database can't read this table</span>}
        description={
          reachabilityBlocked && reachabilityBlocked.length === 1
            ? `AMX couldn't reach ${reachabilityBlocked[0].schema}.${reachabilityBlocked[0].table} on the live database. The catalog still has its column list from the last /search sync — use that instead?`
            : `AMX couldn't reach ${reachabilityBlocked?.length ?? 0} table(s) on the live database. The catalog still has their column lists from the last /search sync — use that instead?`
        }
      >
        <div className="space-y-3 text-[13px] text-ink">
          {reachabilityBlocked && reachabilityBlocked.length > 1 ? (
            <ul className="max-h-[180px] divide-y divide-surface-border overflow-y-auto rounded-md border border-surface-border text-[12px] font-mono">
              {reachabilityBlocked.slice(0, 8).map((b) => (
                <li key={`${b.schema}.${b.table}`} className="px-3 py-1.5 text-ink-muted">
                  {b.schema}.{b.table}
                </li>
              ))}
              {reachabilityBlocked.length > 8 ? (
                <li className="px-3 py-1.5 text-[11px] italic text-ink-dim">
                  +{reachabilityBlocked.length - 8} more
                </li>
              ) : null}
            </ul>
          ) : null}
          <p className="text-[12px] text-ink-muted">
            Cached schema gives the LLM column names, dtypes, and existing comments — no live
            samples, PK/FK, or row counts. Approve the descriptions from the Pending page as usual.
          </p>
          <div className="flex flex-col gap-2 sm:flex-row sm:justify-end">
            <Button
              variant="ghost"
              size="md"
              onClick={() => setReachabilityBlocked(null)}
              disabled={generateWithOverride.isPending}
            >
              Cancel
            </Button>
            <Button
              variant="primary"
              size="md"
              loading={generateWithOverride.isPending}
              onClick={() => {
                if (!reachabilityBlocked) return;
                const overrides = reachabilityBlocked.map((b) => `${b.schema}.${b.table}`);
                generateWithOverride.mutate(overrides);
              }}
            >
              Use cached schema
            </Button>
          </div>
        </div>
      </Modal>
      <Modal
        open={pickerArtifacts !== null}
        onClose={() => setPickerArtifacts(null)}
        size="md"
        title={<span>Open lineage</span>}
        description={`${pickerArtifacts?.length ?? 0} saved canvases include this table. Pick one to open.`}
      >
        <ul className="max-h-[420px] divide-y divide-surface-border overflow-y-auto rounded-md border border-surface-border">
          {(pickerArtifacts ?? []).map((a) => (
            <li key={a.id}>
              <button
                type="button"
                onClick={() => {
                  setPickerArtifacts(null);
                  navigate(`/lineage?artifact=${a.id}`);
                }}
                className="block w-full px-3 py-2 text-left transition hover:bg-surface-raised"
              >
                <div className="text-[13px] font-medium text-ink">{a.name}</div>
                <div className="mt-0.5 text-[11px] text-fg-muted">
                  {a.node_count} nodes · {a.edge_count} edges · {a.db_profile || "no profile"}
                </div>
              </button>
            </li>
          ))}
        </ul>
      </Modal>
    </>
  );
}

function SummaryCard({
  label,
  value,
  icon: Icon,
}: {
  label: string;
  value: string;
  icon: typeof Columns;
}) {
  return (
    <div className="rounded-xl border border-border bg-surface-raised p-4 shadow-xs">
      <div className="flex items-center justify-between text-[10.5px] uppercase tracking-wider text-ink-dim">
        <span>{label}</span>
        <Icon size={14} />
      </div>
      <div className="mt-1.5 font-mono text-xl text-ink tabular-nums">{value}</div>
    </div>
  );
}

/** Visible "this generation is awaiting approval" block.
 *
 * Surfaces a description that came out of Browse → AI Generate but
 * has not been written to the live database yet. The text persists
 * across page refreshes because the snapshot endpoint merges it from
 * the pending queue. A "Review in Pending" link deep-links the user
 * into the existing /pending workflow so the explicit
 * review-before-apply contract stays in place.
 */
/** "What touched this table?" card.
 *
 * Fed by ``GET /api/assets/by-table``. Groups results into Reads
 * and Writes with a small filter chip-row so the user can flip
 * between them without paginating; legacy rows that don't carry a
 * direction surface in both groups tagged ``unknown``. Empty state
 * tells the user how to ingest assets so the panel populates.
 */
function LinkedAssetsCard({
  profile,
  database,
  schema,
  table,
}: {
  profile: string;
  database: string;
  schema: string;
  table: string;
}) {
  const [direction, setDirection] = useState<"all" | "read" | "write">("all");
  const query = useQuery({
    queryKey: ["assets-for-table", profile, database, schema, table],
    queryFn: () => assetsForTable({ profile, schema, table, database, sinceDays: 90 }),
    enabled: !!profile && !!schema && !!table,
  });

  const counts = query.data?.counts ?? {};
  const totalLinked = Object.values(counts).reduce((sum, n) => sum + n, 0);

  const reads = query.data?.reads ?? [];
  const writes = query.data?.writes ?? [];
  const visibleReads = direction === "write" ? [] : reads;
  const visibleWrites = direction === "read" ? [] : writes;

  return (
    <Card className="mt-6">
      <CardHeader
        title="Linked assets"
        description="Notebooks, queries, jobs, pipelines, streams, and Streamlit apps that read or write this table."
        actions={
          <div className="flex items-center gap-1 text-[11px]">
            {(["all", "read", "write"] as const).map((d) => (
              <button
                key={d}
                type="button"
                onClick={() => setDirection(d)}
                className={
                  d === direction
                    ? "rounded-md border border-accent bg-accent-soft px-2 py-1 font-medium text-accent-ink"
                    : "rounded-md border border-surface-border bg-surface px-2 py-1 text-ink-dim hover:text-ink"
                }
              >
                {d === "all" ? "All" : d === "read" ? "Reads" : "Writes"}
              </button>
            ))}
          </div>
        }
      />
      <CardBody className="space-y-3">
        {query.isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 3 }).map((_, i) => (
              <Skeleton key={i} className="h-4 w-full" />
            ))}
          </div>
        ) : query.error ? (
          <div className="text-sm text-critical">{(query.error as Error).message}</div>
        ) : totalLinked === 0 ? (
          <div className="text-sm text-ink-dim">
            No ingested asset references this table yet. Run{" "}
            <code className="rounded bg-surface px-1 font-mono text-[12px]">/db ingest-assets</code>{" "}
            so notebooks, queries, jobs, and pipelines feed into the asset graph, then come back
            here.
          </div>
        ) : (
          <>
            <div className="flex flex-wrap gap-1.5 text-[11px] text-ink-dim">
              {Object.entries(counts)
                .filter(([, n]) => n > 0)
                .map(([kind, n]) => (
                  <span
                    key={kind}
                    className="rounded-md border border-surface-border bg-surface px-2 py-0.5"
                  >
                    <span className="font-mono">{kind}</span>{" "}
                    <span className="font-semibold text-ink">{n}</span>
                  </span>
                ))}
            </div>
            <LinkedAssetGroup label="Reads" rows={visibleReads} hidden={direction === "write"} />
            <LinkedAssetGroup label="Writes" rows={visibleWrites} hidden={direction === "read"} />
          </>
        )}
      </CardBody>
    </Card>
  );
}

function LinkedAssetGroup({
  label,
  rows,
  hidden,
}: {
  label: string;
  rows: LinkedAssetRow[];
  hidden: boolean;
}) {
  if (hidden) return null;
  return (
    <div className="space-y-1.5">
      <div className="text-[10.5px] font-semibold uppercase tracking-wider text-ink-dim">
        {label} ({rows.length})
      </div>
      {rows.length === 0 ? (
        <div className="text-[12px] italic text-ink-dim">
          No {label.toLowerCase()} in the last 90 days.
        </div>
      ) : (
        <ul className="divide-y divide-surface-border rounded-md border border-surface-border">
          {rows.map((r) => (
            <li
              key={`${r.kind}::${r.id}::${r.edge_type}`}
              className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 px-3 py-2 text-[12.5px]"
            >
              <span className="rounded-sm bg-surface px-1.5 py-0.5 font-mono text-[10.5px] text-ink-dim">
                {r.kind}
              </span>
              <span className="font-medium text-ink">{r.name || "(unnamed)"}</span>
              {r.path ? <span className="font-mono text-[11px] text-ink-dim">{r.path}</span> : null}
              <span className="ml-auto flex flex-wrap items-baseline gap-x-2 text-[11px] text-ink-dim">
                {r.last_user ? <span>{r.last_user}</span> : null}
                {r.last_used_at ? (
                  <span title={new Date(r.last_used_at * 1000).toISOString()}>
                    {formatRelative(r.last_used_at)}
                  </span>
                ) : null}
                {r.direction === "unknown" ? (
                  <span className="italic">direction unknown</span>
                ) : null}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function formatRelative(epoch: number): string {
  const seconds = Math.max(0, Date.now() / 1000 - epoch);
  if (seconds < 60) return "just now";
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`;
  if (seconds < 86400 * 30) return `${Math.round(seconds / 86400)}d ago`;
  return `${Math.round(seconds / (86400 * 30))}mo ago`;
}

function PendingDescriptionBlock({
  text,
  runId,
  label,
  compact,
}: {
  text: string;
  runId: number | null;
  label: string;
  compact?: boolean;
}) {
  const target = runId != null ? `/runs/${runId}` : "/pending";
  return (
    <div
      className={
        compact
          ? "rounded-md border border-amber-300/60 bg-amber-50/60 px-2 py-1.5 text-[12px] text-amber-900 dark:border-amber-500/40 dark:bg-amber-900/20 dark:text-amber-100"
          : "rounded-lg border border-amber-300/60 bg-amber-50/60 p-3 text-sm text-amber-900 dark:border-amber-500/40 dark:bg-amber-900/20 dark:text-amber-100"
      }
    >
      <div className="flex flex-wrap items-center gap-1.5 text-[10.5px] font-semibold uppercase tracking-wider">
        <Sparkles size={11} />
        <span>{label}</span>
        {runId != null ? (
          <span className="font-mono normal-case opacity-80">· Run #{runId}</span>
        ) : null}
        <Link
          to={target}
          className="ml-auto rounded-sm px-1.5 py-0.5 text-[10.5px] font-semibold uppercase tracking-wider underline-offset-2 hover:underline"
        >
          Review →
        </Link>
      </div>
      <div className={compact ? "mt-1 italic" : "mt-1.5 italic leading-snug"}>{text}</div>
    </div>
  );
}
