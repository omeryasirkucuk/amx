// Run lifecycle: scoped start wizard (profile → schema → tables),
// SSE-backed progress notification, cancel on running rows, rerun of
// a finished run's result items.
import * as vscode from "vscode";

import { eventType } from "../api/sse";
import type { RunSummary } from "../api/types";
import type { ExtensionServices } from "../services";
import { refreshViews } from "../views";
import { guardValue, guardWithRetry } from "./errors";
import { getPromptPort } from "./index";
import { runWizard, type WizardStep } from "./wizard";

interface RunNodeArg {
  run?: RunSummary;
}

interface StartArgs {
  profile?: string;
  /** Database / UC catalog scope from the catalog tree's database level. */
  database?: string;
  schema?: string;
  table?: string;
  /** Schema bulk run: skip the table picker, analyze every table. */
  allTables?: boolean;
}

export function registerRunManagement(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.runs.start", (prefill?: StartArgs) =>
      startRun(services, prefill ?? {}),
    ),
    vscode.commands.registerCommand("amx.runs.cancel", (node?: RunNodeArg) =>
      cancelRun(services, node?.run),
    ),
    vscode.commands.registerCommand("amx.runs.rerun", (node?: RunNodeArg) =>
      rerunRun(services, node?.run),
    ),
  );
}

async function startRun(services: ExtensionServices, prefill: StartArgs): Promise<void> {
  const { client, catalog } = services;
  const profiles = await guardValue("list DB profiles", () => client.profiles.listDb());
  if (profiles === undefined) return;
  if (profiles.length === 0) {
    void vscode.window.showWarningMessage("AMX: no DB profiles configured.");
    return;
  }
  const port = getPromptPort("AMX: Start Run");

  const profileStep: WizardStep = {
    id: "profile",
    kind: "pick",
    title: "DB profile",
    items: profiles.map((profile) => ({
      value: profile.name,
      label: profile.name,
      description: profile.backend,
    })),
  };
  const first = prefill.profile ? { profile: prefill.profile } : await runWizard([profileStep], port);
  if (!first) return;
  const profile = String(first["profile"]);

  const tableScope: { profile: string; database?: string } = { profile };
  if (prefill.database !== undefined) tableScope.database = prefill.database;
  const tables = await guardValue("load catalog tables", () => catalog.getTables(tableScope));
  if (tables === undefined) return;
  if (tables.length === 0) {
    void vscode.window.showWarningMessage(
      `AMX: no indexed tables for '${profile}' — run a catalog sync first.`,
    );
    return;
  }
  const schemas = [...new Set(tables.map((table) => table.schema))].sort();
  const schemaStep: WizardStep = {
    id: "schema",
    kind: "pick",
    title: "Schema",
    items: schemas.map((schema) => ({ value: schema, label: schema })),
  };
  const second = prefill.schema ? { schema: prefill.schema } : await runWizard([schemaStep], port);
  if (!second) return;
  const schema = String(second["schema"]);

  const tablesInSchema = tables
    .filter((table) => table.schema === schema)
    .map((table) => table.name)
    .sort();
  let chosen: string[];
  if (prefill.table) {
    chosen = [prefill.table];
  } else if (prefill.allTables) {
    // Schema bulk run from the tree: every table, no picker — the
    // modal confirm below is the single gate.
    chosen = tablesInSchema;
  } else {
    const tableAnswers = await runWizard(
      [
        {
          id: "tables",
          kind: "pickMany",
          title: "Tables (empty selection = every table in the schema)",
          items: tablesInSchema.map((table) => ({ value: table, label: table })),
        },
      ],
      port,
    );
    if (!tableAnswers) return;
    chosen = (tableAnswers["tables"] as string[]) ?? [];
  }
  // The server rejects empty scopes, so the "every table" shortcut
  // (empty selection) expands to the full table list explicitly.
  if (chosen.length === 0) chosen = tablesInSchema;

  const summary =
    chosen.length === tablesInSchema.length
      ? `${schema}.*`
      : chosen.map((t) => `${schema}.${t}`).join(", ");
  const go = await vscode.window.showInformationMessage(
    `Start an analyze run for ${summary} on '${profile}'?`,
    { modal: true },
    "Start Run",
  );
  if (go !== "Start Run") return;

  await guardWithRetry("start the run", async () => {
    const body: Parameters<typeof client.runs.submit>[0] = {
      scope: { [schema]: chosen },
      db_profile: profile,
    };
    if (prefill.database !== undefined) body.database = prefill.database;
    const job = await client.runs.submit(body);
    void trackRun(services, job.job_id, summary);
  });
}

/** Progress notification fed by the run's SSE stream. */
async function trackRun(
  services: ExtensionServices,
  jobId: string,
  summary: string,
): Promise<void> {
  refreshViews("history");
  await vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: `AMX run: ${summary}`,
      cancellable: true,
    },
    async (progress, cancelToken) => {
      const abort = new AbortController();
      const cancelListener = cancelToken.onCancellationRequested(() => {
        void services.client.runs.cancel(jobId);
        abort.abort();
      });
      let outcome = "finished";
      try {
        for await (const event of services.client.sse(`/api/runs/${jobId}/events`, {
          signal: abort.signal,
        })) {
          const type = eventType(event) ?? "";
          if (type === "job.failed") outcome = "failed";
          if (type === "job.cancelled") outcome = "cancelled";
          const payload = event.data as { label?: string; message?: string } | undefined;
          const label = payload?.label ?? payload?.message;
          if (label) progress.report({ message: String(label).slice(0, 80) });
        }
      } catch {
        outcome = "connection lost";
      }
      cancelListener.dispose();
      if (cancelToken.isCancellationRequested) outcome = "cancelled";
      refreshViews("history");
      if (outcome === "finished") {
        const open = await vscode.window.showInformationMessage(
          `AMX run ${summary}: finished.`,
          "Open History",
        );
        if (open === "Open History") await vscode.commands.executeCommand("amx.history.refresh");
      } else {
        void vscode.window.showWarningMessage(`AMX run ${summary}: ${outcome}.`);
      }
    },
  );
}

async function cancelRun(services: ExtensionServices, run?: RunSummary): Promise<void> {
  const jobId = run?.live_job_id;
  if (!jobId) {
    void vscode.window.showWarningMessage("AMX: that run is not currently running.");
    return;
  }
  try {
    await services.client.runs.cancel(jobId);
    refreshViews("history");
    void vscode.window.showInformationMessage("AMX: cancel requested.");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: cancel failed: ${message}`);
  }
}

async function rerunRun(services: ExtensionServices, run?: RunSummary): Promise<void> {
  if (!run) return;
  try {
    const results = await services.client.runs.results(run.id);
    const ids = results.map((row) => row.id).filter((id) => Number.isInteger(id));
    if (ids.length === 0) {
      void vscode.window.showWarningMessage("AMX: run has no result rows to re-run.");
      return;
    }
    const instructions = await vscode.window.showInputBox({
      title: `AMX: re-run ${ids.length} result(s) of run #${run.id}`,
      prompt: "Optional extra instructions for the model (Enter to skip)",
    });
    if (instructions === undefined) return;
    const job = await services.client.runs.rerunItems(ids, instructions.trim() || undefined);
    void trackRun(services, job.job_id, `rerun #${run.id}`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: rerun failed: ${message}`);
  }
}
