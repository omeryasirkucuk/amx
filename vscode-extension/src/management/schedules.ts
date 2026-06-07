// Schedule CRUD wizards on top of the existing pause/resume/run-now
// commands (registered in commands/index.ts). Create walks name →
// kind → trigger → timing → profile → scope → llm; edit patches the
// picked fields; delete confirms.
import * as vscode from "vscode";

import type { ScheduleCreateBody, ScheduleSummary } from "../api/types";
import type { ExtensionServices } from "../services";
import { refreshViews } from "../views";
import { guardValue, guardWithRetry } from "./errors";
import { getPromptPort } from "./index";
import { runWizard, type WizardStep } from "./wizard";

interface ScheduleNodeArg {
  schedule?: ScheduleSummary;
}

export function registerScheduleManagement(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.schedules.create", () => createSchedule(services)),
    vscode.commands.registerCommand("amx.schedules.edit", (node?: ScheduleNodeArg) =>
      editSchedule(services, node?.schedule),
    ),
    vscode.commands.registerCommand("amx.schedules.delete", (node?: ScheduleNodeArg) =>
      deleteSchedule(services, node?.schedule),
    ),
  );
}

async function createSchedule(services: ExtensionServices): Promise<void> {
  const { client, catalog } = services;
  const [dbProfiles, llmProfiles] = await Promise.all([
    guardValue("load profiles", () => client.profiles.listDb()),
    guardValue("load profiles", () => client.profiles.listLlm()),
  ]);
  if (dbProfiles === undefined || llmProfiles === undefined) return;
  if (dbProfiles.length === 0) {
    void vscode.window.showWarningMessage("AMX: configure a DB profile first.");
    return;
  }
  const port = getPromptPort("AMX: New Schedule");
  const answers = await runWizard(
    [
      {
        id: "name",
        kind: "input",
        title: "Schedule name",
        required: true,
        validate: (value) => (value.trim() ? undefined : "Name is required"),
      },
      {
        id: "kind",
        kind: "pick",
        title: "What should it do?",
        items: [
          {
            value: "analyze",
            label: "Analyze run",
            description: "Generate descriptions for the scope",
          },
          {
            value: "cache_refresh",
            label: "Catalog refresh",
            description: "Re-sync the catalog cache",
          },
        ],
      },
      {
        id: "trigger",
        kind: "pick",
        title: "Trigger",
        items: [
          { value: "time", label: "At a time", description: "One-shot or recurring" },
          {
            value: "change",
            label: "On change",
            description: "Fires when new assets appear in scope",
          },
        ],
      },
      {
        id: "fire_at_local",
        kind: "input",
        title: "Fire at (YYYY-MM-DDTHH:MM, local wall clock)",
        when: (a) => a["trigger"] === "time",
        required: true,
        validate: (value) =>
          /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$/.test(value.trim())
            ? undefined
            : "Use YYYY-MM-DDTHH:MM",
      },
      {
        id: "fire_at_tz",
        kind: "input",
        title: "Timezone (IANA id)",
        value: "UTC",
        when: (a) => a["trigger"] === "time",
      },
      {
        id: "cron_expr",
        kind: "input",
        title: "Cron expression (empty = one-shot)",
        placeholder: "e.g. 0 */6 * * *",
        when: (a) => a["trigger"] === "time",
      },
      {
        id: "db_profile",
        kind: "pick",
        title: "DB profile",
        items: dbProfiles.map((profile) => ({
          value: profile.name,
          label: profile.name,
          description: profile.backend,
        })),
      },
      {
        id: "llm_profile",
        kind: "pick",
        title: "LLM profile",
        when: (a) => a["kind"] === "analyze",
        items: llmProfiles.map((profile) => ({
          value: profile.name,
          label: profile.name,
          description: profile.model,
        })),
      },
    ] satisfies WizardStep[],
    port,
  );
  if (!answers) return;

  // Scope: all schemas, or pick schemas from the indexed catalog.
  const profile = String(answers["db_profile"]);
  const tables = await guardValue("load catalog tables", () => catalog.getTables({ profile }));
  if (tables === undefined) return;
  const schemas = [...new Set(tables.map((table) => table.schema))].sort();
  const scopeAnswers = await runWizard(
    [
      {
        id: "mode",
        kind: "pick",
        title: "Scope",
        items: [
          { value: "all", label: "Everything reachable" },
          ...(schemas.length > 0 ? [{ value: "schemas", label: "Pick schemas…" }] : []),
        ],
      },
      {
        id: "schemas",
        kind: "pickMany",
        title: "Schemas",
        when: (a) => a["mode"] === "schemas",
        items: schemas.map((schema) => ({ value: schema, label: schema })),
      },
    ] satisfies WizardStep[],
    port,
  );
  if (!scopeAnswers) return;
  const scope: Record<string, unknown> =
    scopeAnswers["mode"] === "all"
      ? { mode: "all" }
      : { mode: "schemas", schemas: scopeAnswers["schemas"] ?? [] };

  const body: ScheduleCreateBody = {
    name: String(answers["name"]).trim(),
    fire_at_local: String(answers["fire_at_local"] ?? ""),
    fire_at_tz: String(answers["fire_at_tz"] ?? "UTC"),
    db_profile: profile,
    scope,
    llm_profile: String(answers["llm_profile"] ?? ""),
    review_strategy: "auto",
    kind: answers["kind"] as "analyze" | "cache_refresh",
    trigger: answers["trigger"] as "time" | "change",
  };
  const cron = String(answers["cron_expr"] ?? "").trim();
  if (cron) body.cron_expr = cron;

  await guardWithRetry("create the schedule", async () => {
    await services.client.schedules.create(body);
    refreshViews("schedules");
    void vscode.window.showInformationMessage(`AMX: schedule '${body.name}' created.`);
  });
}

async function editSchedule(
  services: ExtensionServices,
  schedule?: ScheduleSummary,
): Promise<void> {
  const target = schedule ?? (await pickSchedule(services));
  if (!target) return;
  const field = await vscode.window.showQuickPick(
    [
      { label: "Name", id: "name" },
      { label: "Fire time", id: "fire_at_local" },
      { label: "Timezone", id: "fire_at_tz" },
      { label: "Cron expression", id: "cron_expr" },
      { label: "LLM profile", id: "llm_profile" },
    ],
    { title: `AMX: edit '${target.name ?? target.id}' — which field?` },
  );
  if (!field) return;
  const value = await vscode.window.showInputBox({
    title: `AMX: new value for ${field.label}`,
    value: String((target as Record<string, unknown>)[field.id] ?? ""),
  });
  if (value === undefined) return;
  try {
    await services.client.schedules.patch(target.id, { [field.id]: value.trim() });
    refreshViews("schedules");
    void vscode.window.showInformationMessage("AMX: schedule updated.");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not update the schedule: ${message}`);
  }
}

async function deleteSchedule(
  services: ExtensionServices,
  schedule?: ScheduleSummary,
): Promise<void> {
  const target = schedule ?? (await pickSchedule(services));
  if (!target) return;
  const confirmed = await vscode.window.showWarningMessage(
    `Delete schedule '${target.name ?? target.id}'?`,
    { modal: true },
    "Delete",
  );
  if (confirmed !== "Delete") return;
  try {
    await services.client.schedules.remove(target.id);
    refreshViews("schedules");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not delete the schedule: ${message}`);
  }
}

/** QuickPick over the configured schedules; shared with commands/index.ts. */
export async function pickSchedule(
  services: ExtensionServices,
): Promise<ScheduleSummary | undefined> {
  const schedules = await guardValue("list schedules", () => services.client.schedules.list());
  if (schedules === undefined) return undefined;
  if (schedules.length === 0) {
    void vscode.window.showInformationMessage("AMX: no schedules configured.");
    return undefined;
  }
  const pick = await vscode.window.showQuickPick(
    schedules.map((schedule) => ({
      label: schedule.name ?? `Schedule ${schedule.id}`,
      description: schedule.cron ?? "",
      schedule,
    })),
    { title: "Select a schedule" },
  );
  return pick?.schedule;
}
