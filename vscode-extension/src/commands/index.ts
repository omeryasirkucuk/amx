// Registration entry for Command Palette commands. Delegates to the
// per-concern command modules and registers the small view-glue
// commands (refresh, schedule actions, status menu) that exist only
// to bridge tree items and the status bar to the services layer.
import * as vscode from "vscode";

import type { ScheduleSummary } from "../api/types";
import type { ExtensionServices } from "../services";
import { getViews, refreshViews } from "../views";
import { showStatusMenu } from "../views/statusBar";
import { registerManagement } from "../management";
import { pickSchedule } from "../management/schedules";
import { registerOpenPanelCommands } from "./openPanels";
import { registerProfileCommands } from "./profileCommands";
import { registerReplCommand } from "./repl";
import { registerRuntimeCommands } from "./runtimeCommands";
import { registerServerCommands } from "./serverCommands";

export function registerCommands(services: ExtensionServices): void {
  registerServerCommands(services);
  registerProfileCommands(services);
  registerOpenPanelCommands(services);
  registerReplCommand(services);
  registerRuntimeCommands(services);
  registerViewGlueCommands(services);
  registerScheduleCommands(services);
  registerManagement(services);
}

function registerViewGlueCommands(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.catalog.refresh", () => services.catalog.invalidate()),
    vscode.commands.registerCommand("amx.history.refresh", () => refreshViews("history")),
    vscode.commands.registerCommand("amx.history.loadMore", () => getViews().history.loadMore()),
    vscode.commands.registerCommand("amx.schedules.refresh", () => refreshViews("schedules")),
    vscode.commands.registerCommand("amx.statusMenu", () => showStatusMenu()),
  );
}

type ScheduleAction = "pause" | "resume" | "runNow";

const SCHEDULE_ACTION_LABELS: Record<ScheduleAction, string> = {
  pause: "$(debug-pause) Pause",
  resume: "$(debug-start) Resume",
  runNow: "$(run) Run Now",
};

/**
 * Normalize a command argument into a ScheduleSummary. Context-menu
 * invocations pass the tree ELEMENT (`{type: "schedule", schedule}`),
 * programmatic callers pass the summary itself, and palette
 * invocations pass nothing.
 */
const unwrap = (arg?: unknown): ScheduleSummary | undefined => {
  if (!arg || typeof arg !== "object") return undefined;
  const o = arg as Record<string, unknown>;
  if (o["schedule"]) return o["schedule"] as ScheduleSummary;
  if ("id" in o) return arg as ScheduleSummary;
  return undefined;
};

// Invoked from tree context menus (which pass the tree element),
// TreeItem.command, the action picker, and the palette.
function registerScheduleCommands(services: ExtensionServices): void {
  const run = (action: ScheduleAction) => (arg?: unknown) =>
    runScheduleAction(services, action, unwrap(arg));
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.schedules.pause", run("pause")),
    vscode.commands.registerCommand("amx.schedules.resume", run("resume")),
    vscode.commands.registerCommand("amx.schedules.runNow", run("runNow")),
    vscode.commands.registerCommand("amx.schedules.actions", (arg?: unknown) =>
      pickScheduleAction(services, unwrap(arg)),
    ),
  );
}

async function pickScheduleAction(
  services: ExtensionServices,
  schedule?: ScheduleSummary,
): Promise<void> {
  const target = schedule ?? (await pickSchedule(services));
  if (!target) return;
  const actions: ScheduleAction[] = ["pause", "resume", "runNow"];
  const pick = await vscode.window.showQuickPick(
    actions.map((action) => ({ label: SCHEDULE_ACTION_LABELS[action], action })),
    { placeHolder: `Schedule: ${target.name ?? target.id}` },
  );
  if (!pick) return;
  await runScheduleAction(services, pick.action, target);
}

async function runScheduleAction(
  services: ExtensionServices,
  action: ScheduleAction,
  schedule?: ScheduleSummary,
): Promise<void> {
  const target = schedule ?? (await pickSchedule(services));
  if (!target) return;
  try {
    await services.client.schedules[action](target.id);
    refreshViews("schedules");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: schedule action failed: ${message}`);
  }
}
