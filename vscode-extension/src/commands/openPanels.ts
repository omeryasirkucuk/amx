// Thin command shims that route every "open something in Studio"
// entry point to the single amx.panel.open command the webview layer
// registers. No webview logic lives here — only area routing.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";

type PanelArea = "ask" | "run" | "lineage" | "pages" | "settings" | "runDetail" | "table";

function openPanel(area: PanelArea, args?: unknown): Thenable<unknown> {
  return vscode.commands.executeCommand("amx.panel.open", area, args);
}

export function registerOpenPanelCommands(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.openAsk", () => openPanel("ask")),
    vscode.commands.registerCommand("amx.newRun", () => openPanel("run")),
    vscode.commands.registerCommand("amx.openLineage", () => openPanel("lineage")),
    vscode.commands.registerCommand("amx.openPages", () => openPanel("pages")),
    vscode.commands.registerCommand("amx.openSettings", () => openPanel("settings")),
    vscode.commands.registerCommand("amx.openRun", (args?: { runId?: string }) =>
      openPanel("runDetail", args),
    ),
    vscode.commands.registerCommand(
      "amx.openAsset",
      (args?: { schema?: string; table?: string; profile?: string }) => openPanel("table", args),
    ),
  );
}
