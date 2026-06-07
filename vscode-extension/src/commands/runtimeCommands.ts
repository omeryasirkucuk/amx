// Runtime commands: pick a Python interpreter for AMX (persisted to
// the global amx.pythonPath setting) and trigger the managed install
// flow when AMX is not present on the machine.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";

export function registerRuntimeCommands(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.runtime.selectInterpreter", () =>
      selectInterpreter(services),
    ),
    vscode.commands.registerCommand("amx.runtime.installAmx", async () => {
      await services.runtime.ensureInstalled();
    }),
  );
}

async function selectInterpreter(services: ExtensionServices): Promise<void> {
  const picks = await vscode.window.showOpenDialog({
    canSelectFiles: true,
    canSelectFolders: false,
    canSelectMany: false,
    openLabel: "Use Interpreter",
    title: "Select the Python interpreter AMX should use",
  });
  const selected = picks?.[0];
  if (!selected) return;
  await vscode.workspace
    .getConfiguration("amx")
    .update("pythonPath", selected.fsPath, vscode.ConfigurationTarget.Global);
  services.runtime.invalidate();
  void vscode.window.showInformationMessage(`AMX: interpreter set to ${selected.fsPath}.`);
}
