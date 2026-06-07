// Server lifecycle commands: start (with a progress notification),
// stop, restart, and a shortcut to the Studio server output channel.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import { getServerChannel } from "../util/log";

export function registerServerCommands(services: ExtensionServices): void {
  const { server } = services;

  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.server.start", () =>
      withProgressAndErrors("AMX: starting Studio server", () => server.ensure()),
    ),
    vscode.commands.registerCommand("amx.server.stop", () =>
      withProgressAndErrors("AMX: stopping Studio server", () => server.stop()),
    ),
    vscode.commands.registerCommand("amx.server.restart", () =>
      withProgressAndErrors("AMX: restarting Studio server", () => server.restart()),
    ),
    vscode.commands.registerCommand("amx.server.showLogs", () => {
      getServerChannel().show(true);
    }),
  );
}

async function withProgressAndErrors(title: string, task: () => Promise<unknown>): Promise<void> {
  try {
    await vscode.window.withProgress(
      { location: vscode.ProgressLocation.Notification, title, cancellable: false },
      () => task(),
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const choice = await vscode.window.showErrorMessage(`AMX: ${message}`, "Show Logs");
    if (choice === "Show Logs") getServerChannel().show(true);
  }
}
