// Profile commands: QuickPick-based switching of the active database
// and LLM profiles, plus a manual refresh of the Profiles tree.
// Activations invalidate the catalog cache and refresh the affected
// surfaces so trees and status bar reflect the new context at once.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import { refreshViews } from "../views";

export function registerProfileCommands(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.profiles.switchDb", () => switchDbProfile(services)),
    vscode.commands.registerCommand("amx.profiles.switchLlm", () => switchLlmProfile(services)),
    vscode.commands.registerCommand("amx.profiles.refresh", () => refreshViews("profiles")),
  );
}

interface ProfilePick extends vscode.QuickPickItem {
  name: string;
}

async function switchDbProfile(services: ExtensionServices): Promise<void> {
  await runSwitch(services, async () => {
    const profiles = await services.client.profiles.listDb();
    const picks: ProfilePick[] = profiles.map((profile) => {
      const location = profile.database || profile.host;
      return {
        name: profile.name,
        label: profile.is_active ? `$(circle-filled) ${profile.name}` : profile.name,
        description: location ? `${profile.backend} · ${location}` : profile.backend,
      };
    });
    const pick = await vscode.window.showQuickPick(picks, {
      placeHolder: "Select the database profile to activate",
    });
    if (!pick) return undefined;
    await services.client.profiles.activateDb(pick.name);
    return pick.name;
  });
}

async function switchLlmProfile(services: ExtensionServices): Promise<void> {
  await runSwitch(services, async () => {
    const profiles = await services.client.profiles.listLlm();
    const picks: ProfilePick[] = profiles.map((profile) => ({
      name: profile.name,
      label: profile.is_active ? `$(circle-filled) ${profile.name}` : profile.name,
      description: `${profile.provider} · ${profile.model}`,
    }));
    const pick = await vscode.window.showQuickPick(picks, {
      placeHolder: "Select the LLM profile to activate",
    });
    if (!pick) return undefined;
    await services.client.profiles.activateLlm(pick.name);
    return pick.name;
  });
}

/** Shared activate flow: pick + activate, then refresh dependents. */
async function runSwitch(
  services: ExtensionServices,
  pickAndActivate: () => Promise<string | undefined>,
): Promise<void> {
  try {
    const activated = await pickAndActivate();
    if (!activated) return;
    services.catalog.invalidate();
    refreshViews("profiles", "statusBar");
    void vscode.window.showInformationMessage(`AMX: activated profile "${activated}".`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: profile switch failed: ${message}`);
  }
}
