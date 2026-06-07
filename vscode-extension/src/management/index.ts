// Registration entry for the native management surfaces, plus the
// injectable prompt-port seam: wizards obtain their PromptPort through
// getPromptPort() so the integration suite can swap in a scripted port
// (via the hidden amx.test.setScriptedAnswers command) and drive the
// wizards end-to-end without UI.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import { registerCatalogOps } from "./catalogOps";
import { registerProfileManagement } from "./profiles";
import { vscodePromptPort } from "./promptPort";
import { registerRunManagement } from "./runs";
import { registerScheduleManagement } from "./schedules";
import type { PromptPort } from "./wizard";

let promptPortFactory: (title: string) => PromptPort = vscodePromptPort;

/** Replace the prompt-port factory (test seam). */
export function setPromptPortFactory(factory: (title: string) => PromptPort): void {
  promptPortFactory = factory;
}

/** Obtain the PromptPort for a wizard with the given title. */
export function getPromptPort(title: string): PromptPort {
  return promptPortFactory(title);
}

/**
 * Build a PromptPort that answers every prompt by popping the next
 * scripted answer. `undefined` entries simulate Esc; running out of
 * answers also aborts the wizard. Same semantics as the unit-test
 * scripted port. Note: modal confirms via vscode.window.show*Message
 * are not ports and resolve undefined on their own in test mode.
 */
function scriptedPromptPort(queue: (string | string[] | undefined)[]): PromptPort {
  const next = (): string | string[] | undefined => queue.shift();
  return {
    pick: async () => next() as string | undefined,
    pickMany: async () => {
      const value = next();
      if (value === undefined) return undefined;
      return Array.isArray(value) ? value : [value];
    },
    input: async () => next() as string | undefined,
  };
}

export function registerManagement(services: ExtensionServices): void {
  registerProfileManagement(services);
  registerCatalogOps(services);
  registerRunManagement(services);
  registerScheduleManagement(services);
  services.context.subscriptions.push(
    // Hidden test-only command: installs a scripted prompt-port factory
    // so the integration suite can drive wizards without real UI.
    vscode.commands.registerCommand(
      "amx.test.setScriptedAnswers",
      (answers: (string | string[] | undefined)[]) => {
        const queue = [...answers];
        setPromptPortFactory(() => scriptedPromptPort(queue));
      },
    ),
  );
}
