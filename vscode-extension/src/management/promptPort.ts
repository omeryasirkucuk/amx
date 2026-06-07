// VS Code implementation of PromptPort: createQuickPick / createInputBox
// with a Back button, Esc → undefined, validation wiring. All wizard
// LOGIC lives in wizard.ts; this file only translates steps to UI.
import * as vscode from "vscode";

import {
  WIZARD_BACK,
  type InputStep,
  type PickManyStep,
  type PickStep,
  type PromptPort,
} from "./wizard";

export function vscodePromptPort(wizardTitle: string): PromptPort {
  return {
    pick: (step) => showPick(wizardTitle, step, false) as Promise<string | symbol | undefined>,
    pickMany: (step) => showPick(wizardTitle, step, true) as Promise<string[] | symbol | undefined>,
    input: (step) => showInput(wizardTitle, step),
  };
}

function showPick(
  wizardTitle: string,
  step: (PickStep | PickManyStep) & { stepLabel: string },
  many: boolean,
): Promise<string | string[] | symbol | undefined> {
  return new Promise((resolve) => {
    const quickPick = vscode.window.createQuickPick();
    quickPick.title = `${wizardTitle} — ${step.title} ${step.stepLabel}`;
    quickPick.placeholder = step.placeholder ?? "";
    quickPick.canSelectMany = many;
    quickPick.ignoreFocusOut = true;
    quickPick.buttons = [vscode.QuickInputButtons.Back];
    quickPick.items = step.items.map((item) => ({
      label: item.label,
      description: item.description ?? "",
    }));
    const valueOf = (label: string) =>
      step.items.find((item) => item.label === label)?.value ?? label;
    let settled = false;
    const settle = (value: string | string[] | symbol | undefined) => {
      if (settled) return;
      settled = true;
      quickPick.hide();
      quickPick.dispose();
      resolve(value);
    };
    quickPick.onDidTriggerButton(() => settle(WIZARD_BACK));
    quickPick.onDidAccept(() => {
      if (many) settle(quickPick.selectedItems.map((item) => valueOf(item.label)));
      else settle(quickPick.selectedItems[0] ? valueOf(quickPick.selectedItems[0].label) : undefined);
    });
    quickPick.onDidHide(() => settle(undefined));
    quickPick.show();
  });
}

function showInput(
  wizardTitle: string,
  step: InputStep & { stepLabel: string },
): Promise<string | symbol | undefined> {
  return new Promise((resolve) => {
    const input = vscode.window.createInputBox();
    input.title = `${wizardTitle} — ${step.title} ${step.stepLabel}`;
    input.placeholder = step.placeholder ?? "";
    input.value = step.value ?? "";
    input.password = step.password ?? false;
    input.ignoreFocusOut = true;
    input.buttons = [vscode.QuickInputButtons.Back];
    let settled = false;
    const settle = (value: string | symbol | undefined) => {
      if (settled) return;
      settled = true;
      input.hide();
      input.dispose();
      resolve(value);
    };
    input.onDidTriggerButton(() => settle(WIZARD_BACK));
    input.onDidChangeValue((value) => {
      input.validationMessage = step.validate?.(value) ?? "";
    });
    input.onDidAccept(() => {
      const error = step.validate?.(input.value);
      if (error) {
        input.validationMessage = error;
        return;
      }
      settle(input.value);
    });
    input.onDidHide(() => settle(undefined));
    input.show();
  });
}
