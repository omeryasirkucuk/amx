// Status bar entry showing the active LLM model and DB backend from
// /api/context. Updates on server state changes plus a 60s timer
// while the server runs; never calls the API while the server is
// down (that would trigger an unwanted start). Clicking opens the
// amx.statusMenu QuickPick, whose implementation also lives here so
// the menu stays next to the surface that exposes it.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";

const REFRESH_INTERVAL_MS = 60_000;

export class AmxStatusBar implements vscode.Disposable {
  private readonly item: vscode.StatusBarItem;
  private timer: ReturnType<typeof setInterval> | undefined;
  private readonly stateSubscription: vscode.Disposable;

  constructor(private readonly services: ExtensionServices) {
    this.item = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
    this.item.command = "amx.statusMenu";
    this.item.show();
    this.stateSubscription = services.server.onDidChangeState(() => void this.update());
    void this.update();
  }

  async update(): Promise<void> {
    const state = this.services.server.state;
    if (state.status !== "running") {
      this.clearTimer();
      this.item.text = "$(warning) AMX";
      this.item.tooltip =
        state.status === "error"
          ? `AMX Studio server error: ${state.message}`
          : "AMX Studio server is not running — click for actions";
      return;
    }
    this.armTimer();
    try {
      const context = await this.services.client.system.context();
      const parts = [`$(sparkle) ${context.llm_model ?? "no LLM"}`];
      if (context.db_backend) parts.push(`$(database) ${context.db_backend}`);
      this.item.text = parts.join("  ");
      this.item.tooltip = "AMX — click for actions";
    } catch {
      this.item.text = "$(warning) AMX";
      this.item.tooltip = "AMX Studio server is not answering — click for actions";
    }
  }

  dispose(): void {
    this.clearTimer();
    this.stateSubscription.dispose();
    this.item.dispose();
  }

  private armTimer(): void {
    this.timer ??= setInterval(() => void this.update(), REFRESH_INTERVAL_MS);
  }

  private clearTimer(): void {
    if (this.timer !== undefined) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }
}

interface StatusMenuEntry extends vscode.QuickPickItem {
  command: string;
}

/** QuickPick behind the status bar item — delegates to amx.* commands. */
export async function showStatusMenu(): Promise<void> {
  const entries: StatusMenuEntry[] = [
    { label: "$(sparkle) Switch LLM Profile", command: "amx.profiles.switchLlm" },
    { label: "$(settings-gear) Open Studio Settings", command: "amx.openSettings" },
    { label: "$(play) Start Server", command: "amx.server.start" },
    { label: "$(debug-stop) Stop Server", command: "amx.server.stop" },
    { label: "$(debug-restart) Restart Server", command: "amx.server.restart" },
    { label: "$(output) Show Logs", command: "amx.server.showLogs" },
    { label: "$(terminal) Open REPL", command: "amx.openRepl" },
  ];
  const pick = await vscode.window.showQuickPick(entries, { placeHolder: "AMX" });
  if (pick) await vscode.commands.executeCommand(pick.command);
}
