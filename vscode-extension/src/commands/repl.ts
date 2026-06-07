// The amx.openRepl command: resolve (or install) the AMX runtime and
// launch the interactive REPL in a dedicated integrated terminal.
// Paths with spaces are double-quoted; on Windows the PowerShell
// call operator (&) is prepended when the executable token is quoted.
import * as vscode from "vscode";

import type { RuntimeInfo } from "../runtime/runtimeManager";
import type { ExtensionServices } from "../services";

export function registerReplCommand(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.openRepl", () => openRepl(services)),
  );
}

async function openRepl(services: ExtensionServices): Promise<void> {
  const runtime = await services.runtime.ensureInstalled();
  if (!runtime) return; // ensureInstalled already surfaced the error.
  const terminal = vscode.window.createTerminal({ name: "AMX REPL" });
  terminal.show();
  terminal.sendText(buildReplCommand(runtime, process.platform === "win32"));
}

/** Shell line that drops into the AMX REPL for the resolved runtime. */
export function buildReplCommand(runtime: RuntimeInfo, isWindows: boolean): string {
  const executable = quotePath(
    runtime.kind === "amx-cli" ? runtime.amxCliPath : runtime.pythonPath,
  );
  const line =
    runtime.kind === "amx-cli"
      ? executable
      : `${executable} -c "from amx.cli import run_cli; run_cli()"`;
  // PowerShell treats a leading quoted string as data, not a command;
  // the call operator makes it executable again.
  return isWindows && executable.startsWith('"') ? `& ${line}` : line;
}

function quotePath(path: string): string {
  return /\s/.test(path) ? `"${path}"` : path;
}
