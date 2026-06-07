// Output channels for the extension. Two channels keep concerns
// separate: "AMX" carries extension-level diagnostics, "AMX Studio"
// carries the spawned server's stdout/stderr so server noise never
// buries extension messages.
import * as vscode from "vscode";

let extensionChannel: vscode.OutputChannel | undefined;
let serverChannel: vscode.OutputChannel | undefined;

export function getExtensionChannel(): vscode.OutputChannel {
  extensionChannel ??= vscode.window.createOutputChannel("AMX");
  return extensionChannel;
}

export function getServerChannel(): vscode.OutputChannel {
  serverChannel ??= vscode.window.createOutputChannel("AMX Studio");
  return serverChannel;
}

export function log(message: string): void {
  getExtensionChannel().appendLine(`[${new Date().toISOString()}] ${message}`);
}

export function disposeChannels(): void {
  extensionChannel?.dispose();
  serverChannel?.dispose();
  extensionChannel = undefined;
  serverChannel = undefined;
}
