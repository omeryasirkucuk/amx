// amx.openAsset command: forward the asset reference to the Studio
// webview layer, which owns amx.panel.open and renders the asset
// panel. Kept as a thin delegate so hovers, lenses, and quick fixes
// share one entry point regardless of how the panel evolves.
import * as vscode from "vscode";

export function registerOpenAsset(): vscode.Disposable {
  return vscode.commands.registerCommand("amx.openAsset", (args?: unknown) =>
    vscode.commands.executeCommand("amx.panel.open", "table", args),
  );
}
