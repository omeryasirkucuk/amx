// Registration entry for the Studio webview panels: the programmatic
// "amx.panel.open" command (not contributed in package.json — panels
// are opened by other extension surfaces, never the palette) and one
// WebviewPanelSerializer per area for restore across window reloads.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import { log } from "../util/log";
import { coercePanelArgs, isPanelArea } from "./areas";
import { PanelManager } from "./panelManager";

export function registerWebviews(services: ExtensionServices): void {
  const manager = new PanelManager(services);
  services.context.subscriptions.push(
    manager,
    vscode.commands.registerCommand("amx.panel.open", (area: unknown, args?: unknown) => {
      if (!isPanelArea(area)) {
        log(`amx.panel.open called with unknown area: ${String(area)}`);
        return;
      }
      manager.open(area, coercePanelArgs(args));
    }),
    ...manager.registerSerializers(),
  );
}
