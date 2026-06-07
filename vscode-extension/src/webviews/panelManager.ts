// Singleton-per-area lifecycle for the Studio webview panels.
// Opening an area that is already on screen reveals the existing
// panel and re-points its iframe; serializers restore panels (with
// their persisted area arguments) across window reloads.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import {
  AREA_DEFINITIONS,
  coercePanelArgs,
  type PanelArea,
  type PanelAreaArgs,
} from "./areas";
import { StudioPanel } from "./studioPanel";

export class PanelManager implements vscode.Disposable {
  private readonly panels = new Map<PanelArea, StudioPanel>();

  constructor(private readonly services: ExtensionServices) {}

  /** Open (or reveal + navigate) the singleton panel for an area. */
  open(area: PanelArea, args: PanelAreaArgs = {}): void {
    const definition = AREA_DEFINITIONS[area];
    const existing = this.panels.get(area);
    if (existing) {
      existing.navigate(args);
      existing.reveal();
      return;
    }
    this.track(area, StudioPanel.create(this.services, definition, args));
  }

  /** One WebviewPanelSerializer per area viewType ("amx.panel.<area>"). */
  registerSerializers(): vscode.Disposable[] {
    return Object.values(AREA_DEFINITIONS).map((definition) =>
      vscode.window.registerWebviewPanelSerializer(definition.viewType, {
        deserializeWebviewPanel: async (webviewPanel, state: unknown) => {
          const args = restoredArgs(state);
          const existing = this.panels.get(definition.area);
          if (existing) {
            // The singleton already exists (restore raced a fresh
            // open) — keep the live one and drop the restored shell.
            webviewPanel.dispose();
            existing.navigate(args);
            return;
          }
          this.track(definition.area, StudioPanel.adopt(this.services, definition, webviewPanel, args));
        },
      }),
    );
  }

  dispose(): void {
    for (const panel of this.panels.values()) panel.dispose();
    this.panels.clear();
  }

  private track(area: PanelArea, panel: StudioPanel): void {
    this.panels.set(area, panel);
    panel.onDidDispose(() => {
      if (this.panels.get(area) === panel) this.panels.delete(area);
    });
  }
}

/** Recover the area arguments persisted by the shell's setState(). */
function restoredArgs(state: unknown): PanelAreaArgs {
  if (typeof state !== "object" || state === null) return {};
  return coercePanelArgs((state as { args?: unknown }).args);
}
