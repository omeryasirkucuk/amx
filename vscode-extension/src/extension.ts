// Extension entry point: build services, register every surface.
// All real logic lives in the feature modules — this file only wires.
import * as vscode from "vscode";

import { registerCommands } from "./commands";
import { registerLanguageFeatures } from "./language";
import { createServices } from "./services";
import { disposeChannels } from "./util/log";
import { registerViews } from "./views";
import { registerWebviews } from "./webviews";

export function activate(context: vscode.ExtensionContext): void {
  const services = createServices(context);
  registerViews(services);
  registerWebviews(services);
  registerCommands(services);
  registerLanguageFeatures(services);
}

export function deactivate(): void {
  disposeChannels();
}
