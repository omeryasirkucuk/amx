// Service container assembled once in activate() and passed to every
// registration entry point. Plain bag, no DI framework.
import * as vscode from "vscode";

import { AmxClient } from "./api/client";
import { CatalogCache } from "./api/catalogCache";
import { RuntimeManager } from "./runtime/runtimeManager";
import { ServerManager } from "./server/serverManager";

export interface ExtensionServices {
  readonly context: vscode.ExtensionContext;
  readonly runtime: RuntimeManager;
  readonly server: ServerManager;
  readonly client: AmxClient;
  readonly catalog: CatalogCache;
}

export function createServices(context: vscode.ExtensionContext): ExtensionServices {
  const runtime = new RuntimeManager(context.globalStorageUri.fsPath);
  const server = new ServerManager(runtime, context.globalState);
  // Reap any owned server a previous extension host failed to kill
  // (deactivate cannot await) before it can squat on the preferred
  // port and wedge the next spawn. Healthy leftovers are kept and
  // re-adopted through the discovery file.
  void server.reconcileOrphans();
  const client = new AmxClient(server);
  const ttlSeconds = vscode.workspace
    .getConfiguration("amx")
    .get<number>("catalog.cacheTtlSeconds", 300);
  const catalog = new CatalogCache(client, ttlSeconds * 1000);

  // Cross-service wiring kept here so the services themselves stay
  // unaware of each other beyond their constructor dependencies.
  context.subscriptions.push(
    runtime,
    server,
    client.onMutation(({ path }) => {
      if (/\/api\/(generate|pending|comments|profiles|catalog)\b/.test(path)) {
        catalog.invalidate();
      }
    }),
    server.onDidChangeState((state) => {
      if (state.status === "running") catalog.invalidate();
    }),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("amx.pythonPath") || event.affectsConfiguration("amx.cliPath")) {
        runtime.invalidate();
      }
      if (event.affectsConfiguration("amx.catalog.cacheTtlSeconds")) {
        const seconds = vscode.workspace
          .getConfiguration("amx")
          .get<number>("catalog.cacheTtlSeconds", 300);
        catalog.setTtl(seconds * 1000);
      }
    }),
  );

  return { context, runtime, server, client, catalog };
}
