// Registration entry for the SQL editor language features: builds
// the document selector from amx.editor.languages, wires the scanner
// cache and catalog resolver, registers the providers that are
// enabled, and re-registers them whenever an amx.editor.* setting
// changes. Commands are registered once and survive re-registration.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import { DisposableStore } from "../util/disposables";
import { log } from "../util/log";
import { registerGenerateDescription } from "./actions/generateDescription";
import { registerOpenAsset } from "./actions/openInStudio";
import { DocumentStateStore } from "./documentState";
import { AmxCodeLensProvider } from "./providers/codeLensProvider";
import { createCompletionProvider } from "./providers/completionProvider";
import { AmxDiagnostics } from "./providers/diagnosticsProvider";
import { createHoverProvider } from "./providers/hoverProvider";
import { CatalogResolver } from "./resolver";

export function registerLanguageFeatures(services: ExtensionServices): void {
  const { context, catalog } = services;
  const resolver = new CatalogResolver(catalog);
  const state = new DocumentStateStore();
  let providerStore = new DisposableStore();
  let selector: vscode.DocumentSelector = buildSelector();

  const registerProviders = (): void => {
    providerStore.dispose();
    providerStore = new DisposableStore();
    selector = buildSelector();
    const config = vscode.workspace.getConfiguration("amx.editor");
    if (config.get<boolean>("hover.enabled", true)) {
      providerStore.add(
        vscode.languages.registerHoverProvider(selector, createHoverProvider(state, resolver)),
      );
    }
    if (config.get<boolean>("completion.enabled", true)) {
      providerStore.add(
        vscode.languages.registerCompletionItemProvider(
          selector,
          createCompletionProvider(state, resolver),
          ".",
        ),
      );
    }
    if (config.get<boolean>("codeLens.enabled", true)) {
      const lensProvider = providerStore.add(new AmxCodeLensProvider(state, resolver, catalog));
      providerStore.add(vscode.languages.registerCodeLensProvider(selector, lensProvider));
    }
    if (config.get<boolean>("diagnostics.enabled", false)) {
      providerStore.add(new AmxDiagnostics(state, resolver, selector, catalog));
    }
  };

  const warmFor = (document: vscode.TextDocument): void => {
    if (vscode.languages.match(selector, document) === 0) return;
    resolver.ensureWarm().catch((error: unknown) => {
      log(`catalog warm-up for language features failed: ${String(error)}`);
    });
  };

  registerProviders();
  for (const editor of vscode.window.visibleTextEditors) warmFor(editor.document);

  context.subscriptions.push(
    resolver,
    state,
    { dispose: () => providerStore.dispose() },
    registerGenerateDescription(services),
    registerOpenAsset(),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("amx.editor")) registerProviders();
    }),
    vscode.workspace.onDidOpenTextDocument(warmFor),
  );
}

/** amx.editor.languages × the file/untitled schemes. */
function buildSelector(): vscode.DocumentSelector {
  const languages = vscode.workspace
    .getConfiguration("amx.editor")
    .get<string[]>("languages", ["sql"]);
  return languages.flatMap((language) => [
    { language, scheme: "file" },
    { language, scheme: "untitled" },
  ]);
}
