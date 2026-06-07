// Documentation-coverage diagnostics: flags exact-resolved catalog
// references whose description is empty, with a quick fix that runs
// the generate flow. Recomputes on the debounced document-change
// event, on open, and across visible editors when the catalog
// invalidates; clears on close. Only instantiated while
// amx.editor.diagnostics.enabled is on.
import * as vscode from "vscode";

import type { CatalogCache } from "../../api/catalogCache";
import type { DocumentStateStore } from "../documentState";
import type { CatalogResolver } from "../resolver";
import { tokenAt } from "../scanner";
import { assetArgs } from "./hoverProvider";

const DIAGNOSTIC_CODE = "amx.missingDescription";

const SEVERITIES: Record<string, vscode.DiagnosticSeverity> = {
  hint: vscode.DiagnosticSeverity.Hint,
  info: vscode.DiagnosticSeverity.Information,
  warning: vscode.DiagnosticSeverity.Warning,
};

export class AmxDiagnostics implements vscode.Disposable, vscode.CodeActionProvider {
  static readonly actionKinds = [vscode.CodeActionKind.QuickFix];

  private readonly collection = vscode.languages.createDiagnosticCollection("amx");
  private readonly subscriptions: vscode.Disposable[];

  constructor(
    private readonly state: DocumentStateStore,
    private readonly resolver: CatalogResolver,
    private readonly selector: vscode.DocumentSelector,
    catalog: CatalogCache,
  ) {
    this.subscriptions = [
      state.onDidChangeScan((document) => this.refresh(document)),
      vscode.workspace.onDidOpenTextDocument((document) => this.refresh(document)),
      vscode.workspace.onDidCloseTextDocument((document) => this.collection.delete(document.uri)),
      catalog.onDidInvalidate(() => this.refreshVisible()),
      vscode.languages.registerCodeActionsProvider(selector, this, {
        providedCodeActionKinds: AmxDiagnostics.actionKinds,
      }),
    ];
    this.refreshVisible();
  }

  dispose(): void {
    for (const subscription of this.subscriptions) subscription.dispose();
    this.collection.dispose();
  }

  refreshVisible(): void {
    for (const editor of vscode.window.visibleTextEditors) this.refresh(editor.document);
  }

  refresh(document: vscode.TextDocument): void {
    if (vscode.languages.match(this.selector, document) === 0) return;
    const config = vscode.workspace.getConfiguration("amx.editor");
    const severity = SEVERITIES[config.get<string>("diagnostics.severity", "hint")] ??
      vscode.DiagnosticSeverity.Hint;
    const includeColumns = config.get<string>("diagnostics.scope", "tables") === "tablesAndColumns";
    const scan = this.state.getScan(document);
    const diagnostics: vscode.Diagnostic[] = [];
    for (const token of scan.tokens) {
      const isTableSlot = token.context === "tablePosition";
      if (!isTableSlot && !includeColumns) continue;
      if (token.context === "aliasDef" || token.context === "cteDef") continue;
      const resolved = this.resolver.resolve(scan, token);
      if (!resolved || resolved.confidence !== "exact") continue;
      const described =
        resolved.kind === "table"
          ? Boolean(resolved.table.description?.trim())
          : Boolean(resolved.column.description?.trim());
      if (described) continue;
      if (resolved.kind === "column" && !includeColumns) continue;
      const label =
        resolved.kind === "table"
          ? `${resolved.table.schema}.${resolved.table.name}`
          : `${resolved.table.schema}.${resolved.table.name}.${resolved.column.name}`;
      const first = token.parts[0]!;
      const last = token.parts[token.parts.length - 1]!;
      const range = new vscode.Range(
        document.positionAt(first.start),
        document.positionAt(last.end),
      );
      const diagnostic = new vscode.Diagnostic(
        range,
        `AMX: ${label} has no description`,
        severity,
      );
      diagnostic.source = "amx";
      diagnostic.code = DIAGNOSTIC_CODE;
      diagnostics.push(diagnostic);
    }
    this.collection.set(document.uri, diagnostics);
  }

  provideCodeActions(
    document: vscode.TextDocument,
    _range: vscode.Range,
    context: vscode.CodeActionContext,
  ): vscode.CodeAction[] {
    const actions: vscode.CodeAction[] = [];
    for (const diagnostic of context.diagnostics) {
      if (diagnostic.code !== DIAGNOSTIC_CODE) continue;
      const scan = this.state.getScan(document);
      const token = tokenAt(scan, document.offsetAt(diagnostic.range.start));
      if (!token) continue;
      const resolved = this.resolver.resolve(scan, token);
      if (!resolved || resolved.confidence !== "exact") continue;
      const args =
        resolved.kind === "table"
          ? assetArgs(resolved.table)
          : assetArgs(resolved.table, resolved.column.name);
      const action = new vscode.CodeAction(
        "Generate description with AMX",
        vscode.CodeActionKind.QuickFix,
      );
      action.command = {
        title: "Generate description with AMX",
        command: "amx.generateDescription",
        arguments: [args],
      };
      action.diagnostics = [diagnostic];
      actions.push(action);
    }
    return actions;
  }
}
