// CodeLens provider: above the first exact-resolved table reference
// of each statement it offers "AMX: open" / "describe" (both route
// into the Studio asset panel) and "generate description" when the
// catalog has no description yet. Lens count is capped per document.
import * as vscode from "vscode";

import type { CatalogCache, TableMeta } from "../../api/catalogCache";
import type { DocumentStateStore } from "../documentState";
import type { CatalogResolver } from "../resolver";
import type { ScanResult, SqlToken } from "../types";
import { assetArgs } from "./hoverProvider";

interface LensAnchor {
  token: SqlToken;
  table: TableMeta;
  hasDescription: boolean;
}

export class AmxCodeLensProvider implements vscode.CodeLensProvider, vscode.Disposable {
  private readonly changeEmitter = new vscode.EventEmitter<void>();
  private readonly invalidateSub: { dispose(): void };

  readonly onDidChangeCodeLenses = this.changeEmitter.event;

  constructor(
    private readonly state: DocumentStateStore,
    private readonly resolver: CatalogResolver,
    catalog: CatalogCache,
  ) {
    this.invalidateSub = catalog.onDidInvalidate(() => this.changeEmitter.fire());
  }

  dispose(): void {
    this.invalidateSub.dispose();
    this.changeEmitter.dispose();
  }

  provideCodeLenses(document: vscode.TextDocument): vscode.CodeLens[] {
    const maxLenses = vscode.workspace
      .getConfiguration("amx.editor")
      .get<number>("codeLens.maxPerDocument", 50);
    const scan = this.state.getScan(document);
    const lenses: vscode.CodeLens[] = [];
    for (let index = 0; index < scan.statements.length; index += 1) {
      if (lenses.length >= maxLenses) break;
      const anchor = this.firstExactTable(scan, index);
      if (!anchor) continue;
      const { token, table, hasDescription } = anchor;
      const start = token.parts[0]!.start;
      const line = document.lineAt(document.positionAt(start).line);
      const range = new vscode.Range(line.range.start, line.range.start);
      const args = assetArgs(table);
      lenses.push(
        new vscode.CodeLens(range, {
          title: "AMX: open",
          command: "amx.openAsset",
          arguments: [args],
        }),
        new vscode.CodeLens(range, {
          title: "describe",
          command: "amx.openAsset",
          arguments: [args],
        }),
      );
      if (!hasDescription) {
        lenses.push(
          new vscode.CodeLens(range, {
            title: "generate description",
            command: "amx.generateDescription",
            arguments: [args],
          }),
        );
      }
    }
    return lenses.slice(0, maxLenses);
  }

  private firstExactTable(scan: ScanResult, statementIndex: number): LensAnchor | undefined {
    for (const token of scan.tokens) {
      if (token.statementIndex !== statementIndex || token.context !== "tablePosition") continue;
      const resolved = this.resolver.resolve(scan, token);
      if (resolved?.kind === "table" && resolved.confidence === "exact") {
        return {
          token,
          table: resolved.table,
          hasDescription: Boolean(resolved.table.description?.trim()),
        };
      }
    }
    return undefined;
  }
}
