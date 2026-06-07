// Completion provider: columns after `alias.` / `table.` /
// `schema.table.`, tables after FROM/JOIN-style keywords, and warm
// in-statement columns for bare prefixes. Only the dotted-column
// path awaits a column fetch; everything else answers from the warm
// catalog so typing stays responsive.
import * as vscode from "vscode";

import type { ColumnMeta, TableMeta } from "../../api/catalogCache";
import type { DocumentStateStore } from "../documentState";
import type { CatalogResolver } from "../resolver";
import type { ScanResult, StatementInfo } from "../types";

const MAX_TABLE_ITEMS = 500;
const DOTTED_PREFIX = /((?:[A-Za-z_][A-Za-z0-9_$]*\.){1,3})[A-Za-z_]?[A-Za-z0-9_$]*$/;
const TABLE_KEYWORD_TAIL =
  /\b(?:from|join|into|update|using|table|merge\s+into)\s+(?:[A-Za-z_][A-Za-z0-9_$]*\.)*[A-Za-z_]?[A-Za-z0-9_$]*$/i;

export function createCompletionProvider(
  state: DocumentStateStore,
  resolver: CatalogResolver,
): vscode.CompletionItemProvider {
  return {
    async provideCompletionItems(document, position): Promise<vscode.CompletionItem[] | undefined> {
      const linePrefix = document.lineAt(position.line).text.slice(0, position.character);
      const scan = state.getScan(document);
      const offset = document.offsetAt(position);
      const statementIndex = scan.statements.findIndex(
        (statement) => offset >= statement.start && offset <= statement.end,
      );
      const statement = statementIndex >= 0 ? scan.statements[statementIndex] : undefined;

      const dotted = DOTTED_PREFIX.exec(linePrefix);
      if (dotted) {
        const segments = dotted[1]!.split(".").filter((segment) => segment.length > 0);
        return dottedColumnItems(resolver, segments, statement);
      }
      if (TABLE_KEYWORD_TAIL.test(linePrefix)) return tableItems(resolver);
      return bareColumnItems(resolver, scan, statementIndex);
    },
  };
}

/** Columns of the table the dotted prefix points at (async fetch). */
async function dottedColumnItems(
  resolver: CatalogResolver,
  segments: string[],
  statement: StatementInfo | undefined,
): Promise<vscode.CompletionItem[] | undefined> {
  const table = resolver.tableForChain(segments, statement);
  if (!table) return undefined;
  const columns = await resolver.columnsFor(table);
  return columns.map((column) => columnItem(column));
}

/** Every warm catalog table, qualified on insert when ambiguous. */
function tableItems(resolver: CatalogResolver): vscode.CompletionItem[] {
  return resolver
    .allTables()
    .slice(0, MAX_TABLE_ITEMS)
    .map((table) => tableItem(table, resolver.tablesNamed(table.name).length > 1));
}

/** Warm columns of tables referenced in the statement at the cursor. */
function bareColumnItems(
  resolver: CatalogResolver,
  scan: ScanResult,
  statementIndex: number,
): vscode.CompletionItem[] | undefined {
  if (statementIndex < 0) return undefined;
  const items: vscode.CompletionItem[] = [];
  for (const table of resolver.statementTables(scan, statementIndex)) {
    const warm = resolver.warmColumnsFor(table);
    if (!warm) continue;
    for (const column of warm) items.push(columnItem(column));
  }
  return items.length > 0 ? items : undefined;
}

function columnItem(column: ColumnMeta): vscode.CompletionItem {
  const item = new vscode.CompletionItem(column.name, vscode.CompletionItemKind.Field);
  if (column.dataType) item.detail = column.dataType;
  if (column.description) item.documentation = new vscode.MarkdownString(column.description);
  return item;
}

function tableItem(table: TableMeta, ambiguous: boolean): vscode.CompletionItem {
  const item = new vscode.CompletionItem(table.name, vscode.CompletionItemKind.Struct);
  item.detail = table.schema;
  if (table.description) item.documentation = new vscode.MarkdownString(table.description);
  item.insertText = ambiguous ? `${table.schema}.${table.name}` : table.name;
  return item;
}
