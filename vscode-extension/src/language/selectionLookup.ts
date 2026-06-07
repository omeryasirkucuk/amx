// Selection-driven catalog lookup: the user selects text in ANY file
// (a table name inside spark.sql("""…"""), a YAML value, a log line),
// an AMX affordance appears — lightbulb code action + editor context
// menu — and the selection is resolved against the catalog. Local
// resolution via the multi-profile resolver index first, server-side
// hybrid search as the fallback; matches open the Studio asset panel.
import * as vscode from "vscode";

import type { CatalogSearchHit } from "../api/types";
import { guardValue } from "../management/errors";
import type { ExtensionServices } from "../services";
import { log } from "../util/log";
import type { CatalogResolver } from "./resolver";
import { resolveSelectionLocally, type SelectionMatch } from "./selectionResolve";

const MAX_SELECTION_CHARS = 500;
const COMMAND_ID = "amx.searchSelection";

export function registerSelectionLookup(
  services: ExtensionServices,
  resolver: CatalogResolver,
): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand(COMMAND_ID, () => searchSelection(services, resolver)),
    vscode.languages.registerCodeActionsProvider(
      [{ scheme: "file" }, { scheme: "untitled" }],
      new SelectionLookupActionProvider(),
      { providedCodeActionKinds: [vscode.CodeActionKind.QuickFix] },
    ),
  );
}

/** The "AMX marker": offer the lookup action for non-empty selections. */
class SelectionLookupActionProvider implements vscode.CodeActionProvider {
  provideCodeActions(
    document: vscode.TextDocument,
    range: vscode.Range | vscode.Selection,
  ): vscode.CodeAction[] {
    const enabled = vscode.workspace
      .getConfiguration("amx.editor")
      .get<boolean>("selectionLookup.enabled", true);
    if (!enabled || range.isEmpty) return [];
    const text = document.getText(range).trim();
    if (!text || text.length > MAX_SELECTION_CHARS) return [];
    const action = new vscode.CodeAction(
      "AMX: Search Selection in Catalog",
      vscode.CodeActionKind.QuickFix,
    );
    action.command = { command: COMMAND_ID, title: "AMX: Search Selection in Catalog" };
    return [action];
  }
}

async function searchSelection(
  services: ExtensionServices,
  resolver: CatalogResolver,
): Promise<void> {
  const editor = vscode.window.activeTextEditor;
  const text = editor?.document.getText(editor.selection).trim() ?? "";
  if (!text) {
    void vscode.window.showInformationMessage("AMX: select some text first.");
    return;
  }
  if (text.length > MAX_SELECTION_CHARS) {
    void vscode.window.showWarningMessage(
      `AMX: selection is too long for a catalog lookup (max ${MAX_SELECTION_CHARS} characters).`,
    );
    return;
  }

  // Make sure every profile's tables are indexed before resolving —
  // the selection may reference an asset on a non-active profile.
  const profiles = await guardValue("list DB profiles", () =>
    services.client.profiles.listDb(),
  );
  if (profiles === undefined) return;
  await resolver.ensureWarmAll(profiles.map((profile) => profile.name)).catch((error: unknown) => {
    log(`selection lookup warm-up failed: ${String(error)}`);
  });

  let matches = resolveSelectionLocally(text, resolver);
  if (matches.length === 0) {
    const remote = await guardValue("search the catalog", () =>
      remoteSearch(services, text),
    );
    if (remote === undefined) return;
    matches = remote;
  }

  if (matches.length === 0) {
    void vscode.window.showInformationMessage(`AMX: no catalog match for '${truncate(text, 60)}'.`);
    return;
  }
  const chosen = matches.length === 1 ? matches[0]! : await pickMatch(matches);
  if (!chosen) return;
  await vscode.commands.executeCommand("amx.panel.open", "table", {
    schema: chosen.schema,
    table: chosen.table,
    ...(chosen.profile ? { profile: chosen.profile } : {}),
    ...(chosen.database ? { database: chosen.database } : {}),
  });
}

/** Server-side hybrid search fallback (tables + columns). */
async function remoteSearch(
  services: ExtensionServices,
  text: string,
): Promise<SelectionMatch[]> {
  const query = text.replace(/\s+/g, " ").slice(0, 120);
  const [tables, columns] = await Promise.all([
    services.client.catalog.searchTables(query).catch(() => [] as CatalogSearchHit[]),
    services.client.catalog.searchColumns(query).catch(() => [] as CatalogSearchHit[]),
  ]);
  const seen = new Set<string>();
  const matches: SelectionMatch[] = [];
  for (const hit of [...tables, ...columns]) {
    const match: SelectionMatch = { schema: hit.schema_name, table: hit.table_name };
    if (hit.db_profile) match.profile = hit.db_profile;
    if (hit.database_name) match.database = hit.database_name;
    if (hit.column_name) match.column = hit.column_name;
    if (hit.effective_description) match.description = hit.effective_description;
    const key = `${match.profile ?? ""}|${match.schema}|${match.table}|${match.column ?? ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    matches.push(match);
  }
  return matches;
}

async function pickMatch(matches: SelectionMatch[]): Promise<SelectionMatch | undefined> {
  const pick = await vscode.window.showQuickPick(
    matches.map((match) => ({
      label: [match.schema, match.table, match.column].filter(Boolean).join("."),
      description: match.profile ?? "",
      detail: match.description ?? "",
      match,
    })),
    { title: "AMX: catalog matches", placeHolder: "Open in Studio" },
  );
  return pick?.match;
}

function truncate(text: string, maxChars: number): string {
  return text.length <= maxChars ? text : `${text.slice(0, maxChars - 1)}…`;
}
