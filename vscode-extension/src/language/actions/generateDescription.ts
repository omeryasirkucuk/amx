// amx.generateDescription command: run the LLM generate endpoint for
// a table or column, then let the user review the proposal — apply
// it to the local catalog, write it back to the database, edit it
// first, or discard it. Generation consumes tokens on the active
// LLM profile, so nothing is written without an explicit choice.
import * as vscode from "vscode";

import { AmxApiError } from "../../api/errors";
import type { ExtensionServices } from "../../services";
import { log } from "../../util/log";

export interface GenerateArgs {
  schema: string;
  table: string;
  column?: string;
  profile?: string;
}

const APPLY_CATALOG = "Apply to catalog";
const APPLY_DATABASE = "Apply to database";
const EDIT = "Edit…";
const DISCARD = "Discard";

export function registerGenerateDescription(services: ExtensionServices): vscode.Disposable {
  return vscode.commands.registerCommand("amx.generateDescription", async (args?: GenerateArgs) => {
    if (!args?.schema || !args.table) {
      void vscode.window.showErrorMessage("AMX: generate description needs a schema and table.");
      return;
    }
    const assetLabel = args.column
      ? `${args.schema}.${args.table}.${args.column}`
      : `${args.schema}.${args.table}`;
    try {
      const profile = await resolveProfile(services, args);
      if (!profile) {
        void vscode.window.showErrorMessage(
          `AMX: no database profile found for ${assetLabel}. Refresh the catalog and retry.`,
        );
        return;
      }
      const result = await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: `AMX: generating description for ${assetLabel}…`,
        },
        () =>
          args.column
            ? services.client.generate.column(args.schema, args.table, args.column, profile)
            : services.client.generate.table(args.schema, args.table, profile),
      );
      await reviewAndApply(services, args, profile, result.description, assetLabel);
    } catch (error) {
      showError(error, assetLabel);
    }
  });
}

/** Use the explicit profile or borrow it from the catalog entry. */
async function resolveProfile(
  services: ExtensionServices,
  args: GenerateArgs,
): Promise<string | undefined> {
  if (args.profile) return args.profile;
  const schema = args.schema.toLowerCase();
  const table = args.table.toLowerCase();
  const tables = await services.catalog.getTables();
  return tables.find(
    (candidate) =>
      candidate.schema.toLowerCase() === schema && candidate.name.toLowerCase() === table,
  )?.profile;
}

/** Review loop: apply, edit-then-apply, or discard the proposal. */
async function reviewAndApply(
  services: ExtensionServices,
  args: GenerateArgs,
  profile: string,
  initialDescription: string,
  assetLabel: string,
): Promise<void> {
  let description = initialDescription;
  for (;;) {
    const choice = await vscode.window.showInformationMessage(
      `AMX proposes for ${assetLabel}: "${description}"`,
      APPLY_CATALOG,
      APPLY_DATABASE,
      EDIT,
      DISCARD,
    );
    if (choice === undefined || choice === DISCARD) return;
    if (choice === EDIT) {
      const edited = await vscode.window.showInputBox({
        value: description,
        prompt: `Description for ${assetLabel}`,
        ignoreFocusOut: true,
      });
      if (edited === undefined) return;
      description = edited;
      continue;
    }
    await apply(services, args, profile, description, choice === APPLY_DATABASE);
    services.catalog.invalidate();
    void vscode.window.showInformationMessage(
      `AMX: description saved for ${assetLabel} (${choice === APPLY_DATABASE ? "database" : "catalog"}).`,
    );
    return;
  }
}

async function apply(
  services: ExtensionServices,
  args: GenerateArgs,
  profile: string,
  description: string,
  toDatabase: boolean,
): Promise<void> {
  if (toDatabase) {
    if (args.column) {
      await services.client.comments.setColumn(args.schema, args.table, args.column, description, profile);
    } else {
      await services.client.comments.setTable(args.schema, args.table, description, profile);
    }
    return;
  }
  const body: Record<string, unknown> = {
    profile,
    schema: args.schema,
    table: args.table,
    description,
  };
  if (args.column) body["column"] = args.column;
  await services.client.comments.setLocal(body);
}

function showError(error: unknown, assetLabel: string): void {
  if (error instanceof AmxApiError) {
    const hint = error.hint ? ` — ${error.hint}` : "";
    void vscode.window.showErrorMessage(`AMX: ${error.detail}${hint}`);
  } else {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: generate description failed for ${assetLabel}: ${message}`);
  }
  log(`generateDescription failed for ${assetLabel}: ${String(error)}`);
}
