// Hover provider: shows catalog descriptions for resolved table and
// column references, with command links into the generate flow and
// the Studio asset panel. Resolution is synchronous against warmed
// catalog data, so hovering never blocks on HTTP.
import * as vscode from "vscode";

import type { TableMeta } from "../../api/catalogCache";
import type { DocumentStateStore } from "../documentState";
import type { CatalogResolver } from "../resolver";
import { tokenAt } from "../scanner";
import type { ResolvedColumnRef, ResolvedRef, ResolvedTableRef, SqlToken } from "../types";

const ENABLED_COMMANDS = ["amx.generateDescription", "amx.openAsset"];
const MAX_AMBIGUOUS_CANDIDATES = 5;

interface AssetArgs {
  schema: string;
  table: string;
  column?: string;
  profile?: string;
}

/** Markdown command link carrying JSON-encoded arguments. */
export function commandLink(title: string, command: string, args: AssetArgs): string {
  return `[${title}](command:${command}?${encodeURIComponent(JSON.stringify([args]))})`;
}

/** Build the {schema, table, column?, profile?} command payload. */
export function assetArgs(table: TableMeta, column?: string): AssetArgs {
  const args: AssetArgs = { schema: table.schema, table: table.name };
  if (column !== undefined) args.column = column;
  if (table.profile !== undefined) args.profile = table.profile;
  return args;
}

export function createHoverProvider(
  state: DocumentStateStore,
  resolver: CatalogResolver,
): vscode.HoverProvider {
  return {
    provideHover(document, position): vscode.Hover | undefined {
      const scan = state.getScan(document);
      const token = tokenAt(scan, document.offsetAt(position));
      if (!token) return undefined;
      const resolved = resolver.resolve(scan, token);
      if (!resolved) return undefined;
      const markdown = new vscode.MarkdownString(renderHover(resolved));
      markdown.isTrusted = { enabledCommands: ENABLED_COMMANDS };
      return new vscode.Hover(markdown, tokenRange(document, token));
    },
  };
}

function tokenRange(document: vscode.TextDocument, token: SqlToken): vscode.Range | undefined {
  const first = token.parts[0];
  const last = token.parts[token.parts.length - 1];
  if (!first || !last) return undefined;
  return new vscode.Range(document.positionAt(first.start), document.positionAt(last.end));
}

function renderHover(resolved: ResolvedRef): string {
  if (resolved.confidence === "ambiguous") return renderAmbiguous(resolved);
  return resolved.kind === "table" ? renderTable(resolved) : renderColumn(resolved);
}

function renderTable(resolved: ResolvedTableRef): string {
  const table = resolved.table;
  const lines = [`**${table.schema}.${table.name}**`];
  const facts: string[] = [];
  if (table.assetKind) facts.push(table.assetKind);
  if (typeof table.rowCount === "number") facts.push(`${table.rowCount.toLocaleString("en-US")} rows`);
  if (typeof table.columnCount === "number") facts.push(`${table.columnCount} columns`);
  if (facts.length > 0) lines.push(facts.join(" · "));
  lines.push(descriptionLine(table.description, assetArgs(table)));
  lines.push(commandLink("Open in AMX Studio", "amx.openAsset", assetArgs(table)));
  return lines.join("\n\n");
}

function renderColumn(resolved: ResolvedColumnRef): string {
  const { table, column } = resolved;
  const header = column.dataType
    ? `**${table.schema}.${table.name}.${column.name}** · ${column.dataType.toUpperCase()}`
    : `**${table.schema}.${table.name}.${column.name}**`;
  return [
    header,
    descriptionLine(column.description, assetArgs(table, column.name)),
    commandLink("Open in AMX Studio", "amx.openAsset", assetArgs(table)),
  ].join("\n\n");
}

function renderAmbiguous(resolved: ResolvedRef): string {
  if (resolved.kind === "table") {
    const lines = resolved.candidates
      .slice(0, MAX_AMBIGUOUS_CANDIDATES)
      .map((candidate) => `- found in \`${candidate.schema}.${candidate.name}\``);
    return [`**${resolved.table.name}** — ambiguous reference`, lines.join("\n")].join("\n\n");
  }
  const lines = resolved.candidates
    .slice(0, MAX_AMBIGUOUS_CANDIDATES)
    .map((candidate) => `- found in \`${candidate.table.schema}.${candidate.table.name}\``);
  return [`**${resolved.column.name}** — ambiguous reference`, lines.join("\n")].join("\n\n");
}

function descriptionLine(description: string | undefined, args: AssetArgs): string {
  if (description && description.trim().length > 0) return description;
  return `_No description_ — ${commandLink("Generate", "amx.generateDescription", args)}`;
}
