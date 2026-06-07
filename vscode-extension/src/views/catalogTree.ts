// Tree provider for the "Catalog" view: profile scope → schemas →
// tables → columns, fed by the CatalogCache so it shares TTL and
// invalidation with the language features. Clicking a table opens
// the Studio asset panel via the amx.openAsset command.
import * as vscode from "vscode";

import type { ColumnMeta, TableMeta } from "../api/catalogCache";
import type { ExtensionServices } from "../services";

interface ProfileScopeNode {
  type: "profileScope";
  /** undefined means the active profile. */
  profile?: string;
}

interface SchemaNode {
  type: "schema";
  profile?: string;
  schema: string;
}

interface TableNode {
  type: "table";
  meta: TableMeta;
}

interface ColumnNode {
  type: "column";
  meta: ColumnMeta;
}

interface PlaceholderNode {
  type: "placeholder";
  message: string;
  startServer: boolean;
}

export type CatalogNode = ProfileScopeNode | SchemaNode | TableNode | ColumnNode | PlaceholderNode;

const TOOLTIP_MAX_CHARS = 240;

export class CatalogTreeProvider implements vscode.TreeDataProvider<CatalogNode> {
  private readonly didChange = new vscode.EventEmitter<CatalogNode | undefined>();
  readonly onDidChangeTreeData = this.didChange.event;

  constructor(private readonly services: ExtensionServices) {
    services.context.subscriptions.push(
      services.catalog.onDidInvalidate(() => this.refresh()),
    );
  }

  refresh(): void {
    this.didChange.fire(undefined);
  }

  getTreeItem(node: CatalogNode): vscode.TreeItem {
    switch (node.type) {
      case "profileScope":
        return profileScopeItem(node);
      case "schema":
        return schemaItem(node);
      case "table":
        return tableItem(node.meta);
      case "column":
        return columnItem(node.meta);
      case "placeholder":
        return placeholderItem(node);
    }
  }

  async getChildren(node?: CatalogNode): Promise<CatalogNode[]> {
    try {
      if (!node) return await this.rootNodes();
      switch (node.type) {
        case "profileScope":
          return await this.schemaNodes(node.profile);
        case "schema":
          return await this.tableNodes(node);
        case "table":
          return await this.columnNodes(node.meta);
        default:
          return [];
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return [{ type: "placeholder", message: `Start AMX server… (${message})`, startServer: true }];
    }
  }

  private async rootNodes(): Promise<CatalogNode[]> {
    if (!this.serverReachable()) {
      return [{ type: "placeholder", message: "Start AMX server…", startServer: true }];
    }
    const tables = await this.services.catalog.getTables();
    if (tables.length === 0) {
      return [{ type: "placeholder", message: "No tables indexed yet", startServer: false }];
    }
    const profiles = distinct(tables.map((table) => table.profile));
    return profiles.map((profile) => {
      const node: ProfileScopeNode = { type: "profileScope" };
      if (profile !== undefined) node.profile = profile;
      return node;
    });
  }

  private async schemaNodes(profile: string | undefined): Promise<CatalogNode[]> {
    const tables = await this.tablesForProfile(profile);
    const schemas = [...new Set(tables.map((table) => table.schema))].sort();
    return schemas.map((schema) => {
      const node: SchemaNode = { type: "schema", schema };
      if (profile !== undefined) node.profile = profile;
      return node;
    });
  }

  private async tableNodes(node: SchemaNode): Promise<CatalogNode[]> {
    const tables = await this.tablesForProfile(node.profile);
    return tables
      .filter((table) => table.schema === node.schema)
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((meta) => ({ type: "table", meta }) as TableNode);
  }

  private async columnNodes(meta: TableMeta): Promise<CatalogNode[]> {
    const columns = await this.services.catalog.getColumns(meta.schema, meta.name, meta.profile);
    return columns.map((column) => ({ type: "column", meta: column }) as ColumnNode);
  }

  private async tablesForProfile(profile: string | undefined): Promise<readonly TableMeta[]> {
    const tables = await this.services.catalog.getTables();
    return tables.filter((table) => table.profile === profile);
  }

  private serverReachable(): boolean {
    if (this.services.server.state.status === "running") return true;
    return vscode.workspace.getConfiguration("amx").get<boolean>("server.autoStart", true);
  }
}

function distinct(values: readonly (string | undefined)[]): (string | undefined)[] {
  const seen = new Set<string | undefined>();
  const result: (string | undefined)[] = [];
  for (const value of values) {
    if (seen.has(value)) continue;
    seen.add(value);
    result.push(value);
  }
  return result;
}

function profileScopeItem(node: ProfileScopeNode): vscode.TreeItem {
  const item = new vscode.TreeItem(
    node.profile ?? "Active profile",
    vscode.TreeItemCollapsibleState.Expanded,
  );
  item.iconPath = new vscode.ThemeIcon("plug");
  item.contextValue = "amx.catalogProfile";
  return item;
}

function schemaItem(node: SchemaNode): vscode.TreeItem {
  const item = new vscode.TreeItem(node.schema, vscode.TreeItemCollapsibleState.Collapsed);
  item.iconPath = new vscode.ThemeIcon("symbol-namespace");
  item.contextValue = "amx.catalogSchema";
  return item;
}

function tableItem(meta: TableMeta): vscode.TreeItem {
  const item = new vscode.TreeItem(meta.name, vscode.TreeItemCollapsibleState.Collapsed);
  item.iconPath = new vscode.ThemeIcon(meta.assetKind === "view" ? "eye" : "table");
  item.contextValue = "amx.catalogTable";
  if (typeof meta.columnCount === "number") {
    item.description = `${meta.columnCount} columns`;
  }
  if (meta.description) item.tooltip = truncate(meta.description, TOOLTIP_MAX_CHARS);
  const args: { schema: string; table: string; profile?: string } = {
    schema: meta.schema,
    table: meta.name,
  };
  if (meta.profile !== undefined) args.profile = meta.profile;
  item.command = { command: "amx.openAsset", title: "Open Asset in Studio", arguments: [args] };
  return item;
}

function columnItem(meta: ColumnMeta): vscode.TreeItem {
  const item = new vscode.TreeItem(meta.name, vscode.TreeItemCollapsibleState.None);
  item.iconPath = new vscode.ThemeIcon("symbol-field");
  item.contextValue = "amx.catalogColumn";
  if (meta.dataType) item.description = meta.dataType;
  if (meta.description) item.tooltip = truncate(meta.description, TOOLTIP_MAX_CHARS);
  return item;
}

function placeholderItem(node: PlaceholderNode): vscode.TreeItem {
  const item = new vscode.TreeItem(node.message, vscode.TreeItemCollapsibleState.None);
  item.iconPath = new vscode.ThemeIcon(node.startServer ? "play" : "info");
  item.contextValue = "amx.catalogPlaceholder";
  if (node.startServer) {
    item.command = { command: "amx.server.start", title: "Start AMX Server" };
  }
  return item;
}

function truncate(text: string, maxChars: number): string {
  return text.length <= maxChars ? text : `${text.slice(0, maxChars - 1)}…`;
}
