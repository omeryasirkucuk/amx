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

interface DatabaseNode {
  type: "databaseScope";
  profile?: string;
  /** undefined groups legacy rows whose database_name was NULL. */
  database?: string;
  label: string;
}

interface SchemaNode {
  type: "schema";
  profile?: string;
  database?: string;
  schema: string;
}

interface TableNode {
  type: "table";
  meta: TableMeta;
}

interface ColumnNode {
  type: "column";
  meta: ColumnMeta;
  /** Owning table — forwarded to context-menu commands for schema/profile context. */
  table: TableMeta;
}

interface PlaceholderNode {
  type: "placeholder";
  message: string;
  startServer: boolean;
}

export type CatalogNode =
  | ProfileScopeNode
  | DatabaseNode
  | SchemaNode
  | TableNode
  | ColumnNode
  | PlaceholderNode;

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
      case "databaseScope":
        return databaseItem(node);
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
          return await this.databaseNodes(node.profile);
        case "databaseScope":
          return await this.schemaNodes(node.profile, node.database);
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
    // One root per configured DB profile — not just the active one.
    // Each root lazily pulls its own inventory on expand.
    const profiles = await this.services.client.profiles.listDb();
    if (profiles.length === 0) {
      return [{ type: "placeholder", message: "No DB profiles configured", startServer: false }];
    }
    return profiles.map((profile) => ({ type: "profileScope", profile: profile.name }));
  }

  /** Database level under a profile (Postgres databases, UC catalogs). */
  private async databaseNodes(profile: string | undefined): Promise<CatalogNode[]> {
    const scope = profile !== undefined ? { profile } : {};
    const databases = await this.services.client.catalog.databases(scope);
    if (databases.length === 0) {
      return [
        {
          type: "placeholder",
          message: "No databases indexed — run a catalog sync for this profile",
          startServer: false,
        },
      ];
    }
    return databases.map((database) => {
      const node: DatabaseNode = {
        type: "databaseScope",
        label: database === "" ? "(default)" : database,
      };
      if (profile !== undefined) node.profile = profile;
      // Empty string = legacy rows whose database_name was NULL; an
      // undefined database keeps downstream fetches unscoped.
      if (database !== "") node.database = database;
      return node;
    });
  }

  private async schemaNodes(
    profile: string | undefined,
    database: string | undefined,
  ): Promise<CatalogNode[]> {
    const tables = await this.tablesForScope(profile, database);
    if (tables.length === 0) {
      return [
        {
          type: "placeholder",
          message: "No tables indexed — run a catalog sync for this profile",
          startServer: false,
        },
      ];
    }
    const schemas = [...new Set(tables.map((table) => table.schema))].sort();
    return schemas.map((schema) => {
      const node: SchemaNode = { type: "schema", schema };
      if (profile !== undefined) node.profile = profile;
      if (database !== undefined) node.database = database;
      return node;
    });
  }

  private async tableNodes(node: SchemaNode): Promise<CatalogNode[]> {
    const tables = await this.tablesForScope(node.profile, node.database);
    return tables
      .filter((table) => table.schema === node.schema)
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((meta) => ({ type: "table", meta }) as TableNode);
  }

  private async columnNodes(meta: TableMeta): Promise<CatalogNode[]> {
    const columns = await this.services.catalog.getColumns(meta.schema, meta.name, meta.profile);
    return columns.map((column) => ({ type: "column", meta: column, table: meta }) as ColumnNode);
  }

  private async tablesForScope(
    profile: string | undefined,
    database: string | undefined,
  ): Promise<readonly TableMeta[]> {
    const scope: { profile?: string; database?: string } = {};
    if (profile !== undefined) scope.profile = profile;
    if (database !== undefined) scope.database = database;
    return this.services.catalog.getTables(scope);
  }

  private serverReachable(): boolean {
    if (this.services.server.state.status === "running") return true;
    return vscode.workspace.getConfiguration("amx").get<boolean>("server.autoStart", true);
  }
}

function profileScopeItem(node: ProfileScopeNode): vscode.TreeItem {
  const item = new vscode.TreeItem(
    node.profile ?? "Active profile",
    vscode.TreeItemCollapsibleState.Collapsed,
  );
  item.iconPath = new vscode.ThemeIcon("plug");
  item.contextValue = "amx.catalogProfile";
  return item;
}

function databaseItem(node: DatabaseNode): vscode.TreeItem {
  const item = new vscode.TreeItem(node.label, vscode.TreeItemCollapsibleState.Collapsed);
  item.iconPath = new vscode.ThemeIcon("database");
  item.contextValue = "amx.catalogDatabase";
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
  const args: { schema: string; table: string; profile?: string; database?: string } = {
    schema: meta.schema,
    table: meta.name,
  };
  if (meta.profile !== undefined) args.profile = meta.profile;
  // 3-level backends (Databricks UC, BigQuery) need the database /
  // catalog segment for the SPA's table-detail route.
  if (meta.database !== undefined) args.database = meta.database;
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
