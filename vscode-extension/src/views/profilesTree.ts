// Tree provider for the "Profiles" view: four fixed group roots
// (Database, LLM, Docs, Code) whose children are fetched lazily on
// expand from the profiles API. When the Studio server is down (and
// auto-start is disabled) or a fetch fails, the tree shows a single
// placeholder item that starts the server instead of throwing.
import * as vscode from "vscode";

import type {
  DbProfileSummary,
  LlmProfileSummary,
  NamedProfileSummary,
} from "../api/types";
import type { ExtensionServices } from "../services";

type GroupKind = "db" | "llm" | "docs" | "code";

interface GroupNode {
  type: "group";
  kind: GroupKind;
  label: string;
}

interface ProfileNode {
  type: "profile";
  kind: GroupKind;
  name: string;
  detail?: string;
  active: boolean;
}

interface PlaceholderNode {
  type: "placeholder";
  message: string;
}

export type ProfilesNode = GroupNode | ProfileNode | PlaceholderNode;

const GROUPS: readonly GroupNode[] = [
  { type: "group", kind: "db", label: "Database" },
  { type: "group", kind: "llm", label: "LLM" },
  { type: "group", kind: "docs", label: "Docs" },
  { type: "group", kind: "code", label: "Code" },
];

const GROUP_ICONS: Record<GroupKind, string> = {
  db: "database",
  llm: "sparkle",
  docs: "book",
  code: "code",
};

const PROFILE_CONTEXT: Record<GroupKind, string> = {
  db: "amx.dbProfile",
  llm: "amx.llmProfile",
  docs: "amx.docsProfile",
  code: "amx.codeProfile",
};

export class ProfilesTreeProvider implements vscode.TreeDataProvider<ProfilesNode> {
  private readonly didChange = new vscode.EventEmitter<ProfilesNode | undefined>();
  readonly onDidChangeTreeData = this.didChange.event;

  constructor(private readonly services: ExtensionServices) {}

  refresh(): void {
    this.didChange.fire(undefined);
  }

  getTreeItem(node: ProfilesNode): vscode.TreeItem {
    if (node.type === "group") {
      const item = new vscode.TreeItem(node.label, vscode.TreeItemCollapsibleState.Collapsed);
      item.iconPath = new vscode.ThemeIcon(GROUP_ICONS[node.kind]);
      item.contextValue = `amx.profileGroup.${node.kind}`;
      return item;
    }
    if (node.type === "placeholder") {
      return startServerPlaceholder(node.message);
    }
    const item = new vscode.TreeItem(node.name, vscode.TreeItemCollapsibleState.None);
    if (node.detail) item.description = node.detail;
    item.iconPath = new vscode.ThemeIcon(node.active ? "circle-filled" : "circle-outline");
    item.contextValue = PROFILE_CONTEXT[node.kind] + (node.active ? ".active" : "");
    item.tooltip = node.active ? `${node.name} (active)` : node.name;
    return item;
  }

  async getChildren(node?: ProfilesNode): Promise<ProfilesNode[]> {
    if (!node) return [...GROUPS];
    if (node.type !== "group") return [];
    if (!this.serverReachable()) {
      return [{ type: "placeholder", message: "Start AMX server…" }];
    }
    try {
      return await this.fetchGroup(node.kind);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return [{ type: "placeholder", message: `Start AMX server… (${message})` }];
    }
  }

  /** Reachable means already running, or auto-start may bring it up. */
  private serverReachable(): boolean {
    if (this.services.server.state.status === "running") return true;
    return vscode.workspace.getConfiguration("amx").get<boolean>("server.autoStart", true);
  }

  private async fetchGroup(kind: GroupKind): Promise<ProfilesNode[]> {
    const { profiles } = this.services.client;
    switch (kind) {
      case "db":
        return (await profiles.listDb()).map((profile) => toDbNode(profile));
      case "llm":
        return (await profiles.listLlm()).map((profile) => toLlmNode(profile));
      case "docs":
        return (await profiles.listDocs()).map((profile) => toNamedNode(profile, "docs"));
      case "code":
        return (await profiles.listCode()).map((profile) => toNamedNode(profile, "code"));
    }
  }
}

function startServerPlaceholder(message: string): vscode.TreeItem {
  const item = new vscode.TreeItem(message, vscode.TreeItemCollapsibleState.None);
  item.iconPath = new vscode.ThemeIcon("play");
  item.contextValue = "amx.startServerPlaceholder";
  item.command = { command: "amx.server.start", title: "Start AMX Server" };
  return item;
}

function toDbNode(profile: DbProfileSummary): ProfileNode {
  const node: ProfileNode = {
    type: "profile",
    kind: "db",
    name: profile.name,
    active: profile.is_active,
  };
  const location = profile.database || profile.host;
  node.detail = location ? `${profile.backend} · ${location}` : profile.backend;
  return node;
}

function toLlmNode(profile: LlmProfileSummary): ProfileNode {
  const node: ProfileNode = {
    type: "profile",
    kind: "llm",
    name: profile.name,
    active: profile.is_active,
  };
  node.detail = profile.is_active
    ? `${profile.provider} · ${profile.model} · active`
    : `${profile.provider} · ${profile.model}`;
  return node;
}

function toNamedNode(profile: NamedProfileSummary, kind: GroupKind): ProfileNode {
  return {
    type: "profile",
    kind,
    name: profile.name,
    active: profile.is_active === true,
  };
}
