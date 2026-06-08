// "AMX Studio" view: a fixed list of one-click entry points to the
// Studio panels (Ask, New Run, Lineage, Pages, Settings). It exists so
// the panels are discoverable from the sidebar instead of only through
// the Command Palette. Each row just runs the matching open-panel
// command; there is no server/state coupling, so the list is static.
import * as vscode from "vscode";

interface StudioAction {
  label: string;
  command: string;
  icon: string;
  tooltip: string;
}

const ACTIONS: readonly StudioAction[] = [
  { label: "Ask", command: "amx.openAsk", icon: "comment-discussion", tooltip: "Open the Ask chat" },
  { label: "New Run", command: "amx.newRun", icon: "play", tooltip: "Start a new run" },
  { label: "Lineage", command: "amx.openLineage", icon: "type-hierarchy", tooltip: "Open the lineage canvas" },
  { label: "Pages", command: "amx.openPages", icon: "book", tooltip: "Open documentation pages" },
  { label: "Settings", command: "amx.openSettings", icon: "settings-gear", tooltip: "Open Studio settings" },
];

export class StudioActionsProvider implements vscode.TreeDataProvider<StudioAction> {
  getTreeItem(action: StudioAction): vscode.TreeItem {
    const item = new vscode.TreeItem(action.label, vscode.TreeItemCollapsibleState.None);
    item.iconPath = new vscode.ThemeIcon(action.icon);
    item.tooltip = action.tooltip;
    item.contextValue = "amx.studioAction";
    item.command = { command: action.command, title: action.tooltip };
    return item;
  }

  getChildren(element?: StudioAction): StudioAction[] {
    // Flat list — only the root level has items.
    return element ? [] : [...ACTIONS];
  }
}
