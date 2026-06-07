// Tree provider for the "History" view: a flat, status-decorated
// list of recent runs paged in batches of 50, with a "Load more…"
// leaf that appends the next page. Clicking a run opens the Studio
// run-detail panel via the amx.openRun command.
import * as vscode from "vscode";

import type { RunSummary } from "../api/types";
import type { ExtensionServices } from "../services";

interface RunNode {
  type: "run";
  run: RunSummary;
}

interface LoadMoreNode {
  type: "loadMore";
}

interface PlaceholderNode {
  type: "placeholder";
  message: string;
  startServer: boolean;
}

export type HistoryNode = RunNode | LoadMoreNode | PlaceholderNode;

const PAGE_SIZE = 50;

export class HistoryTreeProvider implements vscode.TreeDataProvider<HistoryNode> {
  private readonly didChange = new vscode.EventEmitter<HistoryNode | undefined>();
  readonly onDidChangeTreeData = this.didChange.event;

  private runs: RunSummary[] = [];
  private hasMore = false;
  private loaded = false;

  constructor(private readonly services: ExtensionServices) {}

  refresh(): void {
    this.runs = [];
    this.hasMore = false;
    this.loaded = false;
    this.didChange.fire(undefined);
  }

  /** Append the next page (the "Load more…" leaf delegates here). */
  async loadMore(): Promise<void> {
    try {
      await this.fetchPage(this.runs.length);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      void vscode.window.showErrorMessage(`AMX: could not load more runs: ${message}`);
    }
    this.didChange.fire(undefined);
  }

  getTreeItem(node: HistoryNode): vscode.TreeItem {
    switch (node.type) {
      case "run":
        return runItem(node.run);
      case "loadMore":
        return loadMoreItem();
      case "placeholder":
        return placeholderItem(node);
    }
  }

  async getChildren(node?: HistoryNode): Promise<HistoryNode[]> {
    if (node) return [];
    if (!this.serverReachable()) {
      return [{ type: "placeholder", message: "Start AMX server…", startServer: true }];
    }
    if (!this.loaded) {
      try {
        await this.fetchPage(0);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return [
          { type: "placeholder", message: `Start AMX server… (${message})`, startServer: true },
        ];
      }
    }
    if (this.runs.length === 0) {
      return [{ type: "placeholder", message: "No runs recorded yet", startServer: false }];
    }
    const nodes: HistoryNode[] = this.runs.map((run) => ({ type: "run", run }) as RunNode);
    if (this.hasMore) nodes.push({ type: "loadMore" });
    return nodes;
  }

  private async fetchPage(offset: number): Promise<void> {
    const page = await this.services.client.history.runs(PAGE_SIZE, offset);
    if (offset === 0) this.runs = [];
    this.runs.push(...page.runs);
    this.hasMore = page.has_more ?? page.runs.length === PAGE_SIZE;
    this.loaded = true;
  }

  private serverReachable(): boolean {
    if (this.services.server.state.status === "running") return true;
    return vscode.workspace.getConfiguration("amx").get<boolean>("server.autoStart", true);
  }
}

function statusIcon(status: string | undefined): vscode.ThemeIcon {
  switch (status) {
    case "passed":
      return new vscode.ThemeIcon("check");
    case "failed":
      return new vscode.ThemeIcon("error");
    case "running":
      return new vscode.ThemeIcon("sync~spin");
    default:
      return new vscode.ThemeIcon("circle-outline");
  }
}

function runItem(run: RunSummary): vscode.TreeItem {
  const label = [run.command ?? run.kind ?? "run", run.scope].filter(Boolean).join(" ");
  const item = new vscode.TreeItem(label, vscode.TreeItemCollapsibleState.None);
  item.iconPath = statusIcon(run.status);
  item.contextValue = "amx.run";
  if (run.status) item.description = run.status;
  if (run.started_at) item.tooltip = `Started: ${run.started_at}`;
  item.command = {
    command: "amx.openRun",
    title: "Open Run",
    arguments: [{ runId: run.run_id }],
  };
  return item;
}

function loadMoreItem(): vscode.TreeItem {
  const item = new vscode.TreeItem("Load more…", vscode.TreeItemCollapsibleState.None);
  item.iconPath = new vscode.ThemeIcon("ellipsis");
  item.contextValue = "amx.historyLoadMore";
  item.command = { command: "amx.history.loadMore", title: "Load More Runs" };
  return item;
}

function placeholderItem(node: PlaceholderNode): vscode.TreeItem {
  const item = new vscode.TreeItem(node.message, vscode.TreeItemCollapsibleState.None);
  item.iconPath = new vscode.ThemeIcon(node.startServer ? "play" : "info");
  item.contextValue = "amx.historyPlaceholder";
  if (node.startServer) {
    item.command = { command: "amx.server.start", title: "Start AMX Server" };
  }
  return item;
}
