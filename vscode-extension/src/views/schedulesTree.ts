// Tree provider for the "Schedules" view: one leaf per schedule with
// cron and next-run details. Clicking a schedule opens an action
// picker (amx.schedules.actions) offering pause / resume / run-now;
// the action commands themselves are registered in the commands
// layer since they need the API client and a tree refresh.
import * as vscode from "vscode";

import type { ScheduleSummary } from "../api/types";
import type { ExtensionServices } from "../services";

interface ScheduleNode {
  type: "schedule";
  schedule: ScheduleSummary;
}

interface PlaceholderNode {
  type: "placeholder";
  message: string;
  startServer: boolean;
}

export type SchedulesNode = ScheduleNode | PlaceholderNode;

export class SchedulesTreeProvider implements vscode.TreeDataProvider<SchedulesNode> {
  private readonly didChange = new vscode.EventEmitter<SchedulesNode | undefined>();
  readonly onDidChangeTreeData = this.didChange.event;

  constructor(private readonly services: ExtensionServices) {}

  refresh(): void {
    this.didChange.fire(undefined);
  }

  getTreeItem(node: SchedulesNode): vscode.TreeItem {
    if (node.type === "placeholder") return placeholderItem(node);
    return scheduleItem(node.schedule);
  }

  async getChildren(node?: SchedulesNode): Promise<SchedulesNode[]> {
    if (node) return [];
    if (!this.serverReachable()) {
      return [{ type: "placeholder", message: "Start AMX server…", startServer: true }];
    }
    try {
      const schedules = await this.services.client.schedules.list();
      if (schedules.length === 0) {
        return [{ type: "placeholder", message: "No schedules configured", startServer: false }];
      }
      return schedules.map((schedule) => ({ type: "schedule", schedule }) as ScheduleNode);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return [
        { type: "placeholder", message: `Start AMX server… (${message})`, startServer: true },
      ];
    }
  }

  private serverReachable(): boolean {
    if (this.services.server.state.status === "running") return true;
    return vscode.workspace.getConfiguration("amx").get<boolean>("server.autoStart", true);
  }
}

function isPaused(schedule: ScheduleSummary): boolean {
  return schedule.paused === true || schedule.enabled === false;
}

function scheduleItem(schedule: ScheduleSummary): vscode.TreeItem {
  const label = schedule.name ?? `Schedule ${schedule.id}`;
  const item = new vscode.TreeItem(label, vscode.TreeItemCollapsibleState.None);
  const paused = isPaused(schedule);
  item.iconPath = new vscode.ThemeIcon(paused ? "debug-pause" : "watch");
  item.contextValue = paused ? "amx.schedule.paused" : "amx.schedule";
  const detail: string[] = [];
  if (schedule.cron) detail.push(schedule.cron);
  if (paused) detail.push("paused");
  if (detail.length > 0) item.description = detail.join(" · ");
  if (schedule.next_run_at) item.tooltip = `Next run: ${schedule.next_run_at}`;
  item.command = {
    command: "amx.schedules.actions",
    title: "Schedule Actions",
    arguments: [schedule],
  };
  return item;
}

function placeholderItem(node: PlaceholderNode): vscode.TreeItem {
  const item = new vscode.TreeItem(node.message, vscode.TreeItemCollapsibleState.None);
  item.iconPath = new vscode.ThemeIcon(node.startServer ? "play" : "info");
  item.contextValue = "amx.schedulesPlaceholder";
  if (node.startServer) {
    item.command = { command: "amx.server.start", title: "Start AMX Server" };
  }
  return item;
}
