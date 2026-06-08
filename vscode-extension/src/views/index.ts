// Registration entry for the Activity Bar trees and the status bar.
// Instantiates the four tree providers plus the status bar, wires
// them to server state changes, and exposes a small registry so the
// commands layer can target individual surfaces for refresh.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import { CatalogTreeProvider } from "./catalogTree";
import { HistoryTreeProvider } from "./historyTree";
import { ProfilesTreeProvider } from "./profilesTree";
import { SchedulesTreeProvider } from "./schedulesTree";
import { AmxStatusBar } from "./statusBar";
import { StudioActionsProvider } from "./studioActionsTree";

export interface ViewRegistry {
  readonly studio: StudioActionsProvider;
  readonly profiles: ProfilesTreeProvider;
  readonly catalog: CatalogTreeProvider;
  readonly history: HistoryTreeProvider;
  readonly schedules: SchedulesTreeProvider;
  readonly statusBar: AmxStatusBar;
}

export type RefreshTarget = "profiles" | "catalog" | "history" | "schedules" | "statusBar";

let registry: ViewRegistry | undefined;

/** Providers registered by registerViews — throws before activation. */
export function getViews(): ViewRegistry {
  if (!registry) throw new Error("AMX views are not registered yet");
  return registry;
}

/** Refresh the named surfaces, or every surface when none are given. */
export function refreshViews(...targets: RefreshTarget[]): void {
  const views = registry;
  if (!views) return;
  const wanted: readonly RefreshTarget[] =
    targets.length > 0 ? targets : ["profiles", "catalog", "history", "schedules", "statusBar"];
  for (const target of wanted) {
    switch (target) {
      case "profiles":
        views.profiles.refresh();
        break;
      case "catalog":
        views.catalog.refresh();
        break;
      case "history":
        views.history.refresh();
        break;
      case "schedules":
        views.schedules.refresh();
        break;
      case "statusBar":
        void views.statusBar.update();
        break;
    }
  }
}

export function registerViews(services: ExtensionServices): void {
  const views: ViewRegistry = {
    studio: new StudioActionsProvider(),
    profiles: new ProfilesTreeProvider(services),
    catalog: new CatalogTreeProvider(services),
    history: new HistoryTreeProvider(services),
    schedules: new SchedulesTreeProvider(services),
    statusBar: new AmxStatusBar(services),
  };
  registry = views;

  services.context.subscriptions.push(
    vscode.window.registerTreeDataProvider("amx.studio", views.studio),
    vscode.window.registerTreeDataProvider("amx.profiles", views.profiles),
    vscode.window.registerTreeDataProvider("amx.catalog", views.catalog),
    vscode.window.registerTreeDataProvider("amx.history", views.history),
    vscode.window.registerTreeDataProvider("amx.schedules", views.schedules),
    views.statusBar,
    // The catalog tree refreshes through CatalogCache invalidation
    // (wired in services.ts); the other trees track server state here.
    services.server.onDidChangeState(() =>
      refreshViews("profiles", "history", "schedules"),
    ),
    { dispose: () => (registry = undefined) },
  );
}
