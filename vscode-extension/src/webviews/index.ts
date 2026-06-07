// Registration entry for the Studio webview panels: the programmatic
// "amx.panel.open" command (not contributed in package.json — panels
// are opened by other extension surfaces, never the palette) and one
// WebviewPanelSerializer per area for restore across window reloads.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import { log } from "../util/log";
import { coercePanelArgs, isPanelArea, type PanelAreaArgs } from "./areas";
import { PanelManager } from "./panelManager";

export function registerWebviews(services: ExtensionServices): void {
  const manager = new PanelManager(services);
  services.context.subscriptions.push(
    manager,
    vscode.commands.registerCommand("amx.panel.open", async (area: unknown, args?: unknown) => {
      if (!isPanelArea(area)) {
        log(`amx.panel.open called with unknown area: ${String(area)}`);
        return;
      }
      let panelArgs = coercePanelArgs(args);
      if (area === "table") {
        panelArgs = await enrichTableArgs(services, panelArgs);
      }
      manager.open(area, panelArgs);
    }),
    // Programmatic-only readiness probe (integration suite + doctor
    // diagnostics): which open panels have a booted SPA inside.
    vscode.commands.registerCommand("amx.panel.readyAreas", () => manager.readyAreas()),
    ...manager.registerSerializers(),
  );
}

/**
 * The SPA's table-detail routes need a database (2-level backends,
 * `/db/...`) or a UC catalog (3-level backends, `/cat/...`) segment —
 * without one the deep link degrades to the profile browse page.
 * Callers don't always know the value (hybrid-search hits and legacy
 * catalog rows can carry a NULL database_name), and the catalog
 * stores the UC catalog in the same database field, so this both
 * fills the missing value from the inventory and routes it onto the
 * correct axis for the profile's backend. Best effort: any failure
 * falls back to the original args.
 */
async function enrichTableArgs(
  services: ExtensionServices,
  args: PanelAreaArgs,
): Promise<PanelAreaArgs> {
  if (!args.schema || !args.table || args.catalog) return args;
  try {
    let database = args.database;
    if (!database) {
      // Profile-wide fetch (usually already warm). Never pass a
      // schema filter here — the cache keys scopes on
      // profile|database only, and a filtered fetch would poison the
      // profile-wide entry the tree relies on.
      const scope: { profile?: string } = {};
      if (args.profile) scope.profile = args.profile;
      const tables = await services.catalog.getTables(scope);
      const wanted = args.table.toLowerCase();
      const schemaWanted = args.schema.toLowerCase();
      const row = tables.find(
        (table) =>
          table.name.toLowerCase() === wanted && table.schema.toLowerCase() === schemaWanted,
      );
      database = row?.database;
    }
    if (!database) return args;
    if (await profileUsesCatalogAxis(services, args.profile)) {
      const { database: _dropped, ...rest } = { ...args, database };
      return { ...rest, catalog: database };
    }
    return { ...args, database };
  } catch (error) {
    log(`table-args enrichment failed: ${String(error)}`);
    return args;
  }
}

/** True when the profile's backend is 3-level (Databricks UC,
 *  BigQuery): the SPA routes those through `/cat/:profile/:catalog`. */
async function profileUsesCatalogAxis(
  services: ExtensionServices,
  profile: string | undefined,
): Promise<boolean> {
  if (!profile) return false;
  const [profiles, backends] = await Promise.all([
    services.client.profiles.listDb(),
    services.client.profiles.listBackends(),
  ]);
  const backendId = profiles.find((entry) => entry.name === profile)?.backend;
  if (!backendId) return false;
  return backends.find((entry) => entry.id === backendId)?.supports_catalog === true;
}
