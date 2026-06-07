// Catalog management from the tree: sync / deep sync per profile,
// edit or generate descriptions on tables and columns, copy names.
// Description updates share the generate flow's catalog-local vs
// database-writeback choice.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import { refreshViews } from "../views";
import { catalogArgFromNode, type CatalogNodeArg } from "./catalogNodeArg";

// The freshness endpoint returns `{profiles: [...], stale_profile_count, syncing_profile_count}`.
// Each profile entry carries `{profile, state, ...}` where `state` is
// "syncing" while the sync is in progress and transitions to another value
// when complete. `db_profile` is kept as a defensive fallback in case an
// older server version uses the old key name. We also accept a plain
// top-level `state`/`status` as a defensive fallback in case the shape changes.
interface FreshnessProfile {
  profile?: string;
  db_profile?: string;
  state?: string;
  status?: string;
  [key: string]: unknown;
}

interface FreshnessResponse {
  profiles?: FreshnessProfile[];
  state?: string;
  status?: string;
  [key: string]: unknown;
}

const SYNC_POLL_MS = 2000;
const SYNC_TIMEOUT_MS = 600_000;

// --- registration ---

export function registerCatalogOps(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.catalog.sync", (element?: unknown) =>
      runSync(services, catalogArgFromNode(element)?.profile, false),
    ),
    vscode.commands.registerCommand("amx.catalog.deepSync", (element?: unknown) =>
      runSync(services, catalogArgFromNode(element)?.profile, true),
    ),
    vscode.commands.registerCommand("amx.catalog.editDescription", (element?: unknown) =>
      editDescription(services, catalogArgFromNode(element)),
    ),
    vscode.commands.registerCommand("amx.catalog.copyName", (element?: unknown) => {
      const node = catalogArgFromNode(element);
      if (!node?.schema || !node.table) return;
      const qualified = [node.schema, node.table, node.column].filter(Boolean).join(".");
      void vscode.env.clipboard.writeText(qualified);
      void vscode.window.setStatusBarMessage(`AMX: copied ${qualified}`, 3000);
    }),
    vscode.commands.registerCommand("amx.catalog.analyzeTable", (element?: unknown) =>
      vscode.commands.executeCommand("amx.runs.start", catalogArgFromNode(element)),
    ),
    // Bulk runs from container nodes: schema → whole-schema run (no
    // table picker, one modal confirm); database/profile → run wizard
    // starting at the schema step.
    vscode.commands.registerCommand("amx.catalog.analyzeSchema", (element?: unknown) => {
      const node = catalogArgFromNode(element);
      if (!node?.schema) return;
      void vscode.commands.executeCommand("amx.runs.start", { ...node, allTables: true });
    }),
    vscode.commands.registerCommand("amx.catalog.analyzeScope", (element?: unknown) =>
      vscode.commands.executeCommand("amx.runs.start", catalogArgFromNode(element)),
    ),
    vscode.commands.registerCommand("amx.catalog.generateDescription", (element?: unknown) => {
      const node = catalogArgFromNode(element);
      if (!node) return;
      void vscode.commands.executeCommand("amx.generateDescription", {
        schema: node.schema,
        table: node.table,
        column: node.column,
        profile: node.profile,
      });
    }),
  );
}

// --- helpers ---


// --- command implementations ---

async function runSync(
  services: ExtensionServices,
  profile: string | undefined,
  deep: boolean,
): Promise<void> {
  const label = deep ? "deep sync" : "sync";
  await vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: `AMX: catalog ${label}${profile ? ` (${profile})` : ""}`,
      cancellable: false,
    },
    async (progress) => {
      try {
        if (deep) await services.client.catalog.deepSync(profile);
        else await services.client.catalog.sync(profile);
        progress.report({ message: "sync running on the server…" });

        // The sync endpoint returns immediately; poll freshness until the
        // profile leaves its syncing state. The endpoint returns:
        //   {profiles: [{profile, state, ...}, ...], ...}
        // We match our profile (or any profile when none is specified) and
        // wait until none of them report a state containing "sync". The
        // defensive fallback also handles a hypothetical flat top-level
        // state/status key so the code stays correct if the shape changes.
        const deadline = Date.now() + SYNC_TIMEOUT_MS;
        let timedOut = false;
        pollLoop: while (Date.now() < deadline) {
          await new Promise<void>((resolve) => setTimeout(resolve, SYNC_POLL_MS));
          // If the sleep itself pushed us past the deadline, record timeout
          // and stop rather than making another API call.
          if (Date.now() >= deadline) {
            timedOut = true;
            break pollLoop;
          }
          const raw = (await services.client.catalog.freshness()) as FreshnessResponse;

          const isSyncing = isFreshnessActivelySyncing(raw, profile);
          if (!isSyncing) break pollLoop;
          progress.report({ message: "syncing…" });
        }
        // Loop condition `Date.now() < deadline` became false without a break.
        if (!timedOut && Date.now() >= deadline) timedOut = true;

        services.catalog.invalidate(profile ? { profile } : undefined);
        refreshViews("catalog");
        if (timedOut) {
          void vscode.window.showWarningMessage(
            `AMX: catalog ${label} is still running on the server after 10 minutes — check Studio for progress.`,
          );
        } else {
          void vscode.window.showInformationMessage(`AMX: catalog ${label} finished.`);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        void vscode.window.showErrorMessage(`AMX: catalog ${label} failed: ${message}`);
      }
    },
  );
}

/**
 * Return true when the freshness response indicates that the given profile
 * (or any profile when `targetProfile` is undefined) is still syncing.
 */
function isFreshnessActivelySyncing(
  response: FreshnessResponse,
  targetProfile: string | undefined,
): boolean {
  // Prefer the structured profiles array (actual server shape).
  const profileList = response["profiles"];
  if (Array.isArray(profileList) && profileList.length > 0) {
    const relevant: FreshnessProfile[] = targetProfile
      ? profileList.filter((p) => {
          const name = p["profile"] ?? p["db_profile"];
          return !name || name === targetProfile;
        })
      : profileList;
    return relevant.some((p) => {
      const stateStr = String(p["state"] ?? p["status"] ?? "").toLowerCase();
      return stateStr.includes("sync");
    });
  }

  // Defensive fallback: flat top-level state/status key.
  const stateStr = String(
    (response["state"] ?? response["status"] ?? "") as string,
  ).toLowerCase();
  // Empty or non-"sync" state → done.
  return stateStr.includes("sync");
}

async function editDescription(
  services: ExtensionServices,
  node?: CatalogNodeArg,
): Promise<void> {
  if (!node?.schema || !node.table || !node.profile) {
    void vscode.window.showWarningMessage("AMX: select a catalog table or column first.");
    return;
  }
  const assetLabel = [node.schema, node.table, node.column].filter(Boolean).join(".");
  const text = await vscode.window.showInputBox({
    title: `AMX: describe ${assetLabel}`,
    value: node.description ?? "",
    prompt: "Description text",
    ignoreFocusOut: true,
  });
  if (text === undefined || !text.trim()) return;
  const where = await vscode.window.showQuickPick(
    [
      {
        label: "Apply to catalog",
        description: "Local override — never writes to the source database",
        target: "catalog" as const,
      },
      {
        label: "Apply to database",
        description: "COMMENT ON … against the source database",
        target: "database" as const,
      },
    ],
    { title: "AMX: where should the description go?" },
  );
  if (!where) return;
  try {
    if (where.target === "catalog") {
      await services.client.comments.setLocal({
        profile: node.profile,
        schema: node.schema,
        table: node.table,
        ...(node.column ? { column: node.column } : {}),
        description: text.trim(),
      });
    } else if (node.column) {
      await services.client.comments.setColumn(
        node.schema,
        node.table,
        node.column,
        text.trim(),
        node.profile,
      );
    } else {
      await services.client.comments.setTable(node.schema, node.table, text.trim(), node.profile);
    }
    services.catalog.invalidate({ profile: node.profile });
    refreshViews("catalog");
    void vscode.window.showInformationMessage(`AMX: description saved for ${assetLabel}.`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not save description: ${message}`);
  }
}

export { catalogArgFromNode } from "./catalogNodeArg";
