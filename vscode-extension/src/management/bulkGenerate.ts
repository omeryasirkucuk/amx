// amx.catalog.generateMissing: bulk-generate descriptions for the
// undocumented tables (and optionally columns) under a schema or table
// node in the catalog tree. Generation runs client-side over the
// existing per-asset endpoints with a small concurrency pool, a
// cancellable progress notification, and a final review step before
// anything is written. Generation consumes tokens on the active LLM
// profile, so the wizard always shows the asset count up front and
// never writes without an explicit Apply choice. The pure logic
// (enumerate / generate / apply) lives in bulkGenerateCore.ts.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import { mapPool } from "../util/async";
import { refreshViews } from "../views";
import {
  applyOne,
  describeError,
  enumerateMissing,
  generateOne,
  type GenProposal,
  type GenTarget,
} from "./bulkGenerateCore";
import { catalogArgFromNode } from "./catalogNodeArg";

const COLUMN_SCOPE = "Tables and columns";
const TABLE_SCOPE = "Tables only";
const POOL_WIDTH = 3;

export function registerBulkGenerate(services: ExtensionServices): vscode.Disposable {
  return vscode.commands.registerCommand("amx.catalog.generateMissing", (element?: unknown) =>
    runBulkGenerate(services, catalogArgFromNode(element)),
  );
}

async function runBulkGenerate(
  services: ExtensionServices,
  node: ReturnType<typeof catalogArgFromNode>,
): Promise<void> {
  if (!node?.schema) {
    void vscode.window.showWarningMessage("AMX: select a catalog schema or table first.");
    return;
  }
  const scopeLabel = node.table ? `${node.schema}.${node.table}` : node.schema;

  const includeColumns = await pickScope(scopeLabel);
  if (includeColumns === undefined) return;

  const targets = await vscode.window.withProgress(
    { location: vscode.ProgressLocation.Window, title: "AMX: finding undocumented assets…" },
    () => enumerateMissing(services, node, includeColumns),
  );
  if (targets.length === 0) {
    void vscode.window.showInformationMessage(
      `AMX: every ${includeColumns ? "table and column" : "table"} under ${scopeLabel} already has a description.`,
    );
    return;
  }

  const selected = await pickTargets(targets);
  if (!selected || selected.length === 0) return;

  const proposals = await generateWithProgress(services, selected);
  const generated = proposals.filter((p) => p.description !== undefined);
  const failed = proposals.filter((p) => p.error !== undefined);
  if (generated.length === 0) {
    void vscode.window.showErrorMessage(
      `AMX: no descriptions were generated (${failed.length} failed). First error: ${failed[0]?.error ?? "unknown"}`,
    );
    return;
  }

  const approved = await reviewProposals(generated, failed);
  if (!approved || approved.length === 0) return;

  const toDatabase = await pickDestination();
  if (toDatabase === undefined) return;

  await applyWithProgress(services, approved, toDatabase, node.profile);
}

async function pickScope(scopeLabel: string): Promise<boolean | undefined> {
  const pick = await vscode.window.showQuickPick(
    [
      {
        label: TABLE_SCOPE,
        description: "Generate descriptions for undocumented tables only",
        columns: false,
      },
      {
        label: COLUMN_SCOPE,
        description: "Also generate for undocumented columns (more token usage)",
        columns: true,
      },
    ],
    { title: `AMX: generate missing descriptions under ${scopeLabel}`, ignoreFocusOut: true },
  );
  return pick?.columns;
}

interface TargetItem extends vscode.QuickPickItem {
  target: GenTarget;
}

async function pickTargets(targets: GenTarget[]): Promise<GenTarget[] | undefined> {
  const tableCount = targets.filter((t) => t.kind === "table").length;
  const columnCount = targets.length - tableCount;
  const items: TargetItem[] = targets.map((target) => ({
    label: target.label,
    description: target.kind,
    picked: true,
    target,
  }));
  const picked = await vscode.window.showQuickPick(items, {
    title: `AMX: ${tableCount} tables, ${columnCount} columns — generation consumes tokens on the active LLM`,
    placeHolder: "Select the assets to generate descriptions for",
    canPickMany: true,
    ignoreFocusOut: true,
  });
  return picked?.map((item) => item.target);
}

async function generateWithProgress(
  services: ExtensionServices,
  targets: GenTarget[],
): Promise<GenProposal[]> {
  return vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: "AMX: generating descriptions",
      cancellable: true,
    },
    async (progress, token) => {
      let done = 0;
      const step = 100 / targets.length;
      const results = await mapPool(
        targets,
        POOL_WIDTH,
        async (target) => {
          const proposal = await generateOne(services, target);
          done += 1;
          progress.report({
            increment: step,
            message: `${done}/${targets.length} — ${target.label}`,
          });
          return proposal;
        },
        () => token.isCancellationRequested,
      );
      // Cancelled slots stay undefined; keep whatever was generated so
      // the user can still review and apply the completed work.
      return results.filter((r): r is GenProposal => r !== undefined);
    },
  );
}

interface ProposalItem extends vscode.QuickPickItem {
  proposal: GenProposal;
}

async function reviewProposals(
  generated: GenProposal[],
  failed: GenProposal[],
): Promise<GenProposal[] | undefined> {
  const items: ProposalItem[] = [
    ...generated.map((proposal) => ({
      label: proposal.target.label,
      detail: proposal.description ?? "",
      picked: true,
      proposal,
    })),
    // Surface failures unchecked so the user sees what was skipped and
    // why, without being able to apply an empty description.
    ...failed.map((proposal) => ({
      label: `$(error) ${proposal.target.label}`,
      detail: `failed: ${proposal.error}`,
      picked: false,
      proposal,
    })),
  ];
  const picked = await vscode.window.showQuickPick(items, {
    title: `AMX: review ${generated.length} generated descriptions`,
    placeHolder: "Uncheck any you don't want, then confirm to choose where to save",
    canPickMany: true,
    ignoreFocusOut: true,
  });
  // Drop any failed rows the user may have left checked — nothing to apply.
  return picked?.map((item) => item.proposal).filter((p) => p.description !== undefined);
}

async function pickDestination(): Promise<boolean | undefined> {
  const where = await vscode.window.showQuickPick(
    [
      {
        label: "Apply to catalog",
        description: "Local override — never writes to the source database",
        toDatabase: false,
      },
      {
        label: "Apply to database",
        description: "COMMENT ON … against the source database",
        toDatabase: true,
      },
    ],
    { title: "AMX: where should the descriptions go?", ignoreFocusOut: true },
  );
  return where?.toDatabase;
}

async function applyWithProgress(
  services: ExtensionServices,
  approved: GenProposal[],
  toDatabase: boolean,
  invalidateProfile: string | undefined,
): Promise<void> {
  let applied = 0;
  const failures: string[] = [];
  await vscode.window.withProgress(
    { location: vscode.ProgressLocation.Notification, title: "AMX: saving descriptions" },
    async (progress) => {
      const step = 100 / approved.length;
      for (const proposal of approved) {
        try {
          await applyOne(services, proposal, toDatabase);
          applied += 1;
        } catch (error) {
          failures.push(`${proposal.target.label}: ${describeError(error)}`);
        }
        progress.report({ increment: step, message: proposal.target.label });
      }
    },
  );
  services.catalog.invalidate(invalidateProfile ? { profile: invalidateProfile } : undefined);
  refreshViews("catalog");

  const where = toDatabase ? "database" : "catalog";
  if (failures.length === 0) {
    void vscode.window.showInformationMessage(
      `AMX: saved ${applied} descriptions to the ${where}.`,
    );
  } else {
    void vscode.window.showWarningMessage(
      `AMX: saved ${applied} descriptions to the ${where}; ${failures.length} failed. First: ${failures[0]}`,
    );
  }
}
