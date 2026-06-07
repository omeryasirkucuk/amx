// Resolves how the extension runs AMX: an interpreter that can
// `import amx`, an `amx` CLI binary, or an extension-managed venv.
// This is the only runtime module that talks to vscode (settings,
// consent prompts, progress UI); the probing/install logic lives in
// the pure sibling modules.
import * as vscode from "vscode";

import { log } from "../util/log";
import { discoverAmxCli } from "./discoverAmx";
import { discoverPython, pythonHasAmx } from "./discoverPython";
import { createManagedRuntime, existingManagedRuntime } from "./managedVenv";

export type RuntimeInfo =
  | {
      kind: "python-module" | "managed-venv";
      /** Interpreter that can `import amx` — spawns the server module. */
      pythonPath: string;
      amxCliPath?: string;
    }
  | {
      kind: "amx-cli";
      /** Server starts via `amx studio --no-open --embedded`. */
      amxCliPath: string;
      amxVersion: string;
    };

export class RuntimeManager implements vscode.Disposable {
  private cached: RuntimeInfo | undefined;
  private readonly didChange = new vscode.EventEmitter<RuntimeInfo | undefined>();
  readonly onDidChangeRuntime = this.didChange.event;

  constructor(private readonly globalStoragePath: string) {}

  /**
   * Resolve the runtime, cached after first success. Order:
   * settings overrides → managed venv from a previous session →
   * system interpreter with amx → amx binary → undefined.
   */
  async resolve(): Promise<RuntimeInfo | undefined> {
    if (this.cached) return this.cached;

    const config = vscode.workspace.getConfiguration("amx");
    const pythonOverride = config.get<string>("pythonPath", "").trim();
    const cliOverride = config.get<string>("cliPath", "").trim();

    if (pythonOverride) {
      if (await pythonHasAmx(pythonOverride)) {
        return this.setRuntime({ kind: "python-module", pythonPath: pythonOverride });
      }
      log(`amx.pythonPath is set but \`import amx\` fails under ${pythonOverride}`);
    }
    if (cliOverride) {
      const cli = await discoverAmxCli(cliOverride);
      if (cli) {
        return this.setRuntime({
          kind: "amx-cli",
          amxCliPath: cli.amxCliPath,
          amxVersion: cli.amxVersion,
        });
      }
      log(`amx.cliPath is set but ${cliOverride} did not answer --version`);
    }

    const managed = await existingManagedRuntime(this.globalStoragePath);
    if (managed) {
      return this.setRuntime({ kind: "managed-venv", pythonPath: managed });
    }

    const python = await discoverPython();
    if (python && (await pythonHasAmx(python.pythonPath))) {
      return this.setRuntime({ kind: "python-module", pythonPath: python.pythonPath });
    }

    const cli = await discoverAmxCli();
    if (cli) {
      return this.setRuntime({
        kind: "amx-cli",
        amxCliPath: cli.amxCliPath,
        amxVersion: cli.amxVersion,
      });
    }
    return undefined;
  }

  /**
   * Resolve, and when nothing is found offer the managed-venv
   * install (consent prompt + progress notification).
   */
  async ensureInstalled(): Promise<RuntimeInfo | undefined> {
    const resolved = await this.resolve();
    if (resolved) return resolved;

    const allowed = vscode.workspace
      .getConfiguration("amx")
      .get<boolean>("managedInstall.allow", true);
    if (!allowed) {
      void vscode.window.showErrorMessage(
        "AMX is not installed and managed install is disabled. " +
          "Install amx-cli (pip install amx-cli) or set amx.pythonPath / amx.cliPath.",
      );
      return undefined;
    }

    const python = await discoverPython();
    if (!python) {
      void vscode.window.showErrorMessage(
        "AMX needs Python 3.10+ but no suitable interpreter was found. " +
          "Install Python or set amx.pythonPath.",
      );
      return undefined;
    }

    const choice = await vscode.window.showInformationMessage(
      "AMX is not installed. Install amx-cli into an extension-managed " +
        `virtual environment (using ${python.version})?`,
      "Install",
      "Cancel",
    );
    if (choice !== "Install") return undefined;

    try {
      const venvPython = await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: "AMX: installing amx-cli",
          cancellable: false,
        },
        (progress) =>
          createManagedRuntime(this.globalStoragePath, python.pythonPath, (message) =>
            progress.report({ message }),
          ),
      );
      void vscode.window.showInformationMessage("AMX installed and ready.");
      return this.setRuntime({ kind: "managed-venv", pythonPath: venvPython });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log(`managed install failed: ${message}`);
      void vscode.window.showErrorMessage(`AMX install failed: ${message}`);
      return undefined;
    }
  }

  /** Forget the cached runtime (settings changed, manual reselect). */
  invalidate(): void {
    this.cached = undefined;
    this.didChange.fire(undefined);
  }

  private setRuntime(info: RuntimeInfo): RuntimeInfo {
    this.cached = info;
    log(`runtime resolved: ${JSON.stringify({ ...info })}`);
    this.didChange.fire(info);
    return info;
  }

  dispose(): void {
    this.didChange.dispose();
  }
}
