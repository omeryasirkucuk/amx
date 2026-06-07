// Extension-managed virtual environment: created under the
// extension's global storage and populated with `pip install
// amx-cli`. Pure logic — consent prompts and progress UI live in the
// runtime manager.
import { execFile } from "node:child_process";
import { access } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";

import { managedVenvPython, pythonHasAmx } from "./discoverPython";

const execFileAsync = promisify(execFile);

export const VENV_DIRNAME = "amx-venv";

export function managedVenvDir(globalStoragePath: string): string {
  return join(globalStoragePath, VENV_DIRNAME);
}

/** Python inside the managed venv when it exists and has amx. */
export async function existingManagedRuntime(
  globalStoragePath: string,
): Promise<string | undefined> {
  const python = managedVenvPython(managedVenvDir(globalStoragePath));
  try {
    await access(python);
  } catch {
    return undefined;
  }
  return (await pythonHasAmx(python)) ? python : undefined;
}

export interface InstallProgress {
  (message: string): void;
}

/**
 * Create the venv (idempotent) and install/upgrade amx-cli into it.
 * Returns the venv's interpreter path. Throws with stderr context on
 * failure so the caller can surface an actionable error.
 */
export async function createManagedRuntime(
  globalStoragePath: string,
  basePython: string,
  onProgress: InstallProgress = () => {},
): Promise<string> {
  const venvDir = managedVenvDir(globalStoragePath);
  const venvPython = managedVenvPython(venvDir);

  onProgress("Creating virtual environment...");
  try {
    await execFileAsync(basePython, ["-m", "venv", "--upgrade-deps", venvDir], {
      timeout: 180_000,
    });
  } catch (error) {
    throw new Error(`venv creation failed: ${describeExecError(error)}`);
  }

  onProgress("Installing amx-cli (this can take a few minutes)...");
  try {
    await execFileAsync(
      venvPython,
      ["-m", "pip", "install", "--upgrade", "amx-cli"],
      { timeout: 900_000 },
    );
  } catch (error) {
    throw new Error(`pip install amx-cli failed: ${describeExecError(error)}`);
  }

  if (!(await pythonHasAmx(venvPython))) {
    throw new Error("amx-cli installed but `import amx` still fails in the managed venv");
  }
  return venvPython;
}

function describeExecError(error: unknown): string {
  if (error && typeof error === "object") {
    const err = error as { stderr?: string; message?: string };
    const stderr = (err.stderr ?? "").trim();
    if (stderr) return stderr.split("\n").slice(-5).join("\n");
    if (err.message) return err.message;
  }
  return String(error);
}
