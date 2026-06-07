// Locate an existing `amx` CLI installation (PATH or common pipx /
// user-bin locations) and the interpreter behind it.
import { execFile } from "node:child_process";
import { access } from "node:fs/promises";
import { promisify } from "node:util";

import { amxBinaryProbePaths } from "./discoverPython";

const execFileAsync = promisify(execFile);

export interface AmxCliCandidate {
  amxCliPath: string;
  amxVersion: string;
  /** Interpreter running the CLI — used to spawn the server module. */
  pythonPath: string;
}

async function probeBinary(amxPath: string): Promise<AmxCliCandidate | undefined> {
  try {
    const { stdout } = await execFileAsync(amxPath, ["--version"], { timeout: 20_000 });
    const version = stdout.trim();
    if (!version) return undefined;
    // The console-script shebang interpreter is the environment where
    // the amx package lives; ask it directly.
    const { stdout: exe } = await execFileAsync(
      amxPath,
      ["--python-executable"],
      { timeout: 20_000 },
    ).catch(() => ({ stdout: "" }));
    return {
      amxCliPath: amxPath,
      amxVersion: version,
      pythonPath: exe.trim(),
    };
  } catch {
    return undefined;
  }
}

/**
 * Probe an explicit path, then PATH, then common install locations.
 * `pythonPath` may come back empty when the CLI predates the
 * `--python-executable` flag; the runtime manager falls back to a
 * system interpreter that can `import amx` in that case.
 */
export async function discoverAmxCli(explicitPath?: string): Promise<AmxCliCandidate | undefined> {
  const candidates: string[] = [];
  if (explicitPath) candidates.push(explicitPath);
  candidates.push("amx");
  for (const probePath of amxBinaryProbePaths()) {
    try {
      await access(probePath);
      candidates.push(probePath);
    } catch {
      // not present — skip
    }
  }
  for (const candidate of candidates) {
    const found = await probeBinary(candidate);
    if (found) return found;
  }
  return undefined;
}
