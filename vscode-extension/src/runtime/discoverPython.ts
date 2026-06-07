// Locate a Python 3.10+ interpreter without importing vscode, so the
// probing logic stays unit-testable. The runtime manager layers
// settings overrides and user prompts on top.
import { execFile } from "node:child_process";
import { homedir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const MIN_MAJOR = 3;
const MIN_MINOR = 10;

export interface PythonCandidate {
  pythonPath: string;
  version: string;
}

/** Parse `Python 3.12.4` style output into [major, minor]. */
export function parsePythonVersion(output: string): [number, number] | undefined {
  const match = /Python\s+(\d+)\.(\d+)/.exec(output);
  if (!match) return undefined;
  return [Number(match[1]), Number(match[2])];
}

export function meetsMinimum(version: [number, number]): boolean {
  const [major, minor] = version;
  return major > MIN_MAJOR || (major === MIN_MAJOR && minor >= MIN_MINOR);
}

async function probe(command: string, args: string[] = []): Promise<PythonCandidate | undefined> {
  try {
    const { stdout, stderr } = await execFileAsync(command, [...args, "--version"], {
      timeout: 10_000,
    });
    const output = stdout.trim() || stderr.trim();
    const version = parsePythonVersion(output);
    if (!version || !meetsMinimum(version)) return undefined;
    // Resolve the concrete interpreter path when probing via the
    // Windows `py` launcher so later spawns don't depend on it.
    if (args.length > 0 || command === "py") {
      const { stdout: exe } = await execFileAsync(
        command,
        [...args, "-c", "import sys; print(sys.executable)"],
        { timeout: 10_000 },
      );
      return { pythonPath: exe.trim(), version: output };
    }
    return { pythonPath: command, version: output };
  } catch {
    return undefined;
  }
}

/**
 * Candidate locations in resolution order. PATH names first, then
 * the Windows launcher, then common install locations that often
 * miss PATH (Homebrew, Windows Store stubs are excluded — they fail
 * the probe anyway).
 */
export function defaultProbeOrder(platform: NodeJS.Platform = process.platform): Array<{
  command: string;
  args?: string[];
}> {
  const candidates: Array<{ command: string; args?: string[] }> = [
    { command: "python3" },
    { command: "python" },
  ];
  if (platform === "win32") {
    candidates.push({ command: "py", args: ["-3"] });
  } else {
    candidates.push(
      { command: "/opt/homebrew/bin/python3" },
      { command: "/usr/local/bin/python3" },
      { command: "/usr/bin/python3" },
    );
  }
  return candidates;
}

/** First interpreter satisfying the minimum version, or undefined. */
export async function discoverPython(
  explicitPath?: string,
  platform: NodeJS.Platform = process.platform,
): Promise<PythonCandidate | undefined> {
  if (explicitPath) {
    return probe(explicitPath);
  }
  for (const candidate of defaultProbeOrder(platform)) {
    const found = await probe(candidate.command, candidate.args ?? []);
    if (found) return found;
  }
  return undefined;
}

/** True when `import amx` works under the given interpreter. */
export async function pythonHasAmx(pythonPath: string): Promise<boolean> {
  try {
    await execFileAsync(pythonPath, ["-c", "import amx"], { timeout: 15_000 });
    return true;
  } catch {
    return false;
  }
}

export function managedVenvPython(venvDir: string, platform: NodeJS.Platform = process.platform): string {
  return platform === "win32"
    ? join(venvDir, "Scripts", "python.exe")
    : join(venvDir, "bin", "python");
}

/** Common pipx/venv locations where the `amx` binary may live. */
export function amxBinaryProbePaths(platform: NodeJS.Platform = process.platform): string[] {
  const home = homedir();
  if (platform === "win32") {
    return [join(home, ".local", "bin", "amx.exe"), join(home, "pipx", "venvs", "amx-cli", "Scripts", "amx.exe")];
  }
  return [
    join(home, ".local", "bin", "amx"),
    "/opt/homebrew/bin/amx",
    "/usr/local/bin/amx",
  ];
}
