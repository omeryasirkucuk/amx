// Cross-platform spawn/kill for the Studio server subprocess.
// Pure argv construction is exported separately so unit tests can
// assert the exact command line per platform without spawning.
import { type ChildProcess, execFile, spawn } from "node:child_process";

export interface SpawnSpec {
  command: string;
  args: string[];
}

/**
 * Build the argv for an extension-owned Studio server.
 * Runs `python -m amx.web._studio_subprocess` directly — the actual
 * server entry, parent-agnostic, no browser-open side effects.
 */
export function buildServerSpawnSpec(options: {
  pythonPath: string;
  port: number;
  token: string;
  configPath?: string;
}): SpawnSpec {
  const args = [
    "-m",
    "amx.web._studio_subprocess",
    "--port",
    String(options.port),
    "--token",
    options.token,
    "--embedded",
    "--owner",
    "vscode",
  ];
  if (options.configPath) {
    args.push("--config-path", options.configPath);
  }
  return { command: options.pythonPath, args };
}

export function spawnServer(spec: SpawnSpec): ChildProcess {
  return spawn(spec.command, spec.args, {
    stdio: ["ignore", "pipe", "pipe"],
    detached: false,
    windowsHide: true,
  });
}

/**
 * Stop an owned server, escalating: polite signal first, then a hard
 * kill after `graceMs`. On Windows `SIGINT` is not deliverable to a
 * detached-stdin child, so go through taskkill with /T to take the
 * process tree down.
 */
export async function killServer(proc: ChildProcess, graceMs = 2000): Promise<void> {
  if (proc.exitCode !== null || proc.signalCode !== null) return;
  const exited = new Promise<void>((resolve) => {
    proc.once("exit", () => resolve());
  });
  if (process.platform === "win32") {
    proc.kill();
    const fallback = setTimeout(() => {
      if (proc.pid !== undefined && proc.exitCode === null) {
        execFile("taskkill", ["/pid", String(proc.pid), "/T", "/F"], () => {
          /* best effort */
        });
      }
    }, graceMs);
    await exited;
    clearTimeout(fallback);
    return;
  }
  proc.kill("SIGINT");
  const escalate = setTimeout(() => {
    if (proc.exitCode === null) proc.kill("SIGTERM");
  }, graceMs);
  const lastResort = setTimeout(() => {
    if (proc.exitCode === null) proc.kill("SIGKILL");
  }, graceMs * 2);
  await exited;
  clearTimeout(escalate);
  clearTimeout(lastResort);
}
