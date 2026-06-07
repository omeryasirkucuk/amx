// Reader for the AMX Studio discovery file (<config-dir>/studio.json),
// written by amx/web/discovery.py on the Python side. The extension
// only ever reads it — both extension-owned and REPL-owned servers
// write/clear their own record through the server subprocess.
import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";

export interface StudioDiscovery {
  port: number;
  token: string;
  pid: number;
  startedAt: string;
  owner: string;
  version: string;
}

/**
 * Resolve the AMX config dir the same way amx.config does:
 * $AMX_CONFIG_DIR when set and non-empty, otherwise ~/.amx.
 */
export function amxConfigDir(env: NodeJS.ProcessEnv = process.env): string {
  const override = (env["AMX_CONFIG_DIR"] ?? "").trim();
  if (override) {
    return override.startsWith("~")
      ? join(homedir(), override.slice(1))
      : override;
  }
  return join(homedir(), ".amx");
}

export function discoveryFilePath(env: NodeJS.ProcessEnv = process.env): string {
  return join(amxConfigDir(env), "studio.json");
}

/**
 * Read the discovery record. Returns undefined for every non-usable
 * state — missing file, malformed JSON, missing required fields —
 * mirroring the tolerant Python reader. Callers must still
 * health-check the endpoint before trusting the record.
 */
export async function readDiscovery(
  env: NodeJS.ProcessEnv = process.env,
): Promise<StudioDiscovery | undefined> {
  let raw: unknown;
  try {
    raw = JSON.parse(await readFile(discoveryFilePath(env), "utf-8"));
  } catch {
    return undefined;
  }
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return undefined;
  const record = raw as Record<string, unknown>;
  const port = Number(record["port"]);
  const pid = Number(record["pid"]);
  const token = record["token"];
  if (!Number.isInteger(port) || !Number.isInteger(pid) || typeof token !== "string" || !token) {
    return undefined;
  }
  return {
    port,
    token,
    pid,
    startedAt: String(record["started_at"] ?? ""),
    owner: String(record["owner"] ?? ""),
    version: String(record["version"] ?? ""),
  };
}
