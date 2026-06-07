// Locate an existing `amx` CLI installation (PATH or common pipx /
// user-bin locations). The binary alone is enough to run the server:
// `amx studio --no-open --embedded --port N` starts it, and the
// bearer token comes back through the discovery file the server
// writes (see src/server/discoveryFile.ts).
import { execFile } from "node:child_process";
import { access } from "node:fs/promises";
import { promisify } from "node:util";

import { amxBinaryProbePaths } from "./discoverPython";

const execFileAsync = promisify(execFile);

export interface AmxCliCandidate {
  amxCliPath: string;
  amxVersion: string;
}

/** Parse `amx, version 0.18.0` (Click version_option) output. */
export function parseAmxVersion(output: string): string | undefined {
  const match = /version\s+(\S+)/i.exec(output.trim());
  return match?.[1];
}

async function probeBinary(amxPath: string): Promise<AmxCliCandidate | undefined> {
  try {
    const { stdout } = await execFileAsync(amxPath, ["--version"], { timeout: 20_000 });
    const version = parseAmxVersion(stdout);
    if (!version) return undefined;
    return { amxCliPath: amxPath, amxVersion: version };
  } catch {
    return undefined;
  }
}

/** Probe an explicit path, then PATH, then common install locations. */
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
