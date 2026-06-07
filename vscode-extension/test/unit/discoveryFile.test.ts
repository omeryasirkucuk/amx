// Discovery-file reader: config-dir resolution and tolerant parsing.
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { amxConfigDir, discoveryFilePath, readDiscovery } from "../../src/server/discoveryFile";

let dir: string;
let env: NodeJS.ProcessEnv;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "amx-disco-"));
  env = { AMX_CONFIG_DIR: dir };
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

describe("amxConfigDir", () => {
  it("honors AMX_CONFIG_DIR", () => {
    expect(amxConfigDir(env)).toBe(dir);
  });

  it("falls back to ~/.amx", () => {
    expect(amxConfigDir({})).toMatch(/[/\\]\.amx$/);
  });

  it("ignores empty override", () => {
    expect(amxConfigDir({ AMX_CONFIG_DIR: "  " })).toMatch(/[/\\]\.amx$/);
  });
});

describe("readDiscovery", () => {
  const write = (content: string) => writeFile(discoveryFilePath(env), content, "utf-8");

  it("reads a valid record", async () => {
    await write(
      JSON.stringify({
        port: 47821,
        token: "tok",
        pid: 123,
        started_at: "2026-06-07T00:00:00Z",
        owner: "cli",
        version: "0.18.0",
      }),
    );
    const record = await readDiscovery(env);
    expect(record).toEqual({
      port: 47821,
      token: "tok",
      pid: 123,
      startedAt: "2026-06-07T00:00:00Z",
      owner: "cli",
      version: "0.18.0",
    });
  });

  it("returns undefined when the file is missing", async () => {
    expect(await readDiscovery(env)).toBeUndefined();
  });

  it("returns undefined for malformed JSON", async () => {
    await write("{not json");
    expect(await readDiscovery(env)).toBeUndefined();
  });

  it("returns undefined for non-object payloads", async () => {
    await write("[1,2]");
    expect(await readDiscovery(env)).toBeUndefined();
  });

  it("returns undefined when required fields are missing", async () => {
    await write(JSON.stringify({ port: 1, pid: 2 }));
    expect(await readDiscovery(env)).toBeUndefined();
  });

  it("returns undefined for non-integer port", async () => {
    await write(JSON.stringify({ port: "abc", token: "t", pid: 1 }));
    expect(await readDiscovery(env)).toBeUndefined();
  });
});
