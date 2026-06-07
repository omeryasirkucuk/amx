// Spawn argv construction for both runtime kinds.
import { describe, expect, it } from "vitest";

import { buildCliSpawnSpec, buildServerSpawnSpec } from "../../src/server/spawn";

describe("buildServerSpawnSpec", () => {
  it("runs the server module with embedded mode and vscode owner", () => {
    const spec = buildServerSpawnSpec({
      pythonPath: "/usr/bin/python3",
      port: 47821,
      token: "tok-123",
    });
    expect(spec.command).toBe("/usr/bin/python3");
    expect(spec.args).toEqual([
      "-m",
      "amx.web._studio_subprocess",
      "--port",
      "47821",
      "--token",
      "tok-123",
      "--embedded",
      "--owner",
      "vscode",
    ]);
  });

  it("appends --config-path when provided", () => {
    const spec = buildServerSpawnSpec({
      pythonPath: "python",
      port: 1,
      token: "t",
      configPath: "C:\\Users\\dev\\.amx\\config.yml",
    });
    expect(spec.args.slice(-2)).toEqual(["--config-path", "C:\\Users\\dev\\.amx\\config.yml"]);
  });
});

describe("buildCliSpawnSpec", () => {
  it("drives amx studio headless with embedded headers", () => {
    const spec = buildCliSpawnSpec({ amxCliPath: "/usr/local/bin/amx", port: 47900 });
    expect(spec.command).toBe("/usr/local/bin/amx");
    expect(spec.args).toEqual(["studio", "--no-open", "--embedded", "--port", "47900"]);
  });
});
