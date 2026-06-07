// Entry for the @vscode/test-electron smoke suite: downloads a VS
// Code build, starts the fake Studio server, points the extension at
// it through a temp AMX_CONFIG_DIR discovery file, and runs suite.ts
// inside the extension host.
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { runTests } from "@vscode/test-electron";

import { FAKE_TOKEN, startFakeStudio } from "./fakeStudio";

async function main(): Promise<void> {
  const { server, port } = await startFakeStudio();
  const configDir = await mkdtemp(join(tmpdir(), "amx-vscode-itest-"));
  await writeFile(
    join(configDir, "studio.json"),
    JSON.stringify({
      port,
      token: FAKE_TOKEN,
      pid: process.pid,
      started_at: new Date().toISOString(),
      owner: "integration-test",
      version: "0.99.0",
    }),
    "utf-8",
  );

  // Compiled location is dist-test/test/integration → repo root of
  // the extension is three levels up.
  const extensionDevelopmentPath = resolve(__dirname, "..", "..", "..");
  const extensionTestsPath = resolve(__dirname, "suite");
  try {
    // Short user-data-dir: VS Code binds a unix socket under it and
    // the default .vscode-test path inside this (deep) repo exceeds
    // the 103-char socket-path limit on macOS.
    const userDataDir = await mkdtemp(join(tmpdir(), "amx-ud-"));
    await runTests({
      extensionDevelopmentPath,
      extensionTestsPath,
      launchArgs: [
        "--disable-extensions",
        "--disable-workspace-trust",
        `--user-data-dir=${userDataDir}`,
      ],
      extensionTestsEnv: { AMX_CONFIG_DIR: configDir },
    });
  } finally {
    server.close();
  }
}

main().catch((error: unknown) => {
  console.error("integration tests failed:", error);
  process.exitCode = 1;
});
