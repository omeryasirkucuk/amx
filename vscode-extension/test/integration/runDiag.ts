// Launcher for the webview-iframe diagnostic suite (diagSuite.ts).
// Usage: npm run diag (see package.json). Results print as [diag]
// lines in the test output.
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { runTests } from "@vscode/test-electron";

async function main(): Promise<void> {
  const extensionDevelopmentPath = resolve(__dirname, "..", "..", "..");
  const extensionTestsPath = resolve(__dirname, "diagSuite");
  const userDataDir = await mkdtemp(join(tmpdir(), "amx-ud-"));
  await runTests({
    extensionDevelopmentPath,
    extensionTestsPath,
    launchArgs: [
      "--disable-extensions",
      "--disable-workspace-trust",
      `--user-data-dir=${userDataDir}`,
    ],
  });
}

main().catch((error: unknown) => {
  console.error("diag run failed:", error);
  process.exitCode = 1;
});
