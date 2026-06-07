// Smoke assertions running inside the extension host. Plain
// assert-based runner (no mocha dependency): @vscode/test-electron
// just needs an exported run() that resolves on success.
import * as assert from "node:assert";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import * as vscode from "vscode";

/** Read the fake-studio port from the discovery file written by runTests.ts. */
async function readFakePort(): Promise<number> {
  const configDir = process.env["AMX_CONFIG_DIR"];
  assert.ok(configDir, "AMX_CONFIG_DIR env var is not set");
  const raw = await readFile(join(configDir, "studio.json"), "utf-8");
  const parsed = JSON.parse(raw) as { port: number };
  return parsed.port;
}

export async function run(): Promise<void> {
  const extension = vscode.extensions.getExtension("amx.amx-vscode");
  assert.ok(extension, "extension amx.amx-vscode is not present");
  await extension.activate();
  assert.ok(extension.isActive, "extension failed to activate");

  // Commands from package.json contributes are registered.
  const commands = await vscode.commands.getCommands(true);
  for (const expected of [
    "amx.openAsk",
    "amx.openRepl",
    "amx.server.start",
    "amx.profiles.switchLlm",
    "amx.catalog.refresh",
    "amx.panel.open",
  ]) {
    assert.ok(commands.includes(expected), `command ${expected} is not registered`);
  }

  // All 21 new management command ids must be registered.
  const newCommandIds = [
    "amx.profiles.addDb",
    "amx.profiles.addLlm",
    "amx.profiles.addDocs",
    "amx.profiles.addCode",
    "amx.profiles.editProfile",
    "amx.profiles.deleteProfile",
    "amx.profiles.testDb",
    "amx.profiles.setActive",
    "amx.catalog.sync",
    "amx.catalog.deepSync",
    "amx.catalog.editDescription",
    "amx.catalog.generateDescription",
    "amx.catalog.copyName",
    "amx.catalog.analyzeTable",
    "amx.runs.start",
    "amx.runs.cancel",
    "amx.runs.rerun",
    "amx.schedules.create",
    "amx.schedules.edit",
    "amx.schedules.delete",
    "amx.schedules.pause",
  ];
  for (const id of newCommandIds) {
    assert.ok(commands.includes(id), `management command ${id} is not registered`);
  }

  // The server manager adopts the fake Studio recorded in the
  // discovery file (AMX_CONFIG_DIR points at it) and the catalog
  // populates from the fake inventory.
  await vscode.commands.executeCommand("amx.server.start");
  await vscode.commands.executeCommand("amx.catalog.refresh");

  // Opening a panel must create a webview AND the real SPA inside the
  // iframe must boot: the fake server serves the built bundle from
  // amx/web/static with the embedded-mode headers, and the SPA posts
  // amx:embedReady once its JS executes — a blocked iframe never
  // does, which is exactly the regression this guards against
  // (CSP frame-ancestors must include the vscode-webview:/vscode-file:
  // scheme sources; `*` alone does not match them).
  await vscode.commands.executeCommand("amx.openAsk");
  const readyDeadline = Date.now() + 20_000;
  let readyAreas: string[] = [];
  while (Date.now() < readyDeadline) {
    readyAreas = (await vscode.commands.executeCommand<string[]>("amx.panel.readyAreas")) ?? [];
    if (readyAreas.includes("ask")) break;
    await new Promise((resolveSleep) => setTimeout(resolveSleep, 500));
  }
  assert.ok(
    readyAreas.includes("ask"),
    `the Ask panel SPA never reported amx:embedReady (ready: ${JSON.stringify(readyAreas)})`,
  );

  // Hover pipeline sanity: open a SQL doc referencing the fake table
  // and ask for hovers at the table name.
  const document = await vscode.workspace.openTextDocument({
    language: "sql",
    content: "SELECT id FROM sales.orders;",
  });
  await vscode.window.showTextDocument(document);
  // Allow the catalog warm-up fetch to land before querying hovers.
  await new Promise((resolveSleep) => setTimeout(resolveSleep, 1500));
  const hovers = await vscode.commands.executeCommand<vscode.Hover[]>(
    "vscode.executeHoverProvider",
    document.uri,
    new vscode.Position(0, 21), // inside "orders"
  );
  assert.ok(Array.isArray(hovers), "hover provider did not respond");
  const hoverText = hovers
    .flatMap((hover) => hover.contents)
    .map((content) => (typeof content === "string" ? content : content.value))
    .join("\n");
  assert.ok(
    hoverText.includes("All customer orders"),
    `hover did not surface the catalog description (got: ${hoverText || "<empty>"})`,
  );

  // catalog.sync integration: invoke the command with a profileScope node
  // (matching the shape catalogArgFromNode expects) and confirm the fake
  // server recorded a POST /api/catalog/sync with profile=warehouse.
  const fakePort = await readFakePort();
  await vscode.commands.executeCommand("amx.catalog.sync", {
    type: "profileScope",
    profile: "warehouse",
  });
  // Give the async command a moment to complete the HTTP call.
  await new Promise((resolveSleep) => setTimeout(resolveSleep, 3000));

  interface ReceivedEntry {
    method: string;
    path: string;
    query: Record<string, string>;
    body: unknown;
  }

  const pollDeadline = Date.now() + 10_000;
  let syncEntry: ReceivedEntry | undefined;
  while (Date.now() < pollDeadline) {
    const resp = await fetch(`http://127.0.0.1:${fakePort}/__test/received`);
    const entries = (await resp.json()) as ReceivedEntry[];
    syncEntry = entries.find(
      (e) => e.method === "POST" && e.path === "/api/catalog/sync" && e.query["profile"] === "warehouse",
    );
    if (syncEntry) break;
    await new Promise((resolveSleep) => setTimeout(resolveSleep, 500));
  }
  assert.ok(
    syncEntry !== undefined,
    "POST /api/catalog/sync with profile=warehouse was not recorded by the fake server",
  );
}
