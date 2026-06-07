// Smoke assertions running inside the extension host. Plain
// assert-based runner (no mocha dependency): @vscode/test-electron
// just needs an exported run() that resolves on success.
import * as assert from "node:assert";
import * as vscode from "vscode";

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

  // The server manager adopts the fake Studio recorded in the
  // discovery file (AMX_CONFIG_DIR points at it) and the catalog
  // populates from the fake inventory.
  await vscode.commands.executeCommand("amx.server.start");
  await vscode.commands.executeCommand("amx.catalog.refresh");

  // Opening a panel must create a webview without throwing; with the
  // fake server adopted this exercises ensure() + iframe HTML build.
  await vscode.commands.executeCommand("amx.openAsk");

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
}
