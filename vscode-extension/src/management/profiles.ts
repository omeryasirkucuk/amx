// Native profile CRUD: add (backend-schema-driven wizard), edit
// (field picker → walk only the chosen fields), delete, test. The
// wizards consume the server's field_specs so new backends need no
// extension changes.
import * as vscode from "vscode";

import type { DbProfileSummary, LlmProfileSummary, NamedProfileSummary } from "../api/types";
import type { ExtensionServices } from "../services";
import { refreshViews } from "../views";
import { guard, guardValue, guardWithRetry } from "./errors";
import { vscodePromptPort } from "./promptPort";
import {
  answersToBody,
  fieldSpecToStep,
  runWizard,
  type FieldSpec,
  type WizardStep,
} from "./wizard";

type ProfileKind = "db" | "llm" | "docs" | "code";

interface ProfileNodeArg {
  kind?: ProfileKind;
  name?: string;
}

export function registerProfileManagement(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.profiles.addDb", () => addDbProfile(services)),
    vscode.commands.registerCommand("amx.profiles.addLlm", () => addLlmProfile(services)),
    vscode.commands.registerCommand("amx.profiles.addDocs", () => addPathsProfile(services, "docs")),
    vscode.commands.registerCommand("amx.profiles.addCode", () => addPathsProfile(services, "code")),
    vscode.commands.registerCommand("amx.profiles.editProfile", (node?: ProfileNodeArg) =>
      editProfile(services, node),
    ),
    vscode.commands.registerCommand("amx.profiles.deleteProfile", (node?: ProfileNodeArg) =>
      deleteProfile(services, node),
    ),
    vscode.commands.registerCommand("amx.profiles.testDb", (node?: ProfileNodeArg) =>
      testDbProfile(services, node?.name),
    ),
    vscode.commands.registerCommand("amx.profiles.setActive", (node?: ProfileNodeArg) =>
      setActive(services, node),
    ),
  );
}

async function addDbProfile(services: ExtensionServices): Promise<void> {
  const { client } = services;

  const backends = await guardValue("load backends", () => client.profiles.listBackends());
  if (backends === undefined) return;

  const port = vscodePromptPort("AMX: Add DB Profile");
  const backendPick = await runWizard(
    [
      {
        id: "backend",
        kind: "pick",
        title: "Backend",
        items: backends.map((backend) => ({ value: backend.id, label: backend.label })),
      },
    ],
    port,
  );
  if (!backendPick) return;

  const backend = backends.find((entry) => entry.id === backendPick["backend"]);
  if (!backend) return;

  const specs = backend.field_specs as FieldSpec[];

  const existingNames = await guardValue("list profiles", () => client.profiles.listDb());
  if (existingNames === undefined) return;

  const existing = new Set(existingNames.map((profile) => profile.name));
  const nameStep: WizardStep = {
    id: "__name",
    kind: "input",
    title: "Profile name",
    required: true,
    validate: uniqueNameValidator(existing, "Profile"),
  };
  const basicSteps = specs.filter((spec) => spec.group === "basic").map(fieldSpecToStep);
  // Pre-fill the port input with the backend default.
  for (const step of basicSteps) {
    if (step.id === "port" && step.kind === "input" && backend.default_port !== undefined) {
      step.value = String(backend.default_port);
    }
  }
  const advancedSpecs = specs.filter((spec) => spec.group === "advanced");
  const advancedGate: WizardStep = {
    id: "__advanced",
    kind: "pick",
    title: "Configure advanced options?",
    items: [
      { value: "no", label: "No — finish with defaults" },
      { value: "yes", label: "Yes — walk advanced fields" },
    ],
  };
  const advancedSteps = advancedSpecs.map(fieldSpecToStep).map((step) => ({
    ...step,
    when: (answers: Record<string, string>) => answers["__advanced"] === "yes",
  }));

  const steps: WizardStep[] = [nameStep, ...basicSteps];
  if (advancedSpecs.length > 0) steps.push(advancedGate, ...advancedSteps);
  const answers = await runWizard(steps, port);
  if (!answers) return;

  const name = String(answers["__name"]).trim();
  delete answers["__name"];
  delete answers["__advanced"];
  const body = { backend: backend.id, ...answersToBody(answers, specs) };
  await guardWithRetry(`save profile '${name}'`, async () => {
    await services.client.profiles.upsertDb(name, body);
    refreshViews("profiles", "catalog");
    const test = await vscode.window.showInformationMessage(
      `AMX: DB profile '${name}' saved.`,
      "Test Connection",
      "Done",
    );
    if (test === "Test Connection") await testDbProfile(services, name);
  });
}

async function addLlmProfile(services: ExtensionServices): Promise<void> {
  const { client } = services;

  const providers = await guardValue("load providers", () => client.profiles.listProviders());
  if (providers === undefined) return;

  const existingNames = await guardValue("list profiles", () => client.profiles.listLlm());
  if (existingNames === undefined) return;

  const port = vscodePromptPort("AMX: Add LLM Profile");
  const existing = new Set(existingNames.map((profile) => profile.name));
  const answers = await runWizard(
    [
      {
        id: "provider",
        kind: "pick",
        title: "Provider",
        items: providers.map((provider) => ({ value: provider.id, label: provider.label })),
      },
      {
        id: "__name",
        kind: "input",
        title: "Profile name",
        required: true,
        validate: uniqueNameValidator(existing, "Profile"),
      },
      {
        id: "model",
        kind: "input",
        title: "Model",
        required: true,
        placeholder: "e.g. gpt-4o, claude-sonnet-4-6",
        validate: (value) => (value.trim() ? undefined : "Model is required"),
      },
      {
        id: "api_key",
        kind: "input",
        title: "API key",
        password: true,
        placeholder: "Stored in the OS keyring by the server",
        when: (ans) =>
          providers.find((provider) => provider.id === ans["provider"])?.needs_key ?? false,
      },
      {
        id: "api_base",
        kind: "input",
        title: "API base URL",
        placeholder: "e.g. http://localhost:11434",
        when: (ans) =>
          providers.find((provider) => provider.id === ans["provider"])?.needs_base ?? false,
      },
    ],
    port,
  );
  if (!answers) return;
  const name = String(answers["__name"]).trim();
  const body: Record<string, unknown> = { provider: answers["provider"], model: answers["model"] };
  const rawApiKey = String(answers["api_key"] ?? "").trim();
  if (rawApiKey) body["api_key"] = rawApiKey;
  if (answers["api_base"]) body["api_base"] = answers["api_base"];
  await guardWithRetry(`save profile '${name}'`, async () => {
    await services.client.profiles.upsertLlm(name, body);
    refreshViews("profiles", "statusBar");
    const activate = await vscode.window.showInformationMessage(
      `AMX: LLM profile '${name}' saved.`,
      "Set Active",
      "Done",
    );
    if (activate === "Set Active") {
      await services.client.profiles.activateLlm(name);
      refreshViews("profiles", "statusBar");
    }
  });
}

async function addPathsProfile(services: ExtensionServices, kind: "docs" | "code"): Promise<void> {
  const title = kind === "docs" ? "AMX: Add Docs Profile" : "AMX: Add Code Profile";
  const port = vscodePromptPort(title);
  const answers = await runWizard(
    [
      {
        id: "__name",
        kind: "input",
        title: "Profile name",
        required: true,
        validate: (value) => (value.trim() ? undefined : "Name is required"),
      },
      {
        id: "path",
        kind: "input",
        title: kind === "docs" ? "Paths (comma-separated files/dirs/URLs)" : "Repository path",
        required: true,
        validate: (value) => (value.trim() ? undefined : "Required"),
      },
    ],
    port,
  );
  if (!answers) return;
  const name = String(answers["__name"]).trim();
  await guardWithRetry(`save profile '${name}'`, async () => {
    if (kind === "docs") {
      const paths = splitPaths(String(answers["path"]));
      await services.client.profiles.upsertDocs(name, { paths });
    } else {
      await services.client.profiles.upsertCode(name, { path: String(answers["path"]).trim() });
    }
    refreshViews("profiles");
    void vscode.window.showInformationMessage(`AMX: ${kind} profile '${name}' saved.`);
  });
}

async function editProfile(services: ExtensionServices, node?: ProfileNodeArg): Promise<void> {
  const target = await resolveTarget(services, node);
  if (!target) return;
  const { kind, name } = target;
  if (kind === "docs" || kind === "code") {
    return addPathsProfileEdit(services, kind, name);
  }
  const { client } = services;

  let specs: FieldSpec[];
  if (kind === "db") {
    const fetched = await guardValue(`load profile '${name}'`, () => dbSpecsFor(services, name));
    if (fetched === undefined) return;
    specs = fetched;
  } else {
    specs = llmEditSpecs();
  }

  const fieldPick = await vscode.window.showQuickPick(
    specs.map((spec) => ({ label: spec.label, spec })),
    { canPickMany: true, title: `AMX: Edit ${name} — pick fields to change` },
  );
  if (!fieldPick || fieldPick.length === 0) return;
  const port = vscodePromptPort(`AMX: Edit ${name}`);
  const steps = fieldPick.map((entry) => fieldSpecToStep(entry.spec));
  // LLM temperature is a float; the generic int validator would reject
  // "0.7", so it stays a text field with a float-shaped validator.
  if (kind === "llm") {
    for (const step of steps) {
      if (step.id === "temperature" && step.kind === "input") {
        step.validate = (value) =>
          /^\d*(\.\d+)?$/.test(value.trim()) ? undefined : "Temperature must be a number";
      }
    }
  }
  const answers = await runWizard(steps, port);
  if (!answers) return;
  const body = answersToBody(answers, specs);

  // answersToBody drops empty optionals (the server would then leave
  // them unchanged). For string-shaped fields the user explicitly
  // picked and left blank, send an explicit empty string so the server
  // clears the value rather than ignoring the missing key. Typed
  // (int/bool) fields cannot be cleared with "" — the server would
  // reject the body — so they are skipped with a notice instead.
  const skipped: string[] = [];
  for (const { spec } of fieldPick) {
    if (spec.name in body) continue;
    if (spec.kind === "text" || spec.kind === "password" || spec.kind === "select") {
      body[spec.name] = "";
    } else {
      skipped.push(spec.label);
    }
  }
  if (skipped.length > 0) {
    void vscode.window.showInformationMessage(
      `AMX: numeric fields cannot be cleared — skipped: ${skipped.join(", ")}`,
    );
  }

  // Temperature must reach the API as a number, not the wizard's string.
  if (typeof body["temperature"] === "string" && body["temperature"] !== "") {
    body["temperature"] = Number(body["temperature"]);
  }

  await guardWithRetry(`update profile '${name}'`, async () => {
    if (kind === "db") await client.profiles.upsertDb(name, body);
    else await client.profiles.upsertLlm(name, body);
    refreshViews("profiles", "statusBar");
    void vscode.window.showInformationMessage(`AMX: profile '${name}' updated.`);
  });
}

async function addPathsProfileEdit(
  services: ExtensionServices,
  kind: "docs" | "code",
  name: string,
): Promise<void> {
  const value = await vscode.window.showInputBox({
    title: `AMX: Edit ${kind} profile '${name}'`,
    prompt: kind === "docs" ? "Paths (comma-separated)" : "Repository path",
  });
  if (value === undefined || !value.trim()) return;
  await guardWithRetry(`update profile '${name}'`, async () => {
    if (kind === "docs") {
      const paths = splitPaths(value);
      await services.client.profiles.upsertDocs(name, { paths });
    } else {
      await services.client.profiles.upsertCode(name, { path: value.trim() });
    }
    refreshViews("profiles");
  });
}

/** Field specs for editing an existing DB profile (its backend's). */
async function dbSpecsFor(services: ExtensionServices, name: string): Promise<FieldSpec[]> {
  const detail = await services.client.profiles.getDb(name);
  const backendId = String(detail["backend"] ?? "");
  const backends = await services.client.profiles.listBackends();
  return (backends.find((backend) => backend.id === backendId)?.field_specs ?? []) as FieldSpec[];
}

/** LLM profiles have a fixed editable surface (no server schema). */
function llmEditSpecs(): FieldSpec[] {
  const text = (name: string, label: string, help = ""): FieldSpec => ({
    name,
    kind: "text",
    label,
    help,
    secret: false,
    required: false,
    group: "basic",
    options: [],
  });
  return [
    text("model", "Model"),
    { ...text("api_key", "API key"), kind: "password", secret: true },
    text("api_base", "API base URL"),
    text("temperature", "Temperature", "0.0 – 1.0"),
    { ...text("max_tokens", "Max output tokens"), kind: "int" },
  ];
}

async function deleteProfile(services: ExtensionServices, node?: ProfileNodeArg): Promise<void> {
  const target = await resolveTarget(services, node);
  if (!target) return;
  const confirmed = await vscode.window.showWarningMessage(
    `Delete ${target.kind} profile '${target.name}'? This cannot be undone.`,
    { modal: true },
    "Delete",
  );
  if (confirmed !== "Delete") return;
  await guard(`delete profile '${target.name}'`, async () => {
    await services.client.profiles.deleteProfile(target.kind, target.name);
    refreshViews("profiles", "catalog", "statusBar");
    void vscode.window.showInformationMessage(`AMX: profile '${target.name}' deleted.`);
  });
}

async function testDbProfile(services: ExtensionServices, name?: string): Promise<void> {
  const target = name ?? (await pickName(services, "db", "Test which DB profile?"));
  if (!target) return;
  await vscode.window.withProgress(
    { location: vscode.ProgressLocation.Notification, title: `AMX: testing '${target}'…` },
    () =>
      guard(`test profile '${target}'`, async () => {
        await services.client.profiles.testDb(target);
        void vscode.window.showInformationMessage(`AMX: '${target}' connection OK.`);
      }),
  );
}

async function setActive(services: ExtensionServices, node?: ProfileNodeArg): Promise<void> {
  const target = await resolveTarget(services, node);
  if (!target) return;
  await guard(`activate '${target.name}'`, async () => {
    if (target.kind === "db") await services.client.profiles.activateDb(target.name);
    else if (target.kind === "llm") await services.client.profiles.activateLlm(target.name);
    refreshViews("profiles", "statusBar");
  });
}

// --- local helpers ---

async function resolveTarget(
  services: ExtensionServices,
  node?: ProfileNodeArg,
): Promise<{ kind: ProfileKind; name: string } | undefined> {
  if (node?.kind && node.name) return { kind: node.kind, name: node.name };
  const kindPick = await vscode.window.showQuickPick(["db", "llm", "docs", "code"], {
    title: "AMX: profile kind",
  });
  if (!kindPick) return undefined;
  const name = await pickName(services, kindPick as ProfileKind, "Which profile?");
  return name ? { kind: kindPick as ProfileKind, name } : undefined;
}

async function pickName(
  services: ExtensionServices,
  kind: ProfileKind,
  title: string,
): Promise<string | undefined> {
  const list: Array<DbProfileSummary | LlmProfileSummary | NamedProfileSummary> | undefined =
    await guardValue(
      `list ${kind} profiles`,
      (): Promise<Array<DbProfileSummary | LlmProfileSummary | NamedProfileSummary>> =>
        kind === "db"
          ? services.client.profiles.listDb()
          : kind === "llm"
            ? services.client.profiles.listLlm()
            : kind === "docs"
              ? services.client.profiles.listDocs()
              : services.client.profiles.listCode(),
    );
  if (list === undefined) return undefined;
  const pick = await vscode.window.showQuickPick(
    list.map((profile) => profile.name),
    { title },
  );
  return pick;
}

/** Returns a validator that rejects duplicate names within `existing`. */
function uniqueNameValidator(
  existing: Set<string>,
  what: string,
): (value: string) => string | undefined {
  return (value) => {
    if (!value.trim()) return `${what} name is required`;
    if (existing.has(value.trim())) return `${what} '${value.trim()}' already exists`;
    return undefined;
  };
}

/** Split a comma-separated paths string into a trimmed, non-empty array. */
function splitPaths(value: string): string[] {
  return value
    .split(",")
    .map((p) => p.trim())
    .filter(Boolean);
}
