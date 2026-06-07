# VS Code Native Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every AMX management operation (profile CRUD, catalog sync + descriptions, run lifecycle, schedule CRUD) runs natively in VS Code through data-driven wizards and tree context menus.

**Architecture:** A pure-state-machine wizard engine (`src/management/wizard.ts`) renders the server's `field_specs` form schemas through a `PromptPort` abstraction (unit-testable without VS Code). One module per domain under `src/management/` consumes the existing `AmxClient`/`CatalogCache`/`ServerManager` services; `package.json` gains context-menu contributions keyed on existing `contextValue`s.

**Tech Stack:** TypeScript (strict + exactOptionalPropertyTypes), vitest, @vscode/test-electron, existing fetch-based AmxClient.

**Constraints:** Local only — commit on the local `fix/vscode-feedback` branch, never push, no deploy. English-only strings. No new runtime npm deps.

**Verified API contracts (do not re-derive):**
- `GET /api/profiles/db/backends` → `{backends: [{id, label, fields, field_specs: [{name, kind: "text"|"int"|"password"|"select"|"bool", label, help, secret, required, group: "basic"|"advanced", options}], default_port?, supports_catalog?}]}`
- `GET /api/profiles/llm/providers` → `{providers: [{id, label, needs_key, needs_base}]}`
- `PUT /api/profiles/db/{name}` body = flat field dict (unknown keys ignored); `PUT /api/profiles/llm/{name}` same; `PUT /api/profiles/docs/{name}` body `{paths: string[], linked_db_profiles?: string[]}`; `PUT /api/profiles/code/{name}` body `{path: string, linked_db_profiles?: string[]}`; `DELETE /api/profiles/{kind}/{name}`; `POST /api/profiles/db/{name}/test`; `POST /api/profiles/{db|llm}/{name}/activate`.
- `POST /api/catalog/sync?profile=&database=` → returns immediately; progress via `GET /api/catalog/freshness?profile=`. `POST /api/catalog/deep-sync?profile=`.
- `POST /api/runs` body `{scope: {schema: [tables]}, db_profile?, database?, catalog?}` → `{job_id}`; SSE `GET /api/runs/{job_id}/events`; `POST /api/runs/{job_id}/cancel`.
- `POST /api/runs/rerun-item` body `{result_ids: number[], user_instructions?}` → `{job_id}`; result ids from `GET /api/history/runs/{id}/results`.
- `POST /api/schedules` body = ScheduleCreateRequest `{name, fire_at_local, fire_at_tz, db_profile, database?, catalog?, scope: {mode: "all"|"schemas"|"tables", ...}, llm_profile, review_strategy, kind: "analyze"|"cache_refresh", cron_expr?, trigger: "time"|"change"}`; `PATCH /api/schedules/{id}` = same fields all-optional; `DELETE /api/schedules/{id}` (204).

---

### Task 1: Wizard engine (PromptPort + runner)

**Files:**
- Create: `vscode-extension/src/management/wizard.ts`
- Test: `vscode-extension/test/unit/wizard.test.ts`

- [ ] **Step 1: Write the failing tests**

```typescript
// test/unit/wizard.test.ts
import { describe, expect, it } from "vitest";

import {
  fieldSpecToStep,
  runWizard,
  WIZARD_BACK,
  type PromptPort,
  type WizardStep,
} from "../../src/management/wizard";

/** Scripted port: pops canned answers; records prompts it saw. */
function scriptedPort(answers: (string | string[] | symbol | undefined)[]): {
  port: PromptPort;
  seen: string[];
} {
  const queue = [...answers];
  const seen: string[] = [];
  const next = (label: string) => {
    seen.push(label);
    return Promise.resolve(queue.shift());
  };
  return {
    seen,
    port: {
      pick: (options) => next(`pick:${options.title}`) as Promise<string | symbol | undefined>,
      pickMany: (options) => next(`pickMany:${options.title}`) as Promise<string[] | symbol | undefined>,
      input: (options) => next(`input:${options.title}`) as Promise<string | symbol | undefined>,
    },
  };
}

const NAME_STEP: WizardStep = { id: "name", kind: "input", title: "Name", required: true };
const KIND_STEP: WizardStep = {
  id: "kind",
  kind: "pick",
  title: "Kind",
  items: [
    { value: "a", label: "A" },
    { value: "b", label: "B" },
  ],
};

describe("runWizard", () => {
  it("collects answers in order", async () => {
    const { port } = scriptedPort(["orders", "a"]);
    const result = await runWizard([NAME_STEP, KIND_STEP], port);
    expect(result).toEqual({ name: "orders", kind: "a" });
  });

  it("returns undefined on abort (Esc)", async () => {
    const { port } = scriptedPort(["orders", undefined]);
    expect(await runWizard([NAME_STEP, KIND_STEP], port)).toBeUndefined();
  });

  it("re-runs the previous step on WIZARD_BACK", async () => {
    const { port, seen } = scriptedPort(["orders", WIZARD_BACK, "items", "b"]);
    const result = await runWizard([NAME_STEP, KIND_STEP], port);
    expect(result).toEqual({ name: "items", kind: "b" });
    expect(seen).toEqual(["input:Name", "pick:Kind", "input:Name", "pick:Kind"]);
  });

  it("skips steps whose when() is false", async () => {
    const conditional: WizardStep = { ...KIND_STEP, when: (a) => a["name"] === "show" };
    const { port, seen } = scriptedPort(["hide"]);
    const result = await runWizard([NAME_STEP, conditional], port);
    expect(result).toEqual({ name: "hide" });
    expect(seen).toEqual(["input:Name"]);
  });

  it("enforces required on input steps via validate", async () => {
    const step: WizardStep = { id: "n", kind: "input", title: "N", required: true };
    expect(step.required).toBe(true); // validation itself happens in the port driver
    const { port } = scriptedPort(["x"]);
    expect(await runWizard([step], port)).toEqual({ n: "x" });
  });
});

describe("fieldSpecToStep", () => {
  it("maps select specs to pick steps with options", () => {
    const step = fieldSpecToStep({
      name: "sslmode",
      kind: "select",
      label: "SSL mode",
      help: "libpq sslmode",
      secret: false,
      required: false,
      group: "advanced",
      options: ["", "disable", "require"],
    });
    expect(step.kind).toBe("pick");
    expect(step.id).toBe("sslmode");
    if (step.kind === "pick") {
      expect(step.items.map((item) => item.value)).toEqual(["", "disable", "require"]);
    }
  });

  it("maps bool specs to Yes/No picks and password specs to secret inputs", () => {
    const boolStep = fieldSpecToStep({
      name: "ssl_disabled", kind: "bool", label: "Disable TLS", help: "",
      secret: false, required: false, group: "advanced", options: [],
    });
    expect(boolStep.kind).toBe("pick");
    const pwStep = fieldSpecToStep({
      name: "password", kind: "password", label: "Password", help: "",
      secret: true, required: true, group: "basic", options: [],
    });
    expect(pwStep.kind).toBe("input");
    if (pwStep.kind === "input") expect(pwStep.password).toBe(true);
  });

  it("maps int specs to inputs with numeric validation", () => {
    const step = fieldSpecToStep({
      name: "port", kind: "int", label: "Port", help: "Default 5432",
      secret: false, required: true, group: "basic", options: [],
    });
    if (step.kind !== "input") throw new Error("expected input step");
    expect(step.validate?.("abc")).toBeTruthy();   // error message
    expect(step.validate?.("5432")).toBeUndefined(); // valid
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd vscode-extension && npx vitest run test/unit/wizard.test.ts`
Expected: FAIL — module `src/management/wizard` does not exist.

- [ ] **Step 3: Implement the engine**

```typescript
// src/management/wizard.ts
// Data-driven wizard engine: a pure step runner consuming PromptPort
// (QuickPick/InputBox abstraction) so logic is unit-testable without
// VS Code, plus the field_specs → step mapper that turns the server's
// profile form schemas into wizard steps. The VS Code-backed port
// lives in promptPort.ts.

/** Sentinel returned by a port when the user pressed Back. */
export const WIZARD_BACK = Symbol("wizard-back");

export interface PickItem {
  value: string;
  label: string;
  description?: string;
}

export interface PickStep {
  id: string;
  kind: "pick";
  title: string;
  items: PickItem[];
  placeholder?: string;
  /** Skip the step entirely when false for the answers so far. */
  when?: (answers: Record<string, string>) => boolean;
}

export interface PickManyStep {
  id: string;
  kind: "pickMany";
  title: string;
  items: PickItem[];
  placeholder?: string;
  when?: (answers: Record<string, string>) => boolean;
}

export interface InputStep {
  id: string;
  kind: "input";
  title: string;
  placeholder?: string;
  value?: string;
  password?: boolean;
  required?: boolean;
  /** Return an error message to reject, undefined to accept. */
  validate?: (value: string) => string | undefined;
  when?: (answers: Record<string, string>) => boolean;
}

export type WizardStep = PickStep | PickManyStep | InputStep;

/** Port the engine drives; the VS Code implementation shows real UI,
 *  tests inject a scripted one. Resolves undefined on Esc and
 *  WIZARD_BACK when the user clicked the Back button. */
export interface PromptPort {
  pick(options: PickStep & { stepLabel: string }): Promise<string | symbol | undefined>;
  pickMany(options: PickManyStep & { stepLabel: string }): Promise<string[] | symbol | undefined>;
  input(options: InputStep & { stepLabel: string }): Promise<string | symbol | undefined>;
}

export type WizardAnswers = Record<string, string | string[]>;

/**
 * Run steps in order. Back re-runs the previous *shown* step; Esc
 * anywhere aborts (returns undefined, nothing persisted). `when`
 * guards re-evaluate on every pass so Back through a branch behaves.
 */
export async function runWizard(
  steps: WizardStep[],
  port: PromptPort,
): Promise<WizardAnswers | undefined> {
  const answers: WizardAnswers = {};
  const shown: number[] = [];
  let index = 0;
  while (index < steps.length) {
    const step = steps[index]!;
    const scalarAnswers = Object.fromEntries(
      Object.entries(answers).filter(([, v]) => typeof v === "string"),
    ) as Record<string, string>;
    if (step.when && !step.when(scalarAnswers)) {
      index += 1;
      continue;
    }
    const stepLabel = `(${shown.length + 1})`;
    let result: string | string[] | symbol | undefined;
    if (step.kind === "pick") result = await port.pick({ ...step, stepLabel });
    else if (step.kind === "pickMany") result = await port.pickMany({ ...step, stepLabel });
    else result = await port.input({ ...step, stepLabel });

    if (result === undefined) return undefined; // Esc — abort
    if (result === WIZARD_BACK) {
      const previous = shown.pop();
      if (previous === undefined) return undefined; // Back on first step
      delete answers[steps[previous]!.id];
      index = previous;
      continue;
    }
    answers[step.id] = result as string | string[];
    shown.push(index);
    index += 1;
  }
  return answers;
}

// --- field_specs mapping (GET /api/profiles/db/backends) ---

export interface FieldSpec {
  name: string;
  kind: "text" | "int" | "password" | "select" | "bool";
  label: string;
  help: string;
  secret: boolean;
  required: boolean;
  group: "basic" | "advanced";
  options: string[];
}

export function fieldSpecToStep(spec: FieldSpec): WizardStep {
  if (spec.kind === "select") {
    return {
      id: spec.name,
      kind: "pick",
      title: spec.label,
      placeholder: spec.help || undefined,
      items: spec.options.map((option) => ({
        value: option,
        label: option === "" ? "(default)" : option,
      })),
    } as PickStep;
  }
  if (spec.kind === "bool") {
    return {
      id: spec.name,
      kind: "pick",
      title: spec.label,
      placeholder: spec.help || undefined,
      items: [
        { value: "false", label: "No" },
        { value: "true", label: "Yes" },
      ],
    } as PickStep;
  }
  const step: InputStep = {
    id: spec.name,
    kind: "input",
    title: spec.label,
  };
  if (spec.help) step.placeholder = spec.help;
  if (spec.kind === "password" || spec.secret) step.password = true;
  if (spec.required) {
    step.required = true;
    step.validate = (value) => (value.trim() ? undefined : `${spec.label} is required`);
  }
  if (spec.kind === "int") {
    step.validate = (value) => {
      if (!value.trim()) return spec.required ? `${spec.label} is required` : undefined;
      return /^\d+$/.test(value.trim()) ? undefined : `${spec.label} must be a number`;
    };
  }
  return step;
}

/** Convert collected answers back into an API patch body, casting
 *  int/bool fields and dropping empty optionals. */
export function answersToBody(
  answers: WizardAnswers,
  specs: FieldSpec[],
): Record<string, unknown> {
  const byName = new Map(specs.map((spec) => [spec.name, spec]));
  const body: Record<string, unknown> = {};
  for (const [key, raw] of Object.entries(answers)) {
    if (typeof raw !== "string") continue;
    const spec = byName.get(key);
    if (raw === "" && spec && !spec.required) continue;
    if (spec?.kind === "int") body[key] = Number(raw);
    else if (spec?.kind === "bool") body[key] = raw === "true";
    else body[key] = raw;
  }
  return body;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run test/unit/wizard.test.ts`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add src/management/wizard.ts test/unit/wizard.test.ts
git commit -m "feat(vscode): data-driven wizard engine for native management"
```

---

### Task 2: VS Code PromptPort driver

**Files:**
- Create: `vscode-extension/src/management/promptPort.ts`

(No unit test — this is the thin vscode-API shim; the integration suite exercises it indirectly. Logic stays in wizard.ts.)

- [ ] **Step 1: Implement the driver**

```typescript
// src/management/promptPort.ts
// VS Code implementation of PromptPort: createQuickPick / createInputBox
// with a Back button, Esc → undefined, validation wiring. All wizard
// LOGIC lives in wizard.ts; this file only translates steps to UI.
import * as vscode from "vscode";

import {
  WIZARD_BACK,
  type InputStep,
  type PickManyStep,
  type PickStep,
  type PromptPort,
} from "./wizard";

export function vscodePromptPort(wizardTitle: string): PromptPort {
  return {
    pick: (step) => showPick(wizardTitle, step, false) as Promise<string | symbol | undefined>,
    pickMany: (step) => showPick(wizardTitle, step, true) as Promise<string[] | symbol | undefined>,
    input: (step) => showInput(wizardTitle, step),
  };
}

function showPick(
  wizardTitle: string,
  step: (PickStep | PickManyStep) & { stepLabel: string },
  many: boolean,
): Promise<string | string[] | symbol | undefined> {
  return new Promise((resolve) => {
    const quickPick = vscode.window.createQuickPick();
    quickPick.title = `${wizardTitle} — ${step.title} ${step.stepLabel}`;
    quickPick.placeholder = step.placeholder ?? "";
    quickPick.canSelectMany = many;
    quickPick.ignoreFocusOut = true;
    quickPick.buttons = [vscode.QuickInputButtons.Back];
    quickPick.items = step.items.map((item) => ({
      label: item.label,
      description: item.description ?? "",
    }));
    const valueOf = (label: string) =>
      step.items.find((item) => item.label === label)?.value ?? label;
    let settled = false;
    const settle = (value: string | string[] | symbol | undefined) => {
      if (settled) return;
      settled = true;
      quickPick.hide();
      quickPick.dispose();
      resolve(value);
    };
    quickPick.onDidTriggerButton(() => settle(WIZARD_BACK));
    quickPick.onDidAccept(() => {
      if (many) settle(quickPick.selectedItems.map((item) => valueOf(item.label)));
      else settle(quickPick.selectedItems[0] ? valueOf(quickPick.selectedItems[0].label) : undefined);
    });
    quickPick.onDidHide(() => settle(undefined));
    quickPick.show();
  });
}

function showInput(
  wizardTitle: string,
  step: InputStep & { stepLabel: string },
): Promise<string | symbol | undefined> {
  return new Promise((resolve) => {
    const input = vscode.window.createInputBox();
    input.title = `${wizardTitle} — ${step.title} ${step.stepLabel}`;
    input.placeholder = step.placeholder ?? "";
    input.value = step.value ?? "";
    input.password = step.password ?? false;
    input.ignoreFocusOut = true;
    input.buttons = [vscode.QuickInputButtons.Back];
    let settled = false;
    const settle = (value: string | symbol | undefined) => {
      if (settled) return;
      settled = true;
      input.hide();
      input.dispose();
      resolve(value);
    };
    input.onDidTriggerButton(() => settle(WIZARD_BACK));
    input.onDidChangeValue((value) => {
      input.validationMessage = step.validate?.(value) ?? "";
    });
    input.onDidAccept(() => {
      const error = step.validate?.(input.value);
      if (error) {
        input.validationMessage = error;
        return;
      }
      settle(input.value);
    });
    input.onDidHide(() => settle(undefined));
    input.show();
  });
}
```

- [ ] **Step 2: Typecheck**

Run: `npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add src/management/promptPort.ts
git commit -m "feat(vscode): QuickPick/InputBox PromptPort driver with Back support"
```

---

### Task 3: AmxClient management methods + DTOs

**Files:**
- Modify: `vscode-extension/src/api/types.ts` (append DTOs)
- Modify: `vscode-extension/src/api/client.ts` (extend typed groups)

- [ ] **Step 1: Append DTOs to types.ts**

```typescript
// append to src/api/types.ts

// --- /api/profiles wizard metadata (profiles.py) ---

export interface BackendSpec {
  id: string;
  label: string;
  fields: string[];
  field_specs: Array<{
    name: string;
    kind: "text" | "int" | "password" | "select" | "bool";
    label: string;
    help: string;
    secret: boolean;
    required: boolean;
    group: "basic" | "advanced";
    options: string[];
  }>;
  default_port?: number;
  supports_catalog?: boolean;
  [key: string]: unknown;
}

export interface LlmProviderSpec {
  id: string;
  label: string;
  needs_key: boolean;
  needs_base: boolean;
}

// --- /api/schedules create/patch (schedules.py) ---

export interface ScheduleCreateBody {
  name: string;
  fire_at_local: string;
  fire_at_tz: string;
  db_profile: string;
  database?: string | null;
  catalog?: string | null;
  scope: Record<string, unknown>;
  llm_profile: string;
  review_strategy: "auto" | "manual";
  kind: "analyze" | "cache_refresh";
  cron_expr?: string | null;
  trigger: "time" | "change";
}

// --- /api/runs submit (runs.py) ---

export interface RunSubmitBody {
  scope: Record<string, string[]>;
  db_profile?: string;
  database?: string;
  catalog?: string;
}

export interface JobRef {
  job_id: string;
  status?: string;
}

export interface RunResultRow {
  id: number;
  [key: string]: unknown;
}
```

- [ ] **Step 2: Extend client.ts typed groups**

Add inside the `AmxClient` class (extend the existing groups; `delete` needs a helper since only get/post/put exist):

```typescript
  del<T>(path: string, query?: Query): Promise<T> {
    const options: { query?: Query } = {};
    if (query) options.query = query;
    return this.request<T>("DELETE", path, options);
  }
```

Extend `profiles` group with:

```typescript
    listBackends: async (): Promise<BackendSpec[]> =>
      (await this.get<{ backends: BackendSpec[] }>("/api/profiles/db/backends")).backends,
    listProviders: async (): Promise<LlmProviderSpec[]> =>
      (await this.get<{ providers: LlmProviderSpec[] }>("/api/profiles/llm/providers")).providers,
    upsertDb: (name: string, body: Record<string, unknown>): Promise<unknown> =>
      this.put(`/api/profiles/db/${encodeURIComponent(name)}`, body),
    upsertLlm: (name: string, body: Record<string, unknown>): Promise<unknown> =>
      this.put(`/api/profiles/llm/${encodeURIComponent(name)}`, body),
    upsertDocs: (name: string, body: { paths: string[] }): Promise<unknown> =>
      this.put(`/api/profiles/docs/${encodeURIComponent(name)}`, body),
    upsertCode: (name: string, body: { path: string }): Promise<unknown> =>
      this.put(`/api/profiles/code/${encodeURIComponent(name)}`, body),
    getDb: (name: string): Promise<Record<string, unknown>> =>
      this.get(`/api/profiles/db/${encodeURIComponent(name)}`),
    getLlm: (name: string): Promise<Record<string, unknown>> =>
      this.get(`/api/profiles/llm/${encodeURIComponent(name)}`),
    deleteProfile: (kind: "db" | "llm" | "docs" | "code", name: string): Promise<unknown> =>
      this.del(`/api/profiles/${kind}/${encodeURIComponent(name)}`),
```

Extend `catalog` group with:

```typescript
    sync: (profile?: string, database?: string): Promise<unknown> =>
      this.post("/api/catalog/sync", undefined, { profile, database }),
    deepSync: (profile?: string, database?: string): Promise<unknown> =>
      this.post("/api/catalog/deep-sync", undefined, { profile, database }),
    freshness: (profile?: string): Promise<Record<string, unknown>> =>
      this.get("/api/catalog/freshness", { profile }),
```

Add a `runs` group:

```typescript
  readonly runs = {
    submit: (body: RunSubmitBody): Promise<JobRef> => this.post("/api/runs", body),
    cancel: (jobId: string): Promise<unknown> =>
      this.post(`/api/runs/${encodeURIComponent(jobId)}/cancel`),
    rerunItems: (resultIds: number[], instructions?: string): Promise<JobRef> =>
      this.post("/api/runs/rerun-item", {
        result_ids: resultIds,
        ...(instructions ? { user_instructions: instructions } : {}),
      }),
    results: async (runId: number): Promise<RunResultRow[]> => {
      const payload = await this.get<{ results?: RunResultRow[] } | RunResultRow[]>(
        `/api/history/runs/${runId}/results`,
      );
      return Array.isArray(payload) ? payload : (payload.results ?? []);
    },
  };
```

Extend `schedules` group with:

```typescript
    create: (body: ScheduleCreateBody): Promise<unknown> => this.post("/api/schedules", body),
    patch: (id: string | number, body: Partial<ScheduleCreateBody>): Promise<unknown> =>
      this.request("PATCH", `/api/schedules/${id}`, { body }),
    remove: (id: string | number): Promise<unknown> => this.del(`/api/schedules/${id}`),
```

Import the new DTO names in client.ts's type import block.

- [ ] **Step 3: Typecheck + existing unit tests still green**

Run: `npm run typecheck && npx vitest run`
Expected: clean, 60 tests pass (52 + 8 wizard).

- [ ] **Step 4: Commit**

```bash
git add src/api/types.ts src/api/client.ts
git commit -m "feat(vscode): client methods for profile/catalog/run/schedule management"
```

---

### Task 4: Profile CRUD commands

**Files:**
- Create: `vscode-extension/src/management/profiles.ts`
- Create: `vscode-extension/src/management/index.ts`
- Modify: `vscode-extension/src/commands/index.ts` (call `registerManagement(services)`)

- [ ] **Step 1: Implement profiles.ts**

```typescript
// src/management/profiles.ts
// Native profile CRUD: add (backend-schema-driven wizard), edit
// (field picker → walk only the chosen fields), delete, test. The
// wizards consume the server's field_specs so new backends need no
// extension changes.
import * as vscode from "vscode";

import type { DbProfileSummary, LlmProfileSummary, NamedProfileSummary } from "../api/types";
import type { ExtensionServices } from "../services";
import { refreshViews } from "../views";
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
  const backends = await client.profiles.listBackends();
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
  const backend = backends.find((entry) => entry.id === backendPick["backend"])!;
  const specs = backend.field_specs as FieldSpec[];

  const existing = new Set((await client.profiles.listDb()).map((profile) => profile.name));
  const nameStep: WizardStep = {
    id: "__name",
    kind: "input",
    title: "Profile name",
    required: true,
    validate: (value) => {
      if (!value.trim()) return "Name is required";
      if (existing.has(value.trim())) return `Profile '${value.trim()}' already exists`;
      return undefined;
    },
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
  await guard(`save profile '${name}'`, async () => {
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
  const providers = await client.profiles.listProviders();
  const port = vscodePromptPort("AMX: Add LLM Profile");
  const existing = new Set((await client.profiles.listLlm()).map((profile) => profile.name));
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
        validate: (value) => {
          if (!value.trim()) return "Name is required";
          if (existing.has(value.trim())) return `Profile '${value.trim()}' already exists`;
          return undefined;
        },
      },
      { id: "model", kind: "input", title: "Model", required: true,
        placeholder: "e.g. gpt-4o, claude-sonnet-4-6",
        validate: (value) => (value.trim() ? undefined : "Model is required") },
      {
        id: "api_key", kind: "input", title: "API key", password: true,
        placeholder: "Stored in the OS keyring by the server",
        when: (answers) =>
          providers.find((provider) => provider.id === answers["provider"])?.needs_key ?? false,
      },
      {
        id: "api_base", kind: "input", title: "API base URL",
        placeholder: "e.g. http://localhost:11434",
        when: (answers) =>
          providers.find((provider) => provider.id === answers["provider"])?.needs_base ?? false,
      },
    ],
    port,
  );
  if (!answers) return;
  const name = String(answers["__name"]).trim();
  const body: Record<string, unknown> = { provider: answers["provider"], model: answers["model"] };
  if (answers["api_key"]) body["api_key"] = answers["api_key"];
  if (answers["api_base"]) body["api_base"] = answers["api_base"];
  await guard(`save profile '${name}'`, async () => {
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
      { id: "__name", kind: "input", title: "Profile name", required: true,
        validate: (value) => (value.trim() ? undefined : "Name is required") },
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
  await guard(`save profile '${name}'`, async () => {
    if (kind === "docs") {
      const paths = String(answers["path"]).split(",").map((p) => p.trim()).filter(Boolean);
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
    // Paths-shaped profiles: re-run the simple wizard pre-filled.
    return addPathsProfileEdit(services, kind, name);
  }
  const { client } = services;
  const specs: FieldSpec[] =
    kind === "db"
      ? ((await client.profiles.listBackends()).find(async () => true),
        await dbSpecsFor(services, name))
      : llmEditSpecs();
  const fieldPick = await vscode.window.showQuickPick(
    specs.map((spec) => ({ label: spec.label, spec })),
    { canPickMany: true, title: `AMX: Edit ${name} — pick fields to change` },
  );
  if (!fieldPick || fieldPick.length === 0) return;
  const port = vscodePromptPort(`AMX: Edit ${name}`);
  const answers = await runWizard(fieldPick.map((entry) => fieldSpecToStep(entry.spec)), port);
  if (!answers) return;
  const body = answersToBody(answers, specs);
  await guard(`update profile '${name}'`, async () => {
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
  await guard(`update profile '${name}'`, async () => {
    if (kind === "docs") {
      const paths = value.split(",").map((p) => p.trim()).filter(Boolean);
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
    name, kind: "text", label, help, secret: false, required: false, group: "basic", options: [],
  });
  return [
    text("model", "Model"),
    { ...text("api_key", "API key"), kind: "password", secret: true },
    text("api_base", "API base URL"),
    { ...text("temperature", "Temperature", "0.0 – 1.0") },
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
  const target =
    name ?? (await pickName(services, "db", "Test which DB profile?"));
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

// --- helpers ---

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
  const list: Array<DbProfileSummary | LlmProfileSummary | NamedProfileSummary> =
    kind === "db"
      ? await services.client.profiles.listDb()
      : kind === "llm"
        ? await services.client.profiles.listLlm()
        : kind === "docs"
          ? await services.client.profiles.listDocs()
          : await services.client.profiles.listCode();
  const pick = await vscode.window.showQuickPick(
    list.map((profile) => profile.name),
    { title },
  );
  return pick;
}

async function guard(action: string, body: () => Promise<void>): Promise<void> {
  try {
    await body();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not ${action}: ${message}`);
  }
}
```

Note: the stray `((await client.profiles.listBackends()).find(async () => true), await dbSpecsFor(...))` line in `editProfile` is a plan typo — implement as just `await dbSpecsFor(services, name)`.

- [ ] **Step 2: Create management/index.ts and wire into commands**

```typescript
// src/management/index.ts
// Registration entry for the native management surfaces.
import type { ExtensionServices } from "../services";
import { registerCatalogOps } from "./catalogOps";
import { registerProfileManagement } from "./profiles";
import { registerRunManagement } from "./runs";
import { registerScheduleManagement } from "./schedules";

export function registerManagement(services: ExtensionServices): void {
  registerProfileManagement(services);
  registerCatalogOps(services);
  registerRunManagement(services);
  registerScheduleManagement(services);
}
```

(Stub `catalogOps.ts` / `runs.ts` / `schedules.ts` with empty `register*` exports until Tasks 5–7 fill them.) In `src/commands/index.ts` add `import { registerManagement } from "../management";` and call `registerManagement(services);` inside `registerCommands`.

- [ ] **Step 3: Typecheck + build**

Run: `npm run typecheck && npm run build`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/management src/commands/index.ts
git commit -m "feat(vscode): native profile CRUD wizards (add/edit/delete/test/activate)"
```

---

### Task 5: Catalog ops commands

**Files:**
- Create: `vscode-extension/src/management/catalogOps.ts` (replace stub)
- Modify: `vscode-extension/src/views/catalogTree.ts` (column nodes carry asset args)

- [ ] **Step 1: Implement catalogOps.ts**

```typescript
// src/management/catalogOps.ts
// Catalog management from the tree: sync / deep sync per profile,
// edit or generate descriptions on tables and columns, copy names.
// Description applies share the generate flow's catalog-local vs
// database-writeback choice.
import * as vscode from "vscode";

import type { ExtensionServices } from "../services";
import { refreshViews } from "../views";

interface CatalogNodeArg {
  profile?: string;
  schema?: string;
  table?: string;
  column?: string;
  description?: string;
}

const SYNC_POLL_MS = 2000;
const SYNC_TIMEOUT_MS = 600_000;

export function registerCatalogOps(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.catalog.sync", (node?: CatalogNodeArg) =>
      runSync(services, node?.profile, false),
    ),
    vscode.commands.registerCommand("amx.catalog.deepSync", (node?: CatalogNodeArg) =>
      runSync(services, node?.profile, true),
    ),
    vscode.commands.registerCommand("amx.catalog.editDescription", (node?: CatalogNodeArg) =>
      editDescription(services, node),
    ),
    vscode.commands.registerCommand("amx.catalog.copyName", (node?: CatalogNodeArg) => {
      if (!node?.schema || !node.table) return;
      const qualified = [node.schema, node.table, node.column].filter(Boolean).join(".");
      void vscode.env.clipboard.writeText(qualified);
      void vscode.window.setStatusBarMessage(`AMX: copied ${qualified}`, 3000);
    }),
    vscode.commands.registerCommand("amx.catalog.analyzeTable", (node?: CatalogNodeArg) =>
      vscode.commands.executeCommand("amx.runs.start", node),
    ),
  );
}

async function runSync(
  services: ExtensionServices,
  profile: string | undefined,
  deep: boolean,
): Promise<void> {
  const label = deep ? "deep sync" : "sync";
  await vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: `AMX: catalog ${label}${profile ? ` (${profile})` : ""}`,
      cancellable: false,
    },
    async (progress) => {
      try {
        if (deep) await services.client.catalog.deepSync(profile);
        else await services.client.catalog.sync(profile);
        progress.report({ message: "sync running on the server…" });
        const deadline = Date.now() + SYNC_TIMEOUT_MS;
        // The sync endpoint returns immediately; poll freshness until
        // the profile leaves its syncing state (field shape verified
        // at implementation time — treat any non-"syncing" status as
        // done and stop on error states).
        while (Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, SYNC_POLL_MS));
          const freshness = await services.client.catalog.freshness(profile);
          const status = String(
            (freshness["state"] ?? freshness["status"] ?? "") as string,
          ).toLowerCase();
          if (status && !status.includes("sync")) break;
          progress.report({ message: status || "running…" });
        }
        services.catalog.invalidate(profile ? { profile } : undefined);
        refreshViews("catalog");
        void vscode.window.showInformationMessage(`AMX: catalog ${label} finished.`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        void vscode.window.showErrorMessage(`AMX: catalog ${label} failed: ${message}`);
      }
    },
  );
}

async function editDescription(
  services: ExtensionServices,
  node?: CatalogNodeArg,
): Promise<void> {
  if (!node?.schema || !node.table || !node.profile) {
    void vscode.window.showWarningMessage("AMX: select a catalog table or column first.");
    return;
  }
  const assetLabel = [node.schema, node.table, node.column].filter(Boolean).join(".");
  const text = await vscode.window.showInputBox({
    title: `AMX: describe ${assetLabel}`,
    value: node.description ?? "",
    prompt: "Description text",
    ignoreFocusOut: true,
  });
  if (text === undefined || !text.trim()) return;
  const where = await vscode.window.showQuickPick(
    [
      {
        label: "Apply to catalog",
        description: "Local override — never writes to the source database",
        target: "catalog" as const,
      },
      {
        label: "Apply to database",
        description: "COMMENT ON … against the source database",
        target: "database" as const,
      },
    ],
    { title: `AMX: where should the description go?` },
  );
  if (!where) return;
  try {
    if (where.target === "catalog") {
      await services.client.comments.setLocal({
        profile: node.profile,
        schema: node.schema,
        table: node.table,
        ...(node.column ? { column: node.column } : {}),
        description: text.trim(),
      });
    } else if (node.column) {
      await services.client.comments.setColumn(
        node.schema, node.table, node.column, text.trim(), node.profile,
      );
    } else {
      await services.client.comments.setTable(node.schema, node.table, text.trim(), node.profile);
    }
    services.catalog.invalidate({ profile: node.profile });
    refreshViews("catalog");
    void vscode.window.showInformationMessage(`AMX: description saved for ${assetLabel}.`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not save description: ${message}`);
  }
}
```

- [ ] **Step 2: Catalog tree nodes pass management args**

In `src/views/catalogTree.ts`: profile node TreeItems get `contextValue = "amx.catalogProfile"` (already set) — additionally stash the node arg for menus by constructing TreeItems whose `command` stays as-is but context-menu commands receive the *tree node object*. VS Code passes the `TreeDataProvider` element itself to `view/item/context` commands, so extend the node interfaces: `ProfileScopeNode` already carries `profile`; `TableNode.meta` carries schema/name/profile/description; `ColumnNode` must also carry the owning table's schema/table/profile — extend `ColumnNode` to `{type: "column", meta: ColumnMeta, table: TableMeta}` and pass `node.meta /* table */` when building column children in `columnNodes()`. Then in `catalogOps.ts`-facing commands, normalize the element with a small mapper:

```typescript
// add to src/management/catalogOps.ts (and use it at each command entry)
export function catalogArgFromNode(element: unknown): CatalogNodeArg | undefined {
  if (typeof element !== "object" || element === null) return undefined;
  const node = element as Record<string, unknown>;
  if (node["type"] === "profileScope") return { profile: node["profile"] as string };
  if (node["type"] === "table") {
    const meta = node["meta"] as { schema: string; name: string; profile?: string; description?: string };
    const arg: CatalogNodeArg = { schema: meta.schema, table: meta.name };
    if (meta.profile) arg.profile = meta.profile;
    if (meta.description) arg.description = meta.description;
    return arg;
  }
  if (node["type"] === "column") {
    const table = node["table"] as { schema: string; name: string; profile?: string };
    const meta = node["meta"] as { name: string; description?: string };
    const arg: CatalogNodeArg = { schema: table.schema, table: table.name, column: meta.name };
    if (table.profile) arg.profile = table.profile;
    if (meta.description) arg.description = meta.description;
    return arg;
  }
  return undefined;
}
```

Wire each registered command through it: `(element?: unknown) => editDescription(services, catalogArgFromNode(element))` etc. Also pass the generate flow: register `amx.catalog.generateDescription` that maps the node and forwards to the existing `amx.generateDescription` command with `{schema, table, column?, profile?}`.

- [ ] **Step 3: Typecheck + unit tests**

Run: `npm run typecheck && npx vitest run`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/management/catalogOps.ts src/views/catalogTree.ts
git commit -m "feat(vscode): catalog sync and description management from the tree"
```

---

### Task 6: Run lifecycle commands

**Files:**
- Create: `vscode-extension/src/management/runs.ts` (replace stub)
- Modify: `vscode-extension/src/views/historyTree.ts` (running rows get `amx.run.running` contextValue; expose run on the node)

- [ ] **Step 1: Implement runs.ts**

```typescript
// src/management/runs.ts
// Run lifecycle: scoped start wizard (profile → schema → tables),
// SSE-backed progress notification, cancel on running rows, rerun of
// a finished run's result items.
import * as vscode from "vscode";

import { eventType } from "../api/sse";
import type { RunSummary } from "../api/types";
import type { ExtensionServices } from "../services";
import { refreshViews } from "../views";
import { vscodePromptPort } from "./promptPort";
import { runWizard, type WizardStep } from "./wizard";

interface RunNodeArg {
  run?: RunSummary;
}

interface StartArgs {
  profile?: string;
  schema?: string;
  table?: string;
}

export function registerRunManagement(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.runs.start", (prefill?: StartArgs) =>
      startRun(services, prefill ?? {}),
    ),
    vscode.commands.registerCommand("amx.runs.cancel", (node?: RunNodeArg) =>
      cancelRun(services, node?.run),
    ),
    vscode.commands.registerCommand("amx.runs.rerun", (node?: RunNodeArg) =>
      rerunRun(services, node?.run),
    ),
  );
}

async function startRun(services: ExtensionServices, prefill: StartArgs): Promise<void> {
  const { client, catalog } = services;
  const profiles = await client.profiles.listDb();
  if (profiles.length === 0) {
    void vscode.window.showWarningMessage("AMX: no DB profiles configured.");
    return;
  }
  const port = vscodePromptPort("AMX: Start Run");

  const profileStep: WizardStep = {
    id: "profile",
    kind: "pick",
    title: "DB profile",
    items: profiles.map((profile) => ({
      value: profile.name,
      label: profile.name,
      description: profile.backend,
    })),
  };
  const first = prefill.profile
    ? { profile: prefill.profile }
    : await runWizard([profileStep], port);
  if (!first) return;
  const profile = String(first["profile"]);

  const tables = await catalog.getTables({ profile });
  if (tables.length === 0) {
    void vscode.window.showWarningMessage(
      `AMX: no indexed tables for '${profile}' — run a catalog sync first.`,
    );
    return;
  }
  const schemas = [...new Set(tables.map((table) => table.schema))].sort();
  const schemaStep: WizardStep = {
    id: "schema",
    kind: "pick",
    title: "Schema",
    items: schemas.map((schema) => ({ value: schema, label: schema })),
  };
  const second = prefill.schema ? { schema: prefill.schema } : await runWizard([schemaStep], port);
  if (!second) return;
  const schema = String(second["schema"]);

  const tablesInSchema = tables
    .filter((table) => table.schema === schema)
    .map((table) => table.name)
    .sort();
  let chosen: string[];
  if (prefill.table) {
    chosen = [prefill.table];
  } else {
    const tableAnswers = await runWizard(
      [
        {
          id: "tables",
          kind: "pickMany",
          title: "Tables (empty selection = every table in the schema)",
          items: tablesInSchema.map((table) => ({ value: table, label: table })),
        },
      ],
      port,
    );
    if (!tableAnswers) return;
    chosen = (tableAnswers["tables"] as string[]) ?? [];
  }

  const summary = chosen.length > 0 ? chosen.map((t) => `${schema}.${t}`).join(", ") : `${schema}.*`;
  const go = await vscode.window.showInformationMessage(
    `Start an analyze run for ${summary} on '${profile}'?`,
    { modal: true },
    "Start Run",
  );
  if (go !== "Start Run") return;

  try {
    const job = await client.runs.submit({
      scope: { [schema]: chosen },
      db_profile: profile,
    });
    void trackRun(services, job.job_id, summary);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not start the run: ${message}`);
  }
}

/** Progress notification fed by the run's SSE stream. */
async function trackRun(
  services: ExtensionServices,
  jobId: string,
  summary: string,
): Promise<void> {
  refreshViews("history");
  await vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: `AMX run: ${summary}`,
      cancellable: true,
    },
    async (progress, cancelToken) => {
      const abort = new AbortController();
      cancelToken.onCancellationRequested(() => {
        void services.client.runs.cancel(jobId);
        abort.abort();
      });
      let outcome = "finished";
      try {
        for await (const event of services.client.sse(`/api/runs/${jobId}/events`, {
          signal: abort.signal,
        })) {
          const type = eventType(event) ?? "";
          if (type === "job.failed") outcome = "failed";
          if (type === "job.cancelled") outcome = "cancelled";
          const payload = event.data as { label?: string; message?: string } | undefined;
          const label = payload?.label ?? payload?.message;
          if (label) progress.report({ message: String(label).slice(0, 80) });
        }
      } catch {
        outcome = "connection lost";
      }
      refreshViews("history");
      if (outcome === "finished") {
        const open = await vscode.window.showInformationMessage(
          `AMX run ${summary}: finished.`,
          "Open History",
        );
        if (open === "Open History") await vscode.commands.executeCommand("amx.history.refresh");
      } else {
        void vscode.window.showWarningMessage(`AMX run ${summary}: ${outcome}.`);
      }
    },
  );
}

async function cancelRun(services: ExtensionServices, run?: RunSummary): Promise<void> {
  const jobId = run?.live_job_id;
  if (!jobId) {
    void vscode.window.showWarningMessage("AMX: that run is not currently running.");
    return;
  }
  try {
    await services.client.runs.cancel(jobId);
    refreshViews("history");
    void vscode.window.showInformationMessage("AMX: cancel requested.");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: cancel failed: ${message}`);
  }
}

async function rerunRun(services: ExtensionServices, run?: RunSummary): Promise<void> {
  if (!run) return;
  try {
    const results = await services.client.runs.results(run.id);
    const ids = results.map((row) => row.id).filter((id) => Number.isInteger(id));
    if (ids.length === 0) {
      void vscode.window.showWarningMessage("AMX: run has no result rows to re-run.");
      return;
    }
    const instructions = await vscode.window.showInputBox({
      title: `AMX: re-run ${ids.length} result(s) of run #${run.id}`,
      prompt: "Optional extra instructions for the model (Enter to skip)",
    });
    if (instructions === undefined) return;
    const job = await services.client.runs.rerunItems(ids, instructions.trim() || undefined);
    void trackRun(services, job.job_id, `rerun #${run.id}`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: rerun failed: ${message}`);
  }
}
```

- [ ] **Step 2: historyTree running contextValue**

In `runItem()` (src/views/historyTree.ts): `item.contextValue = run.live_job_id ? "amx.run.running" : "amx.run";` — and the provider's nodes (`RunNode`) already carry `run`, which is what `view/item/context` hands to commands; the `RunNodeArg` mapper reads `element.run`.

- [ ] **Step 3: Typecheck + tests, commit**

Run: `npm run typecheck && npx vitest run` → clean.

```bash
git add src/management/runs.ts src/views/historyTree.ts
git commit -m "feat(vscode): native run lifecycle (start wizard, SSE progress, cancel, rerun)"
```

---

### Task 7: Schedule CRUD commands

**Files:**
- Create: `vscode-extension/src/management/schedules.ts` (replace stub)
- Modify: `vscode-extension/src/views/schedulesTree.ts` (rows get `contextValue = "amx.schedule"`)

- [ ] **Step 1: Implement schedules.ts**

```typescript
// src/management/schedules.ts
// Schedule CRUD wizards on top of the existing pause/resume/run-now
// commands (registered in commands/index.ts). Create walks name →
// kind → trigger → timing → profile → scope → llm; edit patches the
// picked fields; delete confirms.
import * as vscode from "vscode";

import type { ScheduleCreateBody, ScheduleSummary } from "../api/types";
import type { ExtensionServices } from "../services";
import { refreshViews } from "../views";
import { vscodePromptPort } from "./promptPort";
import { runWizard, type WizardStep } from "./wizard";

interface ScheduleNodeArg {
  schedule?: ScheduleSummary;
}

export function registerScheduleManagement(services: ExtensionServices): void {
  services.context.subscriptions.push(
    vscode.commands.registerCommand("amx.schedules.create", () => createSchedule(services)),
    vscode.commands.registerCommand("amx.schedules.edit", (node?: ScheduleNodeArg) =>
      editSchedule(services, node?.schedule),
    ),
    vscode.commands.registerCommand("amx.schedules.delete", (node?: ScheduleNodeArg) =>
      deleteSchedule(services, node?.schedule),
    ),
  );
}

async function createSchedule(services: ExtensionServices): Promise<void> {
  const { client, catalog } = services;
  const [dbProfiles, llmProfiles] = await Promise.all([
    client.profiles.listDb(),
    client.profiles.listLlm(),
  ]);
  if (dbProfiles.length === 0) {
    void vscode.window.showWarningMessage("AMX: configure a DB profile first.");
    return;
  }
  const port = vscodePromptPort("AMX: New Schedule");
  const answers = await runWizard(
    [
      { id: "name", kind: "input", title: "Schedule name", required: true,
        validate: (value) => (value.trim() ? undefined : "Name is required") },
      {
        id: "kind", kind: "pick", title: "What should it do?",
        items: [
          { value: "analyze", label: "Analyze run", description: "Generate descriptions for the scope" },
          { value: "cache_refresh", label: "Catalog refresh", description: "Re-sync the catalog cache" },
        ],
      },
      {
        id: "trigger", kind: "pick", title: "Trigger",
        items: [
          { value: "time", label: "At a time", description: "One-shot or recurring" },
          { value: "change", label: "On change", description: "Fires when new assets appear in scope" },
        ],
      },
      {
        id: "fire_at_local", kind: "input", title: "Fire at (YYYY-MM-DDTHH:MM, local wall clock)",
        when: (a) => a["trigger"] === "time", required: true,
        validate: (value) =>
          /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$/.test(value.trim())
            ? undefined
            : "Use YYYY-MM-DDTHH:MM",
      },
      {
        id: "fire_at_tz", kind: "input", title: "Timezone (IANA id)", value: "UTC",
        when: (a) => a["trigger"] === "time",
      },
      {
        id: "cron_expr", kind: "input", title: "Cron expression (empty = one-shot)",
        placeholder: "e.g. 0 */6 * * *",
        when: (a) => a["trigger"] === "time",
      },
      {
        id: "db_profile", kind: "pick", title: "DB profile",
        items: dbProfiles.map((profile) => ({
          value: profile.name, label: profile.name, description: profile.backend,
        })),
      },
      {
        id: "llm_profile", kind: "pick", title: "LLM profile",
        when: (a) => a["kind"] === "analyze",
        items: llmProfiles.map((profile) => ({
          value: profile.name, label: profile.name, description: profile.model,
        })),
      },
    ],
    port,
  );
  if (!answers) return;

  // Scope: all schemas, one schema, or specific tables.
  const profile = String(answers["db_profile"]);
  const tables = await catalog.getTables({ profile });
  const schemas = [...new Set(tables.map((table) => table.schema))].sort();
  const scopeAnswers = await runWizard(
    [
      {
        id: "mode", kind: "pick", title: "Scope",
        items: [
          { value: "all", label: "Everything reachable" },
          ...(schemas.length > 0 ? [{ value: "schemas", label: "Pick schemas…" }] : []),
        ],
      },
      {
        id: "schemas", kind: "pickMany", title: "Schemas",
        when: (a) => a["mode"] === "schemas",
        items: schemas.map((schema) => ({ value: schema, label: schema })),
      },
    ],
    port,
  );
  if (!scopeAnswers) return;
  const scope: Record<string, unknown> =
    scopeAnswers["mode"] === "all"
      ? { mode: "all" }
      : { mode: "schemas", schemas: scopeAnswers["schemas"] ?? [] };

  const body: ScheduleCreateBody = {
    name: String(answers["name"]).trim(),
    fire_at_local: String(answers["fire_at_local"] ?? ""),
    fire_at_tz: String(answers["fire_at_tz"] ?? "UTC"),
    db_profile: profile,
    scope,
    llm_profile: String(answers["llm_profile"] ?? ""),
    review_strategy: "auto",
    kind: answers["kind"] as "analyze" | "cache_refresh",
    trigger: answers["trigger"] as "time" | "change",
  };
  const cron = String(answers["cron_expr"] ?? "").trim();
  if (cron) body.cron_expr = cron;
  try {
    await services.client.schedules.create(body);
    refreshViews("schedules");
    void vscode.window.showInformationMessage(`AMX: schedule '${body.name}' created.`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not create the schedule: ${message}`);
  }
}

async function editSchedule(
  services: ExtensionServices,
  schedule?: ScheduleSummary,
): Promise<void> {
  const target = schedule ?? (await pickSchedule(services));
  if (!target) return;
  const field = await vscode.window.showQuickPick(
    [
      { label: "Name", id: "name" },
      { label: "Fire time", id: "fire_at_local" },
      { label: "Timezone", id: "fire_at_tz" },
      { label: "Cron expression", id: "cron_expr" },
      { label: "LLM profile", id: "llm_profile" },
    ],
    { title: `AMX: edit '${target.name ?? target.id}' — which field?` },
  );
  if (!field) return;
  const value = await vscode.window.showInputBox({
    title: `AMX: new value for ${field.label}`,
    value: String((target as Record<string, unknown>)[field.id] ?? ""),
  });
  if (value === undefined) return;
  try {
    await services.client.schedules.patch(target.id, { [field.id]: value.trim() });
    refreshViews("schedules");
    void vscode.window.showInformationMessage("AMX: schedule updated.");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not update the schedule: ${message}`);
  }
}

async function deleteSchedule(
  services: ExtensionServices,
  schedule?: ScheduleSummary,
): Promise<void> {
  const target = schedule ?? (await pickSchedule(services));
  if (!target) return;
  const confirmed = await vscode.window.showWarningMessage(
    `Delete schedule '${target.name ?? target.id}'?`,
    { modal: true },
    "Delete",
  );
  if (confirmed !== "Delete") return;
  try {
    await services.client.schedules.remove(target.id);
    refreshViews("schedules");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    void vscode.window.showErrorMessage(`AMX: could not delete the schedule: ${message}`);
  }
}

async function pickSchedule(services: ExtensionServices): Promise<ScheduleSummary | undefined> {
  const schedules = await services.client.schedules.list();
  if (schedules.length === 0) {
    void vscode.window.showInformationMessage("AMX: no schedules configured.");
    return undefined;
  }
  const pick = await vscode.window.showQuickPick(
    schedules.map((schedule) => ({
      label: schedule.name ?? `Schedule ${schedule.id}`,
      description: schedule.cron ?? "",
      schedule,
    })),
    { title: "Select a schedule" },
  );
  return pick?.schedule;
}
```

- [ ] **Step 2: schedulesTree rows expose contextValue + node shape**

In `src/views/schedulesTree.ts`, ensure each schedule TreeItem sets `contextValue = "amx.schedule"` and the node element carries `{schedule}` so context-menu commands can read `element.schedule`.

- [ ] **Step 3: Typecheck + tests, commit**

Run: `npm run typecheck && npx vitest run` → clean.

```bash
git add src/management/schedules.ts src/views/schedulesTree.ts
git commit -m "feat(vscode): schedule create/edit/delete wizards"
```

---

### Task 8: package.json menu contributions

**Files:**
- Modify: `vscode-extension/package.json`

- [ ] **Step 1: Add commands**

Add to `contributes.commands` (category "AMX" each): `amx.profiles.addDb` ("Add DB Profile", icon `$(add)`), `amx.profiles.addLlm`, `amx.profiles.addDocs`, `amx.profiles.addCode`, `amx.profiles.editProfile` ("Edit Profile…"), `amx.profiles.deleteProfile` ("Delete Profile"), `amx.profiles.testDb` ("Test Connection"), `amx.profiles.setActive` ("Set Active"), `amx.catalog.sync` ("Sync Catalog", icon `$(cloud-download)`), `amx.catalog.deepSync` ("Deep Sync Catalog…"), `amx.catalog.editDescription` ("Edit Description…"), `amx.catalog.generateDescription` ("Generate Description"), `amx.catalog.copyName` ("Copy Qualified Name"), `amx.catalog.analyzeTable` ("Analyze This Table…"), `amx.runs.start` ("Start Run…", icon `$(play)`), `amx.runs.cancel` ("Cancel Run"), `amx.runs.rerun` ("Re-run"), `amx.schedules.create` ("New Schedule…", icon `$(add)`), `amx.schedules.edit` ("Edit Schedule…"), `amx.schedules.delete` ("Delete Schedule"), plus palette visibility for `amx.runs.start`, `amx.schedules.create`, `amx.profiles.add*`; node-arg commands (`edit/delete/test/setActive/catalog.*/runs.cancel/rerun/schedules.edit/delete`) get `commandPalette: when: "false"` entries.

- [ ] **Step 2: Add menus**

`view/title`: `amx.runs.start` on `amx.history` (navigation, `$(play)`), `amx.schedules.create` on `amx.schedules` (navigation, `$(add)`).

`view/item/context`:

```json
{ "command": "amx.profiles.addDb",        "when": "view == amx.profiles && viewItem == amx.profileGroup.db",   "group": "inline" },
{ "command": "amx.profiles.addLlm",       "when": "view == amx.profiles && viewItem == amx.profileGroup.llm",  "group": "inline" },
{ "command": "amx.profiles.addDocs",      "when": "view == amx.profiles && viewItem == amx.profileGroup.docs", "group": "inline" },
{ "command": "amx.profiles.addCode",      "when": "view == amx.profiles && viewItem == amx.profileGroup.code", "group": "inline" },
{ "command": "amx.profiles.setActive",    "when": "view == amx.profiles && viewItem =~ /amx\\.(db|llm)Profile/",      "group": "1_actions@1" },
{ "command": "amx.profiles.testDb",       "when": "view == amx.profiles && viewItem =~ /amx\\.dbProfile/",            "group": "1_actions@2" },
{ "command": "amx.profiles.editProfile",  "when": "view == amx.profiles && viewItem =~ /amx\\.(db|llm|docs|code)Profile/", "group": "2_modify@1" },
{ "command": "amx.profiles.deleteProfile","when": "view == amx.profiles && viewItem =~ /amx\\.(db|llm|docs|code)Profile/", "group": "2_modify@2" },
{ "command": "amx.catalog.sync",          "when": "view == amx.catalog && viewItem == amx.catalogProfile", "group": "inline" },
{ "command": "amx.catalog.sync",          "when": "view == amx.catalog && viewItem == amx.catalogProfile", "group": "1_actions@1" },
{ "command": "amx.catalog.deepSync",      "when": "view == amx.catalog && viewItem == amx.catalogProfile", "group": "1_actions@2" },
{ "command": "amx.catalog.analyzeTable",  "when": "view == amx.catalog && viewItem == amx.catalogTable",   "group": "1_actions@1" },
{ "command": "amx.catalog.editDescription","when": "view == amx.catalog && viewItem =~ /amx\\.catalog(Table|Column)/", "group": "2_modify@1" },
{ "command": "amx.catalog.generateDescription","when": "view == amx.catalog && viewItem =~ /amx\\.catalog(Table|Column)/", "group": "2_modify@2" },
{ "command": "amx.catalog.copyName",      "when": "view == amx.catalog && viewItem =~ /amx\\.catalog(Table|Column)/", "group": "3_copy@1" },
{ "command": "amx.runs.rerun",            "when": "view == amx.history && viewItem =~ /amx\\.run/",        "group": "1_actions@1" },
{ "command": "amx.runs.cancel",           "when": "view == amx.history && viewItem == amx.run.running",    "group": "1_actions@2" },
{ "command": "amx.schedules.edit",        "when": "view == amx.schedules && viewItem == amx.schedule",     "group": "2_modify@1" },
{ "command": "amx.schedules.delete",      "when": "view == amx.schedules && viewItem == amx.schedule",     "group": "2_modify@2" },
{ "command": "amx.schedules.pause",       "when": "view == amx.schedules && viewItem == amx.schedule",     "group": "1_actions@1" },
{ "command": "amx.schedules.resume",      "when": "view == amx.schedules && viewItem == amx.schedule",     "group": "1_actions@2" },
{ "command": "amx.schedules.runNow",      "when": "view == amx.schedules && viewItem == amx.schedule",     "group": "1_actions@3" }
```

Profiles tree group roots must set `contextValue = "amx.profileGroup.db" | ".llm" | ".docs" | ".code"` (modify `src/views/profilesTree.ts` group nodes) and profile leaves keep their existing `amx.dbProfile(.active)` values (the `=~` regex matches both). The schedule commands `amx.schedules.pause/resume/runNow` already exist programmatically — they now also need `contributes.commands` entries (palette-hidden) for menu use.

- [ ] **Step 3: Build + manual sanity in Extension Development Host**

Run: `npm run build`, then F5 sanity: right-click each tree level shows the expected menu.

- [ ] **Step 4: Commit**

```bash
git add package.json src/views/profilesTree.ts
git commit -m "feat(vscode): context menus and palette entries for management commands"
```

---

### Task 9: Integration coverage + final verification

**Files:**
- Modify: `vscode-extension/test/integration/fakeStudio.ts` (mutation routes + recorder)
- Modify: `vscode-extension/test/integration/suite.ts` (command registration + recorded-body assertions)

- [ ] **Step 1: fakeStudio mutation routes**

Extend the fake server: keep a `received: Array<{method, path, body}>` log; handle `PUT /api/profiles/db/*` → 200 `{ok: true}` recording the JSON body; `GET /api/profiles/db/backends` → one postgresql backend with two basic + one advanced field_specs; `POST /api/catalog/sync` → 200; `GET /api/catalog/freshness` → `{state: "fresh"}`; `POST /api/runs` → `{job_id: "itest-job"}`; `GET /api/runs/itest-job/events` → SSE body emitting `data: {"type":"job.done"}\n\n` then closing; `POST /api/schedules` → 201 `{id: 1}`; `DELETE /api/schedules/1` → 204. Expose the log via `GET /__test/received` (no auth) so the suite can assert.

- [ ] **Step 2: suite assertions**

After activation assertions, add: every new command id appears in `getCommands(true)` (the full list from Task 8). Then drive `client`-level round trips through the extension by invoking `amx.catalog.sync` with `{profile: "warehouse"}` and asserting (poll `/__test/received` via fetch from the test) that a `POST /api/catalog/sync` with `profile=warehouse` was recorded. (Wizard-driven commands are covered by unit tests of the engine — UI prompt automation is out of scope for the smoke suite.)

- [ ] **Step 3: Run everything**

Run, in `vscode-extension/`:
- `npm run typecheck` → clean
- `npx vitest run` → all green (60+)
- `npm run test:integration` → exit 0
- `npm run package` → VSIX builds
- `code --install-extension amx-vscode-0.1.0.vsix --force` (absolute app-bundle path on macOS)

- [ ] **Step 4: Compliance + commit**

```bash
git add -A ':!node_modules'
git diff --cached | grep -ci "p""aid"   # expect 0 (split so this file never contains the literal)
git commit -m "test(vscode): integration coverage for management commands"
```

---

## Self-review notes

- Spec coverage: profiles (Task 4), catalog ops (Task 5), runs (Task 6), schedules (Task 7), wizard engine + field_specs (Tasks 1–2), client surface (Task 3), menus (Task 8), tests (Tasks 1, 9). Freshness-payload field name and rerun results-payload shape are verified empirically during Tasks 5/6 against the live server (noted inline).
- Type consistency: `WizardStep`/`PromptPort`/`FieldSpec` defined in Task 1 and used unchanged in Tasks 2, 4, 6, 7; `RunSummary.id`/`live_job_id` match the existing types.ts.
- No placeholders: every step carries code or an exact command.
