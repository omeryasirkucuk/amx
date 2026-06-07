// Unit tests for the data-driven wizard engine: step sequencing, Back
// navigation, conditional steps, and field_specs → step mapping.
import { describe, expect, it } from "vitest";

import {
  answersToBody,
  fieldSpecToStep,
  runWizard,
  WIZARD_BACK,
  type FieldSpec,
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
    // Required text field
    const textSpec: FieldSpec = {
      name: "label", kind: "text", label: "Label", help: "",
      secret: false, required: true, group: "basic", options: [],
    };
    const textStep = fieldSpecToStep(textSpec);
    if (textStep.kind !== "input") throw new Error("expected input step");
    expect(textStep.validate?.("")).toMatch(/required/i);
    expect(textStep.validate?.("x")).toBeUndefined();

    // Required int field: empty → required error, valid digits → ok, non-digits → must be a number
    const intSpec: FieldSpec = {
      name: "port", kind: "int", label: "Port", help: "",
      secret: false, required: true, group: "basic", options: [],
    };
    const intStep = fieldSpecToStep(intSpec);
    if (intStep.kind !== "input") throw new Error("expected input step");
    expect(intStep.validate?.("")).toMatch(/required/i);
    expect(intStep.validate?.("12")).toBeUndefined();
    expect(intStep.validate?.("1a")).toMatch(/must be a number/i);
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

describe("answersToBody", () => {
  const specs: FieldSpec[] = [
    { name: "port", kind: "int", label: "Port", help: "", secret: false, required: true, group: "basic", options: [] },
    { name: "enabled", kind: "bool", label: "Enabled", help: "", secret: false, required: false, group: "advanced", options: [] },
    { name: "host", kind: "text", label: "Host", help: "", secret: false, required: true, group: "basic", options: [] },
    { name: "schema", kind: "text", label: "Schema", help: "", secret: false, required: false, group: "advanced", options: [] },
  ];

  it("casts int fields from string to number", () => {
    const body = answersToBody({ port: "5432", host: "localhost" }, specs);
    expect(body["port"]).toBe(5432);
    expect(typeof body["port"]).toBe("number");
  });

  it("casts bool fields: 'true' → true, 'false' → false", () => {
    expect(answersToBody({ enabled: "true", host: "h" }, specs)["enabled"]).toBe(true);
    expect(answersToBody({ enabled: "false", host: "h" }, specs)["enabled"]).toBe(false);
  });

  it("drops empty optional fields", () => {
    const body = answersToBody({ host: "localhost", schema: "" }, specs);
    expect(Object.prototype.hasOwnProperty.call(body, "schema")).toBe(false);
  });

  it("includes empty required text fields as empty string (current behavior)", () => {
    // An empty required text field reaches answersToBody as "" and is
    // included verbatim — the port driver (InputBox) is responsible for
    // blocking submission, but if it slips through, the value is preserved.
    const body = answersToBody({ host: "" }, specs);
    expect(Object.prototype.hasOwnProperty.call(body, "host")).toBe(true);
    expect(body["host"]).toBe("");
  });

  it("passes through unknown keys (no matching spec) as strings", () => {
    const body = answersToBody({ extra: "val" }, specs);
    expect(body["extra"]).toBe("val");
  });

  it("skips string[] (pickMany) answers", () => {
    const body = answersToBody({ host: "localhost", tags: ["a", "b"] }, specs);
    expect(Object.prototype.hasOwnProperty.call(body, "tags")).toBe(false);
  });
});
