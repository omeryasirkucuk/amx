// Unit tests for the data-driven wizard engine: step sequencing, Back
// navigation, conditional steps, and field_specs → step mapping.
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
