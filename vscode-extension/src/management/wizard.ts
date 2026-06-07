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
