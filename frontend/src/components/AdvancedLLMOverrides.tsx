/** Shared "Advanced LLM settings" form block.
 *
 * Originally lived inside ``frontend/src/routes/RunNew.tsx``. Lifted
 * here verbatim (with no behaviour change) so the **Re-Run modal**
 * (``frontend/src/components/RerunDialog.tsx``) can mount the same
 * form block instead of carrying its own one-off Temperature input.
 *
 * Resolution semantics (unchanged): every field renders the active
 * profile's value as a hint chip on the right (``default <value>``).
 * Leaving a field at the profile default emits no override; changing
 * it surfaces an "override" chip and includes the field in the
 * payload built by :func:`buildOverridesPayload`. The saved profile
 * on disk is never written — both /api/runs and /api/runs/rerun-item
 * apply the overrides via immutable ``dataclasses.replace`` on a
 * derived ``LLMConfig``.
 */

import { useMemo, type ReactNode } from "react";
import { ChevronDown } from "lucide-react";

import type { LLMOverrides, LLMProfileDefaults, ModelPrice } from "../lib/api";
import { cn } from "../lib/cn";
import { InfoHint } from "./ui";

/** Stringy form-state — every field is a free-text input so the
 *  user can clear a field to empty, and we only forward the parsed
 *  numeric value when it actually differs from the saved profile
 *  default. Empty / unparseable / unchanged ⇒ no override. */
export interface OverrideFormState {
  temperature: string;
  maxTokens: string;
  nAlternatives: string;
  columnBatchSize: string;
  promptDetail: string;
  descriptionVerbosity: string;
  confidenceSignal: string;
  alternativesMode: string;
  thinkingBudget: string;
  logprobHigh: string;
  logprobMedium: string;
  customInputCost: string;
  customOutputCost: string;
}

export const EMPTY_OVERRIDES: OverrideFormState = {
  temperature: "",
  maxTokens: "",
  nAlternatives: "",
  columnBatchSize: "",
  promptDetail: "",
  descriptionVerbosity: "",
  confidenceSignal: "",
  alternativesMode: "",
  thinkingBudget: "",
  logprobHigh: "",
  logprobMedium: "",
  customInputCost: "",
  customOutputCost: "",
};

/** Build an OverrideFormState seeded with the active profile's
 *  current values so every input shows a real number ("0.20",
 *  "16384") instead of an empty box. Numeric ``null`` values
 *  collapse to ``""`` because cost overrides are nullable on the
 *  backend and the user clears them by emptying the field. Used
 *  both for initial seeding via useEffect and for the "Reset to
 *  profile defaults" button. */
export function seedFromDefaults(
  defaults: LLMProfileDefaults | null,
): OverrideFormState {
  const num = (value: number | null | undefined): string =>
    value === null || value === undefined ? "" : String(value);
  if (!defaults) return EMPTY_OVERRIDES;
  return {
    temperature: num(defaults.temperature),
    maxTokens: num(defaults.max_tokens),
    nAlternatives: num(defaults.n_alternatives),
    columnBatchSize: num(defaults.column_batch_size),
    promptDetail: defaults.prompt_detail ?? "",
    descriptionVerbosity: defaults.description_verbosity ?? "",
    confidenceSignal: defaults.confidence_signal ?? "",
    alternativesMode: defaults.alternatives_mode ?? "",
    thinkingBudget: num(defaults.thinking_budget),
    logprobHigh: num(defaults.logprob_high),
    logprobMedium: num(defaults.logprob_medium),
    customInputCost: num(defaults.custom_input_cost_per_mtok),
    customOutputCost: num(defaults.custom_output_cost_per_mtok),
  };
}

/** Coerce a stringy form value into a numeric override only when the
 *  user actually typed something new. Returns ``undefined`` to mean
 *  "no override" (use the saved profile's value). */
export function pickNumber(
  raw: string,
  profileValue: number | null | undefined,
): number | undefined {
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed)) return undefined;
  if (
    profileValue !== null &&
    profileValue !== undefined &&
    parsed === profileValue
  ) {
    return undefined;
  }
  return parsed;
}

export function pickString(
  raw: string,
  profileValue: string | null | undefined,
): string | undefined {
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  if (profileValue && trimmed === profileValue) return undefined;
  return trimmed;
}

/** Build the ``llm_overrides`` payload from the form state. Returns
 *  ``undefined`` when the user has not changed any field, so the
 *  request body stays byte-identical to the pre-overrides shape in
 *  the common case. */
export function buildOverridesPayload(
  form: OverrideFormState,
  defaults: LLMProfileDefaults | null,
): LLMOverrides | undefined {
  const out: LLMOverrides = {};
  const temperature = pickNumber(form.temperature, defaults?.temperature);
  if (temperature !== undefined) out.temperature = temperature;
  const maxTokens = pickNumber(form.maxTokens, defaults?.max_tokens);
  if (maxTokens !== undefined) out.max_tokens = maxTokens;
  const nAlt = pickNumber(form.nAlternatives, defaults?.n_alternatives);
  if (nAlt !== undefined) out.n_alternatives = nAlt;
  const columnBatch = pickNumber(
    form.columnBatchSize,
    defaults?.column_batch_size,
  );
  if (columnBatch !== undefined) out.column_batch_size = columnBatch;
  const promptDetail = pickString(form.promptDetail, defaults?.prompt_detail);
  if (promptDetail !== undefined) out.prompt_detail = promptDetail;
  const verbosity = pickString(
    form.descriptionVerbosity,
    defaults?.description_verbosity,
  );
  if (verbosity !== undefined) out.description_verbosity = verbosity;
  const confSig = pickString(
    form.confidenceSignal,
    defaults?.confidence_signal,
  );
  if (confSig !== undefined) out.confidence_signal = confSig;
  const altMode = pickString(
    form.alternativesMode,
    defaults?.alternatives_mode,
  );
  if (altMode === "semantic" || altMode === "lexical") {
    out.alternatives_mode = altMode;
  }
  const thinking = pickNumber(form.thinkingBudget, defaults?.thinking_budget);
  if (thinking !== undefined) out.thinking_budget = thinking;
  const high = pickNumber(form.logprobHigh, defaults?.logprob_high);
  if (high !== undefined) out.logprob_high = high;
  const medium = pickNumber(form.logprobMedium, defaults?.logprob_medium);
  if (medium !== undefined) out.logprob_medium = medium;
  const inputCost = pickNumber(
    form.customInputCost,
    defaults?.custom_input_cost_per_mtok,
  );
  if (inputCost !== undefined) out.custom_input_cost_per_mtok = inputCost;
  const outputCost = pickNumber(
    form.customOutputCost,
    defaults?.custom_output_cost_per_mtok,
  );
  if (outputCost !== undefined) out.custom_output_cost_per_mtok = outputCost;
  return Object.keys(out).length === 0 ? undefined : out;
}

interface AdvancedLLMOverridesProps {
  open: boolean;
  onToggle: () => void;
  form: OverrideFormState;
  onChange: (next: OverrideFormState) => void;
  defaults: LLMProfileDefaults | null;
  profileName: string | null;
  /** Auto-resolved (provider, model) price from
   *  ``GET /api/pricing/model``. Used to back-fill the "default X"
   *  chip on the Cost overrides rows when the active profile has no
   *  custom rate set — so the user sees the actual rate AMX will
   *  bill at instead of an unhelpful em-dash. */
  livePrice: ModelPrice | null;
  livePriceLoading: boolean;
  /** Optional title override for the disclosure header. Defaults to
   *  "Advanced LLM settings" (RunNew's wording). */
  title?: string;
  /** Optional note rendered above the form body — used by the
   *  Re-Run modal in the heterogeneous-batch case to flag that the
   *  defaults reflect the first selected item's profile. */
  prelude?: ReactNode;
}

/** Cost-override row variant. The default badge falls back to the
 *  resolved LiveLLM/OpenRouter rate when the profile has no custom
 *  override, so users see the actual price AMX will bill at instead
 *  of "default —". Source is shown in parentheses ("litellm" /
 *  "openrouter" / "fallback") so the user knows where the number
 *  came from and whether it's worth overriding. */
function CostOverrideRow({
  label,
  profileValue,
  liveValue,
  liveSource,
  liveLoading,
  changed,
  children,
}: {
  label: string;
  profileValue: number | null | undefined;
  liveValue: number | null;
  liveSource: string | null;
  liveLoading: boolean;
  changed: boolean;
  children: ReactNode;
}) {
  const profileSet = profileValue !== null && profileValue !== undefined;
  const usingLive = !profileSet && liveValue !== null;
  const renderBadge = (): { text: string; tone: "default" | "muted" } => {
    if (changed) return { text: "override", tone: "default" };
    if (profileSet)
      return {
        text: `profile $${(profileValue as number).toFixed(2)}`,
        tone: "default",
      };
    if (usingLive) {
      const sourceLabel =
        liveSource && liveSource !== "user_override" && liveSource !== "unknown"
          ? ` · ${liveSource}`
          : "";
      return {
        text: `live $${(liveValue as number).toFixed(2)}${sourceLabel}`,
        tone: "default",
      };
    }
    if (liveLoading) return { text: "loading…", tone: "muted" };
    return { text: "default —", tone: "muted" };
  };
  const badge = renderBadge();
  const tooltip = profileSet
    ? `Profile-defined custom rate: $${(profileValue as number).toFixed(4)} / 1M tokens.`
    : usingLive
      ? `Auto-resolved from ${liveSource ?? "pricing cache"}. Override here to bill this run at a different rate.`
      : "No price resolved for this model. Set a custom rate or run /refresh-prices.";
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between gap-2">
        <span className="text-[11px] font-medium text-ink-muted">{label}</span>
        <span
          className={cn(
            "font-mono text-[10px] tabular-nums",
            changed
              ? "text-accent"
              : badge.tone === "muted"
                ? "text-ink-dim"
                : "text-ink-muted",
          )}
          title={tooltip}
        >
          {badge.text}
        </span>
      </div>
      {children}
    </div>
  );
}

/** One row in the disclosure: label + hint icon, default chip on
 *  the right, input on the next line. Single column to fit the
 *  narrow side card without wrapping the labels. */
function OverrideRow({
  label,
  hint,
  defaultValue,
  changed,
  children,
}: {
  label: string;
  hint?: string;
  defaultValue: string;
  changed: boolean;
  children: ReactNode;
}) {
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between gap-2">
        <span className="inline-flex items-center gap-1 text-[11px] font-medium text-ink-muted">
          {label}
          {hint && <InfoHint text={hint} />}
        </span>
        <span
          className={cn(
            "font-mono text-[10px] tabular-nums",
            changed ? "text-accent" : "text-ink-dim",
          )}
          title={`Profile default: ${defaultValue}`}
        >
          {changed ? "override" : `default ${defaultValue}`}
        </span>
      </div>
      {children}
    </div>
  );
}

/** Disclosure block exposing every LLM-profile tuning knob as a
 *  per-run override. Inputs pre-fill from the active profile's
 *  defaults so the user has a real starting point. The parent only
 *  forwards values that differ from the profile default to the
 *  backend; the saved profile is never mutated. */
export default function AdvancedLLMOverrides({
  open,
  onToggle,
  form,
  onChange,
  defaults,
  profileName,
  livePrice,
  livePriceLoading,
  title = "Advanced LLM settings",
  prelude,
}: AdvancedLLMOverridesProps) {
  const update = (patch: Partial<OverrideFormState>) =>
    onChange({ ...form, ...patch });
  const diffMap = useMemo(
    () => ({
      temperature:
        pickNumber(form.temperature, defaults?.temperature) !== undefined,
      maxTokens:
        pickNumber(form.maxTokens, defaults?.max_tokens) !== undefined,
      nAlternatives:
        pickNumber(form.nAlternatives, defaults?.n_alternatives) !== undefined,
      columnBatchSize:
        pickNumber(form.columnBatchSize, defaults?.column_batch_size) !==
        undefined,
      promptDetail:
        pickString(form.promptDetail, defaults?.prompt_detail) !== undefined,
      descriptionVerbosity:
        pickString(form.descriptionVerbosity, defaults?.description_verbosity) !==
        undefined,
      confidenceSignal:
        pickString(form.confidenceSignal, defaults?.confidence_signal) !==
        undefined,
      alternativesMode:
        pickString(form.alternativesMode, defaults?.alternatives_mode) !==
        undefined,
      thinkingBudget:
        pickNumber(form.thinkingBudget, defaults?.thinking_budget) !==
        undefined,
      logprobHigh:
        pickNumber(form.logprobHigh, defaults?.logprob_high) !== undefined,
      logprobMedium:
        pickNumber(form.logprobMedium, defaults?.logprob_medium) !== undefined,
      customInputCost:
        pickNumber(form.customInputCost, defaults?.custom_input_cost_per_mtok) !==
        undefined,
      customOutputCost:
        pickNumber(
          form.customOutputCost,
          defaults?.custom_output_cost_per_mtok,
        ) !== undefined,
    }),
    [form, defaults],
  );
  const overrideCount = useMemo(
    () => Object.values(diffMap).filter(Boolean).length,
    [diffMap],
  );
  const fmt = (value: number | string | null | undefined): string =>
    value === null || value === undefined || value === "" ? "—" : String(value);
  const inputCls =
    "w-full rounded-md border border-surface-border bg-surface px-2 py-1 font-mono text-xs tabular-nums";
  const selectCls =
    "w-full rounded-md border border-surface-border bg-surface px-2 py-1 text-xs";
  const sectionCls =
    "rounded-md border border-border/60 bg-surface-subtle/40 px-3 py-2.5 space-y-2.5";

  // Mirror Studio's tile-disabled rule on RunNew: alternatives_mode has
  // no effect when alternatives per column is 1. The shared component
  // resolves the *effective* n by reading the form override first then
  // falling back to the profile default — so a user who lowers n to 1
  // in this same panel immediately sees the mode row disable.
  const effectiveNAlternatives = useMemo(() => {
    const raw = form.nAlternatives.trim();
    if (raw) {
      const parsed = Number(raw);
      if (Number.isFinite(parsed)) return parsed;
    }
    return defaults?.n_alternatives ?? 1;
  }, [form.nAlternatives, defaults?.n_alternatives]);
  const altModeDisabled = effectiveNAlternatives <= 1;

  return (
    <div className="rounded-md border border-border bg-surface-subtle/30">
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full items-center justify-between gap-2 px-3 py-2 text-left text-xs font-medium text-ink-muted hover:bg-surface-subtle/60"
        aria-expanded={open}
      >
        <span className="inline-flex items-center gap-1.5">
          <ChevronDown
            size={12}
            className={cn(
              "transition-transform duration-fast",
              !open && "-rotate-90",
            )}
            aria-hidden="true"
          />
          {title}
          <InfoHint text="Override the active LLM profile's tuning knobs for this run only. The saved profile is not mutated." />
        </span>
        <span
          className={cn(
            "text-[10px] uppercase tracking-wider",
            overrideCount > 0 ? "text-accent" : "text-ink-dim",
          )}
        >
          {overrideCount > 0
            ? `${overrideCount} override${overrideCount > 1 ? "s" : ""}`
            : "match profile"}
        </span>
      </button>
      {open && (
        <div className="space-y-3 border-t border-border px-3 py-3 text-xs">
          {prelude}
          <p className="text-[11px] text-ink-dim">
            Source profile{" "}
            <span className="font-mono text-ink-muted">
              {profileName ?? "—"}
            </span>
            . Edit a field to override for this run only.
          </p>

          <div className={sectionCls}>
            <h4 className="text-[10px] font-semibold uppercase tracking-wider text-ink-dim">
              Generation
            </h4>

            <OverrideRow
              label="Temperature"
              hint="Creativity: low = consistent, high = varied (0.1–0.3 recommended)."
              defaultValue={fmt(defaults?.temperature)}
              changed={diffMap.temperature}
            >
              <input
                type="number"
                min={0}
                max={2}
                step={0.05}
                value={form.temperature}
                onChange={(e) => update({ temperature: e.target.value })}
                className={inputCls}
              />
            </OverrideRow>

            <OverrideRow
              label="Max output tokens"
              hint="Output budget per LLM call. Reasoning models auto-tune a 32k floor on top. Higher = bigger answers + higher cost."
              defaultValue={fmt(defaults?.max_tokens)}
              changed={diffMap.maxTokens}
            >
              <input
                type="number"
                min={256}
                max={262_144}
                step={1024}
                value={form.maxTokens}
                onChange={(e) => update({ maxTokens: e.target.value })}
                className={inputCls}
              />
            </OverrideRow>

            <OverrideRow
              label="Alternatives per column"
              hint="How many alternative description proposals to generate per column (1–5)."
              defaultValue={fmt(defaults?.n_alternatives)}
              changed={diffMap.nAlternatives}
            >
              <input
                type="number"
                min={1}
                max={5}
                step={1}
                value={form.nAlternatives}
                onChange={(e) => update({ nAlternatives: e.target.value })}
                className={inputCls}
              />
            </OverrideRow>

            <OverrideRow
              label="Column batch size"
              hint="Columns processed per LLM call. Higher = cheaper; lower = more stable."
              defaultValue={fmt(defaults?.column_batch_size)}
              changed={diffMap.columnBatchSize}
            >
              <input
                type="number"
                min={1}
                max={200}
                step={1}
                value={form.columnBatchSize}
                onChange={(e) => update({ columnBatchSize: e.target.value })}
                className={inputCls}
              />
            </OverrideRow>

            <OverrideRow
              label="Prompt detail"
              hint="How much context the model receives. More = accurate; less = fast/cheap."
              defaultValue={fmt(defaults?.prompt_detail)}
              changed={diffMap.promptDetail}
            >
              <select
                value={form.promptDetail}
                onChange={(e) => update({ promptDetail: e.target.value })}
                className={selectCls}
              >
                {!form.promptDetail && <option value="">—</option>}
                {["minimal", "standard", "detailed", "full"].map((v) => (
                  <option key={v} value={v}>
                    {v}
                  </option>
                ))}
              </select>
            </OverrideRow>

            <OverrideRow
              label="Description verbosity"
              hint="Output length: brief = one sentence, exhaustive = detailed."
              defaultValue={fmt(defaults?.description_verbosity)}
              changed={diffMap.descriptionVerbosity}
            >
              <select
                value={form.descriptionVerbosity}
                onChange={(e) =>
                  update({ descriptionVerbosity: e.target.value })
                }
                className={selectCls}
              >
                {!form.descriptionVerbosity && <option value="">—</option>}
                {["brief", "detailed", "comprehensive", "exhaustive"].map((v) => (
                  <option key={v} value={v}>
                    {v}
                  </option>
                ))}
              </select>
            </OverrideRow>

            <OverrideRow
              label="Confidence signal"
              hint="Active per-alternative scorer. 'self_consistency' is universal; 'logprob' needs provider logprobs; 'self_decl' adds prompt cost; 'judge' issues a second LLM call (~2× tokens); 'none' hides the badge."
              defaultValue={fmt(defaults?.confidence_signal)}
              changed={diffMap.confidenceSignal}
            >
              <select
                value={form.confidenceSignal}
                onChange={(e) => update({ confidenceSignal: e.target.value })}
                className={selectCls}
              >
                {!form.confidenceSignal && <option value="">—</option>}
                {["self_consistency", "logprob", "self_decl", "judge", "none"].map(
                  (v) => (
                    <option key={v} value={v}>
                      {v}
                    </option>
                  ),
                )}
              </select>
            </OverrideRow>

            <OverrideRow
              label="Alternatives diversity mode"
              // Per Definition 1 (NLP standard): semantic ⇒ same meaning /
              // different words; lexical ⇒ shared vocabulary / shifted
              // meaning. Do NOT re-invert.
              hint={
                altModeDisabled
                  ? "Has no effect when alternatives per column is 1."
                  : "Semantic = paraphrase the chosen description (same meaning, different wording). Lexical = share core vocabulary with the chosen description while letting the meaning shift through added nuances."
              }
              defaultValue={fmt(defaults?.alternatives_mode)}
              changed={diffMap.alternativesMode}
            >
              <select
                value={form.alternativesMode}
                onChange={(e) => update({ alternativesMode: e.target.value })}
                disabled={altModeDisabled}
                className={cn(
                  selectCls,
                  altModeDisabled && "opacity-60 cursor-not-allowed",
                )}
              >
                {!form.alternativesMode && <option value="">—</option>}
                {["semantic", "lexical"].map((v) => (
                  <option key={v} value={v}>
                    {v}
                  </option>
                ))}
              </select>
            </OverrideRow>

            <OverrideRow
              label="Thinking budget"
              hint="Token budget for the model's internal reasoning (Anthropic extended thinking + similar). 0 = off."
              defaultValue={fmt(defaults?.thinking_budget)}
              changed={diffMap.thinkingBudget}
            >
              <input
                type="number"
                min={0}
                max={64_000}
                step={256}
                value={form.thinkingBudget}
                onChange={(e) => update({ thinkingBudget: e.target.value })}
                className={inputCls}
              />
            </OverrideRow>
          </div>

          <div className={sectionCls}>
            <h4 className="text-[10px] font-semibold uppercase tracking-wider text-ink-dim">
              Confidence thresholds
            </h4>
            <OverrideRow
              label="High threshold (≥)"
              hint="Predictions above this token-probability score are flagged 'high confidence'."
              defaultValue={fmt(defaults?.logprob_high)}
              changed={diffMap.logprobHigh}
            >
              <input
                type="number"
                min={0}
                max={1}
                step={0.05}
                value={form.logprobHigh}
                onChange={(e) => update({ logprobHigh: e.target.value })}
                className={inputCls}
              />
            </OverrideRow>
            <OverrideRow
              label="Medium threshold (≥)"
              hint="Above this is 'medium confidence'; below counts as 'low'."
              defaultValue={fmt(defaults?.logprob_medium)}
              changed={diffMap.logprobMedium}
            >
              <input
                type="number"
                min={0}
                max={1}
                step={0.05}
                value={form.logprobMedium}
                onChange={(e) => update({ logprobMedium: e.target.value })}
                className={inputCls}
              />
            </OverrideRow>
          </div>

          <div className={sectionCls}>
            <h4 className="inline-flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wider text-ink-dim">
              Cost overrides
              <InfoHint text="Reporting only — does not change the LLM call. The default badge shows the rate AMX will bill at: profile override if set, otherwise the auto-detected LiteLLM / OpenRouter price for this model. Both rates must be set together, or both blank." />
            </h4>
            <CostOverrideRow
              label="Input USD / 1M"
              profileValue={defaults?.custom_input_cost_per_mtok}
              liveValue={livePrice?.input_per_mtok ?? null}
              liveSource={livePrice?.source ?? null}
              liveLoading={livePriceLoading}
              changed={diffMap.customInputCost}
            >
              <input
                type="number"
                min={0}
                step={0.01}
                value={form.customInputCost}
                onChange={(e) => update({ customInputCost: e.target.value })}
                className={inputCls}
              />
            </CostOverrideRow>
            <CostOverrideRow
              label="Output USD / 1M"
              profileValue={defaults?.custom_output_cost_per_mtok}
              liveValue={livePrice?.output_per_mtok ?? null}
              liveSource={livePrice?.source ?? null}
              liveLoading={livePriceLoading}
              changed={diffMap.customOutputCost}
            >
              <input
                type="number"
                min={0}
                step={0.01}
                value={form.customOutputCost}
                onChange={(e) => update({ customOutputCost: e.target.value })}
                className={inputCls}
              />
            </CostOverrideRow>
          </div>

          {overrideCount > 0 && (
            <button
              type="button"
              onClick={() => onChange(seedFromDefaults(defaults))}
              className="text-[11px] text-accent underline-offset-2 hover:underline"
            >
              Reset to profile defaults
            </button>
          )}
        </div>
      )}
    </div>
  );
}
