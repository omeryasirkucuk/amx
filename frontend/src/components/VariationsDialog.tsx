/**
 * VariationsDialog — seeded re-run from one chosen alternative.
 *
 * Distinct from RerunDialog by intent: where Re-Run regenerates an
 * asset's alternatives from scratch, Variations anchors generation on
 * a specific alternative the user picked (alt B from row #12345). The
 * modal exposes:
 *
 *  - the seed text read-only above the form so the user always sees
 *    what they're varying around;
 *  - a top-level mode radio (Semantic / Lexical) — this is the
 *    primary question when a seed exists, so it gets pride of place
 *    above the Advanced section;
 *  - additional instructions (free-text addendum);
 *  - the shared AdvancedLLMOverrides block, with
 *    ``hideAlternativesModeRow=true`` so mode isn't duplicated in
 *    two places.
 *
 * The new alternatives appear *inline* under the seed alternative in
 * the source run's detail page; this modal merely submits the job
 * and the parent component scrolls to the new group when ``job.done``
 * arrives.
 */

import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Loader2, Sparkles } from "lucide-react";

import { api, apiFetch } from "../lib/api";
import type { AlternativesMode, LLMProfileDefaults } from "../lib/api";
import { useLLMCapabilities } from "../lib/llmCapabilities";
import AdvancedLLMOverrides, {
  EMPTY_OVERRIDES,
  buildOverridesPayload,
  seedFromDefaults,
  type OverrideFormState,
  type ProfileOption,
} from "./AdvancedLLMOverrides";
import { Button, Dialog, Textarea, useToast } from "./ui";

interface Props {
  open: boolean;
  onClose: () => void;
  /** The originating run's id — recorded on the new run_results row
   *  as ``parent_run_id`` for the inline-tree query. */
  originalRunId: number;
  /** ``run_results.id`` of the row that owns the chosen alternative. */
  resultId: number;
  /** Zero-based index into the source row's ``alternatives_json``. */
  alternativeIndex: number;
  /** Verbatim text of the chosen alternative (the seed). */
  seedText: string;
  /** Letter label for display ("A", "B", "C", …). */
  seedLetter: string;
  /** Initial mode pre-selected on open — typically the parent run's
   *  effective mode so a follow-up Variations stays in the same
   *  exploration. Defaults to ``semantic``. */
  initialMode?: AlternativesMode;
  /** Called when the worker has been spawned. */
  onSubmitted: (jobId: string, newRunIdHint?: number | null) => void;
}

export default function VariationsDialog({
  open,
  onClose,
  originalRunId,
  resultId,
  alternativeIndex,
  seedText,
  seedLetter,
  initialMode = "semantic",
  onSubmitted,
}: Props) {
  const { push: pushToast } = useToast();
  const [mode, setMode] = useState<AlternativesMode>(initialMode);
  const [instructions, setInstructions] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [overrides, setOverrides] = useState<OverrideFormState>(EMPTY_OVERRIDES);
  const [submitting, setSubmitting] = useState(false);

  const contextQ = useQuery({
    queryKey: ["context", "variations-defaults"],
    queryFn: () => api.context(),
    enabled: open,
    staleTime: 60_000,
  });
  const defaults: LLMProfileDefaults | null =
    contextQ.data?.llm_profile_defaults ?? null;
  const profileName: string | null = contextQ.data?.active_llm_profile ?? null;
  const profilesQ = useQuery({
    queryKey: ["profiles", "llm"],
    queryFn: () =>
      apiFetch<{ profiles: ProfileOption[]; active: string | null }>(
        "/api/profiles/llm",
      ),
    enabled: open,
    staleTime: 60_000,
  });
  const profiles: ProfileOption[] = profilesQ.data?.profiles ?? [];
  const pickedProfile = profiles.find((p) => p.name === overrides.profile);
  const effectiveProvider =
    pickedProfile?.provider ?? contextQ.data?.llm_provider ?? null;
  const effectiveModel =
    pickedProfile?.model ?? contextQ.data?.llm_model ?? null;
  const { supportsThinking, supportsLogprobs } = useLLMCapabilities(
    effectiveProvider,
    effectiveModel,
  );
  const credentialsMissing = Boolean(
    pickedProfile && pickedProfile.has_credentials === false,
  );

  useEffect(() => {
    if (open) {
      setOverrides(seedFromDefaults(defaults));
      setMode(initialMode);
      setInstructions("");
      setAdvancedOpen(false);
    }
  }, [open, defaults, initialMode]);

  const summaryDescription = useMemo(
    () =>
      "Original DB, docs, codebase context is preserved. The selected " +
      "alternative is used as a seed to bias the new descriptions.",
    [],
  );

  const submit = async () => {
    if (!seedText.trim()) {
      pushToast({ tone: "error", title: "Seed text is empty." });
      return;
    }
    setSubmitting(true);
    try {
      const llmOverrides = buildOverridesPayload(overrides, defaults);
      const res = await api.generateVariations({
        original_run_id: originalRunId,
        result_id: resultId,
        alternative_index: alternativeIndex,
        seed_text: seedText,
        mode,
        user_instructions: instructions.trim() || null,
        llm_overrides: llmOverrides,
      });
      onSubmitted(res.job_id, res.new_run_id ?? null);
      onClose();
    } catch (err) {
      pushToast({
        tone: "error",
        title: "Variations failed to start",
        description: err instanceof Error ? err.message : String(err),
      });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={submitting ? () => undefined : onClose}
      title={
        <span className="inline-flex items-center gap-2">
          <Sparkles size={14} className="text-accent" />
          Generate variations from this alternative
        </span>
      }
      description={summaryDescription}
      size="lg"
      preventBackdropClose={submitting}
      footer={
        <>
          <Button variant="ghost" onClick={onClose} disabled={submitting}>
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={submit}
            disabled={submitting || credentialsMissing}
          >
            {submitting ? (
              <span className="inline-flex items-center gap-2">
                <Loader2 size={14} className="animate-spin" /> Starting…
              </span>
            ) : (
              <span className="inline-flex items-center gap-2">
                <Sparkles size={14} /> Generate Variations
              </span>
            )}
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        <div>
          <div className="mb-1 text-[10px] uppercase tracking-wider text-ink-dim">
            Seed Alternative
          </div>
          <div className="flex items-start gap-2 rounded-md border border-border bg-surface-subtle/40 px-3 py-2 text-xs">
            <span className="inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-accent/15 font-mono text-[11px] font-semibold text-accent">
              {seedLetter}
            </span>
            <span className="text-ink">{seedText}</span>
          </div>
        </div>

        <div>
          <div className="mb-1 text-[10px] uppercase tracking-wider text-ink-dim">
            Variation type
          </div>
          <div className="flex flex-wrap items-stretch gap-2">
            {(["semantic", "lexical"] as const).map((value) => {
              const active = mode === value;
              const description =
                value === "semantic"
                  ? "Paraphrases of the seed — same meaning, different wording."
                  : "Share vocabulary with the seed; allow meaning to drift through added nuances.";
              return (
                <button
                  key={value}
                  type="button"
                  onClick={() => setMode(value)}
                  disabled={submitting}
                  className={
                    "min-w-[180px] flex-1 rounded-md border px-3 py-2 text-left text-xs " +
                    (active
                      ? "border-accent bg-accent/10 text-ink"
                      : "border-surface-border bg-surface text-ink-muted hover:bg-surface-subtle")
                  }
                >
                  <div className="font-medium capitalize">{value}</div>
                  <div className="mt-0.5 text-[11px] text-ink-dim">{description}</div>
                </button>
              );
            })}
          </div>
        </div>

        <div>
          <label
            htmlFor="variations-instructions"
            className="mb-1 block text-[10px] uppercase tracking-wider text-ink-dim"
          >
            Additional instructions (optional)
          </label>
          <Textarea
            id="variations-instructions"
            placeholder='e.g. "Emphasize the temporal dimension."'
            value={instructions}
            onChange={(e) => setInstructions(e.target.value)}
            rows={3}
            disabled={submitting}
          />
        </div>

        <AdvancedLLMOverrides
          open={advancedOpen}
          onToggle={() => setAdvancedOpen((v) => !v)}
          form={overrides}
          onChange={setOverrides}
          defaults={defaults}
          profileName={profileName}
          livePrice={null}
          livePriceLoading={false}
          title="Advanced LLM settings"
          profiles={profiles}
          hideAlternativesModeRow
          supportsThinking={supportsThinking}
          supportsLogprobs={supportsLogprobs}
          effectiveModel={effectiveModel}
        />
        {credentialsMissing && (
          <p className="rounded-md border border-warn/40 bg-warn/10 px-3 py-2 text-[11px] text-warn">
            The selected LLM profile is missing credentials. Open Settings →
            LLM → {overrides.profile} to add an API key before generating
            variations.
          </p>
        )}
      </div>
    </Dialog>
  );
}
