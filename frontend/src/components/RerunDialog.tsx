/**
 * RerunDialog — single + multi-item Re-Run modal.
 *
 * The user clicks ⟳ "Re-Run" on a result row (or "Re-Run selected" in
 * the multi-select toolbar). This dialog gathers the optional
 * free-text addendum and the full per-run LLM override set (matching
 * RunNew's Advanced LLM settings panel by content), then fires
 * ``POST /api/runs/rerun-item`` and bubbles the resulting ``job_id``
 * back to the caller so the parent component can subscribe to the
 * existing ``/api/runs/{job_id}/events`` SSE stream.
 *
 * Design notes:
 *  - Original DB / docs / code context is preserved server-side via a
 *    short-lived ``rerun_context_snapshots`` row; this dialog only
 *    captures the *delta* the user wants to layer on top.
 *  - The summary line lists every target so the user can sanity-check
 *    what the re-run will touch before submitting.
 *  - Advanced fields (the full LLM-override block) are collapsed by
 *    default so the modal stays focused on the common path: type
 *    extra guidance → submit.
 *  - Defaults shown in the Advanced block come from the active LLM
 *    profile (via ``api.context().llm_profile_defaults``). When a
 *    batch re-run spans multiple parent runs / profiles, a banner
 *    above the Advanced body notes that the defaults reflect the
 *    first selected item; overrides apply uniformly to every target.
 */

import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Loader2, RefreshCw } from "lucide-react";

import { api, apiFetch } from "../lib/api";
import type { LLMProfileDefaults } from "../lib/api";
import { useLLMCapabilities } from "../lib/llmCapabilities";
import AdvancedLLMOverrides, {
  EMPTY_OVERRIDES,
  buildOverridesPayload,
  seedFromDefaults,
  type OverrideFormState,
  type ProfileOption,
} from "./AdvancedLLMOverrides";
import { Button, Dialog, Textarea, useToast } from "./ui";

export interface RerunTarget {
  /** ``run_results.id`` of the original (or latest) row to re-run. */
  resultId: number;
  /** Human-readable label (``schema.table.column`` etc) for the summary. */
  label: string;
}

interface Props {
  open: boolean;
  onClose: () => void;
  targets: RerunTarget[];
  /** Called once the worker has been spawned successfully — caller
   *  uses ``jobId`` to subscribe to the SSE stream and refresh
   *  results when ``job.done`` arrives. */
  onSubmitted: (jobId: string) => void;
  /** Read-only summary of the original run's stack so the user can see
   *  which DB / docs / code / LLM the re-run will use. Optional. */
  contextSummary?: string;
}

export default function RerunDialog({
  open,
  onClose,
  targets,
  onSubmitted,
  contextSummary,
}: Props) {
  const { push: pushToast } = useToast();
  const [instructions, setInstructions] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [overrides, setOverrides] = useState<OverrideFormState>(EMPTY_OVERRIDES);
  const [submitting, setSubmitting] = useState(false);

  // Seed the Advanced block with the active LLM profile's defaults so
  // every input shows a real value instead of an empty box — matches
  // RunNew's seeding pattern. The Re-Run modal does NOT today fetch
  // the parent run's per-run override snapshot (that would require a
  // join through ``analysis_runs.settings_json``); the active profile
  // is the right baseline for "what would the re-run inherit if the
  // user doesn't override anything?".
  const contextQ = useQuery({
    queryKey: ["context", "rerun-defaults"],
    queryFn: () => api.context(),
    enabled: open,
    staleTime: 60_000,
  });
  const defaults: LLMProfileDefaults | null = contextQ.data?.llm_profile_defaults ?? null;
  const profileName: string | null = contextQ.data?.active_llm_profile ?? null;
  // Profile picker: list every saved profile so the user can pick a
  // different model for this single re-run without leaving the modal.
  // ``has_credentials`` is back-filled lazily on the backend.
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
  // Effective (provider, model) for capability gating: when the user
  // picks a different profile in the picker, those values win;
  // otherwise we read from context.
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

  // Whenever the dialog re-opens or the profile defaults arrive, seed
  // the form. We only seed on the open→true transition so user-typed
  // values aren't clobbered mid-edit.
  useEffect(() => {
    if (open) {
      setOverrides(seedFromDefaults(defaults));
    }
  }, [open, defaults]);

  // The heterogeneous-batch note: surfaced when more than one target
  // is selected. We can't cheaply prove the targets actually have
  // different profiles without an extra fetch — the safer / simpler
  // UX is to inform the user that batch overrides apply uniformly
  // whenever N > 1.
  const isMulti = targets.length > 1;
  const heterogeneousNote = useMemo(() => {
    if (!isMulti) return null;
    return (
      <p className="text-[11px] text-ink-dim">
        Defaults shown reflect your active LLM profile{profileName ? (
          <>
            {" "}
            (<span className="font-mono">{profileName}</span>)
          </>
        ) : null}
        . Overrides apply uniformly to all {targets.length} selected items.
      </p>
    );
  }, [isMulti, profileName, targets.length]);

  const submit = async () => {
    if (!targets.length) {
      pushToast({ tone: "error", title: "No targets selected to re-run." });
      return;
    }
    setSubmitting(true);
    try {
      const llmOverrides = buildOverridesPayload(overrides, defaults);
      // Back-compat: existing wire-shape consumers still read
      // ``temperature_override``. Send both — when the new
      // ``llm_overrides.temperature`` is set, the legacy field carries
      // the same value so a stale backend stays consistent with the
      // new client.
      const legacyTemp =
        typeof llmOverrides?.temperature === "number"
          ? Math.max(0, Math.min(1, llmOverrides.temperature))
          : null;
      const res = await api.rerunItems({
        result_ids: targets.map((t) => t.resultId),
        user_instructions: instructions.trim() || null,
        temperature_override: legacyTemp,
        llm_overrides: llmOverrides,
      });
      onSubmitted(res.job_id);
      // Reset form fields so the dialog opens clean next time.
      setInstructions("");
      setOverrides(EMPTY_OVERRIDES);
      setAdvancedOpen(false);
      onClose();
    } catch (err) {
      pushToast({
        tone: "error",
        title: "Re-run failed to start",
        description: err instanceof Error ? err.message : String(err),
      });
    } finally {
      setSubmitting(false);
    }
  };

  const targetCount = targets.length;
  const titleLabel =
    targetCount === 1 ? "Re-Run this item" : `Re-Run ${targetCount} items`;

  return (
    <Dialog
      open={open}
      onClose={submitting ? () => undefined : onClose}
      title={
        <span className="inline-flex items-center gap-2">
          <RefreshCw size={14} className="text-accent" /> {titleLabel}
        </span>
      }
      description={
        contextSummary ||
        "Original DB, docs, codebase context is preserved. Anything you type below is appended as additional guidance — the agents still see all the inputs from the original run."
      }
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
            disabled={submitting || targetCount === 0 || credentialsMissing}
          >
            {submitting ? (
              <span className="inline-flex items-center gap-2">
                <Loader2 size={14} className="animate-spin" /> Starting…
              </span>
            ) : (
              <span className="inline-flex items-center gap-2">
                <RefreshCw size={14} /> Re-Run
              </span>
            )}
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        {isMulti && (
          <div>
            <div className="mb-1 text-[10px] uppercase tracking-wider text-ink-dim">
              Targets
            </div>
            <ul className="max-h-32 overflow-y-auto rounded-md border border-border bg-surface-subtle/40 px-3 py-2 text-xs">
              {targets.map((t) => (
                <li key={t.resultId} className="font-mono text-ink-muted">
                  {t.label}
                </li>
              ))}
            </ul>
          </div>
        )}
        {!isMulti && targets[0] && (
          <div className="rounded-md border border-border bg-surface-subtle/40 px-3 py-2 text-xs">
            <span className="text-[10px] uppercase tracking-wider text-ink-dim">
              Target
            </span>
            <div className="mt-0.5 font-mono text-ink">{targets[0].label}</div>
          </div>
        )}

        <div>
          <label
            htmlFor="rerun-instructions"
            className="mb-1 block text-[10px] uppercase tracking-wider text-ink-dim"
          >
            Additional instructions (optional)
          </label>
          <Textarea
            id="rerun-instructions"
            placeholder='e.g. "Note that this column also includes soft-deleted rows."'
            value={instructions}
            onChange={(e) => setInstructions(e.target.value)}
            rows={4}
            disabled={submitting}
          />
          <p className="mt-1 text-[11px] text-ink-dim">
            Agents always re-read the original DB / docs / codebase
            inputs. This text is appended to bias the new alternatives.
          </p>
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
          prelude={heterogeneousNote}
          profiles={profiles}
          supportsThinking={supportsThinking}
          supportsLogprobs={supportsLogprobs}
          effectiveModel={effectiveModel}
        />
        {credentialsMissing && (
          <p className="rounded-md border border-warn/40 bg-warn/10 px-3 py-2 text-[11px] text-warn">
            The selected LLM profile is missing credentials. Open Settings →
            LLM → {overrides.profile} to add an API key before re-running.
          </p>
        )}
      </div>
    </Dialog>
  );
}
