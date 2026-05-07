/**
 * RerunDialog — single + multi-item Re-Run modal.
 *
 * The user clicks ⟳ "Re-Run" on a result row (or "Re-Run selected" in
 * the multi-select toolbar). This dialog gathers the optional
 * free-text addendum and an optional temperature override, then fires
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
 *  - Advanced fields (temperature override) are collapsed by default
 *    so the modal stays focused on the common path: type extra
 *    guidance → submit.
 */

import { useState } from "react";
import { Loader2, RefreshCw } from "lucide-react";

import { api } from "../lib/api";
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
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [temperature, setTemperature] = useState<number | "">("");
  const [submitting, setSubmitting] = useState(false);

  const submit = async () => {
    if (!targets.length) {
      pushToast({ tone: "error", title: "No targets selected to re-run." });
      return;
    }
    setSubmitting(true);
    try {
      const tempValue =
        typeof temperature === "number" && Number.isFinite(temperature)
          ? Math.max(0, Math.min(1, temperature))
          : null;
      const res = await api.rerunItems({
        result_ids: targets.map((t) => t.resultId),
        user_instructions: instructions.trim() || null,
        temperature_override: tempValue,
      });
      onSubmitted(res.job_id);
      // Reset form fields so the dialog opens clean next time.
      setInstructions("");
      setTemperature("");
      setShowAdvanced(false);
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
  const titleLabel = targetCount === 1 ? "Re-Run this item" : `Re-Run ${targetCount} items`;
  const isMulti = targetCount > 1;

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
          <Button
            variant="ghost"
            onClick={onClose}
            disabled={submitting}
          >
            Cancel
          </Button>
          <Button
            variant="primary"
            onClick={submit}
            disabled={submitting || targetCount === 0}
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
            placeholder='e.g. "Bu kolonun soft-delete satırlarını da içerdiğini de göz önünde bulundur."'
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

        <div>
          <button
            type="button"
            className="text-[11px] uppercase tracking-wider text-ink-dim hover:text-ink"
            onClick={() => setShowAdvanced((v) => !v)}
          >
            {showAdvanced ? "Hide advanced" : "Show advanced"}
          </button>
          {showAdvanced && (
            <div className="mt-2 rounded-md border border-dashed border-border px-3 py-2 text-xs">
              <label
                htmlFor="rerun-temperature"
                className="mb-1 block text-[10px] uppercase tracking-wider text-ink-dim"
              >
                Temperature override (0.0 – 1.0)
              </label>
              <input
                id="rerun-temperature"
                type="number"
                step="0.05"
                min="0"
                max="1"
                value={temperature}
                onChange={(e) => {
                  const raw = e.target.value;
                  if (raw === "") {
                    setTemperature("");
                  } else {
                    const num = Number(raw);
                    setTemperature(Number.isFinite(num) ? num : "");
                  }
                }}
                disabled={submitting}
                placeholder="leave blank to use the original run's temperature"
                className="w-full rounded-md border border-border bg-surface-raised px-3 py-1.5 font-mono text-xs text-ink placeholder:text-ink-dim focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20"
              />
              <p className="mt-1 text-[11px] text-ink-dim">
                Higher values (e.g. 0.8) produce more diverse alternatives;
                blank keeps the original profile's temperature.
              </p>
            </div>
          )}
        </div>
      </div>
    </Dialog>
  );
}
