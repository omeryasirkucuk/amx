/**
 * IngestDialog — multi-select asset-type picker + SSE progress display.
 *
 * On submit, POSTs to /api/assets/ingest, then opens an EventSource
 * to stream per-type progress events. The final "completed" event
 * surfaces a count summary. Errors are surfaced inline.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { Loader2 } from "lucide-react";
import { useQueryClient } from "@tanstack/react-query";

import { api, type RemoteAssetIngestEvent } from "../../lib/api";
import { Button, Dialog, Field, Input } from "../ui";
import AssetBrowsePicker from "./AssetBrowsePicker";

// Ingest is pull-only — chunking + embedding happens via the
// per-asset Chunk button or `/db assets reindex`. ``indexed_chunks``
// always lands as ``0`` and was confusing users on the completion
// banner, so hide it. (Keep the field on the wire so older callers
// don't break; we just don't render the count.)
const _HIDDEN_FINAL_COUNT_KEYS = new Set(["indexed_chunks"]);

// Map the orchestrator's plural ``asset_type`` events back to the
// singular kind that AssetTable's queryKey uses. Naive ``replace(/s$/, "")``
// loses the "streamlit_app" → "streamlit" alias and turns "queries"
// into "querie", so the per-kind invalidation needs an explicit map.
const _ASSET_TYPE_TO_KIND: Record<string, string> = {
  notebooks: "notebook",
  jobs: "job",
  pipelines: "pipeline",
  streamlit_apps: "streamlit",
  streams: "stream",
  queries: "query",
};

// Kinds the discover endpoint serves — anything outside this set
// stays in "ingest all" mode even when the picker is open.
// ``queries`` + ``task_dependencies`` are time-windowed aggregates,
// not per-asset rows the user picks individually.
const PICKABLE_KIND_IDS = new Set([
  "notebooks",
  "jobs",
  "pipelines",
  "streamlit_apps",
  "streams",
]);

const ASSET_TYPE_OPTIONS: Array<{ id: string; label: string }> = [
  { id: "notebooks", label: "Notebooks" },
  { id: "jobs", label: "Jobs" },
  { id: "pipelines", label: "Pipelines" },
  { id: "streamlit_apps", label: "Streamlit apps" },
  { id: "streams", label: "Streams" },
  { id: "task_dependencies", label: "Task dependencies" },
  { id: "queries", label: "Queries" },
];

interface TypeStatus {
  state: "idle" | "started" | "running" | "completed" | "failed";
  count?: number | null;
  message?: string | null;
}

interface Props {
  open: boolean;
  onClose: () => void;
  profile: string;
}

export default function IngestDialog({ open, onClose, profile }: Props) {
  const [selectedTypes, setSelectedTypes] = useState<Set<string>>(
    new Set(ASSET_TYPE_OPTIONS.map((o) => o.id)),
  );
  const [historyDays, setHistoryDays] = useState("30");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [typeStatuses, setTypeStatuses] = useState<Record<string, TypeStatus>>(
    {},
  );
  const [finalCounts, setFinalCounts] = useState<Record<string, number> | null>(
    null,
  );
  const [finalFailures, setFinalFailures] = useState<Record<string, string> | null>(
    null,
  );
  const [done, setDone] = useState(false);
  // PR-A: per-kind cherry-pick state. Empty (or pickerOpen=false)
  // means "ingest every asset of each selected type". Toggled on
  // by the "Pick specific assets" disclosure below.
  const [pickerOpen, setPickerOpen] = useState(false);
  const [selection, setSelection] = useState<Record<string, Set<string>>>({});
  const esRef = useRef<EventSource | null>(null);
  const allCheckboxRef = useRef<HTMLInputElement>(null);
  // After an ingest completes we invalidate the Assets table queries
  // so the row that just landed in remote_notebooks shows up without
  // the user closing the dialog and reloading the page.
  const queryClient = useQueryClient();

  const pickableSelectedKinds = useMemo(
    () => Array.from(selectedTypes).filter((id) => PICKABLE_KIND_IDS.has(id)),
    [selectedTypes],
  );

  // ``indeterminate`` is not a React prop — it has to be set imperatively
  // after the input mounts. Keep it in sync with selectedTypes so the
  // master checkbox correctly shows the partial-selection state.
  useEffect(() => {
    if (allCheckboxRef.current) {
      const count = selectedTypes.size;
      allCheckboxRef.current.indeterminate =
        count > 0 && count < ASSET_TYPE_OPTIONS.length;
    }
  }, [selectedTypes]);

  // Clean up SSE connection when dialog closes.
  useEffect(() => {
    if (!open) {
      esRef.current?.close();
      esRef.current = null;
    }
  }, [open]);

  function resetState() {
    setSubmitting(false);
    setError(null);
    setTypeStatuses({});
    setFinalCounts(null);
    setFinalFailures(null);
    setDone(false);
    setPickerOpen(false);
    setSelection({});
    esRef.current?.close();
    esRef.current = null;
  }

  function handleClose() {
    resetState();
    onClose();
  }

  function toggleType(id: string) {
    setSelectedTypes((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function toggleAll() {
    setSelectedTypes((prev) =>
      prev.size === ASSET_TYPE_OPTIONS.length
        ? new Set()
        : new Set(ASSET_TYPE_OPTIONS.map((o) => o.id)),
    );
  }

  async function handleSubmit() {
    if (selectedTypes.size === 0) {
      setError("Select at least one asset type.");
      return;
    }
    setError(null);
    setSubmitting(true);
    setTypeStatuses({});
    setFinalCounts(null);
    setFinalFailures(null);
    setDone(false);

    const days = parseInt(historyDays, 10);
    // Only fold selection into the body when the picker is open and
    // the user actually picked at least one row. An empty Set for a
    // kind would otherwise instruct the backend to ingest nothing
    // for it — surface that explicitly so the user can correct it.
    let selectionPayload: Record<string, string[]> | undefined;
    if (pickerOpen) {
      const built: Record<string, string[]> = {};
      let pickedSomething = false;
      for (const kind of pickableSelectedKinds) {
        const ids = Array.from(selection[kind] ?? []);
        built[kind] = ids;
        if (ids.length > 0) pickedSomething = true;
      }
      if (!pickedSomething && pickableSelectedKinds.length > 0) {
        setError(
          'Pick at least one asset in the "Browse" step, or turn it off to ingest everything.',
        );
        setSubmitting(false);
        return;
      }
      selectionPayload = built;
    }

    try {
      const { job_id } = await api.startIngestAssets({
        profile,
        types: Array.from(selectedTypes),
        history_days: isNaN(days) ? 30 : days,
        selection: selectionPayload,
      });

      const es = new EventSource(`/api/assets/ingest/${encodeURIComponent(job_id)}/events`);
      esRef.current = es;

      es.onmessage = (evt) => {
        try {
          const data = JSON.parse(evt.data) as RemoteAssetIngestEvent;

          // The orchestrator emits ``state: "completed"`` once per asset
          // type (with ``asset_type`` set and a single-integer ``count``)
          // AND again per intermediate stage (``asset_type: "storage"``,
          // ``asset_type: "lineage"``). The router then sends the FINAL
          // completion event with no ``asset_type`` but a full ``counts``
          // dict + ``failures`` dict. Only that final event closes the
          // stream and renders the banner.
          const isFinalCompletion =
            data.state === "completed" &&
            !data.asset_type &&
            data.counts !== undefined;

          if (isFinalCompletion) {
            setFinalCounts(data.counts ?? {});
            setFinalFailures(data.failures ?? {});
            setDone(true);
            setSubmitting(false);
            es.close();
            esRef.current = null;
            // Final invalidation — every assets-related query for this
            // profile gets a fresh fetch so the table reflects the new
            // rows immediately.
            queryClient.invalidateQueries({
              queryKey: ["remote-assets", profile],
            });
            return;
          }

          // Terminal error from the router's outer try/except (the service
          // itself crashed before producing any per-type results).
          if (data.state === "error") {
            setError(data.message ?? "Ingestion failed.");
            setSubmitting(false);
            es.close();
            esRef.current = null;
            return;
          }

          if (data.asset_type) {
            setTypeStatuses((prev) => ({
              ...prev,
              [data.asset_type as string]: {
                state: data.state as TypeStatus["state"],
                count: data.count,
                message: data.message,
              },
            }));
            // Per-kind real-time refresh: as each asset_type lands
            // ``state: "completed"``, invalidate that kind's table
            // query so the Studio rows trickle in while the next
            // kind is still pulling. ``storage`` / ``lineage`` /
            // ``indexing`` stages don't map to a Studio tab so we
            // skip them.
            if (data.state === "completed" && data.asset_type) {
              const singular = _ASSET_TYPE_TO_KIND[data.asset_type];
              if (singular) {
                queryClient.invalidateQueries({
                  queryKey: ["remote-assets", profile, singular],
                });
              }
            }
          }
        } catch {
          // Non-JSON event — ignore
        }
      };

      es.onerror = () => {
        setError("SSE connection dropped. The job may still be running in the background.");
        setSubmitting(false);
        es.close();
        esRef.current = null;
      };
    } catch (err) {
      setError((err as Error).message ?? "Ingestion failed.");
      setSubmitting(false);
    }
  }

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      title="Ingest remote assets"
      size={pickerOpen ? "xl" : "md"}
      footer={
        done ? (
          <Button variant="primary" onClick={handleClose}>
            Close
          </Button>
        ) : (
          <>
            <Button variant="ghost" onClick={handleClose} disabled={submitting}>
              Cancel
            </Button>
            <Button
              variant="primary"
              onClick={handleSubmit}
              loading={submitting}
              disabled={submitting || selectedTypes.size === 0}
            >
              {submitting ? "Ingesting…" : "Start ingestion"}
            </Button>
          </>
        )
      }
    >
      <div className="space-y-4">
        {/* Asset type multi-select */}
        <Field label="Asset types">
          <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-2">
            {/* Master toggle — spans both columns so it reads as a group
                header rather than a sibling option. Indeterminate when
                some-but-not-all rows are selected; see useEffect above. */}
            <label
              className="col-span-2 flex cursor-pointer items-center gap-2 rounded-md border border-dashed border-border px-2.5 py-1.5 text-sm font-medium text-ink-muted hover:bg-surface-subtle"
            >
              <input
                ref={allCheckboxRef}
                type="checkbox"
                checked={selectedTypes.size === ASSET_TYPE_OPTIONS.length}
                onChange={toggleAll}
                disabled={submitting || done}
                className="h-3.5 w-3.5 accent-accent"
                aria-label="Select all asset types"
              />
              <span className="flex-1">
                {selectedTypes.size === ASSET_TYPE_OPTIONS.length
                  ? "All selected"
                  : selectedTypes.size === 0
                    ? "Select all"
                    : `${selectedTypes.size} of ${ASSET_TYPE_OPTIONS.length} selected`}
              </span>
            </label>
            {ASSET_TYPE_OPTIONS.map((opt) => {
              const status = typeStatuses[opt.id];
              return (
                <label
                  key={opt.id}
                  className="flex cursor-pointer items-center gap-2 rounded-md border border-border px-2.5 py-1.5 text-sm hover:bg-surface-subtle"
                >
                  <input
                    type="checkbox"
                    checked={selectedTypes.has(opt.id)}
                    onChange={() => toggleType(opt.id)}
                    disabled={submitting || done}
                    className="h-3.5 w-3.5 accent-accent"
                  />
                  <span className="flex-1 text-ink">{opt.label}</span>
                  {status && (
                    <span className="shrink-0">
                      {status.state === "started" || status.state === "running" ? (
                        <Loader2 size={12} className="animate-spin text-accent" />
                      ) : status.state === "completed" ? (
                        <span className="text-[11px] text-positive">
                          {status.count != null ? `${status.count}` : "done"}
                        </span>
                      ) : status.state === "failed" ? (
                        <span
                          className="text-[11px] text-critical"
                          title={status.message ?? ""}
                        >
                          fail
                        </span>
                      ) : null}
                    </span>
                  )}
                </label>
              );
            })}
          </div>
        </Field>

        {/* History days */}
        <Field label="History days">
          <Input
            type="number"
            value={historyDays}
            onChange={(e) => setHistoryDays(e.target.value)}
            min={1}
            max={365}
            disabled={submitting || done}
            className="w-24"
          />
        </Field>

        {/* PR-A: optional cherry-pick. Closed by default — "ingest
            all of each selected type" is the common path. When open,
            user can tick individual external_ids per kind. */}
        <div className="rounded-md border border-border">
          <label className="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm">
            <input
              type="checkbox"
              checked={pickerOpen}
              onChange={(e) => setPickerOpen(e.target.checked)}
              disabled={submitting || done || pickableSelectedKinds.length === 0}
              className="h-3.5 w-3.5 accent-accent"
            />
            <span className="flex-1 font-medium text-ink">
              Pick specific assets instead of ingesting all
            </span>
            {pickerOpen && (
              <span className="text-[11px] text-ink-muted">
                {Object.values(selection).reduce(
                  (sum, s) => sum + s.size,
                  0,
                )}{" "}
                selected
              </span>
            )}
          </label>
          {pickerOpen && (
            <div className="border-t border-border px-3 py-3">
              <AssetBrowsePicker
                profile={profile}
                enabledKinds={pickableSelectedKinds}
                selection={selection}
                onSelectionChange={setSelection}
                disabled={submitting || done}
              />
            </div>
          )}
        </div>

        {/* Error */}
        {error && (
          <p className="rounded-md bg-critical/10 px-3 py-2 text-sm text-critical">
            {error}
          </p>
        )}

        {/* Final summary — colored by outcome */}
        {done && finalCounts && (() => {
          const failureCount = finalFailures ? Object.keys(finalFailures).length : 0;
          const totalRequested = Array.from(selectedTypes).length;
          const allFailed = failureCount > 0 && failureCount >= totalRequested;
          const partialFailure = failureCount > 0 && !allFailed;
          const wrapper = allFailed
            ? "rounded-md border border-critical/30 bg-critical/5 px-3 py-2"
            : partialFailure
              ? "rounded-md border border-warning/40 bg-warning/5 px-3 py-2"
              : "rounded-md border border-positive/30 bg-positive/5 px-3 py-2";
          const headlineColor = allFailed
            ? "text-critical"
            : partialFailure
              ? "text-warning"
              : "text-positive";
          const headline = allFailed
            ? "Ingestion failed for every selected asset type"
            : partialFailure
              ? `Ingestion completed with ${failureCount} failure${failureCount === 1 ? "" : "s"}`
              : "Ingestion completed";
          return (
            <div className={wrapper}>
              <p className={`mb-1.5 text-sm font-medium ${headlineColor}`}>{headline}</p>
              <dl className="grid grid-cols-2 gap-x-4 gap-y-0.5 text-xs text-ink-muted sm:grid-cols-3">
                {Object.entries(finalCounts)
                  .filter(([k]) => !_HIDDEN_FINAL_COUNT_KEYS.has(k))
                  .map(([k, v]) => (
                    <div key={k} className="flex gap-1">
                      <dt className="text-ink-dim">{k}:</dt>
                      <dd className="font-mono">{v}</dd>
                    </div>
                  ))}
              </dl>
              {/* Pull-only ingest: tell the user where chunking + embedding
                  live now so they don't expect indexed_chunks > 0. */}
              {!allFailed && (
                <p className="mt-2 border-t border-border/40 pt-2 text-[11px] text-ink-dim">
                  Chunking + embedding is a separate step. Use the{" "}
                  <span className="font-mono">Chunk</span> action on each row
                  of the Assets table, or run{" "}
                  <code className="rounded bg-surface-subtle px-1">
                    /db assets reindex
                  </code>{" "}
                  to embed everything under the active chunking config.
                </p>
              )}
              {finalFailures && Object.keys(finalFailures).length > 0 && (
                <div className="mt-2 border-t border-border/50 pt-2">
                  <p className="mb-1 text-xs font-medium text-critical">
                    Failures (open the matching profile in /db-profiles to fix
                    credentials):
                  </p>
                  <ul className="space-y-1 text-[11px] text-ink-muted">
                    {Object.entries(finalFailures).map(([type, msg]) => (
                      <li key={type}>
                        <span className="font-mono text-critical">{type}</span>
                        {": "}
                        <span className="break-words">{msg}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          );
        })()}
      </div>
    </Dialog>
  );
}
