/**
 * IngestDialog — multi-select asset-type picker + SSE progress display.
 *
 * On submit, POSTs to /api/assets/ingest, then opens an EventSource
 * to stream per-type progress events. The final "completed" event
 * surfaces a count summary. Errors are surfaced inline.
 */

import { useEffect, useRef, useState } from "react";
import { Loader2 } from "lucide-react";

import { api, type RemoteAssetIngestEvent } from "../../lib/api";
import { Button, Dialog, Field, Input } from "../ui";

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
  const [done, setDone] = useState(false);
  const esRef = useRef<EventSource | null>(null);

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
    setDone(false);
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

  async function handleSubmit() {
    if (selectedTypes.size === 0) {
      setError("Select at least one asset type.");
      return;
    }
    setError(null);
    setSubmitting(true);
    setTypeStatuses({});
    setFinalCounts(null);
    setDone(false);

    const days = parseInt(historyDays, 10);
    try {
      const { job_id } = await api.startIngestAssets({
        profile,
        types: Array.from(selectedTypes),
        history_days: isNaN(days) ? 30 : days,
      });

      const es = new EventSource(`/api/assets/ingest/${encodeURIComponent(job_id)}/events`);
      esRef.current = es;

      es.onmessage = (evt) => {
        try {
          const data = JSON.parse(evt.data) as RemoteAssetIngestEvent;

          if (data.state === "completed") {
            setFinalCounts(data.counts ?? {});
            setDone(true);
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
      size="md"
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
                        <span className="text-[11px] text-critical">fail</span>
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

        {/* Error */}
        {error && (
          <p className="rounded-md bg-critical/10 px-3 py-2 text-sm text-critical">
            {error}
          </p>
        )}

        {/* Final summary */}
        {done && finalCounts && (
          <div className="rounded-md border border-positive/30 bg-positive/5 px-3 py-2">
            <p className="mb-1.5 text-sm font-medium text-positive">
              Ingestion completed
            </p>
            <dl className="grid grid-cols-2 gap-x-4 gap-y-0.5 text-xs text-ink-muted sm:grid-cols-3">
              {Object.entries(finalCounts).map(([k, v]) => (
                <div key={k} className="flex gap-1">
                  <dt className="text-ink-dim">{k}:</dt>
                  <dd className="font-mono">{v}</dd>
                </div>
              ))}
            </dl>
          </div>
        )}
      </div>
    </Dialog>
  );
}
