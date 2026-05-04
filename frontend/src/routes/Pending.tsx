import { useState } from "react";
import { Inbox, Play } from "lucide-react";

import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";
import JobProgress from "../components/JobProgress";
import { apiFetch } from "../lib/api";

export default function Pending() {
  const [activeJob, setActiveJob] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);

  async function handleApplyPending() {
    setSubmitError(null);
    try {
      const result = await apiFetch<{ job_id: string; status: string }>(
        "/api/apply",
        { method: "POST", body: JSON.stringify({}) },
      );
      setActiveJob(result.job_id);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Apply failed.";
      setSubmitError(message);
    }
  }

  async function handleCancel() {
    if (!activeJob) return;
    try {
      await apiFetch(`/api/apply/${activeJob}/cancel`, { method: "POST" });
    } catch {
      /* button already swaps state when SSE emits job.cancelled */
    }
  }

  return (
    <>
      <PageHeader
        eyebrow="Review"
        title="Pending review"
        description="Approved suggestions waiting to be written back. The full review queue UI lands in PR-E; today you can apply the entire queue at once."
        actions={
          <button
            type="button"
            onClick={handleApplyPending}
            disabled={!!activeJob && !submitError}
            className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <Play size={14} />
            Apply pending queue
          </button>
        }
      />

      {activeJob ? (
        <JobProgress
          jobId={activeJob}
          kind="apply"
          onCancel={handleCancel}
          onTerminal={() => {
            // Keep the panel visible after the job finishes so the
            // user can read the summary; PR-E adds a "Run another"
            // CTA.
          }}
        />
      ) : submitError ? (
        <EmptyState
          icon={Inbox}
          title="Could not start the apply job"
          description={submitError}
        />
      ) : (
        <EmptyState
          icon={Inbox}
          title="Click Apply pending queue to write back approved descriptions"
          description="The worker reads ~/.amx/pending_metadata.json and writes one COMMENT per row to the live database. You can cancel at any point — already-written rows stay committed."
        />
      )}
    </>
  );
}
