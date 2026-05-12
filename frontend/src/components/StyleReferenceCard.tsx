import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { apiFetch } from "../lib/api";
import { Card, CardBody, CardHeader } from "./Card";

type StoredStyleProfile = {
  llm_profile: string;
  source_ref: string;
  source_db_kind: string;
  enabled: boolean;
  sample_count: number;
  profile: {
    language: string;
    tone: string;
    avg_length_words: number;
    length_range: [number, number];
    person: string;
    capitalization: string;
    ends_with_period: boolean;
    structural_patterns: string[];
    vocabulary_register: string;
    redacted_examples: string[];
  };
  created_at: number;
  updated_at: number;
};

export function StyleReferenceCard({ llmProfile }: { llmProfile: string | null }) {
  const qc = useQueryClient();
  const [sourceRef, setSourceRef] = useState("");

  const profileName = llmProfile ?? "";
  const enabled = Boolean(profileName);

  const styleQuery = useQuery<StoredStyleProfile | null>({
    queryKey: ["style", profileName],
    queryFn: async () => {
      try {
        return await apiFetch<StoredStyleProfile>(
          `/api/llm-profiles/${encodeURIComponent(profileName)}/style`,
        );
      } catch (e) {
        const status = (e as { status?: number }).status;
        if (status === 404) return null;
        throw e;
      }
    },
    enabled,
    retry: false,
  });

  const extract = useMutation({
    mutationFn: (body: { source_ref: string }) =>
      apiFetch(`/api/llm-profiles/${encodeURIComponent(profileName)}/style/extract`, {
        method: "POST",
        body: JSON.stringify(body),
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["style", profileName] }),
  });

  const toggle = useMutation({
    mutationFn: (next: boolean) =>
      apiFetch(`/api/llm-profiles/${encodeURIComponent(profileName)}/style`, {
        method: "PATCH",
        body: JSON.stringify({ enabled: next }),
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["style", profileName] }),
  });

  const clear = useMutation({
    mutationFn: () =>
      apiFetch(`/api/llm-profiles/${encodeURIComponent(profileName)}/style`, {
        method: "DELETE",
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["style", profileName] }),
  });

  if (!profileName) {
    return (
      <Card>
        <CardHeader
          title="Writing style reference"
          description="Activate an LLM profile to configure a style reference."
        />
      </Card>
    );
  }

  const row = styleQuery.data;
  const isBusy = extract.isPending || toggle.isPending || clear.isPending;

  return (
    <Card>
      <CardHeader
        title="Writing style reference"
        description={
          row
            ? `Attached to '${profileName}'. AMX matches this style on runs.`
            : `Attach a reference table so AMX matches your description style for '${profileName}'.`
        }
      />
      <CardBody>
        {extract.isPending ? (
          <div className="text-sm text-ink-dim">Extracting style…</div>
        ) : row ? (
          <div className="space-y-3 text-sm">
            <div>
              Source: <code className="font-mono">{row.source_ref}</code> ·{" "}
              backend: <span className="font-mono">{row.source_db_kind}</span>
            </div>
            <div className="grid grid-cols-2 gap-2 text-ink-muted">
              <div>Language: {row.profile.language}</div>
              <div>Tone: {row.profile.tone}</div>
              <div>
                Length: {row.profile.length_range[0]}–{row.profile.length_range[1]} words
              </div>
              <div>Samples: {row.sample_count}</div>
            </div>
            <details>
              <summary className="cursor-pointer text-xs text-ink-dim">
                Raw profile JSON
              </summary>
              <pre className="mt-2 max-h-72 overflow-auto rounded-md bg-surface-subtle p-3 text-xs">
                {JSON.stringify(row.profile, null, 2)}
              </pre>
            </details>
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={row.enabled}
                disabled={isBusy}
                onChange={(e) => toggle.mutate(e.target.checked)}
              />
              Use this style on runs
            </label>
            <div className="flex flex-wrap gap-2">
              <input
                type="text"
                placeholder="db.schema.table"
                value={sourceRef}
                onChange={(e) => setSourceRef(e.target.value)}
                className="flex-1 rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm"
              />
              <button
                type="button"
                disabled={!sourceRef || isBusy}
                onClick={() => extract.mutate({ source_ref: sourceRef })}
                className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-50"
              >
                Re-extract
              </button>
              <button
                type="button"
                disabled={isBusy}
                onClick={() => {
                  if (confirm(`Clear style reference for '${profileName}'?`)) {
                    clear.mutate();
                  }
                }}
                className="rounded-md border border-surface-border px-3 py-1.5 text-sm text-ink-muted hover:bg-surface-subtle"
              >
                Clear
              </button>
            </div>
          </div>
        ) : (
          <div className="space-y-3 text-sm">
            <p className="text-ink-muted">
              Provide a reference table whose column comments show your team's
              writing style. AMX reads only column comments — never row data —
              and distills a style profile used on future runs. Domain terms
              from the reference table are never leaked into other tables'
              descriptions.
            </p>
            <div className="flex flex-wrap gap-2">
              <input
                type="text"
                placeholder="db.schema.table"
                value={sourceRef}
                onChange={(e) => setSourceRef(e.target.value)}
                className="flex-1 rounded-md border border-surface-border bg-surface px-3 py-1.5 text-sm"
              />
              <button
                type="button"
                disabled={!sourceRef || isBusy}
                onClick={() => extract.mutate({ source_ref: sourceRef })}
                className="rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:opacity-50"
              >
                Extract style
              </button>
            </div>
          </div>
        )}
        {extract.isError && (
          <div className="mt-3 rounded-md bg-critical/10 px-3 py-2 text-sm text-critical">
            Extract failed: {(extract.error as Error).message}
          </div>
        )}
      </CardBody>
    </Card>
  );
}
