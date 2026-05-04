import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Plus, Sparkles } from "lucide-react";
import { useState } from "react";

import PageHeader from "../components/PageHeader";
import AskChat, { type SubmittedTurn } from "../components/AskChat";
import { Card, CardBody, CardHeader } from "../components/Card";
import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";

interface SessionRow {
  id: number;
  title: string | null;
  first_question: string | null;
  started_at: number | null;
  last_active_at: number | null;
  ended_at: number | null;
}

interface SessionsResponse {
  sessions: SessionRow[];
  count: number;
}

interface SessionTurn {
  role: string;
  question: string;
  answer_summary: string;
  turn_index: number;
  created_at: number | null;
}

interface SessionDetailResponse {
  session: Record<string, unknown>;
  turns: SessionTurn[];
}

function turnsToBubbles(turns: SessionTurn[]): SubmittedTurn[] {
  // Skip 'summary' rows: they're internal compaction markers, not
  // chat messages the user typed or saw.
  const bubbles: SubmittedTurn[] = [];
  for (const t of turns) {
    if (t.role === "user" && t.question) {
      bubbles.push({ role: "user", content: t.question });
    } else if (t.role === "assistant" && t.answer_summary) {
      bubbles.push({ role: "assistant", content: t.answer_summary });
    }
  }
  return bubbles;
}

export default function Ask() {
  const queryClient = useQueryClient();
  const [selectedSessionId, setSelectedSessionId] = useState<number | null>(null);
  const [seedTurns, setSeedTurns] = useState<SubmittedTurn[] | null>(null);
  // Monotonic counter so AskChat reseeds even when the user clicks the
  // currently-loaded session (e.g. to discard local in-progress edits).
  const [seedToken, setSeedToken] = useState(0);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loadingSessionId, setLoadingSessionId] = useState<number | null>(null);

  const sessions = useQuery({
    queryKey: ["ask-sessions"],
    queryFn: () => apiFetch<SessionsResponse>("/api/ask/sessions"),
    retry: false,
    staleTime: 0,
  });

  async function openSession(id: number) {
    setLoadError(null);
    setLoadingSessionId(id);
    try {
      const detail = await apiFetch<SessionDetailResponse>(`/api/ask/sessions/${id}`);
      setSelectedSessionId(id);
      setSeedTurns(turnsToBubbles(detail.turns ?? []));
      setSeedToken((n) => n + 1);
    } catch (err) {
      setLoadError(err instanceof Error ? err.message : "Failed to load session.");
    } finally {
      setLoadingSessionId(null);
    }
  }

  function startNewSession() {
    setSelectedSessionId(null);
    setSeedTurns([]);
    setSeedToken((n) => n + 1);
    setLoadError(null);
  }

  function handleSessionAssigned(id: number | null) {
    if (id != null && id !== selectedSessionId) {
      setSelectedSessionId(id);
    }
    queryClient.invalidateQueries({ queryKey: ["ask-sessions"] });
  }

  return (
    <>
      <PageHeader
        eyebrow="Conversational"
        title="Ask"
        description="Chat with the AMX search agent over your live database, catalog, and run history."
      />

      <div className="grid gap-4 md:grid-cols-[18rem_1fr]">
        <Card>
          <CardHeader
            title="Sessions"
            description="Recent /ask threads — CLI and visualizer share the same SQLite-backed history."
            actions={
              <button
                type="button"
                onClick={startNewSession}
                className="inline-flex items-center gap-1 rounded-md border border-surface-border bg-surface px-2 py-1 text-[11px] font-medium text-ink-muted transition hover:border-accent/40 hover:text-ink"
              >
                <Plus size={12} /> New
              </button>
            }
          />
          <CardBody className="max-h-[60vh] overflow-y-auto p-0">
            {sessions.isLoading ? (
              <div className="px-5 py-6 text-sm text-ink-dim">Loading…</div>
            ) : sessions.data?.sessions?.length ? (
              <ul className="divide-y divide-surface-border">
                {sessions.data.sessions.map((session) => {
                  const isActive = session.id === selectedSessionId;
                  const isLoading = session.id === loadingSessionId;
                  return (
                    <li key={session.id}>
                      <button
                        type="button"
                        onClick={() => openSession(session.id)}
                        disabled={isLoading}
                        aria-current={isActive ? "true" : undefined}
                        className={cn(
                          "block w-full px-4 py-3 text-left text-sm transition hover:bg-surface-subtle/60 focus:bg-surface-subtle focus:outline-none",
                          isActive && "bg-surface-subtle",
                          session.ended_at == null && "border-l-2 border-positive",
                          isActive && "border-l-2 border-accent",
                          isLoading && "opacity-60",
                        )}
                      >
                        <div className="flex items-center justify-between gap-2">
                          <span className="truncate font-medium">
                            {session.title || `Session #${session.id}`}
                          </span>
                          <span className="font-mono text-[10px] text-ink-dim">
                            #{session.id}
                          </span>
                        </div>
                        {session.first_question && (
                          <p className="mt-1 line-clamp-2 text-xs text-ink-muted">
                            {session.first_question}
                          </p>
                        )}
                      </button>
                    </li>
                  );
                })}
              </ul>
            ) : (
              <div className="flex flex-col items-center px-5 py-10 text-center text-ink-dim">
                <Sparkles size={20} className="mb-2 opacity-60" />
                <p className="text-sm">No sessions yet.</p>
                <p className="mt-1 text-xs text-ink-dim">
                  Ask a question to start your first one.
                </p>
              </div>
            )}
            {loadError && (
              <div className="border-t border-surface-border px-4 py-2 text-[11px] text-critical">
                {loadError}
              </div>
            )}
          </CardBody>
        </Card>

        <AskChat
          selectedSessionId={selectedSessionId}
          seedTurns={seedTurns}
          seedToken={seedToken}
          onSessionAssigned={handleSessionAssigned}
        />
      </div>
    </>
  );
}
