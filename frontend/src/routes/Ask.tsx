import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CircleStop, Plus, Sparkles } from "lucide-react";
import { useState } from "react";

import PageHeader from "../components/PageHeader";
import AskChat, { type SubmittedTurn } from "../components/AskChat";
import { Card, CardBody, CardHeader } from "../components/Card";
import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";
import { Skeleton, Tooltip, useToast } from "../components/ui";

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

  const toast = useToast();
  const endSession = useMutation({
    mutationFn: (id: number) =>
      apiFetch(`/api/ask/sessions/${id}/end`, { method: "POST" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["ask-sessions"] });
      toast.push({ title: "Session ended", tone: "info", duration: 2200 });
    },
    onError: (e: Error) =>
      toast.push({
        title: "Could not end session",
        description: e.message,
        tone: "error",
      }),
  });

  return (
    <>
      <PageHeader title="Ask" breadcrumbs={[{ label: "Ask" }]} />

      <div className="grid gap-4 md:grid-cols-[18rem_1fr]">
        <Card>
          <CardHeader
            title="Sessions"
            actions={
              <button
                type="button"
                onClick={startNewSession}
                className="inline-flex items-center gap-1 rounded-md border border-border bg-surface px-2 py-1 text-[11px] font-medium text-ink-muted transition-colors duration-fast hover:border-accent/40 hover:text-ink"
              >
                <Plus size={12} /> New
              </button>
            }
          />
          <CardBody className="max-h-[60vh] overflow-y-auto p-0">
            {sessions.isLoading ? (
              <ul className="divide-y divide-border">
                {Array.from({ length: 4 }).map((_, i) => (
                  <li key={i} className="px-4 py-3 space-y-1.5">
                    <Skeleton className="h-3 w-2/3" />
                    <Skeleton className="h-3 w-full" />
                  </li>
                ))}
              </ul>
            ) : sessions.data?.sessions?.length ? (
              <ul className="divide-y divide-surface-border">
                {sessions.data.sessions.map((session) => {
                  const isActive = session.id === selectedSessionId;
                  const isLoading = session.id === loadingSessionId;
                  const isOpen = session.ended_at == null;
                  return (
                    <li
                      key={session.id}
                      aria-current={isActive ? "true" : undefined}
                      className={cn(
                        "group flex items-stretch transition hover:bg-surface-subtle/60",
                        isActive && "bg-surface-subtle",
                        isOpen && "border-l-2 border-positive",
                        isActive && "border-l-2 border-accent",
                      )}
                    >
                      <button
                        type="button"
                        onClick={() => openSession(session.id)}
                        disabled={isLoading}
                        className={cn(
                          "flex-1 px-4 py-3 text-left text-sm focus:outline-none",
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
                      {isOpen && (
                        <Tooltip content="End session — next /ask starts a new one">
                          <button
                            type="button"
                            onClick={(e) => {
                              e.stopPropagation();
                              endSession.mutate(session.id);
                            }}
                            disabled={endSession.isPending}
                            aria-label="End session"
                            className="px-2 text-ink-dim opacity-0 transition-colors duration-fast hover:bg-warning-soft hover:text-warning group-hover:opacity-100 disabled:opacity-50"
                          >
                            <CircleStop size={14} />
                          </button>
                        </Tooltip>
                      )}
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
