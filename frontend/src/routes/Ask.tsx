import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ChevronLeft,
  ChevronRight,
  CircleStop,
  Plus,
  Settings as SettingsIcon,
  Sparkles,
  Trash2,
} from "lucide-react";
import { useEffect, useState } from "react";
import { Link, useLocation } from "react-router-dom";

import PageHeader from "../components/PageHeader";
import AskChat, { type SubmittedTurn } from "../components/AskChat";
import { Card, CardBody, CardHeader } from "../components/Card";
import { api, apiFetch } from "../lib/api";
import { cn } from "../lib/cn";
import { AlertDialog, Skeleton, Tooltip, useToast } from "../components/ui";

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
  /** "cancelled" on tombstone assistant rows the backend writes when
   *  the user cancels mid-stream. Other values are the agent's regular
   *  intent classification (chitchat / metadata-lookup / etc.). */
  intent?: string;
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
    } else if (t.role === "assistant") {
      // Cancelled tombstone — empty ``answer_summary`` with
      // ``intent === "cancelled"``. Surface as a Cancelled pill in the
      // bubble list (AskChat's ``cancelled`` branch); do NOT skip it,
      // or the user bubble above sits orphaned just like on a fresh
      // cancel.
      if (t.intent === "cancelled") {
        bubbles.push({ role: "assistant", content: "", cancelled: true });
        continue;
      }
      if (t.answer_summary) {
        bubbles.push({ role: "assistant", content: t.answer_summary });
      }
    }
  }
  return bubbles;
}

export default function Ask() {
  const queryClient = useQueryClient();
  const location = useLocation();
  const [selectedSessionId, setSelectedSessionId] = useState<number | null>(null);
  const [seedTurns, setSeedTurns] = useState<SubmittedTurn[] | null>(null);
  // Monotonic counter so AskChat reseeds even when the user clicks the
  // currently-loaded session (e.g. to discard local in-progress edits).
  const [seedToken, setSeedToken] = useState(0);
  // Cross-page hand-off auto-submit slot. When set, AskChat fires this
  // prompt as a real /api/ask call on next mount/seedToken bump, then
  // calls back so we clear it.
  const [seedSubmit, setSeedSubmit] = useState<string | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loadingSessionId, setLoadingSessionId] = useState<number | null>(null);
  const [pendingDeleteSessionId, setPendingDeleteSessionId] = useState<number | null>(null);

  // Sessions sidebar collapse state. Persisted to localStorage so a
  // collapse stays collapsed across reloads (the user opts into the
  // sidebar's footprint deliberately). Default open so first-time
  // users see the history list without having to discover the
  // toggle.
  const SESSIONS_COLLAPSED_KEY = "amx.ask.sessionsCollapsed";
  const [sessionsCollapsed, setSessionsCollapsed] = useState<boolean>(() => {
    try {
      return localStorage.getItem(SESSIONS_COLLAPSED_KEY) === "1";
    } catch {
      return false;
    }
  });
  useEffect(() => {
    try {
      localStorage.setItem(SESSIONS_COLLAPSED_KEY, sessionsCollapsed ? "1" : "0");
    } catch {
      /* ignore quota / private-mode errors */
    }
  }, [sessionsCollapsed]);

  // Cross-page hand-off: when /runs/compare's "Ask AMX" button (or any
  // other deep link) navigates to /ask with state.seedPrompt, fire the
  // prompt automatically — the user already consented to send it by
  // clicking the modal button, so leaving it as a stranded orange
  // bubble (the previous behaviour) was confusing. seedTurns stays
  // empty; AskChat's submitText will push the user turn itself.
  // After consuming we strip the state via history.replaceState so a
  // back/forward bounce doesn't re-seed the same prompt twice.
  useEffect(() => {
    const seed = (location.state as { seedPrompt?: string } | null)?.seedPrompt;
    if (!seed) return;
    setSelectedSessionId(null);
    setSeedTurns([]);
    setSeedSubmit(seed);
    setSeedToken((n) => n + 1);
    setLoadError(null);
    window.history.replaceState({}, "");
    // location.state is the only signal we need; selectedSessionId etc.
    // are set inside the effect and re-running on their changes would
    // create a feedback loop.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.state]);

  // Pre-flight: gate the chat surface on a configured LLM. Without
  // this the user can type a question, click submit, and only then
  // get a 412 ``configure-llm`` error — historically that error was
  // rendered as ``[object Object]`` because apiFetch couldn't parse
  // FastAPI's nested-detail shape. Surfacing the gap upfront with a
  // single CTA avoids the dead-end submit entirely.
  const ctx = useQuery({ queryKey: ["context"], queryFn: () => api.context() });
  const llmReady = !!(
    ctx.data?.llm_provider && ctx.data?.llm_model && ctx.data?.active_llm_profile
  );

  const sessions = useQuery({
    queryKey: ["ask-sessions"],
    queryFn: () => apiFetch<SessionsResponse>("/api/ask/sessions"),
    retry: false,
    staleTime: 0,
  });

  async function openSession(id: number) {
    setLoadError(null);
    setLoadingSessionId(id);
    // Clear the prior conversation synchronously so the chat panel
    // doesn't show the wrong session while the detail GET is in flight.
    // ``selectedSessionId`` flips immediately too, so the sidebar
    // highlights the clicked row right away; AskChat below renders a
    // skeleton when ``loadingSessionId === id``.
    setSelectedSessionId(id);
    setSeedTurns([]);
    setSeedToken((n) => n + 1);
    try {
      const detail = await apiFetch<SessionDetailResponse>(`/api/ask/sessions/${id}`);
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
  const deleteSession = useMutation({
    mutationFn: (id: number) =>
      apiFetch(`/api/ask/sessions/${id}`, { method: "DELETE" }),
    // Optimistic splice: the row disappears the instant the user
    // confirms in the dialog. React Query reconciles with the server on
    // settled; on error we restore the snapshot so the row pops back
    // and the toast explains why.
    onMutate: async (id) => {
      await queryClient.cancelQueries({ queryKey: ["ask-sessions"] });
      const snapshot = queryClient.getQueryData<SessionsResponse>([
        "ask-sessions",
      ]);
      if (snapshot) {
        queryClient.setQueryData<SessionsResponse>(["ask-sessions"], {
          ...snapshot,
          sessions: snapshot.sessions.filter((s) => s.id !== id),
        });
      }
      return { snapshot };
    },
    onSuccess: (_data, id) => {
      // If the deleted session was the one open in the chat panel, drop
      // back to a blank "+ New" state — otherwise the panel keeps
      // showing turns from a session that no longer exists.
      if (selectedSessionId === id) {
        setSelectedSessionId(null);
        setSeedTurns([]);
        setSeedToken((n) => n + 1);
      }
      toast.push({ title: "Session deleted", tone: "info", duration: 2200 });
    },
    onError: (e: Error, _id, ctx) => {
      if (ctx?.snapshot) {
        queryClient.setQueryData(["ask-sessions"], ctx.snapshot);
      }
      toast.push({
        title: "Could not delete session",
        description: e.message,
        tone: "error",
      });
    },
    onSettled: () => {
      queryClient.invalidateQueries({ queryKey: ["ask-sessions"] });
    },
    // Errors render via the inline toast above; skip the global one.
    meta: { silentError: true },
  });
  const pendingDeleteSession =
    pendingDeleteSessionId == null
      ? null
      : sessions.data?.sessions.find((s) => s.id === pendingDeleteSessionId) ?? null;
  const endSession = useMutation({
    mutationFn: (id: number) =>
      apiFetch(`/api/ask/sessions/${id}/end`, { method: "POST" }),
    // Optimistic ``ended_at`` — flips the row's open/closed visual the
    // instant the user clicks, so the warning chip and end button can
    // update without waiting for the round-trip.
    onMutate: async (id) => {
      await queryClient.cancelQueries({ queryKey: ["ask-sessions"] });
      const snapshot = queryClient.getQueryData<SessionsResponse>([
        "ask-sessions",
      ]);
      if (snapshot) {
        const nowEpoch = Math.floor(Date.now() / 1000);
        queryClient.setQueryData<SessionsResponse>(["ask-sessions"], {
          ...snapshot,
          sessions: snapshot.sessions.map((s) =>
            s.id === id ? { ...s, ended_at: s.ended_at ?? nowEpoch } : s,
          ),
        });
      }
      return { snapshot };
    },
    onSuccess: () => {
      toast.push({ title: "Session ended", tone: "info", duration: 2200 });
    },
    onError: (e: Error, _id, ctx) => {
      if (ctx?.snapshot) {
        queryClient.setQueryData(["ask-sessions"], ctx.snapshot);
      }
      toast.push({
        title: "Could not end session",
        description: e.message,
        tone: "error",
      });
    },
    onSettled: () => {
      queryClient.invalidateQueries({ queryKey: ["ask-sessions"] });
    },
    // Errors render via the inline toast above; skip the global one.
    meta: { silentError: true },
  });

  // The chat panel is "empty" when the user has no loaded session AND
  // no seeded turns — i.e. they're already looking at the blank state
  // that "+ New" would create. In that case the button is redundant,
  // so we render it dimmer. Once a session is loaded (or the first
  // question gets a session id assigned), the button regains weight.
  const chatIsEmpty =
    selectedSessionId == null && (seedTurns == null || seedTurns.length === 0);

  return (
    <>
      <PageHeader title="Ask" breadcrumbs={[{ label: "Ask" }]} />

      <div
        className={cn(
          "grid gap-4 [&>*]:min-w-0",
          // Collapsed: a thin rail wide enough for the expand button.
          // Expanded: the historical 18rem session column.
          sessionsCollapsed
            ? "md:grid-cols-[2.25rem_minmax(0,1fr)]"
            : "md:grid-cols-[18rem_minmax(0,1fr)]",
        )}
      >
        {sessionsCollapsed ? (
          /* Collapsed rail — just the expand toggle. Sticks to the
             top of the viewport so the user can re-open the sidebar
             without scrolling back up in long answer threads. */
          <div className="hidden md:flex md:flex-col md:items-center md:pt-2">
            <Tooltip content="Show sessions">
              <button
                type="button"
                onClick={() => setSessionsCollapsed(false)}
                aria-label="Expand sessions sidebar"
                aria-expanded={false}
                className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-border bg-surface text-ink-muted transition-colors duration-fast hover:border-accent/40 hover:text-ink"
              >
                <ChevronRight size={14} />
              </button>
            </Tooltip>
          </div>
        ) : (
        <Card>
          <CardHeader
            title="Sessions"
            actions={
              <div className="flex items-center gap-1">
                <button
                  type="button"
                  onClick={startNewSession}
                  disabled={chatIsEmpty}
                  aria-disabled={chatIsEmpty}
                  title={
                    chatIsEmpty
                      ? "You're already on a new session"
                      : "Start a fresh session"
                  }
                  className={cn(
                    "inline-flex items-center gap-1 rounded-md border px-2 py-1 text-[11px] font-medium transition-colors duration-fast",
                    chatIsEmpty
                      ? "cursor-default border-transparent bg-transparent text-ink-dim/60"
                      : "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink",
                  )}
                >
                  <Plus size={12} /> New
                </button>
                <Tooltip content="Hide sessions">
                  <button
                    type="button"
                    onClick={() => setSessionsCollapsed(true)}
                    aria-label="Collapse sessions sidebar"
                    aria-expanded={true}
                    className="inline-flex h-7 w-7 items-center justify-center rounded-md border border-border bg-surface text-ink-muted transition-colors duration-fast hover:border-accent/40 hover:text-ink"
                  >
                    <ChevronLeft size={12} />
                  </button>
                </Tooltip>
              </div>
            }
          />
          <CardBody className="max-h-[60vh] overflow-x-hidden overflow-y-auto p-0">
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
                        // ``min-w-0`` lets the inner ``truncate`` work
                        // (a flex child defaults to ``min-width: auto``
                        // which expands to fit content and breaks
                        // ellipsis); ``overflow-hidden`` clamps the
                        // hover action buttons inside the row so a
                        // long title can never push the sidebar wider
                        // than its column.
                        "group flex min-w-0 items-stretch overflow-hidden transition hover:bg-surface-subtle/60",
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
                          "min-w-0 flex-1 px-4 py-3 text-left text-sm focus:outline-none",
                          isLoading && "opacity-60",
                        )}
                      >
                        <div className="flex min-w-0 items-center justify-between gap-2">
                          <span className="min-w-0 flex-1 truncate font-medium">
                            {session.title || `Session #${session.id}`}
                          </span>
                          <span className="shrink-0 font-mono text-[10px] text-ink-dim">
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
                      <Tooltip content="Delete session permanently">
                        <button
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation();
                            setPendingDeleteSessionId(session.id);
                          }}
                          disabled={deleteSession.isPending}
                          aria-label="Delete session"
                          className="px-2 text-ink-dim opacity-0 transition-colors duration-fast hover:bg-critical/10 hover:text-critical group-hover:opacity-100 disabled:opacity-50"
                        >
                          <Trash2 size={14} />
                        </button>
                      </Tooltip>
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
        )}

        {ctx.isLoading ? (
          <Card>
            <CardBody className="space-y-2 px-6 py-8">
              <Skeleton className="h-3 w-1/3" />
              <Skeleton className="h-3 w-2/3" />
            </CardBody>
          </Card>
        ) : llmReady ? (
          <AskChat
            selectedSessionId={selectedSessionId}
            seedTurns={seedTurns}
            seedToken={seedToken}
            seedSubmit={seedSubmit}
            loadingSession={loadingSessionId === selectedSessionId && selectedSessionId != null}
            onSeedSubmitConsumed={() => setSeedSubmit(null)}
            onSessionAssigned={handleSessionAssigned}
            onResumeStale={() => {
              // The AskChat detected a stored "in-flight" job that has
              // already terminated (worker finished while the user was
              // away, or the CLI process restarted). Pull the session
              // detail again so the assistant turn the worker just
              // persisted lands in the chat history.
              if (selectedSessionId != null) {
                void openSession(selectedSessionId);
              } else {
                queryClient.invalidateQueries({ queryKey: ["ask-sessions"] });
              }
            }}
          />
        ) : (
          <NoLlmProfileCard
            title="Configure an LLM before asking"
            description={
              ctx.data?.llm_provider
                ? "The active LLM profile has no model selected — Studio needs both a provider and a model to answer."
                : "No LLM profile is active yet. Add one in Settings → LLM and Studio will route questions through it."
            }
          />
        )}
      </div>
      <AlertDialog
        open={pendingDeleteSessionId !== null}
        onClose={() => {
          if (!deleteSession.isPending) setPendingDeleteSessionId(null);
        }}
        onConfirm={() => {
          if (pendingDeleteSessionId != null) {
            const id = pendingDeleteSessionId;
            deleteSession.mutate(id, {
              onSettled: () =>
                setPendingDeleteSessionId((current) => (current === id ? null : current)),
            });
          }
        }}
        title={
          pendingDeleteSession
            ? `Delete '${pendingDeleteSession.title || `Session #${pendingDeleteSession.id}`}'?`
            : "Delete session?"
        }
        description="Drops every turn under this session from the local history database. Run logs and apply rows the session produced stay intact — only the chat record is removed."
        confirmLabel="Delete"
        loading={deleteSession.isPending}
      />
    </>
  );
}

/** Friendly inline panel rendered in place of the chat surface (or
 *  the run form) when the active LLM profile is missing or
 *  incomplete. The CTA deep-links to Settings → LLM so the user is
 *  one click away from fixing it. */
function NoLlmProfileCard({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <Card>
      <CardBody className="px-6 py-8">
        <div className="flex items-start gap-3">
          <SettingsIcon size={18} className="mt-0.5 flex-none text-warning" />
          <div className="min-w-0 flex-1 space-y-2">
            <p className="text-sm font-semibold text-ink">{title}</p>
            <p className="text-sm text-ink-muted">{description}</p>
            <Link
              to="/settings?tab=llm"
              className="mt-2 inline-flex h-8 items-center gap-1.5 rounded-md bg-accent px-3 text-xs font-medium text-accent-soft transition hover:opacity-90"
            >
              <SettingsIcon size={12} />
              Open LLM settings
            </Link>
          </div>
        </div>
      </CardBody>
    </Card>
  );
}
