import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Check, ChevronDown, CircleStop, FileText, Send, Settings as SettingsIcon, Sparkles, Wrench } from "lucide-react";
import { Link } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useQuery } from "@tanstack/react-query";

import { useEventSource, type SseEvent } from "../lib/sse";
import { api, apiFetch, ApiError } from "../lib/api";
import { useUi } from "../lib/store";
import { Card } from "./Card";
import { cn } from "../lib/cn";
import { InfoHint } from "./ui";
import AskScopeDropdown from "./AskScopeDropdown";
import ProfilePicker from "./topbar/ProfilePicker";

interface AskContextResponse {
  scope_db_profiles: string[];
  doc_profiles: Array<{
    name: string;
    linked_db_profiles: string[];
    paths: string[];
    indexed_chunks: number;
  }>;
  code_profiles: Array<{
    name: string;
    linked_db_profiles: string[];
    path: string;
    indexed_snippets: number;
  }>;
}

/** PR E: structured citation pulled from a ``search_docs`` tool call.
 *  Same shape as the PR C ``Citation`` interface used on RunDetail —
 *  kept independent so AskChat can render the Sources block without
 *  importing from a route module. */
export interface Citation {
  source: string;
  chunk_idx: number;
  score: number;
  snippet: string;
  /** PR γ: optional 1-based ``(start, end)`` line range for code
   *  citations. ``null`` for legacy doc citations (which only carry
   *  ``chunk_idx``) so the renderer can fall back to ``path:chunk_idx``
   *  without breaking existing tool-call payloads. */
  line_range?: [number, number] | null;
}

/** PR γ: pick the user-visible location suffix for a citation.
 *  Code citations prefer ``line_range`` (``src/foo.py:120-145`` or
 *  ``nb.ipynb:3`` for cell-index spans); doc citations fall back to
 *  ``chunk_idx`` (``spec.pdf:5``); a citation with neither shows the
 *  bare path. Kept inline so AskChat and the tool-call hit table stay
 *  in lockstep without an extra import. */
export function formatCitationLocation(c: Citation): string {
  if (Array.isArray(c.line_range) && c.line_range.length === 2) {
    const [start, end] = c.line_range;
    if (Number.isFinite(start) && start > 0) {
      if (Number.isFinite(end) && end > 0 && end !== start) {
        return `${c.source}:${start}-${end}`;
      }
      return `${c.source}:${start}`;
    }
  }
  if (typeof c.chunk_idx === "number" && c.chunk_idx > 0) {
    return `${c.source}:${c.chunk_idx}`;
  }
  return c.source;
}

export interface SubmittedTurn {
  role: "user" | "assistant";
  content: string;
  toolCalls?: Array<{
    name: string;
    arguments: string;
    result_preview: string;
    latency_ms?: number;
    /** PR E: per-call citations (only present on ``search_docs``). */
    citations?: Citation[];
  }>;
  /** Multi-profile observability stamped on assistant turns. */
  scopeProfiles?: string[];
  focusProfile?: string | null;
  totalLatencyMs?: number;
  /** PR E: aggregated citations across every ``search_docs`` call in
   *  the turn, deduped by ``(source, chunk_idx)``. Rendered as a
   *  Sources block under the answer. */
  citations?: Citation[];
}

interface SubmitResponse {
  job_id: string;
  session_id: number | null;
  status: string;
  scope_profiles?: string[];
}

interface AskChatProps {
  // When the parent loads a stored session, it pushes the sessionId +
  // hydrated turns down. Both null means "fresh session".
  selectedSessionId: number | null;
  seedTurns: SubmittedTurn[] | null;
  // Bumped each time the parent wants to reseed (so identical loads
  // still trigger the effect). Re-using a number works fine.
  seedToken: number;
  // Optional: when set, AskChat fires this prompt as a fresh user
  // turn the moment it mounts (or the moment seedToken bumps). Used
  // by the Compare modal's "Ask AMX" hand-off — the user already
  // consented to send by clicking the modal button, so leaving the
  // seed visible as a stranded orange bubble was confusing.
  // ``onSeedSubmitConsumed`` clears the parent's slot once the
  // submit has fired so a re-render doesn't refire it.
  seedSubmit?: string | null;
  onSeedSubmitConsumed?: () => void;
  // Lets the chat panel notify parent when a brand-new session id is
  // assigned by the backend, so the sidebar can refresh / highlight.
  onSessionAssigned?: (sessionId: number | null) => void;
  // Fires when AskChat detects a stored "in-flight" ask job that has
  // already terminated (or vanished — backend restart) while the user
  // was away. Parent should re-fetch the session detail so the
  // assistant turn the worker just persisted shows up.
  onResumeStale?: () => void;
}

// Self-contained chat panel — owns the question textarea, SSE
// subscription for the current job, the running thinking ribbon,
// and the message history.
export default function AskChat({
  selectedSessionId,
  seedTurns,
  seedToken,
  seedSubmit,
  onSeedSubmitConsumed,
  onSessionAssigned,
  onResumeStale,
}: AskChatProps) {
  const [turns, setTurns] = useState<SubmittedTurn[]>([]);
  const [question, setQuestion] = useState("");
  const [activeJob, setActiveJob] = useState<string | null>(null);
  // ``cancelling`` flips to true the instant the user clicks Cancel,
  // ahead of the SSE ``job.cancelled`` event. The backend can't abort
  // an in-flight LLM HTTP call (LiteLLM is synchronous), so cancellation
  // takes effect only when the current LLM step returns — typically a
  // few seconds, sometimes more for reasoning models. Showing
  // "Cancelling…" immediately stops the UI from looking frozen on
  // "Reasoning…" the whole time. The flag is cleared in the same
  // ``closed`` effect that finalises the turn.
  const [cancelling, setCancelling] = useState(false);
  const [sessionId, setSessionId] = useState<number | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [submitErrorHint, setSubmitErrorHint] = useState<string | null>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const messagesRef = useRef<HTMLDivElement | null>(null);

  // Per-chat sticky scope. Keyed by session id (or "_new_" before the
  // first message of a fresh chat assigns one). The dropdown writes
  // here; submit reads here. Resets when the parent starts a new chat
  // because the session key changes (or the entry hadn't been set).
  const sessionKey = sessionId != null ? String(sessionId) : "_new_";
  const askScopeBySession = useUi((s) => s.askScopeBySession);
  const setAskScope = useUi((s) => s.setAskScope);
  const clearAskScope = useUi((s) => s.clearAskScope);
  const setAskActiveJob = useUi((s) => s.setAskActiveJob);
  const clearAskActiveJob = useUi((s) => s.clearAskActiveJob);
  const scopeForSession =
    sessionKey in askScopeBySession ? askScopeBySession[sessionKey] : null;

  // Doc / code profile override for the NEXT question. ``null`` =
  // auto-derive from the DB scope via the link map (legacy default).
  // ``[]`` = opt OUT of retrieval entirely. ``string[]`` = explicit
  // pick that bypasses the link map. Per-question, not sticky — the
  // user often wants different sources on consecutive questions.
  const [docProfilesOverride, setDocProfilesOverride] = useState<string[] | null>(null);
  const [codeProfilesOverride, setCodeProfilesOverride] = useState<string[] | null>(null);

  // Read the active LLM profile / model from the shared ``["context"]``
  // cache so the inline picker mirrors whatever the sidebar shows.
  // ``ProfilePicker``'s activate mutation invalidates this key, so
  // flipping the model in either trigger refreshes the other without
  // an explicit hand-off.
  const ctxForLlm = useQuery({
    queryKey: ["context"],
    queryFn: () => api.context(),
    staleTime: 0,
  });
  const activeLlmProfile = ctxForLlm.data?.active_llm_profile ?? null;
  const activeLlmModel = ctxForLlm.data?.llm_model ?? null;

  // Reseed history when the parent picks a different session.
  useEffect(() => {
    setSessionId(selectedSessionId);
    setTurns(seedTurns ?? []);
    setActiveJob(null);
    setSubmitError(null);
    setSubmitErrorHint(null);
    // Drop the "_new_" scratch entry when the parent transitions us
    // to a saved session — the saved session has its own key now.
    if (selectedSessionId != null) {
      clearAskScope("_new_");
    }

    // Resume in-flight ask: when the user navigated away mid-question,
    // the backend worker thread keeps running and JobRegistry buffers
    // every event in the job's queue (see amx/web/jobs.py — jobs live
    // until the parent CLI process exits). Reattaching the SSE stream
    // drains the buffered events and surfaces the answer instead of
    // leaving the user stranded on a "only my question" view.
    const resumeKey =
      selectedSessionId != null ? String(selectedSessionId) : "_new_";
    const savedJobId = useUi.getState().askActiveJobBySession[resumeKey];
    if (savedJobId) {
      let cancelled = false;
      // Helper: bail if the saved pointer has changed since we started
      // (e.g. the user submitted a fresh question while the verify GET
      // was in flight). Without this, the late resolve could clobber
      // the new activeJob with the old jobId.
      const stillThis = () =>
        useUi.getState().askActiveJobBySession[resumeKey] === savedJobId;
      void (async () => {
        try {
          const status = await apiFetch<{ id: string; status: string }>(
            `/api/ask/${savedJobId}`,
          );
          if (cancelled || !stillThis()) return;
          if (status.status === "running" || status.status === "queued") {
            setActiveJob(savedJobId);
          } else {
            // Worker terminated while we were away. The assistant turn
            // (or failure) is already persisted to chat_sessions; ask
            // the parent to re-pull the seed so it shows.
            clearAskActiveJob(resumeKey);
            onResumeStale?.();
          }
        } catch {
          // 404 or network error — backend doesn't know this job
          // (CLI process restarted between submit and remount). Drop
          // the stale pointer; the seed turns from history are the
          // authoritative view of the conversation.
          if (cancelled || !stillThis()) return;
          clearAskActiveJob(resumeKey);
          onResumeStale?.();
        }
      })();
      return () => {
        cancelled = true;
      };
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seedToken]);

  const { events, closed } = useEventSource({
    path: activeJob ? `/api/ask/${activeJob}/events` : "",
    enabled: !!activeJob,
  });

  // Aggregate streamed events into the assistant's turn.
  const { thinking, streamingAnswer, finalAnswer, toolCalls, finalMeta, finalCitations, jobFailure } = useMemo(() => {
    const thinkingChunks: string[] = [];
    const tools: Array<{
      name: string;
      arguments: string;
      result_preview: string;
      latency_ms?: number;
      citations?: Citation[];
    }> = [];
    // ``answerChunks`` accumulates ``answer.delta`` events into the
    // streaming assistant bubble. Cleared whenever a ``tool.call``
    // event arrives — any content emitted before a tool call is
    // interim narration ("Let me search the catalog…") that the agent
    // produces while deciding to invoke a tool; only the deltas after
    // the LAST tool call (or all of them, if there are no tool calls)
    // belong to the user-facing answer.
    let answerChunks: string[] = [];
    let finalText: string | null = null;
    let meta: {
      scopeProfiles?: string[];
      focusProfile?: string | null;
      totalLatencyMs?: number;
    } = {};
    let citations: Citation[] = [];
    let failure: { message: string; hint?: string } | null = null;
    for (const event of events) {
      if (event.type === "thinking.delta" && typeof event.text === "string") {
        thinkingChunks.push(event.text);
      } else if (event.type === "answer.delta" && typeof event.text === "string") {
        answerChunks.push(event.text);
      } else if (event.type === "tool.call") {
        // Interim narration before a tool call is not the final answer;
        // reset the streaming-answer accumulator so the bubble starts
        // fresh for the next iteration's content.
        answerChunks = [];
        tools.push({
          name: String(event.name || ""),
          arguments: String(event.arguments || ""),
          result_preview: String(event.result_preview || ""),
          latency_ms:
            typeof event.latency_ms === "number" ? event.latency_ms : undefined,
          citations: Array.isArray(event.citations)
            ? (event.citations as Citation[])
            : undefined,
        });
      } else if (event.type === "answer.final" && typeof event.answer === "string") {
        finalText = event.answer;
        meta = {
          scopeProfiles: Array.isArray(event.scope_profiles)
            ? (event.scope_profiles as string[])
            : undefined,
          focusProfile:
            typeof event.focus_profile === "string"
              ? (event.focus_profile as string)
              : null,
          totalLatencyMs:
            typeof event.total_latency_ms === "number"
              ? (event.total_latency_ms as number)
              : undefined,
        };
        if (Array.isArray(event.citations)) {
          citations = event.citations as Citation[];
        }
      } else if (event.type === "job.failed") {
        failure = {
          message: String(event.error || "Ask failed."),
          hint: typeof event.hint === "string" ? event.hint : undefined,
        };
      }
    }
    return {
      thinking: thinkingChunks.join(""),
      streamingAnswer: answerChunks.join(""),
      finalAnswer: finalText,
      toolCalls: tools,
      finalMeta: meta,
      finalCitations: citations,
      jobFailure: failure,
    };
  }, [events]);

  // Once the worker reports answer.final, snapshot the turn into
  // history so the next question doesn't clobber it. When the worker
  // emits ``job.failed`` instead of ``answer.final``, surface the
  // error inline (so the chat doesn't sit on "Reasoning…" forever)
  // and mark the assistant turn so the user knows the question
  // didn't go through.
  useEffect(() => {
    if (!closed) return;
    if (finalAnswer != null) {
      setTurns((prev) => {
        const last = prev[prev.length - 1];
        if (last?.role === "assistant" && last.content === finalAnswer) return prev;
        return [
          ...prev,
          {
            role: "assistant",
            content: finalAnswer,
            toolCalls,
            scopeProfiles: finalMeta.scopeProfiles,
            focusProfile: finalMeta.focusProfile ?? null,
            totalLatencyMs: finalMeta.totalLatencyMs,
            citations: finalCitations.length > 0 ? finalCitations : undefined,
          },
        ];
      });
      setActiveJob(null);
      setCancelling(false);
      clearAskActiveJob(sessionKey);
      return;
    }
    if (jobFailure) {
      setSubmitError(jobFailure.message);
      setSubmitErrorHint(jobFailure.hint ?? null);
      // Pop the user's question turn off the bubble list — the
      // submitError block below replaces it with the clean error
      // surface so we don't leave an orphaned user-only bubble.
      setActiveJob(null);
      setCancelling(false);
      clearAskActiveJob(sessionKey);
      return;
    }
    // Stream closed without final answer AND without job.failed (rare
    // — proxy reset, network glitch, or a clean job.cancelled after
    // the user clicked Cancel). When the user cancelled, no error
    // banner — drop the activeJob silently so the cancelled turn
    // simply disappears.
    if (!cancelling) {
      setSubmitError(
        "The ask stream ended without a final answer. Try again, or check Settings → LLM if this keeps happening.",
      );
      setSubmitErrorHint("configure-llm");
    }
    setActiveJob(null);
    setCancelling(false);
    clearAskActiveJob(sessionKey);
  }, [closed, finalAnswer, jobFailure, toolCalls, finalMeta, finalCitations, sessionKey, clearAskActiveJob, cancelling]);

  async function submitText(rawText: string) {
    const text = (rawText || "").trim();
    if (!text || activeJob) return;
    setSubmitError(null);
    setSubmitErrorHint(null);
    setTurns((prev) => [...prev, { role: "user", content: text }]);
    setQuestion("");
    try {
      const body: Record<string, unknown> = {
        question: text,
        session_id: sessionId ?? undefined,
      };
      // Multi-profile sticky scope: only attach when the user has
      // narrowed it (scope_profiles=null means "default to config").
      if (scopeForSession !== null) {
        body.scope_profiles = scopeForSession;
      }
      // Doc/code overrides are per-question. ``null`` = auto, the
      // backend already handles missing keys as auto — only forward
      // when the user picked something explicit (incl. an empty list
      // meaning "skip retrieval").
      if (docProfilesOverride !== null) {
        body.doc_profiles = docProfilesOverride;
      }
      if (codeProfilesOverride !== null) {
        body.code_profiles = codeProfilesOverride;
      }
      const result = await apiFetch<SubmitResponse>("/api/ask", {
        method: "POST",
        body: JSON.stringify(body),
      });
      // When the backend assigns a fresh session id, migrate the
      // "_new_" scope entry to the real session key so the dropdown
      // stays sticky across the rest of this chat.
      if (
        result.session_id != null &&
        sessionId == null &&
        sessionKey === "_new_" &&
        "_new_" in askScopeBySession
      ) {
        const migrated = askScopeBySession["_new_"];
        setAskScope(String(result.session_id), migrated);
        clearAskScope("_new_");
      }
      setSessionId(result.session_id);
      setActiveJob(result.job_id);
      // Persist the in-flight job under the resolved session key so a
      // navigation-away-and-back can reattach the SSE stream. For a
      // brand-new session we know the real id from the response — use
      // it directly instead of the stale "_new_" sessionKey, which a
      // sibling effect promotes only on the next render.
      const persistKey =
        result.session_id != null ? String(result.session_id) : sessionKey;
      setAskActiveJob(persistKey, result.job_id);
      onSessionAssigned?.(result.session_id);
    } catch (err) {
      // The 412 pre-flight check on the LLM config carries a
      // ``hint=configure-llm`` so we can show a "Open Settings" CTA
      // alongside the error rather than leaving the user wondering
      // why the chat hung. Other errors render as plain text with no
      // CTA — the message is still actionable.
      let message = "Ask failed.";
      let hint: string | null = null;
      if (err instanceof ApiError) {
        message = err.detail || err.message || message;
        hint = err.hint ?? null;
      } else if (err instanceof Error) {
        message = err.message;
      }
      setSubmitError(message);
      setSubmitErrorHint(hint);
    }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    await submitText(question);
  }

  // Cross-page hand-off auto-submit. The Compare modal's "Ask AMX"
  // button drops a seed prompt onto the parent and bumps seedToken;
  // we pick it up here once the chat is mounted and there's no
  // in-flight job, fire it as a real /api/ask call (so the orange
  // user bubble streams a real assistant reply instead of just
  // sitting there as a stranded pre-loaded turn), and immediately
  // tell the parent to clear its slot so a re-render doesn't refire
  // the same prompt twice.
  useEffect(() => {
    if (!seedSubmit) return;
    if (activeJob) return;
    void submitText(seedSubmit);
    onSeedSubmitConsumed?.();
    // ``seedToken`` is part of the dep list because the parent
    // bumps it on every fresh hand-off; without it, two consecutive
    // hand-offs of the same text would fail the seedSubmit-changed
    // gate. ``submitText`` is intentionally NOT in the deps —
    // including it would refire on every render that tweaks
    // session/scope state.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seedSubmit, seedToken]);

  function handleScopeChange(next: string[] | null) {
    setAskScope(sessionKey, next);
    // Persist to the backend session record so cross-tab reloads pick
    // it up. Skip when there's no session yet — the migration on
    // first /ask covers it.
    if (sessionId != null) {
      apiFetch(`/api/ask/sessions/${sessionId}`, {
        method: "PATCH",
        body: JSON.stringify({ scope_profiles: next }),
      }).catch(() => {
        /* best-effort — local state is still authoritative */
      });
    }
  }

  async function handleCancel() {
    if (!activeJob) return;
    // Flip the UI to "Cancelling…" right away — the backend cancel
    // token only takes effect when the current LLM step returns, and
    // reasoning models can sit on a single step for tens of seconds.
    // Without this, the user clicks Cancel and the bubble keeps
    // pulsing "Reasoning…" until the SSE finally closes, which looks
    // broken.
    setCancelling(true);
    try {
      await apiFetch(`/api/ask/${activeJob}/cancel`, { method: "POST" });
    } catch {
      /* SSE will surface job.cancelled regardless */
    }
  }

  // Auto-scroll the messages container to the bottom whenever new
  // history, streaming reasoning, or tool calls arrive — without this
  // the user has to manually keep scrolling while the LLM thinks.
  useLayoutEffect(() => {
    const node = messagesRef.current;
    if (!node) return;
    node.scrollTop = node.scrollHeight;
  }, [turns, thinking, streamingAnswer, toolCalls.length, activeJob, cancelling]);

  return (
    <div className="flex h-[calc(100vh-12rem)] min-h-[28rem] min-w-0 flex-col gap-4">
      <div
        ref={messagesRef}
        className="flex-1 min-w-0 space-y-4 overflow-y-auto overflow-x-hidden rounded-xl border border-surface-border bg-surface-raised p-4"
      >
        {turns.length === 0 && !activeJob && (
          <div className="flex h-full min-h-[40vh] flex-col items-center justify-center text-center text-ink-dim">
            <Sparkles size={28} className="mb-3 opacity-60" />
            <p className="font-medium text-ink-muted">Ask anything about your metadata.</p>
            <p className="mt-1 max-w-md text-xs">
              Try “which tables don't have comments?”, “what columns store
              email addresses?”, or “show me the latest run on sales.orders”.
            </p>
          </div>
        )}
        {turns.map((turn, idx) => (
          <Bubble key={idx} role={turn.role}>
            {turn.role === "assistant" ? (
              <MarkdownBody text={turn.content} />
            ) : (
              turn.content
            )}
            {turn.toolCalls && turn.toolCalls.length > 0 && (
              <ToolCallList calls={turn.toolCalls} />
            )}
            {turn.role === "assistant" &&
              turn.citations &&
              turn.citations.length > 0 && (
                <CitationsList citations={turn.citations} />
              )}
            {turn.role === "assistant" &&
              (turn.scopeProfiles?.length || turn.totalLatencyMs != null) && (
                <AnswerMeta
                  scopeProfiles={turn.scopeProfiles}
                  focusProfile={turn.focusProfile ?? null}
                  totalLatencyMs={turn.totalLatencyMs}
                />
              )}
          </Bubble>
        ))}
        {activeJob && (
          <Bubble role="assistant" pulsing={!cancelling}>
            {cancelling && (
              <div className="mb-1.5 inline-flex items-center gap-1.5 rounded-full bg-warning-soft/40 px-2 py-0.5 text-[11px] font-medium text-warning">
                <CircleStop size={11} /> Cancelling…
              </div>
            )}
            {thinking && !cancelling ? <ThinkingBlock text={thinking} /> : null}
            {streamingAnswer ? (
              <MarkdownBody text={streamingAnswer} />
            ) : (
              !thinking && !cancelling && (
                <span className="text-sm text-ink-dim">Reasoning…</span>
              )
            )}
            {toolCalls.length > 0 && <ToolCallList calls={toolCalls} live />}
            {finalCitations.length > 0 && (
              <CitationsList citations={finalCitations} />
            )}
          </Bubble>
        )}
        {submitError && (
          <AskErrorBanner
            message={submitError}
            hint={submitErrorHint}
            onDismiss={() => {
              setSubmitError(null);
              setSubmitErrorHint(null);
            }}
          />
        )}
      </div>

      <Card className="p-3">
        <div className="mb-2 flex items-center justify-between gap-2">
          <AskDocCodePicker
            scope={scopeForSession}
            docOverride={docProfilesOverride}
            codeOverride={codeProfilesOverride}
            onDocChange={setDocProfilesOverride}
            onCodeChange={setCodeProfilesOverride}
            disabled={!!activeJob}
          />
          <div className="ml-auto flex items-center gap-2">
            {/* The sidebar already exposes the LLM profile, but users
                landing on /ask via a deep link or working full-width
                often miss it. Mirroring the picker here keeps the
                model switcher under the question input, exactly where
                the user is about to type. ``ProfilePicker``'s activate
                mutation invalidates ``["context"]`` and ``["profiles",
                "llm"]``, which both this trigger and the sidebar read
                from — so flipping it on one surface immediately
                refreshes the other without any extra wiring. */}
            <ProfilePicker
              kind="llm"
              label="LLM"
              variant="pill"
              activeName={activeLlmProfile}
              tooltip={activeLlmModel ?? undefined}
            />
            <AskScopeDropdown
              scope={scopeForSession}
              onChange={handleScopeChange}
              disabled={!!activeJob}
            />
          </div>
        </div>
        <form onSubmit={handleSubmit} className="flex items-end gap-2">
          <div className="relative flex-1">
            <textarea
              ref={inputRef}
              className="min-h-[44px] w-full resize-none rounded-md border border-border bg-surface-raised px-3 py-2 pr-16 text-sm text-ink placeholder:text-ink-dim focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20"
              placeholder={activeJob ? "Waiting for answer…" : "Ask AMX…"}
              value={question}
              disabled={!!activeJob}
              onChange={(e) => setQuestion(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  handleSubmit(e);
                }
              }}
              rows={1}
            />
            {!activeJob && (
              <span
                className="pointer-events-none absolute bottom-1.5 right-2 inline-flex items-center gap-1 text-[10px] text-ink-dim"
                aria-hidden="true"
              >
                <kbd className="inline-flex h-4 min-w-[1rem] items-center justify-center rounded border border-border bg-surface-subtle px-1 font-mono text-[10px] text-ink-muted">
                  ↵
                </kbd>
                send
              </span>
            )}
          </div>
          {activeJob ? (
            <button
              type="button"
              onClick={handleCancel}
              className="inline-flex h-10 items-center gap-1.5 rounded-md bg-critical/10 px-3 text-sm font-medium text-critical hover:bg-critical/20"
            >
              Cancel
            </button>
          ) : (
            <button
              type="submit"
              disabled={!question.trim()}
              className="inline-flex h-10 items-center gap-1.5 rounded-md bg-accent px-4 text-sm font-medium text-accent-soft transition hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-40"
            >
              <Send size={14} />
              Ask
            </button>
          )}
        </form>
      </Card>
    </div>
  );
}

function Bubble({
  role,
  children,
  pulsing,
}: {
  role: "user" | "assistant";
  children: React.ReactNode;
  pulsing?: boolean;
}) {
  return (
    <div
      className={cn(
        "flex",
        role === "user" ? "justify-end" : "justify-start",
      )}
    >
      <div
        className={cn(
          "break-words rounded-2xl px-4 py-2.5 text-sm shadow-sm",
          role === "user"
            ? "max-w-2xl bg-accent text-accent-soft"
            : "min-w-0 max-w-[min(56rem,calc(100%-1rem))] bg-surface-subtle text-ink",
          pulsing && "animate-pulse",
        )}
      >
        {typeof children === "string" ? (
          <p className="whitespace-pre-wrap leading-relaxed">{children}</p>
        ) : (
          children
        )}
      </div>
    </div>
  );
}

function MarkdownBody({ text }: { text: string }) {
  return (
    <div className="markdown-body text-sm leading-relaxed">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          p: ({ children }) => <p className="mb-2 last:mb-0">{children}</p>,
          ul: ({ children }) => (
            <ul className="mb-2 ml-5 list-disc space-y-0.5 last:mb-0">{children}</ul>
          ),
          ol: ({ children }) => (
            <ol className="mb-2 ml-5 list-decimal space-y-0.5 last:mb-0">{children}</ol>
          ),
          li: ({ children }) => <li className="leading-relaxed">{children}</li>,
          h1: ({ children }) => (
            <h1 className="mb-2 mt-3 text-base font-semibold first:mt-0">{children}</h1>
          ),
          h2: ({ children }) => (
            <h2 className="mb-2 mt-3 text-sm font-semibold first:mt-0">{children}</h2>
          ),
          h3: ({ children }) => (
            <h3 className="mb-1.5 mt-2 text-sm font-semibold first:mt-0">{children}</h3>
          ),
          strong: ({ children }) => <strong className="font-semibold text-ink">{children}</strong>,
          em: ({ children }) => <em className="italic">{children}</em>,
          code: ({ children, className }) => {
            const isBlock = className?.startsWith("language-");
            if (isBlock) {
              return (
                <code className={cn("font-mono text-[12px]", className)}>{children}</code>
              );
            }
            return (
              <code className="rounded bg-surface-subtle px-1 py-0.5 font-mono text-[12px] text-ink">
                {children}
              </code>
            );
          },
          pre: ({ children }) => (
            <pre className="mb-2 overflow-x-auto rounded-md bg-surface-subtle p-3 text-[12px] last:mb-0">
              {children}
            </pre>
          ),
          a: ({ href, children }) => (
            <a
              href={href}
              target="_blank"
              rel="noreferrer noopener"
              className="text-accent underline underline-offset-2 hover:opacity-80"
            >
              {children}
            </a>
          ),
          blockquote: ({ children }) => (
            <blockquote className="mb-2 border-l-2 border-accent/50 pl-3 italic text-ink-muted last:mb-0">
              {children}
            </blockquote>
          ),
          hr: () => <hr className="my-3 border-surface-border" />,
          table: ({ children }) => (
            <div className="mb-2 overflow-x-auto last:mb-0">
              <table className="min-w-full border-collapse text-[12px]">{children}</table>
            </div>
          ),
          thead: ({ children }) => (
            <thead className="bg-surface-subtle text-left">{children}</thead>
          ),
          tbody: ({ children }) => (
            <tbody className="divide-y divide-surface-border">{children}</tbody>
          ),
          tr: ({ children }) => <tr>{children}</tr>,
          th: ({ children }) => (
            <th className="border border-surface-border px-2 py-1 font-semibold">{children}</th>
          ),
          td: ({ children }) => (
            <td className="border border-surface-border px-2 py-1 align-top">{children}</td>
          ),
        }}
      >
        {text}
      </ReactMarkdown>
    </div>
  );
}

function ThinkingBlock({ text }: { text: string }) {
  // Cap the visible reasoning panel so a model that thinks for
  // thousands of tokens doesn't push the input field below the fold.
  // The block scrolls internally and auto-pins to the bottom so the
  // freshest reasoning is always on screen — same UX as a tail -f.
  const ref = useRef<HTMLDivElement | null>(null);
  useLayoutEffect(() => {
    const node = ref.current;
    if (!node) return;
    node.scrollTop = node.scrollHeight;
  }, [text]);
  return (
    <div className="space-y-1">
      <div className="flex items-center gap-1 text-[11px] font-semibold uppercase tracking-wider text-ink-dim">
        Thinking
        <InfoHint text="The model's reasoning preview — not the final answer, just its thought steps." />
      </div>
      <div
        ref={ref}
        className="max-h-48 overflow-y-auto whitespace-pre-wrap break-words text-sm text-ink-muted"
      >
        {text}
      </div>
    </div>
  );
}

function ToolCallList({
  calls,
  live,
}: {
  calls:
    | SseEvent[]
    | Array<{
        name: string;
        arguments: string;
        result_preview: string;
        citations?: Citation[];
      }>;
  live?: boolean;
}) {
  return (
    <details className={cn("mt-3", live && "")} open={live}>
      <summary className="flex cursor-pointer items-center gap-1.5 text-[11px] font-medium uppercase tracking-wider text-ink-dim">
        <Wrench size={12} /> {calls.length} tool call{calls.length === 1 ? "" : "s"}
      </summary>
      <ul className="mt-2 space-y-1 text-xs">
        {calls.map((call, idx) => {
          const c = call as {
            name: string;
            arguments: string;
            result_preview: string;
            citations?: Citation[];
          };
          // PR E: search_docs renders a compact hit table inside the
          // expander instead of the truncated result_preview line so
          // the user can read sources / scores at a glance without
          // chasing the answer's Sources block. PR γ extends this to
          // ``search_code`` so code retrievals get the same per-hit
          // breakdown rendered with line ranges instead of chunk_idx.
          const isRetrievalCall = c.name === "search_docs" || c.name === "search_code";
          const hits = Array.isArray(c.citations) ? c.citations : [];
          if (isRetrievalCall && hits.length > 0) {
            return (
              <li key={idx} className="rounded-md bg-surface px-2 py-1 text-[11px] text-ink-muted">
                <div className="font-mono">
                  <span className="text-accent">{c.name}</span>(
                  <span className="text-ink-dim">{c.arguments}</span>) →{" "}
                  {hits.length} hit{hits.length === 1 ? "" : "s"}
                </div>
                <ul className="mt-1 space-y-0.5 pl-3 font-mono text-[10.5px] text-ink-dim">
                  {hits.map((h, hidx) => {
                    // PR γ: ``formatCitationLocation`` picks
                    // ``line_range`` over ``chunk_idx`` so code hits
                    // show ``src/foo.py:120-145`` while docs continue
                    // to render ``spec.pdf:5`` unchanged.
                    const location = formatCitationLocation(h);
                    return (
                      <li key={`${h.source}-${h.chunk_idx}-${hidx}`}>
                        • <span className="text-ink-muted">{location}</span>{" "}
                        <span className="opacity-70">— score {h.score.toFixed(2)}</span>
                      </li>
                    );
                  })}
                </ul>
              </li>
            );
          }
          return (
            <li key={idx} className="rounded-md bg-surface px-2 py-1 font-mono text-[11px] text-ink-muted">
              <span className="text-accent">{c.name}</span>(
              <span className="text-ink-dim">{c.arguments}</span>) → {c.result_preview}
            </li>
          );
        })}
      </ul>
    </details>
  );
}

/**
 * Friendly error surface for /ask failures.
 *
 * Two flavours:
 *   - ``configure-llm`` hint → a "Couldn't reach the LLM" headline
 *     plus an "Open Settings" call-to-action. Shown for every
 *     LLM-side failure (missing API key, bad model, network down to
 *     the provider). This is the path that previously hung on
 *     "Reasoning…" forever.
 *   - any other hint / no hint → plain-text error with the backend's
 *     message verbatim.
 *
 * Always dismissable so the user can retry without reloading the
 * whole chat.
 */
function AskErrorBanner({
  message,
  hint,
  onDismiss,
}: {
  message: string;
  hint?: string | null;
  onDismiss: () => void;
}) {
  const isLlm = hint === "configure-llm";
  return (
    <div
      role="alert"
      className={cn(
        "flex flex-col gap-2 rounded-md border px-3 py-2.5 text-xs",
        isLlm
          ? "border-warning/40 bg-warning-soft/40 text-warning-ink"
          : "border-critical/30 bg-critical/5 text-critical",
      )}
    >
      <div className="flex items-start gap-2">
        <SettingsIcon
          size={14}
          className={cn(
            "mt-0.5 flex-none",
            isLlm ? "text-warning" : "text-critical",
          )}
          aria-hidden="true"
        />
        <div className="min-w-0 flex-1">
          {isLlm ? (
            <>
              <p className="font-semibold text-ink">
                Couldn't reach the LLM.
              </p>
              <p className="mt-0.5 text-ink-muted">
                Check your provider settings — most likely an unset API
                key, a wrong model id, or a network block.
              </p>
              <p className="mt-1 text-ink-dim/80">
                <span className="font-mono text-[10px]">{message}</span>
              </p>
            </>
          ) : (
            <p className="text-ink">{message}</p>
          )}
        </div>
        <button
          type="button"
          onClick={onDismiss}
          aria-label="Dismiss"
          className="ml-2 flex-none text-ink-dim hover:text-ink"
        >
          ×
        </button>
      </div>
      {isLlm && (
        <div className="flex items-center gap-2">
          <Link
            to="/settings?tab=llm"
            className="inline-flex h-7 items-center gap-1.5 rounded-md bg-accent px-3 text-[11px] font-medium text-accent-soft transition hover:opacity-90"
          >
            <SettingsIcon size={12} />
            Open LLM settings
          </Link>
          <Link
            to="/system"
            className="inline-flex h-7 items-center gap-1.5 rounded-md border border-border px-3 text-[11px] text-ink-muted hover:border-accent/40 hover:text-ink"
          >
            Run doctor
          </Link>
        </div>
      )}
    </div>
  );
}

/**
 * Footer beneath an assistant turn showing multi-profile observability:
 * how many profiles answered, the auto-detected focus, and wall-clock
 * latency. Renders inline-italic dim so it doesn't fight the answer
 * for attention. Only shown when there's data to show — single-profile
 * single-second responses get nothing.
 */
function AnswerMeta({
  scopeProfiles,
  focusProfile,
  totalLatencyMs,
}: {
  scopeProfiles?: string[];
  focusProfile?: string | null;
  totalLatencyMs?: number;
}) {
  const parts: string[] = [];
  if (scopeProfiles && scopeProfiles.length) {
    parts.push(
      `${scopeProfiles.length} profile${scopeProfiles.length === 1 ? "" : "s"}`,
    );
  }
  if (typeof totalLatencyMs === "number" && totalLatencyMs > 0) {
    const seconds = totalLatencyMs / 1000;
    parts.push(`${seconds.toFixed(seconds < 10 ? 1 : 0)}s`);
  }
  if (focusProfile) {
    parts.push(`focus: ${focusProfile}`);
  }
  if (parts.length === 0) return null;
  return (
    <div className="mt-2 text-[10.5px] italic text-ink-dim">
      {parts.join(" · ")}
    </div>
  );
}

/**
 * PR E: Sources block under an assistant turn. Same format as PR C's
 * CitationsDisclosure on RunDetail (path:chunk_idx · score · snippet)
 * so the user gets the same provenance read-out in both surfaces.
 *
 * Kept as a copy here rather than imported from RunDetail to avoid
 * coupling the chat component to a route module. TODO: extract a
 * shared ``CitationsList`` component once the citation shape stops
 * evolving — both surfaces should track each other byte-for-byte.
 */
function CitationsList({ citations }: { citations: Citation[] }) {
  if (citations.length === 0) return null;
  return (
    <div className="mt-2 space-y-1 rounded-md border border-border bg-surface-subtle/20 px-2.5 py-1.5 text-xs text-ink-muted">
      <div className="text-[10px] uppercase tracking-wider text-ink-dim">Sources</div>
      {citations.map((c, i) => {
        // PR γ: ``formatCitationLocation`` returns the path either with
        // a ``:start-end`` line range (code citations) or with a
        // ``:chunk_idx`` suffix (doc citations) -- both formats render
        // identically in this row.
        const location = formatCitationLocation(c);
        return (
          <div key={`${c.source}-${c.chunk_idx}-${i}`}>
            <div className="font-mono">
              <span className="text-ink">{location}</span>
              <span className="ml-2 text-ink-dim">score {c.score.toFixed(2)}</span>
            </div>
            {c.snippet && (
              <div className="ml-3 mt-0.5 italic text-ink-dim line-clamp-2">
                &ldquo;{c.snippet}&hellip;&rdquo;
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

interface AskDocCodePickerProps {
  scope: string[] | null;
  docOverride: string[] | null;
  codeOverride: string[] | null;
  onDocChange: (next: string[] | null) => void;
  onCodeChange: (next: string[] | null) => void;
  disabled?: boolean;
}

interface ProfileListItem {
  name: string;
  /** Auto-derived count from the API context (chunks for docs, snippets for code). */
  count: number;
  linked: string[];
}

/**
 * Lets the user override which doc/code profiles /ask uses for THIS
 * question, decoupled from the DB scope. Three modes per surface:
 *
 *   - **Auto** (default, ``null``): backend derives from the link map.
 *   - **None** (``[]``): skip retrieval entirely.
 *   - **Explicit pick** (``string[]``): exactly these profiles.
 *
 * Picks reset per session change in the parent. The trigger pill shows
 * the active mode at a glance; the popover splits docs and code into
 * two sections so the user picks each side independently.
 */
function AskDocCodePicker({
  scope,
  docOverride,
  codeOverride,
  onDocChange,
  onCodeChange,
  disabled,
}: AskDocCodePickerProps) {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) return;
    function onClick(e: MouseEvent) {
      if (!wrapperRef.current) return;
      if (!wrapperRef.current.contains(e.target as Node)) setOpen(false);
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    document.addEventListener("mousedown", onClick);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onClick);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  // Fetch the full configured doc / code profile inventory — independent
  // of the DB scope so the user can pick a profile that the link map
  // wouldn't have auto-included.
  const docProfiles = useQuery({
    queryKey: ["doc-profiles", "list-for-ask-picker"],
    queryFn: () =>
      apiFetch<{ profiles: { name: string; linked_db_profiles?: string[] }[] }>(
        "/api/profiles/docs",
      ),
    staleTime: 30_000,
  });
  const codeProfiles = useQuery({
    queryKey: ["code-profiles", "list-for-ask-picker"],
    queryFn: () =>
      apiFetch<{ profiles: { name: string; linked_db_profiles?: string[] }[] }>(
        "/api/profiles/code",
      ),
    staleTime: 30_000,
  });

  // Reuse /api/ask/context purely to fetch the auto-derived counts so
  // the chip can still display "📄 2 docs (12 chunks)" when the user
  // is in Auto mode. The endpoint already supports a scope filter.
  const ctxQuery = useQuery({
    queryKey: ["ask", "context", "for-picker", scope ?? "_session_default_"],
    queryFn: () => {
      const qs =
        scope && scope.length > 0
          ? `?scope_profiles=${scope.map(encodeURIComponent).join(",")}`
          : "";
      return apiFetch<AskContextResponse>(`/api/ask/context${qs}`);
    },
    retry: false,
    staleTime: 10_000,
  });

  const docInventory: ProfileListItem[] = (docProfiles.data?.profiles ?? []).map(
    (p) => ({
      name: p.name,
      count:
        ctxQuery.data?.doc_profiles?.find((d) => d.name === p.name)?.indexed_chunks ??
        0,
      linked: p.linked_db_profiles ?? [],
    }),
  );
  const codeInventory: ProfileListItem[] = (codeProfiles.data?.profiles ?? []).map(
    (p) => ({
      name: p.name,
      count:
        ctxQuery.data?.code_profiles?.find((c) => c.name === p.name)?.indexed_snippets ??
        0,
      linked: p.linked_db_profiles ?? [],
    }),
  );

  function describe(
    mode: string[] | null,
    autoCount: number,
    label: string,
  ): string {
    if (mode === null) return `${label}: auto (${autoCount})`;
    if (mode.length === 0) return `${label}: off`;
    return `${label}: ${mode.length}`;
  }
  const autoDocCount = ctxQuery.data?.doc_profiles?.length ?? 0;
  const autoCodeCount = ctxQuery.data?.code_profiles?.length ?? 0;
  const triggerLabel = `${describe(docOverride, autoDocCount, "Docs")} · ${describe(
    codeOverride,
    autoCodeCount,
    "Code",
  )}`;
  const isCustom = docOverride !== null || codeOverride !== null;

  function toggle(
    current: string[] | null,
    name: string,
    setter: (next: string[] | null) => void,
  ) {
    // From Auto → first explicit toggle starts a fresh single-pick list.
    if (current === null) {
      setter([name]);
      return;
    }
    if (current.includes(name)) {
      const next = current.filter((p) => p !== name);
      setter(next);
      return;
    }
    setter([...current, name]);
  }

  return (
    <div className="relative" ref={wrapperRef}>
      <button
        type="button"
        disabled={disabled}
        onClick={() => setOpen((v) => !v)}
        title="Pick which doc / code profiles answer this question"
        aria-haspopup="dialog"
        aria-expanded={open}
        className={cn(
          "flex h-7 items-center gap-1.5 rounded-md border px-2 text-[11px] font-medium transition-colors duration-fast",
          disabled
            ? "cursor-default border-surface-border bg-transparent text-ink-dim"
            : isCustom
              ? "border-accent/30 bg-accent-soft text-accent-ink hover:bg-accent-soft/80"
              : "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink",
        )}
      >
        <FileText size={12} className="opacity-70" />
        <span className="max-w-[16rem] truncate">{triggerLabel}</span>
        <ChevronDown size={12} className="opacity-70" />
      </button>
      {open && (
        <div className="absolute left-0 top-full z-30 mt-1 w-80 overflow-hidden rounded-md border border-border bg-surface-raised shadow-md animate-fade-in">
          <DocCodeSection
            heading="Doc profiles"
            emptyHint="No doc profiles configured."
            inventory={docInventory}
            mode={docOverride}
            autoCount={autoDocCount}
            onToggle={(name) => toggle(docOverride, name, onDocChange)}
            onAuto={() => onDocChange(null)}
            onNone={() => onDocChange([])}
          />
          <div className="border-t border-border" />
          <DocCodeSection
            heading="Code profiles"
            emptyHint="No code profiles configured."
            inventory={codeInventory}
            mode={codeOverride}
            autoCount={autoCodeCount}
            onToggle={(name) => toggle(codeOverride, name, onCodeChange)}
            onAuto={() => onCodeChange(null)}
            onNone={() => onCodeChange([])}
          />
          <div className="border-t border-border bg-surface-subtle/40 px-3 py-1.5 text-[10.5px] text-ink-dim">
            <Link to="/settings?tab=docs" className="hover:text-ink-muted">
              Manage profiles in Settings →
            </Link>
          </div>
        </div>
      )}
    </div>
  );
}

function DocCodeSection({
  heading,
  emptyHint,
  inventory,
  mode,
  autoCount,
  onToggle,
  onAuto,
  onNone,
}: {
  heading: string;
  emptyHint: string;
  inventory: ProfileListItem[];
  mode: string[] | null;
  autoCount: number;
  onToggle: (name: string) => void;
  onAuto: () => void;
  onNone: () => void;
}) {
  return (
    <div className="px-3 py-2">
      <div className="mb-1.5 flex items-center justify-between">
        <span className="text-[10.5px] font-medium uppercase tracking-wider text-ink-dim">
          {heading}
        </span>
        <div className="flex gap-1 text-[10.5px]">
          <button
            type="button"
            onClick={onAuto}
            className={cn(
              "rounded px-1.5 py-0.5",
              mode === null
                ? "bg-accent-soft/60 text-accent-ink"
                : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
            )}
          >
            Auto ({autoCount})
          </button>
          <button
            type="button"
            onClick={onNone}
            className={cn(
              "rounded px-1.5 py-0.5",
              mode !== null && mode.length === 0
                ? "bg-warning-soft text-warning"
                : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
            )}
          >
            None
          </button>
        </div>
      </div>
      {inventory.length === 0 ? (
        <p className="text-[10.5px] text-ink-dim">{emptyHint}</p>
      ) : (
        <ul role="listbox" aria-multiselectable className="space-y-0.5">
          {inventory.map((p) => {
            const selected = mode !== null && mode.includes(p.name);
            return (
              <li key={p.name}>
                <button
                  type="button"
                  role="option"
                  aria-selected={selected}
                  onClick={() => onToggle(p.name)}
                  className={cn(
                    "flex w-full items-center justify-between gap-2 rounded px-2 py-1 text-left text-xs hover:bg-surface-subtle",
                    selected && "bg-accent-soft/40",
                  )}
                >
                  <span className="min-w-0 flex-1">
                    <span className="block truncate font-mono text-ink">
                      {p.name}
                    </span>
                    <span className="block truncate text-[10px] text-ink-dim">
                      {p.linked.length ? `linked: ${p.linked.join(", ")}` : "global"}
                      {p.count > 0 && ` · ${p.count}`}
                    </span>
                  </span>
                  {selected && <Check size={12} className="text-accent" />}
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
