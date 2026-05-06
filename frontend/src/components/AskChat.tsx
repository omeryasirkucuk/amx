import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Send, Settings as SettingsIcon, Sparkles, Wrench } from "lucide-react";
import { Link } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { useEventSource, type SseEvent } from "../lib/sse";
import { apiFetch, ApiError } from "../lib/api";
import { useUi } from "../lib/store";
import { Card } from "./Card";
import { cn } from "../lib/cn";
import { InfoHint } from "./ui";
import AskScopeDropdown from "./AskScopeDropdown";

export interface SubmittedTurn {
  role: "user" | "assistant";
  content: string;
  toolCalls?: Array<{
    name: string;
    arguments: string;
    result_preview: string;
    latency_ms?: number;
  }>;
  /** Multi-profile observability stamped on assistant turns. */
  scopeProfiles?: string[];
  focusProfile?: string | null;
  totalLatencyMs?: number;
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
  onSessionAssigned,
  onResumeStale,
}: AskChatProps) {
  const [turns, setTurns] = useState<SubmittedTurn[]>([]);
  const [question, setQuestion] = useState("");
  const [activeJob, setActiveJob] = useState<string | null>(null);
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
  const { thinking, finalAnswer, toolCalls, finalMeta, jobFailure } = useMemo(() => {
    const thinkingChunks: string[] = [];
    const tools: Array<{
      name: string;
      arguments: string;
      result_preview: string;
      latency_ms?: number;
    }> = [];
    let finalText: string | null = null;
    let meta: {
      scopeProfiles?: string[];
      focusProfile?: string | null;
      totalLatencyMs?: number;
    } = {};
    let failure: { message: string; hint?: string } | null = null;
    for (const event of events) {
      if (event.type === "thinking.delta" && typeof event.text === "string") {
        thinkingChunks.push(event.text);
      } else if (event.type === "tool.call") {
        tools.push({
          name: String(event.name || ""),
          arguments: String(event.arguments || ""),
          result_preview: String(event.result_preview || ""),
          latency_ms:
            typeof event.latency_ms === "number" ? event.latency_ms : undefined,
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
      } else if (event.type === "job.failed") {
        failure = {
          message: String(event.error || "Ask failed."),
          hint: typeof event.hint === "string" ? event.hint : undefined,
        };
      }
    }
    return {
      thinking: thinkingChunks.join(""),
      finalAnswer: finalText,
      toolCalls: tools,
      finalMeta: meta,
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
          },
        ];
      });
      setActiveJob(null);
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
      clearAskActiveJob(sessionKey);
      return;
    }
    // Stream closed without final answer AND without job.failed (rare
    // — proxy reset, network glitch). Surface a generic message so
    // the chat doesn't hang on "Reasoning…".
    setSubmitError(
      "The ask stream ended without a final answer. Try again, or check Settings → LLM if this keeps happening.",
    );
    setSubmitErrorHint("configure-llm");
    setActiveJob(null);
    clearAskActiveJob(sessionKey);
  }, [closed, finalAnswer, jobFailure, toolCalls, finalMeta, sessionKey, clearAskActiveJob]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const text = question.trim();
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
  }, [turns, thinking, toolCalls.length, activeJob]);

  return (
    <div className="flex h-[calc(100vh-12rem)] min-h-[28rem] flex-col gap-4">
      <div
        ref={messagesRef}
        className="flex-1 space-y-4 overflow-y-auto rounded-xl border border-surface-border bg-surface-raised p-4"
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
          <Bubble role="assistant" pulsing>
            {thinking ? (
              <ThinkingBlock text={thinking} />
            ) : (
              <span className="text-sm text-ink-dim">Reasoning…</span>
            )}
            {toolCalls.length > 0 && <ToolCallList calls={toolCalls} live />}
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
        <div className="mb-2 flex items-center justify-end">
          <AskScopeDropdown
            scope={scopeForSession}
            onChange={handleScopeChange}
            disabled={!!activeJob}
          />
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
  calls: SseEvent[] | Array<{ name: string; arguments: string; result_preview: string }>;
  live?: boolean;
}) {
  return (
    <details className={cn("mt-3", live && "")} open={live}>
      <summary className="flex cursor-pointer items-center gap-1.5 text-[11px] font-medium uppercase tracking-wider text-ink-dim">
        <Wrench size={12} /> {calls.length} tool call{calls.length === 1 ? "" : "s"}
      </summary>
      <ul className="mt-2 space-y-1 text-xs">
        {calls.map((call, idx) => {
          const c = call as { name: string; arguments: string; result_preview: string };
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
