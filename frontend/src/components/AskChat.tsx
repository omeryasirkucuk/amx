import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Send, Sparkles, Wrench } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { useEventSource, type SseEvent } from "../lib/sse";
import { apiFetch } from "../lib/api";
import { useUi } from "../lib/store";
import { Card } from "./Card";
import { cn } from "../lib/cn";
import { InfoHint } from "./ui";
import AskScopeDropdown from "./AskScopeDropdown";

export interface SubmittedTurn {
  role: "user" | "assistant";
  content: string;
  toolCalls?: Array<{ name: string; arguments: string; result_preview: string }>;
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
}

// Self-contained chat panel — owns the question textarea, SSE
// subscription for the current job, the running thinking ribbon,
// and the message history.
export default function AskChat({
  selectedSessionId,
  seedTurns,
  seedToken,
  onSessionAssigned,
}: AskChatProps) {
  const [turns, setTurns] = useState<SubmittedTurn[]>([]);
  const [question, setQuestion] = useState("");
  const [activeJob, setActiveJob] = useState<string | null>(null);
  const [sessionId, setSessionId] = useState<number | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
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
  const scopeForSession =
    sessionKey in askScopeBySession ? askScopeBySession[sessionKey] : null;

  // Reseed history when the parent picks a different session.
  useEffect(() => {
    setSessionId(selectedSessionId);
    setTurns(seedTurns ?? []);
    setActiveJob(null);
    setSubmitError(null);
    // Drop the "_new_" scratch entry when the parent transitions us
    // to a saved session — the saved session has its own key now.
    if (selectedSessionId != null) {
      clearAskScope("_new_");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seedToken]);

  const { events, closed } = useEventSource({
    path: activeJob ? `/api/ask/${activeJob}/events` : "",
    enabled: !!activeJob,
  });

  // Aggregate streamed events into the assistant's turn.
  const { thinking, finalAnswer, toolCalls } = useMemo(() => {
    const thinkingChunks: string[] = [];
    const tools: Array<{ name: string; arguments: string; result_preview: string }> = [];
    let finalText: string | null = null;
    for (const event of events) {
      if (event.type === "thinking.delta" && typeof event.text === "string") {
        thinkingChunks.push(event.text);
      } else if (event.type === "tool.call") {
        tools.push({
          name: String(event.name || ""),
          arguments: String(event.arguments || ""),
          result_preview: String(event.result_preview || ""),
        });
      } else if (event.type === "answer.final" && typeof event.answer === "string") {
        finalText = event.answer;
      }
    }
    return {
      thinking: thinkingChunks.join(""),
      finalAnswer: finalText,
      toolCalls: tools,
    };
  }, [events]);

  // Once the worker reports answer.final, snapshot the turn into
  // history so the next question doesn't clobber it.
  useEffect(() => {
    if (closed && finalAnswer != null) {
      setTurns((prev) => {
        const last = prev[prev.length - 1];
        if (last?.role === "assistant" && last.content === finalAnswer) return prev;
        return [
          ...prev,
          { role: "assistant", content: finalAnswer, toolCalls },
        ];
      });
      setActiveJob(null);
    }
  }, [closed, finalAnswer, toolCalls]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const text = question.trim();
    if (!text || activeJob) return;
    setSubmitError(null);
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
      onSessionAssigned?.(result.session_id);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Ask failed.";
      setSubmitError(message);
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
          <div className="rounded-md border border-critical/30 bg-critical/5 px-3 py-2 text-xs text-critical">
            {submitError}
          </div>
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
