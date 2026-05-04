import { useEffect, useMemo, useRef, useState } from "react";
import { Send, Sparkles, Wrench } from "lucide-react";

import { useEventSource, type SseEvent } from "../lib/sse";
import { apiFetch } from "../lib/api";
import { Card } from "./Card";
import { cn } from "../lib/cn";

export interface SubmittedTurn {
  role: "user" | "assistant";
  content: string;
  toolCalls?: Array<{ name: string; arguments: string; result_preview: string }>;
}

interface SubmitResponse {
  job_id: string;
  session_id: number | null;
  status: string;
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

  // Reseed history when the parent picks a different session.
  useEffect(() => {
    setSessionId(selectedSessionId);
    setTurns(seedTurns ?? []);
    setActiveJob(null);
    setSubmitError(null);
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
      const result = await apiFetch<SubmitResponse>("/api/ask", {
        method: "POST",
        body: JSON.stringify({ question: text, session_id: sessionId ?? undefined }),
      });
      setSessionId(result.session_id);
      setActiveJob(result.job_id);
      onSessionAssigned?.(result.session_id);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Ask failed.";
      setSubmitError(message);
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

  return (
    <div className="flex h-full min-h-[60vh] flex-col gap-4">
      <div className="flex-1 space-y-4 overflow-y-auto rounded-xl border border-surface-border bg-surface-raised p-4">
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
            {turn.content}
            {turn.toolCalls && turn.toolCalls.length > 0 && (
              <ToolCallList calls={turn.toolCalls} />
            )}
          </Bubble>
        ))}
        {activeJob && (
          <Bubble role="assistant" pulsing>
            {thinking ? (
              <div className="space-y-1">
                <div className="text-[11px] font-semibold uppercase tracking-wider text-ink-dim">
                  Thinking
                </div>
                <div className="whitespace-pre-wrap text-sm text-ink-muted">{thinking}</div>
              </div>
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
        <form onSubmit={handleSubmit} className="flex items-end gap-2">
          <textarea
            ref={inputRef}
            className="min-h-[44px] flex-1 resize-none rounded-md border border-surface-border bg-surface px-3 py-2 text-sm placeholder:text-ink-dim focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20"
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
          "max-w-2xl rounded-2xl px-4 py-2.5 text-sm shadow-sm",
          role === "user"
            ? "bg-accent text-accent-soft"
            : "bg-surface-subtle text-ink",
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
