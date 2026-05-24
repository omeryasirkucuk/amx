// useEventSource — thin React hook wrapping the browser's
// EventSource API. AMX Studio uses SSE instead of WebSockets
// because every server → client stream we need (run progress, ask
// thinking, apply writeback) is one-way and SSE rides through any
// proxy that allows long-lived HTTP.
//
// The hook auto-attaches the bearer token via ?t=... — browsers
// don't let us set headers on EventSource. It also closes the
// stream cleanly on component unmount, on a `terminal` event, or
// when `enabled` flips false.
//
// Resilience
// ----------
// 1. **Exponential reconnect.** A network blip on a long run no
//    longer surfaces as a permanent "Connection lost" toast. The
//    hook retries with 1s, 2s, 4s, 8s, 16s, 30s backoff (cap), up
//    to MAX_RECONNECT_ATTEMPTS times. Once the cap is reached we
//    stop retrying and surface the error so the user can manually
//    reload.
// 2. **Events cap.** The events array is capped at MAX_EVENTS in
//    FIFO order so a runaway producer (e.g. a chatty thinking.delta
//    stream) cannot grow the array unboundedly across a multi-hour
//    run. The cap is generous (5000) — far more than any UI today
//    actually displays at once — so reaching it is a sentinel that
//    something is wrong, not an everyday case.

import { useCallback, useEffect, useRef, useState } from "react";

import { tokenQuerySuffix } from "./auth";

export interface SseEvent {
  type: string;
  [key: string]: unknown;
}

interface UseEventSourceOptions {
  /** Path under the FastAPI app, e.g. "/api/apply/abc/events". */
  path: string;
  /** Set false to keep the hook idle (e.g. before a job id exists). */
  enabled?: boolean;
  /** Event types that close the stream. Defaults to job.{done,cancelled,failed}. */
  terminalTypes?: readonly string[];
}

const DEFAULT_TERMINAL = ["job.done", "job.cancelled", "job.failed"] as const;

/** Cap on retained events so a runaway stream cannot grow memory. */
export const MAX_EVENTS = 5000;
/** Max reconnect attempts after a transport error. Long-running ingest
 *  / writeback jobs need a generous budget — corporate proxies that
 *  close idle HTTP connections every 30–60s would otherwise exhaust
 *  five attempts and surface a misleading "connection lost" banner
 *  in the middle of an otherwise-healthy run. With the 30s backoff
 *  cap, 20 attempts give ~10 minutes of recoverable transient
 *  failure before the hook hands off to the manual Reconnect button. */
const MAX_RECONNECT_ATTEMPTS = 20;
/** Backoff schedule (milliseconds). Index = attempt count. Last entry caps. */
const RECONNECT_BACKOFF_MS = [1_000, 2_000, 4_000, 8_000, 16_000, 30_000] as const;

const NAMED_EVENTS = [
  "activity.added",
  "activity.begin",
  "activity.complete",
  "activity.fail",
  "writeback.progress",
  "thinking.delta",
  "thinking.stop",
  "tool.call",
  "tool.started",
  "tool.result",
  "answer.delta",
  "answer.final",
  "llm.round.started",
  "llm.round.heartbeat",
  "llm.round.finished",
  "tokens",
  "tokens.snapshot",
  // Fine-grained sub-step events bridged from the CLI's LiveDisplay
  // into the web SSE stream (see amx/utils/live_display.py +
  // amx/web/routers/runs.py). These are what makes the run-detail
  // page show "Profiling address.state…" / "Calling LLM (batch 1/3)"
  // instead of just "Waiting for the worker to begin…".
  "step.added",
  "step.begin",
  "step.update",
  "step.complete",
  "step.fail",
  "step.detail",
  "step.thinking",
  "step.thinking_done",
  "tokens.delta",
  // Global pip-install bus events — emitted by amx.utils.optional_deps
  // whenever the backend lazy-installs an optional dependency. Drives
  // the floating InstallBanner so users see "Installing libraries for
  // Snowflake…" instead of a silent 30s delay.
  "pip.install.begin",
  "pip.install.progress",
  "pip.install.done",
  "pip.install.failed",
] as const;

export interface UseEventSourceResult {
  events: SseEvent[];
  closed: boolean;
  error: string | null;
  /** Reconnect-attempt counter exposed for UI ("Reconnecting (3/5)"). */
  retryAttempt: number;
  /** Backoff cap reached — the auto-retry loop has given up. The
   *  caller should show a manual Reconnect affordance and call
   *  ``reconnect()`` to restart the loop. */
  exhausted: boolean;
  /** Manually reset the retry counter and re-open the stream. Use this
   *  instead of forcing a page reload — the caller's in-memory state
   *  (chat transcript, scroll position) stays intact. */
  reconnect: () => void;
}

export function useEventSource({
  path,
  enabled = true,
  terminalTypes = DEFAULT_TERMINAL,
}: UseEventSourceOptions): UseEventSourceResult {
  const [events, setEvents] = useState<SseEvent[]>([]);
  const [closed, setClosed] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [retryAttempt, setRetryAttempt] = useState(0);
  const [exhausted, setExhausted] = useState(false);
  const [reconnectNonce, setReconnectNonce] = useState(0);
  const sourceRef = useRef<EventSource | null>(null);

  const reconnect = useCallback(() => {
    setReconnectNonce((n) => n + 1);
  }, []);

  useEffect(() => {
    if (!enabled) return;
    setEvents([]);
    setClosed(false);
    setError(null);
    setRetryAttempt(0);
    setExhausted(false);
    const suffix = tokenQuerySuffix();
    const sep = path.includes("?") ? "&" : "?";
    const url = suffix ? `${path}${sep}${suffix}` : path;

    let stopped = false;
    let attempts = 0;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

    const handle = (raw: MessageEvent) => {
      if (stopped) return;
      try {
        const event = JSON.parse(raw.data) as SseEvent;
        // A successful frame resets the reconnect counter — a
        // transient blip should not poison the budget for the rest
        // of a long-lived stream.
        attempts = 0;
        setRetryAttempt(0);
        // Clear any "connection lost" message left over from a
        // previous reconnect attempt now that data is flowing again.
        setError(null);
        setEvents((current) => {
          const next =
            current.length >= MAX_EVENTS
              ? [...current.slice(current.length - MAX_EVENTS + 1), event]
              : [...current, event];
          return next;
        });
        if (terminalTypes.includes(event.type)) {
          stopped = true;
          if (reconnectTimer !== null) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
          }
          if (sourceRef.current) {
            sourceRef.current.close();
            sourceRef.current = null;
          }
          setClosed(true);
        }
      } catch {
        // Non-JSON keepalive frames; ignore.
      }
    };

    const connect = () => {
      if (stopped) return;
      const source = new EventSource(url);
      sourceRef.current = source;
      source.onmessage = handle;
      // Some servers (and our backend) send named events too.
      [...terminalTypes, ...NAMED_EVENTS].forEach((evtName) => {
        source.addEventListener(evtName, handle as EventListener);
      });
      source.onerror = () => {
        if (stopped) return;
        // Tear down the failed transport before deciding what to do
        // — leaving a half-open EventSource around triggers
        // duplicate ``onerror`` callbacks during the backoff.
        try {
          source.close();
        } catch {
          // close() is documented as never throwing; defensive guard
          // for non-conformant runtimes (older Edge, JSDOM, etc.).
        }
        if (sourceRef.current === source) {
          sourceRef.current = null;
        }
        if (attempts >= MAX_RECONNECT_ATTEMPTS) {
          // Auto-retry budget exhausted. The caller surfaces a
          // manual Reconnect button that calls ``reconnect()`` —
          // bumping ``reconnectNonce`` re-runs this effect with a
          // fresh attempt counter.
          setError("Connection lost. Click Reconnect to retry.");
          setExhausted(true);
          stopped = true;
          return;
        }
        const delay =
          RECONNECT_BACKOFF_MS[Math.min(attempts, RECONNECT_BACKOFF_MS.length - 1)];
        attempts += 1;
        setRetryAttempt(attempts);
        setError(
          `Connection lost. Reconnecting (attempt ${attempts}/${MAX_RECONNECT_ATTEMPTS})…`,
        );
        reconnectTimer = setTimeout(connect, delay);
      };
    };

    connect();

    return () => {
      stopped = true;
      if (reconnectTimer !== null) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      if (sourceRef.current) {
        sourceRef.current.close();
        sourceRef.current = null;
      }
    };
  }, [enabled, path, terminalTypes, reconnectNonce]);

  return { events, closed, error, retryAttempt, exhausted, reconnect };
}

/** Public constant the caller can read to render the X/N progress
 *  indicator without importing the internal config. */
export const SSE_MAX_RECONNECT_ATTEMPTS = MAX_RECONNECT_ATTEMPTS;
