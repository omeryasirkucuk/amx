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

import { useEffect, useRef, useState } from "react";

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
/** Max reconnect attempts after a transport error. */
const MAX_RECONNECT_ATTEMPTS = 5;
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
  "tool.result",
  "answer.final",
  "tokens",
  "tokens.snapshot",
] as const;

export function useEventSource({
  path,
  enabled = true,
  terminalTypes = DEFAULT_TERMINAL,
}: UseEventSourceOptions): {
  events: SseEvent[];
  closed: boolean;
  error: string | null;
} {
  const [events, setEvents] = useState<SseEvent[]>([]);
  const [closed, setClosed] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const sourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    if (!enabled) return;
    setEvents([]);
    setClosed(false);
    setError(null);
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
          setError("Connection lost. Please reload the page.");
          stopped = true;
          return;
        }
        const delay =
          RECONNECT_BACKOFF_MS[Math.min(attempts, RECONNECT_BACKOFF_MS.length - 1)];
        attempts += 1;
        setError(`Connection lost. Reconnecting (attempt ${attempts})…`);
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
  }, [enabled, path, terminalTypes]);

  return { events, closed, error };
}
