// useEventSource — thin React hook wrapping the browser's
// EventSource API. The visualizer uses SSE instead of WebSockets
// because every server → client stream we need (run progress, ask
// thinking, apply writeback) is one-way and SSE rides through any
// proxy that allows long-lived HTTP.
//
// The hook auto-attaches the bearer token via ?t=... — browsers
// don't let us set headers on EventSource. It also closes the
// stream cleanly on component unmount, on a `terminal` event, or
// when `enabled` flips false.

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
    const source = new EventSource(url);
    sourceRef.current = source;

    const handle = (raw: MessageEvent) => {
      if (stopped) return;
      try {
        const event = JSON.parse(raw.data) as SseEvent;
        setEvents((current) => [...current, event]);
        if (terminalTypes.includes(event.type)) {
          stopped = true;
          source.close();
          setClosed(true);
        }
      } catch (err) {
        // Non-JSON keepalive frames; ignore.
      }
    };
    source.onmessage = handle;
    // Some servers (and our backend) send named events too.
    terminalTypes.forEach((evtName) => {
      source.addEventListener(evtName, handle as EventListener);
    });
    [
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
    ].forEach((evtName) => {
      source.addEventListener(evtName, handle as EventListener);
    });
    source.onerror = () => {
      if (!stopped) {
        setError("Connection lost.");
      }
    };

    return () => {
      stopped = true;
      source.close();
      sourceRef.current = null;
    };
  }, [enabled, path, terminalTypes]);

  return { events, closed, error };
}
