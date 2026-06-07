// SSE client for Node (no EventSource in the extension host).
// Parses a fetch ReadableStream into events, mirroring the SPA's
// frontend/src/lib/sse.ts semantics: `?t=` token auth, exponential
// backoff reconnect, Last-Event-ID resume, terminal event types end
// the stream.
import { backoffDelayMs, sleep } from "../util/async";
import type { SseEvent } from "./types";

const TERMINAL_TYPES = new Set(["job.done", "job.cancelled", "job.failed"]);
const MAX_RECONNECTS = 20;

export interface SseOptions {
  signal?: AbortSignal;
  /** Event types that end the stream. Defaults to job terminals. */
  terminalTypes?: ReadonlySet<string>;
}

/** Parse one raw SSE frame (lines between blank-line separators). */
export function parseSseFrame(frame: string): SseEvent | undefined {
  let event: string | undefined;
  let id: string | undefined;
  const dataLines: string[] = [];
  for (const line of frame.split("\n")) {
    if (line.startsWith(":")) continue; // heartbeat comment
    const colon = line.indexOf(":");
    if (colon === -1) continue;
    const field = line.slice(0, colon);
    const value = line.slice(colon + 1).replace(/^ /, "");
    if (field === "event") event = value;
    else if (field === "id") id = value;
    else if (field === "data") dataLines.push(value);
  }
  if (dataLines.length === 0 && !event) return undefined;
  const raw = dataLines.join("\n");
  let data: unknown = raw;
  if (raw) {
    try {
      data = JSON.parse(raw);
    } catch {
      // plain-text data — keep the string
    }
  }
  const result: SseEvent = { data, raw };
  if (event !== undefined) result.event = event;
  if (id !== undefined) result.id = id;
  return result;
}

/** Type marker carried inside the JSON payload (`{"type": ...}`). */
export function eventType(event: SseEvent): string | undefined {
  if (event.event) return event.event;
  if (event.data && typeof event.data === "object") {
    const type = (event.data as { type?: unknown }).type;
    if (typeof type === "string") return type;
  }
  return undefined;
}

/**
 * Stream SSE events from an authenticated AMX endpoint.
 * `url` must NOT already carry the token — it is appended here the
 * way the SPA does for EventSource (`?t=`).
 */
export async function* streamSse(
  url: string,
  token: string,
  options: SseOptions = {},
): AsyncGenerator<SseEvent> {
  const terminals = options.terminalTypes ?? TERMINAL_TYPES;
  let lastEventId: string | undefined;
  let attempt = 0;

  while (attempt <= MAX_RECONNECTS) {
    const target = new URL(url);
    target.searchParams.set("t", token);
    const headers: Record<string, string> = { Accept: "text/event-stream" };
    if (lastEventId) headers["Last-Event-ID"] = lastEventId;

    let response: Response;
    try {
      const init: RequestInit = { headers };
      if (options.signal) init.signal = options.signal;
      response = await fetch(target, init);
    } catch (error) {
      if (options.signal?.aborted) return;
      attempt += 1;
      await sleep(backoffDelayMs(attempt, 1000, 30_000));
      continue;
    }
    if (!response.ok || !response.body) {
      attempt += 1;
      await sleep(backoffDelayMs(attempt, 1000, 30_000));
      continue;
    }
    attempt = 0; // healthy connection resets the budget

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let separator: number;
        while ((separator = buffer.indexOf("\n\n")) !== -1) {
          const frame = buffer.slice(0, separator).replace(/\r/g, "");
          buffer = buffer.slice(separator + 2);
          const event = parseSseFrame(frame);
          if (!event) continue;
          if (event.id) lastEventId = event.id;
          yield event;
          const type = eventType(event);
          if (type && terminals.has(type)) return;
        }
      }
    } catch {
      if (options.signal?.aborted) return;
      // fall through to reconnect
    } finally {
      reader.releaseLock();
    }
    if (options.signal?.aborted) return;
    attempt += 1;
    await sleep(backoffDelayMs(attempt, 1000, 30_000));
  }
}
