/**
 * Streaming AI hook — consumes SSE from
 * ``/api/lineage/{anchor}/suggest/stream`` and emits edge batches as
 * the extractor pipeline reports them.
 *
 * The caller passes ``onBatch`` to merge each batch into the canvas
 * (FK first, then view DDL, then deterministic, then LLM). ``onDone``
 * fires once the pipeline finishes; ``onError`` for unrecoverable
 * stream failures. SSE auto-reconnect is disabled — for a single
 * generate pass we'd rather report the failure than silently retry.
 *
 * Authentication: the SSE EventSource API can't attach an
 * ``Authorization`` header, so we fall back to ``fetch`` with a
 * manual ReadableStream reader so the existing ``apiFetch`` token
 * flow continues to work.
 */

import { useCallback, useRef } from "react";
import { getStoredToken } from "../../lib/auth";

export interface StreamBatch {
  extractor: string;
  partial?: boolean;
  edges: Array<{
    from: string;
    to: string;
    from_column?: string;
    to_column?: string;
    type: string;
    extractor: string;
    confidence: number;
    operator?: { op_kind: string; expression: string } | null;
  }>;
}

interface Options {
  onBatch: (batch: StreamBatch) => void;
  onDone?: (totals: { total_edges: number }) => void;
  onError?: (message: string) => void;
  onWarning?: (message: string) => void;
}

export function useStreamingAI(opts: Options) {
  const abortRef = useRef<AbortController | null>(null);

  const start = useCallback(
    async (anchorPath: string, params: { profile: string; database?: string }) => {
      abortRef.current?.abort();
      const ctrl = new AbortController();
      abortRef.current = ctrl;
      const qs = new URLSearchParams();
      qs.set("profile", params.profile);
      if (params.database) qs.set("database", params.database);
      const token = getStoredToken();
      try {
        const res = await fetch(
          `/api/lineage/${encodeURI(anchorPath)}/suggest/stream?${qs.toString()}`,
          {
            method: "GET",
            headers: token ? { Authorization: `Bearer ${token}` } : {},
            signal: ctrl.signal,
          },
        );
        if (!res.ok || !res.body) {
          opts.onError?.(`SSE stream failed (HTTP ${res.status})`);
          return;
        }
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          // SSE messages are separated by a blank line.
          let sep = buffer.indexOf("\n\n");
          while (sep >= 0) {
            const raw = buffer.slice(0, sep).trim();
            buffer = buffer.slice(sep + 2);
            sep = buffer.indexOf("\n\n");
            if (!raw) continue;
            const lines = raw.split("\n");
            let event = "message";
            let data = "";
            for (const ln of lines) {
              if (ln.startsWith("event:")) event = ln.slice(6).trim();
              else if (ln.startsWith("data:")) data += ln.slice(5).trim();
            }
            try {
              const payload = data ? JSON.parse(data) : {};
              if (event === "edges-batch") {
                opts.onBatch(payload as StreamBatch);
              } else if (event === "done") {
                opts.onDone?.(payload);
              } else if (event === "error") {
                opts.onError?.(String(payload.message || "stream error"));
              } else if (event === "warning") {
                opts.onWarning?.(String(payload.message || ""));
              }
            } catch {
              // Ignore malformed events; the pipeline can still produce
              // valid subsequent ones.
            }
          }
        }
      } catch (e) {
        if ((e as Error).name === "AbortError") return;
        opts.onError?.((e as Error).message);
      }
    },
    [opts],
  );

  const cancel = useCallback(() => {
    abortRef.current?.abort();
  }, []);

  return { start, cancel };
}
