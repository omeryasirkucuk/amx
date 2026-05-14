// Tiny pub/sub for surfacing React Query failures through the global
// toast layer. The QueryClient's QueryCache + MutationCache call
// `publishQueryError` from their `onError` hooks; a single
// `<QueryErrorListener />` mounted under ToastProvider subscribes and
// turns each event into a toast.
//
// Per-query opt-out: set `meta: { silentError: true }` on a `useQuery`
// or `useMutation` that already renders its own inline error UI to
// avoid double-rendering.

import { ApiError } from "./api";

export interface QueryErrorEvent {
  error: unknown;
  /** "query" or "mutation" — controls the toast title prefix. */
  source: "query" | "mutation";
  /** Stringified queryKey (queries only) for debugging — included so a
   *  future toast variant can show the source path on hover. */
  scope?: string;
}

type Listener = (event: QueryErrorEvent) => void;

const listeners = new Set<Listener>();

export function subscribeQueryErrors(listener: Listener): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function publishQueryError(event: QueryErrorEvent): void {
  for (const listener of listeners) {
    try {
      listener(event);
    } catch (err) {
      // A broken listener should never silently swallow the original
      // error — log so the dev console still flags both.
      // eslint-disable-next-line no-console
      console.error("[amx] query error listener threw:", err);
    }
  }
}

/** Best-effort title for a query failure toast. */
export function describeQueryError(error: unknown): {
  title: string;
  description: string;
  hint?: string;
} {
  if (error instanceof ApiError) {
    return {
      title: `Request failed (${error.status})`,
      description: error.detail || error.message,
      hint: error.hint,
    };
  }
  if (error instanceof Error) {
    return { title: "Request failed", description: error.message };
  }
  return { title: "Request failed", description: String(error) };
}

declare module "@tanstack/react-query" {
  interface Register {
    queryMeta: {
      /** Skip the global error toast — caller renders its own. */
      silentError?: boolean;
    };
    mutationMeta: {
      /** Skip the global error toast — caller renders its own. */
      silentError?: boolean;
    };
  }
}
