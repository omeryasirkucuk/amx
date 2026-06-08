// Small async helpers shared across services. No vscode imports so
// the helpers stay unit-testable in plain vitest.

/**
 * Collapse concurrent calls into one in-flight promise. While a call
 * is pending every new call returns the same promise; after it
 * settles the next call starts fresh.
 */
export function singleFlight<T>(fn: () => Promise<T>): () => Promise<T> {
  let inFlight: Promise<T> | undefined;
  return () => {
    inFlight ??= fn().finally(() => {
      inFlight = undefined;
    });
    return inFlight;
  };
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Exponential backoff delays: base, base*2, base*4 ... capped.
 * Returns the delay for the given retry attempt (0-based).
 */
export function backoffDelayMs(attempt: number, baseMs = 1000, capMs = 30_000): number {
  return Math.min(baseMs * 2 ** attempt, capMs);
}

/**
 * Run `worker` over `items` with at most `concurrency` in flight.
 * Results are returned index-aligned with `items`. Before each item is
 * dispatched the pool consults `shouldStop`; once it returns true no
 * further items start and their slots stay `undefined` (already
 * in-flight workers finish). The worker is expected to handle its own
 * failures and resolve to a result value — a thrown worker rejects the
 * whole pool, which callers generally don't want for best-effort bulk
 * work.
 */
export async function mapPool<T, R>(
  items: readonly T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<R>,
  shouldStop?: () => boolean,
): Promise<(R | undefined)[]> {
  const results: (R | undefined)[] = new Array(items.length).fill(undefined);
  const width = Math.max(1, Math.min(concurrency, items.length));
  let cursor = 0;
  const runLane = async (): Promise<void> => {
    for (;;) {
      if (shouldStop?.()) return;
      const index = cursor++;
      if (index >= items.length) return;
      results[index] = await worker(items[index]!, index);
    }
  };
  await Promise.all(Array.from({ length: width }, () => runLane()));
  return results;
}

/** Reject after `ms` when the wrapped promise hasn't settled. */
export function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });
}
