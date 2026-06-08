// mapPool: bounded-concurrency map with cooperative cancellation,
// the engine behind bulk description generation.
import { describe, expect, it, vi } from "vitest";

import { mapPool } from "../../src/util/async";

describe("mapPool", () => {
  it("returns results index-aligned with the input", async () => {
    const out = await mapPool([1, 2, 3, 4], 2, async (n) => n * 10);
    expect(out).toEqual([10, 20, 30, 40]);
  });

  it("never exceeds the concurrency width", async () => {
    let active = 0;
    let peak = 0;
    await mapPool(Array.from({ length: 10 }, (_v, i) => i), 3, async (n) => {
      active += 1;
      peak = Math.max(peak, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active -= 1;
      return n;
    });
    expect(peak).toBeLessThanOrEqual(3);
  });

  it("stops dispatching once shouldStop flips, leaving slots undefined", async () => {
    let processed = 0;
    let stop = false;
    const worker = vi.fn(async (n: number) => {
      processed += 1;
      if (processed >= 2) stop = true; // cancel after the second item
      return n;
    });
    const out = await mapPool([1, 2, 3, 4, 5], 1, worker, () => stop);
    // Width 1 → strictly sequential; items 1 and 2 run, then stop.
    expect(out.slice(0, 2)).toEqual([1, 2]);
    expect(out.slice(2)).toEqual([undefined, undefined, undefined]);
    expect(worker).toHaveBeenCalledTimes(2);
  });

  it("handles an empty input", async () => {
    expect(await mapPool([], 4, async (n) => n)).toEqual([]);
  });
});
