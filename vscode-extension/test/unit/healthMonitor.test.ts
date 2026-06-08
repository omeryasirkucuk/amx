// HealthMonitor: probe-loop failure threshold, recovery reset, and
// discovery-file change detection across atomic replaces.
import { mkdtemp, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { HealthMonitor } from "../../src/server/healthMonitor";

let dir: string;
let monitor: HealthMonitor | undefined;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "amx-monitor-"));
});

afterEach(async () => {
  monitor?.dispose();
  monitor = undefined;
  await rm(dir, { recursive: true, force: true });
});

const flushTimers = async (ms: number, steps = 10): Promise<void> => {
  // advance in slices so chained setTimeout callbacks (probe →
  // reschedule) all get a chance to run their async bodies.
  for (let i = 0; i < steps; i += 1) {
    await vi.advanceTimersByTimeAsync(ms / steps);
  }
};

describe("HealthMonitor probe loop", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("declares the server lost after consecutive failures", async () => {
    const onServerLost = vi.fn();
    const probe = vi.fn().mockResolvedValue(false);
    monitor = new HealthMonitor({
      probe,
      watchDir: dir,
      intervalMs: 100,
      failureThreshold: 2,
      onServerLost,
    });
    monitor.start();

    await flushTimers(150);
    expect(onServerLost).not.toHaveBeenCalled(); // one failure tolerated

    await flushTimers(150);
    expect(onServerLost).toHaveBeenCalledTimes(1);
    expect(monitor.active).toBe(false); // stops itself before notifying
  });

  it("resets the failure count after a successful probe", async () => {
    const onServerLost = vi.fn();
    const probe = vi
      .fn()
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(false)
      .mockResolvedValue(true);
    monitor = new HealthMonitor({
      probe,
      watchDir: dir,
      intervalMs: 100,
      failureThreshold: 2,
      onServerLost,
    });
    monitor.start();

    await flushTimers(600);
    expect(onServerLost).not.toHaveBeenCalled();
    expect(monitor.lastHealthyAt).toBeDefined();
  });

  it("treats a throwing probe as a failure", async () => {
    const onServerLost = vi.fn();
    const probe = vi.fn().mockRejectedValue(new Error("connection refused"));
    monitor = new HealthMonitor({
      probe,
      watchDir: dir,
      intervalMs: 100,
      failureThreshold: 2,
      onServerLost,
    });
    monitor.start();

    await flushTimers(400);
    expect(onServerLost).toHaveBeenCalledTimes(1);
  });

  it("does not probe after stop()", async () => {
    const probe = vi.fn().mockResolvedValue(true);
    monitor = new HealthMonitor({
      probe,
      watchDir: dir,
      intervalMs: 100,
      onServerLost: vi.fn(),
    });
    monitor.start();
    await flushTimers(150);
    const calls = probe.mock.calls.length;
    monitor.stop();
    await flushTimers(500);
    expect(probe.mock.calls.length).toBe(calls);
  });
});

describe("HealthMonitor discovery watch", () => {
  // Real timers: fs.watch events arrive on the real event loop.
  it("fires onDiscoveryChanged for an atomic temp-file replace", async () => {
    const changed = new Promise<void>((resolve) => {
      monitor = new HealthMonitor({
        probe: () => Promise.resolve(true),
        watchDir: dir,
        intervalMs: 60_000, // keep the probe loop out of the way
        onServerLost: () => {},
        onDiscoveryChanged: () => resolve(),
      });
    });
    monitor?.start();

    // Mirror the Python writer: temp file in the same dir + rename.
    const tmp = join(dir, ".studio.json.tmp");
    await writeFile(tmp, JSON.stringify({ port: 1 }), "utf-8");
    await rename(tmp, join(dir, "studio.json"));

    await expect(
      Promise.race([
        changed,
        new Promise((_resolve, reject) =>
          setTimeout(() => reject(new Error("no discovery event within 5s")), 5_000),
        ),
      ]),
    ).resolves.toBeUndefined();
  });

  it("ignores changes to unrelated files", async () => {
    const onDiscoveryChanged = vi.fn();
    monitor = new HealthMonitor({
      probe: () => Promise.resolve(true),
      watchDir: dir,
      intervalMs: 60_000,
      onServerLost: () => {},
      onDiscoveryChanged,
    });
    monitor.start();

    await writeFile(join(dir, "config.yml"), "x: 1", "utf-8");
    await new Promise((resolve) => setTimeout(resolve, 700)); // > debounce
    expect(onDiscoveryChanged).not.toHaveBeenCalled();
  });
});
