// Liveness watcher for an adopted (REPL-owned) Studio server. The
// extension has no process handle for servers it merely attached to,
// so death is detected out-of-band: a periodic health probe plus a
// watch on the discovery file the server rewrites/clears. Kept free
// of vscode imports so plain vitest can exercise the timing logic.
import { watch, type FSWatcher } from "node:fs";

export interface HealthMonitorOptions {
  /** Health probe for the monitored server; true means alive. */
  readonly probe: () => Promise<boolean>;
  /** Directory containing the discovery file. Watching the directory
   *  (not the file) survives the atomic temp-file + rename writes the
   *  Python side uses, on every platform. */
  readonly watchDir: string;
  /** Discovery file name inside watchDir. */
  readonly watchFileName?: string;
  /** Delay between successful probes. */
  readonly intervalMs?: number;
  /** Consecutive probe failures before the server counts as lost. */
  readonly failureThreshold?: number;
  /** Fired once when the server is declared lost; the monitor stops
   *  itself first so the callback can safely restart/replace it. */
  readonly onServerLost: () => void;
  /** Fired (debounced) when the discovery file changes — a new server
   *  may have replaced the one being monitored. */
  readonly onDiscoveryChanged?: () => void;
}

const DEFAULT_INTERVAL_MS = 5_000;
const DEFAULT_FAILURE_THRESHOLD = 2;
const DISCOVERY_DEBOUNCE_MS = 300;

export class HealthMonitor {
  private timer: ReturnType<typeof setTimeout> | undefined;
  private debounceTimer: ReturnType<typeof setTimeout> | undefined;
  private watcher: FSWatcher | undefined;
  private failures = 0;
  private running = false;
  private probing = false;

  constructor(private readonly options: HealthMonitorOptions) {}

  /** True while the poll loop is active. */
  get active(): boolean {
    return this.running;
  }

  /** Milliseconds timestamp of the last successful probe, if any. */
  lastHealthyAt: number | undefined;

  start(): void {
    if (this.running) return;
    this.running = true;
    this.failures = 0;
    this.scheduleProbe();
    try {
      // fs.watch on a directory is supported on macOS, Windows, and
      // Linux; per-file watches break on the atomic os.replace() the
      // server uses, because the watched inode is swapped out.
      this.watcher = watch(this.options.watchDir, (_event, filename) => {
        const expected = this.options.watchFileName ?? "studio.json";
        // Some platforms omit the filename; treat those events as
        // potentially relevant rather than dropping them.
        if (filename && filename !== expected) return;
        this.debounceDiscoveryChanged();
      });
    } catch {
      // Watching is an accelerator on top of the poll loop, never a
      // requirement — a missing directory just means slower detection.
    }
  }

  stop(): void {
    this.running = false;
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    this.debounceTimer = undefined;
    this.watcher?.close();
    this.watcher = undefined;
  }

  dispose(): void {
    this.stop();
  }

  private scheduleProbe(): void {
    if (!this.running) return;
    this.timer = setTimeout(() => {
      void this.probeOnce();
    }, this.options.intervalMs ?? DEFAULT_INTERVAL_MS);
  }

  private async probeOnce(): Promise<void> {
    if (!this.running || this.probing) {
      this.scheduleProbe();
      return;
    }
    this.probing = true;
    let ok = false;
    try {
      ok = await this.options.probe();
    } catch {
      ok = false;
    } finally {
      this.probing = false;
    }
    if (!this.running) return;
    if (ok) {
      this.failures = 0;
      this.lastHealthyAt = Date.now();
      this.scheduleProbe();
      return;
    }
    this.failures += 1;
    if (this.failures >= (this.options.failureThreshold ?? DEFAULT_FAILURE_THRESHOLD)) {
      // Stop before notifying so the handler can tear down / replace
      // this monitor without racing another probe.
      this.stop();
      this.options.onServerLost();
      return;
    }
    this.scheduleProbe();
  }

  private debounceDiscoveryChanged(): void {
    if (!this.options.onDiscoveryChanged) return;
    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    this.debounceTimer = setTimeout(() => {
      this.debounceTimer = undefined;
      if (this.running) this.options.onDiscoveryChanged?.();
    }, DISCOVERY_DEBOUNCE_MS);
  }
}
