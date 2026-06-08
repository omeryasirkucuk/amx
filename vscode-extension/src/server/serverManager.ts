// Lifecycle of the AMX Studio server the extension talks to.
//
// ensure() is the single entry point: adopt a healthy server recorded
// in the discovery file, otherwise spawn one with the resolved
// runtime. Owned servers are killed on dispose and restarted (with
// backoff) when they crash; adopted servers are left alone.
import type { ChildProcess } from "node:child_process";
import * as vscode from "vscode";

import type { RuntimeManager } from "../runtime/runtimeManager";
import { backoffDelayMs, singleFlight, sleep } from "../util/async";
import { getServerChannel, log } from "../util/log";
import { amxConfigDir, readDiscovery } from "./discoveryFile";
import { HealthMonitor } from "./healthMonitor";
import { pickPort } from "./ports";
import {
  buildCliSpawnSpec,
  buildServerSpawnSpec,
  isPidAlive,
  killPid,
  killServer,
  probeEmbeddedSupport,
  spawnServer,
} from "./spawn";
import { generateToken } from "./token";

export type RunningServer = {
  status: "running";
  mode: "owned" | "attached";
  port: number;
  token: string;
  baseUrl: string;
  /**
   * Whether the server runs in embedded host mode (frameable
   * headers). False for servers started by an older AMX or launched
   * for a browser (REPL /studio): trees and editor features still
   * work, but iframe panels are blocked and render guidance instead.
   */
  embedded: boolean;
};

export type ServerState =
  | { status: "stopped" }
  | { status: "starting" }
  | RunningServer
  | { status: "error"; message: string };

const HEALTH_TIMEOUT_MS = 1500;
const STARTUP_TIMEOUT_MS = 15_000;
const MAX_RESTARTS = 3;
const RESTART_WINDOW_MS = 60_000;
/** Cached "running" state older than this is revalidated in ensure().
 *  The attached-server monitor refreshes the timestamp every probe,
 *  so the revalidation only fires when the monitor itself is wedged
 *  or was never able to start. */
const HEALTH_STALENESS_MS = 15_000;
/** globalState key holding the pid of the owned server subprocess,
 *  written on spawn and cleared on clean shutdown. A surviving value
 *  on activation means the previous extension host could not finish
 *  its kill — see reconcileOrphans(). */
const OWNED_PID_KEY = "amx.server.ownedPid";

export class ServerManager implements vscode.Disposable {
  private current: ServerState = { status: "stopped" };
  private proc: ChildProcess | undefined;
  private disposing = false;
  private restartTimestamps: number[] = [];
  private monitor: HealthMonitor | undefined;
  private lastHealthyAt = 0;
  private readonly didChange = new vscode.EventEmitter<ServerState>();
  readonly onDidChangeState = this.didChange.event;
  private readonly ensureOnce = singleFlight(() => this.startOrAttach());

  constructor(
    private readonly runtime: RuntimeManager,
    private readonly memento?: vscode.Memento,
  ) {}

  get state(): ServerState {
    return this.current;
  }

  /** Start-or-attach, idempotent and single-flight. */
  async ensure(): Promise<RunningServer> {
    const current = this.current;
    if (current.status === "running") {
      // An attached server can die without any signal to us (REPL
      // Ctrl-C). The monitor usually notices first; this staleness
      // guard catches the window before it does, so callers never
      // get handed a dead server.
      if (current.mode === "attached" && Date.now() - this.lastHealthyAt > HEALTH_STALENESS_MS) {
        const health = await this.healthCheck(current.port, current.token);
        if (!health.ok) {
          log(`cached attached server on :${current.port} failed revalidation`);
          this.setState({ status: "stopped" });
          return this.ensureOnce();
        }
        this.lastHealthyAt = Date.now();
      }
      return current;
    }
    return this.ensureOnce();
  }

  /**
   * Reap an owned server left over from a previous extension host.
   * deactivate() cannot await the kill, so a recorded pid that is
   * still alive but no longer healthy is cleaned up here, on the
   * next activation, before it can squat on the preferred port.
   * A leftover that IS healthy is left alone — tryAdopt() will
   * reuse it through the discovery file.
   */
  async reconcileOrphans(): Promise<void> {
    const pid = this.memento?.get<number>(OWNED_PID_KEY);
    if (pid === undefined) return;
    if (!isPidAlive(pid)) {
      await this.memento?.update(OWNED_PID_KEY, undefined);
      return;
    }
    const record = await readDiscovery();
    const isOurs = record !== undefined && record.pid === pid && record.owner === "vscode";
    if (isOurs && (await this.healthCheck(record.port, record.token)).ok) {
      log(`previous owned server (pid ${pid}) is still healthy — leaving it for adoption`);
      return;
    }
    log(`reaping orphaned Studio server from a previous session (pid ${pid})`);
    await killPid(pid);
    await this.memento?.update(OWNED_PID_KEY, undefined);
  }

  async stop(): Promise<void> {
    if (this.proc) {
      const proc = this.proc;
      this.proc = undefined;
      await killServer(proc);
      await this.memento?.update(OWNED_PID_KEY, undefined);
    }
    this.setState({ status: "stopped" });
  }

  async restart(): Promise<void> {
    await this.stop();
    await this.ensure();
  }

  dispose(): void {
    this.disposing = true;
    this.monitor?.dispose();
    this.monitor = undefined;
    if (this.proc) {
      // Fire and forget — deactivate cannot await long shutdowns.
      // reconcileOrphans() on the next activation handles the case
      // where this kill never finishes; the recorded pid is its input.
      void killServer(this.proc);
      this.proc = undefined;
    }
    this.didChange.dispose();
  }

  private setState(state: ServerState): void {
    this.current = state;
    this.syncMonitor(state);
    this.didChange.fire(state);
  }

  /** Keep the out-of-band liveness monitor running exactly while an
   *  attached server is the current state. Owned servers already
   *  report death through the ChildProcess exit event. */
  private syncMonitor(state: ServerState): void {
    this.monitor?.dispose();
    this.monitor = undefined;
    if (state.status !== "running" || state.mode !== "attached" || this.disposing) return;
    const { port, token } = state;
    this.lastHealthyAt = Date.now();
    const monitor = new HealthMonitor({
      probe: async () => {
        const ok = (await this.healthCheck(port, token)).ok;
        // Feed the staleness guard in ensure() so it only revalidates
        // when this loop has genuinely stopped producing heartbeats.
        if (ok) this.lastHealthyAt = Date.now();
        return ok;
      },
      watchDir: amxConfigDir(),
      onServerLost: () => this.handleAttachedLoss(port),
      onDiscoveryChanged: () => void this.handleDiscoveryChanged(port, token),
    });
    this.monitor = monitor;
    monitor.start();
  }

  /** The attached server stopped answering — drop the state and, when
   *  enabled, re-run the adopt-or-spawn flow through the same backoff
   *  budget owned-server crashes use. */
  private handleAttachedLoss(port: number): void {
    if (this.disposing) return;
    const current = this.current;
    if (current.status !== "running" || current.mode !== "attached" || current.port !== port) {
      return; // superseded — a newer state already replaced this server
    }
    log(`attached Studio server on :${port} stopped responding`);
    this.setState({ status: "stopped" });
    const autoRecover = vscode.workspace
      .getConfiguration("amx")
      .get<boolean>("server.autoRecover", true);
    if (autoRecover) void this.maybeRestart();
  }

  /** The discovery file changed while attached — a new server may have
   *  replaced the monitored one (REPL restart on a new port). Verify
   *  the monitored server; if it is gone, fail over immediately
   *  instead of waiting for the probe loop's failure threshold. */
  private async handleDiscoveryChanged(port: number, token: string): Promise<void> {
    if (this.disposing) return;
    const current = this.current;
    if (current.status !== "running" || current.mode !== "attached" || current.port !== port) {
      return;
    }
    if ((await this.healthCheck(port, token)).ok) {
      this.lastHealthyAt = Date.now();
      return; // still alive — the file change was about someone else
    }
    this.handleAttachedLoss(port);
  }

  private async startOrAttach(): Promise<RunningServer> {
    this.setState({ status: "starting" });
    try {
      const adopted = await this.tryAdopt();
      if (adopted) return adopted;
      return await this.spawnOwned();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.setState({ status: "error", message });
      throw error;
    }
  }

  private async tryAdopt(): Promise<RunningServer | undefined> {
    const adoptExisting = vscode.workspace
      .getConfiguration("amx")
      .get<boolean>("server.adoptExisting", true);
    if (!adoptExisting) return undefined;

    const record = await readDiscovery();
    if (!record) return undefined;
    const health = await this.healthCheck(record.port, record.token);
    if (!health.ok) {
      log(`discovery file points at :${record.port} but the health check failed — ignoring`);
      return undefined;
    }
    log(
      `adopted running Studio server on :${record.port} ` +
        `(owner=${record.owner} embedded=${health.embedded})`,
    );
    const running: RunningServer = {
      status: "running",
      mode: "attached",
      port: record.port,
      token: record.token,
      baseUrl: `http://127.0.0.1:${record.port}`,
      embedded: health.embedded,
    };
    this.setState(running);
    return running;
  }

  private async spawnOwned(): Promise<RunningServer> {
    const runtime = await this.runtime.ensureInstalled();
    if (!runtime) {
      throw new Error("no AMX runtime available — install amx-cli or set amx.pythonPath");
    }
    const preferred = vscode.workspace
      .getConfiguration("amx")
      .get<number>("server.preferredPort", 47821);
    const port = await pickPort(preferred);

    let token: string;
    let embedded: boolean;
    if (runtime.kind === "amx-cli") {
      // The CLI launcher generates its own token; recover it from the
      // discovery file once the server has written it. Both the
      // discovery file and --embedded shipped together, so an older
      // CLI can't be driven headless at all — fail with guidance
      // instead of hanging on a discovery file that never appears.
      embedded = await probeEmbeddedSupport({ kind: "cli", amxCliPath: runtime.amxCliPath });
      if (!embedded) {
        throw new Error(
          `the installed amx CLI (${runtime.amxVersion}) predates the embedded host mode — ` +
            "run `pip install --upgrade amx-cli` (or pipx upgrade amx-cli) and retry",
        );
      }
      const spec = buildCliSpawnSpec({ amxCliPath: runtime.amxCliPath, port });
      this.attachProcess(spawnServer(spec));
      token = await this.waitForDiscoveryToken(port);
    } else {
      embedded = await probeEmbeddedSupport({ kind: "python", pythonPath: runtime.pythonPath });
      if (!embedded) {
        log("installed AMX predates embedded mode — panels disabled until upgrade");
      }
      token = generateToken();
      const spec = buildServerSpawnSpec({
        pythonPath: runtime.pythonPath,
        port,
        token,
        supportsEmbedded: embedded,
      });
      this.attachProcess(spawnServer(spec));
    }

    await this.waitForHealth(port, token);
    log(`owned Studio server running on :${port} (embedded=${embedded})`);
    const running: RunningServer = {
      status: "running",
      mode: "owned",
      port,
      token,
      baseUrl: `http://127.0.0.1:${port}`,
      embedded,
    };
    this.setState(running);
    return running;
  }

  private attachProcess(proc: ChildProcess): void {
    this.proc = proc;
    // Persist the pid so the NEXT extension host can reap this server
    // if deactivate's fire-and-forget kill never lands.
    if (proc.pid !== undefined) void this.memento?.update(OWNED_PID_KEY, proc.pid);
    const channel = getServerChannel();
    proc.stdout?.on("data", (chunk: Buffer) => channel.append(chunk.toString()));
    proc.stderr?.on("data", (chunk: Buffer) => channel.append(chunk.toString()));
    proc.on("exit", (code, signal) => {
      if (this.proc !== proc) return; // superseded or stopped on purpose
      this.proc = undefined;
      void this.memento?.update(OWNED_PID_KEY, undefined);
      log(`Studio server exited (code=${code} signal=${signal})`);
      if (this.disposing) return;
      this.setState({ status: "stopped" });
      void this.maybeRestart();
    });
  }

  private async maybeRestart(): Promise<void> {
    const now = Date.now();
    this.restartTimestamps = this.restartTimestamps.filter(
      (ts) => now - ts < RESTART_WINDOW_MS,
    );
    if (this.restartTimestamps.length >= MAX_RESTARTS) {
      this.setState({
        status: "error",
        message: "Studio server keeps crashing — see the AMX Studio output channel",
      });
      const choice = await vscode.window.showErrorMessage(
        "The AMX Studio server keeps crashing.",
        "Show Logs",
        "Retry",
      );
      if (choice === "Show Logs") getServerChannel().show(true);
      if (choice === "Retry") {
        this.restartTimestamps = [];
        void this.ensure().catch(() => {});
      }
      return;
    }
    const attempt = this.restartTimestamps.length;
    this.restartTimestamps.push(now);
    const delay = backoffDelayMs(attempt, 1000, 8000);
    log(`restarting Studio server in ${delay}ms (attempt ${attempt + 1}/${MAX_RESTARTS})`);
    await sleep(delay);
    if (!this.disposing && this.current.status !== "running") {
      void this.ensure().catch(() => {});
    }
  }

  /**
   * Liveness + capability probe. `embedded` is derived from the
   * response headers: an embedded-mode server omits X-Frame-Options
   * and serves `frame-ancestors *`; the strict browser profile sends
   * X-Frame-Options: DENY on every response.
   */
  private async healthCheck(
    port: number,
    token: string,
  ): Promise<{ ok: boolean; embedded: boolean }> {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/api/health`, {
        headers: { Authorization: `Bearer ${token}` },
        signal: AbortSignal.timeout(HEALTH_TIMEOUT_MS),
      });
      const csp = response.headers.get("content-security-policy") ?? "";
      const embedded =
        !response.headers.has("x-frame-options") && csp.includes("frame-ancestors *");
      return { ok: response.ok, embedded };
    } catch {
      return { ok: false, embedded: false };
    }
  }

  private async waitForHealth(port: number, token: string): Promise<void> {
    const deadline = Date.now() + STARTUP_TIMEOUT_MS;
    while (Date.now() < deadline) {
      if (this.proc === undefined && this.current.status !== "starting") {
        throw new Error("Studio server process exited during startup");
      }
      if ((await this.healthCheck(port, token)).ok) return;
      await sleep(300);
    }
    throw new Error(`Studio server did not become healthy on :${port} within ${STARTUP_TIMEOUT_MS / 1000}s`);
  }

  private async waitForDiscoveryToken(port: number): Promise<string> {
    const deadline = Date.now() + STARTUP_TIMEOUT_MS;
    while (Date.now() < deadline) {
      const record = await readDiscovery();
      if (record && record.port === port) return record.token;
      await sleep(300);
    }
    throw new Error("Studio server started but never wrote its discovery file");
  }
}
