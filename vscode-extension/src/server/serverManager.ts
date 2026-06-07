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
import { readDiscovery } from "./discoveryFile";
import { pickPort } from "./ports";
import { buildCliSpawnSpec, buildServerSpawnSpec, killServer, spawnServer } from "./spawn";
import { generateToken } from "./token";

export type RunningServer = {
  status: "running";
  mode: "owned" | "attached";
  port: number;
  token: string;
  baseUrl: string;
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

export class ServerManager implements vscode.Disposable {
  private current: ServerState = { status: "stopped" };
  private proc: ChildProcess | undefined;
  private disposing = false;
  private restartTimestamps: number[] = [];
  private readonly didChange = new vscode.EventEmitter<ServerState>();
  readonly onDidChangeState = this.didChange.event;
  private readonly ensureOnce = singleFlight(() => this.startOrAttach());

  constructor(private readonly runtime: RuntimeManager) {}

  get state(): ServerState {
    return this.current;
  }

  /** Start-or-attach, idempotent and single-flight. */
  async ensure(): Promise<RunningServer> {
    if (this.current.status === "running") return this.current;
    return this.ensureOnce();
  }

  async stop(): Promise<void> {
    if (this.proc) {
      const proc = this.proc;
      this.proc = undefined;
      await killServer(proc);
    }
    this.setState({ status: "stopped" });
  }

  async restart(): Promise<void> {
    await this.stop();
    await this.ensure();
  }

  dispose(): void {
    this.disposing = true;
    if (this.proc) {
      // Fire and forget — deactivate cannot await long shutdowns.
      void killServer(this.proc);
      this.proc = undefined;
    }
    this.didChange.dispose();
  }

  private setState(state: ServerState): void {
    this.current = state;
    this.didChange.fire(state);
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
    const healthy = await this.healthCheck(record.port, record.token);
    if (!healthy) {
      log(`discovery file points at :${record.port} but the health check failed — ignoring`);
      return undefined;
    }
    log(`adopted running Studio server on :${record.port} (owner=${record.owner})`);
    const running: RunningServer = {
      status: "running",
      mode: "attached",
      port: record.port,
      token: record.token,
      baseUrl: `http://127.0.0.1:${record.port}`,
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
    if (runtime.kind === "amx-cli") {
      // The CLI launcher generates its own token; recover it from the
      // discovery file once the server has written it.
      const spec = buildCliSpawnSpec({ amxCliPath: runtime.amxCliPath, port });
      this.attachProcess(spawnServer(spec));
      token = await this.waitForDiscoveryToken(port);
    } else {
      token = generateToken();
      const spec = buildServerSpawnSpec({ pythonPath: runtime.pythonPath, port, token });
      this.attachProcess(spawnServer(spec));
    }

    await this.waitForHealth(port, token);
    log(`owned Studio server running on :${port}`);
    const running: RunningServer = {
      status: "running",
      mode: "owned",
      port,
      token,
      baseUrl: `http://127.0.0.1:${port}`,
    };
    this.setState(running);
    return running;
  }

  private attachProcess(proc: ChildProcess): void {
    this.proc = proc;
    const channel = getServerChannel();
    proc.stdout?.on("data", (chunk: Buffer) => channel.append(chunk.toString()));
    proc.stderr?.on("data", (chunk: Buffer) => channel.append(chunk.toString()));
    proc.on("exit", (code, signal) => {
      if (this.proc !== proc) return; // superseded or stopped on purpose
      this.proc = undefined;
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

  private async healthCheck(port: number, token: string): Promise<boolean> {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/api/health`, {
        headers: { Authorization: `Bearer ${token}` },
        signal: AbortSignal.timeout(HEALTH_TIMEOUT_MS),
      });
      return response.ok;
    } catch {
      return false;
    }
  }

  private async waitForHealth(port: number, token: string): Promise<void> {
    const deadline = Date.now() + STARTUP_TIMEOUT_MS;
    while (Date.now() < deadline) {
      if (this.proc === undefined && this.current.status !== "starting") {
        throw new Error("Studio server process exited during startup");
      }
      if (await this.healthCheck(port, token)) return;
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
