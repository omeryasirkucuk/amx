// One Studio webview panel: a thin shell hosting the SPA in an
// iframe. Resolves the server through ServerManager.ensure(), builds
// the iframe src with bearer token + embed flag, renders an error
// shell with a Retry affordance when the server cannot start, and
// rebuilds the iframe whenever the server comes back on a different
// port or token.
import * as vscode from "vscode";

import type { RunningServer } from "../server/serverManager";
import type { ExtensionServices } from "../services";
import { log } from "../util/log";
import type { AreaDefinition, PanelAreaArgs } from "./areas";
import { buildErrorHtml, buildFrameHtml } from "./html";

const WEBVIEW_OPTIONS: vscode.WebviewOptions & vscode.WebviewPanelOptions = {
  enableScripts: true,
  retainContextWhenHidden: true,
};

export class StudioPanel implements vscode.Disposable {
  private args: PanelAreaArgs;
  /** Server identity of the currently rendered iframe, if any. */
  private rendered: { port: number; token: string } | undefined;
  /** Guards against a slow rebuild overwriting a newer one. */
  private generation = 0;
  private readonly subscriptions: vscode.Disposable[] = [];

  /** Create a fresh panel in the active column. */
  static create(
    services: ExtensionServices,
    definition: AreaDefinition,
    args: PanelAreaArgs,
  ): StudioPanel {
    const panel = vscode.window.createWebviewPanel(
      definition.viewType,
      definition.buildTitle(args),
      vscode.ViewColumn.Active,
      WEBVIEW_OPTIONS,
    );
    return new StudioPanel(services, definition, panel, args);
  }

  /** Wrap a panel handed back by a WebviewPanelSerializer on restore. */
  static adopt(
    services: ExtensionServices,
    definition: AreaDefinition,
    panel: vscode.WebviewPanel,
    args: PanelAreaArgs,
  ): StudioPanel {
    panel.webview.options = WEBVIEW_OPTIONS;
    return new StudioPanel(services, definition, panel, args);
  }

  private constructor(
    private readonly services: ExtensionServices,
    private readonly definition: AreaDefinition,
    private readonly panel: vscode.WebviewPanel,
    args: PanelAreaArgs,
  ) {
    this.args = args;
    this.subscriptions.push(
      panel.webview.onDidReceiveMessage((message: unknown) => this.handleMessage(message)),
      this.services.server.onDidChangeState((state) => {
        if (
          state.status === "running" &&
          this.rendered !== undefined &&
          (state.port !== this.rendered.port || state.token !== this.rendered.token)
        ) {
          log(`Studio server moved to :${state.port} — rebuilding ${this.definition.viewType}`);
          void this.rebuild();
        }
      }),
    );
    panel.onDidDispose(() => {
      while (this.subscriptions.length) this.subscriptions.pop()?.dispose();
    });
    void this.rebuild();
  }

  onDidDispose(listener: () => void): vscode.Disposable {
    return this.panel.onDidDispose(listener);
  }

  reveal(): void {
    this.panel.reveal();
  }

  /** Re-point the existing panel at a (possibly) different deep link. */
  navigate(args: PanelAreaArgs): void {
    this.args = args;
    void this.rebuild();
  }

  dispose(): void {
    this.panel.dispose();
  }

  private async rebuild(): Promise<void> {
    const generation = ++this.generation;
    this.panel.title = this.definition.buildTitle(this.args);

    let server: RunningServer;
    try {
      server = await vscode.window.withProgress(
        { location: vscode.ProgressLocation.Window, title: "AMX: starting Studio server" },
        () => this.services.server.ensure(),
      );
    } catch (error) {
      if (generation !== this.generation) return;
      const message = error instanceof Error ? error.message : String(error);
      log(`Studio panel ${this.definition.viewType} could not reach the server: ${message}`);
      this.rendered = undefined;
      this.panel.webview.html = buildErrorHtml({
        title: this.definition.buildTitle(this.args),
        message,
        state: this.persistedState(),
      });
      return;
    }
    if (generation !== this.generation) return;

    // asExternalUri keeps the panel working under Remote / Codespaces
    // by mapping the loopback base URL through the port forwarder.
    const externalBase = await vscode.env.asExternalUri(vscode.Uri.parse(server.baseUrl));
    if (generation !== this.generation) return;
    const base = externalBase.toString(true).replace(/\/+$/, "");
    const route = this.definition.buildRoute(this.args);
    const src = `${base}${route}?t=${encodeURIComponent(server.token)}&embed=1`;

    this.rendered = { port: server.port, token: server.token };
    this.panel.webview.html = buildFrameHtml({
      iframeSrc: src,
      title: this.definition.buildTitle(this.args),
      state: this.persistedState(),
    });
  }

  private persistedState(): unknown {
    return { area: this.definition.area, args: this.args };
  }

  private handleMessage(message: unknown): void {
    if (typeof message !== "object" || message === null) return;
    const { type, url } = message as { type?: unknown; url?: unknown };
    if (type === "amx:openExternal" && typeof url === "string") {
      void vscode.env.openExternal(vscode.Uri.parse(url));
      return;
    }
    if (type === "amx:retry") {
      void this.rebuild();
    }
  }
}
