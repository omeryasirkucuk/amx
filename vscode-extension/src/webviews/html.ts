// Shell HTML for the Studio webview panels. The webview hosts a
// full-bleed cross-origin iframe pointing at the Studio SPA; the
// nonce'd inline script relays the SPA's `amx:openExternal` bridge
// messages to the extension host, persists panel state for tab
// restore, and drives the loading / error overlays.
import { randomBytes } from "node:crypto";

export function createNonce(): string {
  return randomBytes(16).toString("base64");
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

/** JSON for embedding inside a <script> block. `</` is escaped so a
 *  crafted string value cannot close the script element. */
function scriptJson(value: unknown): string {
  return JSON.stringify(value).replaceAll("</", "<\\/");
}

const SHARED_STYLE = `
  html, body { height: 100%; width: 100%; margin: 0; padding: 0; overflow: hidden; }
  body { font-family: var(--vscode-font-family, sans-serif); color: var(--vscode-foreground, #ccc); background: var(--vscode-editor-background, #1e1e1e); }
  .overlay { position: absolute; inset: 0; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 12px; background: var(--vscode-editor-background, #1e1e1e); }
  .spinner { width: 28px; height: 28px; border: 3px solid var(--vscode-progressBar-background, #0e70c0); border-top-color: transparent; border-radius: 50%; animation: amx-spin 0.9s linear infinite; }
  @keyframes amx-spin { to { transform: rotate(360deg); } }
`;

export interface FrameHtmlOptions {
  readonly iframeSrc: string;
  readonly title: string;
  /** Persisted via acquireVsCodeApi().setState() for tab restore. */
  readonly state: unknown;
}

export function buildFrameHtml(options: FrameHtmlOptions): string {
  const nonce = createNonce();
  const frameOrigin = new URL(options.iframeSrc).origin;
  const csp = [
    "default-src 'none'",
    `frame-src ${frameOrigin}`,
    `script-src 'nonce-${nonce}'`,
    "style-src 'unsafe-inline'",
  ].join("; ");
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy" content="${csp}">
  <title>${escapeHtml(options.title)}</title>
  <style>
    ${SHARED_STYLE}
    iframe { display: block; height: 100%; width: 100%; border: none; }
    .hidden { display: none; }
    .message { max-width: 480px; text-align: center; white-space: pre-wrap; word-break: break-word; opacity: 0.9; }
    button { padding: 6px 16px; border: none; border-radius: 2px; cursor: pointer; color: var(--vscode-button-foreground, #fff); background: var(--vscode-button-background, #0e639c); font-family: inherit; font-size: 13px; }
    button:hover { background: var(--vscode-button-hoverBackground, #1177bb); }
    .actions { display: flex; gap: 8px; }
  </style>
</head>
<body>
  <iframe id="studio" src="${escapeHtml(options.iframeSrc)}" allow="clipboard-read; clipboard-write"></iframe>
  <div id="loading" class="overlay">
    <div class="spinner" role="presentation"></div>
    <p>Loading AMX Studio…</p>
  </div>
  <div id="stalled" class="overlay hidden">
    <h3>AMX Studio did not load</h3>
    <p class="message">The Studio page never reported in. The server may still be starting, or the installed AMX version may not support embedded panels.</p>
    <div class="actions">
      <button id="retry" type="button">Retry</button>
      <button id="browser" type="button">Open in Browser</button>
    </div>
  </div>
  <script nonce="${nonce}">
    (function () {
      const vscode = acquireVsCodeApi();
      vscode.setState(${scriptJson(options.state)});
      let ready = false;
      // Focus bridge. The Studio iframe is cross-origin, so VS Code
      // never hands it keyboard focus on its own — clicks land but
      // typed keys go to this (empty) wrapper document. Explicitly
      // pushing focus into the frame on load, on window focus, and
      // once the SPA reports in makes inputs (Ask composer, settings
      // forms) actually typeable. contentWindow.focus() is one of the
      // few cross-origin-legal calls; VS Code's own Simple Browser
      // uses the same trick.
      const frame = document.getElementById("studio");
      const focusFrame = () => {
        try {
          frame.contentWindow.focus();
        } catch (_error) {
          /* frame not ready yet — the next trigger retries */
        }
      };
      frame.addEventListener("load", focusFrame);
      window.addEventListener("focus", focusFrame);
      // A blocked iframe still fires \`load\` but never executes the
      // SPA, so the overlay waits for the SPA's explicit boot signal
      // (amx:embedReady) rather than the load event.
      const stallTimer = setTimeout(() => {
        if (ready) return;
        document.getElementById("loading").classList.add("hidden");
        document.getElementById("stalled").classList.remove("hidden");
        // Tell the extension host too: it re-checks the server and
        // rebuilds the panel once it is healthy again, so a stall
        // caused by a server restart heals without a manual Retry.
        vscode.postMessage({ type: "amx:stalled" });
      }, 12000);
      window.addEventListener("message", (event) => {
        const data = event.data;
        if (!data || typeof data !== "object") return;
        if (data.type === "amx:embedReady") {
          ready = true;
          clearTimeout(stallTimer);
          const overlay = document.getElementById("loading");
          if (overlay) overlay.remove();
          document.getElementById("stalled").classList.add("hidden");
          // The SPA is interactive now — make sure the keyboard
          // follows (the load-event focus can fire before the SPA
          // mounts its inputs).
          focusFrame();
          // Relay to the extension host: panels track SPA readiness
          // for logging and the integration suite asserts on it.
          vscode.postMessage({ type: "amx:embedReady" });
          return;
        }
        if (data.type === "amx:openExternal" && typeof data.url === "string") {
          vscode.postMessage({ type: "amx:openExternal", url: data.url });
        }
      });
      document.getElementById("retry").addEventListener("click", () => {
        vscode.postMessage({ type: "amx:retry" });
      });
      document.getElementById("browser").addEventListener("click", () => {
        vscode.postMessage({ type: "amx:openBrowser" });
      });
    })();
  </script>
</body>
</html>`;
}

export interface ErrorHtmlOptions {
  readonly title: string;
  readonly message: string;
  /** Persisted via acquireVsCodeApi().setState() for tab restore. */
  readonly state: unknown;
}

export function buildErrorHtml(options: ErrorHtmlOptions): string {
  const nonce = createNonce();
  const csp = ["default-src 'none'", `script-src 'nonce-${nonce}'`, "style-src 'unsafe-inline'"].join(
    "; ",
  );
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy" content="${csp}">
  <title>${escapeHtml(options.title)}</title>
  <style>
    ${SHARED_STYLE}
    .message { max-width: 480px; text-align: center; white-space: pre-wrap; word-break: break-word; opacity: 0.9; }
    button { padding: 6px 16px; border: none; border-radius: 2px; cursor: pointer; color: var(--vscode-button-foreground, #fff); background: var(--vscode-button-background, #0e639c); font-family: inherit; font-size: 13px; }
    button:hover { background: var(--vscode-button-hoverBackground, #1177bb); }
  </style>
</head>
<body>
  <div class="overlay">
    <h3>AMX Studio is unavailable</h3>
    <p class="message">${escapeHtml(options.message)}</p>
    <button id="retry" type="button">Retry</button>
  </div>
  <script nonce="${nonce}">
    (function () {
      const vscode = acquireVsCodeApi();
      vscode.setState(${scriptJson(options.state)});
      document.getElementById("retry").addEventListener("click", () => {
        vscode.postMessage({ type: "amx:retry" });
      });
    })();
  </script>
</body>
</html>`;
}
