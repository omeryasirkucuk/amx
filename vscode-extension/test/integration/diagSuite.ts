// Diagnostic suite: determines empirically how VS Code webviews treat
// a cross-origin http://127.0.0.1 iframe. The fake page posts a
// message to its parent on load; receiving it in the extension host
// proves the iframe document actually executed (a blocked iframe
// still fires `load` but never runs script). Variants exercise
// different response-header sets so the working combination can be
// read straight from the test output.
import { createServer, type Server } from "node:http";
import * as vscode from "vscode";

interface HeaderVariant {
  name: string;
  headers: Record<string, string>;
}

const VARIANTS: HeaderVariant[] = [
  {
    name: "frame-ancestors only (current embedded mode)",
    headers: {
      "Content-Security-Policy": "frame-ancestors *",
    },
  },
  {
    name: "no headers at all",
    headers: {},
  },
  {
    name: "frame-ancestors * vscode-webview: (scheme source added)",
    headers: {
      "Content-Security-Policy": "frame-ancestors * vscode-webview: vscode-file:",
    },
  },
  {
    name: "full embedded CSP with vscode-webview: scheme source",
    headers: {
      "Content-Security-Policy":
        "default-src 'self'; script-src 'self' 'unsafe-inline'; " +
        "frame-ancestors * vscode-webview: vscode-file:",
    },
  },
];

function startEchoServer(): Promise<{ server: Server; port: number }> {
  const server = createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    const variantIndex = Number(url.searchParams.get("v") ?? "0");
    const variant = VARIANTS[variantIndex] ?? VARIANTS[0]!;
    response.writeHead(200, { "Content-Type": "text/html", ...variant.headers });
    response.end(
      `<!DOCTYPE html><html><body><script>parent.postMessage({type:"amx:itest",variant:${variantIndex}},"*")</script>ok</body></html>`,
    );
  });
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen({ port: 0, host: "127.0.0.1" }, () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("echo server did not bind"));
        return;
      }
      resolve({ server, port: address.port });
    });
  });
}

function probeUrl(label: string, frameUrl: string): Promise<string> {
  return new Promise((resolve) => {
    const panel = vscode.window.createWebviewPanel("amx.diag", label, vscode.ViewColumn.Active, {
      enableScripts: true,
    });
    const events: string[] = [];
    const timer = setTimeout(() => {
      panel.dispose();
      resolve(`NO MESSAGE; shell events: [${events.join(", ")}]`);
    }, 6000);
    panel.webview.onDidReceiveMessage(
      (message: { type?: string; payload?: unknown; event?: string }) => {
        if (message.type === "diag") {
          clearTimeout(timer);
          panel.dispose();
          resolve(`OK ${JSON.stringify(message.payload)} events: [${events.join(", ")}]`);
        } else if (message.type === "shellEvent" && typeof message.event === "string") {
          events.push(message.event);
        }
      },
    );
    const frameOrigin = new URL(frameUrl).origin;
    panel.webview.html = `<!DOCTYPE html>
<html><head><meta http-equiv="Content-Security-Policy"
  content="default-src 'none'; frame-src ${frameOrigin}; script-src 'unsafe-inline'">
</head><body>
<iframe id="f" src="${frameUrl}"></iframe>
<script>
  const vscode = acquireVsCodeApi();
  vscode.postMessage({ type: "shellEvent", event: "coi=" + self.crossOriginIsolated });
  const f = document.getElementById("f");
  f.addEventListener("load", () => vscode.postMessage({ type: "shellEvent", event: "iframe-load" }));
  f.addEventListener("error", () => vscode.postMessage({ type: "shellEvent", event: "iframe-error" }));
  window.addEventListener("message", (event) => {
    if (event.data && event.data.type === "amx:itest") {
      vscode.postMessage({ type: "diag", payload: { variant: event.data.variant } });
    }
  });
</script>
</body></html>`;
  });
}

export async function run(): Promise<void> {
  const { server, port } = await startEchoServer();
  try {
    for (let index = 0; index < VARIANTS.length; index += 1) {
      const result = await probeUrl(`diag ${index}`, `http://127.0.0.1:${port}/?v=${index}`);
      console.log(`[diag] 127.0.0.1 variant ${index} (${VARIANTS[index]!.name}): ${result}`);
    }
    console.log(
      `[diag] localhost variant 0: ${await probeUrl("diag lh", `http://localhost:${port}/?v=0`)}`,
    );
  } finally {
    server.close();
  }
}
