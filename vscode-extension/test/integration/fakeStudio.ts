// Minimal in-process stand-in for the AMX Studio server: just enough
// REST surface for the extension to adopt it (health), populate the
// trees (profiles/catalog/history/schedules), and build panel URLs.
// Non-API paths serve the REAL built SPA from amx/web/static with the
// embedded-mode headers, so panel tests exercise the actual iframe
// boot path (CSP frame-ancestors scheme sources included — see
// amx/web/security_headers.py for why vscode-webview:/vscode-file:
// are load-bearing).
import { createReadStream, existsSync } from "node:fs";
import { createServer, type IncomingMessage, type Server } from "node:http";
import { extname, join, normalize, resolve } from "node:path";

export const FAKE_TOKEN = "integration-fake-token";

/** One recorded request entry, captured for test assertions. */
interface ReceivedEntry {
  method: string;
  path: string;
  query: Record<string, string>;
  body: unknown;
}

/** Read the full request body as a UTF-8 string. */
async function readBody(request: IncomingMessage): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    request.on("error", reject);
  });
}

const STATIC_ROOT = resolve(__dirname, "..", "..", "..", "..", "amx", "web", "static");

const EMBEDDED_HEADERS: Record<string, string> = {
  "Content-Security-Policy":
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
    "img-src 'self' data:; connect-src 'self'; font-src 'self' data:; " +
    "base-uri 'self'; form-action 'self'; " +
    "frame-ancestors * vscode-webview: vscode-file:",
};

const MIME: Record<string, string> = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".css": "text/css",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".json": "application/json",
};

const BACKENDS_RESPONSE = {
  backends: [
    {
      id: "postgresql",
      label: "PostgreSQL",
      fields: ["host", "port", "sslmode"],
      field_specs: [
        {
          name: "host",
          kind: "text",
          label: "Host",
          help: "Hostname or IP",
          secret: false,
          required: true,
          group: "basic",
          options: [],
        },
        {
          name: "port",
          kind: "int",
          label: "Port",
          help: "Default 5432",
          secret: false,
          required: true,
          group: "basic",
          options: [],
        },
        {
          name: "sslmode",
          kind: "select",
          label: "SSL mode",
          help: "libpq sslmode",
          secret: false,
          required: false,
          group: "advanced",
          options: ["", "disable", "require"],
        },
      ],
      default_port: 5432,
      supports_catalog: true,
    },
  ],
};

const ROUTES: Record<string, unknown> = {
  "/api/health": { ok: true, version: "0.99.0" },
  "/api/version": { amx: "0.99.0", schema: 1, web: "v1" },
  "/api/context": {
    active_llm_profile: "default",
    active_doc_profile: null,
    active_code_profile: null,
    current_schema: null,
    current_table: null,
    db_backend: "postgresql",
    llm_provider: "openai",
    llm_model: "gpt-4o",
    llm_supports_batch: true,
  },
  "/api/profiles/db": {
    profiles: [
      {
        name: "warehouse",
        backend: "postgresql",
        host: "localhost",
        database: "dwh",
        catalog: "",
        project: "",
        is_active: true,
      },
    ],
    count: 1,
  },
  "/api/profiles/llm": {
    profiles: [{ name: "default", provider: "openai", model: "gpt-4o", is_active: true }],
    count: 1,
  },
  "/api/profiles/docs": { profiles: [], count: 0 },
  "/api/profiles/code": { profiles: [], count: 0 },
  "/api/catalog/inventory": {
    schema: null,
    database: null,
    tables: [
      {
        db_profile: "warehouse",
        database_name: null,
        schema_name: "sales",
        table_name: "orders",
        asset_kind: "table",
        row_count: 120,
        column_count: 2,
        effective_description: "All customer orders",
      },
    ],
    count: 1,
    limit: 5000,
  },
  "/api/catalog/explain": {
    table: { schema_name: "sales", table_name: "orders" },
    columns: [
      { column_name: "id", data_type: "INT", effective_description: "Primary key" },
      { column_name: "total", data_type: "NUMERIC", effective_description: null },
    ],
    relationships: [],
  },
  "/api/history/runs": { runs: [], total: 0, has_more: false },
  "/api/schedules": { schedules: [] },
  "/api/profiles/db/backends": BACKENDS_RESPONSE,
  "/api/catalog/freshness": { profiles: [{ profile: "warehouse", state: "fresh" }] },
  "/api/catalog/databases": {
    databases: [
      { database_name: "dwh", entity_count: 10 },
      { database_name: "analytics", entity_count: 5 },
    ],
    count: 2,
  },
  "/api/catalog/search/tables": { q: "", rows: [], count: 0 },
  "/api/catalog/search/columns": { q: "", rows: [], count: 0 },
};

/** Start the fake server on an ephemeral port; resolves the port. */
export function startFakeStudio(): Promise<{ server: Server; port: number }> {
  const received: ReceivedEntry[] = [];

  const server = createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    const auth = request.headers.authorization ?? "";
    const tokenParam = url.searchParams.get("t") ?? "";
    const authorized = auth === `Bearer ${FAKE_TOKEN}` || tokenParam === FAKE_TOKEN;
    const method = request.method?.toUpperCase() ?? "GET";

    // Embedded-mode header shape: frameable CSP, no X-Frame-Options.
    response.setHeader(
      "Content-Security-Policy",
      "default-src 'self'; frame-ancestors *",
    );

    // Test-only introspection endpoint — no auth required.
    if (url.pathname === "/__test/received" && method === "GET") {
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify(received));
      return;
    }

    if (url.pathname.startsWith("/api/") && !authorized) {
      response.writeHead(401, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ detail: "Missing or invalid token." }));
      return;
    }

    // --- mutation routes ---

    // PUT /api/profiles/db/:name
    if (method === "PUT" && /^\/api\/profiles\/db\/[^/]+$/.test(url.pathname)) {
      void readBody(request).then((raw) => {
        let body: unknown;
        try { body = JSON.parse(raw); } catch { body = raw; }
        const query: Record<string, string> = {};
        for (const [k, v] of url.searchParams.entries()) query[k] = v;
        received.push({ method, path: url.pathname, query, body });
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ ok: true }));
      });
      return;
    }

    // POST /api/catalog/sync
    if (method === "POST" && url.pathname === "/api/catalog/sync") {
      void readBody(request).then((raw) => {
        let body: unknown;
        try { body = JSON.parse(raw); } catch { body = raw; }
        const query: Record<string, string> = {};
        for (const [k, v] of url.searchParams.entries()) query[k] = v;
        received.push({ method, path: url.pathname, query, body });
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({}));
      });
      return;
    }

    // POST /api/runs
    if (method === "POST" && url.pathname === "/api/runs") {
      void readBody(request).then((raw) => {
        let body: unknown;
        try { body = JSON.parse(raw); } catch { body = raw; }
        const query: Record<string, string> = {};
        for (const [k, v] of url.searchParams.entries()) query[k] = v;
        received.push({ method, path: url.pathname, query, body });
        response.writeHead(200, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ job_id: "itest-job" }));
      });
      return;
    }

    // GET /api/runs/itest-job/events — SSE stream
    if (method === "GET" && url.pathname === "/api/runs/itest-job/events") {
      response.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });
      response.write('data: {"type":"job.done"}\n\n');
      response.end();
      return;
    }

    // POST /api/schedules
    if (method === "POST" && url.pathname === "/api/schedules") {
      void readBody(request).then((raw) => {
        let body: unknown;
        try { body = JSON.parse(raw); } catch { body = raw; }
        const query: Record<string, string> = {};
        for (const [k, v] of url.searchParams.entries()) query[k] = v;
        received.push({ method, path: url.pathname, query, body });
        response.writeHead(201, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ id: 1 }));
      });
      return;
    }

    // DELETE /api/schedules/1
    if (method === "DELETE" && url.pathname === "/api/schedules/1") {
      const query: Record<string, string> = {};
      for (const [k, v] of url.searchParams.entries()) query[k] = v;
      received.push({ method, path: url.pathname, query, body: null });
      response.writeHead(204);
      response.end();
      return;
    }

    // --- read-only routes ---

    const payload = ROUTES[url.pathname];
    if (payload !== undefined) {
      response.writeHead(200, { "Content-Type": "application/json", ...EMBEDDED_HEADERS });
      response.end(JSON.stringify(payload));
      return;
    }
    if (url.pathname.startsWith("/api/")) {
      response.writeHead(404, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ detail: `No fake route for ${url.pathname}` }));
      return;
    }
    // SPA fallback: serve the real built bundle; unknown paths get
    // index.html the way the real server's catch-all does.
    const safePath = normalize(url.pathname).replace(/^([.][.][/\\])+/, "");
    let filePath = join(STATIC_ROOT, safePath);
    if (!filePath.startsWith(STATIC_ROOT) || !existsSync(filePath) || extname(filePath) === "") {
      filePath = join(STATIC_ROOT, "index.html");
    }
    response.writeHead(200, {
      "Content-Type": MIME[extname(filePath)] ?? "application/octet-stream",
      ...EMBEDDED_HEADERS,
    });
    createReadStream(filePath).pipe(response);
  });
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen({ port: 0, host: "127.0.0.1" }, () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("fake studio did not bind"));
        return;
      }
      resolve({ server, port: address.port });
    });
  });
}
