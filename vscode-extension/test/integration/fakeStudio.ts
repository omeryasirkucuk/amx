// Minimal in-process stand-in for the AMX Studio server: just enough
// REST surface for the extension to adopt it (health), populate the
// trees (profiles/catalog/history/schedules), and build panel URLs.
import { createServer, type Server } from "node:http";

export const FAKE_TOKEN = "integration-fake-token";

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
};

/** Start the fake server on an ephemeral port; resolves the port. */
export function startFakeStudio(): Promise<{ server: Server; port: number }> {
  const server = createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    const auth = request.headers.authorization ?? "";
    const tokenParam = url.searchParams.get("t") ?? "";
    const authorized = auth === `Bearer ${FAKE_TOKEN}` || tokenParam === FAKE_TOKEN;
    // Embedded-mode header shape: frameable CSP, no X-Frame-Options.
    response.setHeader(
      "Content-Security-Policy",
      "default-src 'self'; frame-ancestors *",
    );
    if (url.pathname.startsWith("/api/") && !authorized) {
      response.writeHead(401, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ detail: "Missing or invalid token." }));
      return;
    }
    const payload = ROUTES[url.pathname];
    if (payload === undefined) {
      response.writeHead(404, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ detail: `No fake route for ${url.pathname}` }));
      return;
    }
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify(payload));
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
