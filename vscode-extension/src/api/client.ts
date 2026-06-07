// Typed REST client for the AMX Studio API. Resolves the server
// lazily through ServerManager on every call so token rotation and
// restarts are transparent. No vscode UI imports — testable with a
// stubbed ServerManager.
import type { ServerManager } from "../server/serverManager";
import { errorFromResponse } from "./errors";
import { streamSse, type SseOptions } from "./sse";
import type {
  CatalogScope,
  ContextInfo,
  DbProfileSummary,
  GenerateResult,
  Health,
  InventoryTable,
  LlmProfileSummary,
  NamedProfileSummary,
  RunsPage,
  RunSummary,
  ScheduleSummary,
  SseEvent,
  TableExplain,
  VersionInfo,
} from "./types";

type Query = Record<string, string | number | undefined>;

export interface MutationEvent {
  method: string;
  path: string;
}

export class AmxClient {
  private readonly mutationListeners = new Set<(event: MutationEvent) => void>();

  constructor(private readonly server: ServerManager) {}

  onMutation(listener: (event: MutationEvent) => void): { dispose(): void } {
    this.mutationListeners.add(listener);
    return { dispose: () => this.mutationListeners.delete(listener) };
  }

  async request<T>(
    method: string,
    path: string,
    options: { body?: unknown; query?: Query; signal?: AbortSignal } = {},
  ): Promise<T> {
    const { baseUrl, token } = await this.server.ensure();
    const url = new URL(path, baseUrl);
    for (const [key, value] of Object.entries(options.query ?? {})) {
      if (value !== undefined) url.searchParams.set(key, String(value));
    }
    const init: RequestInit = {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        ...(options.body !== undefined ? { "Content-Type": "application/json" } : {}),
      },
    };
    if (options.body !== undefined) init.body = JSON.stringify(options.body);
    if (options.signal) init.signal = options.signal;
    const response = await fetch(url, init);
    if (!response.ok) throw await errorFromResponse(response);
    if (method !== "GET") {
      const event: MutationEvent = { method, path };
      for (const listener of this.mutationListeners) listener(event);
    }
    if (response.status === 204) return undefined as T;
    return (await response.json()) as T;
  }

  get<T>(path: string, query?: Query, signal?: AbortSignal): Promise<T> {
    const options: { query?: Query; signal?: AbortSignal } = {};
    if (query) options.query = query;
    if (signal) options.signal = signal;
    return this.request<T>("GET", path, options);
  }

  post<T>(path: string, body?: unknown, query?: Query): Promise<T> {
    const options: { body?: unknown; query?: Query } = {};
    if (body !== undefined) options.body = body;
    if (query) options.query = query;
    return this.request<T>("POST", path, options);
  }

  put<T>(path: string, body?: unknown, query?: Query): Promise<T> {
    const options: { body?: unknown; query?: Query } = {};
    if (body !== undefined) options.body = body;
    if (query) options.query = query;
    return this.request<T>("PUT", path, options);
  }

  /** Stream an SSE endpoint with reconnect + Last-Event-ID resume. */
  async *sse(path: string, options: SseOptions = {}): AsyncGenerator<SseEvent> {
    const { baseUrl, token } = await this.server.ensure();
    yield* streamSse(new URL(path, baseUrl).toString(), token, options);
  }

  // --- typed convenience groups ---

  readonly system = {
    health: (): Promise<Health> => this.get("/api/health"),
    version: (): Promise<VersionInfo> => this.get("/api/version"),
    context: (): Promise<ContextInfo> => this.get("/api/context"),
  };

  readonly profiles = {
    listDb: async (): Promise<DbProfileSummary[]> =>
      (await this.get<{ profiles: DbProfileSummary[] }>("/api/profiles/db")).profiles,
    listLlm: async (): Promise<LlmProfileSummary[]> =>
      (await this.get<{ profiles: LlmProfileSummary[] }>("/api/profiles/llm")).profiles,
    listDocs: async (): Promise<NamedProfileSummary[]> =>
      (await this.get<{ profiles: NamedProfileSummary[] }>("/api/profiles/docs")).profiles,
    listCode: async (): Promise<NamedProfileSummary[]> =>
      (await this.get<{ profiles: NamedProfileSummary[] }>("/api/profiles/code")).profiles,
    activateDb: (name: string): Promise<unknown> =>
      this.post(`/api/profiles/db/${encodeURIComponent(name)}/activate`),
    activateLlm: (name: string): Promise<unknown> =>
      this.post(`/api/profiles/llm/${encodeURIComponent(name)}/activate`),
    testDb: (name: string): Promise<unknown> =>
      this.post(`/api/profiles/db/${encodeURIComponent(name)}/test`),
  };

  readonly catalog = {
    databases: async (scope: CatalogScope = {}): Promise<string[]> => {
      const payload = await this.get<{ databases: unknown[] }>("/api/catalog/databases", {
        profile: scope.profile,
      });
      return payload.databases.map((row) =>
        typeof row === "string"
          ? row
          : String((row as Record<string, unknown>)["database_name"] ?? ""),
      );
    },
    schemas: async (scope: CatalogScope = {}): Promise<string[]> => {
      const payload = await this.get<{ schemas: unknown[] }>("/api/catalog/schemas", {
        profile: scope.profile,
        db: scope.database,
      });
      return payload.schemas.map((row) =>
        typeof row === "string"
          ? row
          : String((row as Record<string, unknown>)["schema_name"] ?? ""),
      );
    },
    inventory: async (scope: CatalogScope = {}, limit = 5000): Promise<InventoryTable[]> =>
      (
        await this.get<{ tables: InventoryTable[] }>("/api/catalog/inventory", {
          profile: scope.profile,
          db: scope.database,
          schema: scope.schema,
          limit,
        })
      ).tables,
    explain: (path: string, profile?: string): Promise<TableExplain> =>
      this.get("/api/catalog/explain", { path, profile }),
  };

  readonly history = {
    runs: async (limit = 50, offset = 0): Promise<RunsPage> =>
      this.get("/api/history/runs", { limit, offset, command: "all" }),
    run: (runId: string): Promise<RunSummary> =>
      this.get(`/api/history/runs/${encodeURIComponent(runId)}`),
  };

  readonly schedules = {
    list: async (): Promise<ScheduleSummary[]> => {
      const payload = await this.get<{ schedules?: ScheduleSummary[] } | ScheduleSummary[]>(
        "/api/schedules",
      );
      return Array.isArray(payload) ? payload : (payload.schedules ?? []);
    },
    pause: (id: string | number): Promise<unknown> => this.post(`/api/schedules/${id}/pause`),
    resume: (id: string | number): Promise<unknown> => this.post(`/api/schedules/${id}/resume`),
    runNow: (id: string | number): Promise<unknown> => this.post(`/api/schedules/${id}/run-now`),
  };

  readonly comments = {
    setLocal: (body: Record<string, unknown>): Promise<unknown> =>
      this.post("/api/comments/local", body),
    setColumn: (
      schema: string,
      table: string,
      column: string,
      comment: string,
      profile?: string,
    ): Promise<unknown> =>
      this.put(
        `/api/comments/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/columns/${encodeURIComponent(column)}`,
        { comment },
        { profile },
      ),
    setTable: (schema: string, table: string, comment: string, profile?: string): Promise<unknown> =>
      this.put(
        `/api/comments/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}`,
        { comment },
        { profile },
      ),
  };

  readonly generate = {
    // `profile` is a required query param on the generate endpoints.
    table: (schema: string, table: string, profile: string): Promise<GenerateResult> =>
      this.post(
        `/api/generate/table/${encodeURIComponent(schema)}/${encodeURIComponent(table)}`,
        undefined,
        { profile },
      ),
    column: (
      schema: string,
      table: string,
      column: string,
      profile: string,
    ): Promise<GenerateResult> =>
      this.post(
        `/api/generate/column/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/${encodeURIComponent(column)}`,
        undefined,
        { profile },
      ),
  };
}
