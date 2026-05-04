# `/visualize` — Local AMX Web UI

`amx /visualize` boots a local web UI on `127.0.0.1:<port>`,
generates a one-shot bearer token, and opens your default browser
at the token-protected URL. Press <kbd>Ctrl-C</kbd> in the parent
terminal to stop the server.

## Quick start

```sh
pip install --upgrade amx-cli
amx /visualize
```

The default port is **47821**; pass `--port 8080` to pin a specific
port, or `--no-open` to skip the auto-launch when running over SSH.

The visualizer ships in `amx-cli`. There is **no separate package**
to install and **no Node toolchain** required on your machine — the
SPA bundle is vendored inside the wheel.

## What's in the UI

| Page | What it does | Backed by |
|---|---|---|
| Dashboard `/` | Stat cards (active DB / LLM / total runs / success rate) + last 8 runs. | `/api/history/stats`, `/api/history/runs` |
| Schema `/db/:profile/:schema` | Tables + views + materialized views with comment-coverage status. | `/api/live/schemas/{schema}/assets` |
| Table `/db/:profile/:schema/:table` | Columns table with dtype / nullable / comment columns. | `/api/live/.../columns` + `/snapshot` |
| Runs `/runs` | Every `/run`, `/run-apply`, `/ask` invocation in one filterable list. | `/api/history/runs` |
| Run detail `/runs/:id` | Scope, settings snapshot, metrics. | `/api/history/runs/{id}` |
| Ask `/ask` | Streaming chat with the AMX search agent — reasoning + tool calls + answer. Sessions sidebar. | `/api/ask` (SSE) |
| Pending `/pending` | Edit / drop / clear / apply the on-disk approved-suggestion queue with live progress. | `/api/pending`, `/api/pending/apply` (SSE) |
| Settings `/settings` | DB and LLM profile management — view, activate, connection-test. | `/api/profiles/db`, `/api/profiles/llm` |

### Keyboard shortcuts

- <kbd>⌘K</kbd> / <kbd>Ctrl-K</kbd> — open the command palette and
  jump to any page or quick action.
- <kbd>Esc</kbd> — close the command palette.

### Theme

The top-bar theme button cycles **System → Light → Dark**. The
choice persists in `localStorage`; "System" tracks
`prefers-color-scheme` live.

## Security model

The visualizer binds **only** to `127.0.0.1` — never `0.0.0.0`. On
top of loopback isolation, every API call carries a one-shot bearer
token generated fresh per `/visualize` invocation. The SPA captures
the token from `?t=…` on first load, stashes it in `localStorage`,
and strips it from the URL bar so it doesn't end up in browser
history.

EventSource clients (the SSE streams behind `/ask` and `/apply`)
re-attach the token via `?t=…` because browsers don't allow custom
headers on `EventSource`.

You can rotate the token by stopping the server (<kbd>Ctrl-C</kbd>)
and re-running `amx /visualize`. Future versions will expose an
in-UI "Rotate token" button under Settings.

### What's NOT exposed

- The JSON API does not include `/docs` or `/redoc`. The OpenAPI
  surface stays internal so the visualizer doesn't accidentally
  expose your AMX configuration to anyone who happened to grab the
  token.
- Static asset routes (`/`, `/assets/*`) are unauthenticated by
  design — they only ship the SPA bundle, not data.
- Secret fields (`password`, `access_token`, `api_key`) are masked
  as `********` in every response. PUT bodies treat the placeholder
  as "leave the existing value alone" so editing one field on a
  profile doesn't blank the secret.

## Cancelling long-running jobs

`/run` and `/apply` jobs run in daemon threads inside the parent
CLI process. Each carries a `threading.Event` cancel token plumbed
through the orchestrator; clicking **Cancel** on the progress card
sets the token and the loop bails between rows.

`/apply` cancellation **commits whatever was already written** —
matching the CLI's <kbd>Ctrl-C</kbd> behaviour. The transaction
boundary is per-row, so partial work is never rolled back to spare
the user a multi-minute redo.

In-flight LLM HTTP calls cannot be killed mid-flight (provider
SDKs don't expose cancellation), so cancellation latency is
"one tool/agent step" — typically a few seconds.

## Troubleshooting

### Browser opens to a 404 / "Connection refused"

The server may have failed to bind (port `47821` busy and the
ephemeral fallback also fell over). Pass `--port` to force a
different port:

```sh
amx /visualize --port 8765
```

### "Visualizer auth is not configured" on every request

The token cookie / localStorage entry was wiped or never captured
on this browser. Re-launch `amx /visualize` and let the launcher
re-open the browser with a fresh `?t=…` URL.

### `/ask` returns "Search catalog isn't initialised yet"

Run `/sync` (or `/run` once) to populate the SQLite-backed catalog.
The visualizer reuses the same store the CLI's `/ask` uses, so a
sync from either side surfaces immediately.

### Pending queue is empty but I just approved rows

The visualizer reads `~/.amx/pending_metadata.json`. Older AMX
versions wrote it to a different path — confirm with
`ls -la ~/.amx/pending_metadata.json`. If you're on shared-mode
history, the queue is still local-only by design (write-back is
per-machine).

### Connection-test on a Databricks profile fails with TLS error

The visualizer reuses `DatabaseConnector.test_connection_result()`,
so the same TLS setup as the CLI applies. Set
`tls_no_verify=true` (or pin a `tls_trusted_ca_file`) on the
profile from Settings, then click Test again.

## Development

The web UI source lives in two places:

- `amx/web/` — FastAPI backend (Python).
- `frontend/` — Vite + React + TypeScript + Tailwind project.

To work on the SPA locally:

```sh
# Terminal A — backend on :47821
amx /visualize --no-open

# Terminal B — Vite dev server on :5173 with /api proxy
cd frontend
npm run dev
```

Open `http://127.0.0.1:5173/?t=<token>` (the launcher prints the
token in Terminal A). Vite hot-reloads the UI; the proxy forwards
`/api/*` to uvicorn.

To ship a new SPA dist:

```sh
make web-build
git diff --quiet amx/web/static/   # CI requires this to be clean
```

The `amx/web/static/` directory is committed (vendored) so a fresh
clone running `pip install -e .` gets a working `/visualize`
without Node. CI's `web-build-freshness` job re-runs the build and
fails the PR if the committed dist is stale.
