# Security Policy

## Reporting a vulnerability

If you believe you have found a security issue in AMX, please **do not open a public GitHub issue**. Instead, report it privately so we have time to investigate and ship a fix before details are public.

- **Preferred:** open a [private security advisory](https://github.com/omeryasirkucuk/amx/security/advisories/new) on the repository.
- **Alternative:** email `omeryasirkucuk@gmail.com` with the subject `AMX security: <short summary>`.

Please include:

- A description of the issue and the impact you believe it has.
- Steps to reproduce, or a minimal proof of concept.
- The AMX version (`amx --version`), Python version, and OS.
- Whether the issue is already public anywhere.

We will acknowledge receipt within 5 business days and aim to provide an initial assessment within 10 business days. Once a fix is available, we will coordinate disclosure timing with you.

## Supported versions

AMX is on a 0.x release line during initial stabilization. Only the latest minor version receives security fixes; older releases are best-effort.

| Version  | Supported          |
| -------- | ------------------ |
| 0.3.x    | :white_check_mark: |
| 0.2.x    | :x: (upgrade)      |
| < 0.2    | :x:                |

## What's in scope

- The `amx` CLI and its agents (search, RAG, profile, code).
- DB connectors and adapters bundled with AMX (PostgreSQL, Snowflake, Databricks, BigQuery).
- The first-run / setup flow and how it persists credentials.

## What's out of scope

- Vulnerabilities in upstream dependencies (please report those upstream — we will pull the fix once it is released).
- Issues that require an attacker who already has root or filesystem access on the user's machine.
- Issues in third-party LLM or database providers used by AMX.

## Where AMX stores secrets

AMX persists configuration to `~/.amx/config.yml`. Database passwords and API keys may be stored there in plaintext on older versions; from 0.3.x onward they are stored in the OS keychain (macOS Keychain, Windows Credential Manager, Linux Secret Service) when available, and the YAML stores a reference rather than the secret itself. The config file is written with `0o600` permissions.

If you find a way to leak secrets out of AMX (for example via logs, crash reports, or telemetry), please report it under this policy.

## Air-gapped / offline operation

AMX is designed to run fully offline. When configured with a local database and a local LLM, **no outbound network calls are made** — not on startup, not at idle, not during a query.

### Zero-network configuration

- **Database:** any backend reachable on `localhost` or your private network (PostgreSQL, MySQL, DuckDB, ClickHouse, …). The connection string is whatever you put in `~/.amx/config.yml`; AMX never contacts a registry to "discover" databases.
- **LLM:** any local provider — Ollama, vLLM, llama.cpp, LM Studio, any OpenAI-compatible endpoint pointed at `127.0.0.1`. Set this in `/setup` or directly in `config.yml`.
- **Embeddings:** the default (`minilm`) runs locally via `sentence-transformers`. No remote embedding service is contacted unless you explicitly switch to `openai_compatible` in `/embeddings`.
- **Documents (`/docs`):** local filesystem paths only. Cloud sources (S3, Google Drive, SharePoint, GitHub) require explicit credentials and a remote URL — none are configured by default.

### What we explicitly do NOT do

- **No telemetry.** AMX ships no analytics SDK (Sentry, PostHog, Segment, Datadog, Mixpanel, Rollbar, Bugsnag). `pip install amx` pulls in none of them.
- **No update / version checks.** AMX never contacts PyPI or GitHub on startup.
- **No "phone home" identifier.** No anonymous user ID is generated or transmitted.
- **No remote crash reporting.** Crashes are written to `~/.amx/logs/crashes/<timestamp>.txt` with secrets redacted; the file stays local. You may attach it to a GitHub issue manually.
- **No CDN / external assets in Studio.** The Studio frontend (`/studio`) loads only its own bundled assets. There are no Google Fonts, no CDN imports, no tracker scripts. Content-Security-Policy `connect-src 'self'` enforces this in the browser as well.
- **No remote model-cost map fetch.** AMX sets `LITELLM_LOCAL_MODEL_COST_MAP=True` before importing LiteLLM so the upstream cost-map fetch is skipped entirely; LiteLLM uses its bundled local copy.

### When AMX *does* make network calls

Network activity only happens when you opt in by configuring it:

| Feature | Trigger | Endpoint |
| --- | --- | --- |
| Cloud LLM provider | You choose OpenAI / Anthropic / Gemini / OpenRouter / DeepSeek / Databricks Foundation in `/setup` | The provider you chose |
| Cloud DB adapter | You enter a Snowflake / BigQuery / Databricks / Redshift / cloud-hosted Postgres connection in `/setup` | The DB you chose |
| `/docs` cloud sources | You pass an `s3://`, `https://drive.google.com/…`, SharePoint URL, or GitHub URL to `/docs` | The source you passed |
| Batch API | You run `/batch` against an OpenAI / Anthropic batch job | OpenAI / Anthropic |
| Optional dep install | You first use `/studio`, `/docs`, `/code`, etc. and the optional package isn't already installed | PyPI (one-time `pip install`) |

If you operate in a fully air-gapped environment, leave the features in the right column unconfigured and the corresponding network calls are unreachable code paths.

### Corporate TLS / proxy

For environments behind a TLS-inspecting proxy (Zscaler, Netskope, …), set `AMX_CA_BUNDLE=/path/to/corp_root.pem` so cloud LLM providers (when used) trust the corporate root CA. `AMX_INSECURE_SSL=1` exists for one-shot diagnostics — do not use it in production.

### Verifying offline behaviour yourself

A quick sanity check on a sandboxed machine:

```bash
# Block egress at the OS level, then run AMX with a local config.
sudo pfctl -e   # macOS — enable packet filter
amx /setup      # configure local DB + Ollama (127.0.0.1)
amx /run "select 1"
```

The session should complete without any DNS resolution or outbound TCP attempt. Reports of unexpected network activity in a local-only configuration are treated as security issues under the policy above.
