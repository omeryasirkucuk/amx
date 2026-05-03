# AMX — Agentic Metadata Extractor

> **Stop staring at `T0001.AUDAT NUMBER(8)` wondering what it means.**

AMX walks your database, reads your documentation and codebase, then emits a complete description for every table and column — with confidence scores and a human-in-the-loop review before anything lands in the live DB. Multi-agent (Profile + RAG + Code), supports PostgreSQL / Snowflake / Databricks / BigQuery, talks to OpenAI / Anthropic / Gemini / DeepSeek / OpenRouter / vLLM / Ollama / LM Studio.

<!-- TODO: hero screenshot — paste the AMX banner + first /run wizard here -->

### What it produces

Cryptic identifier in:

```
sap_s6p.t001.audat   NUMBER(8) NULL
```

Reviewed description out (after one `/run`):

```
sap_s6p.t001.audat — Document date. The calendar date the source business event
was recorded, distinct from posting date (BUDAT) which controls the accounting
period the transaction lands in.

  confidence: high · logprob: 0.91 · sources: code (3 refs), docs, db profile
```

The same multi-agent pipeline runs against tables, views, materialized views, and schema-level descriptions.

## Quick start (5 minutes)

```bash
pip install amx                  # installs all four DB drivers and the LLM SDKs
amx                              # opens the interactive session
/setup                           # walks you through DB + LLM profiles
/run                             # picks scope, runs the agents, opens review
amx doctor                       # if anything looks weird — diagnoses install + config
```

That's the happy path. Read on for what each agent does, supported backends, and the full slash-command reference.

<!-- TODO: screenshot — `/setup` wizard prompt sequence -->

<!-- TODO: screenshot — `/run` review screen with Accept / Alternatives / Skip choices -->

## Problem

Enterprise databases accumulate thousands of tables and columns without proper documentation. Technical identifiers in any language or vendor dialect can be cryptic, and understanding what they represent requires institutional knowledge, scattered documentation, or deep code archaeology. AMX automates this discovery process.

## How It Works

AMX employs three **sub-agents** that independently analyze your data and then merge their findings:

| Agent | Source | What It Does |
|-------|--------|-------------|
| **Profile Agent** | Database | Analyzes column types, statistics (min/max/null counts/distinct values), sample data, comments, and relationships to infer meaning |
| **RAG Agent** | Documents | Ingests your documentation (PDFs, Word docs, Markdown, HTML, etc.) into a vector store and retrieves relevant context for each asset |
| **Code Agent** | Codebase | Scans application code (Python, SQL, Java, etc.) for references to tables/columns to understand how they're used |

Results from all agents are **merged** by an orchestrator using LLM reasoning, then presented to the user in a **human-in-the-loop** review where you can:
- Accept the top suggestion
- Choose from alternatives
- Provide your own description
- Skip individual items
- Bulk-accept high-confidence results
- Write approved metadata back to the database as `COMMENT ON TABLE/VIEW/COLUMN` (write-back support)

For release notes see [CHANGELOG.md](./CHANGELOG.md) and the [GitHub Releases page](https://github.com/omeryasirkucuk/amx/releases).

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                      AMX CLI                               │
├────────────────────────────────────────────────────────────┤
│                    Orchestrator Agent                      │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐   │
│  │ Profile Agent │  │   RAG Agent   │  │  Code Agent   │   │
│  │   (DB stats)  │  │  (Documents)  │  │ (Codebase)    │   │
│  └───────┬───────┘  └───────┬───────┘  └───────┬───────┘   │
│          └──────────────────┼──────────────────┘           │
│                         Merge & Rank                       │
│                              │                             │
│                   Human-in-the-Loop Review                 │
│                              │                             │
│                      Apply to Database                     │
├────────────────────────────────────────────────────────────┤
│ LLM Provider (OpenAI / Anthropic / Gemini / Local / ...)   │
└────────────────────────────────────────────────────────────┘
```

## Detailed setup

The 5-minute quick start at the top covers the happy path. This section spells out prerequisites, installation options, and the explicit slash-command sequence for users who want full control.

### Prerequisites

- Python 3.10+
- A database you can connect to — **PostgreSQL**, **Snowflake**, **Databricks** (Unity Catalog SQL warehouse), or **BigQuery** (drivers ship with the package)
- Access to at least one LLM provider you plan to configure (API key, local endpoint, etc.)

### Installation

From [PyPI](https://pypi.org/project/amx/) (recommended):

```bash
pip install amx
```

From source:

```bash
git clone https://github.com/omeryasirkucuk/amx.git
cd amx
pip install -e .
```

Optional extras:

```bash
# Richer SQL parsing in codebase scans (sqlglot)
pip install "amx[code-intel]"
# or from source: pip install -e ".[code-intel]"
```

A single `pip install amx` includes SQLAlchemy drivers for PostgreSQL, Snowflake, Databricks, and BigQuery so you can add profiles for any supported engine without extra packages.

AMX focuses on **metadata inference**, not bulk data loading. Populate schemas and tables with your own import or ETL process, then use AMX against that database.

### Configure AMX

```bash
amx
# then inside the session:
/setup
```

This interactive wizard walks you through:
1. **Database connection** — choose **PostgreSQL**, **Snowflake**, **Databricks**, or **BigQuery**, then enter that engine’s connection details
2. **AI model** — provider and API key (see [Supported LLM Providers](#supported-llm-providers))
3. **Data sources** — optional named **document** and **codebase** profiles for RAG and code scanning

In an interactive `amx` session, configuration is grouped by namespace:

- `/db` — database profiles + introspection. Entering `/db` explains how to list profiles, switch engines with `/use-db` (each option shows `[backend] connection summary`), and add a profile with `/add-db-profile` (engine first, then credentials)
- `/metadata` — inspect, edit, and monitor database/schema/table/column comments without running LLM agents (`/manual` is a compatibility alias)
- `/docs` — document roots + RAG (`/doc-profiles`, `/add-doc-profile`, `/ingest`, `/search-docs`)
- `/llm` — LLM profiles, metadata generation language, and cost controls (`/llm-profiles`, `/language`, `/add-llm-profile`, …)
- `/code` — codebase profiles (`/code-profiles`, `/add-code-profile`, …)
- `/search` — LLM-backed metadata discussion grounded on generated/manual metadata, relationships, and code evidence

AMX may **auto-select** the right namespace when you run an unambiguous command from the root prompt (it will print which namespace it assumed).

### Run Analysis

AMX is interactive-first: start with `amx`, then run slash commands.

```bash
# Start AMX session
amx

# Inside AMX (slash commands)
/db
/db-profiles
/connect
/schema sap_s6p
/docs
/add-doc-profile default
/analyze
/run t001 vbak
/run-apply t001 vbak
/apply
```

<!-- TODO: screenshot — `/db-profiles` table showing one or two profiles with backend column -->

<!-- TODO: screenshot — `/run` mid-flight: agent activity panel + per-column progress -->

<!-- TODO: screenshot — `/run` review wizard: top description, alternatives list, accept/skip prompts -->

<!-- TODO: screenshot — `/history list` after several runs, then `/compare --last 3 --diff` showing the per-column pivot -->

If anything goes wrong — profiles missing, can't connect, multiple `amx` on PATH — run `amx doctor` from any shell. It diagnoses install, config, and connectivity and prints actionable hints next to each ✗.

## Interactive Commands (inside `amx` session)

| Command | Description |
|---------|-------------|
| `/setup` | Interactive first-time configuration wizard |
| `/config` | Display current configuration, including active profiling guardrails |
| `/db` + `/db-profiles` | List DB profiles (shows **backend** + connection summary per row) |
| `/db` + `/use-db [name]` | Switch active profile; interactive picker lists each profile’s engine (PostgreSQL, BigQuery, …) |
| `/db` + `/add-db-profile [name]` | Add/update a profile: **choose engine first**, then connection fields for that backend |
| `/db` + `/remove-db-profile <name>` | Remove a DB profile |
| `/db` + `/profiling [mode] [max_rows] [sample_size]` | Show or set active DB profiling guardrails. Modes: `full`, `sampled`, `metadata`; use `off` for no max-row cutoff. |
| `/db` + `/tls [on\|off] [ca_path\|clear]` | Show or set Databricks TLS settings directly on the active profile. |
| `/db` + `/schema <name>` | Set default schema context (used by /tables, /analyze, …) |
| `/db` + `/table <name>` | Set default table context (used by /profile, /analyze, …) |
| `/db` + `/connect` | Test database connectivity |
| `/db` + `/schemas` | List available schemas |
| `/db` + `/tables [schema]` | List all assets (tables, views, materialized views) in a schema |
| `/db` + `/profile [schema] [table]` | Profile table structure and data |
| `/db` + `/inspect [profile]` | Diagnose a profile: backend, capabilities, connection test, visible schemas, table counts. Read-only. Helpful when metadata looks incomplete. |
| `/metadata` + `/inspect [schema] [table]` | Show current database, schema, table/view, and column comments. This namespace is for **database metadata**, not document manuals. |
| `/metadata` + `/edit` | Start the interactive edit wizard: DB profile, granularity, schema, table/view, column, then comment text. |
| `/metadata` + `/edit <db>` | Edit a database/profile comment. |
| `/metadata` + `/edit <db>.<schema>` | Edit one schema comment. |
| `/metadata` + `/edit <db>.<schema>.<table>` | Edit one table/view comment. |
| `/metadata` + `/edit <db>.<schema>.<table>.<column>` | Edit one column comment. |
| `/metadata` + `/edit table <schema>.<table>` | Legacy scoped form remains supported; ambiguous forms like `/edit table` start the wizard. |
| `/metadata` + `/monitor [schema]` | Show table/view and column comment coverage for one schema or all user schemas. |
| `/llm` + `/llm-profiles` | List LLM profiles |
| `/llm` + `/use-llm <name>` | Switch active LLM profile |
| `/llm` + `/add-llm-profile [name]` | Add/update an LLM profile (interactive) |
| `/llm` + `/remove-llm-profile <name>` | Remove an LLM profile |
| `/llm` + `/language [name]` | Show or set the preferred metadata generation language for the active LLM profile |
| `/llm` + `/prompt-detail [level]` | Show or set prompt detail level (`minimal` \| `standard` \| `detailed` \| `full`). Run without args to see a comparison table of all presets. |
| `/llm` + `/n-alternatives [N]` | Show or set number of description alternatives per column (1–5, default 3). Fewer = lower cost. |
| `/llm` + `/llm-batch-size [N]` | Show or set how many columns the Profile Agent sends in one LLM call. |
| `/llm` + `/batch-context-columns [off\|all\|N]` | Show or set how many non-batch column names are added as context in each profile batch. |
| `/llm` + `/logprob-thresholds [high] [medium]` | Show or set token-probability thresholds used to calibrate confidence labels when logprobs are available. AMX scores generated description text per suggestion when provider token offsets can be reconstructed, with a whole-response fallback. |

Notes:
- `/llm` settings are saved per **active LLM profile** and command feedback prints the profile name that was updated.
- `/llm /language` controls generated metadata language in `/run` and `/run-apply`; `/search` answers follow the user's question language.
- When defining an OpenRouter profile, enter the model in its natural provider/model form such as `openai/gpt-4o-mini`, `anthropic/claude-3.5-sonnet`, or `qwen/qwen3.6-plus`; AMX handles provider wiring internally.
- AMX now normalizes common provider-prefix typos in model ids as well; for example `oepnai/gpt-4o-mini` under an OpenRouter or OpenAI profile is corrected to the proper OpenAI namespace automatically.
- Profile selections made in the interactive `/run` wizard are persisted to `~/.amx/config.yml` immediately.
- `max_tokens` defaults to `4096`; when `finish_reason=length`, AMX now halts processing so truncated JSON is not parsed silently.
- `force_logprobs` defaults to `true` to force-request logprobs even when provider capability metadata is inconsistent.
- OpenAI Batch mode requests and stores returned logprobs; Anthropic Batch mode does not provide token logprobs, so those batch results keep model-declared confidence labels until merged by a logprob-capable chat call.
- `write_through_config` defaults to `true` to save profile switches and config mutations immediately.
- Direct changes to loaded top-level and nested DB/LLM config fields are write-through as well; AMX saves the active `config.yml` atomically after each mutation.
- `/analyze /run` now tests the active LLM before profiling any asset and stops immediately if the model/profile is unreachable or deactivated.
- Third-party LiteLLM warnings/debug lines stay out of the terminal by default; AMX surfaces only its own actionable warnings.
| `/code` + `/code-profiles` | List codebase profiles |
| `/code` + `/use-code <name>` | Switch active codebase profile |
| `/code` + `/add-code-profile [name]` | Add/update a codebase path (interactive) |
| `/code` + `/remove-code-profile <name>` | Remove a codebase profile |
| `/code` + `/code-scan [path]` | Scan codebase, save results + build `amx_code` semantic index. `--code-profile NAME` |
| `/code` + `/code-refresh` | Clear the active code profile’s scan cache and semantic `amx_code` chunks |
| `/code` + `/code-results` | View the last cached code-scan results |
| `/code` + `/code-analyze [TABLE …]` | Run Code Agent standalone (LLM); results saved for next `/run` |
| `/code` + `/export-code-report [FILE]` | Export scan results to a markdown file |
| `/docs` + `/doc-profiles` | List named document path profiles |
| `/docs` + `/use-doc <name>` | Switch active document profile |
| `/docs` + `/add-doc-profile [name]` | Add/update document roots (interactive) |
| `/docs` + `/remove-doc-profile <name>` | Remove a document profile |
| `/docs` + `/scan [paths...]` | Scan and preview documents for RAG (`--doc-profile NAME` when no paths) |
| `/docs` + `/ingest [paths...]` | Ingest documents into the RAG vector store (`--doc-profile`, `--refresh`) |
| `/docs` + `/search-docs <text>` | Similarity search over ingested docs (Chroma; no LLM) |
| `/docs` + `/doc-analyze [TABLE …]` | Run RAG Agent standalone (LLM); results saved for next `/run` |
| `/docs` + `/export-doc-report [FILE]` | Export RAG summary to a markdown file |
| `/analyze` + `/run [ASSET …]` | Run all agents with scope picker: Database / Schema / Asset; `--code-profile`, `--code-refresh` |
| `/analyze` + `/run-apply [ASSET …]` | Same as `/run --apply` |
| `/analyze` + `/apply` | Write pending approved metadata to the database |
| `/search` + `/ask [--actions] [--debug] <question>` | Ask conversational metadata questions with LLM grounding over the internal search catalog. Each `/ask` appends to a persistent session (`~/.amx/history.db`), so follow-ups like "any others?" or "what about its columns?" remember prior turns even after `/exit` and restart. The default output is a natural-language summary plus a clean `Schema.Table | Match | Why | Rows | Cols | Description` table; `--debug` (alias `--verbose`) reveals the planner's Thought Trace and adds raw `Score`/`Source`/`Conf` columns. `--actions` prompts before running approved follow-up actions. |
| `/session` + `/new \| /list \| /resume <id> \| /end \| /show` | Manage `/ask` conversation sessions. `/session new [--title]` starts a fresh chat session and pins it as active; `/session list [-n N] [--all-profiles]` shows recent sessions with first-question excerpts; `/session resume <id>` switches the active session pointer (refuses cross-profile resume); `/session end` closes the current session; `/session show [--id N] [--include-compacted]` dumps the per-turn audit trail. New REPL boots always start fresh — the user opts back in via `/session resume`. When a session's live token estimate exceeds ~40% of the model's input budget, AMX summarises the oldest slice with a single LLM call (Claude/Gemini/OpenAI/DeepSeek) and replaces it with a synthetic summary turn so follow-ups still ground against compacted history; falls back to a stub when no LLM is available. |
| `/search` + `/status` | Show catalog counts, freshness, and recent sync jobs |
| `/search` + `/sources` | Show enabled search settings and evidence-source coverage |
| `/search` + `/config [key] [value]` | View or update `/search` settings for the active DB profile |
| `/search` + `/context-detail [minimal\|standard\|rich\|deep]` | Control how much catalog/code/history context `/search` sends into grounded reasoning |
| `/search` + `/sync [--schema …] [--table …]` | Sync DB structure/comments and cached code evidence into the catalog |
| `/search` + `/rebuild` | Rebuild effective search state and the `amx_search` vector index |
| `/doctor` (or `amx doctor` from the shell) | Diagnose installation / config / connectivity. Reports every `amx` binary on `PATH` (catches the version-skew bug class), Python runtime, config schema version, optional backend deps, active DB + LLM reachability. Use `--skip-network` for an offline quick check. Runs from a broken state — does not require an interactive session. |
| `/history` + `/compare [RUN_IDS…] [--schema …] [--table …] [--column …] [--last N] [--command analyze.run\|search.ask\|all] [--by auto\|llm_profile\|doc_profile\|code_profile\|llm_model\|db_profile] [--diff] [--csv FILE] [--md FILE] [--json FILE]` | Pivot past runs side-by-side. Four Rich tables: **Run summary** (identity — profiles, model, duration, approval rate; highlights the dimension that varies), **Run settings** (prompt detail, language, batch size, n alternatives, dedup/missing-only flags, review strategy — exactly which knobs you tuned between runs), **Per-column results** (top description + confidence band + `logprob_score` + tokens; best logprob per row in green), and **Aggregate metrics** (timing + tokens + confidence distribution; best per row bolded). `--diff` adds word-level highlights vs the leftmost run; `--csv` / `--md` / `--json` also write the comparison to disk (JSON pairs cleanly with pandas / Jupyter — see `tests/eval/README.md`). |
| `/search` + `/embeddings [kind] [model]` | Show or change the search-index embedding provider. Kinds: `MiniLM` (default, offline), `OpenAI-compatible` (OpenAI / OpenRouter / Together / Mistral / Azure / vLLM / LM Studio), `Local` (sentence-transformers, requires `pip install "amx[local-embeddings]"`). Run `/rebuild` after switching to re-embed the catalog. |
| `/usage [window]` | Local LLM token + approximate-cost summary read from `~/.amx/history.db`. Windows: `24h`, `7d` (default), `30d`, `all`. No network calls. Pricing built in for OpenAI / Anthropic / Gemini / DeepSeek; unknown models show `—` for cost. |
| `/history` + `/list [-n N]` | List recent runs with end-to-end and model-processing duration |
| `/history` + `/show <run_id>` | Show full JSON payload for one run (scope, metrics, tokens, results, errors) |
| `/history` + `/stats` | Aggregate local run/event statistics plus search lifecycle counts |
| `/history` + `/events [-n N]` | List recent app events (profile switches, run status, apply outcomes, etc.) |
| `/history` + `/results <run_id>` | Show all saved LLM alternatives for a past run, including catalog state, effective source, index state, and DB-apply state |
| `/history` + `/review <run_id>` | Re-evaluate saved alternatives for a past run; `--unevaluated-only` to skip already-decided rows; `--apply` to write to DB immediately |

## Codebase and document intelligence

- **Profiles without switching context**: use `--code-profile` / `--doc-profile` on CLI commands (or the same flags after `/code-scan`, `/ingest`, `/run` in session) instead of `/use-code` / `/use-doc` first.
- **Code scan cache**: `~/.amx/code_cache/<slug>/` stores a manifest plus serialized scan results so `/run` does not re-walk the repo every time. Use **`--code-refresh`** or **`/code-refresh`** after the tree changes; refresh clears the active profile’s cache and semantic chunks.
- **Semantic code RAG**: Chroma collection **`amx_code`** holds embedded chunks (Python by function/class span; other languages by text split). Chunks are tagged by source path, and the Code Agent filters nearest-neighbor retrieval to the active code profile. This is **assistive**, not a proof of dataflow—wide schemas use **capped** table/column lists for performance.
- **Identifiers outside the DB**: strings that look like catalog objects but are not in the connected table list appear as **secondary context** for the LLM (for example external lake tables).
- **Doc RAG refresh**: **`/ingest --refresh`** removes existing chunks whose stored resolved file path or original profile source path matches the files you are ingesting, then re-upserts—useful when files shrink or move, including remote sources downloaded to temporary paths.
- **Remote Git cleanup**: GitHub document and codebase sources are cloned into temporary directories only for the active scan/ingest operation, then removed after AMX finishes reading and indexing them.

## Supported Document Sources

When you add paths with `/add-doc-profile` or during `/setup`, AMX checks **reachability only** (for example `git ls-remote` for GitHub, bucket/prefix checks for S3, lightweight HTTP checks for Drive/SharePoint). Full file discovery happens on `/scan` and `/ingest`.

| Source | Path Format | Status |
|--------|-------------|--------|
| Local files/directories | `/path/to/docs` | Supported |
| GitHub repositories | `https://github.com/user/repo` or `git@github.com:user/repo.git` | Supported |
| AWS S3 | `s3://bucket/prefix` | Supported |
| Google Drive links | `https://drive.google.com/...` | Supported — public links work with zero setup; private files need credentials (see below) |
| SharePoint / OneDrive links | `https://...sharepoint.com/...` or `https://onedrive.live.com/...` | Supported — public sharing links work with zero setup; private files need Azure credentials (see below) |

S3 scans and ingests preserve object key prefixes in the temporary download tree, so files with the same basename under different prefixes remain distinct.

### Cloud document access

AMX always **tries the public/anonymous download first** — no credentials needed if the file is shared as "Anyone with the link". Credentials are only required for private files or folder listings.

**Google Drive**

- **Public files** (shared as "Anyone with the link can view"): just paste the link, no setup needed.
- **Google Docs/Sheets/Slides**: public export to PDF/CSV works automatically.
- **Private files or entire folders**: set one of:
  - `AMX_GOOGLE_SERVICE_ACCOUNT_JSON` — path to a service account JSON; share the file/folder with that service account email.
  - `AMX_GOOGLE_OAUTH_TOKEN_JSON` — path to a user OAuth token JSON from a prior consent flow.

**SharePoint / OneDrive**

- **Public sharing links** ("Anyone with the link"): just paste the link, no setup needed.
- **Private / org-restricted files**: set:
  - `AMX_AZURE_TENANT_ID`
  - `AMX_AZURE_CLIENT_ID`
  - `AMX_AZURE_CLIENT_SECRET`

  Use an Azure AD app registration with Graph permissions (**Files.Read.All**, **Sites.Read.All**).

### Supported Document File Types

AMX scans/ingests these extensions:

`pdf`, `docx`, `doc`, `txt`, `md`, `csv`, `xlsx`, `xls`, `html`, `htm`, `pptx`, `json`, `yaml`, `yml`, `rst`, `rtf`

## Supported database backends

| Backend | Config (`backend`) | Install | Notes |
|---------|--------------------|---------|-------|
| PostgreSQL | `postgresql` | `pip install amx[postgresql]` | `COMMENT ON TABLE/COLUMN/SCHEMA/DATABASE`; back-fills procedures, functions, sequences, triggers, UDTs |
| Snowflake | `snowflake` | `pip install amx[snowflake]` | Account, warehouse, role; Snowflake `COMMENT`; back-fills procedures, functions, sequences, tasks, stages, shares, external tables |
| Databricks | `databricks` | `pip install amx[databricks]` | SQL warehouse HTTP path + PAT; Unity Catalog; back-fills user functions, **volumes**, external tables |
| BigQuery | `bigquery` | `pip install amx[bigquery]` | GCP project + dataset; descriptions via `ALTER … SET OPTIONS`; back-fills routines (procedures + functions), external tables |
| MySQL / MariaDB | `mysql` | `pip install amx[mysql]` | `ALTER TABLE … COMMENT`; surfaces stored procedures, functions, triggers, **events** (scheduled jobs), partition strategy, storage engine |
| Oracle | `oracle` | `pip install amx[oracle]` | `service_name` (preferred) or SID; surfaces materialized views, procedures, functions, **packages** (Oracle-distinctive), triggers, sequences, synonyms, UDTs |
| SQL Server | `mssql` | `pip install amx[mssql]` (requires ODBC Driver 18) | Comments via `sp_addextendedproperty` / `sp_updateextendedproperty`; surfaces procedures, functions (FN/TF/IF), triggers, sequences, synonyms, partitions |
| Redshift | `redshift` | `pip install amx[redshift]` | PG-compatible `COMMENT ON`; surfaces materialized views, procedures, UDFs, **datashares**, **external tables** (Spectrum); analytics metadata includes diststyle/sortkey/encoding |
| ClickHouse | `clickhouse` | `pip install amx[clickhouse]` | `ALTER TABLE … MODIFY COMMENT` (21.x+); surfaces materialized views, UDFs, **dictionaries** (ClickHouse-distinctive), skipping indices, MergeTree engine info |
| DuckDB | `duckdb` | `pip install amx[duckdb]` | Single-file or `:memory:`; surfaces sequences, functions, **macros** (DuckDB-distinctive), attached databases (Parquet/S3/Postgres scanner) |

> Each backend driver is now an optional extra. Pick what you use:
> `pip install amx[postgresql,duckdb]` — or grab everything with `pip install amx[all]`.

Introspection and profiling use backend-specific SQL where needed; metadata write-back uses each platform's supported description/comment mechanism. Each adapter advertises its capabilities so unsupported operations fail clearly instead of being counted as applied. For example, BigQuery project-level descriptions are blocked before connection, MySQL has no `COMMENT ON SCHEMA` so schema comments raise rather than silently no-op, and DuckDB schema-level comments are unsupported in DuckDB 1.x.

Beyond tables and views, each adapter exposes the object types that are first-class on its backend: stored procedures, user functions, sequences, triggers, scheduled jobs/tasks/events, packages (Oracle), synonyms (Oracle / SQL Server), user-defined types, dictionaries (ClickHouse), macros (DuckDB), volumes (Databricks Unity Catalog), datashares (Snowflake / Redshift), and external tables (Snowflake / Databricks / BigQuery / Redshift Spectrum / DuckDB Parquet). Capability flags on `BackendCapabilities` gate which list operations the connector even attempts, so unsupported types short-circuit cleanly.

## Supported LLM Providers

| Provider | Config value |
|----------|--------------|
| OpenAI | `openai` |
| OpenRouter | `openrouter` |
| Anthropic | `anthropic` |
| Google Gemini | `gemini` |
| DeepSeek | `deepseek` |
| Ollama | `ollama` |
| OpenAI-compatible (custom base URL) | `local` |

Notes:
- For `ollama`, use API base URL `http://localhost:11434` (no `/v1`).
- For `openrouter`, use API base URL `https://openrouter.ai/api/v1` and an OpenRouter API key.
- For OpenAI-compatible local endpoints (`local`), use `http://localhost:11434/v1` when the server exposes OpenAI-style routes.

## Database Details Sent to LLM (Profile Agent)

When AMX profiles a table, it sends the following database-derived context to the Profile Agent prompt:

- Scope: database name, schema, table
- Table-level: row count, existing table comment, schema comment, database comment
- Constraints and relationships:
  - Primary key columns
  - Outgoing foreign keys (upstream dependencies)
  - Incoming foreign keys (downstream dependents)
  - Unique constraints
  - Check constraints
- Usage stats
- Related metadata: existing comments on FK-related neighbor tables
- Per-column profile:
  - name, type, nullable
  - null count, distinct count, cardinality ratio (`distinct_count / row_count`)
  - min/max value (as text)
  - up to 5 distinct non-null sample values
  - existing column comment

AMX does not send full table dumps; it sends summarized profiling signals and small samples for inference.

### Profiling guardrails

Each DB profile has profiling guardrails to control warehouse cost:

- `full` — exact row count plus per-column null count, distinct count, min/max, and samples. If table statistics report more rows than `profiling_max_rows`, AMX skips the expensive full column scans and keeps lightweight metadata plus samples. Snowflake, Databricks, and BigQuery also skip full scans when row-count statistics are unavailable.
- `sampled` — skips exact row count and full per-column aggregate scans; uses backend table statistics when available and retrieves only small sample values with backend sampling syntax where supported.
- `metadata` — skips table-data reads entirely; uses schema metadata, comments, constraints, and backend table statistics when available.

Use `/db` then `/profiling` to view the active settings. Use `/profiling sampled 500000 3`, `/profiling metadata`, or `/profiling full off 5` to update them. Settings are saved on the active DB profile in `~/.amx/config.yml`.

Backend profiling failures are normalized into actionable messages where possible. PostgreSQL, Snowflake, Databricks, and BigQuery permission, missing-object, warehouse, quota, or connection failures surface remediation text instead of leaking raw driver traces, and AMX can skip expensive per-column stats when a single column-level stats query fails.

### Databricks TLS notes

If your Databricks workspace is reached through a company proxy or private CA, the default TLS trust chain may fail with `CERTIFICATE_VERIFY_FAILED`.

- AMX now uses Databricks' native Python SQL connector for the `db connect` health check itself, while continuing to use the Databricks SQLAlchemy dialect for metadata inspection and normal runtime SQLAlchemy flows.
- `db connect` now tries Databricks connectivity in stages: saved profile first, then a CA bundle discovered from supported environment variables, then `tls_no_verify` as a last resort. The first successful recovery path is saved back into the active DB profile and printed in the terminal.
- Set a **Trusted CA bundle path** in the Databricks DB profile to point at your corporate/root CA PEM bundle.
- The path may use `~` or environment variables such as `$HOME/certs/company-ca.pem`; AMX expands it before opening the Databricks connection.
- If the profile field is empty, AMX checks `AMX_DATABRICKS_TRUSTED_CA_FILE`, `DATABRICKS_TRUSTED_CA_FILE`, `REQUESTS_CA_BUNDLE`, then `SSL_CERT_FILE` and passes the first configured bundle to the Databricks connector.
- If you do not have the CA bundle yet, you can temporarily enable **Disable TLS certificate verification** in that DB profile. This is insecure and should only be a last resort for internal troubleshooting.

Example profile fields in `~/.amx/config.yml`:

```yaml
db_profiles:
  databricks-prod:
    backend: databricks
    host: adb-1234567890123456.7.azuredatabricks.net
    http_path: /sql/1.0/warehouses/abcd1234ef567890
    catalog: my_catalog
    database: my_schema
    tls_trusted_ca_file: ~/certs/internal-ca.pem
    tls_no_verify: false
```

## Configuration

AMX stores its configuration at `~/.amx/config.yml`. To use a different file, start the CLI with `amx --config path/to/config.yml`.

## Run history storage

AMX automatically initializes a local SQLite database at:

- `~/.amx/history.db`

Current persisted data includes:

- `/analyze run` history (status, mode, duration, backend/provider/model, scope)
- token usage (summary + per-step records)
- approved/skipped metadata results
- run failures (error text)
- app events (profile switches, apply outcomes, run success/failure)
- **all LLM-generated alternatives per column/table per run** — every merged suggestion set is
  saved before human review so you can revisit and change your mind at any time

Query it directly in AMX via `/history` namespace:

| Command | Description |
|---------|-------------|
| `/list [-n N]` | Recent runs (includes `Duration(s)` and `Model(s)`) |
| `/show <run_id>` | Full run JSON |
| `/stats` | Aggregate stats |
| `/events [-n N]` | App events |
| `/results <run_id>` | All saved LLM alternatives for a run |
| `/review <run_id>` | Re-evaluate alternatives interactively; `--unevaluated-only` / `--apply` |

### Shared mode (team collaboration, optional)

By default the SQLite history is per-machine — your teammate cannot
see runs you executed. Enable shared mode to dual-write every
run/result/event into a backend the team already owns.

`/history-store` lives under the `/db` namespace. Type the bare
command to open an interactive picker — Status comes first, followed
by context-aware actions (Enable / Disable / Migrate / Flush) based
on whether shared mode is already on:

```
/db
/history-store
```

The picker prints the current shared-mode status and then shows a
numbered menu (Enter accepts the highlighted default, "Status"):

```
1. Status — show shared-mode state and outbox depth   (default)
2. Enable — bootstrap an AMX schema on a saved DB profile
3. Dump DDL — print bootstrap SQL for a DBA to run by hand
4. Cancel — exit without doing anything
```

Once shared mode is enabled, the menu shifts to Status / Disable /
Migrate from local / Flush pending / Dump DDL / Cancel. AMX
bootstraps an `AMX` schema (configurable) on the chosen DB profile
and creates four tables: `analysis_runs`, `run_results`,
`app_events`, `session_state`. Every subsequent write goes to local
SQLite first (always-on cache, source of truth for `/history list`)
and best-effort to the shared backend.

Power users / scripts can invoke each action directly:

| Picker option | Click subcommand | Description |
|---|---|---|
| Status | `amx db history-store status` | Show shared mode state, profile, schema, outbox depth |
| Enable | `amx db history-store enable [--profile P --schema S]` | Bootstrap the AMX schema and start dual-writing |
| Disable | `amx db history-store disable` | Stop dual-writing. Existing shared rows are not deleted |
| Migrate from local | `amx db history-store migrate-from-local` | Idempotent one-shot copy of existing local history rows into the shared store |
| Flush pending | `amx db history-store flush-pending` | Replay queued shared writes that failed at write time |
| Dump DDL | `amx db history-store dump-ddl [--profile P --schema S]` | Print bootstrap DDL for a DBA to run by hand |

**Supported backends for shared mode:** PostgreSQL, MySQL, MSSQL,
Oracle, Snowflake, Databricks, Redshift, BigQuery. DuckDB (local
file) and ClickHouse (no row UPDATE support) are blocked at Enable
time with a clear error.

**Failure semantics:** when the shared backend is unreachable, the
local row still lands and the failed write is queued in
`pending_shared_writes` for replay via the Flush pending action.
Your CLI session is never blocked by team-store outages.

**Reads:** v0.12.0 reads still come from local SQLite — `/history
list` shows your machine's runs. Cross-machine read views are slated
for a follow-up minor.

**Attribution:** every shared row records `created_by`, `hostname`,
and `client_version`, plus a `local_id` linking back to the SQLite
INT id on the originating machine.

## Search Catalog

AMX now maintains an internal `/search` catalog inside the same local SQLite history database (`~/.amx/history.db`). It stores:

- effective metadata state per database/schema/table/column
- generated, reviewed, manual, imported, and rejected description candidates
- FK and inferred relationships
- normalized code-usage evidence from `/code scan`
- sync/rebuild job history and per-profile `/search` settings

Sync behavior:

- `/analyze run` and `/run-apply` automatically persist generated alternatives and refresh `/search`
- `/history review` mirrors accepted/custom/skipped decisions into the catalog
- `/metadata edit` writes a `manual` catalog description immediately
- `/code scan` refreshes code-usage evidence for `/search`
- `/search rebuild` recomputes effective state and rebuilds the `amx_search` vector index

Answering behavior:

- `/search` is chat-first: inside the `/search` tab, plain text is treated as a metadata question
- each question now runs through a dedicated **Search Agent** pipeline: interpretation, retrieval planning, grounded retrieval, live verification for high-risk structural claims, answer synthesis, and optional follow-up action suggestions
- `/search ask` can answer both semantic questions and catalog-overview questions such as "which databases are known", "which schemas exist", or "how many tables are in this schema"
- broad structural questions such as "how many columns per table?" use SchemaExplorer and return set-based Markdown tables with table names, column counts, row counts, and semantic clusters instead of a single best match
- **superlative and top-K inventory questions answer with one fact, not a dump**: prompts like "which table has the most rows in `sap_s6p`", "satır sayısı en fazla olan tablo hangisi", or "top 5 tables by row count" produce a one-sentence headline naming the top table (or a 2–5 row markdown table for top-K) instead of dumping all tables. The agent picks one of seven explicit *answer shapes* — `single_fact`, `short_table`, `full_table`, `ranked_list`, `table_summary`, `join_candidates`, `prose` — emitted by the interpretation pass and respected by both the deterministic formatters and the LLM synthesis prompt
- the bottom Rich "Search matches" table is suppressed for inventory and prose answers (where the headline already carries the data) and now filters out rows whose score is exactly `0.00` so inventory leakage and zero-score diagnostics never leak into the user-facing match list. Inventory rows render in a dedicated Schema | Table | Columns | Rows | Cluster table when shown
- `/search ask` now distinguishes table-level semantic discovery from inventory questions, so prompts like "which tables contain address details" route to ranked table matches instead of accidental table-count answers
- `/search ask` uses an LLM-native interpretation pipeline with deterministic safeguards only as resilience fallbacks; semantic results are grounded by lexical, structural, statistical, and documentation evidence instead of vendor-specific naming rules
- inventory/count questions such as schema lists or table counts use live DB introspection so they remain correct even if only part of the catalog has generated descriptions
- semantic questions use effective metadata first, with exact/fuzzy name matching, multilingual query variants, and vector support as an independent fallback when lexical terms do not match
- synthesized answers still receive the visible grounded result set, but `/search` now suppresses low-confidence tail rows before answering so weak vector-only matches do not dominate the user-facing summary
- `/search` prompting now treats explicit/live evidence as stronger than semantic fallback evidence, and its interpreter prompt is more conservative about follow-up scope, ambiguity, and enum selection
- table-scoped factual questions are live-first: questions such as "what is the ADRC table?" or "are all ADRC columns commented?" resolve the requested table and run safe live metadata probes before answering structural facts like column count, types, nullability, table comments, and column-comment coverage. Open-ended semantic column searches, such as "city related column names", stay on catalog/vector retrieval unless the user explicitly scopes them to a table.
- table-scoped factual verification now uses deterministic read-only probe selection instead of a second LLM planning hop, so `/search` can take safe metadata actions automatically without drifting into irrelevant planner output
- explicit table mentions such as `schema.table`, `ADRC table`, or `adrc tablosunda` take precedence over fuzzy catalog matches. If the exact live table cannot be verified, AMX refuses to substitute a similar candidate such as `ADR6`; fuzzy matches are shown only as suggestions.
- `/search` only labels an answer as live verified when live metadata rows were actually collected; catalog-only or fuzzy evidence is capped to lower confidence.
- join questions prioritize verified FK relationships, then semantic join inference, then observed code usage; one-table join questions can also surface non-FK semantic candidates with confidence bands such as `verified`, `high_likelihood`, `possible`, and `weak_hypothesis`
- join answers now prefer a deterministic short-form summary when the top verified join target or join-column pair is clear, reducing unnecessary LLM synthesis
- follow-up questions reuse narrower session memory so users can keep discussing the same table or field naturally without broad semantic result sets contaminating later table-scoped questions
- `/search ask` shows live progress while AMX interprets the question, retrieves evidence, and synthesizes the answer
- `/search ask` records retrieval policy, evidence sources, ambiguity flags, per-stage timings, suggested actions, executed read-only actions, answer strategy, and suppressed-row counts into history/event payloads so answers remain diagnosable
- `/search ask` now also records a concise thought trace of observable planning/tool stages (`interpret_question`, `metadata_query` or `schema_explorer`, `data_peek`, `verify_evidence`) for debugging without exposing hidden model chain-of-thought
- `/search ask --actions` turns selected suggestions into a human-approved execution loop: AMX asks before running catalog sync, cached code-evidence refresh, or single-table metadata analysis actions, then records the action outcome
- `/search /context-detail` controls how much neighborhood, code, and history context is exposed to the search synthesizer for cost/latency tuning
- `/search` answer language is forced to the detected language of the user's question, even if the interpreter LLM suggests a different `answer_language`
- aggregate answers avoid dumping the generic schema/table/column result grid when that grid would be irrelevant to the user question
- if no active LLM profile exists, `/search ask` fails closed and tells you to configure `/llm`
- `/analyze` agent prompts are now more conservative: profile, code, and RAG agents explicitly avoid unsupported business claims, explain confidence using evidence classes, and the orchestrator merge prompt now uses source precedence instead of averaging conflicting descriptions

## Project Structure

```
amx/
├── cli.py              # Thin CLI bootstrap, session wiring, and command registration
├── cli_analyze_flow.py # Compatibility shim -> amx.cli_support.commands.analyze_flow
├── cli_code.py         # Compatibility shim -> amx.cli_support.commands.code
├── cli_db.py           # Compatibility shim -> amx.cli_support.commands.db
├── cli_docs.py         # Compatibility shim -> amx.cli_support.commands.docs
├── cli_history.py      # Compatibility shim -> amx.cli_support.commands.history
├── cli_manual.py       # Compatibility shim -> amx.cli_support.commands.manual
├── cli_profiles.py     # Compatibility shim -> amx.cli_support.commands.profiles
├── cli_run.py          # Compatibility shim -> amx.cli_support.commands.run
├── core/
│   ├── __init__.py      # Programmatic API exports
│   ├── application.py   # Headless AMXApplication facade for `import amx`
│   ├── ask_agent.py     # Tool-loop ask primitives and trace/result types
│   ├── inference.py     # Single-table metadata inference entrypoint used by approved search actions
│   ├── metadata.py      # Universal Metadata Interface canonical entities
│   ├── state.py         # Write-through config/session state manager
│   └── token_budget.py  # Prompt-size guard and deterministic context compaction
├── cli_support/
│   ├── __init__.py      # Session-helper export surface
│   ├── session.py       # Interactive shell, slash completion, namespace switching, and session defaults
│   ├── root_commands.py # /setup, /config, and top-level /db registration
│   └── commands/
│       ├── __init__.py   # Command package marker
│       ├── analyze_flow.py # /analyze run command flow
│       ├── code.py         # /code namespace commands
│       ├── db.py           # /db profile and profiling helpers
│       ├── docs.py         # /docs namespace commands
│       ├── history.py      # /history namespace commands
│       ├── manual.py       # /metadata namespace commands (/manual alias)
│       ├── profiles.py     # /llm plus document/code profile helpers
│       ├── search.py       # /search namespace commands
│       └── run.py          # /analyze helpers, scope resolution, and apply flow
├── search/
│   ├── agent.py        # Multi-step Search Agent (planner, retriever, verifier, synthesizer)
│   ├── catalog.py      # SQLite-backed metadata catalog and lifecycle sync
│   ├── index.py        # Chroma `amx_search` vector index
│   └── service.py      # Thin compatibility facade over the Search Agent
├── services/
│   ├── __init__.py         # Service-layer package marker
│   ├── analyze_scope.py    # Scope resolution, asset filtering, and codebase preparation
│   └── manual_metadata.py  # Metadata inspect/edit/coverage business logic
├── config.py           # Configuration management
├── agents/
│   ├── base.py         # Base agent types and shared data structures
│   ├── orchestrator.py # Multi-agent coordination + human-in-the-loop
│   ├── profile_agent.py # Database profiling agent
│   ├── rag_agent.py    # Document RAG agent
│   ├── code_agent.py   # Codebase analysis agent
│   └── tools/          # Reusable agent tools such as SchemaExplorer
├── db/
│   ├── connector.py    # Database introspection and metadata I/O
│   └── adapters/       # Backend-specific SQL and connections (PG, Snowflake, …)
├── docs/
│   ├── scanner.py      # Multi-source document scanner
│   └── rag.py          # ChromaDB vector store and RAG pipeline
├── codebase/
│   ├── analyzer.py     # Codebase reference analysis
│   ├── cache.py        # Disk cache for scan results
│   └── code_rag.py     # Semantic code index (Chroma amx_code)
├── llm/
│   ├── provider.py     # Unified LLM interface via LiteLLM
│   └── batch.py        # Provider-agnostic Batch API (OpenAI, Anthropic)
├── core/
│   ├── __init__.py     # Importable core surface
│   └── inference.py    # Programmatic metadata inference API (no CLI shell)
└── utils/
    ├── console.py       # Rich console helpers
    ├── live_display.py  # Live terminal UI for agent runs (rich.Live)
    ├── token_tracker.py # tiktoken-based token counting and usage tracking
    └── logging.py       # Structured logging
```

## Changelog

See [`CHANGELOG.md`](./CHANGELOG.md) for the complete release history, or the [GitHub Releases page](https://github.com/omeryasirkucuk/amx/releases) for tagged releases.

## Programmatic API

Run AMX inference without entering the interactive CLI shell:

```python
import amx
from amx.core import infer_table_metadata

app = amx.init()                       # loads ~/.amx/config.yml by default
results = infer_table_metadata(
    app.config, schema="sap_test", table="adr6"
)
print(results[0])
```

The full stable surface — every name guaranteed to keep working across minor versions — is documented in [`docs/PUBLIC_API.md`](./docs/PUBLIC_API.md). Anything not listed there (including most of `amx.config`, `amx.cli_support`, `amx.agents`, `amx.search`, etc.) is internal and may move or change in any release.

## License

Apache-2.0 — see [`LICENSE`](./LICENSE).
