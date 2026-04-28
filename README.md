# AMX — Agentic Metadata Extractor

AI-powered CLI application that automatically infers, reviews, and applies metadata (descriptions, tags) to database assets — tables, views, and materialized views — using a multi-agent system with human-in-the-loop validation.

## Problem

Enterprise databases accumulate thousands of tables and columns without proper documentation. Column names like `BUKRS`, `MANDT`, or `WAERS` are cryptic, and understanding what they represent requires institutional knowledge, scattered documentation, or deep code archaeology. AMX automates this discovery process.

## How It Works

AMX employs three **sub-agents** that independently analyze your data and then merge their findings:

| Agent | Source | What It Does |
|-------|--------|-------------|
| **Profile Agent** | Database | Analyzes column types, statistics (min/max/null counts/distinct values), sample data, and naming patterns to infer meaning |
| **RAG Agent** | Documents | Ingests your documentation (PDFs, Word docs, Markdown, HTML, etc.) into a vector store and retrieves relevant context for each asset |
| **Code Agent** | Codebase | Scans application code (Python, SQL, Java, etc.) for references to tables/columns to understand how they're used |

Results from all agents are **merged** by an orchestrator using LLM reasoning, then presented to the user in a **human-in-the-loop** review where you can:
- Accept the top suggestion
- Choose from alternatives
- Provide your own description
- Skip individual items
- Bulk-accept high-confidence results
- Write approved metadata back to the database as `COMMENT ON TABLE/VIEW/COLUMN` (write-back support)

Recent release notes:
- `v0.1.122`: Apply-mode write-back now shows live elapsed time and per-asset progress in the terminal, and failed writes persist a `failed` DB-apply status for the corresponding saved result row.
- `v0.1.121`: Apply-mode database write-back now reuses one transaction per batch, and Databricks profiles with `tls_no_verify` no longer print one insecure-request warning per write-back request.

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

## Quick Start

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
| `/db` + `/schema <name>` | Set default schema context (used by /tables, /analyze, …) |
| `/db` + `/table <name>` | Set default table context (used by /profile, /analyze, …) |
| `/db` + `/connect` | Test database connectivity |
| `/db` + `/schemas` | List available schemas |
| `/db` + `/tables [schema]` | List all assets (tables, views, materialized views) in a schema |
| `/db` + `/profile [schema] [table]` | Profile table structure and data |
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
| `/search` + `/ask [--actions] <question>` | Ask conversational metadata questions with LLM grounding over the internal search catalog; `--actions` prompts before running approved follow-up actions |
| `/search` + `/status` | Show catalog counts, freshness, and recent sync jobs |
| `/search` + `/sources` | Show enabled search settings and evidence-source coverage |
| `/search` + `/config [key] [value]` | View or update `/search` settings for the active DB profile |
| `/search` + `/context-detail [minimal\|standard\|rich\|deep]` | Control how much catalog/code/history context `/search` sends into grounded reasoning |
| `/search` + `/sync [--schema …] [--table …]` | Sync DB structure/comments and cached code evidence into the catalog |
| `/search` + `/rebuild` | Rebuild effective search state and the `amx_search` vector index |
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

| Backend | Config (`backend` in `~/.amx/config.yml`) | Notes |
|---------|-------------------------------------------|--------|
| PostgreSQL | `postgresql` | Default; `COMMENT ON TABLE/COLUMN` |
| Snowflake | `snowflake` | Account, warehouse, role; Snowflake `COMMENT` syntax |
| Databricks | `databricks` | SQL warehouse HTTP path + personal access token; Unity Catalog optional |
| BigQuery | `bigquery` | GCP project, dataset; table/schema/column descriptions via `ALTER … SET OPTIONS`; project-level descriptions are not supported through SQL write-back |

Introspection and profiling use backend-specific SQL where needed; metadata write-back uses each platform’s supported description/comment mechanism. Each adapter advertises its capabilities so unsupported operations fail clearly instead of being counted as applied. For example, BigQuery project-level descriptions are blocked before connection, and Databricks catalog comments require a configured Unity Catalog name.

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

- Set a **Trusted CA bundle path** in the Databricks DB profile to point at your corporate/root CA PEM bundle.
- If you do not have the CA bundle yet, you can temporarily enable **Disable TLS certificate verification** in that DB profile. This is insecure and should only be a last resort for internal troubleshooting.

## Configuration

AMX stores its configuration at `~/.amx/config.yml`. To use a different file, start the CLI with `amx --config path/to/config.yml`.

## Local SQLite history

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
- `/search ask` now distinguishes table-level semantic discovery from inventory questions, so prompts like "which tables contain address details" route to ranked table matches instead of accidental table-count answers
- inventory/count questions such as schema lists or table counts use live DB introspection so they remain correct even if only part of the catalog has generated descriptions
- semantic questions use effective metadata first, with exact/fuzzy name matching, multilingual query variants, and vector support as an independent fallback when lexical terms do not match
- synthesized answers receive every retrieved row in the current result set, with result indexes, so AMX can answer across all returned candidates instead of only the first few matches
- table-scoped factual questions are live-first: questions such as "what is the ADRC table?" or "are all ADRC columns commented?" resolve the requested table and run safe live metadata probes before answering structural facts like column count, types, nullability, table comments, and column-comment coverage. Open-ended semantic column searches, such as "city related column names", stay on catalog/vector retrieval unless the user explicitly scopes them to a table.
- explicit table mentions such as `schema.table`, `ADRC table`, or `adrc tablosunda` take precedence over fuzzy catalog matches. If the exact live table cannot be verified, AMX refuses to substitute a similar candidate such as `ADR6`; fuzzy matches are shown only as suggestions.
- `/search` only labels an answer as live verified when live metadata rows were actually collected; catalog-only or fuzzy evidence is capped to lower confidence.
- join questions prioritize verified FK relationships, then semantic join inference, then observed code usage; one-table join questions can also surface non-FK semantic candidates with confidence bands such as `verified`, `high_likelihood`, `possible`, and `weak_hypothesis`
- join answers now pass the resolved base/target join columns into the synthesis prompt, so AMX can explain not just which tables are joinable but also which column pairs it found
- follow-up questions reuse short session memory so users can keep discussing the same table or field naturally
- `/search ask` shows live progress while AMX interprets the question, retrieves evidence, and synthesizes the answer
- `/search ask` records retrieval policy, evidence sources, ambiguity flags, per-stage timings, and action suggestions into history/event payloads so answers remain diagnosable
- `/search ask --actions` turns selected suggestions into a human-approved execution loop: AMX asks before running catalog sync, cached code-evidence refresh, or single-table metadata analysis actions, then records the action outcome
- `/search /context-detail` controls how much neighborhood, code, and history context is exposed to the search synthesizer for cost/latency tuning
- `/search` answer language is forced to the detected language of the user's question, even if the interpreter LLM suggests a different `answer_language`
- aggregate answers avoid dumping the generic schema/table/column result grid when that grid would be irrelevant to the user question
- if no active LLM profile exists, `/search ask` fails closed and tells you to configure `/llm`

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
│   └── inference.py     # Single-table metadata inference entrypoint used by approved search actions
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
│   └── code_agent.py   # Codebase analysis agent
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

Release notes for the latest versions also live in [`CHANGELOG.md`](CHANGELOG.md).

### v0.1.120

- **Databricks write-back fix**: Databricks table and column comment write-back now uses inline escaped SQL literals for `COMMENT` DDL, fixing failed `/run-apply` and metadata edit writes caused by `:cmt` parameter markers.
- **Connector cleanup**: `/connect` is back to the standard synchronous flow, and the Databricks profile wizard no longer silently forces insecure TLS defaults.

### v0.1.119

- **Databricks connect timeout**: `/connect` added a bounded timeout path for warehouses that appear stuck while starting.

### v0.1.118

- **Databricks TLS controls**: Databricks profiles now support a trusted CA bundle path and an optional insecure TLS bypass for corporate/self-signed certificate chains.
- **Clearer TLS failures**: `/connect` now classifies Databricks certificate verification problems explicitly instead of only echoing the raw SSL error.

### v0.1.117

- **Databricks connect guardrails**: `/connect` now prints a short progress line and uses bounded Databricks connector timeouts/retries, so a bad workspace host, HTTP path, or token fails quickly instead of looking stuck.

### v0.1.116

- **Databricks connect cleanup**: `/connect` no longer emits the Databricks SQL connector `_user_agent_entry` deprecation warning; AMX now passes the non-deprecated `user_agent_entry` connect arg.

### v0.1.115

- **Connector capabilities**: Database adapters now declare supported comment, relationship, materialized-view, statistics, and profiling behavior so unsupported write-back fails clearly.
- **Safer profiling**: Cloud backends avoid full scans when row-count statistics are unknown, and sampled mode uses backend sampling syntax for Snowflake, Databricks, and BigQuery.
- **Backend fixes**: Snowflake metadata commands are less fragile, Databricks catalog comment write-back no longer silently no-ops, and connector/apply flows no longer count unsupported writes as applied.

### v0.1.114

- **Column search scope guardrail**: Open-ended semantic column searches no longer get converted into live table snapshots from fuzzy matches or prior session memory.
- **Planner repair**: If the interpreter misclassifies a column-list query as table explanation, `/search` reroutes it back to semantic column discovery.

### v0.1.113

- **Live-first table facts in `/search`**: Table-understanding questions now run live metadata snapshots before answering structural facts.
- **No silent fuzzy substitution**: Explicit table names are not replaced by similar catalog candidates; unresolved targets are reported directly with candidates as suggestions only.
- **Provenance guardrails**: `/search` no longer claims live verification unless live metadata evidence was actually collected.

### v0.1.95

- **Flexible `/metadata edit` targets**: Edit database/schema/table/column comments with `<db>`, `<db>.<schema>`, `<db>.<schema>.<table>`, or `<db>.<schema>.<table>.<column>`.
- **Interactive edit wizard**: Missing or ambiguous targets now prompt for DB profile, target granularity, schema, table/view, and column as needed. Database-level edits do not force schema/table selection.
- **Cancellation**: Typing `exit`, `quit`, `q`, `cancel`, or pressing Ctrl+C cancels the wizard without writing metadata.

### v0.1.94

- **Metadata namespace**: `/metadata` is now the primary tab for database comment inspection/editing/coverage; `/manual` remains as a compatibility alias.
- **Guided `/edit`**: Bare `/edit` now explains the workflow: select DB profile, set schema/table context if needed, then choose a concrete database/schema/table/column target.
- **Softer target guidance**: Missing edit targets now show guidance warnings instead of red command errors.

### v0.1.93

- **Manual target safety**: `/manual edit schema` and `/manual edit table` now require explicit targets, so AMX will not silently edit the current schema/table when the user intended to type a target.
- **Qualified manual targets**: Manual edits accept dotted targets such as `/edit table sap_test.adr6`, `/edit column adr6.smtp_addr`, and `/edit column sap_test.adr6.smtp_addr`.
- **Manual terminology**: `/manual` help now states that it edits database metadata; document workflows remain under `/docs`.
- **Short DB errors**: Manual DB connection failures now show a concise cause summary instead of raw driver output.

### v0.1.89

- **Service layer**: Manual metadata logic and analyze scope/codebase-preparation logic now live under `amx.services`, reducing business logic inside the Click command modules.
- **Command thinness**: `amx.cli_support.commands.manual` and `amx.cli_support.commands.run` now mostly handle CLI wiring and defer to service functions.
- **Coverage**: Added direct regression coverage for the new service-layer helpers.

### v0.1.90

- **Manual session fix**: Inside `/manual`, incomplete slash commands like `/edit` now report the real missing-argument error instead of the misleading `Unknown command` message.

### v0.1.91

- **Manual edit guidance**: Inside `/manual`, typing bare `/edit` now prints the valid targets and concrete examples like `/edit database` and `/edit column vbeln`.

### v0.1.92

- **Manual error messages**: `/manual` inspect/edit/monitor now collapse database connection problems into a concise message with a concrete recovery step: check the active DB profile and run `/db` then `/connect`.
- **Manual cancellation**: Cancelling an interactive manual edit prompt now exits cleanly as `Manual edit cancelled.` instead of bubbling up as an empty command failure.

### v0.1.88

- **CLI package layout**: Extracted command modules now live under `amx.cli_support.commands`, and `amx.cli_support.root_commands` owns setup, config, and DB registration.
- **Bootstrap size**: `amx.cli` is now roughly 200 lines and is limited to entrypoint, session wiring, and module registration.
- **Compatibility**: Top-level `amx.cli_*` modules remain as re-export shims so existing imports do not break immediately.

### v0.1.87

- **Manual metadata tab**: Added a `/manual` namespace for direct database/schema/table/column comment inspection, editing, and coverage monitoring without running LLM agents.

### v0.1.86

- **Logprob calibration granularity**: AMX now calibrates confidence from the generated description text for each suggestion when response text and token logprobs are available, instead of assigning every parsed suggestion the same whole-response score.
- **Batch logprob propagation**: OpenAI Batch requests now include logprob parameters and parsed batch results preserve returned token logprobs for calibration before merge/review.

### v0.1.85

- **CLI maintainability**: The interactive shell now lives in `amx.cli_support.session`, and `amx.cli` is down to roughly 400 lines focused on entrypoint wiring and top-level commands.
- **Package structure**: Session helpers now live under `amx/cli_support/`, which is the first move away from putting every CLI module directly under `amx/`.
- **Regression coverage**: Added tests for slash-command shortcut translation and schema-default injection in the extracted session helpers.

### v0.1.84

- **CLI maintainability**: The `/analyze run` command flow now lives in `amx.cli_analyze_flow`, cutting `amx.cli` down to roughly 1.3k lines while isolating mode selection, profile switching, orchestration, and history finalization from the shell entrypoint.

## Programmatic API

Run AMX inference without entering the interactive CLI shell:

```python
from amx.config import AMXConfig
from amx.core import infer_table_metadata

cfg = AMXConfig.load()
results = infer_table_metadata(cfg, schema="sap_test", table="adr6")
print(results[0])
```

### v0.1.79

- **Persistence clarity**: `/run` wizard profile selections now persist immediately to `config.yml`, `/llm` setting commands print the exact LLM profile being updated, and config writes are now atomic for better crash safety.

### v0.1.78

- **OpenRouter support**: Added first-class `openrouter` provider wiring in setup/runtime with default `https://openrouter.ai/api/v1`, plus in-app model-name examples to reduce profile setup errors.

### v0.1.77

- **CLI maintainability**: `analyze` scope-resolution helpers and `/analyze apply` now live in `amx.cli_run`, and the repo now has real Click-level integration tests covering extracted CLI modules.

### v0.1.76

- **CLI maintainability**: `/history` commands, saved-result inspection, and review/apply flows now live in `amx.cli_history`, shrinking `amx.cli` further without changing the interactive command surface.

### v0.1.75

- **CLI maintainability**: `/db` profile and profiling commands now live in `amx.cli_db`, reducing the size of `amx.cli` while preserving the existing interactive commands.

### v0.1.74

- **Reliability cleanup**: AMX now keeps missed model outputs visible with low-confidence fallback review rows, treats confidence as low when calibrated logprobs are unavailable, keeps schema/database descriptions reviewable, and bounds long-run live display output.

### v0.1.73

- **Profiling guardrails**: DB profiles now support `full`, `sampled`, and `metadata` profiling modes through `/db` → `/profiling`, plus source-scoped document/code RAG filtering.

### v0.1.72

- **History alternatives UX**: `/results <run_id>` now shows all saved alternatives and reminds you to use `/review <run_id> --apply` to choose a different option and apply it later.

### v0.1.71

- **Realtime connection timer**: Database connection/test spinners now show continuously updating elapsed seconds while waiting.

### v0.1.70

- **History status fix**: `/history list` no longer gets stuck with completed runs shown as `running` due to missing finalization paths.

### v0.1.69

- **LLM batch size save fix**: `/llm-batch-size` now persists correctly in LLM profile config and is honored by `/run`.

### v0.1.68

- **Codebase `__none__` fix**: Selecting the disabled codebase profile during `/run` or `/run-apply` no longer errors.

### v0.1.67

- **`/run-apply` profile switch hotfix**: Fixed `DatabaseConnector` NameError when changing DB profile mid-run.
- **Cleaner failures**: `/run` no longer emits duplicate `Command failed` lines with debug traceback spill.

### v0.1.66

- **Graceful Ctrl+C during `/run`**: Interrupting prompts now exits with a clean user interruption message instead of Python traceback noise.

### v0.1.65

- **CLI startup hotfix**: Fixed a misindented exception handler in `amx/cli.py` so `amx` imports and `/run` startup behave normally again.

### v0.1.40

- **Prompt detail presets**: New `PromptDetail` system with four named levels — `minimal`, `standard`
  (default), `detailed`, `full` — controlling exactly which database context fields (samples,
  min/max, cardinality, usage stats, FK comments, RAG chunk counts, etc.) are included in each
  LLM prompt. **Nothing is removed**; all fields remain accessible via the `full` or `detailed`
  presets. Switch with `/llm` → `/prompt-detail <level>`. Settings are saved per LLM profile.
- **Configurable alternatives count**: `n_alternatives` (1–5) controls how many description
  alternatives the LLM generates per column. Fewer alternatives = fewer output tokens = lower
  cost at review time. Set with `/llm` → `/n-alternatives <N>`.
- **`max_tokens` default** lowered from 16384 to 4096 (reasoning models still auto-raise).

### v0.1.39

- **Persistent LLM alternatives**: Every set of LLM-generated descriptions is now saved to
  `~/.amx/history.db` (`run_results` table) before the user evaluates it, keyed by run ID and
  timestamp. Multiple runs over the same table or different assets are all tracked independently.
- **`/history results <run_id>`**: Display all saved alternatives for a past run — confidence,
  source, evaluation status, chosen description, and evaluation timestamp.
- **`/history review <run_id>`**: Interactively re-evaluate any past run's alternatives.
  Use `--unevaluated-only` to skip already-decided rows, and `--apply` to write approved
  descriptions directly to the database. Evaluation decisions are recorded in SQLite with timestamps.

### v0.1.38

- **LiteLLM import stability**: LiteLLM is imported only when the first LLM call runs (avoids `litellm_core_utils` circular-import failures under Python 3.12+ / pipx). Minimum LiteLLM version is now **1.83.7**. If you use pipx: `pipx upgrade amx` (or `pipx inject amx 'litellm>=1.83.7'` on an older install).
- **SQLite history backend**: AMX now auto-creates `~/.amx/history.db` and persists run metadata, performance, token usage, results, and app events. New `/history` namespace lets users query this data without leaving AMX.

### v0.1.37

- **All database drivers in one install**: `pip install amx` now includes Snowflake, Databricks, and BigQuery SQLAlchemy stacks alongside PostgreSQL — no optional `[snowflake]` / `[bigquery]` extras.
- **`/db` namespace UX**: Entering `/db` prints a short hint listing supported engines and how to use `/db-profiles`, `/use-db`, and `/add-db-profile`. `/use-db` without a name shows an interactive list with `[backend] connection summary` per profile. `/add-db-profile` and setup wizard use `SUPPORTED_BACKENDS` with one-line descriptions for each engine.
- **Docs**: README updated for multi-backend installation and configuration; new top-level `CHANGELOG.md` for version history.

### v0.1.36

- **Three-level scope picker**: `/run` now offers Database, Schema, or Asset scope — run metadata inference across an entire database, selected schemas, or specific assets in a single command.
- **Views and materialized views**: AMX now discovers and profiles views and materialized views alongside tables. The `/tables` command shows all asset types with kind labels. `COMMENT ON VIEW` and `COMMENT ON MATERIALIZED VIEW` are used when writing metadata back.
- **`AssetKind` enum**: New `AssetKind` (TABLE, VIEW, MATERIALIZED_VIEW) in `connector.py` propagates through profiling, agent context, review results, and pending metadata serialization for correct SQL comment syntax.
- **Multi-schema processing**: A single `/run` invocation can now iterate over multiple schemas when Database or Schema scope is selected — one orchestrator per schema with shared codebase and RAG context.
- **Asset-kind-aware orchestrator**: `process_table`, `process_tables_batch_mode`, and `apply_review_results_to_db` all accept and propagate asset kind for correct COMMENT ON syntax and display labels.

### v0.1.35

- **DB command simplification**: Removed `/c` alias from `/db` namespace. `/connect` is now the single connectivity command in command catalog, help text, and interactive shortcut routing.
- **Docs alignment fix**: Updated architecture ASCII block formatting so labels align cleanly on the same lines in monospace terminals.

### v0.1.34

- **Live display during agent runs**: New `LiveDisplay` using `rich.Live` renders a persistent header (AMX version, provider, model, schema, mode, elapsed time, token counter), a hierarchical activity tree (● done, ✦ active, ○ pending), a thinking indicator with live timer, and a context-aware footer — all updating in-place without flooding scroll history.
- **Persistent session footer**: Interactive prompt now shows a dynamic status bar with current namespace, schema/table context, LLM profile, and keyboard shortcuts.
- **Non-blocking UI**: All terminal output during agent runs uses in-place updates via `rich.Live`. The display pauses automatically for human-review prompts and resumes after.

### v0.1.33

- **Logprob-calibrated confidence**: Confidence levels (HIGH/MEDIUM/LOW) are now derived from actual token probabilities (`logprobs`) instead of trusting the model's self-declared label. The calibration thresholds: p > 0.85 → HIGH, p > 0.50 → MEDIUM, p ≤ 0.50 → LOW. Works automatically on providers that support logprobs.
- **Batch API mode**: New `--mode batch` flag on `/run` submits all LLM requests as a single asynchronous batch job at ~50% cost reduction. Provider-agnostic architecture with strategy pattern — currently supports **OpenAI** and **Anthropic** batch APIs. User selects between "Chat Completions" (real-time) and "Batch" (async, cheaper) at run start.
- **tiktoken token counting**: Replaced the `chars // 4` heuristic with proper tiktoken (cl100k_base) encoding for accurate token estimation.
- **Provider-agnostic design**: Removed all hardcoded provider lists. Batch support and logprob capability are detected dynamically via a provider registry and litellm introspection.

### v0.1.30

- **Batched LLM calls**: RAG, Code, and Merge agents now process all columns in a single LLM call per table instead of one call per column (~80-90% cost reduction).
- **Progress and token tracking**: Spinners, progress bars, and per-step token usage summary displayed during scans and agent runs.
- **Table name normalization**: Codebase scanner now deduplicates fully-qualified (`schema.table`) and bare table names against the catalog.
- **Input validation**: Interactive table selection validates against the database with similarity hints for typos.

## License

MIT
