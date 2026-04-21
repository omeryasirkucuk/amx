# AMX — Agentic Metadata Extractor

AI-powered CLI application that automatically infers, reviews, and applies metadata (descriptions, tags) to database assets using a multi-agent system with human-in-the-loop validation.

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
- Write approved metadata back to the database as `COMMENT ON TABLE/COLUMN` (write-back support)

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     AMX CLI (Click)                      │
├─────────────────────────────────────────────────────────┤
│                    Orchestrator Agent                    │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐ │
│  │ Profile Agent │ │  RAG Agent   │ │   Code Agent     │ │
│  │ (DB stats)   │ │ (Documents)  │ │ (Codebase scan)  │ │
│  └──────┬───────┘ └──────┬───────┘ └────────┬─────────┘ │
│         └────────────────┼──────────────────┘           │
│                    Merge & Rank                          │
│                          │                               │
│              Human-in-the-Loop Review                    │
│                          │                               │
│                 Apply to Database                         │
├─────────────────────────────────────────────────────────┤
│  LLM Provider (OpenAI / Anthropic / Gemini / Local / …) │
└─────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.10+
- A database you can connect to (PostgreSQL supported; more engines planned)
- Access to at least one LLM provider you plan to configure (API key, local endpoint, etc.)

### Installation

```bash
git clone https://github.com/omeryasirkucuk/amx.git
cd amx
pip install -e .
# Optional: richer SQL parsing in codebase scans (sqlglot)
pip install -e ".[code-intel]"
```

AMX focuses on **metadata inference**, not bulk data loading. Populate schemas and tables with your own import or ETL process, then use AMX against that database.

### Configure AMX

```bash
amx setup
```

This interactive wizard walks you through:
1. **Database connection** — host, port, credentials
2. **AI model** — provider and API key (see [Supported LLM Providers](#supported-llm-providers))
3. **Data sources** — optional named **document** and **codebase** profiles for RAG and code scanning

In an interactive `amx` session, configuration is grouped by namespace:

- `/db` — database profiles + introspection (`/db-profiles`, `/add-db-profile`, `/connect`, …)
- `/docs` — document roots + RAG (`/doc-profiles`, `/add-doc-profile`, `/ingest`, `/search-docs`)
- `/llm` — LLM profiles (`/llm-profiles`, `/add-llm-profile`, …)
- `/code` — codebase profiles (`/code-profiles`, `/add-code-profile`, …)

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
| `/config` | Display current configuration |
| `/db` + `/db-profiles` | List DB connection profiles |
| `/db` + `/use-db <name>` | Switch active DB profile |
| `/db` + `/add-db-profile [name]` | Add/update a DB profile (interactive) |
| `/db` + `/remove-db-profile <name>` | Remove a DB profile |
| `/db` + `/schema <name>` | Set default schema context (used by /tables, /analyze, …) |
| `/db` + `/table <name>` | Set default table context (used by /profile, /analyze, …) |
| `/db` + `/connect` | Test database connectivity |
| `/db` + `/schemas` | List available schemas |
| `/db` + `/tables [schema]` | List tables in a schema |
| `/db` + `/profile [schema] [table]` | Profile table structure and data |
| `/llm` + `/llm-profiles` | List LLM profiles |
| `/llm` + `/use-llm <name>` | Switch active LLM profile |
| `/llm` + `/add-llm-profile [name]` | Add/update an LLM profile (interactive) |
| `/llm` + `/remove-llm-profile <name>` | Remove an LLM profile |
| `/code` + `/code-profiles` | List codebase profiles |
| `/code` + `/use-code <name>` | Switch active codebase profile |
| `/code` + `/add-code-profile [name]` | Add/update a codebase path (interactive) |
| `/code` + `/remove-code-profile <name>` | Remove a codebase profile |
| `/code` + `/code-scan [path]` | Scan codebase, save results + build `amx_code` semantic index. `--code-profile NAME` |
| `/code` + `/code-refresh` | Clear scan cache and reset `amx_code` Chroma |
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
| `/analyze` + `/run [TABLE …]` | Run all agents; tables as args or `--table`; `--code-profile`, `--code-refresh` |
| `/analyze` + `/run-apply [TABLE …]` | Same as `/run --apply` |
| `/analyze` + `/apply` | Write pending approved metadata to the database |

## Codebase and document intelligence

- **Profiles without switching context**: use `--code-profile` / `--doc-profile` on CLI commands (or the same flags after `/code-scan`, `/ingest`, `/run` in session) instead of `/use-code` / `/use-doc` first.
- **Code scan cache**: `~/.amx/code_cache/<slug>/` stores a manifest plus serialized scan results so `/run` does not re-walk the repo every time. Use **`--code-refresh`** or **`/code-refresh`** after the tree changes or when you want a clean semantic index.
- **Semantic code RAG**: Chroma collection **`amx_code`** holds embedded chunks (Python by function/class span; other languages by text split). The Code Agent combines regex-style hits with a few nearest-neighbor chunks. This is **assistive**, not a proof of dataflow—wide schemas use **capped** table/column lists for performance.
- **Identifiers outside the DB**: strings that look like catalog objects but are not in the connected table list appear as **secondary context** for the LLM (for example external lake tables).
- **Doc RAG refresh**: **`/ingest --refresh`** removes existing chunks whose stored `source` path matches the files you are ingesting, then re-upserts—useful when files shrink or move.

## Supported Document Sources

When you add paths with `/add-doc-profile` or during `/setup`, AMX checks **reachability only** (for example `git ls-remote` for GitHub, bucket/prefix checks for S3, lightweight HTTP checks for Drive/SharePoint). Full file discovery happens on `/scan` and `/ingest`.

| Source | Path Format | Status |
|--------|-------------|--------|
| Local files/directories | `/path/to/docs` | Supported |
| GitHub repositories | `https://github.com/user/repo` or `git@github.com:user/repo.git` | Supported |
| AWS S3 | `s3://bucket/prefix` | Supported |
| Google Drive links | `https://drive.google.com/...` | Supported — public links work with zero setup; private files need credentials (see below) |
| SharePoint / OneDrive links | `https://...sharepoint.com/...` or `https://onedrive.live.com/...` | Supported — public sharing links work with zero setup; private files need Azure credentials (see below) |

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

## Supported LLM Providers

| Provider | Config value |
|----------|--------------|
| OpenAI | `openai` |
| Anthropic | `anthropic` |
| Google Gemini | `gemini` |
| DeepSeek | `deepseek` |
| Ollama | `ollama` |
| OpenAI-compatible (custom base URL) | `local` |

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

## Configuration

AMX stores its configuration at `~/.amx/config.yml`. To use a different file, start the CLI with `amx --config path/to/config.yml`.

## Project Structure

```
amx/
├── cli.py              # Click-based CLI entry point
├── config.py           # Configuration management
├── agents/
│   ├── base.py         # Base agent types and shared data structures
│   ├── orchestrator.py # Multi-agent coordination + human-in-the-loop
│   ├── profile_agent.py # Database profiling agent
│   ├── rag_agent.py    # Document RAG agent
│   └── code_agent.py   # Codebase analysis agent
├── db/
│   └── connector.py    # Database introspection and metadata I/O
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
└── utils/
    ├── console.py      # Rich console helpers
    ├── token_tracker.py # tiktoken-based token counting and usage tracking
    └── logging.py      # Structured logging
```

## Changelog

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
