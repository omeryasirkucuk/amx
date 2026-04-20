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
- Write approved metadata back to PostgreSQL as `COMMENT ON TABLE/COLUMN` (write-back support)

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
- Docker (for the demo PostgreSQL database)
- An LLM API key (OpenAI, Anthropic, Gemini, DeepSeek, or a local model)

### Installation

```bash
git clone https://github.com/omeryasirkucuk/amx.git
cd amx
pip install -e .
```

### Start the Demo Database

```bash
docker-compose up -d
```

AMX focuses on **metadata inference**, not bulk data loading. Load CSVs or restore dumps with your own tooling (for example `psql` `COPY`, `pgloader`, or ETL jobs), then point AMX at the populated database with `amx setup`.

### Configure AMX

```bash
amx setup
```

This interactive wizard walks you through:
1. **Database connection** — PostgreSQL host, port, credentials
2. **AI model** — provider and API key (supports OpenAI, Anthropic, Gemini, DeepSeek, Ollama, local endpoints)
3. **Data sources** — optional named **document** and **codebase** profiles for RAG and code scanning

In an interactive `amx` session you can manage several **DB connections**, **LLM configs**, **document roots**, and **codebases**, then switch the active profile with commands such as `/use`, `/use-llm`, `/use-doc`, and `/use-code`. See `/help` in the shell for the full list.

### Run Analysis

AMX is interactive-first: start with `amx`, then run slash commands.

```bash
# Start AMX session
amx

# Inside AMX (slash commands)
/db
/connect
/schema sap_s6p
/analyze
/run --table t001 --table vbak
/run-apply
/apply
```

## Interactive Commands (inside `amx` session)

| Command | Description |
|---------|-------------|
| `/setup` | Interactive first-time configuration wizard |
| `/config` | Display current configuration |
| `/db` + `/connect` | Test database connectivity |
| `/db` + `/schemas` | List available schemas |
| `/db` + `/tables [schema]` | List tables in a schema |
| `/db` + `/profile [schema] [table]` | Profile table structure and data |
| `/docs` + `/scan [paths...]` | Scan and preview documents for RAG |
| `/docs` + `/ingest [paths...]` | Ingest documents into the RAG vector store |
| `/docs` + `/query <question>` | Query the document store |
| `/analyze` + `/run` | Run all agents and review suggestions |
| `/analyze` + `/run-apply` | Run analysis and apply approved metadata immediately |
| `/analyze` + `/apply` | Write pending approved metadata back to PostgreSQL (`COMMENT ON ...`) |
| `/analyze` + `/codebase <path> [--schema NAME]` | Scan a codebase for asset references (schema defaults from session context) |

## Supported Document Sources

| Source | Path Format | Status |
|--------|-------------|--------|
| Local files/directories | `/path/to/docs` | Supported |
| GitHub repositories | `https://github.com/user/repo` or `git@github.com:user/repo.git` | Supported |
| AWS S3 | `s3://bucket/prefix` | Supported |
| Google Drive links | `https://drive.google.com/...` | Not supported yet |
| SharePoint / OneDrive links | `https://...sharepoint.com/...` | Not supported yet |

### Supported Document File Types

AMX scans/ingests these extensions:

`pdf`, `docx`, `doc`, `txt`, `md`, `csv`, `xlsx`, `xls`, `html`, `htm`, `pptx`, `json`, `yaml`, `yml`, `rst`, `rtf`

## Supported LLM Providers

| Provider | Config Value | Notes |
|----------|-------------|-------|
| OpenAI | `openai` | GPT-4o, GPT-4, etc. |
| Anthropic | `anthropic` | Claude Sonnet, Opus, etc. |
| Google Gemini | `gemini` | Gemini 2.0 Flash, Pro, etc. |
| DeepSeek | `deepseek` | DeepSeek Chat |
| Ollama | `ollama` | Local models via Ollama |
| Any OpenAI-compatible | `local` | Custom API base URL |

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
- Usage stats (`pg_stat_user_tables`): `seq_scan`, `idx_scan`, `n_live_tup`
- Related metadata: existing comments on FK-related neighbor tables
- Per-column profile:
  - name, type, nullable
  - null count, distinct count, cardinality ratio (`distinct_count / row_count`)
  - min/max value (as text)
  - up to 5 distinct non-null sample values
  - existing column comment

AMX does not send full table dumps; it sends summarized profiling signals and small samples for inference.

## Configuration

AMX stores its configuration at `~/.amx/config.yml`. You can also pass `--config path/to/config.yml` to any command.

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
│   └── analyzer.py     # Codebase reference analysis
├── llm/
│   └── provider.py     # Unified LLM interface via LiteLLM
└── utils/
    ├── console.py      # Rich console helpers
    └── logging.py      # Structured logging
```

## License

MIT
