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

```bash
# Interactive schema/table selection
amx analyze run

# Target specific schema and tables
amx analyze run --schema sap_s6p --table t001 --table vbak --apply
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `amx setup` | Interactive first-time configuration wizard |
| `amx config` | Display current configuration |
| `amx db connect` | Test database connectivity |
| `amx db schemas` | List available schemas |
| `amx db tables <schema>` | List tables in a schema |
| `amx db profile <schema> <table>` | Profile table structure and data |
| `amx docs scan [paths...]` | Scan and preview documents for RAG |
| `amx docs ingest [paths...]` | Ingest documents into the RAG vector store |
| `amx docs query <question>` | Query the document store |
| `amx analyze run` | Run all agents and review suggestions |
| `amx analyze codebase <path> [--schema NAME]` | Scan a codebase for asset references (schema defaults from config session) |

## Supported Document Sources

| Source | Path Format |
|--------|------------|
| Local files/directories | `/path/to/docs` |
| GitHub repositories | `https://github.com/user/repo` |
| AWS S3 | `s3://bucket/prefix` |

Supported file types: PDF, DOCX, TXT, Markdown, CSV, Excel, HTML, PPTX, JSON, YAML, RST.

## Supported LLM Providers

| Provider | Config Value | Notes |
|----------|-------------|-------|
| OpenAI | `openai` | GPT-4o, GPT-4, etc. |
| Anthropic | `anthropic` | Claude Sonnet, Opus, etc. |
| Google Gemini | `gemini` | Gemini 2.0 Flash, Pro, etc. |
| DeepSeek | `deepseek` | DeepSeek Chat |
| Ollama | `ollama` | Local models via Ollama |
| Any OpenAI-compatible | `local` | Custom API base URL |

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
