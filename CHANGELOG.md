# Changelog

All notable changes to the AMX project are documented in this file.

## [0.1.1] — 2026-04-28

### Changed
- **Interactive session UX** (`amx/cli.py`): increased contrast for prompt-toolkit completion menu text (meta descriptions) so `/` autocomplete remains readable on dark gray menus.
- **Changelog policy**: `CHANGELOG.md` is tracked in git again (removed from `.gitignore`) so release notes ship with the repository.

## [0.1.0] — 2026-04-19

### Added
- **Project scaffolding**: `pyproject.toml`, package structure, `.gitignore`, `docker-compose.yml`.
- **CLI framework** (`amx/cli.py`): Click-based CLI with command groups for `setup`, `db`, `docs`, and `analyze`.
- **Interactive session mode** (`amx/cli.py`): running `amx` without subcommands starts a persistent slash-command shell.
- **Slash autocomplete** (`amx/cli.py`): `/` command discovery via prompt-toolkit completions.
- **Contextual `/help`**: help text adapts to the active namespace (`/db`, `/docs`, `/analyze`).
- **Multi DB connection profiles** (`amx/config.py`, `amx/cli.py`): named PostgreSQL profiles stored in `~/.amx/config.yml` with session commands `/profiles`, `/use`, `/add-profile`, `/remove-profile`, `/save`, plus `/schema` and `/table` defaults.
- **Terminal branding** (`amx/utils/console.py`): neon-style startup banner.
- **Setup wizard** (`amx setup`): Interactive configuration for database, LLM provider, document paths, and codebase paths.
- **Database connector** (`amx/db/connector.py`):
  - Connection testing, schema listing, table listing.
  - Full table profiling: column types, null counts, distinct counts, min/max, sample data.
  - Read and write table/column comments (PostgreSQL `COMMENT ON`).
- **CSV bulk loader** (`amx/db/loader.py`):
  - Loads CSV files into a PostgreSQL schema using fast `COPY` protocol.
  - Loaded 78 SAP S/4HANA sample tables into `sap_s6p` schema under the `SAP` database.
- **LLM provider** (`amx/llm/provider.py`):
  - Unified interface via LiteLLM supporting OpenAI, Anthropic, Gemini, DeepSeek, Ollama, and custom local endpoints.
  - Configurable temperature, max tokens, API base URL.
- **Document scanner** (`amx/docs/scanner.py`):
  - Multi-source scanning: local files, GitHub repos, S3 buckets.
  - Supports 15+ file formats (PDF, DOCX, TXT, Markdown, Excel, HTML, PPTX, etc.).
  - Size estimation with user approval for large document sets.
- **RAG pipeline** (`amx/docs/rag.py`):
  - ChromaDB-backed vector store with persistent storage.
  - Recursive text splitting with configurable chunk size/overlap.
  - Format-aware document loaders (PDF, DOCX, CSV, Excel, HTML, Markdown, etc.).
- **Codebase analyzer** (`amx/codebase/analyzer.py`):
  - Scans local directories or clones GitHub repos.
  - Regex-based matching of table/column names across 15+ code file types.
  - Extracts surrounding context lines for each reference.
- **Agent system** (`amx/agents/`):
  - **Profile Agent**: Infers metadata from database statistics and column naming patterns.
  - **RAG Agent**: Enriches metadata using document context from the vector store.
  - **Code Agent**: Analyzes how assets are used in application code.
  - **Orchestrator**: Coordinates sub-agents, merges multi-source suggestions via LLM, drives human-in-the-loop review.
- **Human-in-the-loop review**:
  - One-by-one review with multiple choice options.
  - Bulk accept modes: accept-all, accept-all-high-confidence, reject-all.
  - Custom description input option ("Other").
  - Skip individual items.
- **Configuration** (`amx/config.py`):
  - YAML-based persistent config at `~/.amx/config.yml`.
  - Dataclass-based config objects for DB, LLM, and source paths.
- **Rich console UI** (`amx/utils/console.py`):
  - Color-coded output (info, success, warning, error).
  - Interactive prompts with autocompletion.
  - Formatted tables for data display.
- **Structured logging** (`amx/utils/logging.py`):
  - File-based debug logging at `~/.amx/logs/amx.log`.
  - Console-level warning/error output.
- **Docker setup**: `docker-compose.yml` with PostgreSQL 16 container and persistent volume.
- **README.md**: Full project documentation with architecture diagram, quick start guide, and CLI reference.
