# Changelog

All notable changes to this project are documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.40] — 2026-04-25

### Added

- **Prompt detail presets** (`PromptDetail` dataclass + `prompt_detail_for(level)` in `config.py`):
  Four named presets — `minimal`, `standard` (new default), `detailed`, `full` — control exactly
  which database context fields are sent to the LLM in every prompt. Fields include:
  sample values, null counts, min/max, cardinality ratio, PK/FK keys, unique/check constraints,
  usage stats (pg_stat), schema/DB comments, FK neighbour comments, and RAG chunk counts.
  Nothing is removed — any field can be re-enabled by switching presets.
- **`/llm` → `/prompt-detail [level]`**: Shows a comparison table of all four presets and their
  flags. When given a level name, sets and saves it to the active LLM profile.
- **Configurable alternatives count** (`n_alternatives: int` in `LLMConfig`, default 3, range 1–5):
  The number of description alternatives the LLM is asked to generate per column. Fewer
  alternatives = fewer output tokens = lower cost. Stored in `~/.amx/config.yml`.
- **`/llm` → `/n-alternatives [N]`**: Shows current value or sets it with a plain integer.
- **All three agents** (`ProfileAgent`, `RAGAgent`, `CodeAgent`) now build their system prompts
  dynamically based on `n_alternatives`, so the prompt template always matches what is requested.
- **`max_tokens` default lowered** from 16384 to 4096 in `LLMConfig`.
  Reasoning models (gpt-5, o-series) still auto-raise to 16384 as before.

## [0.1.39] — 2026-04-24

### Added

- **Persistent LLM alternatives** (`run_results` table in `~/.amx/history.db`):  
  Every set of LLM-generated descriptions is now saved to SQLite before the user evaluates it,
  keyed by `run_id` + timestamp. Multiple runs over the same table are fully tracked.
- **`/history results <run_id>`**: Tabular view of all saved alternatives for any past run,
  including confidence, source, evaluation status, chosen description, and evaluation timestamp.
- **`/history review <run_id>`**: Interactive re-evaluation of past run alternatives.
  Supports `--unevaluated-only` (skip already-evaluated rows) and `--apply` (write
  approved comments to the database immediately). Evaluation decisions (accepted / skipped /
  custom) are recorded back into SQLite with a timestamp.
- The `Orchestrator` now accepts a `run_id` parameter; it persists all merged suggestions
  before calling `_human_review`, and records each evaluation decision as it is made.

## [0.1.38] — 2026-04-24

### Added

- **SQLite history backend** (`~/.amx/history.db`) auto-initialized on startup.
- New **`/history` namespace** with query commands:
  - `/history list`
  - `/history show <run_id>`
  - `/history stats`
  - `/history events`
- App event tracking for key actions (profile switches, analyze run status, apply outcomes).

### Fixed

- **LiteLLM circular import** (`partially initialized module 'litellm' … litellm_core_utils`): defer importing LiteLLM until the first LLM call, and require **LiteLLM ≥ 1.83.7** so older broken releases are not installed.

## [0.1.37] — 2026-04-24

### Changed

- **Single install for all SQL backends**: Core package dependencies now include Snowflake, Databricks, and BigQuery SQLAlchemy connectors (in addition to PostgreSQL). `pip install amx` is sufficient for every supported engine; optional `[snowflake]`, `[databricks]`, `[bigquery]`, and `[all-backends]` extras were removed.
- **Interactive `/db` workflow**: Entering `/db` shows which engines are supported and how to list profiles, switch the active profile, or add a new one. `/use-db` (no argument) lists each saved profile with `[backend]` and a connection summary. `/add-db-profile` and the setup wizard describe each engine when you pick PostgreSQL, Snowflake, Databricks, or BigQuery.

### Documentation

- README: installation from PyPI, multi-backend table, `db/adapters` in project layout, changelog pointer.
- This file added for release-focused notes; older entries remain in the README “Changelog” section.

## [0.1.36] and earlier

See the **Changelog** section in [README.md](README.md).

[0.1.40]: https://github.com/omeryasirkucuk/amx/compare/v0.1.39...v0.1.40
[0.1.39]: https://github.com/omeryasirkucuk/amx/compare/v0.1.38...v0.1.39
[0.1.38]: https://github.com/omeryasirkucuk/amx/compare/v0.1.37...v0.1.38
[0.1.37]: https://github.com/omeryasirkucuk/amx/compare/v0.1.36...v0.1.37
