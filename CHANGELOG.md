# Changelog

All notable changes to this project are documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.48] — 2026-04-25

### Added
- **High-Level Metadata**: Support for inferring and applying descriptions for Database Schemas and the Database itself.
- **Review Strategies**: Choice between `individual` (real-time) and `deferred` (batch) human review at the start of `/run`.
- **Local/Ollama Improvements**: API keys are now optional in the setup wizard for local providers, and `api_base` is correctly propagated for Ollama.
- **Logprob Configuration**: User-configurable thresholds for HIGH/MEDIUM confidence levels via `/llm /logprob-thresholds`.

## [0.1.47] — 2026-04-25

### Fixed
- **Ctrl+C incorrectly logged as `failed`**: A `KeyboardInterrupt` is now caught separately from `Exception`. If results were already produced before the interrupt, the run is saved as **`cancelled`** (yellow in `/list`). If interrupted mid-processing with no results, it is still `cancelled` (not `failed`).
- **`/list` status color-coding**: Status column now renders in distinct colors — `success` in green, `failed` in red, `cancelled` in yellow, `running` in cyan.

## [0.1.46] — 2026-04-25

### Fixed
- **`/list` showing stale "running" status with 0.00s duration**: Runs that crashed, were interrupted by Ctrl+C, or exited through the human review prompt could leave the SQLite `analysis_runs` row frozen at `status=running`. Fixed by consolidating both the success and failure `finish_run()` calls into a single `finally:` block that uses `sys.exc_info()` to detect the execution path.
- **`/history results` missing table description**: Top-level (`column=None`) results were blended into the flat column table with just `(table)` as the label. They are now shown in a prominent **cyan Panel** at the top listing all alternatives with their chosen description highlighted in green.
- **`/history review` not surfacing table description first**: Review items are now sorted so `column=None` (table/schema/db) entries are always processed first, and they display with a `▶ TABLE DESCRIPTION` cyan banner instead of the standard column heading.

## [0.1.45] — 2026-04-25

### Fixed
- **Table-level description persistence**: `/history review` was not storing the table's own description — only column descriptions were saved. Root causes were:
  1. The system prompt only asked for a single `TABLE_DESCRIPTION:` line while columns got multiple `DESCRIPTION_N:` alternatives.
  2. The parser consumed it as a single-element list, losing the multi-alternative format entirely.
  3. For tables with >10 columns (multiple batches), each batch generated a separate table-level suggestion causing duplicates, but only the first was retained without deduplication logic.
- **All three parsers fixed** (`_parse_response`, `_parse_response_loose`, system prompt) to:
  - Emit `TABLE_DESCRIPTION_1/2/3:` alternatives matching `n_alternatives`.
  - Collect all alternatives into a single `MetadataSuggestion(column=None, suggestions=[...])` with all options.
  - Deduplicate table-level entries across batches, keeping only the first (most complete) one.

## [0.1.44] — 2026-04-25

### UI Refinements
- **High-Density Minimal Spinner**: Replaced the custom 2x2 bulky Table spinner with a single-character high-density Braille spinner (`⢹⢺⢼⣸⣇⡧⡏⡟`). This perfectly matches the footprint of the `[green]●[/green]` success state, creating a much cleaner, tighter, and tighter square border animation for active tasks.

## [0.1.43] — 2026-04-25

### Refactored
- **Live UI Engine**: Completely refactored the `LiveDisplay` rendering loop. Instead of blocking synchronously and manually pushing frame updates to `rich.live.Live`, the class now implements a native `__rich_console__` method. This allows `Live` to automatically poll and redraw the entire UI 10 times a second in an asynchronous, non-blocking background thread.
- **Dynamic Clock & Animations**: The elapsed time counters across all pipelines (activities, thinking states, global execution time) and the new Braille trail spinner now natively update completely smoothly in real-time as a result of the background refresh loop.

## [0.1.42] — 2026-04-25

### Fixed
- **SQLite Persistence**: Fixed an issue where metadata review options were silently failing to write to the SQLite history store because the `AssetKind` Enum wasn't properly serialized to a string. `amx /review <id>` will now correctly retrieve run data.

### Added
- **UI Enhancements**:
  - Replaced the static active indicator with an animated Braille spinner (`⠋`) in the `LiveDisplay` pipeline tree to visually convey real-time concurrent agent processing.
  - Added elapsed time console output showing the total duration for the "Agent processing" phase (before review) and the "Human review" phase (after review).

## [0.1.41] — 2026-04-25

### Added
- **Keyboard Navigation**: Implemented Left/Right arrow key navigation for switching tabs seamlessly in the TUI when the input buffer is empty.
- **Persistent Header (App UI)**: Emulated Claude Code CLI's persistent header by anchoring the AMX banner and namespace hints to the top of the terminal. Switching tabs now clears the screen and immediately re-renders the header and context hint.
- **Target Scope display**: Updated `/history list` to include a new **Target Scope** column which dynamically parses the saved JSON scope to show exactly what schemas/tables were analyzed (e.g., `sap.vbrk`, `3 schemas (120 tables)`).

### Changed
- **Parallel Profile Execution**: Completely rewrote the `ProfileAgent` execution loop. Instead of processing batches of 10 columns sequentially, wide tables are now split into batches and processed **concurrently** using a `ThreadPoolExecutor`, dramatically speeding up analysis for large tables.
- **Zero-Delay Start**: Moved the synchronous Database connection test in `/run` down past the Completion Mode prompt. The UI now appears instantly when `/run` is executed, instead of blocking for 1-2 seconds on the network check.
- **Center Alignment**: Fixed the `show_banner` UI so the title and subtitle strings are perfectly center-aligned relative to the ASCII art logo.

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
