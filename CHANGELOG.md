# Changelog

All notable changes to this project are documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.83] — 2026-04-27
### Changed
- **CLI maintainability**: Extracted the `/code` namespace commands from `amx/cli.py` into `amx/cli_code.py`, reducing `amx/cli.py` to roughly 1.8k lines while preserving scan, refresh, results, report export, and standalone Code Agent analysis behavior.

### Added
- **Integration coverage**: Added Click-level tests for `/code results` empty-cache guidance, `/code refresh` cache invalidation wiring, and `/code analyze` missing-cache guardrails.

## [0.1.82] — 2026-04-27
### Changed
- **CLI maintainability**: Extracted the `/docs` namespace commands from `amx/cli.py` into `amx/cli_docs.py`, reducing `amx/cli.py` to roughly 2.26k lines while preserving scan, ingest, search, export, and standalone RAG analysis behavior.

### Added
- **Integration coverage**: Added Click-level tests for `/docs scan` empty-path guidance and `/docs search-docs` command wiring.

## [0.1.81] — 2026-04-27
### Changed
- **CLI maintainability**: Extracted interactive LLM, document-profile, and codebase-profile helper commands from `amx/cli.py` into `amx/cli_profiles.py`, reducing `amx/cli.py` to roughly 2.5k lines.

### Added
- **Regression coverage**: Added helper coverage for OpenRouter default-model selection and document-profile disable aliases.

## [0.1.80] — 2026-04-26
### Added
- **Importable core API**: Added `amx.core.infer_table_metadata(...)` so metadata inference can run from Python scripts without entering the CLI shell.

### Changed
- **Confidence calibration math**: Logprob confidence now uses a weighted geometric mean over value-bearing tokens instead of relying on a single confidence-label token.
- **Truncation guard**: LLM responses with `finish_reason=length` now fail fast with an explicit truncation error, preventing partial JSON from reaching metadata extractors.
- **Write-through config**: Added `write_through_config` (default `true`) and autosave hooks so profile switches and profile CRUD operations persist immediately.

### Fixed
- **PostgreSQL profiling resilience**: Added actionable remediation hints for common Postgres profiling failures and non-blocking skip behavior for failed assets/columns during `/run`.

## [0.1.79] — 2026-04-26
### Changed
- **Immediate config persistence**: `/run` wizard profile selections (DB/LLM/docs/code) are now saved to `~/.amx/config.yml` immediately after selection instead of remaining in-memory for the current session only.
- **Explicit profile-scoped feedback**: `/llm` setting commands now report which active LLM profile was updated (prompt detail, alternatives, batch size, batch context columns, logprob thresholds).

### Fixed
- **Crash-safe config writes**: `AMXConfig.save()` now uses an atomic temp-file + `os.replace(...)` write path to reduce config corruption/state-loss risk during abrupt interruptions.

## [0.1.78] — 2026-04-26
### Added
- **OpenRouter provider support**: Added `openrouter` as a first-class LLM provider in setup/profile flows with default API base `https://openrouter.ai/api/v1`.

### Changed
- **Provider wiring**: LiteLLM model-prefix and environment mapping now include OpenRouter (`openrouter/...`, `OPENROUTER_API_KEY`) and pass configured `api_base` for OpenRouter requests.
- **Documentation**: Updated README supported-provider matrix and provider notes with explicit OpenRouter configuration guidance.

## [0.1.77] — 2026-04-26
### Changed
- **CLI maintainability**: Extracted `analyze` scope-resolution helpers and `/analyze apply` from `amx/cli.py` into `amx/cli_run.py`, reducing `amx/cli.py` below 3,000 lines.

### Added
- **Integration coverage**: Added Click-level integration tests for `/history list` and `/analyze apply` so future CLI refactors can be validated against the real command wiring.

## [0.1.76] — 2026-04-26
### Changed
- **CLI maintainability**: Extracted `/history` list/show/stats/events/results/review commands from `amx/cli.py` into `amx/cli_history.py`, including the saved-alternative review/apply flow.

### Added
- **Regression coverage**: Added tests for the extracted history scope formatter so run target summaries remain stable after the refactor.

## [0.1.75] — 2026-04-26
### Changed
- **CLI maintainability**: Extracted `/db` profile, profile-switching, profile-removal, and profiling guardrail commands from `amx/cli.py` into `amx/cli_db.py` while keeping the interactive command behavior unchanged.

### Added
- **Regression coverage**: Added a CLI profiling helper test to verify that guardrail changes still update the active DB profile after the extraction.

## [0.1.74] — 2026-04-26
### Changed
- **Confidence calibration**: Agent confidence now defaults to `LOW` when token logprobs are unavailable or unparseable, so model-declared text labels are not treated as calibrated confidence.
- **Profile batching**: Profile Agent batching now uses the persisted `/llm-batch-size` setting consistently in chat and batch flows, with optional context-only non-batch column names.
- **Schema/database review**: Schema-level and database-level generated descriptions remain reviewable instead of being auto-accepted.

### Fixed
- **Coverage gaps**: Orchestrator now injects low-confidence fallback rows when model output misses the table-level description or individual columns, preventing silent drops from review/history.
- **History recovery**: SQLite history recovers stale `running` runs left behind by crashes or interrupted sessions when the next run starts.
- **Live display bounds**: Pipeline activity rendering now windows older activities so long runs do not overflow the terminal.

### Added
- **Regression coverage**: Added tests for missing-logprob confidence downgrades and fallback coverage for model omissions.

## [0.1.73] — 2026-04-26
### Added
- **Profiling guardrails**: Added DB-profile-level profiling modes (`full`, `sampled`, `metadata`) plus `/db` → `/profiling [mode] [max_rows|off] [sample_size]` to control expensive table-data scans.
- **Config visibility**: `/config` now shows the active DB profile’s profiling guardrails.
- **Regression coverage**: Added focused unit tests for remote RAG source filtering, code RAG source scoping, BigQuery unsupported database write-back, and metadata-only profiling.

### Changed
- **Warehouse-cost control**: `profile_table()` now uses backend table statistics before deciding whether to run exact row counts and per-column aggregates. Tables above `profiling_max_rows` skip full column scans.
- **Code scan efficiency**: Codebase scan setup uses metadata-only column introspection instead of full table profiling when it only needs column names.

### Fixed
- **Remote document RAG profiles**: Ingested chunks now keep the original configured source path, so remote document profiles still filter correctly after files are downloaded to temporary paths.
- **Code RAG profile isolation**: Semantic code retrieval now filters by the active code profile/source path instead of querying a global mixed collection.
- **BigQuery database write-back**: Project-level description write-back now fails explicitly because BigQuery does not support it through SQL.
- **Databricks row estimate**: Databricks profiling no longer treats `numFiles` as a row count.

## [0.1.72] — 2026-04-26
### Changed
- **History results visibility**: `/results <run_id>` now shows **all** saved alternatives for each column (not truncated top-3) so past choices are fully visible.
- **Re-apply guidance**: `/results` now prints explicit guidance to use `/review <run_id> --apply` to re-pick and apply different alternatives later.

## [0.1.71] — 2026-04-26
### Changed
- **Realtime step timer**: `step_spinner` now updates elapsed seconds continuously while running (for example during database connection tests), instead of showing elapsed time only at completion.

## [0.1.70] — 2026-04-26
### Fixed
- **History status accuracy**: Restored guaranteed `finish_run(...)` finalization in `/run` so completed/cancelled/failed runs no longer remain stuck as `running`.
- **Stale run recovery**: New run creation now auto-recovers orphan `running` rows left by unclean shutdowns and marks them as failed with recovery text.

## [0.1.69] — 2026-04-26
### Fixed
- **`/llm-batch-size` persistence**: Fixed config serialization so `column_batch_size` is saved in LLM profiles and correctly used during `/run`.

## [0.1.68] — 2026-04-26
### Fixed
- **Disabled codebase profile handling**: Selecting `__none__` for Codebase in `/run` or `/run-apply` no longer raises `Unknown codebase profile`.

## [0.1.67] — 2026-04-26
### Fixed
- **`/run` profile switch NameError**: Fixed `NameError: name 'DatabaseConnector' is not defined` when changing DB profile in the interactive run flow.
- **Duplicate error noise**: Removed duplicate `Command failed: ...` lines and debug traceback leakage from the `/run` wrapper by normalizing failures through `click.ClickException`.

## [0.1.66] — 2026-04-26
### Fixed
- **Graceful interrupt handling**: Pressing `Ctrl+C` during `/run` prompts now exits cleanly with a user-facing interruption message instead of traceback noise and `UnboundLocalError`.

## [0.1.65] — 2026-04-26
### Fixed
- **CLI startup hotfix**: Fixed a misindented exception handler in `amx/cli.py` that prevented `amx` from importing cleanly.

## [0.1.64] — 2026-04-26
### Added
- **UI Feedback**: Added a "Testing database connection..." spinner to the `/run` command to provide immediate feedback during the initial connection phase.

## [0.1.63] — 2026-04-25
### Fixed
- **Hotfix: BATCH_SIZE regression**: Fixed an `AttributeError` caused by missing references to the newly dynamic batch size property in `ProfileAgent`.

## [0.1.62] — 2026-04-25
### Changed
- **Optimized `/run` Workflow**: Reordered prompts to resolve the analysis scope *before* asking for a review strategy. If only one table is selected, the review strategy prompt is now automatically skipped.

## [0.1.61] — 2026-04-25
### Added
- **Configurable LLM Batch Size**: Added `/llm-batch-size` command to control how many columns are processed in a single LLM call (default: 10). Larger batches are faster, while smaller batches can improve precision.

## [0.1.60] — 2026-04-25
### Added
- **Sliding Window Live Display**: The pipeline activity tree now only shows the last 15-25 items, preventing terminal overflow and scrolling issues during massive runs.

## [0.1.59] — 2026-04-25

### Fixed
- **BigQuery View Comments**: Fixed a bug where AMX used `ALTER TABLE` for views, causing comment application to fail in BigQuery.
- **History Re-review Logic**: Fixed an issue where "skipped" items would revert to the first LLM alternative when re-reviewing a past run.
- **Asset Limits**: Increased the codebase scanner limit from 450 to 5000 assets to support massive schemas with thousands of columns.

## [0.1.58] — 2026-04-25

### Changed
- **Review Options Parity**: `/history review` now uses the same batch review menu as `/run`, allowing you to quickly "accept-all-high", "accept-all", or "reject-all" instead of evaluating items strictly one-by-one.

## [0.1.56] — 2026-04-25

### Fixed
- **Apply Errors**: Fixed `NameError: name 'hs' is not defined` during the database application phase in `/apply` and `/history-review`.

## [0.1.55] — 2026-04-25

### Fixed
- **Profile batches showing `x` unexpectedly**: Fixed a regression in LLM timing capture where `elapsed_sec` could be referenced before assignment in `LLMProvider.chat()`. This caused profile batch steps to fail and render as failed (`✗` / `x`) in the live pipeline.

## [0.1.54] — 2026-04-25

### Added
- **Model processing duration tracking**: AMX now captures per-LLM-call processing time and stores aggregated `model_processing_sec` in run metrics inside SQLite history.
- **History visibility**: `/history list` now shows a dedicated `Model(s)` column alongside end-to-end `Duration(s)` for accurate runtime diagnostics.
- **History stats**: `/history stats` now reports `avg_model_processing_sec`.

## [0.1.53] — 2026-04-25

### Fixed
- **Ollama 404 on profile analysis**: Normalized Ollama `api_base` values that incorrectly include `/v1` (for example `http://localhost:11434/v1`) to `http://localhost:11434`, preventing `OllamaException - 404 page not found` failures during `/run`.
- **Noisy LiteLLM error lines in TUI**: Suppressed LiteLLM debug/info spill (`Give Feedback` / `litellm._turn_on_debug()`) so profile failures do not pollute interactive output.
- **Profile batching stability on local providers**: `ProfileAgent` now runs profile batches sequentially for `ollama`, `local`, and `kimi` providers to avoid unstable parallel behavior with local endpoints while keeping parallel execution for remote providers.

## [0.1.52] — 2026-04-25

### Fixed
- **ModuleNotFoundError**: Fixed incorrect import path for `history_store` in `Orchestrator` (`amx.history` → `amx.storage.sqlite_store`).

## [0.1.51] — 2026-04-25

### Added
- **Interactive Profile Selection**: When starting `/run`, you can now optionally switch your active DB, LLM, Document, or Codebase profiles through sequential prompts.

### Removed
- **Environment Variable Overrides**: Reverted session-specific overrides via environment variables in favor of the interactive workflow.

## [0.1.50] — 2026-04-25

### Added
- **Multi-Session Support**: Use `AMX_CONFIG_PATH`, `AMX_ACTIVE_DB_PROFILE`, and `AMX_ACTIVE_LLM_PROFILE` environment variables to run multiple AMX sessions with independent configurations from different terminals.

## [0.1.49] — 2026-04-25

### Fixed
- **NameError in Batch Review**: Fixed missing `history_store` import in `Orchestrator` that caused deferred review to fail.

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
