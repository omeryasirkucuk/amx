# Changelog

All notable changes to this project are documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.121] — 2026-04-28
### Fixed
- **Databricks batch write-back latency**: Database write-back now reuses a single transaction/connection across an apply batch, instead of opening a fresh transaction for every table or column comment.
- **Databricks insecure TLS warning spam**: When a Databricks profile explicitly sets `tls_no_verify=true`, AMX now suppresses the repeated `urllib3 InsecureRequestWarning` lines that were printed once per write-back request.

### Added
- **Regression coverage**: Added tests proving apply-mode write-back reuses one transaction and that Databricks suppresses insecure-request warnings only when TLS verification is intentionally disabled.

## [0.1.120] — 2026-04-28
### Fixed
- **Databricks write-back DDL**: Databricks table and column comment write-back no longer sends `:cmt` parameter markers inside DDL statements. AMX now inlines a safely quoted SQL literal for Databricks comment DDL, fixing failed `/run-apply` and manual metadata writes.

### Changed
- **Simpler `/connect` flow**: Removed the temporary threaded-connect and live-spinner path, keeping the bounded Databricks connector timeout/retry settings while returning the standard synchronous `/connect` behavior.
- **Databricks profile defaults**: The interactive Databricks profile flow no longer silently forces `tls_no_verify=True`; TLS options are again explicit user choices.

### Added
- **Regression coverage**: Added a Databricks test proving comment DDL renders an inline quoted literal instead of bound parameters.

## [0.1.119] — 2026-04-28
### Fixed
- **Databricks connection hang on startup**: Fixed an issue where testing the database connection would hang indefinitely if the Databricks SQL Warehouse was in a "STARTING" or "SUSPENDED" state. The test now runs in a non-blocking daemon thread and gracefully times out.

## [0.1.118] — 2026-04-28
### Added
- **Databricks TLS profile controls**: Databricks DB profiles now support an optional trusted CA bundle path and an insecure TLS-verification bypass for environments with corporate proxies or private certificate authorities.

### Changed
- **Actionable Databricks TLS failures**: `/connect` now reports certificate-validation failures as a clear Databricks TLS setup problem instead of only surfacing the raw SSL exception text.
- **Regression coverage**: Added Databricks adapter tests for TLS connect args and actionable certificate-verification failures.

## [0.1.117] — 2026-04-28
### Changed
- **Faster Databricks connect failure**: Databricks `/connect` now uses bounded connector timeouts and retry limits so bad host/path/token configurations fail in seconds instead of appearing to hang.
- **Connect progress feedback**: `/connect` now prints a short "Testing connection ..." line before attempting the network call.

### Added
- **Regression coverage**: Added Databricks adapter coverage for the explicit socket-timeout and retry connect args.

## [0.1.116] — 2026-04-28
### Fixed
- **Databricks connect deprecation warning**: AMX now passes `user_agent_entry` through SQLAlchemy `connect_args` so newer `databricks-sql-connector` releases no longer print the `_user_agent_entry` deprecation warning during `/connect`.

### Added
- **Regression coverage**: Added a Databricks adapter test that verifies engine creation uses the non-deprecated `user_agent_entry` connect arg.

## [0.1.115] — 2026-04-28
### Changed
- **Database connector capabilities**: Backend adapters now advertise metadata/comment/profiling capabilities so AMX can block unsupported write-back operations before treating them as successful.
- **Safer warehouse profiling**: Cloud backends no longer perform expensive full scans when row-count statistics are unknown, and sampled mode uses backend sampling syntax for Snowflake, Databricks, and BigQuery.
- **Backend-specific connector hardening**: Snowflake materialized view discovery and database comment reads no longer rely on fragile bind/result-index behavior; Databricks database comment write-back without a catalog now fails clearly instead of silently no-oping.

### Added
- **Connector regression coverage**: Added adapter contract tests for SQL generation, unsupported write-back, Snowflake metadata commands, sampled profiling, unknown-row-count scan blocking, and apply-flow failure accounting.

## [0.1.114] — 2026-04-28
### Fixed
- **Column discovery no longer becomes table snapshots**: Global semantic column questions such as "city ile alakalı tüm kolon isimlerini getir" no longer run a live table metadata snapshot just because conversation memory or fuzzy matching can resolve a nearby table.
- **Live probe scope guardrails**: Live-first probing is now limited to explicit table-scoped factual questions and table-understanding requests; open-ended column discovery stays on catalog/vector retrieval and synthesis.

### Added
- **Regression coverage**: Added a search test where prior ADRC session memory and a bad `table_explain` planner output cannot turn a global city-column search into an ADRC live snapshot answer.

## [0.1.113] — 2026-04-28
### Changed
- **Live-first factual table answers in `/search`**: Table-understanding questions now resolve explicit table targets first and run a live `table_metadata_snapshot` before answering structural facts such as column counts, types, and table comments.
- **No silent fuzzy substitution**: Explicit user table mentions are no longer replaced by similar catalog candidates. If the requested table is not found in live metadata, AMX says so and lists fuzzy matches only as suggestions.
- **Provenance and confidence guardrails**: `/search` no longer marks `table_explain` answers as live verified unless a live metadata row was actually collected; exact catalog-only table context is capped below live confidence.

### Added
- **Regression coverage**: Added tests for `adrc tablosu nedir` resolving to live `sap_s6p.adrc` instead of fuzzy `adr6`, unresolved explicit targets staying unresolved, and table resolution not producing fake live verification.

## [0.1.112] — 2026-04-28
### Fixed
- **Explicit table targeting in `/search` live probes**: Live probes now prioritize explicit user table mentions (`schema.table`, `ADRC table`, `adrc tablosunda`) over fuzzy catalog candidates and LLM-provided hints, preventing similarly named tables such as `ADR6` from being probed when the user asked for `ADRC`.

### Added
- **Regression coverage**: Added a test where the planner hint points at `ADR6` and the catalog only has an `ADR6` fuzzy candidate, but the explicit `adrc tablosunda` wording still forces the live probe to `sap_s6p.adrc`.

## [0.1.111] — 2026-04-28
### Changed
- **Deterministic live-probe defaults in `/search`**: Table-scoped factual metadata questions now run a default live metadata probe/snapshot whenever catalog evidence cannot prove the answer, even if the planner LLM says no probe is needed.
- **Broader live metadata snapshots**: Added a generic `table_metadata_snapshot` probe for table structure, column type/nullability, table comments, and column comments so `/search` has a reusable tool path beyond one-off comment coverage.
- **More robust table resolution**: Search live probes now infer `current_schema.<table>` from phrases like `adrc tablosunda`, allowing factual table questions to probe the live DB even when the table was not explicitly written as `schema.table`.

### Added
- **Regression coverage**: Strengthened the ADRC comment coverage test to prove the default live probe runs even when the LLM planner declines to request one.

## [0.1.110] — 2026-04-28
### Changed
- **Agentic live evidence probing in `/search`**: `/search ask` can now detect when catalog/semantic evidence is insufficient for a table-scoped metadata fact and ask the LLM to choose a safe live metadata probe from an allow-list.
- **Live column-comment coverage answers**: Table-scoped comment/completeness questions can resolve the table, inspect live column comments, and answer deterministically with coverage counts, missing columns, and the probe query used.

### Added
- **Probe-query visibility**: Database adapters can expose the metadata query/operation used for column-comment probes, with PostgreSQL returning the concrete `pg_class`/`pg_attribute`/`col_description` query shape.
- **Regression coverage**: Added a search test for the ADRC-style "are all columns commented?" question that verifies `/search` plans and runs a live `column_comments` probe instead of relying on semantic matches.

## [0.1.109] — 2026-04-28
### Changed
- **Fuller `/search` evidence use**: Semantic column and table search now keeps vector-only matches when lexical/exact matching finds nothing, so AMX can use the available vector index instead of returning an empty result set prematurely.
- **All-result answer synthesis**: `/search` now passes every retrieved result row in the current answer set into the grounded synthesis prompt, with result indexes, so answers can cover all returned candidates rather than only the first few.
- **Actionable profiling failures**: Backend profiling failures can now surface actionable remediation text, including PostgreSQL guidance for unavailable `pg_stat_statements`, missing relations, and insufficient privileges.

### Added
- **Human-approved search actions**: Added `/search ask --actions`, which prompts before running executable follow-up actions such as catalog sync, cached code-evidence refresh, or single-table metadata analysis.
- **Programmatic inference entrypoint**: Added `amx.core.inference.infer_table_metadata()` so approved search actions can run single-table metadata analysis without invoking the interactive CLI flow.
- **Regression coverage**: Added tests for vector-only column/table retrieval, full synthesis payload coverage, and declined human-in-the-loop search actions.

## [0.1.108] — 2026-04-28
### Changed
- **More assertive descriptions**: System prompts in `profile_agent`, `code_agent`, and `rag_agent` were updated to force the LLM to generate assertive and direct descriptions, stopping it from starting with phrases like "This column likely represents...".
- **Dynamic max_tokens**: The `ProfileAgent` now dynamically increases `max_tokens` based on the number of columns in the batch if the user-configured limit (`4096`) is too low for large tables, preventing truncation (`finish_reason=length`) errors during `/run`.
- **Unlock batch size limit**: The `ProfileAgent` unrestricted the hard-coded `100` maximum batch size limit, so the configured `/llm-batch-size` value is correctly honored.

## [0.1.107] — 2026-04-27
### Changed
- **Table-level semantic discovery in `/search`**: `/search` now distinguishes between semantic questions about columns and semantic questions about tables, so prompts like "içinde adres detayları olan tüm tablolar" no longer fall back to `count_tables` inventory answers.
- **Smarter search-plan correction**: The Search Agent now repairs common interpreter misroutes by rerouting table-listing concept questions from inventory/count mode into table-focused semantic discovery before retrieval runs.

### Added
- **Aggregated table retrieval**: Added table-level semantic retrieval that groups evidence from table descriptions and matching child-column metadata to surface the most relevant tables for concept-oriented discovery questions.
- **Regression coverage**: Added a test that forces the interpreter to misclassify an address-detail table question as inventory and verifies that `/search` still returns the matching tables.

## [0.1.106] — 2026-04-27
### Changed
- **Join-answer synthesis fix**: `/search` now includes resolved `left_column` and `right_column` evidence in the answer-synthesis payload for one-table join discovery, fixing responses that listed joinable tables but then incorrectly claimed no specific join columns were available.

### Added
- **Regression coverage**: Added a search test asserting that joinable-table synthesis receives the resolved join-column pair in its grounded prompt.

## [0.1.105] — 2026-04-27
### Changed
- **Question-language enforcement in `/search`**: `/search` now forces the final answer language to match the detected language of the user's question even when the interpreter LLM returns the wrong `answer_language`, which fixes Turkish inventory questions incorrectly answering in English.

### Added
- **Regression coverage**: Added a search test for Turkish inventory questions where the interpreter wrongly asks for an English answer.

## [0.1.104] — 2026-04-27
### Changed
- **More robust LLM model normalization**: AMX now corrects common provider-prefix typos in model ids before persisting them or sending them to LiteLLM, which fixes failures such as `oepnai/gpt-4o-mini` under OpenRouter or OpenAI profiles.
- **Runtime model guard**: `LLMProvider` now re-normalizes the configured model on initialization so older saved profiles with malformed provider prefixes are repaired at call time instead of breaking `/search` and other LLM-backed flows.

### Added
- **Regression coverage**: Added tests for typo recovery in OpenRouter and OpenAI model-id normalization.

## [0.1.103] — 2026-04-27
### Changed
- **Production-style `/search` orchestration**: Replaced the monolithic `/search` flow with a dedicated Search Agent pipeline that explicitly separates interpretation, retrieval planning, grounded retrieval, live verification, answer synthesis, and follow-up action suggestions.
- **Hybrid truth and observability**: `/search ask` now records question class, retrieval policy, evidence sources, ambiguity flags, verification details, and per-stage timings in history payloads while inventory questions continue to prefer live DB truth.
- **Stronger join reasoning**: Added semantic join inference alongside FK and code evidence so `/search` can surface likely non-FK join candidates, including SAP-style business-key matches such as `KUNNR` ↔ `customer_id`, with confidence bands in the rendered output.

### Added
- **`/search /context-detail`**: Added a dedicated search-context control (`minimal`, `standard`, `rich`, `deep`) so operators can tune how much catalog, code, and memory context the Search Agent uses.
- **Regression coverage**: Added tests for semantic non-FK join inference and live-verification metadata on inventory answers.

## [0.1.102] — 2026-04-27
### Changed
- **Question-language `/search` answers**: `/search ask` now answers in the user's question language instead of reusing the LLM profile's metadata-generation language.
- **Deterministic inventory answers**: Table counts and schema inventory questions now use live database introspection instead of relying on the search catalog's synced description coverage, so counts stay correct even when metadata generation is incomplete.
- **Cleaner LLM profile UX**: Provider-specific model handling now normalizes stored model ids so users can enter natural model names like `qwen/qwen3.6-plus` for OpenRouter without duplicating the provider prefix in config or UI.

### Added
- **Regression coverage**: Added tests for question-language answer synthesis, live table counts from the active schema, and OpenRouter model normalization.

## [0.1.101] — 2026-04-27
### Changed
- **Broader `/search ask` coverage**: `/search` now treats database inventory, schema listing, table counting, and single-table join-discovery questions as valid metadata discussion instead of rejecting them as out-of-domain.
- **Smarter grounded retrieval**: Added catalog-level retrieval modes for known databases, known schemas, scoped table counts, and joinable-table discovery from one table path.
- **More relevant `/search` rendering**: Aggregate answers no longer dump the generic schema/table/column grid after the LLM response; AMX only renders result tables when they are actually relevant to the question.

### Added
- **Regression coverage**: Added tests for catalog-overview questions, schema-scoped table counts, and single-table join discovery.

## [0.1.100] — 2026-04-27
### Changed
- **Language-aware LLM profiles**: Added a preferred language setting per LLM profile so `/run`, schema/database summaries, and `/search` answers can follow the user's chosen language instead of always defaulting to English.
- **Multilingual `/search` retrieval**: `/search ask` now plans multilingual search variants, including canonical English retrieval phrases, so Turkish and other non-English questions can retrieve the same metadata that English prompts would find.
- **Improved `/search` UX**: `/search ask` now shows live progress stages while interpreting, retrieving, and synthesizing, and search-result descriptions wrap cleanly in the table output instead of truncating awkwardly.

### Added
- **`/llm /language`**: Added a dedicated command for viewing or changing the preferred metadata-generation language of the active LLM profile.
- **Regression coverage**: Added tests for multilingual semantic retrieval and language-aware `/search` behavior.

## [0.1.99] — 2026-04-27
### Changed
- **LLM-native `/search` copilot**: `/search ask` is now a chat-first metadata discussion surface that uses the active LLM for query interpretation, typo recovery, follow-up handling, and grounded answer synthesis.
- **Search UX simplification**: Interactive `/search` now treats plain text as a metadata question, and the public command surface focuses on `ask`, `status`, `sources`, `config`, `sync`, and `rebuild`.
- **Grounded search behavior**: Name-like lookups such as `MANDT`/`mangdt` prioritize exact and fuzzy field-name matches before semantic description matches, while table explanation and join questions route through intent-specific retrieval paths.
- **Search history**: `/search ask` now records structured run history and app events with intent, confidence, provenance, and retrieved scope summaries.

### Added
- **Session memory for `/search`**: Follow-up questions in the `/search` tab can reuse recent table/column context during the current process.
- **Regression coverage**: Added tests for no-LLM fail-closed behavior, typo-oriented name lookup, out-of-domain rejection, and follow-up table explanation.

## [0.1.98] — 2026-04-27
### Added
- **`/search` namespace**: Added a dedicated search surface for natural-language metadata questions, join-candidate discovery, provenance, config, sync, and rebuild operations.
- **Search catalog**: Added SQLite-backed catalog tables and the `amx_search` vector index for effective metadata, relationships, code evidence, and sync job tracking.
- **Regression coverage**: Added catalog tests covering generated/reviewed/manual precedence, semantic column search, and join candidate extraction.

### Changed
- **Automatic catalog sync**: `/analyze`, `/run`, `/history review`, `/metadata edit`, `/code scan`, and successful DB apply flows now refresh `/search` automatically.
- **`/history` lifecycle visibility**: `/history show`, `/history stats`, and `/history results` now expose catalog status, effective source kind, indexed state, and DB-apply state for saved metadata.

## [0.1.97] — 2026-04-27
### Fixed
- **Remote Git clone cleanup**: Document GitHub scans now mark temporary clone roots for cleanup after preview/ingestion, and codebase Git scans use a temporary-directory context so cloned repos are removed after scanning and semantic indexing finish.

### Added
- **Regression coverage**: Added coverage for cleanup of temporary document and codebase Git clones.

## [0.1.96] — 2026-04-27
### Fixed
- **S3 document ingestion path collisions**: S3 downloads now preserve object key prefixes under the local staging directory, so objects such as `team-a/spec.md` and `team-b/spec.md` no longer overwrite each other during `/docs scan` or `/docs ingest`.

### Added
- **Regression coverage**: Added coverage for duplicate S3 basenames under different prefixes.

## [0.1.95] — 2026-04-27
### Changed
- **Flexible metadata edit targeting**: `/metadata edit` now accepts path targets: `<db>`, `<db>.<schema>`, `<db>.<schema>.<table>`, and `<db>.<schema>.<table>.<column>`.
- **Interactive edit wizard**: Missing or ambiguous edit targets now launch a guided wizard that selects DB profile, granularity, schema, table/view, and column as needed. Database-level edits stop immediately after choosing the database target.
- **Edit screen context**: Interactive comment entry now displays the resolved target before prompting for the new comment.

### Fixed
- **Graceful edit cancellation**: Typing `exit`, `quit`, `q`, `cancel`, or pressing Ctrl+C during the wizard cancels with `Manual edit cancelled.` and performs no write.

### Added
- **Coverage**: Added tests for path-based database/column targets and wizard drilling to a column target.

## [0.1.94] — 2026-04-27
### Changed
- **Metadata namespace UX**: Added `/metadata` as the primary database-metadata editing namespace and moved the interactive tab label from `MANUAL` to `METADATA`. `/manual` remains a compatibility alias.
- **Guided edit workflow**: Bare `/edit` now prints a guided workflow that starts with DB profile selection, then schema/table context, then concrete edit examples.
- **Softer usage guardrails**: Expected target-selection problems in `/metadata edit` now render as guidance warnings instead of red command failures.

### Added
- **Regression coverage**: Added coverage for `/metadata` shortcut routing and the guided bare-`/edit` workflow.

## [0.1.93] — 2026-04-27
### Fixed
- **Manual edit target safety**: `/manual edit schema` and `/manual edit table` now require an explicit schema/table target instead of silently editing the current context. This prevents typed text such as `edit table sap_test.adr6` from being saved as a comment on the wrong table.
- **Manual DB error summaries**: Database failures in `/manual` now show a short cause summary instead of dumping raw driver/SQLAlchemy exception text.

### Added
- **Qualified manual targets**: `/manual edit` now accepts dotted target forms such as `table sap_test.adr6`, `column adr6.smtp_addr`, and `column sap_test.adr6.smtp_addr`.
- **Regression coverage**: Added tests for explicit manual target parsing, implicit-table rejection, and the cleaned database-error path.

## [0.1.92] — 2026-04-27
### Fixed
- **Manual command error handling**: `/manual` commands now catch database-resolution/connection failures and report them with actionable guidance instead of leaking raw driver exceptions into the session. Interactive prompt cancellation during manual edits is also handled cleanly instead of surfacing as an empty `Command failed:` message.

### Added
- **Integration coverage**: Added a manual-edit integration test for friendly database connection error reporting.

## [0.1.91] — 2026-04-27
### Fixed
- **Manual edit guidance**: In the interactive `/manual` namespace, typing bare `/edit` now shows the valid targets and concrete examples instead of only a generic missing-parameter error.

### Added
- **Regression coverage**: Added a session-level test for the new `/manual /edit` shortcut guidance.

## [0.1.90] — 2026-04-27
### Fixed
- **Manual session UX**: In the interactive shell, known slash commands with missing required arguments now show their real usage error instead of being mislabeled as unknown commands. This fixes `/manual` workflows such as `/edit` where the session should tell you that a target like `database`, `schema`, `table`, or `column` is required.

### Added
- **Regression coverage**: Added session-level error-formatting tests for missing-argument and unknown-command cases.

## [0.1.89] — 2026-04-27
### Changed
- **Service extraction**: Moved manual metadata logic and analyze-scope/codebase-preparation logic into `amx/services/manual_metadata.py` and `amx/services/analyze_scope.py`. The `manual` and `run` command modules now act as thin wrappers over service-layer functions instead of owning the business logic directly.

### Added
- **Service-level coverage**: Added regression tests for manual target resolution and non-business asset filtering in the new service layer.

## [0.1.88] — 2026-04-27
### Changed
- **CLI package layout**: Moved the extracted command implementations under `amx/cli_support/commands/` and added `amx/cli_support/root_commands.py` for setup, DB, and config registration. `amx/cli.py` is now a thin bootstrap at roughly 200 lines.

### Added
- **Compatibility shims**: Kept import-compatible top-level `amx/cli_*.py` shim modules so existing imports continue working while the command package becomes the canonical layout.

## [0.1.87] — 2026-04-27
### Added
- **Manual metadata namespace**: Added a `/manual` interactive tab for inspecting current comments, manually editing database/schema/table/column metadata, and monitoring schema-level comment coverage without running LLM agents.
- **Manual command coverage**: Added regression and integration tests for manual command routing, comment coverage counting, and context-based column edits.

## [0.1.86] — 2026-04-27
### Fixed
- **Logprob calibration granularity**: Confidence calibration now scores the generated description text for each suggestion when response text is available instead of copying one whole-response logprob score to every column/table suggestion.
- **Batch logprob propagation**: OpenAI Batch requests now ask for logprobs and preserve returned token logprobs so batch-mode results can be calibrated like chat-mode results.

### Added
- **Regression coverage**: Added tests for per-suggestion logprob scoring and OpenAI Batch logprob request generation.

## [0.1.85] — 2026-04-27
### Changed
- **CLI maintainability**: Extracted the interactive session shell from `amx/cli.py` into `amx/cli_support/session.py` and turned `amx/cli_support/` into the first dedicated CLI support package so new CLI modules do not keep accumulating directly under `amx/`. `amx/cli.py` now focuses on entrypoint wiring, setup, and top-level commands at roughly 400 lines.

### Added
- **Regression coverage**: Added tests for session shortcut translation and schema-default injection in the extracted session helpers.

## [0.1.84] — 2026-04-27
### Changed
- **CLI maintainability**: Extracted the `/analyze run` command flow from `amx/cli.py` into `amx/cli_analyze_flow.py`, reducing `amx/cli.py` to roughly 1.3k lines while preserving profile switching, completion-mode selection, orchestration, review persistence, and run-history finalization.

### Added
- **Integration coverage**: Added Click-level routing coverage for `/analyze run` to verify the command now dispatches through the extracted analyze-flow module.

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
