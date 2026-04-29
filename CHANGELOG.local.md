# AMX — Local development changelog (not published to GitHub)

This file is intentionally **gitignored**. Use it for granular notes while keeping `CHANGELOG.md` as the public release log.

## [0.1.130] — 2026-04-29
### Library-First Core Foundation
- **amx/core/application.py / amx/__init__.py**: Added `AMXApplication` and lazy public exports so scripts can use `import amx` without booting the CLI shell.
- **amx/core/metadata.py**: Added the Universal Metadata Interface (`AbstractEntity`, lexical/structural/statistical/semantic signals, and `UniversalMetadataAdapter`) to normalize catalog/profile inputs without naming-convention rules.
- **amx/core/ask_agent.py**: Added a bounded tool-loop ask scaffold with `metadata_query`, `semantic_search`, `doc_rag_query`, and `sample_data_query`, returning a transparent observable trace.
- **amx/config.py / amx/core/state.py / amx/storage/sqlite_store.py**: Added loaded-config write-through for direct top-level and nested DB/LLM mutations plus SQLite `session_state` persistence.
- **amx/storage/sqlite_store.py / amx/agents/orchestrator.py**: Added `raw_logprob`, `token_count`, and `model_version` audit fields for saved inference results.
- **amx/search/agent.py / amx/cli_support/commands/search.py**: Added thought-trace diagnostics into `/search ask` result payloads.
- **amx/core/token_budget.py / amx/agents/rag_agent.py**: Added deterministic RAG context compaction before prompt construction.
- **tests/test_regressions.py / README.md / CHANGELOG.md / pyproject.toml**: Added core architecture regression coverage, documentation updates, and version bump to `0.1.130`.

## [0.1.127] — 2026-04-29
### `/search` LLM-First Routing
- **amx/search/agent.py**: Switched interpretation flow to LLM-first so `/search` always runs `_interpret_question()` before any deterministic routing logic.
- **amx/search/agent.py**: Kept `_rule_first_plan()` only as a guarded fallback path when interpreter LLM classification fails, preserving continuity without dominating normal routing.
- **amx/search/agent.py**: Updated join-discovery policy so `joinable_tables` keeps deterministic answer strategy after the interpreter flow change.
- **tests/test_search_catalog.py / amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Verified `/search` regressions and bumped version to `0.1.127`.

## [0.1.126] — 2026-04-29
### Live Process Visibility Pass
- **amx/utils/live_commands.py**: Added a shared helper for starting/stopping the common live display around non-`/run` commands without duplicating lifecycle logic.
- **amx/services/analyze_scope.py / amx/cli_support/commands/manual.py**: Added timed progress around schema, asset, and column discovery so interactive pickers no longer sit on a blank terminal while warehouse metadata loads.
- **amx/cli_support/root_commands.py / amx/cli_support/commands/search.py / amx/cli_support/commands/docs.py / amx/cli_support/commands/code.py / amx/cli_support/commands/analyze_flow.py**: Wired long-running DB inspection, sync, docs, code, and analyze-setup flows into the shared live display and added visible timing for cache refresh and catalog-write stages.
- **amx/search/catalog.py / amx/llm/batch.py**: Added progress callbacks for `/search rebuild` and surfaced batch polling status through the live activity tree when available.
- **tests/test_cli_integration.py / amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Added CLI coverage for the new display lifecycle and bumped the release to `0.1.126`.

## [0.1.125] — 2026-04-29
### Prompt Quality Pass
- **amx/search/agent.py**: Rewrote the `/search` interpretation and answer prompts to be more conservative about routing, ambiguity, evidence ranking, and weak-tail summarization.
- **amx/agents/profile_agent.py / amx/agents/code_agent.py / amx/agents/rag_agent.py**: Hardened all analyze-agent prompts with explicit abstention/fallback guidance, confidence rules, and evidence-based reasoning requirements.
- **amx/agents/orchestrator.py**: Strengthened merge/schema/database prompts with source precedence, conflict handling, and conservative summarization rules; added lightweight fenced-output stripping in prompt parsers.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.125` and documented the prompt-quality pass.

## [0.1.124] — 2026-04-29
### `/search` Hardening Refactor
- **amx/search/agent.py**: Added rule-first routing, deterministic probe planning, row normalization/suppression, deterministic short-form answers, executed/suggested action separation, answer-strategy reporting, and tighter session-memory retention.
- **amx/search/catalog.py**: Added normalized retrieval metadata defaults (`evidence_tier`, `answer_role`, `match_reason`) on ranked rows so the agent can reason over a stable retrieval shape.
- **tests/test_search_catalog.py**: Updated `/search` regression coverage for deterministic answer paths, follow-up table memory, and executed read-only live-probe tracking.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.124` and documented the `/search` hardening changes.

## [0.1.123] — 2026-04-29
### Databricks Bulk Column Write-Back
- **amx/db/adapters/base.py / amx/db/adapters/databricks.py / amx/db/connector.py**: Added backend support for multi-column comment DDL and used it to batch Databricks column comment updates per table when possible.
- **amx/agents/orchestrator.py**: Apply-mode write-back now detects adjacent Databricks column rows for the same table, issues a grouped write when supported, and falls back to per-column statements if the grouped SQL is rejected.
- **amx/utils/live_display.py / amx/cli_support/commands/run.py / amx/cli_support/commands/history.py / amx/agents/orchestrator.py**: Reworked write-back live progress to use one rolling activity line instead of one line per asset; failure details are still preserved as subordinate detail rows only when needed.
- **tests/test_regressions.py / tests/test_cli_integration.py**: Added coverage for Databricks multi-column SQL generation, grouped apply behavior, and the updated single-activity live display contract.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.123` and documented the grouped write-back behavior.

## [0.1.122] — 2026-04-29
### Write-Back Visibility
- **amx/agents/orchestrator.py / amx/cli_support/commands/run.py / amx/cli_support/commands/history.py**: Extended DB write-back callbacks with progress and failure hooks, then wired apply flows into the live display so write-back now shows elapsed time and per-item progress in real time.
- **amx/storage/sqlite_store.py**: Successful DB writes now mark `db_applied_status='applied'`, and failed writes now mark `db_applied_status='failed'` with the error text persisted into `rejection_reason`.
- **tests/test_regressions.py / tests/test_cli_integration.py**: Added regression coverage for failed write-back callbacks, SQLite failure-state persistence, and the updated apply callback signature with live-display wiring.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.122` and documented the new write-back visibility behavior.

## [0.1.121] — 2026-04-28
### Databricks Apply Throughput
- **amx/db/connector.py / amx/agents/orchestrator.py**: Added a shared `apply_comment()` path and updated apply-mode write-back to hold one transaction open for the whole batch instead of opening a new `engine.begin()` block per comment.
- **amx/db/adapters/databricks.py**: Suppressed `urllib3.exceptions.InsecureRequestWarning` only when the operator explicitly enables `tls_no_verify` for Databricks.
- **tests/test_regressions.py**: Added regression coverage for single-transaction apply batches and intentional Databricks insecure-request warning suppression.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.121` and documented the throughput and warning cleanup.

## [0.1.120] — 2026-04-28
### Databricks Write-Back Cleanup
- **amx/db/adapters/base.py / amx/db/connector.py**: Added a small `comment_sql_with_params()` adapter hook so backends can choose bound params or inline comment literals for DDL execution.
- **amx/db/adapters/databricks.py**: Databricks comment write-back now renders inline quoted literals instead of `:cmt` markers, which Databricks SQL rejects inside `COMMENT` DDL.
- **amx/cli_support/commands/db.py / amx/cli_support/root_commands.py / amx/db/connector.py**: Removed the temporary forced `tls_no_verify=True`, restored explicit TLS prompts, and simplified `/connect` back to the normal synchronous flow.
- **tests/test_regressions.py**: Added a regression proving Databricks DDL comment SQL is rendered with an inline escaped literal.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.120` and documented the cleanup.

## [0.1.119] — 2026-04-28
### Threaded Database Connection Verification
- **amx/db/connector.py**: Moved `conn.execute(text("SELECT 1"))` into a daemon thread with `Thread.join(timeout)` when running `test_connection()`. Solved a critical hang where Databricks `pool_pre_ping=True` and cursor fetch calls block indefinitely while the SQL warehouse is starting/suspended.
- **amx/__init__.py**, **pyproject.toml**: Bumped version to `0.1.119`.

## [0.1.118] — 2026-04-28
### Databricks TLS Trust Controls
- **amx/config.py / amx/cli_support/commands/db.py**: Added Databricks profile fields and setup prompts for a trusted CA bundle path and optional insecure TLS verification bypass.
- **amx/db/adapters/databricks.py / amx/db/connector.py**: Forwarded Databricks TLS trust settings into connector `connect_args` and converted certificate verification failures into actionable operator guidance.
- **tests/test_regressions.py**: Added coverage for Databricks TLS connect args and actionable TLS failure logging.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.118` and documented Databricks TLS trust controls.

## [0.1.117] — 2026-04-28
### Databricks Connect Timeout Guardrails
- **amx/db/adapters/databricks.py**: Added bounded Databricks connect args for `_socket_timeout`, `_retry_stop_after_attempts_count`, and `_retry_stop_after_attempts_duration`, while keeping the non-deprecated `user_agent_entry`.
- **amx/cli_support/root_commands.py**: `/connect` now prints a short progress line before opening the DB connection.
- **tests/test_regressions.py**: Expanded Databricks engine-creation coverage to verify the timeout and retry connect args.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.117` and documented the connect-timeout guardrails.

## [0.1.116] — 2026-04-28
### Databricks Connector Warning Cleanup
- **amx/db/adapters/databricks.py**: Added `connect_args={"user_agent_entry": "amx"}` so Databricks SQLAlchemy stops triggering the connector's `_user_agent_entry` deprecation warning on connect.
- **tests/test_regressions.py**: Added regression coverage for Databricks engine creation using the non-deprecated connect arg.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.116` and documented the warning cleanup.

## [0.1.115] — 2026-04-28
### Database Connector Backend Correctness
- **amx/db/adapters/base.py / amx/db/connector.py**: Added backend capability metadata and explicit unsupported-operation handling for comments, relationships, materialized views, row-count stats, and profiling behavior.
- **amx/db/adapters/snowflake.py**: Fixed materialized-view discovery to avoid unsupported SHOW bind syntax, made database comments read from named result fields, and kept exact-case metadata lookup before uppercase fallback.
- **amx/db/adapters/databricks.py / amx/db/adapters/bigquery.py**: Replaced silent unsupported write-back paths with clear failures, added actionable profiling hints, and switched sampled profiling to backend sampling SQL.
- **tests/test_regressions.py**: Added connector contract coverage for adapter SQL, unsupported database/catalog comments, sampled profiling, unknown cloud row-count scan blocking, and apply-flow failure accounting.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.115` and documented connector capability/profiling behavior.

## [0.1.114] — 2026-04-28
### Column Discovery Probe Guardrails
- **amx/search/agent.py**: Rerouted misclassified global column-list questions away from `table_explain`, limited non-table-understanding live probes to explicit table mentions, and stopped candidate-table discovery from driving live probes for open-ended semantic column searches.
- **tests/test_search_catalog.py**: Added a regression for `city ile alakalı tüm kolon isimlerini getir` with prior ADRC session memory and a bad planner response.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.114` and documented the table-scoped boundary for live-first probing.

## [0.1.113] — 2026-04-28
### Live-First Search Facts
- **amx/search/agent.py**: Added explicit target-resolution records, live exact table checks, live-first `table_metadata_snapshot` probes for `table_explain`, deterministic live table summaries, and unresolved-target answers that refuse to replace explicit table names with fuzzy candidates.
- **amx/search/agent.py**: Fixed provenance/confidence so table resolution alone no longer counts as live verification.
- **tests/test_search_catalog.py**: Added regressions for `adrc tablosu nedir`, missing explicit tables with fuzzy `adr6` candidates, and no fake live verification without live rows.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.113` and documented live-first factual search behavior.

## [0.1.112] — 2026-04-28
### Explicit Table Targeting For Search Probes
- **amx/search/agent.py**: Split explicit table-path extraction from fuzzy candidate discovery and made explicit user mentions take precedence for live probes. `schema.table`, `X table`, and `x tablosunda` now choose the intended target before any catalog fuzzy match or LLM hint.
- **tests/test_search_catalog.py**: Added a regression where an `ADR6` fuzzy candidate and bad LLM hint cannot override an explicit `ADRC` probe target.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.112` and documented explicit table precedence.

## [0.1.111] — 2026-04-28
### Deterministic Search Tool Use
- **amx/search/agent.py**: Added default live-probe operations for table-scoped factual metadata questions so the agent runs a live snapshot even when the planner LLM declines a probe. Added `table_metadata_snapshot`, better current-schema table extraction, and deterministic operation merging.
- **amx/db/adapters/base.py / amx/db/adapters/postgresql.py / amx/db/connector.py**: Added `table_metadata_probe_query()` and `get_table_metadata_snapshot()` for reusable live table metadata snapshots.
- **tests/test_search_catalog.py**: Updated ADRC coverage regression so the fake planner says no probe is needed, while AMX still executes the default `column_comments` probe and answers from live metadata.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.111` and documented deterministic live-probe defaults.

## [0.1.110] — 2026-04-28
### Agentic Live Metadata Probes
- **amx/search/agent.py**: Added an evidence-gap planning step that lets the Search Agent ask the LLM whether retrieved evidence is enough, choose safe live metadata probes from an allow-list, execute `column_comments`, and answer deterministically from live coverage rows.
- **amx/db/adapters/base.py / amx/db/adapters/postgresql.py / amx/db/connector.py**: Added `column_comments_probe_query()` so live probe answers can disclose the concrete metadata operation/query used.
- **tests/test_search_catalog.py**: Added ADRC-style coverage regression proving `/search` resolves the table, runs a live column-comment probe, reports missing columns, and avoids semantic-only guessing.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.110` and documented agentic live-probe behavior.

## [0.1.109] — 2026-04-28
### Search Evidence Coverage & HITL Actions
- **amx/search/catalog.py**: Removed the premature exact-hit requirement from `search_columns()` and `search_tables()` so vector-only matches are used when lexical matching misses.
- **amx/search/agent.py**: Expanded synthesis evidence payloads to include every retrieved row in the active result set, added result indexes, and added low-confidence action suggestions for empty semantic/entity answers.
- **amx/cli_support/commands/search.py / amx/cli_support/session.py**: Added `/search ask --actions`, a human-in-the-loop execution path that asks before running catalog sync, cached code-evidence refresh, or single-table metadata analysis.
- **amx/core/inference.py / amx/core/__init__.py**: Added a programmatic single-table metadata inference entrypoint for approved search actions.
- **amx/db/adapters/base.py / amx/db/adapters/postgresql.py / amx/db/connector.py**: Added backend-specific actionable profiling errors and tolerant column-stat warnings so search-triggered sync/analyze actions fail with clearer remediation.
- **tests/test_search_catalog.py / tests/test_cli_integration.py**: Added regression coverage for vector-only retrieval, all-result synthesis payloads, and declined action execution.
- **amx/__init__.py / pyproject.toml / README.md / CHANGELOG.md**: Bumped version to `0.1.109` and documented the search behavior changes.

## [0.1.108] — 2026-04-28
### Assertive Agent Descriptions & Dynamic `max_tokens` Limit
- **amx/agents/profile_agent.py**, **amx/agents/code_agent.py**, **amx/agents/rag_agent.py**: Updated `_BASE_SYSTEM_PROMPT` adding "Write descriptions assertively and directly" rule to stop fuzzy beginnings ("This column likely represents", "This column may contain").
- **amx/agents/profile_agent.py**: Removed the hardcoded `100` column `batch_size` limit in `batch_size` property, so `/llm-batch-size 300` works as expected. Added a dynamic `max_tokens` bump (`len(columns) * 150`) before running `chat` to avoid truncation on massive batch processing.
- **amx/__init__.py**, **pyproject.toml**: Bumped version to `0.1.108`.

## [0.1.107] — 2026-04-27
### Table-Level Semantic Search Routing
- **amx/search/agent.py**: Added `target_entity` planning, a shape-alignment repair step, and a dedicated table-semantic retrieval policy so concept questions about tables no longer collapse into inventory counts.
- **amx/search/catalog.py**: Added `search_tables()` to aggregate table descriptions plus matching child-column evidence into ranked table candidates for concept-based discovery.
- **tests/test_search_catalog.py**: Added regression coverage for the Turkish "adres detaylari olan tum tablolar" case, including a deliberately misclassified interpreter payload.
- **README.md / CHANGELOG.md**: Documented the new table-level semantic discovery behavior in `/search`.

## [0.1.106] — 2026-04-27
### Join-Column Prompt Fix
- **amx/search/agent.py**: Added `left_column` and `right_column` to the grounded synthesis payload so joinable-table answers can explicitly mention the discovered join columns instead of only the target tables.
- **tests/test_search_catalog.py**: Added regression coverage asserting that one-table join synthesis sees the resolved `kunnr` ↔ `kunnr` join columns.
- **README.md / CHANGELOG.md**: Documented the join-column synthesis fix.

## [0.1.105] — 2026-04-27
### Search Answer-Language Enforcement
- **amx/search/agent.py**: Forced `/search` answers to use the detected question language after interpretation so deterministic and synthesized answers no longer drift back to the LLM-returned `answer_language`.
- **tests/test_search_catalog.py**: Added regression coverage for Turkish schema-inventory questions where the interpreter incorrectly returns `answer_language="english"`.
- **README.md / CHANGELOG.md**: Documented that `/search` answer language is hard-aligned with the user's question language.

## [0.1.104] — 2026-04-27
### LLM Model Normalization Guardrails
- **amx/config.py**: Hardened `normalize_llm_model()` so provider-prefixed model ids recover common namespace typos such as `oepnai/...` and cleanly strip redundant prefixes according to the selected provider.
- **amx/llm/provider.py**: Added a runtime normalization pass in `LLMProvider.__init__()` so malformed saved profiles are repaired before any LiteLLM call is made.
- **README.md / CHANGELOG.md**: Documented automatic typo correction for provider/model prefixes.
- **tests/test_regressions.py**: Added regression tests for OpenRouter and OpenAI typo recovery during model normalization.

## [0.1.103] — 2026-04-27
### Search Agent + Semantic Join Inference
- **amx/search/agent.py / amx/search/service.py**: Introduced a dedicated Search Agent that separates interpretation, retrieval planning, grounded retrieval, live verification, answer synthesis, session-memory reuse, and follow-up action suggestions while keeping `SearchService` as a compatibility facade.
- **amx/search/catalog.py**: Added production-oriented `/search` settings (`context_detail`, live-verification toggles, semantic join inference), semantic join candidate scoring, confidence-band classification, SAP alias expansion, and semantic joinable-table discovery for one-table join questions.
- **amx/cli_support/commands/search.py**: Extended `/search` rendering with join confidence bands, stage/action payloads, richer history/event serialization, status visibility for production search settings, and the new `/search /context-detail` command.
- **README.md / CHANGELOG.md**: Documented the Search Agent architecture, context-detail control, and the stronger hybrid truth model for `/search`.
- **tests/test_search_catalog.py**: Added coverage for semantic non-FK join discovery and live-verification metadata on inventory answers.

## [0.1.102] — 2026-04-27
### Search Language + Inventory Truth + Provider UX
- **amx/search/service.py**: Made `/search ask` answer in the user's question language, moved count/schema/database inventory questions onto deterministic live DB introspection, and added explicit scope assumptions for ambiguous count questions.
- **amx/config.py / amx/llm/provider.py / amx/cli_support/commands/profiles.py / amx/cli_support/session.py / amx/cli.py / amx/cli_support/root_commands.py**: Normalized provider-prefixed model ids, removed the need to type duplicated OpenRouter prefixes, and clarified that `/llm /language` controls metadata generation rather than `/search` answer language.
- **tests/test_search_catalog.py / tests/test_regressions.py**: Added coverage for question-language answer prompts, live inventory counts, and OpenRouter model normalization.

## [0.1.101] — 2026-04-27
### Broader Metadata Discussion In `/search`
- **amx/search/service.py**: Expanded the LLM interpreter/retrieval contract so `/search ask` can answer catalog-overview questions like known databases, schema lists, scoped table counts, and single-table joinability in addition to column semantics and two-table joins.
- **amx/search/catalog.py**: Added catalog inventory helpers (`known_databases`, `known_schemas`, `count_tables`) plus direct relationship-based `joinable_tables` discovery for one-table join questions.
- **amx/cli_support/commands/search.py**: Suppressed irrelevant generic result grids for aggregate answers and added a dedicated joinable-table renderer when the question is about which tables can join to one base table.
- **tests/test_search_catalog.py**: Added regression tests for catalog inventory questions, schema table counts, and single-table join discovery.

## [0.1.100] — 2026-04-27
### Multilingual Search + LLM Language Control
- **amx/config.py / amx/cli_support/commands/profiles.py / amx/cli_support/session.py / amx/cli.py / amx/cli_support/root_commands.py**: Added `LLMConfig.language`, exposed it through interactive profile setup and the new `/llm /language` command, and surfaced the active language in CLI summaries.
- **amx/agents/profile_agent.py / amx/agents/rag_agent.py / amx/agents/code_agent.py / amx/agents/orchestrator.py**: Made generated metadata descriptions, merge outputs, and schema/database summaries follow the configured language while keeping parse-critical field labels in English.
- **amx/search/service.py / amx/search/catalog.py / amx/cli_support/commands/search.py**: Added multilingual query normalization, canonical English retrieval variants, live `/search ask` progress stages, and wrapped description rendering for long result text.
- **tests/test_search_catalog.py**: Added multilingual retrieval coverage for Turkish semantic questions against English metadata.

## [0.1.99] — 2026-04-27
### LLM-Native Search Copilot
- **amx/search/service.py**: Replaced the rule-based `/search` router with an LLM-backed interpretation, retrieval-planning, short session-memory, and grounded answer-synthesis flow.
- **amx/search/catalog.py**: Added name-first field lookup, fuzzy table resolution, LLM-oriented retrieval settings, and better effective-description loading for `/search` rendering.
- **amx/cli_support/commands/search.py / amx/cli_support/session.py**: Simplified `/search` to a chat-first surface, enabled plain-text questions inside the `/search` tab, logged `/search ask` runs/events into history, and kept sync/rebuild as admin operations.
- **tests/test_search_catalog.py / tests/test_cli_integration.py**: Added coverage for fail-closed no-LLM behavior, typo correction toward `mandt`, out-of-domain rejection, and follow-up table explanations using session memory.

## [0.1.98] — 2026-04-27
### Search Catalog + History Lifecycle
- **amx/search/catalog.py / amx/search/index.py / amx/search/service.py**: Added the SQLite-backed search catalog, effective-description resolver, relationship/evidence storage, and the `amx_search` Chroma index.
- **amx/cli_support/commands/search.py / amx/cli.py / amx/cli_support/session.py**: Registered the `/search` namespace and wired help text, completion, namespace inference, and command routing.
- **amx/agents/orchestrator.py / amx/cli_support/commands/manual.py / amx/cli_support/commands/code.py / amx/cli_support/commands/history.py**: Auto-synced generated metadata, review outcomes, manual edits, code scans, and DB apply state into the search catalog.
- **amx/storage/sqlite_store.py**: Extended `run_results` with catalog lifecycle columns and created the new search catalog tables inside `history.db`.
- **tests/test_search_catalog.py**: Added coverage for precedence, semantic column lookup, and join candidate extraction.

## [0.1.97] — 2026-04-27
### Remote Git Cleanup
- **amx/docs/scanner.py / amx/cli_support/commands/docs.py**: Tagged GitHub document scan results with their temporary clone root and removed those roots after scan preview and optional ingestion finish.
- **amx/codebase/analyzer.py**: Replaced persistent remote code clone directories with a `TemporaryDirectory` context that spans scanning and semantic indexing.
- **tests/test_regressions.py**: Added mocked Git clone coverage for document and codebase cleanup.

## [0.1.96] — 2026-04-27
### S3 Document Ingestion
- **amx/docs/scanner.py**: Preserved S3 object key prefixes when staging downloads, preventing duplicate basenames from different prefixes from overwriting each other.
- **tests/test_regressions.py**: Added a mocked S3 regression test for duplicate `spec.md` basenames under separate prefixes.

## [0.1.95] — 2026-04-27
### Metadata Edit Wizard
- **amx/cli_support/commands/manual.py**: Refactored `/metadata edit` to accept free-form target paths, dispatch legacy scope syntax, and fall back to an interactive DB/profile/schema/table/column wizard.
- **amx/services/manual_metadata.py**: Added path-target parsing and typed edit target descriptors for database/schema/table/column writer callbacks.
- **amx/cli_support/session.py**: Updated metadata help/completion text for path-based targets and the edit wizard.
- **tests/test_regressions.py / tests/test_cli_integration.py**: Added coverage for path targets, wizard drilling, and ambiguous-target wizard dispatch.

## [0.1.94] — 2026-04-27
### Metadata Namespace UX
- **amx/cli_support/session.py**: Promoted `/metadata` as the primary interactive namespace, updated the tab label, root help, completion catalog, shortcut routing, namespace assumptions, and bare-`/edit` guidance.
- **amx/cli_support/commands/manual.py**: Registered the Click group as `/metadata` while keeping `/manual` as a compatibility alias, and downgraded target-selection guidance from red errors to warnings.
- **tests/test_regressions.py**: Updated session routing and bare-`/edit` coverage for the new metadata-first UX.

## [0.1.93] — 2026-04-27
### Manual Target UX
- **amx/services/manual_metadata.py**: Added explicit target parsing for manual edits, dotted schema/table/column target support, and guardrails that reject implicit schema/table edits.
- **amx/cli_support/commands/manual.py**: Accepted `db` as a database scope alias and shortened database exception reporting to a concise cause summary.
- **amx/cli_support/session.py**: Clarified `/manual` help as database metadata editing and pointed document workflows to `/docs`.
- **tests/test_regressions.py / tests/test_cli_integration.py**: Added coverage for explicit table/column targets, rejected implicit table edits, and concise DB error output.

## [0.1.92] — 2026-04-27
### Manual UX Fix
- **amx/cli_support/commands/manual.py**: Added friendly database error reporting for inspect/edit/monitor paths and clean cancellation handling for interactive manual-edit prompts.
- **tests/test_cli_integration.py**: Added coverage for the manual table-edit connection-failure path so raw backend exceptions do not leak as the primary user message.

## [0.1.91] — 2026-04-27
### Session UX Fix
- **amx/cli_support/session.py**: Added a `/manual` shortcut guard so bare `/edit` prints the valid targets and examples before Click dispatch.
- **tests/test_regressions.py**: Added direct coverage for the new bare-`/edit` guidance path.

## [0.1.90] — 2026-04-27
### Session UX Fix
- **amx/cli_support/session.py**: Added slash-session-specific Click error formatting so known commands with missing arguments surface their real usage error while unknown commands still use the custom slash-native message.
- **tests/test_regressions.py**: Added coverage for missing-argument and unknown-command session error formatting.

## [0.1.89] — 2026-04-27
### Service Refactor
- **amx/services/manual_metadata.py**: Added service helpers for manual metadata coverage, inspect row building, context-aware schema/table resolution, and manual edit target resolution.
- **amx/services/analyze_scope.py**: Added service helpers for scope selection, asset filtering/validation, and codebase report preparation for analyze flows.
- **amx/cli_support/commands/manual.py / run.py**: Replaced in-module business logic with thin wrappers around the new service functions.
- **tests/test_regressions.py**: Added direct service-layer coverage for manual target resolution and non-business asset filtering.

## [0.1.88] — 2026-04-27
### CLI Refactor
- **amx/cli_support/commands/**: Moved the extracted CLI command implementations into a dedicated command package (`analyze_flow`, `code`, `db`, `docs`, `history`, `manual`, `profiles`, `run`) so the codebase no longer treats them as a flat top-level namespace.
- **amx/cli_support/root_commands.py**: Added dedicated registration for `/setup`, `/config`, and top-level `/db` command wiring.
- **amx/cli.py**: Replaced in-file setup, DB, and config command definitions with imports/registration from the support package and reduced the file to roughly 200 lines.
- **amx/cli_*.py**: Reintroduced top-level modules as compatibility shims that re-export from `amx.cli_support.commands.*`.
- **tests/test_cli_integration.py / tests/test_regressions.py**: Updated imports and patch targets to the canonical `amx.cli_support.commands.*` modules.

## [0.1.87] — 2026-04-27
### Manual Metadata
- **amx/cli_manual.py**: Added `/manual inspect`, `/manual edit`, and `/manual monitor` commands for direct comment inspection, write-back, and coverage reporting.
- **amx/cli_support/session.py**: Added a Manual tab to keyboard namespace navigation, slash completion, root help, contextual help, and root auto-routing.
- **amx/cli.py**: Registered the manual command group with the main interactive CLI.
- **tests/test_regressions.py / tests/test_cli_integration.py**: Added coverage for manual shortcut routing, coverage counting, and context-based column edits.

## [0.1.86] — 2026-04-27
### Logprob Calibration
- **amx/llm/provider.py**: Added best-effort token-span reconstruction and description-fragment scoring so AMX can score the generated description text rather than the full structured response.
- **amx/agents/base.py**: `apply_logprob_confidence()` now accepts `response_text` and calibrates each suggestion independently when its description text can be matched in the completion.
- **amx/agents/profile_agent.py / rag_agent.py / code_agent.py / orchestrator.py**: Passed raw response text into logprob calibration for chat-mode agent, schema, database, and merge outputs.
- **amx/llm/batch.py / amx/agents/orchestrator.py**: OpenAI Batch JSONL now requests logprobs, result parsing keeps them, and batch-mode parsed suggestions are calibrated before merging.
- **tests/test_regressions.py**: Added coverage for per-suggestion scoring and batch logprob request generation.

## [0.1.85] — 2026-04-27
### CLI Refactor
- **amx/cli_support/session.py**: Added a dedicated session-layer module for prompt-toolkit handling, slash-command completion, namespace navigation, and session-only shortcut/default injection.
- **amx/cli_support/__init__.py**: Added a small support package export surface so session helpers live under `amx/cli_support/` instead of adding more top-level `cli_*.py` files.
- **amx/cli.py**: Removed the in-file interactive shell implementation and reduced the file to entrypoint wiring, setup, DB commands, and config display at roughly 400 lines.
- **tests/test_regressions.py**: Added focused regression coverage for session shortcut routing and schema-default injection.

## [0.1.84] — 2026-04-27
### CLI Refactor
- **amx/cli_analyze_flow.py**: Added a dedicated module for `/analyze run`, including profile switching, completion-mode selection, orchestration execution, interrupt handling, and run-history finalization.
- **amx/cli.py**: Replaced the in-file `/analyze run` block with `register_analyze_run_command(...)`, bringing the file down to roughly 1.3k lines.
- **tests/test_cli_integration.py**: Added Click integration coverage to verify `/analyze run` dispatches through the extracted module.

## [0.1.83] — 2026-04-27
### CLI Refactor
- **amx/cli_code.py**: Added a dedicated module for `/code` scan, refresh, results, export-report, and standalone analyze commands.
- **amx/cli.py**: Replaced the in-file code namespace block with `register_code_commands(...)`, bringing the file down to roughly 1.8k lines.
- **tests/test_cli_integration.py**: Added Click integration coverage for `/code results`, `/code refresh`, and `/code analyze` guardrails.

## [0.1.82] — 2026-04-27
### CLI Refactor
- **amx/cli_docs.py**: Added a dedicated module for `/docs` scan, ingest, search, export-report, and standalone analyze commands.
- **amx/cli.py**: Replaced the in-file document namespace block with `register_docs_commands(...)`, bringing the file down to roughly 2.26k lines.
- **tests/test_cli_integration.py**: Added Click integration coverage for `/docs scan` empty-path guidance and `/docs search-docs`.

## [0.1.81] — 2026-04-27
### CLI Refactor
- **amx/cli_profiles.py**: Added a dedicated module for interactive LLM profile editing, prompt/detail batch settings, and document/code profile helper commands.
- **amx/cli.py**: Replaced the in-file LLM/doc/code profile helper block with imports from `cli_profiles`, while keeping existing OpenRouter-aware prompts and setup flow behavior.
- **tests/test_regressions.py**: Added profile-helper coverage and aligned the missing-logprobs confidence expectation with the current implementation.

## [0.1.77] — 2026-04-26
### CLI Refactor
- **amx/cli_run.py**: Added a dedicated module for `analyze` scope resolution helpers, codebase report setup, and `/analyze apply`.
- **amx/cli.py**: Replaced the in-file analyze helper block and apply command with imports/registration from `cli_run`.
- **tests/test_cli_integration.py**: Added Click integration tests for `/history list` and `/analyze apply`.

## [0.1.76] — 2026-04-26
### CLI Refactor
- **amx/cli_history.py**: Added a dedicated module for `/history` list/show/stats/events plus saved-result inspection and review/apply flows.
- **amx/cli.py**: Replaced the in-file history namespace block with `register_history_commands(...)` registration.
- **tests/test_regressions.py**: Added coverage for compact history scope rendering.

## [0.1.75] — 2026-04-26
### CLI Refactor
- **amx/cli_db.py**: Added a dedicated module for `/db` namespace hints, DB profile listing/switching/add/remove flows, interactive DB config prompts, and profiling guardrail updates.
- **amx/cli.py**: Replaced the in-file DB command implementations with delegated imports, keeping command routing and event logging behavior intact.
- **tests/test_regressions.py**: Added coverage for `cmd_profiling()` updating the active DB profile.

## [0.1.74] — 2026-04-26
### Reliability Cleanup
- **amx/agents/base.py**: `apply_logprob_confidence()` now downgrades confidence to `LOW` when logprobs are missing or unusable.
- **amx/agents/profile_agent.py**: Replaced remaining fixed `BATCH_SIZE` usage with the configured `column_batch_size`; added optional non-batch column-name context.
- **amx/agents/orchestrator.py**: Added table/column fallback suggestions for missing model outputs, schema/database reviewability, query-usage hints, and `ReviewResult.alternatives` support.
- **amx/storage/sqlite_store.py**: Added stale `running` run recovery and an `asset_kind` migration guard.
- **amx/utils/console.py**: Added continuously refreshing elapsed time in `step_spinner()`.
- **amx/utils/live_display.py**: Added activity windowing to prevent long-run terminal overflow.
- **tests/test_regressions.py**: Added coverage for missing-logprob confidence downgrades and fallback suggestion injection.

## [0.1.73] — 2026-04-26
### Profiling Guardrails
- **amx/config.py**: Added `DBConfig.profiling_mode`, `profiling_max_rows`, and `profiling_sample_size`; persisted them through DB profile serialization.
- **amx/cli.py**: Added `/db` → `/profiling [full|sampled|metadata] [max_rows|off] [sample_size]` and showed active guardrails in `/config`.
- **amx/db/connector.py**: `profile_table()` now supports `full`, `sampled`, and `metadata` modes and skips full column aggregate scans above the configured row threshold.
- **amx/db/connector.py / amx/cli.py**: Added and used `list_column_profiles()` for code-scan setup so it does not profile every table just to collect column names.

### RAG / Backend Follow-up
- **amx/docs/scanner.py / amx/docs/rag.py**: Added stable `source_root` metadata so remote document profiles filter and refresh correctly after temp-file downloads.
- **amx/codebase/code_rag.py / amx/agents/code_agent.py**: Scoped semantic code chunks and queries to the active code profile/source path.
- **amx/db/adapters/bigquery.py**: BigQuery project/database comment write-back now raises `NotImplementedError`.
- **amx/db/adapters/databricks.py**: Stopped using `numFiles` as a row-count proxy.
- **tests/test_regressions.py**: Added focused regression tests for the above behavior.

## [0.1.72] — 2026-04-26
### History
- **amx/cli.py**: `history results` now renders full alternative lists per column and adds a direct hint for re-picking/applying alternatives via `/review <run_id> --apply`.

## [0.1.71] — 2026-04-26
### UX
- **amx/utils/console.py**: `step_spinner` now refreshes elapsed time every 0.1s in non-live mode so wait-time seconds are visible in realtime.

## [0.1.70] — 2026-04-26
### CLI / History
- **amx/cli.py**: Reintroduced robust run finalization (`finally` + `finish_run`) for `analyze.run`.
- **amx/storage/sqlite_store.py**: Added stale `running` run recovery inside `create_run()`.

## [0.1.69] — 2026-04-26
### Config
- **amx/config.py**: Added `column_batch_size` to `_llm_to_mapping` so `/llm-batch-size` values persist across save/load and profile switches.

## [0.1.68] — 2026-04-26
### CLI
- **amx/cli.py**: Treat `__none__` codebase profile as disabled in `_resolve_codebase_for_run` to avoid spurious errors.

## [0.1.67] — 2026-04-26
### CLI
- **amx/cli.py**: Imported `DatabaseConnector` inside `_analyze_run_logic` to fix profile-switch `NameError`.
- **amx/cli.py**: Updated `analyze_run` wrapper to avoid traceback + duplicate error lines by converting unexpected exceptions to `click.ClickException`.

## [0.1.66] — 2026-04-26
### CLI
- **amx/cli.py**: Initialized safe defaults for interrupt/failure handlers in `_analyze_run_logic` and handled `KeyboardInterrupt` with a clean `User interrupted process.` message (no traceback spam on prompt interrupts).

## [0.1.65] — 2026-04-26
### CLI
- **amx/cli.py**: Fixed the `analyze.run` exception handler indentation so the module imports successfully again.

## [0.1.64] — 2026-04-26
### CLI
- **analyze.run**: Added `step_spinner` around `db.test_connection()`.

## [0.1.63] — 2026-04-25
### Bug Fixes
- **ProfileAgent**: Fixed all remaining `BATCH_SIZE` (constant) references to `batch_size` (property).
- **Orchestrator**: Fixed `process_table` chat-mode reference to `BATCH_SIZE`.

## [0.1.62] — 2026-04-25
### CLI
- **analyze.run**: Moved scope resolution earlier.
- **UX**: Skipped `ask_choice` for review strategy when `total_assets == 1`.

## [0.1.61] — 2026-04-25
### Core
- **LLMConfig**: Added `column_batch_size` (int).
- **ProfileAgent**: Replaced `BATCH_SIZE` constant with a dynamic property.
- **CLI**: Added `/llm-batch-size` command and updated setup wizard.

## [0.1.60] — 2026-04-25
### Live Display
- **Windowing**: Implemented `max_visible` items in `_render_activity_tree`.
- **Dynamic Limit**: Shows 15 items when collapsed, 25 when details are visible.
- **Header**: Added a hint that older items are hidden.

## [0.1.58] — 2026-04-25

### Review Parity
- **`ReviewResult`**: Added `alternatives` field to cleanly pass SQLite alternatives into the orchestrator.
- **`Orchestrator`**: Updated `_review_single_result` to read `r.alternatives` directly.
- **`history_review`**: Replaced custom sequential loop with a conversion to `ReviewResult` followed by `orch.batch_review()`. This ensures the exact same UI/UX (e.g., "accept-all-high") for past runs as live runs.

## [0.1.56] — 2026-04-25

### Bug Fixes
- **CLI Apply**: Fixed scope issue where `hs` (history_store) was not defined in `_on_applied` callbacks.

## [0.1.55] — 2026-04-25

### Hotfix — Live Profile Batch Fail Marker
- `amx/llm/provider.py`
  - Fixed `elapsed_sec` scope/indentation in `LLMProvider.chat()`.
  - `elapsed_sec` is now computed after the `try/except` block unconditionally, so both normal and fallback paths have a defined timing value.
  - Resolves runtime error: `cannot access local variable 'elapsed_sec' where it is not associated with a value`.
  - Side effect fixed: Profile batch activities no longer appear as failed (`x`) due to this regression.

## [0.1.54] — 2026-04-25

### Model Processing Duration Metric
- `amx/llm/provider.py`
  - Added per-call elapsed timing using `time.perf_counter()` inside `LLMProvider.chat()`.
  - Extended `usage` payload to include `model_processing_sec` (even when token usage is absent).

- `amx/utils/token_tracker.py`
  - Extended `_UsageRecord` with `model_processing_sec`.
  - Added aggregate property `total_model_processing_sec`.
  - Included `model_processing_sec` in persisted token records.

- `amx/cli.py`
  - Persisted `model_processing_sec` into `analysis_runs.metrics_json` for both success and failure/cancel finalization paths.
  - Updated `/history list` to display new `Model(s)` column from `metrics_json.model_processing_sec`.
  - Updated `/history stats` to print `avg_model_processing_sec`.

- `amx/storage/sqlite_store.py`
  - `list_recent_runs()` now includes/parses `metrics_json`.
  - `stats()` now computes `avg_model_processing_sec` from stored run metrics.

## [0.1.53] — 2026-04-25

### LLM / Provider Reliability
- `amx/llm/provider.py`
  - Added `_normalized_api_base()` to sanitize provider-specific base URLs.
  - For `provider=ollama`, strips trailing `/v1` from `api_base` to match LiteLLM Ollama endpoint expectations.
  - In `_configure_env()`, normalizes and persists `cfg.api_base` before setting env vars.
  - Suppressed LiteLLM debug/info spill into the TUI by setting `suppress_debug_info=True` and `set_verbose=False` when available.
  - Added one-shot fallback retry on Ollama `404 page not found` by retrying without `/v1` and updating `OLLAMA_API_BASE` in-process.

### Profile Agent Stability
- `amx/agents/profile_agent.py`
  - Added `_profile_batch_workers()`.
  - For `ollama`, `local`, and `kimi`, profile batches now run sequentially (`max_workers=1`) instead of using `ThreadPoolExecutor`.
  - Remote providers keep parallel profile batching (up to 5 workers).

### CLI Setup Defaults
- `amx/cli.py`
  - In `_interactive_llm_block()`, changed default Ollama API base from `http://localhost:11434/v1` to `http://localhost:11434`.
  - Kept `http://localhost:11434/v1` default for OpenAI-compatible local endpoints (`local`, `kimi`).

## [0.1.52] — 2026-04-25

### Bug Fixes
- **Import Error**: Fixed `amx.history` → `amx.storage.sqlite_store` for `history_store` in `orchestrator.py`.

## [0.1.51] — 2026-04-25

### Interactive Run Setup
- **`analyze_run`**: Added sequential `ask_choice` prompts for DB, LLM, Docs, and Codebase profiles if the user confirms "Do you want to modify profiles before run?".
- Reverted all `AMX_ACTIVE_...` environment variable logic in `AMXConfig`.

## [0.1.50] — 2026-04-25

### Multi-Session Support
- **`AMX_CONFIG_PATH`**: Allows specifying a custom config file path.
- **`AMX_ACTIVE_DB_PROFILE`**: Override the active database profile via environment.
- **`AMX_ACTIVE_LLM_PROFILE`**: Override the active LLM profile via environment.
- Updated `AMXConfig.load` and `AMXConfig.save` to respect these overrides.

## [0.1.49] — 2026-04-25

### Bug Fixes
- **Import Error**: Added `from amx.history import history_store` to `orchestrator.py` to fix NameError during deferred review.

## [0.1.48] — 2026-04-25

### High-level Meta & Review Strategy
- **`process_schema_meta`**: New `Orchestrator` method that summarizes table descriptions into a schema description.
- **`process_database_meta`**: New `Orchestrator` method that summarizes schema descriptions into a database description.
- **`review_strategy`**: Added "individual" (default) vs "deferred" choice at start of `/run`.
- **`batch_review`**: Implemented deferred review loop in `Orchestrator`.
- **Local/Ollama Setup**: Made API keys optional in the setup wizard and improved `api_base` / env var handling for local providers.
- **AssetKind**: Expanded with `SCHEMA` and `DATABASE`.
- **Comment Write**: Implemented `set_schema_comment` and `set_database_comment` across all database adapters.

## [0.1.47] — 2026-04-25

### Ctrl+C / Cancel Fix
- Added `except KeyboardInterrupt:` before `except Exception:` in `analyze_run`.
- In `finally` block: `isinstance(_exc_obj, KeyboardInterrupt)` → `_final_status = "cancelled"` regardless of whether results exist.
- `/list` status rendered with Rich color markup: green/red/yellow/cyan per status value.
- `all_results: list = []` initialized before `try:` so it's accessible in `finally` even on early-exit paths.

## [0.1.46] — 2026-04-25

### Status & Display Fix
- `finish_run()` moved into single `finally:` block; `sys.exc_info()[1]` used to detect exception path vs success path inside `finally`.
- `/history results`: `top_level` (column=None) rows split out and rendered as a cyan `Panel` with numbered alternatives before the column table.
- `/history review`: rows sorted by `(is_column_row, id)` so table-level always reviewed first. Added `▶ TABLE DESCRIPTION` / `▶ SCHEMA DESCRIPTION` bold cyan header for top-level items.

## [0.1.45] — 2026-04-25

### Table Description Bug Fix
- Prompt: Added `TABLE_DESCRIPTION_1/2/3:` alternatives to system prompt (matching `n_alternatives`).
- Strict parser: Replaced single-line `TABLE_DESCRIPTION:` capture with a streaming multi-line collector using `re.match(r"TABLE_DESCRIPTION(?:_\d+)?:")`.
- Loose parser: Updated to `re.finditer` across all `TABLE_DESCRIPTION_N:` matches, collapsing into one `MetadataSuggestion(column=None, suggestions=[...all alts...])`.
- Batching: Added deduplication pass after `ThreadPoolExecutor` join — first table-level suggestion wins, rest discarded.

## [0.1.44] — 2026-04-25

### UI Size and Density Match
- Refactored `ActiveTrailSpinner` 2x2 logic back down to a clean single character using the highly dense `dots12` rich spinner array (`⢹⢺⢼⣸⣇⡧⡏⡟`).
- This fixes the UI footprint issue where the spinner was bulky compared to the final `●` marker. The trailing effect is now handled naturally by the physical shape of the 8 dots orbiting inside a single character space.

## [0.1.43] — 2026-04-25

### UI Live Engine Refactoring
- Transformed `LiveDisplay` from a static rendered component manually pushed via `.update()` to a dynamic `__rich_console__` yielding class.
- Instructed `rich.live.Live` to consume `self` directly with `refresh_per_second=10`.
- All `Activity` time tracking naturally re-evaluates 10x per second, giving a smooth fractional realtime clock.
- The `ActiveTrailSpinner` frame tracking smoothly rotates driven by the background polling.

## [0.1.42] — 2026-04-25

### Real-Time UX & Persistence Hotfix
- Fixed an issue where SQLite refused to insert `run_results` due to receiving an `AssetKind` Enum instead of string `.value` via `getattr`.
- Added dynamic Braille spinner (`["⠋", "⠙", "⠹", ...]`) synchronized with `time.monotonic()` directly in the `LiveDisplay._render_activity_tree` active state check.
- Added elapsed time logs around `_run_enabled_agents` and `_human_review` in `Orchestrator.process_table`.

## [0.1.41] — 2026-04-25

### TUI & Performance Optimization
- Added Left/Right arrow key navigation and persistent headers to mimic Claude Code CLI.
- Added `_print_namespace_hint` for all namespaces.
- Centered the ASCII banner text via `Text.assemble(..., justify="center")`.
- Refactored `ProfileAgent.run` to batch column sets into a `ThreadPoolExecutor` for concurrent LLM processing.
- Moved `db.test_connection()` in `_cmd_run_pipeline` until after UI `mode` selection for zero-delay start.
- Updated `/history list` SQLite query to fetch `scope_json` and compute `Target Scope` strings.

## [0.1.40] — 2026-04-25

### Cost Optimization & UX Refinement
- Added `PromptDetail` (minimal, standard, detailed, full) + `/prompt-detail` command.
- Added configurable `n_alternatives` + `/n-alternatives` command.
- Replaced standard scroll navigation with screen-clearing TUI namespaces.

## [0.1.39] — 2026-04-24

### Persistent LLM Alternatives & Re-evaluation System

**Storage (`amx/storage/sqlite_store.py`)**
- New `run_results` table: stores every merged `MetadataSuggestion` set (all alternatives list,
  confidence, source, reasoning) keyed by `run_id` (FK to `analysis_runs`) + `saved_at` timestamp.
- Two indexes: `idx_run_results_run_id` and `idx_run_results_asset` (schema/table/column).
- `save_run_results(run_id, suggestions)` — bulk-insert before human review; returns row IDs.
- `record_evaluation(result_id, *, chosen_description, evaluation)` — updates a row with the
  user's decision (`accepted | skipped | custom`) and `evaluated_at` timestamp.
- `get_run_results(run_id, *, unevaluated_only=False)` — fetch all or pending rows for a run.
- `list_runs_with_result_counts(limit)` — augmented run list with `total_alternatives` + `pending_count`.

**Orchestrator (`amx/agents/orchestrator.py`)**
- `ReviewResult` dataclass: new `result_id: int | None` field (FK to `run_results.id`).
- `Orchestrator.__init__`: new `run_id: int | None = None` parameter; stored as `self.run_id`.
- `_save_merged_suggestions(suggestions, *, asset_kind)` — serializes suggestions to DB before
  `_human_review` is called; returns `{column_name: row_id}` map.
- `_record_evaluation(result_id, *, chosen_description, evaluation)` — thin wrapper around `hs.record_evaluation`.
- `_human_review` / `_review_single`: accept `result_id_map` / `result_id`; call `_record_evaluation`
  for every decision path (accept-all, accept-all-high, reject-all, one-by-one, custom, skip).
- Batch mode (`process_tables_batch_mode`): same save + evaluate integration.

**CLI (`amx/cli.py`)**
- `Orchestrator(... run_id=run_id)` wired in `analyze_run`.
- `/history results <run_id>` — renders saved alternatives table (row ID, table, column, confidence,
  top-3 alternatives, evaluation status, chosen description, eval timestamp).
- `/history review <run_id> [--unevaluated-only] [--apply]`:
  - Loads alternatives from SQLite for the given run.
  - `--unevaluated-only` skips rows that already have an evaluation.
  - Each row shows prior evaluation context; user picks from alternatives or types custom text.
  - Records the new decision in SQLite.
  - Without `--apply`: saves newly approved rows to `pending_metadata.json` for later `/apply`.
  - With `--apply`: connects to DB, applies `COMMENT ON TABLE/COLUMN` immediately.
- Help text + autocomplete updated: `results` and `review` added to `/history` namespace.
- `_history_cmd_heads` frozenset extended with `"results"` and `"review"`.

## [0.1.24] — 2026-04-21

### GitHub URL normalization
- `normalize_github_url()` in `scanner.py`: strips `/blob/…`, `/tree/…`, `/raw/…` and trailing `.git` to extract the repo root URL. SSH URLs are left as-is.
- Used in `_resolve_github`, `test_git_remote_reachable`, and `_clone_if_remote` (analyzer).
- Fixes: pasting a GitHub file URL (e.g. `/blob/main/file.sql`) no longer fails with "repo not found".

## [0.1.23] — 2026-04-21

### CLI + scanner
- `test_source_reachable` / `test_git_remote_reachable` in `scanner.py`; `test_codebase_path_reachable` in `analyzer.py`.
- `/add-doc-profile`, `/setup` doc paths: reachability-only (no `scan_source` clone).
- `/add-code-profile`, `/setup` code path: same for Git + local dir.

## [0.1.22] — 2026-04-21

### Docs
- README: removed Docker / `docker-compose` demo DB section; Quick Start assumes user-supplied PostgreSQL.
- README: LLM providers table — two columns only (provider, config value).
- README: database context bullet shortened to “Usage stats” (no pg_stat column enumeration).
- README: `--config` wording aligned with `amx --config …` startup.

## [0.1.21] — 2026-04-21

### CLI — /docs RAG
- Renamed user-facing command from `/query` to `/search-docs` (and `/similarity` alias) to avoid implying a chat/LLM interface; implementation unchanged (Chroma `query_texts` embedding similarity).
- `/query` kept as hidden Click command + deprecation warn; help, autocomplete, root inference updated.

## [0.1.20] — 2026-04-21

### UX — /add-doc-profile
- Replaced "empty line to finish" batch-then-validate loop with inline validation per path.
- Each path is validated via `scan_source` immediately after entry; success/warning/error shown instantly.
- After each valid path, prompts "Add another path?" (y/N) instead of requiring a blank line.
- Setup wizard doc-profile step uses the same inline-validate + confirm flow.
- Removed dead `_validate_doc_sources` helper.

## [0.1.19] — 2026-04-21

### Cloud document sources — public-first download
- Google Drive: `_gdrive_public_download` attempts `drive.google.com/uc?export=download` for public files. `_gdrive_public_export` handles Google Docs/Sheets/Slides public export. Only falls back to Drive API (`_download_google_drive_file_api`, `_list_google_drive_folder_api`) when credentials are configured.
- SharePoint/OneDrive: `_onedrive_try_public_download` attempts direct download via the sharing URL (with `download=1` fallback). Only falls back to Graph API when `AMX_AZURE_*` credentials are set.
- Shared helper `_download_to_file` for stream downloads.
- `_gdrive_has_api_credentials` / `_graph_has_credentials` guard API fallbacks.
- Error messages now distinguish between "file is private, set credentials" vs "could not parse URL".

### Docs
- README cloud auth section rewritten: emphasizes zero-setup for public links, credentials only for private/folder access.

## [0.1.18] — 2026-04-21

### CLI
- `/schema` + `/table` now require `/db` namespace (or root auto-infer → `/db`).
- Root slash catalog no longer advertises `/schema` / `/table` as global.
- Updated help/copy in analyze + codebase paths + startup summary to point to `/db /schema`.

### Docs
- README command table includes `/db` + `/schema` and `/db` + `/table`.

## [0.1.17] — 2026-04-21

### CLI namespaces
- Added `/llm` and `/code` interactive namespaces + contextual `/help`.
- Renamed DB profile commands to `db-*` variants; old names error with migration hint.
- Namespace gating: doc profile commands only in `/docs`, LLM profile commands only in `/llm`, code profile commands only in `/code`.
- Root slash catalog trimmed to namespaces + global `/save`, `/schema`, `/table`.
- Auto-infer namespace from root prompt for unambiguous command heads (db/docs/llm/code/analyze) with info line.

### Docs
- README updated for namespace grouping + new command names.

## [0.1.16] — 2026-04-21

### UX — /docs namespace
- Reordered `/docs` help + slash autocomplete to prioritize document profile management before RAG operations.
- Intercepted bare `/query` in-session to print usage + example (avoid Click missing-arg output).
- Intercepted bare `/scan` and `/ingest` when no effective doc paths exist; replaced generic error with guided steps.
- Updated `docs_scan` / `docs_ingest` empty-path messaging to reuse the same helper (no `amx docs …` references).
- Updated `docs_query` empty-store message to slash-first workflow.

### Docs
- README interactive command table includes document profile commands under `/docs`.

## [0.1.15] — 2026-04-21

### Added
- `amx/docs/scanner.py`: Google Drive resolution via Drive API v3 (`scan_source` detects drive.google.com / docs.google.com URLs).
  - Service account: `AMX_GOOGLE_SERVICE_ACCOUNT_JSON`
  - User OAuth token file: `AMX_GOOGLE_OAUTH_TOKEN_JSON`
  - Recurses folders; exports Google Workspace native files (Docs/Sheets/Slides) to ingestible formats.
- SharePoint / OneDrive resolution via Microsoft Graph:
  - App-only token via MSAL client credentials (`AMX_AZURE_TENANT_ID`, `AMX_AZURE_CLIENT_ID`, `AMX_AZURE_CLIENT_SECRET`)
  - Share URL → `/shares/{shareId}/driveItem` → download supported extensions; recurse folders.

### Dependencies
- Added `google-api-python-client`, `google-auth`, `msal` to `pyproject.toml`.

### CLI
- Removed hard rejection of Drive/SharePoint URLs in `_validate_doc_sources` (now attempts `scan_source` like other sources).
- Updated user-facing strings that still referenced `amx analyze …` after interactive-only enforcement.

### Docs
- README: cloud auth section + updated supported sources table.

## [0.1.14] — 2026-04-21

### CLI and workflow
- Enforced **interactive-only** execution model:
  - `amx` starts the session, and operational commands are slash-based (`/db`, `/docs`, `/analyze`, etc.).
  - Direct subcommands from terminal (e.g. `amx db connect`) now fail with a guidance message.
- Added `/run-apply` shortcut in analyze namespace, mapped to `analyze run --apply`.
- Updated analyze help text, command listings, and messaging to point to `/run-apply`.

### Apply behavior
- Added pending metadata persistence in `amx/pending_review.py`:
  - save/load/clear approved review items at `~/.amx/pending_metadata.json`.
- `analyze run` now saves approved items for later write-back when not applying immediately.
- Added `analyze apply` command and session `/apply` path to write pending COMMENTs later.
- Refactored DB write-back into `apply_review_results_to_db()` so apply-only path does not depend on LLM initialization.

### Setup and source validation
- Setup DB prompts no longer prefill sample defaults; now require explicit host/port/user/db input with basic validation.
- Added document source connection tests during profile creation and setup:
  - validates local/GitHub/S3 source access by scanning at add time.
  - rejects unsupported Google Drive and SharePoint/OneDrive links with clear guidance.

### Profiling context expansion (sent to LLM)
- Added table-level metadata:
  - PK, outgoing FKs, incoming FKs (upstream/downstream), unique/check constraints.
  - usage stats from `pg_stat_user_tables` (`seq_scan`, `idx_scan`, `n_live_tup`).
  - schema and database comments.
  - related table comments for FK neighbors.
- Added per-column metadata:
  - `existing_comment`
  - `cardinality_ratio = distinct_count / row_count`
- Orchestrator context and profile-agent prompt updated to include all new fields.

### Docs
- README updated to:
  - document write-back support clearly.
  - list supported document sources + unsupported (Drive/SharePoint) explicitly.
  - list supported file extensions explicitly.
  - describe interactive-only command usage.
  - describe exact DB details sent to the profile LLM.

## [0.1.6] — 2026-04-20

### Root cause analysis
Both bugs (raw ANSI + ghost `amx>`) shared one root cause: `prompt_toolkit.patch_stdout()`.
- It wraps `sys.stdout` in a proxy object that Rich cannot detect as a terminal, so Rich falls back to dumping raw escape sequences.
- prompt-toolkit's internal resize handler (SIGWINCH) redraws the prompt line every time the terminal size changes, producing ghost `amx>` lines.

### Fix
- **Removed `patch_stdout()` entirely.** All Rich output now happens *between* `PromptSession.prompt()` calls using the standard `console` from `amx.utils.console`, which correctly auto-detects the real TTY.
- Removed all `_ipt_*` helper functions and `_interactive_console()` — no longer needed since the global `console` works correctly.
- Removed `c: Console | None` parameters from `_handle_session_builtin`, `_cmd_profiles`, `_cmd_use`, `_cmd_add_profile`, `_cmd_remove_profile`, and `_print_session_help`.
- Removed Rich imports (`Console`, `Panel`, `Table`, `Theme`) from `cli.py` — they live in `amx/utils/console.py`.
- Removed `patch_stdout` import.
- Store/restore `SIGWINCH` handler as a safety net.

### Files changed
- `amx/cli.py` — major simplification (~80 lines removed)
- `amx/__init__.py` — version bump 0.1.6
- `pyproject.toml` — version bump 0.1.6

## [0.1.5] — 2026-04-28

### Fixed
- **Raw ANSI in output (`?[1;35m…`)**: Rich treated stdout as non-color under `patch_stdout()` and emitted markup-ish output; interactive session now prints via a forced-terminal Rich console tied to `sys.stdout`.

## [0.1.4] — 2026-04-28

### Fixed
- **Terminal.app reflow**: many stacked `amx>` lines after resizing window during interactive session.
  - Mitigation: wrap the entire interactive session (Rich banner text + prompt loop) in `patch_stdout()` so prompt-toolkit controls stdout during redraw.
  - Also set `mouse_support=False` on `PromptSession` to avoid extra redraw/mouse protocol interactions on some terminals.

## [0.1.3] — 2026-04-28

### Fixed
- **pipx / older prompt_toolkit**: `ModuleNotFoundError: prompt_toolkit.formatted_html` when launching `amx`.
  - Fix: import `HTML` from `prompt_toolkit.formatted_text`.
  - Dependency: raise floor to `prompt_toolkit>=3.0.40` to reduce mismatched installs.

## [0.1.2] — 2026-04-28

### Fixed
- **Interactive session + terminal resize**: duplicated `amx>` lines when resizing the terminal while running slash-session commands.
  - Mitigation: wrap `click` invocations from the interactive loop with `prompt_toolkit.patch_stdout.patch_stdout()` so stdout is owned by the prompt UI during reflow.

### Added
- **Esc navigation** in interactive session:
  - `Esc` on an empty prompt exits nested namespace back to root (Claude Code-like “go back”).
  - `Esc` when text exists clears the current line (quick cancel).
- **Bottom toolbar hints** in interactive session (↑/↓, Enter, Esc, Ctrl+C).

### Changed
- **Help readability**: root `/help` examples now render commands in `bright_white` instead of theme “info” cyan/magenta that looked low-contrast in some terminals.

### Repo workflow
- `CHANGELOG.md` remains **ignored by git** in this repository (per maintainer preference), while a short public changelog is still committed for GitHub consumers.

---

## [0.1.1] — 2026-04-28

### Changed
- **Interactive session UX** (`amx/cli.py`): increased contrast for prompt-toolkit completion menu meta text so `/` autocomplete stays readable on gray menus.

---

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
