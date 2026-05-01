# AMX — Agentic Metadata Extractor

AI-powered CLI application that automatically infers, reviews, and applies metadata (descriptions, tags) to database assets — tables, views, and materialized views — using a multi-agent system with human-in-the-loop validation.

## Problem

Enterprise databases accumulate thousands of tables and columns without proper documentation. Technical identifiers in any language or vendor dialect can be cryptic, and understanding what they represent requires institutional knowledge, scattered documentation, or deep code archaeology. AMX automates this discovery process.

## How It Works

AMX employs three **sub-agents** that independently analyze your data and then merge their findings:

| Agent | Source | What It Does |
|-------|--------|-------------|
| **Profile Agent** | Database | Analyzes column types, statistics (min/max/null counts/distinct values), sample data, comments, and relationships to infer meaning |
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
- `v0.6.3`: **Stop writing auto-inference fallback placeholders to the live database.** User reported their DB had `Column rewrt in table bseg. Auto-inference missed a reliable description; please review manually.` written as the actual `COMMENT ON COLUMN` for several columns. The placeholder was meant as a UI hint for human review (`_ensure_complete_table_coverage` injects it when the LLM misses a column), but it flowed through to `apply_review_results_to_db` and got persisted as metadata. Three fixes: (1) `is_placeholder_description` predicate filters them out at the top of `apply_review_results_to_db`. (2) `missing-only` filter now treats placeholder comments as "still missing" so legacy DBs auto-clean on the next `/run-apply`. (3) New `/db cleanup-placeholders [schema]` slash command for one-shot scrub of legacy pollution.
- `v0.6.2`: **`/ask "tables without description"` now skips system / extension assets** (e.g. `pg_stat_statements`, `pg_statio_*`). User reported these showing up as "tables without descriptions" — but they're PostgreSQL extension views, not user tables, and the `/run` flow has been filtering them out via `services.analyze_scope.is_non_business_asset` for releases. The `find_assets_missing_comment` agent tool now reuses the same filter so coverage queries are consistent across `/run` and `/ask`. New `include_system: bool` parameter (default false) lets the LLM opt in only when the user explicitly asks about system views.
- `v0.6.1`: **`/description-verbosity` is now visible in `/llm` namespace help + autocomplete.** v0.6.0 added the slash command but only registered it in the dispatch handler — it was missing from the namespace help text (`/help` inside `/llm` didn't list it) and from `_slash_command_catalog` (autocomplete dropdown didn't show it). Now wired through all four discovery paths: dispatch, routing (`llm_cmd_heads`), help, and autocomplete catalog.
- `v0.6.0`: **auto-apply is now fully unattended + description-verbosity toggle + tables-without-comments tool.** Five user-reported issues addressed: (1) `/llm description-verbosity brief|detailed` lets you pick 1-sentence vs 2-4-sentence column descriptions (separate from `/prompt-detail`, which controls input context). (2) auto-apply was still asking for confirmation at the schema/database meta step and at the end-of-run apply prompt — both now skipped when the user picked auto-apply. (3) `/ask "tables without description"` was reading stale catalog data; new `find_assets_missing_comment` tool queries the LIVE DB and is routed to via system prompt. (4) `analysis_runs` migration now also runs at the top of `create_run` as a safety net for users whose `init()` ran on stale code (pipx editable install quirks) — `Processed` column should populate reliably. (5) Skipped: `/ask` "Only those?" follow-up was already covered by v0.4.x conversational memory fixes; verifying.
- `v0.5.9`: **Reasoning-model failure mode is now fatal, not a soft retry.** User on `openrouter/tencent/hy3-preview:free` saw the model burn every output token on internal thinking and return `content=""` with `finish_reason=length` on every batch — the soft `LLMTruncationError` was caught per batch and the run kept iterating, never producing useful work. Now: when `finish_reason=length` AND `content == ""`, raise `FatalLLMError` so the run aborts after one attempt with a model-recommendation message (paid `gpt-4o-mini` / `claude-3-5-haiku` / `gemini-1.5-flash` for non-reasoning, or `AMX_LLM_MIN_MAX_TOKENS=32000` + `AMX_REASONING_EFFORT=minimal` for users who insist on a reasoning model). Saves hours of confused retries and burned quota on free / preview tiers that aren't suitable for AMX's structured-JSON output.
- `v0.5.8`: **Auto-apply now writes per-table immediately + Processed column accuracy.** User completed bkpf, Ctrl+C'd during bseg, found bkpf marked applied=True in catalog but its COMMENT ON SQL never reached the live DB because the batch apply step at the end of the run never executed. `process_table` now calls `apply_review_results_to_db` for the table's results before returning when auto_apply=True, so partial completion = partial live-DB state (and the missing-only filter on retry skips what's already there). Plus: the `Processed` column in `/history list` was stuck at `—` even when 6 tables filter-skipped, because the `update_run_planned_count` formula used `len(skipped_assets)` which only counts `ProfilingError` skips, not filter skips. Separate `filter_skipped_count` now drives the math correctly.
- `v0.5.7`: **Abort `/run` immediately on fatal LLM errors (402 / 401 / 403 / 404).** User reported their OpenRouter account hit 402 mid-run; AMX kept retrying every batch on every remaining table, accumulating 1090s and 111K tokens of failed attempts before manual Ctrl+C. New `FatalLLMError` class + `_classify_fatal_llm_error` detector (matches both `status_code` and body patterns: "more credits", "insufficient_quota", "invalid api key", "model not found", "can only afford"). `LLMProvider.chat` raises it before the retry loop kicks in; `ProfileAgent` lets it propagate AND cancels sibling parallel-batch futures so the executor drains fast; `execute_analyze_run` catches it and prints `LLM run aborted: <user_message>` plus a hint that the missing-only filter will skip already-finished tables on the retry. The previous behavior — thousands of warnings + `analysis_runs` row that looked like 'AMX did 87 things' — replaced with a single actionable exit.
- `v0.5.6`: **Fix amx startup crash + cap LLM call duration at 180s.** `v0.5.5` accidentally inserted `_raise_open_file_limit` between the `@click.group(...)` decorator stack and `def main(...)`, so the decorators landed on the helper and `amx` itself crashed at import time with `AttributeError: function object has no attribute 'group'`. Helper moved above the decorator stack — fixed. Also: a single profile batch was hanging at 9m58s while peers finished in 1m. LLM calls now carry a 180s default timeout (tunable via `AMX_LLM_TIMEOUT_SEC`); on expiry the existing retry-with-backoff kicks in automatically instead of silently waiting forever.
- `v0.5.5`: **Make the FD-leak fix open-source-friendly.** Two complementary changes so end users never see the `Too many open files` crash without manually setting `ulimit -n`. (1) `amx/cli.py:_raise_open_file_limit` lifts the soft NOFILE limit to 4096 (capped at hard) at startup via `resource.setrlimit` — cross-platform safe, no-ops on Windows. (2) `SearchService` now caches one live DB connector across its lifetime and disposes it via `close()`; the class is a context manager and every `/search ask` callsite (including hidden `find-columns / join-candidates / explain / explain-table`) wraps `svc` in `with svc:`. Combined with the v0.5.4 ToolBox fix, the tool-agent path and the legacy planner path both stop leaking after each question.
- `v0.5.4`: **Fix file-descriptor leak in `/ask` tool agent + observable migration.** Each `/ask` question instantiated a fresh `ToolBox` → fresh `DatabaseConnector` → fresh SQLAlchemy engine + pool, but never disposed it. After several turns the FD count crossed macOS/Linux ulimit and `prompt_toolkit.prompt` crashed with `OSError: [Errno 24] Too many open files` (the user-reported case). `ToolBox` is now a context manager; `run_tool_agent` wraps the loop in `with` so the connector is disposed each turn. Plus: `analysis_runs` migration now probes `PRAGMA table_info` first, logs every column add, and logs true failures at WARNING — helps diagnose why `/history` shows `—` for `Processed` when migration didn't apply.
- `v0.5.3`: **OpenRouter models with non-OpenAI vendor namespaces now route correctly.** `provider=openrouter, model=qwen/qwen3.5-flash-02-23` used to fail with `LLM Provider NOT provided` because `LLMProvider.model_name` early-returned for any model id containing a slash, bypassing the `openrouter/` prefix. OpenAI-prefixed models (`openai/gpt-4o-mini`) happened to work via LiteLLM's OpenAI client + api_base override, but `qwen/`, `mistralai/`, `meta-llama/`, `google/`, `x-ai/`, `moonshotai/`, etc. had no fallback. Fix: the prefix is now always applied (`openrouter/<vendor>/<model>` is the canonical form OpenRouter expects); `model_name` skips it only when `raw` already begins with it.
- `v0.5.2`: **Honest progress reporting in `/history`.** A user starting a run on the remaining 60 tables out of a 78-table schema, then hitting Ctrl+C after 3 tables completed, used to see `Target Scope: 78 tables, Status: ready_for_review` — wrong on both axes (78 implies all of them, ready_for_review contradicts auto-apply). New `selected_count / planned_count / processed_count / applied_count / review_strategy` columns on `analysis_runs` record real intent vs reality separately. New `Processed` column in `/history list` renders as `processed/planned` (e.g. `3/60`), with an `applied N` annotation when divergent. auto-apply runs that get Ctrl+C'd now land in `cancelled` instead of `ready_for_review`, since the user explicitly opted out of review. Counter-update helpers commit per-row so partial progress survives Ctrl+C even when `finish_run` is never reached.
- `v0.5.1`: **Third review strategy: `auto-apply`.** The review-strategy picker (shown after the scope + coverage filter pickers when more than one asset is selected) now offers `individual / deferred / auto-apply`. With `auto-apply`, the orchestrator accepts each entity's top LLM suggestion as the final description, marks it `applied=True`, records it as `evaluation=accepted`, and writes it through `sync_review_decision` to the catalog — all without prompting. When combined with `/run-apply`, the comments land in the live DB at the end of the run; with plain `/run` the catalog updates but the DB write is deferred (with a warning). Safety prompts spell out the trade-off: `existing comments inside the chosen scope will be replaced`. Default stays `individual` so existing flows are untouched.
- `v0.5.0`: **Coverage filter for `/run` and `/run-apply`.** After the scope picker (Database / Schema / Asset / Default), AMX now asks `Run for which assets / columns? — missing-only / all`, defaulting to `missing-only`. Tables that already have a table comment AND every column commented are skipped entirely with an info line; tables with partial coverage have their column list narrowed to the gaps before the Profile / RAG / Code agents see them; tables with all column comments but a missing table comment drop column work and analyze only the table-level description. Wired through both chat mode and batch mode (`process_tables_batch_mode`). Solves the user-reported pain of re-paying for LLM tokens on already-curated databases (e.g. a 100-table SAP DB where 95 tables are already commented and the user only wants to fill the 5 gaps, or a wide table that grew from 10 to 12 columns and only needs analysis on the 2 new columns). The user can still opt into `all` to overwrite — the prompt makes the trade-off explicit.
- `v0.4.4`: **Keystrokes are now visible during interactive prompts.** When a `LiveDisplay` was active during `/run`, `/setup`, or `/search sync`, the 10 Hz Rich refresh painted over the user's keystrokes between frames — pressing `2` then Enter still worked but the `2` never appeared on screen. New `_live_paused_for_input()` context manager pauses the live region while `prompt_toolkit.prompt` reads stdin, then resumes it after. Wired into every interactive helper: `ask`, `ask_password`, `ask_choice`, `ask_multi_choice`, `confirm`. No-op when no display is active.
- `v0.4.3`: **Live display no longer leaves stacked header bars.** The running `AMX v0.4.x  openrouter/openai/gpt-4o-mini │ SEARCH  Xs` panel is now `transient=True`, so the entire live region (header + thinking spinner + active pipeline tree) clears when `stop()` runs. Previously every height change in the renderable left a frame behind in the scroll buffer, producing 2–4 stacked "SEARCH 2s / 3s / 9s" bars per question. To preserve the pipeline tree as a useful summary, `LiveDisplay.stop()` now re-prints a quiet single-block `Pipeline` tree with check-marked steps and durations once the live region clears.
- `v0.4.2`: **`/ask` chat session id now survives across REPL turns.** The interactive REPL re-runs Click's `main()` for every `/ask <q>` line, which calls `AMXConfig.load(cfg_path)` and rebuilds `ctx.obj`. Because `active_chat_session_id` is intentionally ephemeral (not in `_PERSISTED_FIELDS`), every question used to open a brand-new session — `_handle_meta_query` then read an empty store and answered "this is the first question in this session" even when the user had asked several. The id now rides on an `AMX_CHAT_SESSION_ID` environment variable: `_run_ask_repl` writes it on entry, `SearchAgent._ensure_session_id` mirrors it whenever a session is created, and `AMXConfig.load` reads it back. All turns inside one `/ask` REPL session now land in the same `chat_sessions` row.
- `v0.4.1`: **Conversation memory + cross-DB awareness fixes for the tool agent.** The current user question used to be written to the session store before short-circuits ran, then forwarded as BOTH `prior_turns` context AND the live user message — so follow-ups like `"Only those?"` came back as "your question is unclear". `_answer_via_tool_agent` now drops the duplicated trailing entry. Chitchat / meta-query / reaffirmation short-circuits also now write a synthetic assistant row so memory pairs stay balanced. Assistant-summary truncation bumped from 200 to 1000 chars so long prior answers (boolean-column rollups, schema dumps) survive intact for follow-up resolution. **Two new agent tools:** `find_columns_by_dtype(dtype)` returns columns whose dtype matches a SQL type token with FAMILY support (`boolean` → BOOL/BOOLEAN, `int` → BIGINT/INTEGER/SMALLINT, `date` → DATE/TIMESTAMP, ...) — fixes "which tables have boolean columns?" hitting every dtype-BOOLEAN column instead of only fuzzy-name matches. `find_joinable_tables(table)` takes ONE table and returns the tables it can be joined with — fixes "which tables can I join with adr6?" no longer bottoming out at `public.adr6` (a non-existent table). **Cross-DB profile awareness in the system prompt:** lists every connected DB profile (name + backend + database, with `(active)` marker) plus a note that tools target the active profile only and `/use-db <name>` switches. New tests cover memory-pair persistence and the duplicated-turn fix.
- `v0.4.0`: **`/ask` is now a tool-calling agent.** The regex-routed Pass1/alignment/retrieval cascade is replaced with a thin loop where the LLM is handed a fixed set of metadata tools (`list_schemas`, `list_tables_in_schema`, `find_table_by_name`, `describe_table`, `search_tables_by_concept`, `search_columns_by_concept`, `get_join_candidates`, `list_databases`) and decides itself which to call. The system prompt now ships the live database name, schema list, and pinned schema/table directly, so the model doesn't have to guess whether `sap_test` is a schema or a table. This fixes the user-reported "What's the tables under sap_test" → "table named `under` not found" regression and the broader class of regex over-matches that the 0.3.x line kept patching. Bounded at 6 iterations per question; deterministic short-circuits for chitchat, meta-query, and reaffirmation still run before any LLM call. The legacy planner stays as a fallback under `/search config use_tool_agent false`. Tools API: `LLMProvider.chat` now extracts `tool_calls` from the LiteLLM response across OpenAI/Anthropic/Gemini/OpenRouter providers; new `ToolCall` dataclass and `ChatResult.tool_calls` field. New tests cover end-to-end tool-call routing and plain-text fallback.
- `v0.3.5`: **Strong-vs-weak explicit-mention strength** unblocks two more `/ask` failure modes. Mentions captured via `<token> table` / `table <token>` / `schema.table` patterns are tagged `strength="strong"` (the user explicitly called the noun a table) and now override the LLM's mode UNCONDITIONALLY — no catalog match required. This fixes the user-reported case where "which schema have vbrk table" drifted to a generic "couldn't find" answer because vbrk was in live DB but not yet sync'd into the catalog. Subject-form patterns (`what's the X` / `describe X` / `X nedir`) stay tagged `strength="weak"` and still require catalog or live-DB confirmation, preventing column-shaped tokens like `vbrk_id` from being mis-routed to a missing table. Plus `_catalog_resolvable_subject` now ALSO probes the live DB when `current_schema` is set, so weak subject-form mentions get one more chance to confirm. **Follow-up reaffirmation short-circuit** ("Are you sure?", "really?", "is that right?", "why?", "emin misin?", "gerçekten mi?", "neden?", "öyle mi?") now restates the prior assistant turn verbatim instead of falling through to the unhelpful clarification reply. Two new tests assert "which schema have vbrk table" reroutes correctly with an empty catalog, and "Are you sure?" doesn't consume a new LLM response.
- `v0.3.4`: **`/ask` quality fixes for chitchat, meta-queries, and inventory-mode drift.** Greetings ("nasılsın", "hi", "hello", "merhaba", "thanks", "ok", "günaydın", ...) now short-circuit before the LLM planner with a friendly bilingual redirect that names what AMX actually does, instead of falling through to the unhelpful "Could you clarify the exact scope?" clarification reply. **Meta-queries about the chat itself** ("what was my previous question?", "bir önceki sorum neydi", "ben ne sordum") answer directly from `ChatSessionStore.recent_turns` — returning the literal prior user-turn text — so users can verify what they asked without the planner making up an interpretation. **Catalog-grounded alignment guard** narrows the override condition: `_align_plan_shape` now reroutes to `table_explain` only when the extracted subject token is an EXACT-name catalog table (`SearchCatalog.find_tables_by_exact_name`). The protected-mode set was also tightened to `{table_explain, join_candidates, joinable_tables, count_tables, check_coverage}` — `list_databases`, `list_schemas`, `schema_inventory`, `semantic_concept`, `name_lookup`, and `compare_entities` are no longer protected, so `"vbrk tablosu var mı bizde"` no longer drifts to a `list_databases` answer. Clarification is skipped when the question already names a catalog-resolvable subject (no more "could you clarify?" right after the user named a real table by exact name). Two new tests assert chitchat hits zero LLM calls and meta-queries return the literal prior question.
- `v0.3.3`: **Bare `/ask` enters a sticky `ask>` REPL** — typing `/ask` with no question now opens a question-only inner prompt that re-uses the same persistent chat session; each line dispatches as `/search ask <line>`, `/exit` (or empty Ctrl-D) leaves. **`what's the <X>` no longer drifts to an unrelated table** — four new regex branches catch subject-form questions ("what's the X / what is X / describe X / explain X / X nedir / X hakkında / anlat bana X / açıkla X"), expanded stopword list filters meta-words (`table/column/schema/data/info/most/least/popular/...`), and a new `_align_plan_shape` guard re-routes to `table_explain` so target resolution actually runs. Unqualified mentions ("describe vbrk" with no current_schema) are now resolved via a new `SearchCatalog.find_tables_by_exact_name` lookup: exactly-one-match → resolved; multiple matches → "exists in more than one schema, which one?" with all candidates listed; zero matches → "could not find a table named `vbrk`" plus fuzzy suggestions. Default `/ask` output is uncluttered: provenance/confidence info lines moved behind `--debug` or explicit `/search config show_provenance true` opt-in (defaults flipped from `true` to `false`). The `table_summary` ("what is table X") rendering drops the empty header row, lifts schema.table into the panel title, and never shows "-" placeholders for missing column names.
- `Unreleased`: **`/ask` is now a stateful conversational agent** — each `/ask` invocation appends a turn to a persistent SQLite-backed session (two new tables in `~/.amx/history.db`); follow-ups like "any others?" / "what about its columns?" survive `/exit` + restart. New `/session new|list|resume|end|show` commands manage the persistent conversations (resume refuses cross-profile to prevent stitching unrelated histories). LLM-driven context compaction summarises the oldest slice with a single LLM call when live turns exceed ~40% of the model's input budget (Claude 60K, Gemini 100K, OpenAI/DeepSeek 24K), with a stub fallback for no-LLM mode. Default `/ask` output is now natural-language first plus a clean `Schema.Table | Match | Why | Rows | Cols | Description` table — High/Medium/Low confidence bands replace abstract numeric scores, and the `Why` column surfaces the matched columns from the existing `matched_columns` field. Internal pipeline noise (Thought Trace, raw scores, source kind) moved behind a new `--debug` (alias `--verbose`) flag. **Profile persistence fix** — newly-created DB and LLM profiles now actually survive `/exit` + restart with their fields populated; an autosave race in `set_active_*_profile` was wiping the just-added profile's data with the previous active mirror, surfacing as "newly created profiles are gone, ghost test profile from a previous session loads instead". The interactive session now also prints `Config: ~/.amx/config.yml` at startup so the on-disk path is visible at all times. **Answer-shape rework for `/search /ask`** — superlative/top-K inventory questions ("which table has the most rows in `sap_s6p`") get a one-sentence headline instead of an 80-row dump. **Production-readiness sweep** across release engineering, security, search, connectors, and observability. **Search:** pluggable embeddings (MiniLM default, OpenAI-compatible for OpenAI/OpenRouter/Together/Mistral/Azure/vLLM/LM Studio, local sentence-transformers) configurable via the new `/search /embeddings` command; per-profile Chroma collections eliminate cross-profile pollution; LLM transient errors (429 / timeout / 5xx) auto-retry with exponential backoff. **Connectors:** auth / network / SSL / missing-DB errors get distinct actionable hints from a categorised `ErrorMapper`; new `/db /inspect [profile]` for self-service diagnosis (visible schemas, table counts, capability flags); DB connection retries on transient failures. **Security & UX:** DB passwords / LLM API keys / Databricks tokens are now stored in the OS keyring (macOS Keychain, Windows Credential Manager, Linux Secret Service) — the YAML keeps only opaque `keyring:...` references and migrates plaintext on the next save; fresh installs no longer create a phantom `localhost / amx_pass / SAP` "default" profile; `--debug` flag and a themed top-level crash handler replace raw tracebacks; `~/.amx/config.yml` is `chmod 0o600`. **Observability:** the on-disk log is now JSON-Lines with a per-run `request_id` so `jq 'select(.request_id == "...")' ~/.amx/logs/amx.log` extracts a single trace; new top-level `/usage [window]` summarises local LLM token and approximate-cost spend. **Engineering:** GitHub Actions CI (ruff + mypy + pytest on Python 3.10/3.11/3.12) and tag-driven PyPI release via OIDC Trusted Publisher; `python-semantic-release` derives versions from Conventional Commits; pre-commit hooks (ruff + gitleaks); 29 new DB-adapter unit tests across all four backends; `CONTRIBUTING.md`, `SECURITY.md`, `LICENSE`. Run `pip install -e ".[dev]"` for the dev tooling, or `pip install "amx[local-embeddings]"` for offline sentence-transformers embeddings.
- `v0.3.2`: Hardened profile persistence transactions: profile upserts no longer write an intermediate YAML snapshot while add+activate is still in progress, and LLM profile creation now uses the same transaction pattern as DB profiles.
- `v0.2.9`: `/run` now shows the LLM health-check with the same live progress/timing style as DB connect, and missing-LLM guidance now points to `/llm`, `/add-llm-profile`, and `/setup`.
- `v0.2.8`: Fixed `/add-db-profile` overwriting the active profile with its old backend during autosave; replacing an active PostgreSQL profile with Databricks now persists the Databricks connection settings correctly.
- `v0.2.7`: `/analyze /run` now performs an LLM health-check before profiling starts, Profile Agent failures are surfaced as explicit AMX warnings, and LiteLLM warning/debug spill is suppressed unless AMX chooses to show a message.
- `v0.2.6`: Reworked `/add-db-profile` editing so profile updates are deterministic: `Enter` keeps the current value, `-` clears optional fields, and Databricks TLS choices no longer depend on ambiguous prompt behavior.
- `v0.2.5`: Added a deterministic `db tls` command so Databricks TLS settings can be set and verified from the app without relying on interactive yes/no prompts.
- `v0.2.4`: Databricks `db connect` now uses the native `databricks-sql-connector` test path directly, so TLS and invalid-token failures are classified more accurately before SQLAlchemy-based introspection starts.
- `v0.2.3`: `db connect` now runs staged Databricks recovery attempts, reports which TLS mechanism passed, and persists the successful CA bundle or last-resort no-verify setting back into the active profile.
- `v0.2.2`: Hardened Databricks corporate TLS setup by expanding trusted CA paths, honoring CA bundle environment variables, and reporting missing CA bundle files with a direct remediation message.
- `v0.2.1`: Added the SchemaExplorer macro-vision tool, inventory/definition/relationship/deep-dive ask strategies, set-based Markdown synthesis for column-count/table-inventory questions, and thought-trace visibility for schema inventory tool use.
- `v0.2.0`: Added the headless `AMXApplication`/`amx.init()` API, UMI-normalized profile entities, rule-purged semantic join scoring, description-only weighted logprob confidence, visible `/search ask` thought traces, tool-loop ask aliases, actionable error mapping, RAG reranking, SQLite audit columns, and write-through state persistence.
See `CHANGELOG.md` for older release history.

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
| `/db` + `/tls [on\|off] [ca_path\|clear]` | Show or set Databricks TLS settings directly on the active profile. |
| `/db` + `/schema <name>` | Set default schema context (used by /tables, /analyze, …) |
| `/db` + `/table <name>` | Set default table context (used by /profile, /analyze, …) |
| `/db` + `/connect` | Test database connectivity |
| `/db` + `/schemas` | List available schemas |
| `/db` + `/tables [schema]` | List all assets (tables, views, materialized views) in a schema |
| `/db` + `/profile [schema] [table]` | Profile table structure and data |
| `/db` + `/inspect [profile]` | Diagnose a profile: backend, capabilities, connection test, visible schemas, table counts. Read-only. Helpful when metadata looks incomplete. |
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
- Direct changes to loaded top-level and nested DB/LLM config fields are write-through as well; AMX saves the active `config.yml` atomically after each mutation.
- `/analyze /run` now tests the active LLM before profiling any asset and stops immediately if the model/profile is unreachable or deactivated.
- Third-party LiteLLM warnings/debug lines stay out of the terminal by default; AMX surfaces only its own actionable warnings.
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
| `/search` + `/ask [--actions] [--debug] <question>` | Ask conversational metadata questions with LLM grounding over the internal search catalog. Each `/ask` appends to a persistent session (`~/.amx/history.db`), so follow-ups like "any others?" or "what about its columns?" remember prior turns even after `/exit` and restart. The default output is a natural-language summary plus a clean `Schema.Table | Match | Why | Rows | Cols | Description` table; `--debug` (alias `--verbose`) reveals the planner's Thought Trace and adds raw `Score`/`Source`/`Conf` columns. `--actions` prompts before running approved follow-up actions. |
| `/session` + `/new \| /list \| /resume <id> \| /end \| /show` | Manage `/ask` conversation sessions. `/session new [--title]` starts a fresh chat session and pins it as active; `/session list [-n N] [--all-profiles]` shows recent sessions with first-question excerpts; `/session resume <id>` switches the active session pointer (refuses cross-profile resume); `/session end` closes the current session; `/session show [--id N] [--include-compacted]` dumps the per-turn audit trail. New REPL boots always start fresh — the user opts back in via `/session resume`. When a session's live token estimate exceeds ~40% of the model's input budget, AMX summarises the oldest slice with a single LLM call (Claude/Gemini/OpenAI/DeepSeek) and replaces it with a synthetic summary turn so follow-ups still ground against compacted history; falls back to a stub when no LLM is available. |
| `/search` + `/status` | Show catalog counts, freshness, and recent sync jobs |
| `/search` + `/sources` | Show enabled search settings and evidence-source coverage |
| `/search` + `/config [key] [value]` | View or update `/search` settings for the active DB profile |
| `/search` + `/context-detail [minimal\|standard\|rich\|deep]` | Control how much catalog/code/history context `/search` sends into grounded reasoning |
| `/search` + `/sync [--schema …] [--table …]` | Sync DB structure/comments and cached code evidence into the catalog |
| `/search` + `/rebuild` | Rebuild effective search state and the `amx_search` vector index |
| `/search` + `/embeddings [kind] [model]` | Show or change the search-index embedding provider. Kinds: `MiniLM` (default, offline), `OpenAI-compatible` (OpenAI / OpenRouter / Together / Mistral / Azure / vLLM / LM Studio), `Local` (sentence-transformers, requires `pip install "amx[local-embeddings]"`). Run `/rebuild` after switching to re-embed the catalog. |
| `/usage [window]` | Local LLM token + approximate-cost summary read from `~/.amx/history.db`. Windows: `24h`, `7d` (default), `30d`, `all`. No network calls. Pricing built in for OpenAI / Anthropic / Gemini / DeepSeek; unknown models show `—` for cost. |
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

- AMX now uses Databricks' native Python SQL connector for the `db connect` health check itself, while continuing to use the Databricks SQLAlchemy dialect for metadata inspection and normal runtime SQLAlchemy flows.
- `db connect` now tries Databricks connectivity in stages: saved profile first, then a CA bundle discovered from supported environment variables, then `tls_no_verify` as a last resort. The first successful recovery path is saved back into the active DB profile and printed in the terminal.
- Set a **Trusted CA bundle path** in the Databricks DB profile to point at your corporate/root CA PEM bundle.
- The path may use `~` or environment variables such as `$HOME/certs/company-ca.pem`; AMX expands it before opening the Databricks connection.
- If the profile field is empty, AMX checks `AMX_DATABRICKS_TRUSTED_CA_FILE`, `DATABRICKS_TRUSTED_CA_FILE`, `REQUESTS_CA_BUNDLE`, then `SSL_CERT_FILE` and passes the first configured bundle to the Databricks connector.
- If you do not have the CA bundle yet, you can temporarily enable **Disable TLS certificate verification** in that DB profile. This is insecure and should only be a last resort for internal troubleshooting.

Example profile fields in `~/.amx/config.yml`:

```yaml
db_profiles:
  company-databricks:
    backend: databricks
    host: adb-4217046554757008.8.azuredatabricks.net
    http_path: /sql/1.0/warehouses/cdda8fcb11f4c83b
    catalog: dap_eu_60_prod
    database: dev
    tls_trusted_ca_file: ~/certs/company-root-ca.pem
    tls_no_verify: false
```

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
- broad structural questions such as "how many columns per table?" use SchemaExplorer and return set-based Markdown tables with table names, column counts, row counts, and semantic clusters instead of a single best match
- **superlative and top-K inventory questions answer with one fact, not a dump**: prompts like "which table has the most rows in `sap_s6p`", "satır sayısı en fazla olan tablo hangisi", or "top 5 tables by row count" produce a one-sentence headline naming the top table (or a 2–5 row markdown table for top-K) instead of dumping all tables. The agent picks one of seven explicit *answer shapes* — `single_fact`, `short_table`, `full_table`, `ranked_list`, `table_summary`, `join_candidates`, `prose` — emitted by the interpretation pass and respected by both the deterministic formatters and the LLM synthesis prompt
- the bottom Rich "Search matches" table is suppressed for inventory and prose answers (where the headline already carries the data) and now filters out rows whose score is exactly `0.00` so inventory leakage and zero-score diagnostics never leak into the user-facing match list. Inventory rows render in a dedicated Schema | Table | Columns | Rows | Cluster table when shown
- `/search ask` now distinguishes table-level semantic discovery from inventory questions, so prompts like "which tables contain address details" route to ranked table matches instead of accidental table-count answers
- `/search ask` uses an LLM-native interpretation pipeline with deterministic safeguards only as resilience fallbacks; semantic results are grounded by lexical, structural, statistical, and documentation evidence instead of vendor-specific naming rules
- inventory/count questions such as schema lists or table counts use live DB introspection so they remain correct even if only part of the catalog has generated descriptions
- semantic questions use effective metadata first, with exact/fuzzy name matching, multilingual query variants, and vector support as an independent fallback when lexical terms do not match
- synthesized answers still receive the visible grounded result set, but `/search` now suppresses low-confidence tail rows before answering so weak vector-only matches do not dominate the user-facing summary
- `/search` prompting now treats explicit/live evidence as stronger than semantic fallback evidence, and its interpreter prompt is more conservative about follow-up scope, ambiguity, and enum selection
- table-scoped factual questions are live-first: questions such as "what is the ADRC table?" or "are all ADRC columns commented?" resolve the requested table and run safe live metadata probes before answering structural facts like column count, types, nullability, table comments, and column-comment coverage. Open-ended semantic column searches, such as "city related column names", stay on catalog/vector retrieval unless the user explicitly scopes them to a table.
- table-scoped factual verification now uses deterministic read-only probe selection instead of a second LLM planning hop, so `/search` can take safe metadata actions automatically without drifting into irrelevant planner output
- explicit table mentions such as `schema.table`, `ADRC table`, or `adrc tablosunda` take precedence over fuzzy catalog matches. If the exact live table cannot be verified, AMX refuses to substitute a similar candidate such as `ADR6`; fuzzy matches are shown only as suggestions.
- `/search` only labels an answer as live verified when live metadata rows were actually collected; catalog-only or fuzzy evidence is capped to lower confidence.
- join questions prioritize verified FK relationships, then semantic join inference, then observed code usage; one-table join questions can also surface non-FK semantic candidates with confidence bands such as `verified`, `high_likelihood`, `possible`, and `weak_hypothesis`
- join answers now prefer a deterministic short-form summary when the top verified join target or join-column pair is clear, reducing unnecessary LLM synthesis
- follow-up questions reuse narrower session memory so users can keep discussing the same table or field naturally without broad semantic result sets contaminating later table-scoped questions
- `/search ask` shows live progress while AMX interprets the question, retrieves evidence, and synthesizes the answer
- `/search ask` records retrieval policy, evidence sources, ambiguity flags, per-stage timings, suggested actions, executed read-only actions, answer strategy, and suppressed-row counts into history/event payloads so answers remain diagnosable
- `/search ask` now also records a concise thought trace of observable planning/tool stages (`interpret_question`, `metadata_query` or `schema_explorer`, `data_peek`, `verify_evidence`) for debugging without exposing hidden model chain-of-thought
- `/search ask --actions` turns selected suggestions into a human-approved execution loop: AMX asks before running catalog sync, cached code-evidence refresh, or single-table metadata analysis actions, then records the action outcome
- `/search /context-detail` controls how much neighborhood, code, and history context is exposed to the search synthesizer for cost/latency tuning
- `/search` answer language is forced to the detected language of the user's question, even if the interpreter LLM suggests a different `answer_language`
- aggregate answers avoid dumping the generic schema/table/column result grid when that grid would be irrelevant to the user question
- if no active LLM profile exists, `/search ask` fails closed and tells you to configure `/llm`
- `/analyze` agent prompts are now more conservative: profile, code, and RAG agents explicitly avoid unsupported business claims, explain confidence using evidence classes, and the orchestrator merge prompt now uses source precedence instead of averaging conflicting descriptions

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
│   ├── application.py   # Headless AMXApplication facade for `import amx`
│   ├── ask_agent.py     # Tool-loop ask primitives and trace/result types
│   ├── inference.py     # Single-table metadata inference entrypoint used by approved search actions
│   ├── metadata.py      # Universal Metadata Interface canonical entities
│   ├── state.py         # Write-through config/session state manager
│   └── token_budget.py  # Prompt-size guard and deterministic context compaction
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
│   ├── code_agent.py   # Codebase analysis agent
│   └── tools/          # Reusable agent tools such as SchemaExplorer
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
- **Live wait visibility beyond `/run`**: Long-running sync, scope-discovery, DB inspection, docs/code scan, and batch-polling flows now keep a visible live activity/timer on screen instead of leaving the terminal blank while warehouse or filesystem work is in progress.
- **Table name normalization**: Codebase scanner now deduplicates fully-qualified (`schema.table`) and bare table names against the catalog.
- **Input validation**: Interactive table selection validates against the database with similarity hints for typos.

## License

MIT
