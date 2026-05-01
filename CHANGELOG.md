# Changelog

All notable changes to this project are documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). From the next release onward, version numbers and entries below `[Unreleased]` are derived from [Conventional Commits](https://www.conventionalcommits.org/) by [`python-semantic-release`](https://python-semantic-release.readthedocs.io/) — manual edits to released sections are no longer expected.

## [Unreleased]

### Fixed — Tests no longer pollute the developer's `~/.amx/`

User report: after merging the `fresh-install YAML is clean` fix and reopening AMX, `/db-profiles` STILL showed a `databricks-default` row pointing at the synthetic test placeholders (`adb-1234567890123456.7.azuredatabricks.net`, `catalog=my_catalog`). The earlier fix made `cfg.save()` write a clean YAML on truly-fresh installs, but on this developer's machine the file already contained synthetic data — written by **`pytest`**.

Root cause: many tests construct `AMXConfig()` with no path override, then trigger a code path that calls `cfg.save()` (e.g. `cmd_add_profile(cfg, ["databricks-default"])`). Without isolation, those saves resolve `~/.amx/config.yml` and overwrite the developer's real config with synthetic test fixtures. CI containers have an empty home so they never noticed; local devs got their config nuked silently every test run.

- **New autouse `_isolate_amx_home` conftest fixture** — patches `pathlib.Path.home()` to a per-test tempdir for every test (`unittest.TestCase` included). `AMXConfig.CONFIG_DIR` is computed from `Path.home() / ".amx"` at instance creation, so anything constructed inside a test now points at the tempdir. Pytest cleans the tempdir up after each test.
- **Regression test in `FirstRunConfigTests`** asserts that `Path.home()` resolves under a tempdir during the test and that `AMXConfig().CONFIG_DIR` lives under the same tempdir — locks the isolation in place so a future "helpful" refactor can't accidentally re-introduce the pollution path.

### Fixed — Fresh-install YAML is clean + actionable PostgreSQL no-DB error

User report: 1) the saved `~/.amx/config.yml` carried phantom `db:` and `llm:` blocks on a fresh install with no profiles, including hardcoded credential defaults (`user: amx`, `password: amx_pass`) that masqueraded as configured connection details. 2) When connecting to a PostgreSQL profile with no `database` pinned, the failure surfaced as the misleading "Referenced relation is missing or inaccessible in the current schema search path."

- **`DBConfig` credential defaults are now empty** (`user: ""`, `password: ""`). The pre-fix demo defaults were a 2018-era debugging convenience that survived into production. Existing user configs are unaffected — defaults only apply when constructing a fresh `DBConfig()`.
- **`AMXConfig.save()` skips top-level `db:` / `llm:` blocks when no profiles exist.** The mirror is for backwards-compat with pre-profile configs; on a fresh install with empty `db_profiles` and `llm_profiles`, the YAML now contains only `*_profiles: {}` and `active_*: ''`. No phantom rows in `/db-profiles`, no leaked credentials.
- **PostgreSQL adapter detects "database does not exist" specifically.** When libpq falls back to a database named after the user (because the profile has no `database` pinned), AMX now emits an actionable message: "PostgreSQL connection requires a database name. Open the profile with /edit and fill in the `database` field…" instead of the generic relation-missing wording. Note that PostgreSQL is a 2-level backend (database → schema → table), so unlike Databricks/BigQuery, the connection itself can't proceed without a target database — surfaced explicitly in the error.
- **3 new regression tests** covering the YAML-write side of fresh install, the empty credential defaults, and the new postgres error mapping. The previous `FirstRunConfigTests` only checked in-memory state; now the on-disk YAML shape is locked too.

### Fixed — `/doctor` now works from every namespace, not just `/search`

`/doctor` was registered with `cross_namespace=True` in the slash registry (so it appeared in autocomplete from every tab), but its dispatch was incomplete. Inside `/search` the verb fell through to `["search", "ask", "doctor"]` — sending the literal string "doctor" to the search agent as a question, which silently "looked like it worked." From any other namespace (`/db`, `/llm`, `/code`, …) the verb hit Click as `[namespace, "doctor"]`, an unknown subcommand.

- **Root cause**: `session.session_to_click_args` had no entry for `doctor` in its `shortcut_map`, so the function couldn't translate `/doctor` to the top-level Click subcommand. The slash registry's `cross_namespace=True` only controls help/autocomplete visibility — runtime dispatch is a separate code path.
- **Fix**: one-line addition (`"doctor": ["doctor"]`) to `shortcut_map`. Now `/doctor [--skip-network]` dispatches to the top-level Click subcommand from any namespace, exactly the same way `amx doctor` from a shell does.
- **4 new regression tests** (`tests/test_doctor_and_schema.py::DoctorCrossNamespaceDispatchTests`) cover the dispatch from root, `/search`, every other namespace, and the `--skip-network` flag passthrough. Each fails on `main` without the fix.

### Added — Public Python API contract (`docs/PUBLIC_API.md`)

The structural change with the longest tail of consequences: pinning what's stable before `1.0` ships. Every name documented in the new contract follows SemVer from 1.0 onward; everything else is internal and can move freely.

- **New `docs/PUBLIC_API.md`** — canonical reference for the stable surface (top-level `amx`, `amx.core`, the CLI command set, on-disk config + history schemas, `--json` export shape). Spells out what's NOT public (`amx.cli_support`, `amx.agents`, `amx.search`, `amx.config.AMXConfig` shape, internal helpers).
- **Explicit `__all__`** added to every module backing `amx.core`: `application.py`, `ask_agent.py`, `inference.py`, `metadata.py`, `state.py`. Each docstring now references the contract doc.
- **`amx.config` marked INTERNAL** in its module docstring — programmatic users go through `amx.init()` or `amx.core.AMXApplication.load()` instead. The two stable leaks (`CONFIG_SCHEMA_VERSION`, `ConfigSchemaTooNewError`) are called out explicitly.
- **README's "Programmatic API" example** rewritten to use `amx.init()` instead of importing `AMXConfig` directly, plus a link to `docs/PUBLIC_API.md`.
- **`tests/test_public_api_contract.py`** locks the contract: 10 cases that fail loudly if a public name is removed, renamed, or its `__all__` declaration silently drifts during a future `ruff format` import sort.

### Documentation — README hero, 5-minute quickstart, and screenshot slots

The README's first 130 lines were a feature list; new readers had to scroll past architecture diagrams and prerequisite tables to see what AMX actually produces. Restructured so the value proposition + a concrete before/after example + the 30-second quickstart all live above the fold.

- **Hero block** with a one-line value prop and the supported backends / LLM providers.
- **"What it produces"** mini-example: cryptic `T0001.AUDAT NUMBER(8)` in, reviewed description with confidence + logprob + sources out. Synthetic identifier (no real connection details), so safe to reproduce in talks / posts.
- **5-minute quickstart** with the four commands that get a new user from `pip install amx` to a reviewed run, plus `amx doctor` for when something looks wrong.
- The original "Quick Start" section is preserved as **"Detailed setup"** for users who want full control over each step.
- HTML-comment placeholders (`<!-- TODO: screenshot — ... -->`) at four natural drop-in spots: setup wizard, run review, db-profiles table, /compare diff view. Future PR will fill them in.

### Added — `/compare --json` for thesis / notebook workflows

Direct continuation of the `/compare --csv` and `--md` flags from the previous PR. The JSON document is shaped specifically for pandas / Jupyter consumption — long-format `per_column` and `aggregate_metrics` arrays so notebooks can `pd.DataFrame(payload["per_column"]).pivot(...)` without reshaping.

- **`/compare --json FILE`** writes the comparison as `{schema_version, generated_at, amx_version, run_count, run_summary, per_column, aggregate_metrics}`. Pairs cleanly with `--csv` / `--md` — pass any combination and AMX writes all of them.
- `tests/eval/README.md` now documents how to feed the JSON into a notebook for thesis-style "average logprob by LLM profile" / "tokens-vs-confidence" charts. Two new tests in `tests/test_compare.py` lock the JSON shape so it's safe to depend on.

### Added — `amx doctor` + config schema versioning

Two changes that together kill the **version-skew bug class** that hit on 2026-05-01: two `amx` binaries on `PATH` writing to the same `~/.amx/config.yml` made profiles silently disappear when the older binary stripped keys it didn't recognise.

- **`amx doctor`** — new top-level command (also available as `/doctor` inside the session). Detects: multiple `amx` binaries on `PATH` (the hero check; lists every binary it finds), Python runtime, config directory permissions, config file readability + schema version, optional backend deps (BigQuery, Snowflake, Databricks, keyring), active DB profile connectivity, active LLM profile reachability. `--skip-network` drops the last two for an offline quick run. Returns non-zero on any failure so CI / scripts can chain on it. Bypasses the "interactive only" guard so it works from a broken state — that's its whole point.
- **Config schema versioning** — every saved `~/.amx/config.yml` now carries a `schema_version: 1` stamp. On load, AMX refuses configs whose version is **higher** than the running binary understands (with `ConfigSchemaTooNewError` and a CLI-level actionable message: "Upgrade AMX, or pin an older AMX and re-run"). Configs without the key (pre-versioning, including the existing user base) load happily as legacy. **Net effect**: when an older binary tries to read a config a newer binary wrote, it refuses cleanly instead of silently mangling the file.

### Fixed — CI is green again

Lint + tests had been merging red on `main` since before the public-launch roadmap kicked off. This pass cleans up the rollup so PRs land green and the eventual PyPI badge is honest.

- **Lint** — applied `ruff check --fix` (178 safe autofixes) and `ruff check --fix --unsafe-fixes` (70 more), then fixed the residual 9 by hand. Highlights: orphaned `Union` import in `_db_profile_clause.py` (now `str | Sequence[str]`), an `E701` pair in `orchestrator.py`, a missing `from None` on a `ClickException` re-raise, and a missing `Any` import in `tests/test_search_catalog.py`. Relaxed three intentionally-codebase-pattern rules in `pyproject.toml`: `SIM105` (defensive `try/except/pass` around best-effort I/O), `SIM117` (nested `with` when the inner depends on outer setup), `SIM102` (per-level conditions kept distinct).
- **Real bug found while linting** — `amx/search/agent_tools.py` had `"date"` and `"timestamp"` keys defined twice in the `_DTYPE_FAMILIES` dict; the later (narrower) definitions silently overrode the earlier comprehensive ones, so `/ask` queries asking for "date columns" or "timestamp columns" missed everything but exact-match dtypes. Removed the duplicate keys; `temporal` and `datetime` keys keep the broad lists.
- **Tests** — added `addopts = "-m 'not integration and not live'"` to `pyproject.toml` so headless CI skips tests that need real Postgres / live LLM endpoints / coordinated mock-LLM setups by default. Marked the 18 such tests with `@pytest.mark.integration` (or `@pytest.mark.live` for the one real-OpenAI call). They still run locally with `pytest -m "integration or live"`. Result: `pytest` green at 366 passed / 18 deselected.

### Added — `/compare` slash command

New search-namespace command that pivots run history side-by-side, so users running the same scope under different LLM / doc / code profiles can finally see which configuration produced the best descriptions.

- `/compare [RUN_IDS…] [--schema] [--table] [--column] [--last N] [--command analyze.run|search.ask|all] [--by auto|llm_profile|doc_profile|code_profile|llm_model|db_profile|run]` renders three Rich tables:
  - **Run summary** — one row per run (started, status, command, profiles, model, duration, approval rate). The dimension that varies is auto-detected and highlighted in `bold green`.
  - **Per-column results** — pivots `run_results` so each schema/table/column row shows the top description, confidence band, `logprob_score`, and token count for each run; the highest logprob per row wins in green.
  - **Aggregate metrics** — wall duration, model processing time, prompt/completion/total tokens, average logprob, confidence distribution, approval rate, saved-result count.
- Resolution order: explicit run IDs → scope filter (`--schema` / `--table` / current schema) with `--last N` → error if neither.
- Schema additive migration: `analysis_runs` gains `llm_profile`, `doc_profile`, `code_profile` columns (idempotent ALTER, NULL for legacy rows). `/run` and `/ask` now persist the active profile names so post-hoc comparisons across doc/code/llm-profile differences are meaningful.
- Discovery hint: after a successful `/ask`, when ≥2 prior `search.ask` runs already touched the same schema, a single dim line suggests `/compare --last 3 --schema <X>`. Quiet, never on the first or second ask.
- `--diff` flag enables word-level highlighting in the per-column pivot. The leftmost run is the baseline; insertions render bold green, deletions render strike-red. Uses stdlib `difflib`, no new deps. Off by default so non-diff users still see clean text.
- `--csv FILE` exports all three tables to a single CSV with `# section: <name>` markers (run_summary / per_column / aggregate_metrics), long-format so pandas/Excel users can pivot however they like.
- `--md FILE` exports GitHub-flavoured Markdown with the per-column block in wide format — ready to paste into Notion, GitHub PR descriptions, or thesis appendices without reformatting.

## [0.11.0] - 2026-05-01
### Added — Multi-DB execution + optional `database` per connector

This release introduces two related changes that together unlock cross-database `/ask`:

1. **`database` field is now OPTIONAL on every DB connector profile.** Previously every profile baked in a single database (PostgreSQL/Snowflake) or catalog/dataset (Databricks/BigQuery), and the legacy demo default `database: "SAP"` surfaced as a phantom "SAP @ localhost:5432" row for users who never finished setup. The default is now empty; profiles with no DB pinned will prompt the user to pick at command time (3-level backends) or warn that listings may be empty (2-level backends).

2. **`/ask`, `/run`, and `/sync` accept multiple DB profiles per call.** A new persisted multi-pick scope (`/use-db prod_pg analytics_bq`) lets the user define a default cross-DB workflow; per-call `--db-profile NAME` (multi) overrides it. `/ask` retrieval unions catalog rows across profiles in a single fused query — the killer feature for cross-DB join discovery and "where does customer_id live across all my DBs" type questions. `/run` and `/sync` iterate per profile (with shared LLM/token tracker) so write-back semantics stay correct.

#### Profile / config layer

- `DBConfig.database = ""` (was `"SAP"`). Backend-aware split: `is_connection_configured()` is the new "can we open an engine" predicate (no DB required); `is_database_pinned()` reports whether the user committed to a specific database/catalog/dataset; `is_configured()` is preserved as a back-compat alias to `is_connection_configured()` so all 99 existing call sites stay valid.
- `DBConfig.url` now builds a server-only URL when the database is unpinned (PostgreSQL/Snowflake) instead of trailing `/`. `display_summary` shows `(no DB pinned)` per backend.
- `/add-db-profile` no longer requires the database field for PG/SF (makes it optional, in line with Databricks/BigQuery which were already optional via catalog/dataset).
- `/db-profiles` surfaces unpinned profiles with a `?` next to the backend column and emits a one-time hint when any profile still carries the legacy demo default `database='SAP'` — suggest, don't mutate.
- `has_legacy_database_default()` helper for detection, never auto-edits YAML.

#### Persisted multi-pick scope

- `AMXConfig.active_db_profiles: list[str]` is the new source of truth for the multi-DB execution scope. The legacy `active_db_profile: str` is kept as the first list entry so all 99 existing call sites that read it directly stay working unchanged.
- New `cfg.set_active_db_profiles(names)` / `cfg.effective_db_profiles()` APIs. `cfg.set_active_db_profile(name)` collapses the scope to a single profile (symmetric).
- Save/load round-trip both keys: a 0.10.x reader keeps working from the scalar; 0.11+ readers prefer the list. `remove_db_profile` evicts from the scope.

#### `ProfileScope` service object

- New `amx/services/profile_scope.py`. Frozen, ordered, deduped tuple of profile names + a `default` pointer for write-back operations. `ProfileScope.from_config(cfg)` reads the persisted scope; `from_names([...])` wraps an explicit CLI-derived list. `connectors(cfg)` lazy-yields one connector at a time and disposes each before yielding the next, so the FD budget mirrors the existing single-profile path.

#### `/use-db` is multi-pick

- `/use-db prod_pg` — single (legacy behaviour).
- `/use-db prod_pg analytics_bq` — persisted multi-profile scope used by `/ask`, `/run`, `/sync`.
- Interactive form asks single-vs-multi when ≥2 profiles exist, then routes to `ask_choice` or `ask_multi_choice`.
- Startup banner prints the full active scope when multi-profile so the user always knows which DBs are in play.

#### Catalog read methods accept `Sequence[str]`

- New helper `amx/search/_catalog/_db_profile_clause.py` builds parameterised `WHERE db_profile IN (?, ?, …)` filters.
- `name_search_columns`, `find_table_candidates`, `find_tables_by_exact_name`, `find_columns_by_exact_name`, `_exact_candidates`, `search_columns`, `search_tables`, and `_attach_column_counts` all accept `str | Sequence[str]`. Single-string callers emit identical SQL to before.
- `SearchIndex.query()` accepts `str | Sequence[str]` — multi-profile vector queries hit each Chroma collection separately and union results sorted by distance ascending. Per-profile collection partitioning is unchanged (no schema migration).
- `search_tables` per-row table-lookup queries scope the parent-table lookup to the row's OWN `db_profile` (not the caller's full scope) so column→table linkage stays correct in multi-profile retrieval.

#### `SearchAgent` multi-DB scope

- `SearchAgent.__init__` accepts `db_profiles=[...]` override (falls back to `cfg.effective_db_profiles()`). New `is_multi_profile` and `db_profile_filter` properties; the legacy `self.db_profile` scalar still points at the first profile for write-back / settings anchoring.
- All retrieval/resolution call sites that hit catalog read methods now pass `db_profile_filter`.
- Planner prompt now includes `active_db_profiles` so the LLM can mention all configured profiles in cross-DB answers.
- Join methods (`join_candidates`, `joinable_tables`, `semantic_join_candidates`, `semantic_joinable_tables`) stay single-profile this release — anchor profile only. Cross-DB join inference will surface via `name_search_columns(scope)` results in a follow-up.
- `session_memory` + `sync_status` keep single-profile semantics (sessions / job rows are per-anchor by design).

#### CLI commands

- `/ask` (and `/search ask`) gains `--db-profile NAME` (multi). Unknown profiles emit a clear error with the available list. `_render_search_rows` auto-shows a `Profile` column when results span multiple profiles.
- `/run` and `/run-apply` gain `--db-profile NAME` (multi). Outer per-profile loop with shared LLM/token tracker; `analysis_runs`/`run_results` rows are persisted per profile.
- `/sync` gains `--db-profile NAME` (multi). Per-profile sync loop with shared progress UI; failed profiles are skipped (multi-profile mode) instead of aborting the whole batch.
- New `warn_when_database_unpinned()` helper in `catalog_picker.py` flags 2-level backends without a pinned database so users know listings may be empty before they hit a confusing error.

#### Tests

- `tests/test_db_profile_optional_database.py` — 16 cases for the new `is_database_pinned` / `is_connection_configured` / `display_summary` / `url` / `has_legacy_database_default` contracts.
- `tests/test_profile_scope.py` — 18 cases for the persisted scope semantics + the `ProfileScope` helper.
- `tests/test_search_catalog_multi_profile.py` — 6 unit tests for `_db_profile_clause` + 4 E2E tests against an isolated `SQLiteHistoryStore` confirming multi-profile filtering returns the union.

#### Docs

- `docs/design/multi-db-plan.md` captures the architecture survey, locked-in decisions (multi-pick `/use-db`, `/ask` defaults to active scope, suggest-don't-mutate for legacy SAP), risks, test surface, and the 9-phase implementation roadmap.

### Breaking — None

The release is fully backward-compatible. All 99 existing `cfg.active_db_profile` call sites and 27+ `DatabaseConnector(cfg.db)` call sites work unchanged. Single-profile retrieval emits identical SQL. Old YAML configs with `database: "SAP"` continue loading; users get a one-time hint to clear the value.

### Why this matters

The user reported that defining a single database per profile blocks cross-DB analysis (`/ask` answers across multiple connected systems are the highest-value use case). 0.11.0 makes the database field truly optional and lets `/ask`, `/run`, `/sync` operate on any subset of configured profiles in one call.

### Followups

- Cross-DB join inference: pair same-name columns across profiles via `name_search_columns(scope)` results and surface a join-candidate row that explicitly spans `db_profile_left`/`db_profile_right`. Out of 0.11.0 scope; queued for 0.11.1.
- Live database picker for 2-level backends (PG/SF): currently `warn_when_database_unpinned` only emits a hint. Implementing `list_databases()` on PG (`\l`) and Snowflake (`SHOW DATABASES`) would let the picker prompt at command time the same way the catalog picker does for Databricks/BigQuery.
- `/run-apply` write-back collision detection in multi-profile mode: per-profile loop sidesteps the issue today (each comment is written to its own DB), but a future refinement could pre-flight check for asset-name collisions and warn the user.

## [0.10.15] - 2026-05-01
### Added
- **Corporate-network friendly SSL configuration** (`amx/llm/provider.py:_configure_ssl_environment`). Two new env vars unblock users behind TLS-inspecting proxies (Zscaler, Netskope, internal CA, ZIA, etc.) where every LLM call previously died with `[SSL: CERTIFICATE_VERIFY_FAILED] self-signed certificate in certificate chain`:
  - `AMX_CA_BUNDLE=/path/to/corp_root.pem` — preferred. Sets `REQUESTS_CA_BUNDLE` / `SSL_CERT_FILE` / `CURL_CA_BUNDLE` so requests / httpx / urllib3 / curl all trust the corporate CA. The bundle path must exist; an invalid path is silently ignored so misconfiguration doesn't break clean networks.
  - `AMX_INSECURE_SSL=1` — diagnostics only. Sets `litellm.ssl_verify = False` AND `PYTHONHTTPSVERIFY=0`. Logs a `WARNING` at startup so it's obvious it's on. Use to confirm the failure IS a CA-trust issue; switch to `AMX_CA_BUNDLE` for daily use.

  Both vars are read on first `_litellm()` call (lazy import) so they take effect before httpx clients are constructed. No effect on machines that don't set them, so personal laptops are unchanged.

### Fixed
- **SSL cert errors no longer retry 3× with identical failure noise.** `_classify_fatal_llm_error` now recognises `certificate verify failed` / `self-signed certificate` / `ssl: certificate` substrings and raises `FatalLLMError` immediately with the exact fix-it instruction:
  > SSL certificate verification failed — your network is using a TLS-inspecting proxy whose root CA Python doesn't trust. Fix: set AMX_CA_BUNDLE=/path/to/corp_root.pem (preferred), or AMX_INSECURE_SSL=1 for diagnostics only.

  Previously the user saw 3 stacked `LLM call failed: ... CERTIFICATE_VERIFY_FAILED` ERROR lines (one per retry × per agent — search agent was logging it twice for tool-agent + legacy-router fallback paths) before AMX gave up; now they see one fatal message and an actionable fix.

### Why this matters
The user reported AMX working on a personal laptop but failing on a second computer (corporate network). Same OpenRouter key, same model, same config. Root cause: the second machine routes outbound HTTPS through a corporate inspection proxy that re-signs every connection with an internal root CA — and Python's bundled `certifi` truststore doesn't include it. This is the single most common "AMX works at home but not at work" failure mode and now has a documented two-line fix.

### Followups
- Surface `AMX_CA_BUNDLE` / `AMX_INSECURE_SSL` in `/llm` wizard so users can configure them without editing shell rc files.
- Consider falling back to system trust store automatically (via `pip-system-certs` or `truststore` package) on macOS / Windows where the OS keychain already has the corporate CA installed.
- Apply the same env-var pattern to outbound DB connections — Databricks / Snowflake clients can hit the same proxy and need the same CA bundle.

## [0.10.14] - 2026-05-01
### Changed
- **Catalog picker hoisted into a shared helper used by all flows** (`amx/cli_support/catalog_picker.py`). The v0.10.11 picker only fired in `/edit`; user pointed out it should fire wherever AMX needs to list schemas / tables before knowing the catalog. New helper:
  ```python
  ensure_catalog_selected(db, *, silent_when_set=False) -> str
  ```
  Wired into:
  - `/connect` (`amx/cli_support/root_commands.py:db_connect`) — Databricks-specific path. After a successful connection test the user picks a catalog; the rest of the session inherits it.
  - `/run` and `/run-apply` (`amx/cli_support/commands/analyze_flow.py:execute_analyze_run`) — fires after the LLM-test stage and BEFORE the scope picker so `finalize_scope` walks the right schemas.
  - `/search sync` (`amx/cli_support/commands/search.py:search_sync`) — fires inside the `command_display` block before `_interactive_sync_scope` runs.
  - `/edit` (`amx/cli_support/commands/manual.py:_select_catalog_for_wizard`) — now a 3-line shim around the shared helper.

The picker is still a no-op for backends that return `False` from `supports_catalogs()` (PostgreSQL, Snowflake, BigQuery without project switching), so existing flows on those backends are unchanged. The `silent_when_set=True` flag is available for non-interactive callers that want to skip the prompt when `cfg.catalog` is already populated; the four wired call sites all default to `False` so the picker behaves consistently across flows.

### Why this matters
Without this hoist, `/run` and `/search sync` on Databricks would still fall through to the SQLAlchemy inspector with `catalog=None` and fail with `SHOW TABLES FROM None.<schema>`, just like `/edit` did pre-v0.10.12. Same root cause, same fix, applied uniformly.

### Followups
- A future refinement: make `ensure_catalog_selected` recognise BigQuery projects (`SELECT DISTINCT catalog_name FROM \`region-us\`.INFORMATION_SCHEMA.SCHEMATA`) so cross-project edits / runs work the same way.
- The `_db_for_pick = DatabaseConnector(cfg.db)` instances created in `/connect` and `/search sync` could be threaded through to subsequent steps to avoid an extra connection round-trip — currently the cfg.catalog mutation propagates through cfg only.

## [0.10.13] - 2026-05-01
### Fixed
- **Catalog picker auto-skipped after the first pick** — once the user picked a catalog in `/edit`, `cfg.catalog` was set and subsequent wizard runs treated that as authoritative ("existing catalog → return immediately"). User couldn't switch catalogs without restarting AMX or editing the profile manually.

### Changed
- **`_select_catalog_for_wizard` always shows the picker** when the backend supports catalogs (`amx/cli_support/commands/manual.py`). The picker now defaults to the current `cfg.catalog` if set so a happy user just presses Enter, and a user who wants to switch catalogs picks a different option from the list. Mirrors the schema / table picker UX downstream — "current as default, Enter keeps, different value switches".
- When `SHOW CATALOGS` fails AND `cfg.catalog` is already set, the existing value is honoured silently (no false-positive catalog list).

### Why this matters
The previous early-return path treated catalog as a one-time pinning decision; in reality users want to bounce between catalogs (dev / prod, main / sandbox, staging / final) within the same session. With the picker always shown:
1. First wizard run: no `cfg.catalog` → user picks → cfg gets set.
2. Subsequent run: picker shows current as default → Enter keeps it → schema picker fires.
3. User wants different catalog: picker shows current as default → user picks a different one → cfg.catalog updates.

### Followups
- Same UX should apply to BigQuery project switching once that adapter override lands. The helper is generic now; only the source list (`db.list_catalogs()`) changes per backend.

## [0.10.12] - 2026-05-01
### Fixed
- **`SHOW TABLES FROM None.dev`** still failed after v0.10.11. The catalog picker correctly populated `cfg.catalog` and `list_schemas` switched to `SHOW SCHEMAS IN <catalog>` — but `list_tables` and `list_views` were still using SQLAlchemy's `inspect().get_table_names(schema=schema)`, which on Databricks issues `SHOW TABLES FROM <schema>` (no catalog context) and resolves catalog as whatever the connection's default is — `None` when the user hadn't run `USE CATALOG`.

### Changed
- **Adapter-level overrides for `list_tables` and `list_views`** (`amx/db/adapters/base.py`). Default returns `None` (fall back to SQLAlchemy inspector); Databricks override runs `SHOW TABLES IN \`<catalog>\`.\`<schema>\`` and `SHOW VIEWS IN \`<catalog>\`.\`<schema>\`` so the catalog explicitly travels with each query. Same contract / fallback pattern as the v0.10.11 `list_schemas` override.
- **`DatabaseConnector.list_tables` / `list_views`** consult the adapter override before falling back to the inspector. Reads `cfg.catalog` so the v0.10.11 wizard pick propagates without needing a `USE CATALOG` round-trip on the engine.

### Why this matters
v0.10.11 fixed half the path (schemas) but stopped before the table layer. The user's reproducer ("aynı hata, catalog seçtirdi ama None.dev'e SHOW TABLES FROM atıyo") was exactly that gap. With both layers now catalog-aware, the `/edit` wizard on Databricks Unity Catalog should walk catalog → schema → table without falling through to the inspector.

### Followups
- The same override should extend to `list_materialized_views` and to `list_column_profiles` / `get_column_comments` — the SQLAlchemy `inspect().get_columns(table, schema=schema)` path may still issue catalog-less SQL when the user opens `/edit`'s column picker. Tracked but not yet hit because the user got blocked at the table layer first.
- A USE CATALOG hook on the engine connection would let SQLAlchemy's inspector inherit the catalog implicitly — that's the long-term cleaner path. Until then, per-method overrides are the surgical fix.

## [0.10.11] - 2026-05-01
### Fixed
- **`/edit` wizard on Databricks ran `SHOW SCHEMAS` without USE CATALOG** when the user hadn't pinned a catalog. Reproducer: connect to Databricks Unity Catalog, run `/edit` to update a comment — the schema picker either showed the wrong namespace or returned nothing, and downstream `SHOW TABLES None.dev` failed. Root cause: SQLAlchemy's `inspect(engine).get_schema_names()` is not catalog-aware on Databricks; the connector used it as the only path.

### Added
- **`AdapterBase.supports_catalogs()` + `AdapterBase.list_catalogs(engine)`** (`amx/db/adapters/base.py`) — generic interface for backends with a 3-level catalog → schema → table hierarchy. Default returns `False` / `[]` so existing adapters stay unchanged.
- **`AdapterBase.list_schemas(engine, catalog="")`** with a default `None` return — the connector falls back to the SQLAlchemy inspector when the adapter returns `None`. Lets adapters override schema listing for catalog-scoped backends without forcing every adapter to implement it.
- **`DatabaseConnector.list_catalogs()` + `DatabaseConnector.supports_catalogs()`** convenience wrappers.

### Changed
- **Databricks adapter overrides `list_catalogs` and `list_schemas`** (`amx/db/adapters/databricks.py`):
  - `list_catalogs` runs `SHOW CATALOGS` and returns the catalog names.
  - `list_schemas(engine, catalog)` runs `SHOW SCHEMAS IN \`<catalog>\`` when a catalog is set, filtering system schemas. Returns `None` (fallback to inspector) when no catalog is supplied.
- **Manual-edit wizard catalog picker** (`amx/cli_support/commands/manual.py`):
  - New `_select_catalog_for_wizard(db)` helper. Fires at the start of `_select_schema_for_wizard` whenever `db.supports_catalogs()` is True AND `cfg.catalog` is empty.
  - Lists catalogs via `db.list_catalogs()`, asks the user to pick, persists the pick on the in-memory `cfg.catalog` so subsequent `list_schemas`/`list_assets` calls scope correctly. The pick is per-wizard-session (not written to disk) so users can edit across catalogs without permanent profile changes.

### Why this matters
Without catalog awareness AMX wasn't usable for `/edit` on Unity Catalog Databricks at all — the user got an empty schema list, then watched logs show `SHOW TABLES None.dev` fail. With v0.10.11 the wizard:
1. Detects the 3-level hierarchy (`supports_catalogs` returns True).
2. Lists catalogs via `SHOW CATALOGS`.
3. Prompts the user to pick.
4. Scopes every subsequent `SHOW SCHEMAS IN <catalog>` and `SHOW TABLES IN <catalog>.<schema>` correctly.

The same hooks are available for any other 3-level backend; adding BigQuery project switching is now an adapter-level change rather than touching the wizard.

### Followups
- BigQuery adapter override: implement `list_catalogs` (= `INFORMATION_SCHEMA.SCHEMATA` distinct projects, or accept comma-separated project list from cfg) so cross-project edits also work.
- `/db` profile creation: when adding a Databricks profile, prompt for the default catalog upfront so this wizard prompt only fires when the user explicitly wants to switch.
- Other AMX flows (`/run`, `/ask`, `/search sync`) currently skip the catalog picker — they assume `cfg.catalog` is set. The same `_select_catalog_for_wizard` helper should be hoisted to a shared spot so those flows also catch the empty-catalog case before issuing catalog-less queries.

## [0.10.10] - 2026-05-01
### Fixed
- **"how is X uploaded / loaded / populated / refreshed?" was being routed to `detect_scd_pattern`** (which answers "how is HISTORY kept", a different concern). Reproducer: user asked "how is vbak uploaded?", agent ran the SCD detector, returned the canned "no SCD signals; provide a business_key" recovery message — completely off-topic.

### Changed
- **System prompt new routing rule** (`amx/search/tool_agent.py`). When the user asks the LOAD-MECHANISM question (English: "how is X uploaded / loaded / populated / ingested / refreshed", Turkish: "nasıl yükleniyor / besleniyor / ETL süreci nasıl / data nasıl geliyor"), the agent should:
  1. Call `describe_table` and read `analytics.last_modified` — the most recent write timestamp.
  2. Call `inspect_data_quality` on the main temporal column (`created_at` / `erdat` / `load_date` / `ingestion_ts`) — `min_value` is when data first appeared, `max_value` is the latest record, the gap + row_count is a rough load-cadence hint.
  3. If columns shaped like CDC are present (`created_at` + `updated_at`, `deleted_at` flag), call them out as an in-band CDC signal.
  4. ALWAYS state what AMX CANNOT see: "Direct visibility into the orchestrator (Airflow / Dagster / dbt Cloud / Snowflake Snowpipe / BigQuery Data Transfer / Databricks DLT) is a v0.11 planned feature — AMX currently infers from the data, not from the load job."

The rule explicitly bans `detect_scd_pattern` for this question class because that tool answers a different concern (history retention) and gives an unhelpful recovery message when the user's actual question is unrelated.

### Why this matters
Same pattern as the v0.10.3 "duplication" / "update soon" routing fixes: AMX has the right data already (`last_modified` from v0.10.0 analytics, `inspect_data_quality` from v0.10.2) — it just needs the routing rule. Without the rule the agent picks whatever tool's keyword overlaps loosely ("uploaded" sounds vaguely temporal → SCD); with it, the agent goes straight to the load-pattern signals + the explicit ETL-tap limit.

### Followups
- The actual answer ("scheduled by `ELT_orders_hourly` Airflow DAG, last run 2026-05-01 14:00 UTC") needs the v0.11 query-history / orchestrator tap. Until then, the data-side inference is the best AMX can do.

## [0.10.9] - 2026-05-01
### Fixed
- **`[WARNING] amx.db.connector — Exact row count failed for X.Y: ...`** was bleeding through the live-display panel during `/ask` answers. Same UX noise pattern as the v0.10.1 `tool_calls` warning: the message is purely informational (the code already falls back to the estimated row count and continues), but it surfaced as a WARNING level alarm in the panel mid-stream. Demoted to DEBUG in `amx/db/connector.py:profile_table`. Operators who want to investigate slow / blocked counts can still get the line via `AMX_LOG_LEVEL=debug`.

### Why this matters
Continuing the v0.10.1 cleanup pass: any log line that fires during a clean recovery path doesn't belong at WARNING. The user sees a `[WARNING]` and assumes something is broken — but in this case the answer they got was correct (the duplication probe worked, returned proper numbers). Reserving WARNING for actual problems keeps that signal trustworthy.

### Followups
- Audit the rest of `amx.db.*` for similar fall-back-but-warn patterns. The next likely candidates are connection-retry messages and adapter-specific permission softfails.

## [0.10.8] - 2026-05-01
### Fixed
- **`detect_dimensional_role` returned "unknown"** for FK-free schemas with opaque table names. Reproducer: SAP `vbrk` (billing-document header, archetypal fact table) had no `fact_*` naming, no declared FK constraints, no partitioning, and `erdat` is stored as `varchar(8)` rather than the native `date` type — so neither the naming nor the structural FK signals fired. Pre-v0.10.8 the agent surfaced "unknown / low confidence" with the truthful but unhelpful note "no signals fired".

### Changed
- **New column-shape signal** (`amx/search/agent_tools.py`). The classifier now also counts:
  - **Measure-like columns** — numeric dtype + name matches money/quantity patterns (`_amt`, `_amount`, `_value`, `_qty`, `_quantity`, `_total`, `_sum`, `_price`, `_cost`, `_fee`, `_rate`, `_tax`, `_brutto`, `_netto`, `_revenue`, `_profit`, `_margin`, `_balance`) PLUS SAP-specific currency / quantity columns (`netwr`, `brtwr`, `mwsbp`, `mwsbk`, `kbetr`, `kwert`, `fkimg`, `fklmg`, `kpein`, `kzwi`, `wavwr`).
  - **ID / key columns** — name matches key patterns (`_id`, `_key`, `_no`, `_num`, `_code`) PLUS SAP-specific keys (`mandt`, `vbeln`, `vgbel`, `kunag`, `kunrg`, `kunwe`, `lifnr`, `vkorg`, `vtweg`, `spart`, `matnr`, `werks`, `lgort`, `bukrs`, `gjahr`, `belnr`, `buzei`, `fkart`, `auart`).
  - **Descriptive columns** — string dtype + name matches description patterns (`_name`, `_desc`, `_label`, `_text`, `_title`, `_remark`, `_addr`) PLUS SAP-specific descriptive columns (`name1`, `name2`, `ktokd`).
- **Two new structural-fact rules** based on column shape:
  - `≥3 measures + ≥4 ids + (has_temporal OR row_count ≥ 10K)` → fact (medium confidence). This is the SAP/legacy escape hatch: when naming is opaque and FKs aren't declared, the column mix itself screams "transactional with measures and application-level foreign keys".
  - `≥5 descriptives + 0 measures + row_count ≤ 100K` → dimension (medium confidence).

For `vbrk` specifically: ≥5 SAP currency columns + ≥6 SAP key columns + 89K rows now classify as `fact` with medium confidence and an evidence line stating exactly which signals fired.

### Why this matters
Same `evidence + confidence + indicators` design pattern from earlier interpretive-answering releases, applied to dimensional-role detection. The previous pure-naming + pure-FK approach worked for clean dbt / Kimball schemas but failed on SAP, Oracle eBusiness, peoplesoft, and many enterprise schemas where FK constraints aren't declared in the database. Column shape is a third independent signal that's available even when the first two are silent.

### Followups
- The SAP-name pattern lists are still narrow (covers vbrk / vbap / kna1 territory). A `column_role_inference_packs` namespace where users can add their own org-specific name patterns would let in-house data-warehouse teams steer the classifier without forking AMX.
- Numeric measure detection currently relies on name patterns; a richer signal would also probe value distributions (continuous distribution → likely measure; bounded category set → likely flag/code) but that requires a sample query per column. Marked as a v0.11 followup since it's only worth the cost when the cheaper signals are inconclusive.

## [0.10.7] - 2026-05-01
### Added — `detect_dimensional_role` tool

User: "AMX should answer dimensional-modeling questions — what's the main/fact table?, which tables are dimensions?, is this a star schema or snowflake?". One tool covers all three modes. Per-table mode classifies a single table; schema-wide mode ranks every table and infers the overall pattern.

**Roles detected:**
- `fact` — large + many outgoing FKs / partitioned / temporal column / `fact_*` / `_facts` / `_evt` / `transactions` / `_orders` naming
- `dimension` — high incoming FK fan-in + low fan-out / `dim_*` / `_dim` / `dimension_*` / `lookup_*` naming
- `bridge` — roughly equal in/out FKs (both ≥ 2, |out − in| ≤ 1) / `bridge_*` / `xref_*` / `link_*` naming
- `lookup` — small (≤ 1000 rows, ≤ 12 cols) + at least one incoming FK
- `staging` — `stg_*` / `staging_*` / `raw_*` / `_landing` / `src_*` naming
- `audit` — `_history` / `_audit` / `_log` / `_archive` naming
- `transactional` — has temporal column but no partitioning / no FK fan-out
- `unknown` — no strong signal in any direction

**Schema-level pattern detection:**
- `star_schema` — facts + dimensions present, no dimension-to-dimension FKs
- `snowflake_schema` — at least one dimension references another dimension via FK (the result lists examples like `sap_dim.product_dim → sap_dim.product_category_dim`)
- `flat` — no fact-shaped tables, only dimensions / reference data
- `fact_only` — facts present but no dimension-shaped tables (suggests OBT / one-big-table layout)
- `unknown` — too few tables to classify

**Result shape (per-table):**
```json
{
  "schema": "sales", "table": "fact_orders",
  "role_hypothesis": "fact",
  "confidence": "high",
  "evidence": [
    "Naming pattern matches `fact` role.",
    "Row count 12,400,000 is >5× the schema median (340,000) — likely fact / transactional.",
    "5 outgoing FK(s) — likely fact (joins out to many dimensions).",
    "[implicit] Partitioned by order_date."
  ],
  "indicators": {
    "row_count": 12_400_000, "fk_outgoing": 5, "fk_incoming": 0,
    "column_count": 23, "is_partitioned": true, "has_clustering": false,
    "has_temporal_column": true, "naming_signal": "fact",
    "row_count_percentile": 0.95, "peer_row_count_median": 340_000
  }
}
```

**Result shape (schema-level):**
```json
{
  "schema": "sales",
  "table_count": 18,
  "pattern_hypothesis": "star_schema",
  "pattern_evidence": [
    "1 fact table(s) and 6 dimension table(s); no dimension-to-dimension FKs (star layout)."
  ],
  "fact_tables": ["sales.fact_orders"],
  "dimension_tables": ["sales.dim_customer", "sales.dim_product", "sales.dim_date", ...],
  "bridge_tables": [],
  "lookup_tables": ["sales.lookup_country", "sales.lookup_currency"],
  "staging_tables": ["sales.stg_orders_raw"],
  "classifications": [<full per-table breakdown>]
}
```

### Why this matters
Same single-tool / multi-signal / always-quote-evidence pattern as `detect_scd_pattern` (v0.10.6), `find_joinable_tables` (v0.9.7 inference_source), `find_columns_by_dtype` (v0.10.4 kind tagging). Together these tools make AMX able to talk about **how a schema is organised** — not just what's in it. Real-world data analyst questions land directly on a tool now: "this schema'ın ana tablosu nedir?", "fact ve dim hangileri?", "this is a star or snowflake?", "bu lookup mu transaction mı?".

### Followups
- **Cross-schema linkage** — when a fact table references a dimension that lives in a SHARED dimensional schema (Kimball "conformed dimensions"), surface that link in the schema-level pattern. Currently per-schema only.
- **Galaxy / fact constellation** — multi-fact schemas where facts share dimensions; the current code labels these as `star_schema` per-fact but doesn't call out the cross-fact dimension sharing. Planned for v0.10.8.
- **OBT / wide-table detection refinement** — currently `fact_only` flags potential one-big-table layouts but doesn't quantify width-vs-depth. A `column_count` percentile would help.

## [0.10.6] - 2026-05-01
### Added — `detect_scd_pattern` tool

User: "AMX should answer SCD-type questions ('how does this table hold history?', 'is this Type 2?', 'değişiklik aynı satırda mı yeni satır mı?', 'eski değerler ayrı kolonda mı?') WITHOUT relying on comments. Lots of variations could come."

Right — this is a single-tool design problem, not whack-a-mole. New `detect_scd_pattern(schema, table, business_key?)` infers the pattern from data signals only:

**Type 2 (history-as-rows) signals:**
- Temporal validity pair: `valid_from` + `valid_to`, `effective_from` + `effective_to`, `start_date` + `end_date`, SAP `BEGDA` + `ENDDA`, `row_start` + `row_end`, etc.
- Current/active flag: `is_current`, `is_active`, `current_flag`, `current_record`, `is_latest` (filtered to `bool` / `char(1)` / `varchar(1)` dtypes so a regular int isn't tagged).
- Version / revision / sequence: `version`, `revision`, `rev_no`, `seq_no`, `row_version`, `scd_version`, `history_seq`.

**Type 3 (history-as-columns) signals:**
- Paired columns where one has a "previous" prefix and the canonical sibling exists: `prev_status` ↔ `status`, `old_address` ↔ `new_address`, `previous_price` ↔ `current_price`, `before_X` ↔ `after_X`, `last_X` ↔ `X`. Matched against `prev_*` / `previous_*` / `old_*` / `former_*` / `before_*` / `last_*` against canonical / `new_*` / `current_*` / `now_*` / `after_*` siblings.

**Type 4 (separate history table) signals:**
- Companion table named `<table>_history`, `<table>_hist`, `<table>_audit`, `<table>_log`, `<table>_archive`, `<table>_versions`, `<table>_changes`, `<table>_snapshot` in the same schema (live-DB lookup, not catalog).

**Type 1 vs Type 2 row-cardinality probe (optional):**
- When the caller passes `business_key` (one or more columns), the tool runs `SELECT COUNT(*), COUNT(DISTINCT (cols))` against the live DB. Avg rows-per-key ≤ 1.05 → Type 1 (current-only). Avg > 1.5 → Type 2 (history rows). In between is reported as ambiguous so the LLM doesn't over-claim.

**Result shape:**

```json
{
  "scd_type_hypothesis": "type_2",            // type_1 / type_2 / type_3 / type_4 / unknown
  "confidence": "high",                         // high / medium / low
  "evidence": [                                 // ← always quote these in the answer
    "Type 2 temporal pair: `valid_from` + `valid_to`.",
    "Type 2 current-flag column: `is_current` (dtype=bool).",
    "Avg rows-per-business-key = 3.42 → multiple rows per key (likely Type 2)."
  ],
  "indicators": {
    "type2_temporal_pair": ["valid_from", "valid_to"],
    "type2_current_flag": "is_current",
    "type2_version_col": "version",
    "rows_per_key_avg": 3.42
  },
  "alternative_hypotheses": ["type_6 (Type 2 in main + Type 4 sibling = hybrid)"],
  "recommendation": ""
}
```

**System prompt rule:** "User asks 'how does X hold history' / 'is this SCD2' / 'değişiklik tek satırda mı yeni satır mı' / 'eski değerler nasıl tutuluyor' → call `detect_scd_pattern`. The result includes `scd_type_hypothesis`, `confidence`, `evidence` (ALWAYS quote in the answer; the hypothesis alone is misleading), and `alternative_hypotheses` for hybrid cases. When evidence is empty, suggest the user provide a candidate `business_key` so the rows-per-key probe can disambiguate Type 1 vs Type 2."

### Why this matters
SCD detection is the kind of pattern recognition that could spawn a dozen ad-hoc patches over time ("did you check is_current?", "did you check valid_from?", "is there a history table?"). One purpose-built tool with a structured return + multiple signal sources covers the whole question class, lets the LLM be honest about confidence, and surfaces hybrid (Type 6) cases instead of forcing a single label. Same `kind` / `inference_source` / `evidence` design pattern as v0.9.7-v0.10.4.

### Followups
- Per-row temporal-pair validation: when Type 2 is inferred, verify that `valid_from <= valid_to` for every row and surface violations as data-quality findings.
- CDC stream detection: when Type 1 + an `updated_at` column, hint that change-data-capture would be needed for actual history.
- v0.11 lineage will give the third leg — even when in-table signals are absent, the upstream load job's design tells whether history is preserved.

## [0.10.5] - 2026-05-01
### Added
- **`sample_column_values(schema, table, column, limit=5)`** — lightweight tool that returns distinct non-null example values via a direct `SELECT DISTINCT col FROM schema.table WHERE col IS NOT NULL LIMIT N`. Bypasses `profile_table` (which scans every column + foreign keys + stats) so a "give me an example" question doesn't pay full-table-profile cost. Result also includes `distinct_count` (single-column `COUNT(DISTINCT)`, soft-fails on un-indexed huge columns).

### Fixed
- **"give me a sample value from X column" was incorrectly routing to `/search sync` advice** when the agent picked the wrong schema. Reproducer: user asked "format of `aedat` in `bkpf`" — agent picked `public.bkpf` (PG default schema) instead of `sap_s6p.bkpf`, `profile_table` failed because the table doesn't exist in `public`, and the agent told the user to run `/search sync` (misleading — the catalog isn't the issue; the agent just chose the wrong schema). 

  Two-part fix:
  1. The new `sample_column_values` tool is the right fit for "give me an example" — direct SELECT, no profile overhead. It returns a structured `error` + `hint` when the schema is wrong, instead of the cryptic profile-table failure.
  2. System prompt rule: "user asks for a sample/example value AND didn't qualify the schema → call `find_table_by_name` FIRST so you don't blindly pick the wrong schema. Only fall through to `/search sync` hints when find_table_by_name returns NO exact AND no fuzzy matches."

### Why this matters
The previous flow taxed the user twice: once for asking the wrong agent path (full-table profile when they wanted one example), and once for the misleading recovery hint (`/search sync` when the catalog wasn't even relevant). User feedback: "I'll run sync, but is sync expensive every time? My question is just for an example — shouldn't AMX query the database directly?". Right. With v0.10.5 the agent has a cheap direct path AND knows to resolve the schema first instead of guessing.

### Followups
- The `find_table_by_name` → `sample_column_values` chain could be inlined as a one-shot helper for the common case ("just give me an example value of X.Y, figure out the schema yourself") — but the tool-level chain is more flexible and matches the existing routing pattern.

## [0.10.4] - 2026-05-01
### Fixed
- **`/ask "which tables have date related columns"` returned "no date columns"** on schemas where dates are stored as varchar (SAP-style `erdat`, `audat`, `*_date`, etc.). Pre-v0.10.4 `_DTYPE_FAMILIES["date"]` was just `["date"]` — `timestamp`/`timestamptz`/`datetime` were misses, and varchar-with-date-name columns were never considered.

### Changed
- **`_DTYPE_FAMILIES` semantic-bucket expansion** (`amx/search/agent_tools.py`):
  - `date`, `timestamp`, `time`, `temporal` are now full temporal families covering `date`, `timestamp`, `timestamptz`, `datetime`, `datetime2`, `smalldatetime`, `time`, `timetz`, `timestamp_ntz`, `timestamp_ltz`. The user can ask any of those tokens and get the same broad coverage.
- **Name-pattern inference for temporal columns** — when token is one of `{date, timestamp, time, temporal}`, after the dtype query runs, a second catalog query matches column names against well-known temporal patterns:
  - Suffix patterns: `*_date`, `*_dt`, `*_at`, `*_time`, `*_ts`
  - Prefix patterns: `dat_*`, `date_*`, `time_*`
  - SAP-specific names: `erdat`, `audat`, `ernam_dat`, `letzd`, `valid_from`, `valid_to`, `begda`, `endda`, `rldat`, `psotg`, `tzonso`
  - Generic timestamp patterns: `created*`, `updated*`, `modified*`, `deleted*`
  
  Restricted to string-family dtypes (`char`, `varchar`, `text`, `string`) so a numeric column with `_date` in its name doesn't get tagged.
- **`kind` field on every result row** — `native_temporal` (real date/timestamp dtype) or `name_inferred_temporal` (varchar with date-like name). Same shape as the v0.9.8 boolean `flag_candidate` tagging.
- **Tool description rewritten** to make the semantic-bucket contract explicit: when LLM queries `boolean` / `date` / `timestamp` / `time`, it ALSO gets columns whose semantics match even though the dtype doesn't. Explicit rule: "NEVER say 'no date columns' when name_inferred_temporal rows are present — say 'no native date dtype, but the schema stores dates as varchar with names like X, Y, Z'".

### Why this matters
Same false-negative pattern as v0.9.7 (joins) / v0.9.8 (boolean flags) / v0.9.10 (dtype overview) / v0.9.11 (table-name fuzzy) / v0.10.3 (duplication / update-soon). The recurring problem: AMX answered the literal question ("what columns have native dtype X") instead of the semantic question ("what columns CARRY semantics X"). Each release closes a category. v0.10.4 covers temporal columns and reuses the v0.9.8 `kind`-tagging precedent so the LLM stays honest about how the match was found.

### Followups
- Apply the same name-pattern inference to other semantic categories: `email` / `phone` / `currency` / `id` (the user's original v0.6.4 push-back use case). Each gets its own pattern set + a `kind=name_inferred_<category>` tag.
- A reverse query: `find_columns_by_pattern(name_pattern)` — instead of mapping pattern → semantic, take an explicit pattern and return matches. Useful for advanced users who know exactly which suffix they want.

## [0.10.3] - 2026-05-01
### Fixed — interpretive answering for "duplication" + "update soon"
Two more cases where the agent fell back to literal "I don't know" / "give me columns" instead of using the data it actually had access to. Both followed v0.9.11's interpretive-answering principle: surface what's available, be explicit about limits.

- **"is there any duplication in vbak"** — pre-v0.10.3 the agent called `check_uniqueness` without columns, hit the no-PK branch, got a useless `error: "pass columns explicitly"` payload, and bounced the question back to the user. Fix: when `check_uniqueness` is called with no columns AND the table has no declared PK, the tool now **runs `inspect_data_quality` itself** and returns:
  - `duplicate_summary` (full inspect_data_quality payload)
  - `likely_unique_columns` (every column whose `distinct_ratio` ≥ 0.99)
  - a `hint` field telling the LLM to propose a candidate composite key from `likely_unique_columns` and offer to verify it with a follow-up `check_uniqueness` call.
  
  The LLM now has data to work with on the first round. The system prompt was updated with a matching routing rule: "user asks 'is there duplication' WITHOUT naming a candidate key → call inspect_data_quality first, propose the most likely composite key, offer to verify; NEVER bounce back asking for columns".

- **"is there any update on vbak tables soon?"** — pre-v0.10.3 the agent answered "I don't have access to information about upcoming updates", which is technically true but useless. AMX *does* know when the table was LAST modified (via `analytics.last_modified` from v0.10.0), and *does not* yet have an ETL/orchestrator tap. The honest answer is both halves of that, not one or the other. New system prompt rule:
  > User asks 'when was X last updated' / 'is there an update soon' / 'son güncelleme' / 'next refresh' / 'ETL ne zaman çalıştı' → call describe_table and read `analytics.last_modified`. NEVER answer "I don't know about future updates" as a flat response — instead surface the LAST known modification time AND state explicitly: "AMX can see vbak was last modified at `<ts>` (from `<backend's freshness signal>`). Scheduled future updates require an ETL / orchestrator tap that AMX doesn't currently expose — that's a planned v0.11 feature."

### Why this matters
Both bugs were the same shape as v0.9.7-v0.9.11: tool returned a thin/empty primary result and the LLM treated that as the answer. The fix pattern is the same too: enrich the tool response with a wider-net field (here: auto-derived `duplicate_summary`) and teach the system prompt to surface what's available + name the limit explicitly. That's the v0.9.11 "interpretive answering" rule applied to two new question shapes.

### Followups
- ETL / orchestrator tap (the actual answer to "next refresh" — Airflow / Dagster / dbt Cloud) is the v0.11 lineage feature; currently we only know "last_modified", not "next_run_at".
- A column-rarity heuristic: when `inspect_data_quality` finds many columns with `distinct_ratio = 1.0`, the tool could also recommend the SHORTEST tuple that uniquely identifies rows (instead of returning every candidate).

## [0.10.2] - 2026-05-01
### Added — Data-quality + uniqueness probes

User shared a wishlist of questions their analyst friends would actually ask AMX. Several mapped cleanly to two new tools that the search agent now ships, both calling the live DB:

- **`check_uniqueness(schema, table, columns?)`** — runs `SELECT COUNT(*), COUNT(DISTINCT (col1, col2, ...))` and reports `total_rows`, `distinct_rows`, `duplicate_rows`, `uniqueness_ratio`, `is_unique`. When `columns` is omitted it falls back to the table's declared primary key. Answers questions like "is `id` a unique key or do I need `(id, time, op)`?", "are the PKs duplicated?", "do composite PKs collapse if I drop one column?".

- **`inspect_data_quality(schema, table, columns?)`** — per-column live-DB stats: `null_count`, `null_ratio`, `distinct_count`, `distinct_ratio`, `min_value`, `max_value`, plus `detected_format` for varchar/text columns whose samples look like dates. Format detection covers ISO 8601, `YYYY-MM-DD`, `YYYY/MM/DD`, `YYYYMMDD`, `DD-MM-YYYY`, `DD/MM/YYYY`, `DD.MM.YYYY`, and a few short forms — first-match-wins with a 60% confidence threshold so a table that just happens to have a few date-shaped strings doesn't get mislabelled. Answers "how many nulls in `email`?", "date format `ddmmyyyy` mi?", "ne zamandır tutuluyor?" (read `min_value`/`max_value` of the date column), "çoklama oranı?".

### Changed
- **System prompt** routes the new questions to the right tool. Concrete examples in the prompt: "is X a primary key" / "(id, time) unique mi" / "composite PK gerekli mi yoksa id yeter mi" → `check_uniqueness`; "date format nedir" / "ne zamandır tutuluyor" / "çoklama oranı" / "how nullable is X" → `inspect_data_quality`.

### Why this matters

The user's analyst friends asked questions like "PK duplicate oluyor mu?", "date'in formatı ddmmyyyy mı yoksa dd-mm-yy mi?", "ne zamandır tutuluyor?", "çoklama durumları var mı?". Pre-v0.10.2 the agent had no tool that answered these directly — `describe_table` knew the structure but never queried actual values, and the catalog rarely carries this information. With these two tools the agent can give grounded data-aware answers without falling back to "you'd need to check that yourself".

### Followups

- **Datamart / aggregate-table detection** ("Bu tablo için belirli bir tarih ya da segment için oluşturulmuş datamart var mı") — naming-pattern heuristic (`_summary` / `_dm` / `_mart` / `_history` / `_snapshot` / `_agg_` / `_daily_` / `_monthly_`) over the catalog. Planned for v0.10.3.
- **ETL / refresh-frequency inference** ("update edilme nasıl işliyor") — needs query-history tap (same prerequisite as lineage; v0.11).
- **Best-join cardinality estimate** ("en uygun joinleme mantığı") — extend `find_joinable_tables` rows with sample-based join-cardinality (1:1 / 1:N / N:M) when the user has SELECT permission on both sides. Planned for v0.10.3.

## [0.10.1] - 2026-05-01
### Fixed
- **Suppressed false-positive `LLM returned EMPTY content` warning during tool-calling rounds** (`amx/llm/provider.py`). When the LLM returns `finish_reason=tool_calls` (or the legacy `function_call`), an empty content body is the expected OpenAI-protocol shape — the actual call lives in `message.tool_calls`. The pre-v0.10.1 code emitted a noisy `WARNING — LLM returned EMPTY content … Check model name, API key, and provider dashboard.` on every tool-call round, which surfaced in mid-stream of `/ask` answers and looked alarming despite being normal flow.

  Reproducer: any `/ask` question that triggers a tool-calling loop (i.e. anything more complex than chitchat) on `gpt-4o-mini` / Claude / Gemini through `litellm`. The warning fired once per tool-call round and bled through the live display panel.

  Fix: gate the warning on `finish_reason NOT IN {tool_calls, function_call}`. The genuine "model returned nothing" cases — `finish_reason` in `{stop, content_filter, length, end_turn, ""}` with no accompanying tool_calls — still warn loudly because those ARE the symptoms of a misconfigured key / wrong model / quota issue. Tool-call rounds drop to DEBUG level so users running with `AMX_LOG_LEVEL=debug` can still trace them when needed.

### Why this matters
The warning appeared on every multi-step `/ask` answer (search agent always uses tools), so users were seeing a confidence-eroding "EMPTY content / check your API key" message during what was actually a clean run. With this fix the log stays quiet during normal tool-calling and reserves the warning for the cases where it actually points at a problem.

## [0.10.0] - 2026-05-01
### Added — Analytics-DB metadata extension

AMX now extracts analytics-aware metadata for every profiled table — partition keys, clustering keys, storage format (native / parquet / delta / iceberg / external), storage size, file count, last-modified, table type, governance tags, PII columns, indexes — across all 4 backends (PostgreSQL, Snowflake, BigQuery, Databricks). The fields surface in the `describe_table` tool result so the search agent can answer the questions analytics-DB users actually ask: performance optimization opportunities, freshness, storage footprint, governance, format.

**Why this matters.** AMX's previous metadata coverage was tuned for OLTP-style introspection (FK relationships, table comments, column samples). Analytics workloads — Snowflake / BigQuery / Databricks where AMX is most useful — care about partitions, clustering, size, format, last-altered, and governance tags. Without that metadata the agent could only chitchat about "performance optimization" instead of pointing at a specific opportunity.

### Changes

- **New `AnalyticsMetadata` dataclass** (`amx/db/connector.py`) — per-table fields:
  - `partition_keys: list[str]` + `partition_strategy: str` (range / list / hash / time / bucket / none)
  - `clustering_keys: list[str]` (Snowflake CLUSTER BY, BigQuery CLUSTER BY, Databricks ZORDER)
  - `storage_format: str` (native / parquet / delta / iceberg / csv / external)
  - `storage_bytes: int`, `storage_files_count: int`
  - `last_modified: str` (ISO 8601)
  - `table_type: str` (managed / external / view / materialized_view / temporary / foreign)
  - `tags: dict[str, str]` (column-or-table → tag value)
  - `pii_columns: list[str]` (auto-derived from tag names containing PII / SENSITIVE / GDPR)
  - `indexes: list[dict]` (`{name, columns, unique}`; PostgreSQL only for now)
  - `warnings: list[str]` — per-adapter best-effort: when a query fails (permissions, unsupported region, view-not-allowed), the affected field is left empty and a warning is recorded. The agent surfaces these so users know the scope of "no data" answers.
- **`TableProfile.analytics` field** — populated by `profile_table` via the new `AdapterBase.get_analytics_metadata(engine, schema, table)` method. Old call sites continue to work unchanged.
- **PostgreSQL adapter** — partition info from `pg_partitioned_table` + `pg_attribute`; indexes from `pg_indexes`; storage size from `pg_total_relation_size` (table + TOAST + indexes); last-modified from `pg_stat_user_tables` (max of last_analyze / last_autoanalyze / last_vacuum / last_autovacuum); table_type from `pg_class.relkind` (view / materialized_view / partitioned / foreign).
- **BigQuery adapter** — partition / cluster / type / DDL parsed from `INFORMATION_SCHEMA.TABLES`; size + last_modified_time from legacy `__TABLES__`. Partition expressions like `DATE(_PARTITIONTIME)` are recognised and tagged as `time`-strategy.
- **Snowflake adapter** — clustering_key + bytes + last_altered + table_type from `INFORMATION_SCHEMA.TABLES`; tags + PII column derivation from `TAG_REFERENCES_ALL_COLUMNS` (soft-fails when the role lacks permission).
- **Databricks adapter** — `DESCRIBE DETAIL` for format / size / partition / clustering / lastModified; `DESCRIBE TABLE EXTENDED` for table type. Supports Delta, Parquet, Iceberg, CSV, and external tables. ZORDER columns surface as `clustering_keys`.

### Tool integration

- **`describe_table` returns the new `analytics` field** (`amx/search/agent_tools.py`) — only non-empty sub-fields are included to keep the prompt tight on backends that expose less.
- **Tool description rewritten** to teach the LLM the analytics fields and how to use them. Concrete examples in the prompt cover the user's wishlist:
  - "is there a performance optimization opportunity?" → check `partition_keys`, `clustering_keys`, `indexes`, `storage_bytes` vs `row_count`.
  - "when was X last updated?" → `last_modified`.
  - "which tables are > 1 TB?" → `storage_bytes` (cross-table — would need multi-call from caller).
  - "is there any PII column in finance schema?" → `pii_columns` + `tags`.
  - "what format is sales_fact stored in?" → `storage_format`.
- **System prompt rule** — when fields are absent (because the backend doesn't expose that signal), the LLM should say "this DB doesn't surface partition info" instead of "this table has no partition" — same interpretive-answering principle from v0.9.11.

### Backend coverage matrix

| Field | PostgreSQL | Snowflake | BigQuery | Databricks |
|---|---|---|---|---|
| partition_keys / strategy | ✓ | — (handled via clustering) | ✓ | ✓ |
| clustering_keys | — | ✓ | ✓ | ✓ (ZORDER) |
| storage_format | native | native / external | native / external | delta / parquet / iceberg / csv / external |
| storage_bytes | ✓ | ✓ | ✓ | ✓ |
| storage_files_count | — | — | — | ✓ |
| last_modified | ✓ | ✓ | ✓ | ✓ |
| table_type | ✓ | ✓ | ✓ | ✓ |
| tags / pii_columns | — | ✓ | partial (column policy tags follow-up) | partial (Unity Catalog tags follow-up) |
| indexes | ✓ | — | — | — |

### Followups

- **Lineage** — `inventory_reports → upstream tables`. Requires query history tap (Snowflake QUERY_HISTORY, BigQuery `INFORMATION_SCHEMA.JOBS`, Databricks SystemTable). Out-of-scope for v0.10.0 because each backend exposes it differently and the per-call cost is high. Planned for a separate `lineage_for_table` tool.
- **Schema evolution** — diff between two snapshots. Requires AMX to keep periodic snapshots of `columns_by_dtype` per table, then expose a `compare_schema_snapshots` tool. Tracked as v0.11 work.
- **Cross-table aggregates** — "which tables > 1 TB" needs a list-then-filter call across the whole catalog. The agent currently has to call `describe_table` per table; a future `find_tables_by_size_range` tool will batch this.
- **Column-level analytics** — null ratios per column are already in `ColumnProfile.null_count` / `row_count`; surfacing them through `describe_table` is a small follow-up.

## [0.9.11] - 2026-05-01
### Fixed
- **`/ask "I only remember 'trog' from the table name"` returned "no table similar to trog"**. Reproducer: any partial / approximate / fragment table-name query. Pre-v0.9.11 `find_table_by_name` did exact-match only (`asset_name.lower() == target.lower()`), so a non-exact fragment returned 0 and the LLM honestly said "nothing found".

### Changed — interpretive answering, not another patch
- **`find_table_by_name` adds substring + fuzzy fallback** (`amx/search/agent_tools.py`). Result now carries:
  - `matches` — exact-name hits (catalog + live DB), unchanged.
  - `fuzzy_matches` — list of `{path, match_kind}` where `match_kind` is `prefix` / `suffix` / `contains` / `fuzzy`. Populated from BOTH the live DB walk and the catalog scan, ranked by tier (prefix > suffix > contains > fuzzy) and by table-name length within each tier (shorter names win as closer hits). Capped at 25 entries to keep prompts tight on huge schemas.
  - `match_kind=fuzzy` uses `SequenceMatcher` ratio ≥ 0.7 with length difference ≤ 3 — calibrated for short SAP-style names (4-8 chars) so single-character typos / dropped letters surface.
- **System prompt — new "Interpretive answering" section** (`amx/search/tool_agent.py`). The rule, in plain English: NEVER reply with a flat "no" — look for adjacent fields in the tool response before declaring nothing found. Concrete examples in the prompt cover the four cases AMX has surfaced this week (fuzzy_matches, columns_truncated → columns_by_dtype, find_joinable_tables inference_source, dtype_summary). General rule: "if you're going to say 'no X', double-check that no related field (fuzzy_matches, dtype_summary, columns_by_dtype, inference_source, kind) carries the answer in another shape."

### Why this matters

The user's broader complaint — accumulating individually-patched question shapes ("we can't enumerate every dtype-question one by one", "this is whack-a-mole") — keeps surfacing because the system was treating tool results as primary facts and the LLM was treating empty primary lists as authoritative answers. v0.9.10 fixed that for dtype questions; v0.9.11 generalises it: every tool now ships a "wider net" field (fuzzy_matches / dtype_summary / inference_source / kind) and the system prompt teaches the LLM to read those fields before saying "no". The next time a user types a partial name, asks about a rare dtype, or queries an FK-free schema, the answer should be useful instead of confidently empty.

### Followups
- Apply the same `fuzzy_matches` pattern to `find_table_by_name`'s sibling tools (`find_columns_by_concept`, `search_columns_by_concept`) so concept queries that miss exact tokens fall through to substring/fuzzy + cite the partial-match tier.
- The 0.7 SequenceMatcher threshold is calibrated for short identifiers; longer table names (e.g. snake_case `customer_address_history`) may need a separate threshold or a token-overlap match.

## [0.9.10] - 2026-05-01
### Fixed
- **`/ask "which columns are int or double in vbak"` returned "no" with confidence**. Reproducer: any wide-table dtype question. The user pointed out the meta-pattern after v0.9.7 (joins) / v0.9.8 (boolean flags) / v0.9.9 (truncation) all required hand-tuning a separate question shape: "we can't enumerate every dtype-question pattern one by one". Right — the underlying problem was that AMX wasn't giving the LLM a complete dtype picture, so each question class needed a separate fix.

### Changed — design fix, not another patch
- **`describe_table` now returns `columns_by_dtype: {family: [column_names]}` — complete coverage, never truncated** (`amx/search/agent_tools.py`). On a 200-column SAP `vbak` the LLM now sees:
  ```
  {
    "bool":   ["is_deleted"],
    "int":    ["mandt", "vbeln", "posnr", ...],
    "float":  ["gwldt", "submi", "lifsk", ...70 names...],
    "string": ["autlf", "kunnr", "vkorg", ...],
    "date":   ["erdat", "audat", ...],
    "timestamp": ["created_at", "updated_at"]
  }
  ```
  No matter how the user phrases their question ("int or double", "boolean", "which columns are dates", "any json columns"), the LLM has the complete answer in one map. No more whack-a-mole.

- **Tool description rewritten** to teach the LLM the new contract:
  - `dtype_summary` — counts across ALL columns (authoritative for "how many").
  - `columns_by_dtype` — names across ALL columns, NEVER truncated (authoritative for "which columns").
  - `columns` — sorted-and-truncated detailed metadata (use ONLY for comments / nullability per column, NOT for dtype questions).
  - Explicit rule: "when the user asks 'which columns are dtype X', the COMPLETE answer is in `columns_by_dtype` — read it directly and list the names. Do NOT say 'no X columns' unless the family key is absent or the list is empty."

### Why this matters

The user's diagnosis was correct: this is a **design problem, not a bug**. v0.9.7 / 0.9.8 / 0.9.9 all added more conditional logic ("if the user asks about boolean, also try `char(1)`"; "if the table is wide, sort by interestingness"; "if the dtype is `int`, expand the family map"). That approach scales linearly with question variety. v0.9.10 inverts it — the LLM gets the complete dtype map up front and reasons from there. Future dtype questions ("any json column", "any uuid", "are there bytea fields") need no AMX-side change.

The same lesson applied to v0.9.7's join discovery (3-tier fallback chain with `inference_source`) and v0.9.8's boolean flag (kind-tagging native_boolean / flag_candidate). The pattern: give the LLM a complete picture + honest source attribution + don't pretend a partial result is exhaustive.

### Followups
- Apply the `columns_by_dtype` pattern to `find_columns_by_dtype` for cross-table queries: instead of returning rows by family, return all rows with their families pre-tagged so the LLM can group them however the question demands.
- The same "complete map" principle should extend to other surface gaps: e.g. `describe_table` could ALSO return per-column "samples" (top-N distinct values) so the LLM can answer "which columns hold currency codes" without a separate live-DB probe.

## [0.9.9] - 2026-05-01
### Fixed
- **`describe_table` truncated wide tables before the LLM could see boolean columns**. Reproducer: user asks "do vbak have any boolean column?" against an SAP schema where `vbak` has 155+ columns and the only `bool` column is `is_deleted` at column position 155. Pre-v0.9.9, `_tool_describe_table` returned `cols[:60]` — `is_deleted` was past the cap and invisible. The LLM read 60 columns, saw no `bool` dtype, and said "no native boolean columns" with confidence. (`is_deleted` IS a real PG `bool` here; the v0.9.8 char(1) flag fallback didn't apply.)

### Changed
- **`describe_table` now ships a `dtype_summary` field** (`amx/search/agent_tools.py`) — a `{"bool": 1, "int": 30, "float": 70, "string": 50, ...}` count of dtype families across **all** columns (not just the truncated head). This is the LLM's authoritative source for "does this table have a column of family X" — it travels with the prompt even when the columns list is truncated. Tool description now instructs the LLM to ALWAYS read `dtype_summary` instead of inferring from the truncated `columns` list.
- **Smart truncation order** — when the columns list is capped at 60, the cap now applies to a sorted list, not insertion order. Columns are sorted by:
  1. Commented columns first (someone curated them — they're worth seeing).
  2. Rare dtypes next (a single `bool` column on a table with 100 `float8`s gets surfaced ahead of the floats).
  3. Alphabetical tiebreak.
  This means `is_deleted` (the only `bool` on `vbak`) now sits in the first batch of returned columns instead of being silently dropped at position 155.
- **New `columns_truncated: bool` field** so the LLM knows whether to caveat its answer with "showing X of Y columns; use find_columns_by_dtype for the complete dtype picture".
- **New `_dtype_family_label` static helper** that maps raw dtypes to coarse family labels (`bool` / `int` / `float` / `string` / `date` / `timestamp` / `time` / `json` / `uuid` / `binary` / fallback to lowered raw). Same vocabulary as the equivalence-class deduplication module, kept as a static method so it can be reused without instantiating the tool box.

### Why this matters
Same false-negative pattern as v0.9.7 (joins) and v0.9.8 (boolean flags): the agent's tool surface didn't carry enough information for the LLM to answer correctly, and the LLM's "I checked and found nothing" confidence misled the user. The combination of `dtype_summary` (complete picture) + smart-sorted `columns` (rare dtypes survive truncation) + `columns_truncated` (explicit honesty signal) closes the gap for wide tables.

### Followups
- Apply the same dtype_summary pattern to `find_columns_by_dtype` so list-mode answers also include a per-family summary even when the result is truncated.
- The hardcoded 60-column cap could become a soft target — keep ALL rare dtypes (`bool`, `date`, `uuid`, `json`) regardless of position, then fill the rest of the budget with the truncation-sorted head.

## [0.9.8] - 2026-05-01
### Fixed
- **`/ask "is there any boolean column in vbak"` returned "no" with confidence on SAP-style schemas**. Reproducer: any DB where boolean SEMANTICS are stored as `char(1)` / `varchar(1)` flag columns (`'X'` / `''` or `'Y'` / `'N'`) — the dominant pattern in SAP and many enterprise systems. Pre-v0.9.8, `find_columns_by_dtype('boolean')` only matched literal `bool` / `boolean` PG dtypes, so SAP `vbak`'s flag columns (`autlf`, `faksk`, `lifsk`, …) were invisible and the LLM honestly said "no boolean columns" — same false-negative pattern as the join-discovery bug fixed in v0.9.7.

### Changed
- **`_DTYPE_FAMILIES["boolean"]` now includes single-character fixed-width strings** (`amx/search/agent_tools.py`): `bool`, `boolean`, `char(1)`, `varchar(1)`, `character(1)`, `character varying(1)`. The query continues to match by `IN (...)` plus `LIKE '%token%'`, so any column whose dtype contains `(1)` and a char-family base will surface.
- **Each result row carries a new `kind` field** — `native_boolean` (real `bool` / `boolean` dtype), `flag_candidate` (single-char fixed-width that's commonly used as a flag), or `exact_dtype_match` (any other dtype family). The `kind` field is propagated through the per-table roll-up so the LLM gets it whether it reads the flat list or the grouped output.
- **Tool description for `find_columns_by_dtype` now spells out the boolean semantics** and instructs the LLM: do NOT say "no boolean columns" when `flag_candidate` rows are present — say "no native boolean columns, but the table has these likely flag columns: …" and list them. Without this rule the LLM defaults to a literal interpretation of "boolean" that's wrong for the user's question.
- **Tool description for `describe_table`** now includes the same boolean-flag guidance, since `describe_table(vbak)` is the natural call when the user names a specific table — the LLM should scan the column list for `char(1)`/`varchar(1)` flags and surface them alongside any native booleans.

### Why this matters
Same lesson as v0.9.7 (join discovery on FK-free schemas): the catalog's surface schema doesn't always carry the user's semantic intent. "Boolean" almost always means "the column has Y/N or X-blank semantics" — even though the stored type is `char(1)`. Returning an empty list with confidence misled users into thinking AMX had searched the table when in fact it had answered a literal-dtype question that had nothing to do with the user's actual data model.

### Followups
- A future tier could ALSO probe the live DB for distinct-value distribution on flag candidates: a `char(1)` column with cardinality ≤ 2 and values like `{X, ''}` or `{Y, N}` is virtually certain to be a boolean flag, while one with `{A, B, C, D, …}` is a category code. AMX could surface that distinction as a confidence band on each `flag_candidate` row.
- The same `kind`-tagging pattern should extend to other "semantic" queries (date columns hidden as `varchar(8)` in `YYYYMMDD` form, percentages stored as `numeric(5,2)`, etc.).

## [0.9.7] - 2026-05-01
### Fixed
- **`/ask "which tables can I join with vbrk"` returned 0 candidates on FK-free schemas**. Reproducer: SAP / legacy schemas with no declared `FOREIGN KEY` constraints. Pre-v0.9.7, `find_joinable_tables` only consulted `catalog_relationships` (populated from `profile.foreign_keys` / `profile.referenced_by` during `/sync`); when those were empty the tool returned `joinable_tables=[]` and the LLM honestly said "no joinable tables found", which is wrong — `vbrk` is the SAP billing header and should obviously join with `vbrp`, `kna1`, `vbpa`, etc. on shared keys like `vbeln` and `mandt`.

### Added
- **Name-overlap heuristic — `JoinMixin.name_overlap_joinable_tables`** (`amx/search/_catalog/join.py`). For schemas without FK constraints AND without per-column descriptions yet (catalog hasn't been `/run` -populated), the cheapest signal that two tables might be joinable is "they share a column NAME". Common columns like `mandt` or `id` give a low-signal hit and are deweighted by an inverse-log rarity score: a column present in N tables contributes `1 / log2(N+1)` to the join weight. So a column shared with only 3 other tables (rare, high-signal — likely a real foreign key by convention) wins over `mandt` (shared by every table in the schema). Returns rows in the same shape as `joinable_tables` so existing renderers / tool-result schemas work unchanged.

### Changed
- **`find_joinable_tables` now follows a 3-tier fallback chain** (`amx/search/agent_tools.py`):
  1. **Symbolic** — `catalog.joinable_tables` (declared FK relationships, score=10.0, the strongest signal).
  2. **Name-overlap** — `catalog.name_overlap_joinable_tables` (rarity-weighted shared column names, no FK / no descriptions needed).
  3. **Semantic** — `catalog.semantic_joinable_tables` (vector similarity on column descriptions, requires a populated catalog).
  The first non-empty tier wins; the result includes a new `inference_source` field (`foreign_key` / `name_overlap` / `semantic_similarity`) so the LLM can be honest in its answer ("via the declared `vbeln` foreign key" vs "by shared column names: `vbeln`, `posnr`" vs "by semantic similarity to `customer description`").
- **Tool description for `find_joinable_tables`** updated to spell out the three tiers and to instruct the LLM to ALWAYS surface the `inference_source` in the final answer. Without this rule the LLM would generate the same authoritative-sounding "verified joinable" prose for an FK-backed match and a name-inferred match, which would be misleading.

### Why this matters
SAP schemas (and most enterprise systems running on PostgreSQL / Oracle) manage referential integrity at the application layer, not the database layer. The pre-v0.9.7 code path was structurally unable to surface joins for those schemas — and the user was being told "no joins found" with confidence when in reality dozens of strong candidates existed via shared `vbeln` / `mandt` / `customer_id` columns. The name-overlap tier closes that gap without requiring `/run` to have completed.

### Followups
- The name-overlap rarity score could blend in dtype compatibility (`varchar(10)` vs `int4` for the same column name is a weaker signal than two `varchar(10)`s).
- A future tier could read the foreign-key conventions of the live DB (e.g. SAP table cross-references encoded in `DD03L`) when available.

## [0.9.6] - 2026-05-01
### Fixed
- **Cascading mixin import gaps** that v0.9.5 missed: 16 more module-level names referenced from mixin method bodies but not imported into their respective files. Reproducer surfaced after v0.9.5: `/ask` failed with `name 're' is not defined`.

  Full audit + fixes (15 stdlib/typing/project imports added across 12 files):
  - **`_agent/short_circuits.py`** — added `re`, `SearchAnswer`, `step_spinner`.
  - **`_agent/planning.py`** — added `json`.
  - **`_agent/resolution.py`** — added `asdict`.
  - **`_agent/retrieval.py`** — added `DatabaseConnector`.
  - **`_agent/session_memory.py`** — added `LLMProvider`.
  - **`_catalog/entity_crud.py`** — added `sqlite3`.
  - **`_catalog/sync.py`** — added `sqlite3`, `Callable`, `CodebaseReport`, `TableProfile`, `MetadataSuggestion`.
  - **`_catalog/search.py`** — added `sqlite3`, `SequenceMatcher`.
  - **`_catalog/usage.py`** — added `json`, `sqlite3`, `TableProfile`.
  - **`_catalog/join.py`** — added `CodeReference`.
  - **`_catalog/settings.py`** — added `time`.

  **Verification:** new full-scope name-resolution audit (every `ast.Name` in `Load` context across all 13 mixin files vs imports + locals + parameters + comprehension targets + tuple-unpacks + builtins) now returns **0 unresolved references**, up from 23 before this fix.

### Why this matters
v0.9.5 fixed the dataclasses (`SearchPlan` etc.) but skipped the long tail of bare-name references — stdlib modules (`re`, `json`, `time`, `sqlite3`), typing helpers (`Callable`, `asdict`), and project-level type names (`DatabaseConnector`, `LLMProvider`, `TableProfile`, `CodebaseReport`, `MetadataSuggestion`, `CodeReference`, `SearchAnswer`, `SequenceMatcher`, `step_spinner`). Each was originally a top-level import in `agent.py` / `catalog.py`; the mixin split lost them. A full-scope audit (not just keyword spot-checks) would have caught them at refactor time. The new audit script lives in this commit's notes — adding it to CI is the v0.10 followup.

### Followups
- Adopt the audit script as a `pre-commit` hook so any future mixin extraction can't ship with this class of regression.
- The original v0.9.0 / v0.9.1 commits' AST-only verification was insufficient; future refactor releases should run runtime-equivalent name-resolution audits before tagging.

## [0.9.5] - 2026-05-01
### Fixed
- **Mixin regression: `name 'DEFAULT_SETTINGS' is not defined` (and 16 sibling errors)**. Reproducer: `/ask` would crash with `Ask failed: name 'DEFAULT_SETTINGS' is not defined` after running v0.9.0–v0.9.4. Root cause: when v0.9.0 split `SearchAgent` and v0.9.1 split `SearchCatalog`, the moved method bodies still referenced module-level names (`SearchPlan(...)`, `_input_token_budget_for(...)`, `DEFAULT_SETTINGS`, `SOURCE_PRIORITY`, etc.) by bare name — but each mixin was now its own Python module, so those names weren't in scope. The bugs only fired on first invocation of the affected code paths (synthesizer, catalog `get_settings`, etc.), not at import time, so the AST-only verification I ran during the splits missed them.

  **Fix:** create two new shared modules — `amx/search/_agent/_types.py` and `amx/search/_catalog/_constants.py` — that own the dataclasses + helpers + constants. Both `agent.py` / `catalog.py` and every mixin file import from these shared modules. No circular imports because `_types.py` and `_constants.py` import nothing from their dependents. Public API preserved — `from amx.search.agent import SearchPlan` still works (re-exported).

  **Affected files this release fixes:**
  - `_agent/_types.py` (new) — `SearchPlan`, `SearchPolicy`, `SearchActionSuggestion`, `LiveProbePlan`, `ResolvedTarget`, `_ANSWER_SHAPES`, `_DEFAULT_INPUT_TOKEN_BUDGET`, `_input_token_budget_for`, `_json_block`, `_merge_usage`, `_question_language_hint`, `_trim_rows_to_token_budget`.
  - `_catalog/_constants.py` (new) — `DEFAULT_SETTINGS`, `SOURCE_PRIORITY`, `_PROVIDER_SCORE_FLOOR`, `_DEFAULT_SCORE_FLOOR`, `_vector_score_floor`, `_active_embedding_kind`, `_json_loads`, `_database_name`.
  - `agent.py` and `catalog.py` re-export from these shared modules (no inline duplicates).
  - 6 `_agent/*.py` mixins + 4 `_catalog/*.py` mixins now import what they reference from the shared modules.
  - `_agent/deterministic.py` — dropped its locally-redefined `_question_language_hint` (and the `SearchPlan = Any` forward-ref alias) in favour of the shared canonical version.

### Why this matters
The v0.9.0 / v0.9.1 splits demonstrated the limitation of AST-only verification when refactoring across module boundaries. Splitting a god-class into mixin modules requires every name a mixin method references to either be (a) a `self.*` attribute, (b) imported into the mixin file, or (c) a Python builtin. Bare module-level names that worked in the original god-class file silently break at runtime. v0.9.5 closes that gap by introducing the shared `_types.py` / `_constants.py` modules; future splits should follow this pattern (do the AST analysis, then run a name-resolution audit against each mixin) before shipping.

### Followups
- Add a CI-time check that imports each mixin module standalone and instantiates the parent class against a stub to catch this class of bug pre-release.

## [0.9.4] - 2026-05-01
### Changed — `execute_analyze_run` extracted into 3 phase helpers (S4 refactor)

The `/run` and `/run-apply` entry point — `execute_analyze_run` — was a 600-line procedural script juggling 12 local variables across 3 top-level exception handlers (`FatalLLMError`, `KeyboardInterrupt`, `Exception`) and a `finally` block. Three different bugs surfaced here in this conversation alone (UnboundLocalError on Ctrl+C during scope picker, dedup-question ordering, `review_strategy` initialization) — every change risked tripping over neighbouring state.

v0.9.4 lifts the three largest contiguous chunks into standalone functions under a new `amx/cli_support/commands/_analyze/` package:

```
amx/cli_support/commands/analyze_flow.py        1099 →  877 LOC  (-20%)
amx/cli_support/commands/_analyze/
  __init__.py                                     -   →   35 LOC   (re-exports)
  run_loop.py                                     -   →  227 LOC   (PerSchemaLoopResult dataclass + run_per_schema_loop + chat-mode helper)
  run_summary.py                                  -   →  206 LOC   (render_summary_and_apply + dedup recap + apply branch)
  interrupt.py                                    -   →   85 LOC   (handle_keyboard_interrupt — final-status decision)
```

**`execute_analyze_run` is now 390 lines** (was 600). The remaining body is mostly the 4 runtime prompts (dedup / scope / coverage / review-strategy) plus history-run creation plus the equivalence-dedup pre-walk; each of those is short and tightly coupled to local state, so further extraction would just move pain around.

**Phase functions:**

* **`run_per_schema_loop(...)` → `PerSchemaLoopResult`** — replaces the `for schema_name, assets in scope.items()` block. Mutates the result lists in-place so the surrounding exception handlers can still inspect partial progress on cancel; returns the accumulated lists + the `last_orchestrator` so the caller can run `apply_results` after the loop. Internally splits chat-mode (`process_table` per asset, with history counter bumps) from batch-mode (`process_tables_batch_mode`) as helper `_process_assets_chat_mode`.
* **`render_summary_and_apply(...)` → `(approved, skipped)`** — replaces the post-loop block: deferred batch_review (skipped for auto-apply), token-tracker drop, summary heading, dedup recap (separate counter from per-table counts), approved-table render, save_pending, apply branch (auto-apply meta-only writes vs interactive confirm + write).
* **`handle_keyboard_interrupt(...)` → `(final_status, final_error_text)`** — replaces the `except KeyboardInterrupt:` body. Saves partial pending, decides between `cancelled` / `ready_for_review` based on `review_strategy` (auto-apply always cancels, others may be reviewable), emits the structured log event.

### Why this matters

Before v0.9.4, every new feature in the analyze flow (Phase 2 dedup, Column scope, equivalence pre-walk) had to thread its state through `execute_analyze_run`'s local namespace and remember to update each of the 3 exception handlers. The recent v0.8.3 fix — pre-init `review_strategy` to avoid an UnboundLocalError on Ctrl+C during scope picking — was a direct consequence of this fragility.

Now:

* **Each phase is independently testable** — feed `run_per_schema_loop` a synthetic scope dict + stub Orchestrator, assert the counters / pending state.
* **The exception handlers no longer race the main path for shared variables** — `handle_keyboard_interrupt` consumes pre-defined values and returns its decisions explicitly.
* **Future features touch only the phase they affect** — adding e.g. a "stage 2 LLM verification pass" goes in `run_summary.py` next to `_emit_dedup_recap`, not in the middle of the orchestrator.

### Followups

* `execute_analyze_run` still has 4 user-prompt blocks (dedup choice, scope finalization, coverage filter, review strategy) interleaved with state setup; these could be extracted into a `RunBuilder` class in v0.9.5.
* `_finalize_history_run` (87 LOC) is a near-pure data formatter and could move into `_analyze/finalize.py` if `_analyze/` keeps growing.
* The codebase analysis still flagged `_run_bulk_edit_by_name` (290 LOC) as the next problematic method (S6); shorter than execute_analyze_run but the same procedural-script smell.

## [0.9.3] - 2026-05-01
### Changed — Slash commands collapsed into a single registry (S5 refactor)

Pre-v0.9.3 each AMX slash command had to be listed in **four** separate places inside `amx/cli_support/session.py` (1283 LOC):

1. `_slash_command_catalog` (~129 LOC) — autocomplete `(slash, short_description)` pairs per namespace.
2. `*_cmd_heads` frozensets in `run_interactive_session` (~63 LOC) — bare command names used by the dispatch chain.
3. `_print_session_help` (~298 LOC) — multi-line help blocks with numbered commands per namespace.
4. The dispatch `if head in db_cmd_heads: namespace = "db"` ladder.

Drift between those four sources was the root cause of the v0.6.1 / v0.6.2 regressions where new commands (the v0.5.x `/description-verbosity` setting) were missing from autocomplete + help even though they had handlers wired up. v0.9.3 collapses the data-side duplication into a single Python data module — `amx/cli_support/slash_commands.py` — that the autocomplete catalog and `cmd_heads` frozensets now derive from.

**File layout:**

```
amx/cli_support/session.py          1283 → 1125 LOC  (-158, -12%)
amx/cli_support/slash_commands.py     -  →  301 LOC  (single source of truth)
```

**New module — `slash_commands.py`:**

* `SlashCommand` frozen dataclass — one entry per command, fields: `command`, `namespace`, `short_desc`, `long_desc`, `aliases`, `cross_namespace`.
* `ALL_COMMANDS` — declared in registry order; 77 entries cover root + 8 sub-namespaces.
* `commands_for_namespace(ns)` — autocomplete pairs for that namespace (with cross-namespace builtins prepended).
* `cmd_heads_for_namespace(ns)` — bare-head frozenset used by the dispatch chain.
* `find_command(slash_or_head)` — resolve a user-typed token to its `SlashCommand` (handles aliases like `/manual` → `/metadata`).

**`session.py` adapter:**

* `_slash_command_catalog` is now a 5-line adapter that calls the registry.
* The 7 hand-maintained `*_cmd_heads = frozenset({...})` definitions inside `run_interactive_session` now read `_registry_cmd_heads("db")` etc., except the `search_cmd_heads` set still adds `embeddings`/`embedding` heads (those are routed through search but not first-class commands).
* `_print_session_help` is unchanged — its prose is text-heavy (engine lists, examples, navigation hints), not just a command list, so the registry can supplement but not replace it. A follow-up could enrich the help blocks from the registry's `long_desc` field.

**Drift fix found by the refactor:** `/tls` was listed in the autocomplete catalog but missing from `db_cmd_heads`, so typing `/tls` from root wouldn't enter the db namespace. With the registry as single source, `/tls` now correctly routes to db.

### Why this matters

Adding a new slash command is now a one-line edit to `slash_commands.py` instead of three coordinated edits across `session.py`. Drift between autocomplete and dispatch is structurally impossible — both derive from the same dataclass tuple. Future enhancements (per-command `requires_db`/`requires_llm` gating, `/help <command>` long-form rendering, validation checks for handler dotted-paths) all become feasible without re-introducing duplication.

### Followups

- `_print_session_help` (298 LOC, 8 namespace blocks) can be partially generated from the registry — the per-command rows could be auto-rendered while engine summaries / navigation hints stay hand-written.
- The `search_cmd_heads` extras (`embeddings`, `embedding`, `find-columns`, `join-candidates`, `explain`, `explain-table`) are still hand-listed; if these become first-class they should join the registry.

## [0.9.2] - 2026-05-01
### Changed — `Orchestrator.process_table` god-method extracted into `TableProcessor` (S3 refactor)

The 281-line `Orchestrator.process_table` method had grown four overlapping filter chains (missing-only, column-scope, dedup-skip), an agent loop, and three apply branches (auto-apply, deferred, interactive-review). Every recent feature (Phase 2 dedup, Column scope) added another filter at the top, pushing the method up to 281 LOC. v0.9.2 extracts that flow into `amx/agents/_orchestrator/table_processor.py` as a stateful helper class with one method per phase.

**File layout (before → after):**

```
amx/agents/orchestrator.py        1719 → 1463   (-256 LOC out of process_table)
amx/agents/_orchestrator/
  __init__.py                       -   →   12   (TableProcessor re-export)
  table_processor.py                -   →  447   (12 phase methods, none over 77 LOC)
```

**`Orchestrator.process_table` is now 25 lines** — a thin delegator that constructs a `TableProcessor` and calls `.run()`. Public signature is unchanged (`schema`, `table`, `asset_kind`, `interactive_review`, `auto_apply`).

**TableProcessor phase methods:**

```
run                            18 LOC   public entry point
_fetch_profile                  5 LOC   db.profile_table(...)
_apply_filters                  9 LOC   chain of 3 filters; bails on first 'False'
  _filter_missing_only         48 LOC   skip already-commented columns
  _filter_column_override      32 LOC   restrict to Column-scope picks
  _filter_dedup_skip           33 LOC   drop columns handled by upfront dedup pass
_run_agents_and_persist        51 LOC   Profile / RAG / Code agents + merge + save
_dispatch_apply_or_review      12 LOC   pick branch by run-mode
  _auto_apply_branch           77 LOC   accept top suggestion + write live DB
  _deferred_branch             25 LOC   wrap as un-applied for batch review
  _interactive_review_branch   33 LOC   prompt-toolkit picker w/ live-display pause
```

### Why this matters

`process_table` was the architectural pain point named in the v0.9 codebase analysis: every new analyze-flow feature landed in this single method, and every feature pushed it 30-40 LOC larger. After this refactor:

- **Each filter is unit-testable in isolation.** `_filter_missing_only` no longer needs a full agent stack to test — give it a `TableProfile` and `orch.missing_only=True`, assert the column list shrinks correctly.
- **New filters land beside their siblings, not on top.** Adding e.g. a `_filter_pinned_columns` (next-release request) becomes a 30-LOC method next to the existing three; the chain method `_apply_filters` adds one line.
- **The three apply branches stop competing for context.** `_auto_apply_branch` (77 LOC) is the only one that touches the live DB; `_deferred_branch` (25 LOC) is a pure data wrap; `_interactive_review_branch` (33 LOC) only handles display+prompt. Previously they were interleaved in one method body.

### Followups

- The codebase analysis ranked `execute_analyze_run` (600 LOC, single function) as the next refactor target. Same technique applies — extract `RunBuilder` / `RunExecutor` from the procedural script.
- `_auto_apply_branch` at 77 LOC is the largest TableProcessor method; could be split further into `_persist_decisions` + `_writeback_to_db` if needed.

## [0.9.1] - 2026-05-01
### Changed — SearchCatalog god-class split into 6 mixin modules (S2 refactor)

After v0.9.0 trimmed `SearchAgent`, `SearchCatalog` was the next biggest god-class on the codebase analysis: 2033 LOC, 53 methods, fan-in 13 (everything that touches the catalog imports it). v0.9.1 applies the same mixin pattern: split by responsibility, keep the public API stable, let Python MRO compose the result.

**File layout (before → after):**

```
amx/search/catalog.py    2033 →  200   (-90.2%, -1833 LOC moved out)
amx/search/_catalog/
  __init__.py               -  →   28   (mixin re-exports)
  entity_crud.py            -  →  315   (6 methods: _entity_row, _upsert_entity, _insert_description, _index_entity, _resolve_effective_description, _update_search_text)
  sync.py                   -  →  534   (11 methods: sync_table_profile, _sync_table_profile_conn, sync_review_decision, sync_generated_suggestions, sync_code_report, sync_status, sources_status, start_sync_job, finish_sync_job, rebuild_profile, clear_code_evidence)
  search.py                 -  →  603   (17 methods: search_tables, search_columns, name_search_columns, find_*, _exact_candidates, _rank_rows, _description_tokens, _tokens, _similarity, _attach_column_counts, _dtype_family, schema_inventory, count_tables, known_databases, known_schemas)
  join.py                   -  →  385   (7 methods: joinable_tables, join_candidates, semantic_join_candidates, semantic_joinable_tables, _semantic_column_pair_score, _band_for_semantic_score, _extract_join_pairs)
  usage.py                  -  →  249   (6 methods: _store_query_usage, mark_applied, _mark_run_result_state, history_counts, record_manual_description, record_dedup_decision)
  settings.py               -  →   91   (3 methods: set_setting, get_settings, explain_table)
```

**Method distribution:**

```
SearchCatalog (core)       3 methods   __init__, from_history_store, _connect
EntityCrudMixin            6 methods
SyncMixin                 11 methods
SearchMixin               17 methods
JoinMixin                  7 methods
UsageMixin                 6 methods
SettingsMixin              3 methods
                          ─────────
Total                     53 methods   (matches original — no methods lost)
```

### Why this matters

`SearchCatalog` is the most-imported internal module after `config` (fan-in 13). Every read-path call site (`SearchAgent.ask()` retrieval, `/metadata edit`, `/run-apply` write-back, `/history` rendering) goes through it. Splitting by responsibility makes it possible to:

- **Unit-test the search path in isolation** — `SearchMixin` has 17 methods that produce row dicts; with a fixture connection they don't need real LLM/DB providers.
- **Add a new sync source without touching the search path.** Want to ingest descriptions from a Confluence dump? Edit only `sync.py`; everything else is unaffected.
- **Replace the underlying storage.** A future move from SQLite to Postgres would touch only `entity_crud.py` + the connection layer; the public methods on the other mixins keep their signatures.

### Followups

- `JoinMixin.joinable_tables` is still 90 LOC and `JoinMixin.join_candidates` is 87 LOC — these can be decomposed (symbolic vs semantic paths) in a follow-up.
- The codebase's #3 god-class is now `Orchestrator` (1719 LOC, 21 methods). `process_table` is still 281 LOC — that's the next refactor target on the original analysis (the agreed S3 step: `Orchestrator.process_table` → `TableProcessor`).

## [0.9.0] - 2026-05-01
### Changed — SearchAgent god-class split into mixin modules (S1 refactor)

The historical `amx/search/agent.py` carried a 3733-LOC `SearchAgent` class with **70 methods** spanning 6+ logical responsibilities (session memory, planning, target resolution, short-circuit handlers, retrieval, answer synthesis, deterministic answers). v0.9.0 splits those clusters into mixin modules under `amx/search/_agent/` so each file is a manageable size, each cluster is testable in isolation, and `SearchAgent` itself becomes a thin facade composed of the mixins.

**Public API unchanged.** `SearchAgent.ask()` is the only call site outside this package; it still works identically. All inheritance goes through Python's MRO; cross-mixin calls (`self._memory()`, `self._resolve_table_targets()`, etc.) resolve transparently because every mixin is composed into the final `SearchAgent`.

**File layout (before → after):**

```
amx/search/agent.py    3733 →  767   (-79.5%, -2966 LOC moved out)
amx/search/_agent/
  __init__.py             -   →   33   (mixin re-exports)
  answering.py            -   →  210   (4 methods: _synthesize_answer, _provenance, _confidence, _action_suggestions)
  deterministic.py        -   →  549   (6 methods: 6 _deterministic_* answer composers)
  planning.py             -   →  528   (10 methods: _plan_*, _interpret_*, _align_*, _policy_for_plan, _derive_answer_shape)
  resolution.py           -   →  597   (12 methods: _resolve_*, _explicit_*, _candidate_*, _catalog_resolvable_subject)
  retrieval.py            -   →  812   (19 methods: _retrieve, _live_*, _plan_live_probe, _execute_live_probe, row helpers)
  session_memory.py       -   →  201   (10 methods: _llm_*, _memory*, _ensure_session_*, _last_tables, _catalog_ready)
  short_circuits.py       -   →  338   (6 methods: _handle_chitchat, _handle_meta_query, _handle_followup_reaffirmation, _answer_via_tool_agent, etc.)
```

**Method distribution after refactor:**

```
SearchAgent (core)         3 methods   __init__, ask(), _scope_from_tables
AnsweringMixin             4 methods
DeterministicAnswersMixin  6 methods
PlanningMixin             10 methods
ResolutionMixin           12 methods
RetrievalMixin            19 methods
SessionMemoryMixin        10 methods
ShortCircuitsMixin         6 methods
                          ─────────
Total                     70 methods   (matches original — no methods lost)
```

### Why this matters

The 3733-LOC god-class was the #1 refactor pain point identified in the codebase analysis (24% of the entire 31K LOC codebase concentrated in 3 files; `agent.py` alone was 12%). With the split:

- **Each cluster is independently testable.** `DeterministicAnswersMixin` has zero LLM dependencies — its 6 methods can be unit-tested with synthetic plans + rows. Previously they were tangled in a class that required a full `LLMProvider` + `SearchCatalog` + `DatabaseConnector` to instantiate.
- **Method discovery becomes O(file)** instead of O(scroll-3700-lines). When extending the planning step (e.g. for the upcoming Phase 3 model-fallback work), the developer opens `planning.py` and sees the 10 relevant methods — not 70.
- **Cross-mixin calls are explicit.** Each mixin's docstring lists which sibling mixins it depends on (which `self.*` methods it expects). Previously implicit; now documented.
- **Future splits are cheaper.** The `RetrievalMixin` is still 812 LOC (one method, `_retrieve`, is 217 LOC); a follow-up release can split it further (live-probe pipeline → its own module) without disturbing the rest.

### Followups

- `SearchAgent.ask()` is still 428 LOC — next release will decompose it into a small dispatcher that delegates to the appropriate mixin path (chitchat short-circuit → `_handle_chitchat`, meta-query → `_handle_meta_query`, normal flow → `_plan_with_overrides → _retrieve → _synthesize_answer`).
- `SearchCatalog` (2033 LOC, 53 methods) is the next god-class on the refactor list per the codebase analysis. Same mixin-extraction technique applies.

## [0.8.7] - 2026-05-01
### Changed
- **Banner footer no longer duplicates the version** (`amx/utils/console.py:show_banner`): the v0.8.6 footer line was `v0.8.6  •  AI-inferred database descriptions`, but the version is also shown in the "AMX Interactive Session" info block right below the banner alongside Config / Database / LLM context. Two visible "0.8.6" stamps in adjacent panels were noise. The footer is now just the tagline `AI-inferred database descriptions`; version stays in the session info block where it groups naturally with the rest of the runtime state.

### Why this matters
Banner = identity (what is this tool); session info block = runtime state (what version + which profiles are active). Mixing the two produces redundancy and dilutes both.

## [0.8.6] - 2026-05-01
### Changed
- **Startup banner cleanup** (`amx/utils/console.py:show_banner`):
  - Dropped the redundant "Metadata Extraction System" subtitle — it was saying the same thing as "Agentic Metadata Extractor" two lines above. The new banner is single-source-of-truth: tagline at top, ASCII art in the middle, footer at the bottom.
  - Replaced the asterisk framing (`* AMX (Agentic Metadata Extractor) *`) with box-drawing brackets (`┃  Agentic Metadata Extractor  ┃`). The asterisks were rendered with the system font; the brackets sit in the same Unicode block as the rest of the ANSI Shadow art, so the framing now matches the grid aesthetic instead of mixing vector glyphs with grid art.
  - Added a footer line: `v0.8.6  •  AI-inferred database descriptions`. The version is auto-pulled from `amx.__version__` (lazy import keeps `utils.console` free of a hard top-level dependency); the tagline gives new users an immediate one-liner about what the tool does without scrolling.
  - Tier-style cyan hierarchy across the three tiers (`bold cyan`, `bold bright_cyan`, `cyan`) so the eye picks out the levels.

### Why this matters
The banner is the first thing users see when they `amx` in a fresh terminal. Open-source readability matters here — three discrete tiers (what / how it looks / which version) beats two redundant tiers + decorative noise. Box-drawing framing also keeps the UTF-8 art consistent: no more mixed-glyph-class look.

## [0.8.5] - 2026-05-01
### Added
- **`/run` Column scope** (`amx/services/analyze_scope.py`, `amx/agents/orchestrator.py`, `amx/cli_support/commands/analyze_flow.py`): user noticed there was no way to re-run AI inference on a single column. Added a 5th option to the analysis-scope picker — `Column` — that drills schema → table → column and restricts the run to just that one column. Useful when one comment came out wrong (the LLM picked the wrong meaning for `code`, say) and you want to regenerate it without re-profiling the whole table.
- **`ScopeResult` (subclass of dict)** in `amx/services/analyze_scope.py` carries an optional `column_overrides: dict[(schema, table), set[str]]`. Existing scope shapes (Database / Schema / Asset / Default) are unchanged plain dicts, so consumers that just iterate `scope.items()` keep working. Column scope returns `ScopeResult({schema: [table]}, column_overrides={(schema, table): {column}})`.
- **`Orchestrator.column_overrides`**: new attribute consulted in `process_table` before the missing-only / dedup filters. When a `(schema, table)` key is present, `profile.columns` is restricted to the override set; nothing else on the table gets re-inferred. The table-level comment is preserved as-is — Column scope is column-targeted by definition.
- **Equivalence pre-walk respects Column overrides** so the dedup pass doesn't accidentally walk every column of the table when the user only picked one.

### Why this matters
Before this release, the only way to fix one bad column comment was: `/metadata edit <db>.<schema>.<table>.<column>` and type the description manually, OR re-run `/run` on the whole table (paying for ALL columns to be re-profiled). The new Column scope gives the AI a single-column re-inference path without spending tokens on already-curated columns next to it.

## [0.8.4] - 2026-05-01
### Changed
- **Dedup question now precedes the scope picker** (`amx/cli_support/commands/analyze_flow.py`): user clarified — twice — that the `/edit` pattern means the binary mode-selector is the FIRST runtime question, before any drill-down. v0.8.2 already moved dedup ahead of coverage and review_strategy, but it still came AFTER the analysis-scope picker (Database / Schema / Asset / Default) AND the schema picker. v0.8.4 hoists `ask_choice("Equivalence-class deduplication?", ["dedup", "per-column"])` to right after `_resolve_completion_mode` and BEFORE the `with command_display(...)` block — so it fires before the scope picker fires. Profile-modification, LLM-test, and completion-mode prompts stay where they are because they're infrastructure questions, not run-mode questions.

### Why this matters
The whole point of the `/edit` pattern is that the user makes the high-impact yes/no decision before AMX walks any structure. With the previous ordering, users on a `/run` had to wade through 4–5 prompts (profile / completion mode / scope / schema picker) before they could opt out of dedup; if they wanted per-column profiling, they were forced through scope selection they didn't really care about for that particular run. Asking dedup right at the start lets the user lock in the run mode first, then drill down — exactly like `/metadata edit` Single-vs-Bulk.

## [0.8.3] - 2026-05-01
### Fixed
- **`UnboundLocalError: cannot access local variable 'review_strategy'`** when the user cancels at the schema picker (`amx/cli_support/commands/analyze_flow.py`). Pre-existing bug: the `KeyboardInterrupt` handler at line 933 reads `review_strategy` to decide whether to mark the run as `cancelled` vs `ready_for_review`, but the variable is only assigned inside the `with command_display(...)` block — which the user hasn't reached yet if they cancel during scope finalization. Now `review_strategy="individual"`, `use_dedup=False`, and `dedup_outcome=None` are pre-initialised at the function top alongside the other early-init defaults (`final_status`, `final_error_text`, etc.), so the cancellation paths can finalize history cleanly without a secondary crash. Reproducer: `/run` → Asset scope → Enter on the schema picker (no selection) → Ctrl+C / blank input.

### Why this matters
The crash hid the original cancellation reason behind a confusing Python traceback. Open-source users who reach for AMX would assume the tool itself is broken, when really they just hit Ctrl+C at the wrong moment. With pre-initialised defaults, the cancellation finalises with a clean log entry.

## [0.8.2] - 2026-05-01
### Changed
- **Dedup question is now the FIRST runtime question** (`amx/cli_support/commands/analyze_flow.py`): user feedback "ask in the same order as /edit". v0.8.1 still placed dedup AFTER coverage and review_strategy; the user wanted it as the very first question after the scope picker, mirroring `/metadata edit`'s rule that the binary mode-selector (Single vs Bulk) always comes first. Order is now: scope → dedup (dedup / per-column) → coverage (missing-only / all) → review_strategy (individual / deferred / auto-apply).
- **Equivalence analysis panel** mirrors `/metadata edit`'s "Bulk-update analysis for 'X'" header. After the user picks `dedup` and AMX walks the scope, the panel shows: a heading, the headline numbers (total members, total classes, multi-member count, singletons), the largest class by member count, the estimated LLM-call saving, AND a small table of the top 10 classes that will dedup (column, dtype family, member count, sample tables). So the user can sanity-check what's about to happen before any LLM call.

### Why this matters
The dedup choice is the highest-impact decision in a run (can change LLM cost by 90%+). It's now asked at the same level as the other run-mode pickers but with the same primacy as `/metadata edit`'s Single-vs-Bulk prompt — first runtime question, no analysis-before-decision. The new analysis panel gives the user a concrete preview of what will be deduplicated; that's especially important on schemas where they're about to spend hundreds of LLM calls.

## [0.8.1] - 2026-05-01
### Changed
- **Equivalence dedup question is asked UPFRONT** (`amx/cli_support/commands/analyze_flow.py`): user said "ask this first, like the /metadata edit pattern". Previously v0.8.0 walked the scope, computed classes, showed a summary, AND THEN asked Y/N — too much work happened before the user had a chance to opt out. Now the dedup choice is a regular `ask_choice("Equivalence-class deduplication?", ["dedup", "per-column"])` asked alongside coverage (missing-only / all) and review strategy (individual / deferred / auto-apply), BEFORE any scope walking. When the user picks `dedup`, AMX walks the scope and runs the pass; the post-walk summary still prints (so the user sees what was analyzed) but no longer asks for re-confirmation. When the user picks `per-column`, AMX skips the pre-walk entirely and goes straight to the legacy per-table flow.

### Why this matters
The previous flow violated AMX's UX rule that high-impact yes/no decisions are asked before any compute work. Users on huge SAP-style schemas (47+ tables, 500+ columns) had to wait through the full pre-walk just to see the dedup question — even if they intended to say no. The new ordering puts dedup at the same level as the other run-mode decisions, which is also where users expect it (mirrors `/metadata edit` Single-vs-Bulk first prompt).

## [0.8.0] - 2026-05-01
### Added — Equivalence-class deduplication for `/run` & `/run-apply` (Phase 2)

Wide schemas (think SAP) repeat the same column hundreds of times across
hundreds of tables — `mandt`, `client`, `created_at`, `customer_id` —
and the per-column LLM cost adds up. AMX now collapses these into
**equivalence classes** before the per-table ProfileAgent loop runs.
One LLM call per class, applied to every member, with all member
tables listed in the prompt as context.

- **`amx/agents/equivalence.py`** (new): pure data + logic for the
  feature. `dtype_family()` normalizes raw dtypes into coarse buckets
  (`varchar(50)` → `string`, `numeric(10,2)` → `numeric`,
  `timestamp without time zone` → `timestamp`, `text[]` →  the family of
  the element type). `ColumnMember` is a frozen dataclass for one
  scope-resident column. `EquivalenceClass` groups members by
  `(name.lower(), dtype_family)`. `compute_column_equivalence_classes()`
  buckets a list of members into classes; `summarize_classes()` returns
  the headline numbers (total members, total classes, multi-member
  count, largest class name + size, llm_call_savings_pct).
- **`amx/agents/equivalence_agent.py`** (new): the dedup LLM pass.
  `run_equivalence_pass(classes, llm, db, apply_to_db, run_id, …)`
  loops over multi-member classes, builds a prompt that lists every
  member table with its existing comment if any, and asks the LLM for
  ONE generalized description. The prompt allows the model to respond
  with `DIVERGES` to opt out of dedup for that class — those members
  fall back to per-table profiling. Successful classes are: written to
  the catalog via the new `SearchCatalog.record_dedup_decision()`,
  written to the live DB (when `apply_to_db=True`), and added to the
  outcome's `skip_set`.
- **`amx/cli_support/commands/analyze_flow.py`**:
  `_build_equivalence_members()` walks the in-scope tables, applies the
  same `missing_only` filter the orchestrator will use, and produces
  the `ColumnMember` list. `_maybe_run_equivalence_dedup()` shows a
  user-facing summary ("Found 145 columns → 12 classes; estimated
  92.0% fewer column-level prompts") and asks `Use equivalence-class
  deduplication for this run? (Y/n)`. On accept, runs the pass and
  returns a `DedupOutcome`. The outcome's `skip_set` is then attached
  to every `Orchestrator` instance so subsequent `process_table`
  calls filter the dedup'd columns out of the ProfileAgent batch.
- **`amx/agents/orchestrator.py`**: new `Orchestrator.dedup_skip_set`
  attribute. `process_table()` now filters `profile.columns` against
  this set right after the missing-only filter, with an info message
  reporting how many columns were skipped and why. When every column
  on a table is dedup'd AND the table-level comment already exists, the
  whole table is skipped.
- **End-of-run summary** prints the dedup recap: `Equivalence dedup: 12
  class(es) applied → 145 column(s) (~91.7% fewer column-level LLM
  calls).` Diverged and failed classes are reported separately so the
  user can see exactly where dedup didn't fire.
- **`amx/search/catalog.py`**: `record_dedup_decision()` persists each
  class member to the catalog with `source_kind='dedup'` and a
  `source_agent` string carrying the equivalence key + run id + member
  count, so `/history` reporting can later distinguish dedup-applied
  descriptions from per-column inferences.

### Why this matters

On a 47-table SAP scope where 145 columns share names like `mandt`,
`bukrs`, `belnr`, dedup collapses 145 column-level LLM calls into 12
class-level calls — roughly 92% saving on the per-column profiling
budget. The descriptions also become consistent across the schema (the
same column gets the same comment), which improves `/ask` answer
quality. The user keeps full control: the dedup pass is opt-in per
run, the LLM can DIVERGES out of any class where the meaning genuinely
differs across members, and post-dedup edits flow through `/metadata
edit` (including the v0.7.x bulk-edit picker) like any other comment.

### Followup

- `/history` rendering will gain a "Dedup classes" column in a follow-up
  release so historical runs surface the savings without needing
  end-of-run output.
- The dedup prompt currently sends up to 25 member tables before
  compressing to "+ N more"; we may need to dial that for very wide
  schemas.

## [0.7.3] - 2026-05-01
### Fixed
- **`NameError: name 'info' is not defined`** in the new bulk-edit wizard (`amx/cli_support/commands/manual.py`): `info` was used in the bulk-update analysis header + summary lines but never imported. Added it to the `from amx.utils.console import …` line. The first user-visible run of `/metadata edit` → `Bulk by name` after v0.7.2 crashed with this error before it even produced the match table. Regression caught immediately by the user.

### Changed
- **Bulk-edit picks the entity from the live DB instead of asking the user to type a name** (`amx/cli_support/commands/manual.py:_resolve_bulk_target_name`): user said "why am I typing the name? Let me PICK from a list, and let me drill down to column level — find similar to the asset I select." After choosing `Bulk by name`, the wizard now offers three sub-modes:
  - `Pick a column from the catalog` — drills DB profile → schema → table → column, then uses the picked column's NAME (not its full path) so AMX bulk-fans-out to every other column with the same name in the catalog. The default option, since column-level bulk edit is the most common bulk case (`mandt`, `client`, `created_at`, `customer_id` …).
  - `Pick a table from the catalog` — drills DB profile → schema → table, then uses the table NAME so AMX picks up the same table across every schema.
  - `Type a name manually` — preserves the legacy text-entry path for power users who already know exactly what they want.
- After the pick, AMX prints a confirmation line ("Using column name 'mandt' (from sap_s6p.bseg) as bulk target — AMX will find every other column that shares this name.") so the user understands which name is being fanned out.

### Why this matters
Bulk-edit only works when the entity name matches the user's intent. Forcing the user to remember and type the exact spelling is brittle (typos, case mismatches, wrong synonyms) and defeats the purpose of an interactive wizard. By letting the user pick a concrete asset and pulling its name programmatically, the wizard guarantees correct spelling AND lets the user explore the catalog naturally — they don't have to know the name in advance.

## [0.7.2] - 2026-04-30
### Changed
- **`/metadata edit` wizard now asks bulk-vs-individual at the FIRST step** (`amx/cli_support/commands/manual.py:_run_edit_wizard`): user said "I want the bulk option BEFORE 'What do you want to edit?'". The wizard now starts with a top-level choice: `Single entity` (existing database → schema → table → column flow) or `Bulk by name` (type a column or table name once, AMX handles every match across schemas). Single mode is unchanged; bulk mode reuses `_run_bulk_edit_by_name` with the new `preselected_mode="bulk"` argument so the user isn't asked the same question twice.
- **Bulk-update analysis header** before the match table: counts match types ("12 column(s) across 5 schema(s): sap_s6p, sap_test, …") and explicitly states "Whatever you select below will be updated TOGETHER with the same comment." So the user understands the impact of multi-select BEFORE picking rows. Per the user's request: "show me a simple analysis — these columns in these tables will all be updated."

## [0.7.1] - 2026-04-30
### Changed
- **`/metadata edit <name>` now asks bulk-vs-individual before locking the user into bulk** (`amx/cli_support/commands/manual.py`): v0.7.0 went straight to the multi-select picker after finding matches, but a user might want to handle each entity separately when entities just happen to share a name (e.g. `code` in `country.code` vs `currency.code`). New three-way prompt: `bulk` (one comment for selected rows — original 0.7.0 behavior), `individual` (walk through each match one at a time, type a different comment per row, Enter to skip), `cancel`. Single-match cases auto-switch to single-target edit and skip the prompt entirely.

### Added
- **`_run_individual_edits` flow** (`amx/cli_support/commands/manual.py`): per-row edit loop that prints each match's full path + dtype + existing comment, accepts a NEW comment (Enter to skip, `cancel` to stop the loop), writes via `apply_comment` with the right `AssetKind`, and re-syncs each result to the catalog via `record_manual_description`. Reports `applied / skipped / failed` counts at the end.

## [0.7.0] - 2026-04-30
### Added — `/metadata edit <name>` bulk-edit by bare name
- **`/metadata edit customer_id` (any bare token, no dots, no scope keyword)** now triggers a NEW bulk-edit flow (`amx/cli_support/commands/manual.py:_run_bulk_edit_by_name`) instead of falling into the wizard. AMX searches the catalog for every table whose name matches AND every column whose name matches across all schemas, prints a numbered table (kind / Schema.Table[.Column] / dtype / existing comment), then asks for a multi-select picker (`1,3,5 / 1-4 / all`). The user types ONE comment and AMX writes it via batched `COMMENT ON …` SQL to every selected entity, then re-syncs the catalog so `/ask` sees the new descriptions immediately.
- **`SearchCatalog.find_columns_by_exact_name`** (`amx/search/catalog.py`): mirror of `find_tables_by_exact_name` but for column-level lookups. Used by the bulk-edit flow + future deduplication features.

### Why this matters
Wide SAP-style schemas have repeated column names (`mandt`, `client`, `created_at`, `customer_id`) in dozens or hundreds of tables. Until now the user had to type `/metadata edit <db>.<schema>.<table>.<column>` once per occurrence — typically 50+ commands for a single concept. The bulk-by-name flow turns that into one command + one comment + one multi-select. Per the user's preference, the picker is user-curated (no auto-apply to all) so semantically-different tables sharing a column name aren't accidentally given the same description.

### Followup
A separate Phase-2 task ([#57]) plans equivalence-class deduplication BEFORE the LLM call inside `/run`: when ProfileAgent encounters identical (column_name, dtype, fk-pattern) tuples across a run scope, it would send ONE prompt and apply the resulting description to every member, saving tokens. Deferred to a follow-up release; this release ships only the manual-side bulk edit.

## [0.6.4] - 2026-04-30
### Changed
- **Tool-agent system prompt now demands relevance filtering and proper push-back handling** (`amx/search/tool_agent.py`): user reported asking "which tables have phone-number columns" and getting `addrnumber`, `consnumber`, `persnumber`, `roomnumber` (and `tel_number`/`fax_number`) — the raw `search_columns_by_concept` candidate set, not actually-phone-number columns. When pushed back ("I guess some are not correct"), the agent just thanked the user and repeated the same list. Two prompt-level fixes: (a) explicit "Result validation" rule telling the model that `search_*_by_concept` returns a candidate set with FALSE POSITIVES and that it MUST drop rows whose description doesn't fit before composing the final answer; (b) "Push-back handling" rule listing concrete actions the model should take when the user pushes back (re-call with refined query, drill into descriptions, or admit the limitation) — explicitly forbids "Thank you for your patience!" + same list.
- **Tool descriptions for `search_columns_by_concept` and `search_tables_by_concept`** now state inline that the result is a "CANDIDATE SET" and warn about false positives, so the model doesn't have to rediscover this each time. Includes a worked example for the phone-number case.

### Why this matters
Concept search is a fuzzy ranking, not a query language. If the LLM treats every returned row as "definitely matches the user's intent", the answers look authoritative but are wrong (the failure mode that prompted this fix). Two changes — one in tool description, one in system prompt — push the model toward an explicit filter step + a productive push-back response, both improving open-source UX without changing any retrieval logic.

## [0.6.3] - 2026-04-30
### Fixed
- **Auto-inference fallback placeholders no longer reach the live database** (`amx/agents/orchestrator.py`): user reported their DB had `Column rewrt in table bseg. Auto-inference missed a reliable description; please review manually.` written as the actual `COMMENT ON COLUMN` for several columns. The placeholder was meant as a UI hint for human review (`_ensure_complete_table_coverage` injects it when the LLM misses a column in its response), but it flowed through `apply_review_results_to_db` and got persisted as real metadata. New `is_placeholder_description` predicate + filter at the top of `apply_review_results_to_db` block these out before any SQL hits the DB. Existing rows produced by older `/run-apply` invocations stay polluted; use the new cleanup command (below) to remove them.
- **`missing-only` filter now treats placeholder comments as "still missing"** (`amx/agents/orchestrator.py`): legacy DBs polluted with placeholder strings are organically cleaned up — re-running `/run-apply` with the missing-only filter (the default) will detect the placeholder via `is_placeholder_description` and re-analyse those columns. Real metadata replaces fallback text.

### Added
- **`/db cleanup-placeholders [schema]` slash command** (`amx/cli_support/commands/db.py`): one-shot cleanup that scans every table and column comment in the active DB profile, NULLs out anything matching the auto-inference fallback string, and reports counts. Use this once when upgrading from pre-0.6.3 to scrub legacy pollution; v0.6.3+ never writes the placeholder in the first place. Wired through all four discovery paths (`db_cmd_heads`, dispatch handler, `/db` namespace help, autocomplete catalog).

### Why this matters for open source
A user that ran `/run-apply` with auto-apply on a flaky model could end up with thousands of columns whose `COMMENT ON COLUMN` reads "Auto-inference missed a reliable description; please review manually." That's not a hint — it's pollution that misleads anyone querying the DB metadata directly (BI tools, schema explorers, future AMX runs). Two fixes plus a cleanup tool ensure the placeholder never escapes AMX's review UI.

## [0.6.2] - 2026-04-30
### Fixed
- **`/ask "tables without description"` no longer surfaces system / extension assets** (`amx/search/agent_tools.py`): user reported `pg_stat_statements` and `pg_stat_statements_info` (PostgreSQL extension views) showing up as "tables without descriptions". These aren't user data — they're statistics views AMX never describes, and the `/run` flow has been filtering them out for releases via `services.analyze_scope.is_non_business_asset`. The `find_assets_missing_comment` agent tool now reuses the same filter so coverage queries don't surface these as gaps. New `include_system: bool` parameter (default false) lets the LLM opt back in only when the user explicitly asks about system tables (e.g. "tables including system views?"). Result payload now reports `system_assets_skipped` + count so the LLM can mention the filter in the answer.

## [0.6.1] - 2026-04-30
### Fixed
- **`/description-verbosity` now appears in `/llm` namespace help, autocomplete, and Tab-toggle catalog** (`amx/cli_support/session.py`): v0.6.0 added the slash command but only registered it in the dispatch handler and the `llm_cmd_heads` routing set — it was missing from the namespace help text (so `/help` inside `/llm` didn't show it) and from `_slash_command_catalog` (so the autocomplete dropdown didn't list it). Now it's wired through all four discovery paths: dispatch (`_handle_session_builtin`), routing (`llm_cmd_heads`), help (namespace help text), and autocomplete (`llm_cmds`).

### Why this matters
Open-source users who don't know a command exists won't ever run it. Slash-command discovery has historically been fragmented across four lists in this codebase; v0.6.1 ensures the new commands surface uniformly. Future additions need to update all four points; we should consolidate to a single source of truth in a follow-up but that's a larger refactor.

## [0.6.0] - 2026-04-30
### Added
- **`/llm description-verbosity` slash command** + `description_verbosity` LLM-profile field (`amx/config.py`, `amx/cli_support/commands/profiles.py`, `amx/cli_support/session.py`): two presets, `brief` (default — current 1-sentence-per-column behavior) and `detailed` (2–4 sentences covering purpose + typical values + relationships when supported by evidence). Wired into `ProfileAgent._build_system_prompt` so the model emits longer descriptions when asked. Detailed mode roughly doubles per-column output cost; the slash command warns about that.
- **`find_assets_missing_comment` agent tool** (`amx/search/agent_tools.py`): queries the LIVE DB (NOT the catalog) to list tables and/or columns with no comment. Routes via the system prompt for questions like "are there any tables without descriptions?" / "açıklaması olmayan tablolar". Catalog can lag right after `/run-apply`, so this tool is the source of truth for coverage questions.
- **`Orchestrator._ensure_run_columns` helper** (`amx/storage/sqlite_store.py`): idempotent migration that adds `selected_count / planned_count / processed_count / applied_count / review_strategy` to `analysis_runs`. Runs on every `init()` AND now also at the top of every `create_run` as a belt-and-suspenders for users whose `init()` ran on stale code under a pipx editable install.

### Fixed
- **auto-apply was still asking for review at the schema/database meta step** (`amx/agents/orchestrator.py`, `amx/cli_support/commands/analyze_flow.py`): `process_schema_meta` and `process_database_meta` produced `ReviewResult(applied=False)` regardless of the picked strategy, so `batch_review` brought the picker back. Both methods now accept `auto_apply: bool` and mark results applied accordingly. The end-of-run `confirm("Apply these metadata comments to the database?")` is also skipped in auto-apply mode (per-table writes already happened in `process_table`); only schema/database-level meta produced after the loop are written by a final `apply_results` call.
- **`/ask "tables without description"` returned stale data** — the catalog-search tools matched concept names, not actual coverage. Now the system prompt explicitly routes these questions to `find_assets_missing_comment`, which queries the live DB. Also: tools are renamed in the routing guidance so the LLM doesn't fall back to `search_*` when asked about coverage.

### Why 0.6.0 (MINOR bump)
- New CLI command (`/llm description-verbosity`) and new agent tool (`find_assets_missing_comment`) constitute a public-API addition.
- Behavior change for auto-apply: previously prompted at meta steps, now fully unattended. Existing scripts that relied on the post-loop confirm will need to know it's gone.

## [0.5.9] - 2026-04-30
### Fixed
- **Reasoning-style models that return empty content now abort the run with one clear message** (`amx/llm/provider.py`): user reported `openrouter/tencent/hy3-preview:free` exhausting all output tokens on internal "thinking" and returning `content=""` with `finish_reason=length` on every batch. Previous behavior raised the soft `LLMTruncationError` per batch, which the agents caught and recorded as a diagnostic, churning through the table list while the same failure repeated. Now: when `finish_reason=length` AND `content == ""`, we raise `FatalLLMError` instead — the run aborts after the first attempt with a friendly message naming non-reasoning paid alternatives (`openrouter/openai/gpt-4o-mini`, `openrouter/anthropic/claude-3-5-haiku`, `openrouter/google/gemini-1.5-flash`) and pointing at `AMX_LLM_MIN_MAX_TOKENS` / `AMX_REASONING_EFFORT=minimal` for users who insist on a reasoning model.

### Why this matters for open source
Free / preview tiers on OpenRouter often expose reasoning-style models (Tencent Hunyuan 3, DeepSeek-R1, QwQ, etc.) where every output token goes to internal chain-of-thought. AMX's structured-JSON prompts can't produce useful work in that mode regardless of how many times we retry. Aborting early with a model-recommendation message saves users hours of confused retries and burned API quota.

## [0.5.8] - 2026-04-30
### Fixed
- **auto-apply now writes each table's comments to the live DB IMMEDIATELY after that table finishes** (`amx/agents/orchestrator.py`): the previous flow marked results `applied=True` per-table but the actual `COMMENT ON ...` SQL ran in one batch at the END of the run. A user that completed bkpf and then Ctrl+C'd during bseg saw bkpf in catalog as 'applied' but its comment never reached the live DB. Now `process_table` calls `apply_review_results_to_db` for the table's results before returning, so partial completion = partial DB state (and the missing-only filter on the retry skips what's already there).
- **`Processed` column in `/history list` was stuck at `—` even when the run made progress** (`amx/cli_support/commands/analyze_flow.py`): the `update_run_planned_count` formula computed `max(0, total_assets - len(skipped_assets) - 1)` but `skipped_assets` only grows on `ProfilingError`, NOT on missing-only filter skips. So six filter skips in a row all set planned_count to the same value (77 instead of stepping 78 → 77 → 76 → ... → 72). Now we maintain a separate `filter_skipped_count` and recompute `planned_count = total_assets - filter_skipped_count` per filter-skip event.
- **Extracted `_record_applied_state` helper on Orchestrator** (`amx/agents/orchestrator.py`): the per-table auto-apply path and the end-of-run batch apply path now share the same history+catalog "applied" bookkeeping, so a partial auto-apply run shows up correctly in both `analysis_runs` and the search catalog.

## [0.5.7] - 2026-04-30
### Added
- **`FatalLLMError` class** (`amx/llm/provider.py`): non-recoverable LLM errors (auth / quota / payment / model-not-found) now raise this dedicated exception with a short, user-facing message. Caught at `analyze_flow.execute_analyze_run` so the entire run aborts cleanly with one actionable message instead of producing 200+ identical warnings while iterating through tables.
- **`_classify_fatal_llm_error` detector**: inspects the exception's `status_code` AND the lowercased error message body. Maps HTTP 401 / 402 / 403 / 404 + provider-specific message patterns ("more credits", "insufficient_quota", "invalid api key", "model not found", "can only afford") to a friendly user message ("Your account is out of credits — top up to continue."). Tested against the user-reported OpenRouter 402 ("This request requires more credits") output.

### Fixed
- **`/run` no longer continues blasting LLM calls when the account is out of credits** (`amx/llm/provider.py`, `amx/agents/profile_agent.py`, `amx/cli_support/commands/analyze_flow.py`): user reported their OpenRouter account hit 402 mid-run; AMX kept retrying every batch on every remaining table, accumulating 1090 seconds and 111K tokens of failed attempts before manual Ctrl+C. `LLMProvider.chat` now classifies fatal errors before retry, raising `FatalLLMError` immediately. `ProfileAgent.run` lets `FatalLLMError` propagate (and cancels sibling futures in the parallel-batch path so the executor drains fast). `execute_analyze_run` catches `FatalLLMError` and prints `LLM run aborted: <user_message>` plus a hint that the missing-only filter will skip already-finished tables on the retry — then exits with status `failed`.
- **Cancelled futures in parallel batch mode** (`amx/agents/profile_agent.py`): when one batch detects a fatal error, sibling futures get `cancel()`'d so the executor doesn't keep spending tokens on the rest of the wave.

### Rationale
The previous behavior was hostile to the user: a single recoverable typo in the API key, or an out-of-credits afternoon, produced thousands of lines of warnings, drained the rest of the table queue's effort, and left an `analysis_runs` row that looked like 'AMX did 87 things'. v0.5.7 turns these into "fix the LLM, retry — your missing-only filter has your back" exits.

## [0.5.6] - 2026-04-30
### Fixed
- **`amx` startup crashed with `AttributeError: function object has no attribute 'group'`** (`amx/cli.py`): the v0.5.5 patch inserted the new `_raise_open_file_limit` helper between the existing `@click.group(...) ... @click.pass_context` decorator stack and `def main(...)`, so the decorators ended up applied to the helper instead of `main`. `register_history_commands(main, ...)` then failed because `main` was a plain function, not a Click `Group`. Helper moved above the decorator stack so the decorators land on `main()` again.
- **LLM calls could hang indefinitely** (`amx/llm/provider.py`): user reported a single 25-column profile batch sitting at 9m58s while sibling 50-column batches finished in 1–1.5 min. No per-request timeout was being passed to LiteLLM, so a stalled upstream connection (OpenRouter/qwen mid-stream stall in this case) waited forever. Added a default `180s` per-call `timeout` (tunable via `AMX_LLM_TIMEOUT_SEC` env var). On expiry LiteLLM raises `Timeout` / `APITimeoutError`, both of which `_is_transient_llm_error` already classifies as retry-able — so the existing retry-with-backoff (`MAX_LLM_RETRIES=2`) automatically starts a fresh request instead of silently waiting.

## [0.5.5] - 2026-04-30
### Added
- **Programmatic NOFILE limit raise at AMX startup** (`amx/cli.py:_raise_open_file_limit`): lifts the per-process soft NOFILE limit to 4096 (capped at the hard limit) via `resource.setrlimit`. Open-source users no longer need to set `ulimit -n` manually before running `amx` on macOS (default soft limit 256). Cross-platform safe — no-op on Windows where the `resource` module isn't available, no-op when the user's hard cap is already lower, never reduces the limit.

### Fixed
- **`SearchService` was leaking a SQLAlchemy engine + connection pool per `_inventory_db()` call** (`amx/search/service.py`): the old code returned `DatabaseConnector(self.cfg.db)` on every call, and the legacy planner calls it many times per question. Cache one connector per `SearchService` instance and dispose it via the new `close()` method. `SearchService` is now a context manager; every `/search ask` callsite wraps its `svc` in `with svc:` so the engine is disposed when the question finishes — preventing the FD-exhaustion crash users saw after several REPL turns.
- **Same fix applied to find-columns / join-candidates / explain / explain-table hidden commands** so every entry path through `_service(cfg)` releases its connector.

### Changed
- The combination of these two fixes (programmatic ulimit raise + per-question connector disposal) means the `OSError: [Errno 24] Too many open files` from 0.5.3 should not surface on any open-source user's machine, even with default-256-FD systems and long REPL sessions.

## [0.5.4] - 2026-04-30
### Fixed
- **`OSError: [Errno 24] Too many open files` after several `/ask` turns** (`amx/search/agent_tools.py`, `amx/search/tool_agent.py`): each tool-agent question instantiated a fresh `ToolBox` → fresh `DatabaseConnector` → fresh SQLAlchemy engine + connection pool, but never disposed it. After enough turns the file-descriptor count crossed the macOS / Linux ulimit and the next `prompt_toolkit.prompt` failed inside `asyncio.new_event_loop()` because no FDs were left for selectors. `ToolBox` now exposes `close()` and acts as a context manager; `run_tool_agent` wraps the loop in `with ToolBox(...) as toolbox:` so the connector is disposed at the end of every question. The session memory between turns continues to live in `ChatSessionStore`, not on the `ToolBox`, so dropping the connector mid-session is safe.

### Changed
- **`analysis_runs` migration probes the live schema before adding columns** (`amx/storage/sqlite_store.py`): the previous `try: ALTER except: pass` swallowed every error including unrelated ones. Now we read `PRAGMA table_info(analysis_runs)` first to get the actual column set, skip already-present columns idempotently, log every successful column add at INFO, and log only true failures at WARNING. Helps diagnose why `/history` shows `—` for `Processed` when the migration didn't apply.

## [0.5.3] - 2026-04-30
### Fixed
- **OpenRouter models with non-OpenAI vendor namespaces no longer fail with `LLM Provider NOT provided`** (`amx/llm/provider.py`): the user reported `provider=openrouter, model=qwen/qwen3.5-flash-02-23` failing because LiteLLM saw the `qwen/` head and didn't recognise it as a routable provider. Root cause: `LLMProvider.model_name` had an early-return `if "/" in raw: return raw` that bypassed the `openrouter/` prefix for any model id containing a slash, and `PROVIDER_MODEL_PREFIX["openrouter"]` was set to an empty string. OpenAI-prefixed models (`openai/gpt-4o-mini`) happened to work via LiteLLM's OpenAI client + api_base override, but vendor namespaces (qwen/, mistralai/, meta-llama/, google/, x-ai/, ...) had no fallback. Fix: `PROVIDER_MODEL_PREFIX["openrouter"] = "openrouter/"` is now always applied; `model_name` skips the prefix only when `raw` already begins with it. Net effect: every OpenRouter model id reaches LiteLLM as `openrouter/<vendor>/<model>`, the canonical form OpenRouter expects.

## [0.5.2] - 2026-04-30
### Added
- **Progress counters on `analysis_runs`** (`amx/storage/sqlite_store.py`): four new columns recorded per run — `selected_count` (assets the user originally picked), `planned_count` (post missing-only-filter target), `processed_count` (assets that actually started processing — survives Ctrl+C), `applied_count` (results successfully written to live DB). Plus a `review_strategy` column so the status logic can distinguish auto-apply from individual / deferred. Idempotent ALTER TABLE migrations let existing DBs pick up the new columns transparently.
- **Counter-update helpers** (`amx/storage/sqlite_store.py`): `update_run_planned_count`, `increment_run_processed`, `increment_run_applied`. Each commits per-row so partial progress survives Ctrl+C even when `finish_run` is never reached.
- **`Processed` column in `/history list`** (`amx/cli_support/commands/history.py`): rendered as `processed/planned`, with an `applied N` annotation when the apply count diverges from the processed count (e.g. user accepted only some during interactive review). Falls back to `—` for older rows that pre-date the new columns.

### Changed
- **`Target Scope` no longer lies about partial runs** (`amx/cli_support/commands/analyze_flow.py`, `amx/cli_support/commands/history.py`): `Target Scope` keeps the original user intent (e.g. `sap_s6p (78 tables)`); the new `Processed` column shows what actually happened (`3/60` after the missing-only filter dropped 18 already-commented tables and Ctrl+C interrupted at table 3 of the remaining 60). Solves the user-reported confusion where a one-table-completed run still displayed `78 tables` as if all of them had been touched.
- **auto-apply runs that get Ctrl+C'd land in `cancelled`, not `ready_for_review`** (`amx/cli_support/commands/analyze_flow.py`): the user explicitly opted out of review when picking auto-apply, so directing them to "go review" contradicts that choice. Successfully-processed tables are already in the catalog (and on `/run-apply`, in the live DB), and the count is preserved on `applied_count`. The terminal status is `cancelled`, with the reporting columns telling the rest of the story.
- **`create_run` accepts `selected_count`, `planned_count`, `review_strategy`** so the caller can record initial intent. Defaults derive `selected_count` from the scope dict if not passed (backward compatible).

### Rationale
A user starting a run on the remaining 60 tables out of a 78-table schema, then hitting Ctrl+C after 3 tables completed, used to see in `/history`: `Target Scope: 78 tables, Status: ready_for_review` — wrong on both axes (78 implies all of them, ready_for_review contradicts auto-apply). The new columns answer "what did the user ask for?" and "what did AMX actually finish?" separately, so partial state is honest.

## [0.5.1] - 2026-04-30
### Added
- **Third review strategy: `auto-apply`** (`amx/cli_support/commands/analyze_flow.py`, `amx/agents/orchestrator.py`): the review-strategy picker now offers `individual / deferred / auto-apply`. With `auto-apply`, the orchestrator accepts each entity's top LLM suggestion as the final description, marks it `applied=True`, records it as `evaluation=accepted` in the run history, and writes it through `sync_review_decision` to the catalog as a reviewed description — all without prompting the user. When combined with `/run-apply`, the comments land in the live DB at the end of the run; with plain `/run` the catalog is updated but the DB write is deferred (a warning explains this).
- **Safety warnings** for the auto-apply path:
  * If selected with plain `/run` (no `--apply`), AMX warns that nothing will be written to the database.
  * If selected with `/run-apply`, AMX warns that existing comments inside the chosen scope will be replaced.
- New `auto_apply: bool` argument on `Orchestrator.process_table` so the chat-mode caller can pin the strategy per-table without affecting the batch-mode path (the batch picker doesn't expose the review-strategy choice).

### Rationale
Some users — especially the ones running AMX on large legacy SAP DBs where every column needs a description — would rather trust the agents and inspect afterwards via `/ask` than gate on a per-asset confirmation prompt. The new option keeps the interactive flow intact for everyone else (default stays `individual`) while removing the friction for power-users who explicitly opt in.

## [0.5.0] - 2026-04-30
### Added — Coverage filter for `/run` and `/run-apply`
- **Missing-only / all coverage filter** (`amx/cli_support/commands/analyze_flow.py`, `amx/agents/orchestrator.py`): after the user picks a scope (Database / Schema / Asset / Default), AMX now asks `Run for which assets / columns? — missing-only / all`, defaulting to `missing-only`. The user-reported pain was that `/run` always re-processed every asset in the chosen scope even when 90% of them already had comments — wasteful on hundreds-of-tables databases. With `missing-only`:
  * **Tables that already have a table-level comment AND every column has a comment are skipped entirely.** A single info line tells the user which assets were skipped: `Skipping sap_s6p.adrc: already has a table comment and all 24 column(s) commented (missing-only filter).`
  * **Tables with partial coverage have their column list narrowed to the gaps.** Profile / RAG / Code agents only see the missing columns: `Filtering sap_s6p.bseg: 350/356 columns already have comments — analyzing only the 6 missing one(s).`
  * **Tables where every column has a comment but the TABLE comment is missing** drop the column list to `[]` so the agents focus on the table description only.
  * Also wired through batch mode (`process_tables_batch_mode`) — fully-commented tables are dropped from the request batch entirely; partial coverage narrows the column list before agent prompt assembly.
- The user can still pick `all` to overwrite; the prompt's `descriptions=` block makes the trade-off explicit (`existing comments will be replaced after review`).

### Rationale
This was the user-reported case where a 100-table SAP database had been partially curated — they didn't want to re-pay for LLM tokens on the already-commented 95 tables, but the previous CLI gave them all-or-nothing with no asset-level cherry-picking. The new filter applies at column granularity, which matters for the wide tables that appear after a schema migration (10 columns → 12 columns → only the 2 new ones need analysis).

## [0.4.4] - 2026-04-30
### Fixed
- **Keystrokes are now visible during interactive prompts inside `/run`, `/setup`, `/search sync`, etc.** (`amx/utils/console.py`): when a `LiveDisplay` was active (header bar showing `AMX v0.4.x ... ANALYZE-SETUP 10s`), Rich's 10 Hz refresh painted over the user's keystrokes between frames. Pressing `2` then Enter still worked — the input was read correctly — but the user never saw their `2` echoed. New `_live_paused_for_input()` context manager pauses the live region while `prompt_toolkit.prompt` is reading stdin, then resumes it after. Wired into every interactive helper: `ask`, `ask_password`, `ask_choice`, `ask_multi_choice`, `confirm`. No-op when no display is active so non-interactive callers don't pay any cost.

## [0.4.3] - 2026-04-30
### Changed
- **Live display no longer leaves stacked header bars** (`amx/utils/live_display.py`): the running `AMX v0.4.x  openrouter/openai/gpt-4o-mini │ SEARCH  Xs` panel is now `transient=True`, so the entire live region (header + thinking spinner + active pipeline tree) clears when `stop()` runs. Previously every height change in the renderable left a frame behind in the scroll buffer, producing 2–4 stacked "SEARCH 2s / 3s / 9s" bars per question. To preserve the pipeline tree as a useful summary, `LiveDisplay.stop()` now re-prints a quiet single-block `Pipeline` tree with check-marked steps and durations once the live region clears.
- **Resume path also uses `transient=True`** (`amx/utils/live_display.py:resume`) for symmetry — when an outer command pauses and resumes the display (e.g. nested `command_display` blocks), the resumed live region is no less clean than the original.

## [0.4.2] - 2026-04-30
### Fixed
- **`/ask` follow-ups no longer lose memory between turns** (`amx/config.py`, `amx/search/agent.py`, `amx/cli_support/session.py`): the interactive REPL dispatches each `/ask <q>` line through `main_command.main(args=...)`, which re-runs Click's `main()` and rebuilds `ctx.obj = AMXConfig.load(cfg_path)` from disk. `active_chat_session_id` is intentionally ephemeral (not in `_PERSISTED_FIELDS`), so every question opened a brand-new chat session — `_handle_meta_query` then read an empty store and answered "this is the first question in this session" even when the user had asked several. We now bridge the id through an `AMX_CHAT_SESSION_ID` environment variable: `_run_ask_repl` writes it on entry (or clears it for a fresh REPL), `SearchAgent._ensure_session_id` mirrors it whenever a session is created, and `AMXConfig.load` reads it back at the top of every load. Net result: all turns inside one `/ask` REPL session land in the same `chat_sessions` row, so memory survives across follow-ups.

## [0.4.1] - 2026-04-30
### Added
- **`find_columns_by_dtype` tool** (`amx/search/agent_tools.py`): returns columns whose dtype matches a SQL type token (`boolean`, `int`, `date`, `timestamp`, `text`, ...). Supports dtype FAMILIES — `boolean` covers BOOL/BOOLEAN, `int` covers BIGINT/INTEGER/SMALLINT, `date` covers DATE/TIMESTAMP/TIMESTAMPTZ. Rolled up to a per-table view so the LLM sees `"sap_s6p.cskt has 1 boolean column: is_deleted"`. Fixes the user-reported case where `"which tables have boolean columns?"` only surfaced 2 tables via fuzzy semantic search instead of all dtype-BOOLEAN columns.
- **`find_joinable_tables` tool** (`amx/search/agent_tools.py`): given ONE table, returns the tables it can be joined with (verified FKs first, then semantic candidates). Different from the existing `get_join_candidates` which requires both sides upfront. Resolves bare table names through `find_tables_by_exact_name` first, surfacing ambiguity when the name lives in multiple schemas. Fixes `"which tables can I join with adr6?"` previously bottoming out at `public.adr6` (which doesn't exist).
- **Cross-DB profile awareness in the system prompt** (`amx/search/tool_agent.py:_agent_system_prompt`): the prompt now lists every connected DB profile with backend + database name + an "(active)" marker, plus an explicit note that tools target the active profile only and the user must `/use-db <name>` to switch. This is what was missing for "we have multiple DBs" questions.

### Changed
- **Memory pairing fix for short-circuits** (`amx/search/agent.py`): chitchat / meta-query / reaffirmation handlers now write a synthetic assistant row to `ChatSessionStore` before returning. Previously `ask()` wrote the user-side row at the top of the call but the short-circuits skipped writing an assistant row, leaving orphan user entries that confused the next `_memory_summary` pass.
- **Tool agent skips the duplicated current-question user turn** (`amx/search/agent.py:_answer_via_tool_agent`): `ask()` writes the current user question to the session store before short-circuits run, so by the time we read `_memory_summary()` the latest entry IS the question we're about to ask the LLM. Forwarding it as both prior context AND the live user message duplicated the question and broke follow-up resolution (`"Only those?"` came back as "your question is incomplete or unclear"). We now drop the trailing entry whose `question` matches the current one and has no paired assistant answer yet.
- **Memory summary bumps assistant truncation to 1000 chars** (`amx/search/agent.py:_memory_summary`): the previous 200-char cap was tuned for the JSON planner payload and cut off long answers (e.g. the boolean-column response). The tool agent feeds these straight into a chat history; 1000 chars stays comfortably under the 24K input budget while keeping enough context for follow-ups to resolve.
- **System prompt routing guidance covers dtype + joinable-tables tools and emphasises follow-ups** (`amx/search/tool_agent.py`): added explicit hints for `find_columns_by_dtype`, `find_joinable_tables`, plus stronger language reminding the model to read prior turns BEFORE calling a new tool when the question is a short follow-up (`"Only those?"`, `"sadece bunlar mı?"`, `"gerçekten?"`).

### Fixed
- **`"Only those?"` no longer returns "your question is incomplete or unclear"** — duplicated user-message bug + over-aggressive truncation of the prior answer summary kept the agent from resolving the follow-up. With the dedup fix and the 1000-char summary budget, the model now sees the prior boolean-column answer in context and can answer in one round.
- **`"which tables have boolean columns?"` now reaches every dtype-BOOLEAN column** instead of only the two whose names happened to score on fuzzy lexical search. The new `find_columns_by_dtype` tool queries `catalog_entities` directly with a dtype-family LIKE filter.
- **`"which tables can I join with adr6?"` no longer falls back to `public.adr6` (a non-existent table)** — the new `find_joinable_tables` tool resolves bare names via `find_tables_by_exact_name` and surfaces multi-schema ambiguity instead of silently picking the wrong one.

### Tests
- `test_tool_agent_drops_duplicated_current_user_turn_from_memory` — asserts a follow-up is resolvable against prior context.
- `test_short_circuits_persist_assistant_turn` — asserts chitchat writes a paired assistant turn so memory roles read as `[user, assistant]`, not `[user]`.

## [0.4.0] - 2026-04-30
### Added — Tool-calling `/ask` agent (architectural change)
- **New `amx/search/tool_agent.py`** runs `/ask` as a tool-calling loop instead of the regex-routed Pass1/alignment/retrieval cascade. The LLM receives a fixed set of metadata tools (`list_schemas`, `list_tables_in_schema`, `find_table_by_name`, `describe_table`, `search_tables_by_concept`, `search_columns_by_concept`, `get_join_candidates`, `list_databases`) and decides itself which to call. Bounded at 6 iterations per question; final-answer compose forced when the budget runs out.
- **New `amx/search/agent_tools.py`** — `ToolBox` class wraps the existing `SearchCatalog` / `DatabaseConnector` / `SchemaExplorer` with eight JSON-schema-described tools. Each tool returns structured rows + an optional `next-step hint` so the model can plan follow-up calls. Errors surface verbatim to the model (`{"error": ...}`) rather than crashing the loop.
- **`LLMProvider.chat` now extracts `tool_calls`** from the LiteLLM response (`amx/llm/provider.py`). New `ToolCall` dataclass; `ChatResult` gains a `tool_calls: list[ToolCall] | None` field. Backward compatible — callers that don't pass `tools=` see no change.
- **`SearchCatalog.find_tables_by_exact_name`** (already shipped in 0.3.3) is now consumed by the new `find_table_by_name` tool; live DB iteration falls back when the catalog is empty.
- **New catalog setting `use_tool_agent`** (default `"true"`). Test setup pins it to `"false"` so the legacy Pass1 tests still drive the regex-routed pipeline through queued planner JSON.
- **`SearchAgent._answer_via_tool_agent`** wraps the loop and persists the assistant turn to `ChatSessionStore` so follow-ups still resolve. Returns a `SearchAnswer` with `intent="tool_agent"`, `provenance=["tool_calling_agent"]`, and a `tool_calls` audit log under `details`.
- **Two new tests** in `tests/test_search_catalog.py`: `test_tool_agent_routes_tables_under_schema_to_list_tables` (asserts "What's the tables under sap_test" hits `list_tables_in_schema(schema='sap_test')` and surfaces `bseg`/`bkpf`/`vbak_test`); `test_tool_agent_falls_back_to_plain_answer_without_tool_calls` (covers the plain-text path). `_FakeLLMProvider.queue_tool_calls()` helper added for tool-aware fixture queuing.

### Changed
- **`SearchAgent.ask` dispatches to the tool agent first** when `use_tool_agent` is true (the default). The legacy regex-routed Pass1 pipeline stays as the fallback when the tool path raises or the setting is `false`. Deterministic short-circuits (chitchat, meta-query, reaffirmation) still run before either path so they continue to cost zero LLM calls.
- **System prompt for the agent** ships the live database name, schema list, pinned schema/table, and language preference directly. The model no longer has to guess whether `sap_test` is a schema; it can read the schema list from the prompt and route accordingly. This is what fixes the user-reported "What's the tables under sap_test" → "table named `under` not found" regression — and it's the architectural shift the rest of the routing fixes were patching around.

### Rationale
The 0.3.x line accumulated a lot of regex-based routing patches (strong-vs-weak mentions, alignment guards, stopword expansions). Each new phrasing required a new pattern; over-matches like "tables under sap_test" → "under" treated as a table name kept slipping through. This release moves routing back into the LLM but, unlike the original prompt-only design, gives the model real catalog/live-DB tools so it doesn't have to hallucinate. The legacy planner is retained as a fallback to de-risk the rollout — `use_tool_agent=false` reverts to the prior pipeline at any time.

## [0.3.5] - 2026-04-30
### Added
- **Strong-vs-weak explicit-mention strength** (`amx/search/agent.py:_explicit_table_mentions_for_question`): mentions captured from `<token> table` / `table <token>` / `schema.table` patterns are tagged `strength="strong"` (the user explicitly called the noun a table); subject-form patterns (`what's the X` / `describe X` / `X nedir`) are tagged `strength="weak"` (the noun could be a column or a generic entity). The alignment guard now reads this signal to override LLM mode unconditionally for strong mentions and require catalog/live-DB confirmation for weak ones.
- **Live-DB fallback in `_catalog_resolvable_subject`** (`amx/search/agent.py`): when the catalog has no entry but the user is in a `current_schema`, we now also probe `_live_table_exists(current_schema, token)`. This handles the user-reported case where vbrk lives in live PostgreSQL but hasn't been `/search sync`'d into the catalog yet.
- **Follow-up reaffirmation short-circuit** (`amx/search/agent.py:_handle_followup_reaffirmation`): brief push-back questions ("Are you sure?", "really?", "is that right?", "why?", "emin misin?", "gerçekten mi?", "neden?", "öyle mi?") no longer fall into clarification — instead, the most recent assistant turn from `ChatSessionStore.recent_turns` is restated verbatim with a confirmation prefix. Bilingual reply, deterministic, zero LLM calls.
- **Two new tests** in `tests/test_search_catalog.py`: `test_strong_table_mention_wins_when_catalog_is_empty` (asserts "which schema have vbrk table" routes to `table_explain` even with an empty catalog, as long as live DB has it under `current_schema`); `test_followup_reaffirmation_restates_prior_assistant_turn` (asserts "Are you sure?" reuses the prior assistant turn without consuming a new LLM response).

### Changed
- **`_align_plan_shape` overrides for strong mentions even when catalog is empty** (`amx/search/agent.py`): previously the override required an exact-name catalog match. Strong mentions (`X table` / `table X`) now bypass that requirement — when the user explicitly calls the noun a "table", we trust the route and let `_resolve_table_targets` surface "not found" cleanly if both catalog and live DB come up empty.

### Fixed
- **`which schema have vbrk table` no longer drifts to a generic "couldn't find" answer** (`amx/search/agent.py`): the prior version required vbrk to be in the catalog before the alignment guard would override the LLM's inventory mode. Users running against a live DB that hadn't been `/search sync`'d got the wrong answer. With strength-tagged mentions, the explicit `<token> table` form is now respected unconditionally and target resolution falls back to live-DB existence check.
- **`Are you sure?` no longer triggers clarification** (`amx/search/agent.py`): brief reaffirmation questions used to fall into the LLM planner with no scope and end up in `should_clarify`. The new short-circuit reuses the prior assistant turn.

## [0.3.4] - 2026-04-30
### Added
- **Catalog-grounded alignment guard** (`amx/search/agent.py:_catalog_resolvable_subject`): pre-check whether an extracted subject token from the question is actually an exact-name table in the catalog before forcing `table_explain`. This narrows the override to high-confidence cases — `vbrk` (real table) gets re-routed; `vbrk_id` (column-shaped, no table match) does not. Used by both `_align_plan_shape` and the clarification-skip guard.
- **Chitchat short-circuit** (`amx/search/agent.py:_handle_chitchat`): one-line friendly redirect for greetings ("nasılsın", "hi", "hello", "merhaba", "selam", "naber", "iyi misin", "thanks", "teşekkürler", "ok", "günaydın"). Bilingual reply explains what AMX does and gives an example question. Pre-empts the LLM planner so users no longer get a confusing "Could you clarify the exact scope (database/schema/table)?" reply for "nasılsın".
- **Meta-query short-circuit** (`amx/search/agent.py:_handle_meta_query`): "what was my previous question?", "bir önceki sorum neydi", "ben ne sordum", "son sorum neydi" now answer directly from `ChatSessionStore.recent_turns` rather than going through the LLM planner. Returns the literal prior user-turn text, or "this is the first question in this session" when there's nothing on record.
- **Two new tests** in `tests/test_search_catalog.py`: `test_chitchat_short_circuits_without_llm_call` (asserts no LLM call is made for "nasılsın" / "hi"), `test_meta_query_returns_prior_question_from_session_store` (asserts "what was my previous question?" returns the actual prior question text).

### Changed
- **`_align_plan_shape` is more aggressive about overriding the LLM's mode** (`amx/search/agent.py`): previously the guard skipped any plan whose `search_mode` was in `{table_explain, join_candidates, joinable_tables, schema_inventory, list_databases, list_schemas, count_tables, check_coverage}` — far too permissive. The user reported `"vbrk tablosu var mı bizde"` getting routed to `list_databases` and answered with "we have info about: SAP". Now the protected set is narrowed to `{table_explain, join_candidates, joinable_tables, count_tables, check_coverage}`. Inventory-style modes (`list_databases`, `list_schemas`, `schema_inventory`) and generic semantic/lookup modes (`semantic_concept`, `name_lookup`, `compare_entities`) get re-routed to `table_explain` whenever the question carries a catalog-confirmed subject token. The override also pins `decision_confidence="high"` so the rerouted plan does not loop back into clarification.
- **Clarification round is skipped when the user named a real table** (`amx/search/agent.py`): when `should_clarify` is True but `_catalog_resolvable_subject(clean_question)` finds an exact-name catalog match, we re-run `_align_plan_shape` with the new high-confidence plan and proceed with retrieval — instead of asking "could you clarify the exact scope?" right after the user named a table by exact name.

### Fixed
- **`/ask` no longer drifts to "we have info about: SAP" when asked about a real table** (`amx/search/agent.py`): the user-reported sequence — "vbrk tablosu var mı bizde" → list_databases response, "Peki hangi schema'da?" → "Could you specify which database?", "OK. which schema have vbrk table?" → "I couldn't find any direct evidence" — was caused by the LLM planner picking inventory modes for clearly table-scoped questions. With the catalog-grounded alignment guard and tightened protected-mode set, all four phrasings now route to `table_explain` for `vbrk` and return the live metadata.
- **Greetings no longer return the clarification fallback** (`amx/search/agent.py`): "nasılsın" used to land in `should_clarify` with empty `clarification_question` and emit "Dogru yonlendirme icin tam kapsami...". Now the chitchat short-circuit replies with a friendly redirect that names the kind of question AMX is built for.
- **Meta-queries about the chat itself now resolve from session storage** (`amx/search/agent.py`): "bir önceki sorum neydi" used to also land in clarification. Now we read `ChatSessionStore.recent_turns` and return the literal prior question.

## [0.3.3] - 2026-04-30
### Added
- **Bare `/ask` enters a sticky `ask>` REPL** (`amx/cli_support/session.py`): typing `/ask` with no question now opens a question-only inner prompt that re-uses the same persistent chat session. Each line is dispatched as `/search ask <line>` so follow-ups still link to the same `chat_sessions` row. Exits on `/exit`, `/quit`, `/back`, an empty Ctrl-D, or Ctrl-C. Slash commands other than the exit verbs are rejected inside the REPL with a hint, so users don't accidentally jump out of the conversation by typing `/db` mid-thread.
- **`SearchCatalog.find_tables_by_exact_name`** (`amx/search/catalog.py`): returns every catalog table whose `table_name` matches the given identifier exactly, ordered by `schema_name`. Used by the new unqualified-name resolver to disambiguate a bare token like `vbrk` across schemas.

### Changed
- **`/ask` answers no longer drift to an unrelated table when the user names a missing one** (`amx/search/agent.py`): four new regex branches in `_explicit_table_mentions_for_question` catch the table name in subject-form questions ("what's the X / what is X / describe X / explain X / tell me about X / X nedir / X hakkında / anlat bana X / açıkla X"). Without these, a question like "what's the vbrk" left the planner with no entity hints, and the live probe drifted to whatever `current_table` or fuzzy match the LLM happened to suggest (the user-reported case where asking about `vbrk` returned columns of an unrelated `setheadert` table). Stopword list expanded with `table/tables/tablo/tablosu` and Turkish case forms (`tablosunda/tablosuna/tablosundan/tablosunu/tabloları`), `column/columns/kolon/kolonlar`, `field/fields/alan`, `data/info/information/metadata/veri/bilgi`, `schema/schemas/şema/şemalar/database/databases/veritabanı`, plus generic adjectives (`most/least/popular/common/total/average/newest/oldest/recent`).
- **`_align_plan_shape` re-routes subject-form questions to `table_explain` mode** (`amx/search/agent.py`): when a question carries an explicit subject-form mention but the LLM picked `semantic_concept` / `name_lookup`, the alignment pass now forces `search_mode=table_explain, question_class=table_understanding, target_entity=table` so target resolution actually runs. Without this guard the unresolved-explicit path didn't fire and the live probe could pick an unrelated table. Skipped when the question contains join keywords (`join/link/relate/relationship/bağ/ilişk`).
- **`_resolve_table_targets` handles unqualified mentions via catalog lookup** (`amx/search/agent.py`): mentions without a `schema.table` qualifier (path == "") used to be silently skipped, falling through to LLM-hint resolution. Now they round-trip through `catalog.find_tables_by_exact_name`: exactly one match → resolved with high/medium confidence; two-or-more matches → marked `ambiguous_unqualified_table` with all candidates surfaced; zero matches → `explicit_table_not_found_live` with fuzzy `find_table_candidates` results offered as suggestions only.
- **`_target_resolution_details` reports an `ambiguous_unqualified` flag** (`amx/search/agent.py`) so downstream code can distinguish "this name maps to multiple tables in different schemas" from "this name doesn't exist at all" and surface different deterministic answers for each case.
- **`_deterministic_target_resolution_answer` rewrites both messages** (`amx/search/agent.py`): "not found" answers now read as plain English/Turkish ("I could not find a table named `vbrk` in this DB profile's catalog or live metadata") with a `/search sync` hint when no fuzzy candidates exist; ambiguous names get a dedicated message ("`orders` exists as a table in more than one schema. Could you clarify which one you mean? Candidates: `sap_a.orders`, `sap_b.orders`."). Suppressed the older "exact olarak doğrulayamadım" jargon that confused users.
- **`/ask` output is uncluttered by default** (`amx/cli_support/commands/search.py`, `amx/search/catalog.py`): the `Provenance:` and `Confidence:` info lines now require either `--debug` or an explicit opt-in via `/search config show_provenance true` / `show_confidence true`. The two settings flipped default from `"true"` to `"false"`. The natural-language summary plus the result panel are all most users want; the diagnostic strings drowned out the answer.
- **`table_summary` rendering drops the empty header row and lifts schema.table into the panel title** (`amx/cli_support/commands/search.py`): the previous render pulled the `row_type="table"` row to the top of a 5-column grid, leaving a "—" line in the Column cell and duplicating Schema/Table on every subsequent row. New layout shows `Key columns — schema.table` as the title and a 3-column Column | Type | Description grid below it, with rows whose `column_name` is empty filtered out so users never see "-" placeholders.

### Fixed
- **`what's the <X>` no longer returns an arbitrary table** (`amx/search/agent.py`, `amx/search/catalog.py`, `amx/cli_support/commands/search.py`): the user reported that `/ask "what's the vbrk"` returned columns of `sap_s6p.setheadert` (an unrelated table) with high confidence and an "agent-planned live metadata probe" provenance trail. With the new subject-form regex + alignment guard + unqualified-name resolver, the same question now either resolves to the real `vbrk` (when present), surfaces all schemas containing a `vbrk` table (when ambiguous), or replies "I could not find a table named `vbrk` in this DB profile's catalog or live metadata" with fuzzy suggestions — never silently swapping in another table.

### Tests
- **`tests/test_search_catalog.py`** gains three subject-form coverage tests: `test_subject_form_unknown_table_returns_not_found` (asks "what's the vbrk" against a catalog without vbrk and asserts the answer says "could not find" with no live probe operations), `test_subject_form_existing_table_resolves_unqualified` (resolves "describe adrc" with no `current_schema` to `sap_s6p.adrc`), and `test_find_tables_by_exact_name_disambiguates_across_schemas` (same name in two schemas returns both candidates). Existing `test_explicit_missing_table_is_not_replaced_by_fuzzy_candidate` updated to match the new Turkish wording (`bulunamadı` instead of `exact olarak doğrulayamadım`).

## [0.3.2.post1] - prior unreleased work below
### Added
- **`/ask` is now a stateful conversational agent with persistent sessions** (`amx/search/session_store.py`, `amx/cli_support/commands/chat_session.py`, `amx/storage/sqlite_store.py`): conversation history survives `/exit`+restart by writing each Q/A turn to two new SQLite tables (`chat_sessions`, `chat_turns`) inside the existing `~/.amx/history.db`. Each `/ask` invocation appends a `user` then `assistant` row with the question, answer summary, intent, topic, matched tables/columns, plan, tokens, and request_id; soft-deletion (`compacted_at`) preserves the audit trail when old turns are summarised. Sessions are scoped to the active `(db_profile, llm_profile)` pair so cross-profile contamination is prevented at the read path. The legacy module-level `_SESSION_MEMORY` dict is retained as a thin shim whose `.clear()` wipes the new SQLite tables, so existing tests that called it for isolation continue to pass.
- **`/session` namespace** (`amx/cli_support/commands/chat_session.py`) for managing the new persistent conversations: `/session new [--title]` starts a fresh chat session and pins it as active; `/session list [-n N] [--all-profiles] [--include-ended]` shows recent sessions for the active profile pair (or all of them) with started/last-active timestamps, turn counts, titles, and a first-question excerpt; `/session resume <id>` switches the active session pointer (refuses cross-profile resume); `/session end` closes the current session; `/session show [--id N] [--include-compacted]` dumps the per-turn audit trail for inspection. New REPL boots always start with no active session — fresh by default — and the user opts back in via `/session resume`.
- **LLM-driven context compaction** (`amx/search/session_store.py:maybe_compact`) keeps the conversation context inside the model's input budget: when live turns exceed `0.40 × model_input_budget` (24K for OpenAI/DeepSeek/local; 60K for Claude; 100K for Gemini), the oldest slice that doesn't fit a `0.70 × threshold` retained tail is sent to the active LLM with a system prompt asking for "one short paragraph + bullet lines for tables/columns/intents". The synthetic summary turn (`role='summary'`) replaces the soft-deleted slice and is surfaced first in `recent_turns()` so follow-up planners still see entity context from compacted history. If the LLM call fails or no provider is configured, the algorithm falls back to a stub `(history truncated, N earlier turns dropped)` summary so degraded mode still works. A concurrency guard re-reads `compaction_state_json` inside the replace transaction and aborts when another process already advanced past the same `through_turn_id`.
- **`active_chat_session_id` ephemeral field on `AMXConfig`** (`amx/config.py`): tracks which chat session the current REPL is appending to. Excluded from `_PERSISTED_FIELDS` and the `save()` data dict so the id never lands in `~/.amx/config.yml` (it would be meaningless on another machine where the SQLite DB differs). Defaults to `None` on every load — fresh REPL = fresh session.
- **`amx/search/confidence.py` band helper** with `band(score) -> "High"|"Medium"|"Low"` and `band_style(b)`. Thresholds (12.0, 6.0) are tuned to the existing scoring formula in `amx/search/catalog.py`: an exact column-name hit alone (`+12.0`, line 1308) lands in High; prefix/contains (+9.0/+7.0) plus weights lands in Medium; vector-only hits and weak keyword smear stay in Low. Stable across the 100+ scores the user reported (e.g. 165.00 → High) without needing a fourth tier.
- **`--debug` / `--verbose` flag on `/ask`** (`amx/cli_support/commands/search.py:search_ask`): when set, the rendered output includes the planner's Thought Trace and three extra ranked-list columns on the right (`Score`, `Source`, `Conf`). Without the flag (the new default), end users get a clean Schema.Table | Match | Why | Rows | Cols | Description grid plus the natural-language summary — no internal pipeline noise.
- **`Why` column** on the default ranked-list table populated from each row's existing `matched_columns` list (already computed in `catalog.py:1796` but never displayed before): users now see "matched on supplier_id, vendor_name" rather than guessing why a table was returned.
- **`Cols` column** populated by a new `SearchCatalog._attach_column_counts` helper (`amx/search/catalog.py`) that runs one batched `SELECT schema_name, table_name, COUNT(*) FROM catalog_entities WHERE entity_kind='column' AND (schema_name, table_name) IN (...) GROUP BY ...` per `/ask` to enrich `search_tables` rows. `Rows` continues to read from `catalog_entities.row_count`.
- **15 new tests** under `tests/test_chat_sessions.py` covering: persistence across simulated restart (open store → write → drop → reopen → assert), profile-pair filtering on `list_sessions`, first-question excerpts, compaction summarises with one LLM call, no-op under threshold, fallback to stub when LLM is unavailable or raises, summary turn carries prior tables forward (so follow-ups like "what about its columns?" still ground), concurrency guard skips when state already advanced, summary placement on top after `recent_turns(limit=N)`, and confidence band thresholds (12.0 → High, 11.99 → Medium, 5.99 → Low, 165.0 → High, None → Low).
- **3 new render tests** in `tests/test_cli_integration.py`: default mode hides Score/Source/Conf, debug mode appends them and bands 165.0 to High, the `Why` column joins `matched_columns` into a comma-separated list.

### Changed
- **`SearchAgent` no longer keeps memory in a module-level dict** (`amx/search/agent.py`): `_memory()`/`_remember()`/`_memory_summary()` now read/write the `ChatSessionStore`. `ask()` resolves the session id at the top of the call (lazy `start_session` if `cfg.active_chat_session_id` is None), appends the user turn, runs `maybe_compact`, then on completion writes the assistant turn with `answer_summary`, `confidence`, plan snapshot, and merged token usage. `_memory_summary()` now includes `answer_summary` (≤200 chars) so the interpretation prompt can resolve "the first one" / "that table" against the prior recap, not just the table list.
- **`_synthesize_answer` system prompt** (`amx/search/agent.py:_synthesize_answer`) now instructs the LLM that for `ranked_list` answers the headline sentence should name the 1-3 best-matching tables and weave in WHY each matched, citing specific `matched_columns` from the rows. The field already reached the prompt; it just wasn't being used.
- **Default `_render_search_rows` ranked-list table** (`amx/cli_support/commands/search.py`) replaces the old Schema | Table | Column | Source | Conf | Score | Description layout with Schema.Table | Match | Why | Rows | Cols | Description. Numeric scores moved behind `--debug`. Other shapes (`joinable_table`, `join_candidates`, `schema_explorer_table`, `table_summary`, `single_fact`) are untouched.
- **`/search /ask` now picks an answer shape per question instead of always dumping the full inventory** (`amx/search/agent.py`, `amx/cli_support/commands/search.py`): superlative and top-K questions over `schema_inventory` (e.g. "which table has the most rows in `sap_s6p`", "ilk 5 tablo", "top 5 tables by row count") now produce a one-sentence headline (`single_fact`) or a 2-5 row markdown table (`short_table`) instead of dumping all 80+ tables. Same retrieval, smarter presentation. Closed set of seven shapes — `single_fact`, `short_table`, `full_table`, `ranked_list`, `table_summary`, `join_candidates`, `prose` — chosen by the existing interpretation LLM pass and respected end-to-end by deterministic formatters, the synthesis prompt, and the renderer. No new LLM calls; the shape rides on the existing `_interpret_question_pass1` JSON.
- **`SearchPlan` carries `aggregation_op`, `aggregation_field`, `aggregation_limit`, `answer_shape`** (`amx/search/agent.py`): emitted by the interpret/review prompts in any language (Turkish "en fazla / en az / en cok", English "most/least/top N/biggest/smallest", etc.). Defaults to `""`/`0` so older payloads stay valid; `_derive_answer_shape` picks a shape from `question_class` + `search_mode` when the LLM didn't.
- **New `_deterministic_aggregate_inventory_answer`** (`amx/search/agent.py`) runs first inside `_deterministic_inventory_answer`'s `schema_inventory` branch. Sorts the SchemaExplorer rows by `row_count` or `column_count`, returns one bilingual headline for `single_fact` (Turkish thousand-separator-aware: "10.772.134" vs English "10,772,134") or a 2-5 row markdown table for `short_table`. Returns `None` when the aggregation hint is unusable so the legacy 50-row dump remains the broad-inventory fallback.
- **`_synthesize_answer` prompt leads with one direct sentence** (`amx/search/agent.py:1821-1893`) and dispatches on `answer_shape`. The user payload now carries an explicit `answer_shape` field so the model can render to the right shape rather than always producing prose. Existing grounding rules ("answer only from evidence") are preserved.
- **`_render_search_rows` dispatches on `row_type` and `answer_shape`** (`amx/cli_support/commands/search.py`): inventory rows render as a dedicated `Inventory` Rich table (Schema | Table | Columns | Rows | Cluster) instead of the generic Search matches grid; `table_summary` shapes render a focused Key-columns table; the default Search matches table now filters out rows whose score is exactly `0.00` so inventory leakage and zero-score diagnostics never surface as noise. The bottom Rich table is suppressed entirely for `single_fact`, `short_table`, `full_table`, and `prose` shapes — their summary already carries the data inline.

### Fixed
- **Newly-created DB and LLM profiles no longer come back blank after `/exit` and restart** (`amx/config.py`, `amx/cli_support/commands/db.py`): `set_active_db_profile` and `set_active_llm_profile` had an autosave race — assigning `self.active_*_profile = name` triggered an intermediate `save()` that mirrored the still-stale `self.<thing>` into `<thing>_profiles[name]`, wiping the just-added profile's data with empty defaults. The LLM half was the most user-visible: `/add-llm-profile work` with provider=openai/model=gpt-4o-mini produced an empty `llm_profiles['work']` after restart, and AMX fell back to whatever profile still had data (the user-reported "ghost test profile from a previous session"). Both `set_active_*_profile` helpers now wrap their two assignments in `cfg.transaction()` so save runs once at the end with consistent state. `cmd_add_profile` in `db.py` was refactored to use the safe `cfg.upsert_db_profile + cfg.set_active_db_profile` path inside a transaction instead of the inline 4-mutation race that previously masked the bug only because `cfg.db = db` happened to correct the dict on the third autosave.
- **`load()` no longer auto-synthesizes a phantom `default` profile from the dataclass mirror** (`amx/config.py`): an existing config file with no `db_profiles` / `llm_profiles` section used to silently get `db_profiles["default"] = cfg.db` (and the symmetric LLM equivalent) populated from the empty mirror, leaving an `active = "default"` pointer that masqueraded as configured. Now empty profiles stay empty and `cfg.active_*_profile` stays `""`, so the CLI startup summary correctly shows `"(not configured — run /setup or /add-*-profile)"`.
- **`_explicit_table_mentions_for_question` no longer treats English question-words as table names** (`amx/search/agent.py`): the regex `\b(\w+)\s+(?:table|tablo|...)` was matching "which" in "which table has the most rows" and "has", "with", "in", "of", etc. after `table`/`tables`, then routing the query to look up a literal `sap.which` or `sap.has` and bailing with `The live metadata check could not run: sap.has`. Stopword list expanded to cover English question/quantifier words (`which`, `this`, `that`, `each`, `every`, `any`, `all`, `some`, `no`, `many`, `much`), superlatives (`biggest`, `largest`, `smallest`, `top`, `bottom`, `first`, `last`, `primary`, `main`, `the`), and post-`table` connector verbs/prepositions (`has`, `have`, `with`, `in`, `of`, `for`, `by`, `from`, `to`, `into`, `and`, `or`, `but`, `contains`, `shows`, `named`, `called`).

### Added
- **`AMXConfig.config_path` property + startup display** (`amx/config.py`, `amx/cli.py`): the interactive session now prints `Config: /Users/<you>/.amx/config.yml` right after the version banner so the actual on-disk path is visible at all times. Diagnosing "my settings aren't persisting" used to require tracing through `Path.home()` / `CONFIG_DIR` / `_config_path` to find which file the running session was actually reading. Now it's a single line at startup. The new `cfg.config_path` property is the public API the CLI and tests should use instead of touching `cfg._config_path` directly.
- **3 new `ProfilePersistenceRaceTests`** (`tests/test_regressions.py`) covering the user-reported scenario end-to-end: (1) creating a DB profile via `cmd_add_profile`, dropping cfg, reloading from disk, asserting host/user/database survive; (2) same flow for LLM profiles via `cmd_add_llm_profile`, asserting `provider`/`model` survive — this was the failing case that reproduced the user's report (got `''` instead of `'openai'`); (3) the worst-case combined scenario where an existing pre-seeded Databricks profile is on disk, a new DB profile and a new LLM profile are added on top, and BOTH must survive restart with their fields populated. All three fail on `origin/main` without the fix and pass with it.
- **6 new tests** covering the answer-shape rework (`tests/test_search_catalog.py`, `tests/test_cli_integration.py`): single_fact aggregate inventory names the top table and suppresses the dump; top-K aggregate produces a 2-row short table; broad inventory still dumps but with `display_rows=False`; synth payload carries `answer_shape`; renderer drops zero-score rows from Search matches; renderer dispatches the dedicated Inventory Rich table for `schema_explorer_table` rows.

## [0.3.2] — 2026-04-30
### Fixed
- **Profile add+activate transactions are now truly single-flush** (`amx/config.py`, `amx/cli_support/commands/profiles.py`): `_autosave()` now honors `cfg.transaction()` suspension, so `upsert_db_profile()` / `upsert_llm_profile()` cannot write an intermediate YAML snapshot while a profile is still being added and activated. LLM profile creation now wraps upsert+optional activation in the same transaction pattern as DB profile creation. This hardens the profile persistence fix for the reported `/exit` + restart case where stale active mirrors could replace newly-created profiles.
- **Regression coverage for suspended upsert autosaves** (`tests/test_regressions.py`): added a test proving profile upsert + activation performs no save inside the transaction and writes exactly once on exit with the new active profile intact.

## [0.3.1] — 2026-04-29
### Fixed
- **Profile creation no longer pre-fills with the active profile's secrets** (`amx/cli_support/commands/db.py`, `amx/cli_support/commands/profiles.py`): typing `/add-db-profile newname` (or `/add-llm-profile`) for a name that does not exist used to call `interactive_db_block(cfg.db)` (or `interactive_llm_block(cfg.llm)`) — passing the **active** profile as the form's defaults. The "Enter to keep" hints would silently inherit values from a different profile, including the host, password, API key, Databricks PAT, and base URL. If the active profile was Databricks and a user typed `/add-db-profile new-postgres`, pressing Enter would fill the new postgres profile with the Databricks workspace URL.
- **Cross-backend reset inside the form**: even when editing an existing profile, switching the backend mid-flow (e.g. PostgreSQL → Databricks) now drops every default. Previously, the postgres host could leak into the Databricks form's `host` field as an Enter-to-keep value.
- **Empty form for new profiles**: brand-new DB and LLM profiles now start every prompt blank rather than inheriting the dataclass placeholders (`host="localhost"`, `user="amx"`, `database="SAP"`, etc.). Example values are surfaced in the prompt label (e.g. `"Database host (e.g. db.example.com)"`) instead of being pre-filled.
- **Better example placeholders** in `/add-db-profile` prompt labels: postgres, snowflake, databricks, and bigquery prompts now include `(e.g. ...)` hints with obviously-fake examples like `adb-xxxxxxxxxxxxxxxx.0.azuredatabricks.net`, `xy12345.us-east-1`, `my-company-prod`, `/sql/1.0/warehouses/abc1234567890` rather than relying on dataclass defaults.

### Added
- **4 new `ProfileCreationLeakageTests`** covering: new DB profile gets blank defaults (active Databricks values do not leak), editing an existing DB profile passes existing values through, cross-backend reset clears the host default when switching backends, new LLM profile gets blank defaults (active OpenAI key does not leak).

## [0.3.0] — 2026-04-29
### Added
- **Crash reports with secret redaction** (`amx/utils/crash.py`): when the top-level CLI handler catches an unhandled exception (and `AMX_DEBUG` is not set), AMX now writes a sanitized crash report to `~/.amx/logs/crashes/<timestamp>-<request_id>.txt` and prints the path so the user can attach it to a GitHub issue without leaking their DB password or API key. The report contains the timestamp, request id, exception class + message, full traceback, AMX-prefixed env vars, and any caller-supplied extra context — every component runs through `redact_secrets` before being written. Files are `chmod 0o600` on POSIX. The crash-report writer is itself wrapped in a try/except so a redaction failure cannot crash the crash handler.
- **`redact_secrets(text)` helper** matches and replaces: provider-prefixed API keys (`sk-`, `sk-or-`, `sk-ant-`), Databricks PATs (`dapi…`), AWS access keys (`AKIA…`), GitHub PATs (`ghp_…`, `github_pat_…`), `Authorization: Bearer …` headers, and `password=` / `api_key=` / `access_token=` / `token=` / `secret=` k/v pairs. Best-effort by design — the helper is documented as not exhaustive and users are advised to skim the report before sharing.
- **Env-var scoping**: the crash report's environment section dumps only `AMX_*` (plus `PATH` / `PYTHONPATH`) so arbitrary CI / IDE secrets in the surrounding shell never land in the file.
- **9 new `CrashReportSanitizationTests`** covering: OpenAI / Anthropic / OpenRouter / Databricks / Bearer-token / KV-pair redaction, file path format + content, `0o600` perms on POSIX, and env-var scoping (`AMX_TEST_FOO` retained, unrelated `MY_SUPER_SECRET_TOKEN_ZZZ` not).

- **Engine-bound DB adapter tests** (`tests/test_regressions.py`): follow-up to PR #12, which covered the SQL builders. This batch covers the methods that actually drive a SQLAlchemy connection. Adds a small `_FakeRow` + `_fake_engine` helper so tests can mock the `with engine.connect() as conn: conn.execute(...).fetchall()` shape without standing up a real database.
  - **PostgreSQL** (4 tests): `list_materialized_views` returns `relname` strings and binds `:schema`; `get_incoming_foreign_keys` normalises rows into dicts; `get_database_comment` and `get_schema_comment` return string or `None`.
  - **Snowflake** (1 test): `list_materialized_views` reads the second column / `name` mapping from a `SHOW MATERIALIZED VIEWS` row.
  - **Databricks** (2 tests): `get_table_stats` parses `numRows` from `DESCRIBE DETAIL`; failures absorbed and zero stats returned so the profile run keeps going.
  - **BigQuery** (2 tests): `get_table_stats` reads `INFORMATION_SCHEMA.TABLES.row_count`; missing row → zeroes.
- **Token-budget pre-check before LLM synthesis** (`amx/search/agent.py`): `_synthesize_answer` now estimates the prompt token count with `tiktoken` and, if it exceeds the LLM's input budget, drops the lowest-scored retrieval rows until it fits. Without this guard, large catalogs blew the model context window with an opaque LLM error; now the user sees a one-line warning in `~/.amx/logs/amx.log` describing how many rows were trimmed. Per-family budgets: 60K for OpenAI gpt-4o / DeepSeek / local; 150K for Claude (200K context); 250K for Gemini (1M-2M context). Unknown models fall back to 60K.
- **`_input_token_budget_for(model)` and `_trim_rows_to_token_budget(...)`** helpers, both pure-functional and reusable. The trimmer is O(n) — it estimates per-row cost from one full encoding plus one no-rows encoding, rather than re-encoding inside a loop.
- **4 new `TokenBudgetPreCheckTests`** covering: per-model-family budget mapping, all-rows-fit no-trim path, tight-budget keeps highest-scored rows in descending order, empty-input round-trip.

### Deprecated
- **`amx.core.ask_agent.LoopBasedAskAgent` and `AMXApplication.ask_with_tools()`** are deprecated as of 0.3.0 and will be removed in 0.4.0. The canonical `/search ask` path is `SearchService` → `SearchAgent`, which performs full multi-stage interpretation, retrieval, live probes, verification, and synthesis. Constructing `LoopBasedAskAgent` now emits a once-per-process `DeprecationWarning` pointing to `AMXApplication.ask()`. Library users should migrate before the 0.4.0 release. The `amx/search/service.py` module docstring and the `LoopBasedAskAgent` class docstring document the routing decision.
- **2 new `AskPathDeprecationTests`** covering: deprecation warning is emitted with the right message and version, and the canonical `SearchService` is still wired to `SearchAgent`.

### Added
- **Retrieval evaluation harness** (`tests/eval/`): a small kit for measuring search retrieval quality and comparing embedding providers. Pure-functional metric implementations for `hit@k`, `reciprocal_rank`, `mean_reciprocal_rank`, `precision@k`, and `ndcg@k` in `tests/eval/metrics.py`. End-to-end smoke harness in `tests/eval/test_smoke.py` exercises a fake retriever so contributors can copy the shape into a real eval script. `tests/eval/README.md` documents how to add fixture files and compare MiniLM vs OpenAI vs SentenceTransformers side-by-side.
- **20 new tests** under `tests/eval/`: 16 metric unit tests covering edge cases (empty input, no relevant items, invalid k, multiple relevant items, normalisation against ideal ranking) and 4 smoke tests that confirm the harness loop works end-to-end without a live catalog.

### Changed
- **Per-provider distance threshold for vector-only retrieval hits** (`amx/search/catalog.py`): the previous code hardcoded a `2.5` floor on `match_score` (= `3.0 - cosine_distance`) for every embedding model. That worked for MiniLM but is conservative for the OpenAI v3 embeddings family and for stronger sentence-transformers models like `BAAI/bge-large-en-v1.5`, where relevant matches sit at tighter distances. The threshold is now per-provider (`minilm` 2.50, `openai_compatible` 2.60, `sentence_transformers` 2.55) and overridable via the new `vector_score_floor` search setting (set with `/search /config vector_score_floor 2.7`).
- **6 new `VectorScoreFloorTests`** covering the MiniLM legacy default, the tighter OpenAI / sentence-transformers floors, the explicit override path (valid float), invalid override falls back to the provider default, and the unknown-provider fallback.

### Added
- **Request-id threading on `/analyze run` and `/search ask`** (`amx/cli_support/commands/analyze_flow.py`, `amx/cli_support/commands/search.py`): each invocation now sets a fresh 12-character request id at entry and clears it at exit. Every log line emitted while the command runs — from `SearchAgent`, `SearchCatalog`, `LLMProvider`, `DatabaseConnector`, all of them — carries the same `request_id` field in the JSON log, so users can extract a single run from `~/.amx/logs/amx.log` with `jq 'select(.request_id == "abc123def456")'`. The id is logged at command start so it is easy to find. Cleared in `finally` blocks so it never leaks across runs even if the command raises.
- **2 new `RequestIdWiringTests`** covering: id is set during `_run_search_ask` execution and cleared after, and the id is cleared even when the inner body raises.
- **Structured JSON logging** (`amx/utils/logging.py`): the on-disk log at `~/.amx/logs/amx.log` now writes one JSON object per line with fields `ts`, `level`, `logger`, `request_id`, `message`, and `exc_info` (when applicable). Log shippers, `jq`, and grep-by-field workflows work without a custom AMX parser. The stderr handler keeps the historical human-readable format so users see the same WARN / ERROR lines they always have.
- **`set_request_id()` / `clear_request_id()` / `get_request_id()`** helpers backed by a `contextvars.ContextVar` so callers can thread a per-request id through all log lines emitted between entry and exit of a CLI command. Will be wired into `/analyze run` and `/search ask` in a follow-up PR; this PR ships only the plumbing.
- **`_RequestIdFilter`** injects the active request id into every log record's `request_id` attribute under the JSON formatter. Records emitted while no id is set show `"-"` so log readers do not need to handle missing fields.
- **5 new `StructuredLoggingTests`** covering: set/get/clear round-trip, JSON formatter emits valid objects, exc_info ends up in a single string field, filter injects default when unset, filter picks up the active id.
- **DB connection transient retry** (`amx/db/connector.py`): `test_connection_result()` now retries once with backoff on transient failures (DNS glitches, connection reset, connection refused, timeouts, 502/503/504, broken pipe). Auth, permission, missing-database, and SSL-trust errors are classified as **non-transient** and propagate immediately so the categorised actionable message from `ErrorMapper` reaches the user without an artificial delay. Mirrors the LLM transient-retry pattern shipped in PR #9.
- **`_is_transient_db_connection_error()` helper**: pattern matches against both the exception class (`TimeoutError`, `ConnectionError`) and the message text. The non-transient pattern list includes `password authentication failed`, `permission denied`, `401 unauthorized`, `403 forbidden`, `invalid token`, `does not exist`, `unknown database`, `certificate_verify_failed`, `self-signed certificate` — anything an admin needs to fix before retrying.
- **6 new `DatabaseConnectionRetryTests`** covering: transient-then-success, persistent DNS failure exhausts retries with categorised message, auth-failure does not retry, permission-denied does not retry, certificate-verify-failed does not retry, and the `_is_transient_db_connection_error` truth table.

### Fixed
- **`/embeddings` from the root tab now auto-shifts into `/search`**, matching the UX every other namespace-scoped command already had (e.g. `/add-db-profile` → "Assumed /db namespace for this command."). Previously typing `/embeddings` from `[ ROOT ]` printed `✗ /embeddings belongs in /search.` and forced the user to manually `/search` first; now it just works.
- **`/embedding` (singular) is accepted as an alias for `/embeddings`** so the typo `/embedding` no longer hits "Unknown command."

### Added
- **`/usage [window]` slash command** (`amx/cli_support/commands/usage.py`): top-level read-only summary of LLM token usage and approximate cost over a time window (`24h` / `7d` / `30d` / `all`, default `7d`). Reads from `~/.amx/history.db` — local-only, no network calls. Aggregates runs by `(provider, model)`, prints input / output / total tokens and an approximate USD cost using a built-in price table covering OpenAI (gpt-4o family, o1, o3-mini, embeddings), Anthropic (Claude 4 Opus / Sonnet / Haiku, 3.5 Sonnet / Haiku, 3 Opus), Gemini (2.0 / 1.5), and DeepSeek. Models without pricing show an em-dash. Provider-prefixed model ids (`openai/gpt-4o`, `openrouter/openai/gpt-4o-mini`) and dated suffixes (`-20250514`, `-v2`) are matched against the table by stripping the prefix / suffix.
- **11 new `UsageCommandTests`** covering: window normalisation (default + known + unknown fallback), pricing lookup (exact match, provider-prefix stripping, dated-suffix stripping, unknown returns `None`), aggregation grouping, malformed-`tokens_json` skip, cost formatting (known model dollar, unknown em-dash, sub-cent), empty-history warning, and the no-store-initialised path.
- **`/inspect [profile]` slash command** under `/db` (`amx/cli_support/commands/db.py`): self-service connector diagnosis. Shows backend, non-secret connection fields per backend (postgres host/port/user/db, snowflake account/warehouse/role, databricks host/http_path/catalog/TLS, bigquery project/dataset/credentials), tests the connection through the existing typed `ConnectionTestResult` so the categorised error hints from the `ErrorMapper` flow through unchanged, lists visible schemas with table counts, and surfaces partial-enumeration failures (e.g. one schema's `list_tables` fails with permission denied) without aborting the rest of the diagnostic. Read-only — never mutates config or runs profiling.
- **5 new `DBInspectCommandTests`** covering: no active profile, unknown-profile name, connection failure with categorised hint, successful schema/table enumeration, and partial-failure resilience.
- **Unit tests for all four DB adapters** (`tests/test_regressions.py`): the audit flagged that none of `PostgreSQLAdapter`, `SnowflakeAdapter`, `DatabricksAdapter`, `BigQueryAdapter` had any unit tests. This PR adds 29 pure-functional tests covering the SQL builders, identifier quoting, fully-qualified-name composition, `system_schemas()` exclusions, and `actionable_profile_error()` categorisation per backend:
  - **PostgreSQL** (12 tests): `pg_stat_statements`, permission denied, undefined-table mappings; null-filter / distinct-count / min-max stats SQL; sample SQL with `:lim` parameter; `COMMENT ON TABLE/COLUMN/SCHEMA/DATABASE` SQL including the `MATERIALIZED VIEW` keyword.
  - **Snowflake** (6 tests): insufficient-privileges and warehouse-suspended categorisation; `::VARCHAR` cast and `SUM(CASE…)` null counter; `SAMPLE (1)` clause; `set_database_comment_sql` references the active `cfg.database`.
  - **Databricks** (6 tests): invalid-token, missing CA bundle, certificate-verify-failed categorisation; backtick-quoted catalog/schema/table FQN with the catalog optionally omitted; `CAST(... AS STRING)` casts.
  - **BigQuery** (5 tests): access-denied and quota-exhausted categorisation; project-prefixed FQN; `COUNTIF(... IS NULL)` builtin instead of `FILTER` / `SUM-CASE`; `TABLESAMPLE SYSTEM (1 PERCENT)`.

  Engine-bound methods (`list_materialized_views`, `get_table_stats`, `get_incoming_foreign_keys`, `get_*_comment`) will get SQLAlchemy-mocked tests in a follow-up PR.

### Changed
- **Per-profile Chroma collections** (`amx/search/index.py`): each DB profile is now mapped to its own Chroma collection (`amx_search_<sha256-prefix>`) instead of sharing a single `amx_search` collection filtered by a `db_profile` metadata field. Cross-profile pollution is now physically impossible regardless of caller discipline. The empty profile name still maps to the legacy `amx_search` collection so callers and tests that have not adopted the per-profile API keep working. The `query()` method no longer needs a `where` clause since the collection is already profile-scoped.
  - **Migration:** existing users should run `/rebuild` (inside `/search`) to populate the new per-profile collections — old data sitting in the shared `amx_search` collection is no longer queried for non-empty profiles.
- **`_collection_name_for(db_profile)`** helper: hashes profile names so they always land within Chroma's allowed character set (alnum, dot, dash, underscore; 3–63 chars) regardless of what the user typed (spaces, unicode, slashes).
- **6 new `PerProfileCollectionTests`** covering: empty-profile maps to the legacy name, deterministic and Chroma-valid hashing, two profiles get distinct names, unicode/special-char profile names hash safely, `upsert_entities` routes rows to the right collection per `db_profile`, and `query()` no longer passes a `where` clause.

- **`/embeddings` now lives under `/search` namespace** so users discover it inside the related tab — matching the pattern used by `/llm-profiles` (only valid inside `/llm`). The command also appears in the `/search` namespace help text.
- **Improved `/embeddings` picker UX**: the previous "keep" option (which looked ambiguous) is replaced with the current provider's labelled choice (e.g. `MiniLM (--default, current)` or `OpenAI-compatible (current)`); pressing Enter on the default is a no-op. Adds an explicit `Cancel` option for clarity.
- **Verbose provider labels** in the picker (`MiniLM`, `OpenAI-compatible`, `Local sentence-transformers`) replacing the terse `minilm`/`openai`/`local` aliases. The aliases still work as command arguments.
- **`/embeddings openai` prompt now lists endpoint examples** for OpenAI, OpenRouter, Together, Mistral, DeepInfra, Azure OpenAI, and local servers (vLLM / LM Studio / llama.cpp). The OpenAI-compatible mode already covers all of these — they only differ in `base_url` and the API key.
- **`/embeddings local` prompt now lists recommended HuggingFace models** (`BAAI/bge-large-en-v1.5`, `BAAI/bge-m3`, `intfloat/e5-large-v2`, `intfloat/multilingual-e5-large`) so users do not have to hunt for a starting point.
- **3 new picker tests** covering: default-Enter is a no-op, `Cancel` does not mutate, and the verbose `MiniLM` label routes to the same branch as `/embeddings minilm`.

### Added
- **LLM transient-retry** (`amx/llm/provider.py`): rate-limit (HTTP 429), timeouts, connection-reset, and 5xx upstream errors are now retried up to twice with exponential backoff (1s, 2s) before propagating. Authentication / bad-request errors still propagate immediately so the categorised `ErrorMapper` hint reaches the user fast. The existing Ollama legacy `/v1` 404 fallback is preserved as a first-attempt special case.
- **`_is_transient_llm_error()` helper**: classifies LLM exceptions by class name (`RateLimitError`, `APITimeoutError`, `APIConnectionError`, `InternalServerError`, `ServiceUnavailableError`, plus stdlib `TimeoutError` / `ConnectionError`) and by substring matching of common transient phrases (`429`, `rate limit`, `timed out`, `connection reset`, `503 service`, `502 bad gateway`, etc.). Available for unit testing and re-use.
- **5 new `LLMTransientRetryTests`** covering: 429-retried-then-succeeds, persistent timeout exhausts retries (3 total attempts), authentication errors do NOT retry (single attempt), substring-pattern classification for generic `RuntimeError`, and the `_is_transient_llm_error` truth table.
- **`/embeddings` slash command** (`amx/cli_support/commands/embeddings.py`): users can switch the search-index embedding provider without hand-editing `~/.amx/config.yml`. Forms:
  - `/embeddings` — show the current provider + interactive picker.
  - `/embeddings minilm` — switch to Chroma's bundled default (no setup).
  - `/embeddings openai [model]` — switch to an OpenAI-compatible endpoint; prompts for `base_url` and `api_key` (the key is stored in the OS keyring, never in YAML).
  - `/embeddings local [model]` — switch to a local sentence-transformers model (requires `pip install "amx[local-embeddings]"`).
  Switching reinstalls the runtime factory immediately and reminds the user to run `/search rebuild` so the catalog is re-embedded with the new provider.
- **`amx.search.embeddings.configure_from_amx_config`**: extracted from `cli.py` so the slash command can re-install the runtime factory without depending on the CLI module. Accepts an optional `on_warning` callback so misconfigured providers surface as themed warnings.
- **5 new `EmbeddingsSlashCommandTests`** covering the MiniLM / OpenAI / local branches, unknown-kind error, and OpenAI-with-empty-model rejection.
- **Runtime wire-up of `cfg.embedding`** (`amx/cli.py`, `amx/search/embeddings.py`, `amx/search/index.py`): the configured embedding provider is now actually used at runtime, not just stored in `~/.amx/config.yml`. The CLI installs a process-wide factory at startup based on `cfg.embedding`, and `SearchIndex(...)` falls back to that factory when no explicit `embedding_function` is passed. Misconfigured profiles emit a themed warning ("Embedding provider 'X' is not fully configured…") and gracefully fall back to MiniLM rather than failing retrieval.
- **`set_default_embedding_function` / `get_default_embedding_function`**: small singleton in `amx/search/embeddings.py` so `SearchIndex` constructors deep in the codebase pick up the user-chosen provider without needing the live `AMXConfig` plumbed through every caller. Test suite resets the singleton between cases to avoid cross-test bleed.
- **5 new `EmbeddingDefaultFactoryTests`** covering get-without-set, set/get round-trip, factory failure is swallowed (so retrieval keeps working with MiniLM fallback), `SearchIndex` picks up the default factory, and explicit `embedding_function` arg overrides the default.
- **`EmbeddingConfig` dataclass on `AMXConfig`**: `cfg.embedding` persists the user's chosen embedding provider (`kind` ∈ `minilm` / `openai_compatible` / `sentence_transformers`), `model`, `base_url`, and `api_key`. Round-trips through `~/.amx/config.yml`. The `api_key` is externalised to the OS keyring under `embedding/api_key` (same path as DB / LLM secrets) so it never lands on disk in plaintext. Legacy plaintext configs migrate on the next save.
- **`EmbeddingConfig.is_configured()`**: returns `True` for the MiniLM default (which needs no setup) and requires a non-empty `model` for the other two kinds. The CLI uses this to route users to a future `/embeddings` setup command without failing silently.
- **5 new `EmbeddingConfigPersistenceTests`** covering the default-MiniLM state, `openai_compatible` model-required validation, full save/load round-trip, keyring externalisation of the api_key, and legacy-plaintext migration.
- **Pluggable search embeddings** (`amx/search/embeddings.py`): the AMX search index now accepts a swap-in `EmbeddingFunction` so users can pick how their catalog is vectorised:
  - `MiniLMEmbedding` (default) — explicit wrapper around Chroma's bundled `all-MiniLM-L6-v2`. Behaviour unchanged for existing users; the choice is now visible in the codebase rather than implicit.
  - `OpenAICompatibleEmbedding` — points at any OpenAI-compatible `/embeddings` endpoint via `base_url + api_key + model`. Plugs in OpenAI proper, Azure OpenAI, OpenRouter, Together, Mistral, or local servers like LM Studio / vLLM / llama.cpp.
  - `SentenceTransformerEmbedding` — wraps `sentence-transformers` for stronger offline models (`BAAI/bge-large-en-v1.5`, `intfloat/e5-large-v2`, …). Requires the new `local-embeddings` extra: `pip install "amx[local-embeddings]"`.
- **`make_embedding_function()` factory**: returns `None` for the MiniLM default so callers can hand the result straight to `Chroma.get_or_create_collection` without special-casing the default path. Unknown kinds raise `ValueError`; missing optional deps raise `RuntimeError` with a remediation hint.
- **`SearchIndex(..., embedding_function=...)`**: new keyword argument threads the chosen provider into the underlying Chroma collection. Default remains `None` (Chroma's MiniLM) for backwards compatibility.
- **Tests** (7 new) covering: MiniLM default returns `None`, missing-model rejection, unknown-kind rejection, OpenAI-compatible mocked roundtrip with float32-precision-aware assertions, sentence-transformers missing-dep actionable error, and `SearchIndex` wiring (default vs explicit provider).
- **Transactional config writes** (`AMXConfig.transaction()`): a context manager that defers per-leaf-mutation YAML writes until the block exits and then performs a single atomic save. A bulk update like `with cfg.transaction(): cfg.db.host = ...; cfg.db.user = ...; cfg.db.password = ...` now writes the YAML once instead of three times, and a raise inside the block leaves the file unchanged so partially-written profiles never reach disk. Nested transactions are supported; only the outermost exit flushes. (`amx/config.py`)
- **4 new `ConfigTransactionTests`** covering single-save coalescing, raise-inside-block leaves disk untouched, nested-transaction outermost-only flush, and `write_through_config = False` honoured inside transactions.
- **Connector error categorisation** (`amx/core/errors.py`): `ErrorMapper` now classifies authentication failures (wrong password, expired token, `401 Unauthorized`), network errors (DNS, connection refused, no route to host), TLS/SSL handshake failures, and missing-database/catalog errors as distinct actionable categories with backend-specific remediation hints. The previous behaviour bucketed everything under "Object not found" or returned the raw driver string.
- **8 new `ErrorMapper` regression tests** covering PostgreSQL auth failure, Databricks invalid-token, generic network unreachable, DNS failure, SSL handshake failure, the existing Databricks-specific TLS branch (still wins over the new generic SSL branch), missing database, and the explicit "uncategorised" passthrough.
- **OS-keyring secret storage** (`amx/storage/secrets.py`): DB passwords, LLM API keys, and Databricks access tokens are now externalised to the OS keyring (macOS Keychain, Windows Credential Manager, Linux Secret Service) on save. The YAML stores opaque references like `keyring:db_profiles/default/password` instead of plaintext. Loaders resolve references transparently, so the rest of AMX still sees `cfg.db.password` as a string. Adds `keyring>=24.0` to runtime dependencies.
- **Backward-compatible secret migration**: Existing configs with plaintext passwords keep working; the next save promotes those secrets into the keyring and blanks the plaintext. No manual migration step required.
- **`NullSecretStore` fallback**: On platforms with no available keyring backend (rare — typically a headless Linux box without GNOME / KDE / D-Bus), AMX leaves secrets in plaintext rather than dropping them silently, so users can decide how to proceed.
- **Test isolation**: New `tests/conftest.py` swaps in an `InMemorySecretStore` for every test via an `autouse` fixture, preventing the suite from leaking placeholder credentials into the developer's real keyring.
- **Regression coverage**: 6 new tests in `tests/test_regressions.py::SecretKeychainTests` covering externalisation, resolution, legacy-plaintext migration, missing-key graceful fallback, LLM-vs-DB scope separation, and `NullSecretStore` behaviour.
- **First-run safety**: A truly fresh install (no `~/.amx/config.yml`) now leaves `db_profiles` and `llm_profiles` empty instead of silently creating a "default" DB profile pointing at `localhost / amx / amx_pass / SAP`. The startup banner now warns "First run detected — run /setup" so new users no longer see a phantom broken connection. Legacy configs that already exist on disk keep their previous fallback behavior. (`amx/config.py`, `amx/cli.py`)
- **`AMXConfig.is_first_run`** property and `_fresh_install` field record whether the config file was found on load — callers can route to `/setup` instead of fabricating placeholder state.
- **`DBConfig.is_configured()` / `LLMConfig.is_configured()`**: lightweight per-backend "do we have the minimum fields to actually connect / dispatch" check used by the startup banner to distinguish "not configured yet" from "incomplete profile".
- **`--debug` flag** on `amx`: shows full tracebacks, sets `AMX_DEBUG=1` in the environment so other subsystems can opt in to verbose logging.
- **Top-level CLI crash handler**: unexpected exceptions are rendered as a themed error line with a pointer to `~/.amx/logs/amx.log` and the `--debug` flag, instead of dumping a raw traceback to the terminal. `KeyboardInterrupt` exits with code 130 cleanly.
- **`os.chmod(config_path, 0o600)`** after every config save (POSIX): the YAML holds DB passwords and API keys, so it must not be world-readable. No-op on Windows.
- **Regression coverage**: 5 new tests in `tests/test_regressions.py::FirstRunConfigTests` covering fresh-install detection, legacy-config fallback, chmod-on-save, and `is_configured()` per backend.
- **GitHub Actions CI** (`.github/workflows/ci.yml`): ruff lint + format check, mypy (advisory), `pytest` on Python 3.10/3.11/3.12, and a build/`twine check` job that uploads the sdist + wheel as artifacts on every PR and push to `main`.
- **GitHub Actions release pipeline** (`.github/workflows/release.yml`): tag-driven (`v*.*.*`) build → PyPI publish via OIDC Trusted Publisher (no API token in repo) → GitHub Release with auto-generated notes. Verifies `pyproject.toml` version matches the tag before publishing.
- **`python-semantic-release` configuration** in `pyproject.toml`: future versions and changelog entries are derived from Conventional Commit subjects (`feat:` → minor, `fix:`/`perf:` → patch, `BREAKING CHANGE:` → major). Repo PyPI/GitHub-release upload steps are owned by `release.yml` so semantic-release only computes the version + writes the changelog.
- **Pre-commit configuration** (`.pre-commit-config.yaml`): ruff + ruff-format, standard hygiene hooks, large-file guard, `detect-private-key`, and `gitleaks` for secret scanning before commit.
- **PyPI metadata**: `keywords`, 13 trove `classifiers`, and `[project.urls]` for Homepage / Repository / Issues / Changelog now ship in the wheel for proper PyPI listing.
- **Contributor docs**: new `CONTRIBUTING.md` (dev setup, branching, Conventional Commits table, lint/test/release commands), `SECURITY.md` (private disclosure path, supported versions, secret-storage statement), `LICENSE` (MIT — was missing despite `pyproject.toml` declaring it), and a `.github/pull_request_template.md` with Summary / Test plan / Risk sections.
- **Dev extras**: `pip install -e ".[dev]"` now provides `pytest`, `pytest-cov`, `ruff`, `mypy`, `pre-commit`, `build`, and `twine`.

### Changed
- **Lint/format/type/test configuration** centralised in `pyproject.toml`: `[tool.ruff]` (line-length 100, E/F/I/UP/B/SIM/C4 rule sets), `[tool.ruff.format]`, `[tool.mypy]` (`ignore_missing_imports`, gradual-typing baseline), `[tool.pytest.ini_options]` (custom markers `slow`/`integration`/`live`, `--strict-markers`, deprecation filters for litellm/pydantic).

### Notes
- This release does not change runtime behaviour; it is a release-engineering and contributor-experience baseline. The next runtime change tagged via `git tag v…` will trigger an automated PyPI release for the first time.
- `CHANGELOG.local.md` is being kept temporarily for archival reference but will be retired in a future release.

## [0.2.9] — 2026-04-29
### Changed
- **Interactive LLM preflight in `/run`**: The analyze-run LLM health-check now uses the same live progress display style as the database connection test, with visible activity timing before profiling starts.
- **Slash-command LLM guidance**: User-facing “LLM not configured” and setup-retry messages now point to `/llm`, `/add-llm-profile`, and `/setup` instead of shell-style `amx setup` wording.

### Added
- **Regression coverage**: Added CLI tests for the interactive LLM preflight output and the new slash-command guidance when no active LLM profile exists.

## [0.2.8] — 2026-04-29
### Fixed
- **Active DB profile overwrite bug**: Re-saving an existing active DB profile through `/add-db-profile` no longer reverts the new backend/settings back to the previously active connection during autosave. Switching an active profile from PostgreSQL to Databricks now persists the Databricks backend, warehouse path, token, and TLS settings correctly.

### Added
- **Regression coverage**: Added tests for active-profile DB upserts and atomic `/add-db-profile` replacement of an existing active profile.

## [0.2.7] — 2026-04-29
### Changed
- **Analyze LLM preflight**: `/analyze /run` now performs an LLM health-check before profiling begins and stops immediately with an actionable AMX error when the active model/profile is unreachable or deactivated.
- **Quiet third-party LLM output**: LiteLLM warning/debug logger spill is now suppressed by default so SSL/cost-map and debug chatter do not leak into the interactive terminal unless AMX chooses to surface a message.
- **Visible Profile Agent failures**: Profile Agent batch/parse/empty-response failures are now promoted to explicit AMX warnings instead of staying buried in log files or transient live-display rows.

### Added
- **Regression coverage**: Added CLI and regression tests for analyze-time LLM preflight, LiteLLM logger suppression, and explicit Profile Agent failure surfacing.

## [0.2.6] — 2026-04-29
### Changed
- **Deterministic `/add-db-profile` editing**: Interactive DB profile edits now use explicit update semantics instead of implicit default reuse. Pressing Enter keeps the current value, `-` clears optional fields, and Databricks TLS selection now uses an explicit yes/no choice list.

### Added
- **Regression coverage**: Added tests proving Databricks profile edits correctly overwrite, clear, and preserve values during `/add-db-profile`.

## [0.2.5] — 2026-04-29
### Added
- **Deterministic Databricks TLS command**: `/db tls [on|off] [ca_path|clear]` now updates the active Databricks profile directly and prints the saved value, so operators do not have to rely on interactive yes/no prompts.

### Added
- **Regression coverage**: Added helper and CLI tests for direct Databricks TLS setting changes on the active profile.

## [0.2.4] — 2026-04-29
### Changed
- **Native Databricks connect test**: Databricks connection checks now use `databricks-sql-connector` directly for the health-check query instead of validating through the SQLAlchemy engine path first.
- **Sharper Databricks auth diagnostics**: Invalid Databricks access tokens are now classified separately from TLS and warehouse-path failures.

### Added
- **Regression coverage**: Added Databricks tests proving native connector usage during connect checks and explicit invalid-token error mapping.

## [0.2.3] — 2026-04-29
### Added
- **Staged Databricks connect recovery**: `/db connect` now retries Databricks TLS failures in ordered stages, reports which recovery step passed, and persists the first successful CA bundle or last-resort `tls_no_verify` setting into the active DB profile.
- **Regression coverage**: Added unit and CLI integration tests for Databricks staged connect recovery via environment CA bundle and `tls_no_verify` fallback.

### Changed
- **Connection diagnostics**: Database connection checks now expose structured result details so CLI flows can react to actionable backend failures instead of treating every failed connect as a bare boolean.

## [0.2.2] — 2026-04-29
### Changed
- **Databricks corporate TLS setup**: Trusted CA bundle paths now expand `~` and environment variables, and Databricks profiles can inherit a CA bundle from `AMX_DATABRICKS_TRUSTED_CA_FILE`, `DATABRICKS_TRUSTED_CA_FILE`, `REQUESTS_CA_BUNDLE`, or `SSL_CERT_FILE`.
- **Actionable CA path validation**: Missing Databricks CA bundle files now fail before opening the warehouse connection and report a direct remediation message instead of being misclassified as a missing database object.

### Added
- **Regression coverage**: Added Databricks TLS tests for path validation and environment-variable CA bundle fallback.

## [0.2.1] — 2026-04-29
### Added
- **SchemaExplorer tool**: Added a macro-level structural inventory tool that returns namespace table lists, row counts, column counts, UMI paths, and evidence-derived semantic cluster labels.
- **Analytical inventory strategy**: `/search ask` now routes broad structural questions such as “how many columns per table?” to `schema_inventory` instead of narrowing to one best match.

### Changed
- **Set-based synthesis**: Inventory answers now render a structured Markdown table across all discovered tables and include aggregate totals, preventing point-biased summaries for broad questions.
- **Headless ask reasoning**: `LoopBasedAskAgent` now classifies questions into inventory, definition, relationship, and deep-dive strategies, and uses SchemaExplorer for inventory in library mode.
- **Tool-use transparency**: Thought traces now show the `schema_explorer` step, table count, total column count, and gap-fill operations for inventory answers.

## [0.2.0] — 2026-04-29
### Added
- **Headless core facade**: Added `AMXApplication` plus public `import amx` exports so AMX can be embedded as a library without entering the interactive CLI.
- **Universal Metadata Interface**: Added canonical `AbstractEntity` metadata objects and a `UniversalMetadataAdapter` for table profiles and catalog rows, separating downstream reasoning from backend-specific names.
- **Tool-loop ask foundation**: Added `AskToolbox` and `LoopBasedAskAgent` primitives with `metadata_query`, `semantic_search`, `doc_rag_query`, and `sample_data_query` tool surfaces.
- **State and audit persistence**: Added SQLite `session_state` plus `raw_logprob`, `token_count`, and `model_version` audit columns on saved inference results.
- **Actionable error mapping**: Added a central `ErrorMapper` for translating common backend failures into operator instructions.

### Changed
- **Write-through config persistence**: Loaded top-level and nested DB/LLM config mutations now trigger atomic config saves immediately.
- **Search diagnostics**: `/search ask` now records a concise observable thought trace for interpretation, retrieval, live probe, and verification stages.
- **RAG token guard and reranking**: RAG prompts now compact oversized document chunks and document retrieval reranks explanatory chunks over repetitive technical headers.
- **Rule-purged semantic scoring**: Removed vendor-specific alias/date naming boosts from semantic join and exact-candidate scoring; retrieval now relies on lexical, structural, statistical, and semantic evidence without hardcoded business-code aliases.
- **Description-only logprob scoring**: Whole-response confidence now scores only generated description/comment values when structured output is present, excluding JSON/labeled boilerplate.

## [0.1.129] — 2026-04-29
### Changed
- **LLM coverage-audit routing contract**: `/search` interpretation/review prompts now require `request_type`, and broad “missing comment” requests are explicitly modeled as `coverage_audit`.
- **Safer LLM-to-route normalization**: If the LLM returns `request_type=coverage_audit`, plan normalization now forces `intent=check_coverage` and `search_mode=check_coverage` before retrieval, preventing semantic table-match drift.
- **Regression coverage**: Added tests for both reviewer-corrected and single-pass classifier coverage-audit routing paths.

## [0.1.128] — 2026-04-29
### Changed
- **LLM-native multilingual interpretation**: `/search` now uses an LLM-first interpretation contract with LLM-derived `answer_language`, and no longer relies on a rule-first intent router in the normal path.
- **Balanced two-pass routing**: Added a classifier + selective reviewer interpretation flow, plus decision metadata (`decision_confidence`, `needs_clarification`, `clarification_question`) to improve routing quality before retrieval.
- **Clarification-first safety**: When interpretation confidence is low (or clarification is explicitly required), `/search` now asks a scope clarification question instead of silently misrouting.
- **Configurable interpretation behavior**: Added search settings for `interpretation_mode` and `clarification_on_low_confidence` to tune routing strictness without code changes.

## [0.1.127] — 2026-04-29
### Changed
- **LLM-first `/search` interpretation**: `/search` question routing now always asks the interpreter LLM first instead of short-circuiting through rule-first intent detection.
- **Safer fallback behavior**: Deterministic rule routing is still preserved as a resilience fallback only when LLM interpretation fails, reducing misrouted scope/intent decisions while keeping fail-safe continuity.
- **Join-discovery answer consistency**: `joinable_tables` routing now keeps deterministic answer strategy where expected, so join-column outputs remain stable after the LLM-first transition.

## [0.1.126] — 2026-04-29
### Added
- **Shared live progress across long-running CLI flows**: `/search sync`, `/search rebuild`, `/db schemas`, `/db tables`, `/db profile`, `/docs scan`, `/docs ingest`, `/docs analyze`, `/code scan`, `/code analyze`, and `/code refresh` now reuse the `/run`-style live activity display with continuously visible elapsed timing.

### Changed
- **Interactive scope discovery visibility**: Schema/asset selection helpers and manual-edit pickers now show timed progress while AMX waits on `list_schemas()`, `list_assets()`, and column-introspection calls.
- **Batch polling visibility**: Batch-mode LLM polling now updates the live activity tree when a live display is active, instead of only emitting intermittent plain terminal lines.

## [0.1.125] — 2026-04-29
### Changed
- **Prompt hardening across `/search` and `/analyze`**: Search interpretation/answer prompts and analyze-agent prompts now use stricter grounding language, clearer ambiguity handling, stronger confidence discipline, and more conservative fallback behavior.
- **Conservative merge guidance**: The orchestrator merge prompt now uses explicit source precedence and conflict-resolution guidance instead of implicitly averaging profile, code, and RAG suggestions.

### Fixed
- **Prompt-output resilience**: Profile, code, RAG, merge, and metadata-summary parsers now tolerate fenced LLM output more reliably, reducing avoidable parse failures from otherwise valid responses.
- **Schema/database summarization guardrails**: Schema-level and database-level metadata prompts now explicitly forbid unsupported business-scope extrapolation from table summaries alone.

## [0.1.124] — 2026-04-29
### Changed
- **`/search` rule-first routing**: High-confidence intents such as exact field lookups, explicit table explanations, join questions, and inventory questions now route through deterministic guardrails before the interpreter LLM is consulted.
- **Shorter `/search` answers**: Search responses now prefer deterministic short-form summaries and only fall back to LLM synthesis when the grounded evidence still needs comparative or grouped narration.
- **Narrower search memory scope**: Follow-up memory now preserves table scope more carefully so broad semantic result sets are less likely to contaminate later table-scoped questions.

### Fixed
- **Deterministic live probe planning**: Table-scoped factual metadata checks now select safe read-only probes without a second planner LLM call, reducing irrelevant planner output while still executing live verification automatically.
- **Weak result suppression**: Low-confidence vector-only tail matches are now deprioritized or suppressed before `/search` produces the user-facing answer, reducing noisy or irrelevant responses.
- **Search diagnostics**: `/search` answers now persist the executed read-only actions, suppressed-row count, and answer strategy in addition to the existing retrieval and verification payloads.

## [0.1.123] — 2026-04-29
### Changed
- **Single-line write-back progress**: Apply-mode live progress now uses one rolling write-back activity line instead of rendering one completed line per column comment.
- **Databricks grouped column comment writes**: AMX now groups adjacent Databricks column comment updates for the same table into one `ALTER TABLE ... ALTER COLUMN ...` statement when the warehouse accepts the syntax, reducing request count substantially.

### Fixed
- **Databricks fallback safety**: If a grouped Databricks column-comment statement fails, AMX falls back to per-column write-back rather than aborting the remaining items in that table batch.

## [0.1.122] — 2026-04-29
### Added
- **Realtime write-back progress**: `analyze apply`, `history review --apply`, and orchestrator-driven write-back now render a live activity view with elapsed time and per-asset progress, mirroring the runtime feel of `/run`.

### Fixed
- **Persistent DB write-back failure state**: When a comment fails during database write-back, AMX now stores `db_applied_status='failed'` for that `run_results.id`, so the SQLite-backed review/history views can show that the description was not written to the database.
- **Applied status consistency**: Successful write-back now explicitly marks `db_applied_status='applied'` and clears any older stored failure reason for the same row.

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
