"""Single source of truth for AMX-internal table/column descriptions.

AMX is a metadata-generation tool whose product thesis is "every database
table and column should ship with a meaningful description." Its own
internal storage must meet that bar. This module holds the canonical
description strings for every table AMX creates in:

* the **local** SQLite history store at ``~/.amx/history.db`` (created
  on first run by :class:`amx.storage.sqlite_store.SQLiteHistoryStore`).
  Descriptions land in the sidecar table ``_amx_schema_descriptions``
  populated idempotently at every startup, since SQLite has no native
  ``COMMENT ON`` syntax.
* the **shared** warehouse-hosted history store bootstrapped by
  ``/history-store enable`` (declared in
  :mod:`amx.storage.shared_schema` as SQLAlchemy ``Column(comment=…)``
  annotations, which emit ``COMMENT ON COLUMN`` / ``COMMENT ON TABLE``
  on every backend that supports them).

Both stores import their description strings from here so they cannot
drift. ``tests/test_local_schema_comments.py`` and
``tests/test_shared_schema_comments.py`` enforce that every declared
column has an entry, no entry is orphaned, and the two stores agree
byte-for-byte on overlapping columns.

When adding a new table or column to either store, the matching entry
in :data:`SCHEMA_DESCRIPTIONS` is mandatory in the same commit — see
the "Database schema conventions" section of ``CONTRIBUTING.md``.
"""

from __future__ import annotations

# ── Attribution column descriptions reused across multiple tables ──────────
# Pulled out as module-level constants so the same string is not
# duplicated across every table that carries provenance metadata.

ATTRIBUTION_CREATED_BY = (
    "OS username (or AMX_USER override) of the principal that wrote this row. "
    "Populated on every shared-mode write so '/history-store list-team' can answer "
    "'who ran what?'."
)
ATTRIBUTION_HOSTNAME = (
    "Machine that wrote this row. Part of the (hostname, local_id) provenance pair "
    "that lets the dual-write coordinator re-find the shared row when a later UPDATE "
    "(e.g. finish_run) fires from the same machine."
)
ATTRIBUTION_CLIENT_VERSION = (
    "AMX version string (e.g. '0.12.1') of the client that wrote this row. Used by "
    "/doctor and post-mortems to correlate row shape changes with client upgrades."
)
ATTRIBUTION_LOCAL_ID = (
    "Corresponding INT id in the writer machine's local SQLite history.db. Scoped "
    "by hostname so two machines can both have local_id=5 without collision; lets "
    "the dual-write coordinator locate the shared row for in-flight UPDATEs."
)


# ── Database-level descriptions ────────────────────────────────────────────

LOCAL_DATABASE_DESCRIPTION = (
    "AMX local history store. Created on first run by SQLiteHistoryStore at "
    "~/.amx/history.db. Holds per-machine analysis run history, per-asset LLM "
    "alternatives, catalog search index, scheduled runs, apply audit trail, "
    "and various caches used to keep the CLI and Studio responsive. In shared "
    "mode the dual-write coordinator mirrors a subset of these tables into the "
    "warehouse-hosted AMX schema (see amx.storage.shared_schema). The sidecar "
    "table _amx_schema_descriptions records this description plus one row per "
    "table and column for queryability."
)

SHARED_SCHEMA_COMMENT = (
    "AMX shared run-history schema. Created by AMX (Agentic Metadata "
    "Extractor) via /history-store enable. Holds cross-machine analysis "
    "history, per-asset LLM alternatives, app events, agent session "
    "state, and a schema version stamp so multiple AMX clients can "
    "share run history under one warehouse. See "
    "https://github.com/omeryasirkucuk/amx for details."
)


# ── Per-table column descriptions ──────────────────────────────────────────
# Each entry maps a column name to its description. The ``__table__`` key
# (sentinel, not a real column) holds the table-level description.
#
# Tables that appear in **both** the local SQLite store and the shared
# SQLAlchemy store list every column from the union of the two — local-only
# and shared-only columns coexist in the same dict because the table is
# the natural grouping. Each store reads only the columns it actually
# declares; the cross-store test (test_local_schema_comments.py) asserts
# that columns present in BOTH carry the same description.

SCHEMA_DESCRIPTIONS: dict[str, dict[str, str]] = {
    # ── analysis_runs (local + shared) ────────────────────────────────────
    "analysis_runs": {
        "__table__": (
            "One row per AMX analysis run (/run, /run-apply, /ask, "
            "doc-analyze, code-analyze). Captures the inputs (scope, "
            "profiles, settings) and outputs (results, metrics, errors) of "
            "an LLM-driven metadata generation. Joined to run_results for "
            "per-asset alternatives. Read by /list, /show, /stats, /compare."
        ),
        "id": (
            "UUID v4 primary key. Surfaced as a short prefix in CLI output "
            "(/list, /show, /results, /review, /compare). UUID rather than "
            "INT autoincrement because shared mode admits concurrent writers "
            "from multiple machines."
        ),
        "started_at": (
            "UTC timestamp when the agent run began. Indexed for "
            "last-N-days filters in /list and /stats."
        ),
        "ended_at": (
            "UTC timestamp when the run finished. NULL while a run is in "
            "flight; also NULL for runs killed mid-flight by Ctrl-C."
        ),
        "duration_sec": (
            "Wall-clock seconds between started_at and ended_at. Convenience "
            "column so /stats does not have to recompute it on every read."
        ),
        "status": (
            "Lifecycle state of the run: running | completed | failed | "
            "cancelled. Drives /list filters and the colored status badge."
        ),
        "command": (
            "Top-level CLI command that triggered the run: run | run-apply | "
            "ask | doc-analyze | code-analyze. Distinguishes batch metadata "
            "generation from one-off Q&A in /stats breakdowns."
        ),
        "mode": (
            "Sub-mode chosen at the run picker: human-review | auto-apply | "
            "confidence-threshold | dry-run."
        ),
        "db_backend": (
            "Backend of the analyzed DB profile: postgresql | snowflake | "
            "bigquery | databricks | mssql | mysql | oracle | redshift. "
            "Enables /compare --by db_backend cross-backend audits."
        ),
        "db_profile": (
            "Named DB profile used for this run (see /db-profiles). Multi-"
            "profile runs (0.11+) record the first profile here and the "
            "full list in scope_json.profiles."
        ),
        "llm_provider": (
            "LLM vendor: openai | anthropic | gemini | openrouter | "
            "deepseek | ollama | …. Distinct from llm_model which records "
            "the specific model id."
        ),
        "llm_model": (
            "Specific model id served by llm_provider — e.g. 'gpt-4o', "
            "'claude-sonnet-4-20250514', 'openai/gpt-4o-mini' on OpenRouter."
        ),
        "scope_json": (
            "JSON describing the analyzed scope: {schemas, tables, columns, "
            "asset_kinds, profiles}. /compare reads this to find prior runs "
            "of the same assets for side-by-side pivots."
        ),
        "metrics_json": (
            "JSON of run metrics — counts, per-stage timings, retries, "
            "skipped assets. Free-form so newer agents can add fields "
            "without a schema bump."
        ),
        "tokens_json": (
            "JSON of token usage broken down by phase: "
            "{prompt, completion, cached, reasoning, total}. Drives /stats "
            "cost reporting and /compare --by tokens."
        ),
        "results_json": (
            "JSON summary of run outputs (counts, top-level rollups). "
            "Per-asset detail lives in run_results joined on run_id."
        ),
        "error_text": (
            "Stack trace or error message when status='failed'. NULL on successful runs."
        ),
        "selected_count": (
            "Assets the user selected at the run picker. First step of the "
            "selected → planned → processed → applied funnel."
        ),
        "planned_count": (
            "Assets that survived post-selection filtering (already-good "
            "comments skipped, unsupported asset kinds dropped, etc.)."
        ),
        "processed_count": (
            "Assets the LLM successfully produced descriptions for. "
            "planned_count - processed_count = LLM/network failures."
        ),
        "applied_count": (
            "Assets whose chosen description was written to the live DB "
            "via COMMENT ON. processed_count - applied_count = approved-"
            "but-not-yet-applied (when running without --apply)."
        ),
        "review_strategy": (
            "How alternatives were chosen: human | auto-best | "
            "confidence-threshold. Affects how to read evaluated_at on "
            "joined run_results rows."
        ),
        "llm_profile": (
            "Named LLM profile used for this run (see /llm-profiles). "
            "Captures the user-facing handle; concrete provider/model "
            "values are mirrored in llm_provider/llm_model."
        ),
        "doc_profile": (
            "Named document profile that supplied RAG evidence (see "
            "/doc-profiles). NULL when /docs was not used in this run."
        ),
        "code_profile": (
            "Named code profile that supplied code-evidence (see "
            "/code-profiles). NULL when /code was not used in this run."
        ),
        "settings_json": (
            "Snapshot of LLM settings at run time — temperature, "
            "prompt_detail, n_alternatives, llm_batch_size, "
            "description_verbosity, logprob_thresholds. Drives "
            "/compare --by settings pivots so changes can be A/B-attributed."
        ),
        "created_by": ATTRIBUTION_CREATED_BY,
        "hostname": ATTRIBUTION_HOSTNAME,
        "client_version": ATTRIBUTION_CLIENT_VERSION,
        "local_id": ATTRIBUTION_LOCAL_ID,  # shared-only
        "shared_uuid": (  # local-only
            "When a row was pulled down from the team's shared store via "
            "/history-store pull-from-shared, this holds the UUID PK of the "
            "corresponding shared row. NULL for runs created locally. Lets "
            "re-pull be idempotent: we look up by shared_uuid before insert."
        ),
        "triggered_by_schedule_id": (  # local-only
            "Foreign-key-by-convention to scheduled_runs.id when this run "
            "was fired by the scheduler. NULL for ad-hoc runs and for runs "
            "created before the scheduled-runs feature shipped."
        ),
        "last_heartbeat_at": (  # local-only
            "UTC epoch seconds of the most recent heartbeat from the run "
            "orchestrator while a run is in flight. Consumed by the stale-"
            "run recovery path to fail runs whose host crashed mid-flight. "
            "NULL for completed runs and for runs created before this column."
        ),
        "current_step_label": (  # local-only
            "Most recent phase label for an in-flight run (e.g. "
            "'Connecting to local-postgre @ bird_train', 'Initializing LLM "
            "openai/gpt-4o', 'Profiling sales.orders'). Rewritten by the "
            "web worker as it walks startup phases and the per-table loop "
            "so a Studio page refresh can render meaningful progress "
            "instead of falling back to 'Waiting for the worker to begin'. "
            "Cleared back to NULL by finish_run; never populated for "
            "completed runs."
        ),
    },
    # ── run_results (local + shared) ──────────────────────────────────────
    "run_results": {
        "__table__": (
            "Per-asset LLM alternatives generated during an analysis_runs "
            "invocation. One row per (asset, alternative) — a column with "
            "3 alternatives produces 3 rows. Captures the alternative payload, "
            "confidence, evaluation/apply state, and a back-pointer to the run."
        ),
        "id": "UUID v4 primary key for this (asset, alternative) row.",
        "run_id": (
            "Foreign-key-by-convention to analysis_runs.id. Not a hard FK "
            "because shared mode admits replication lag — a result row can "
            "land before its parent run row when two writers race."
        ),
        "saved_at": (
            "UTC timestamp when this alternative was persisted. Distinct "
            "from analysis_runs.started_at when the LLM streams alternatives "
            "across the run window."
        ),
        "schema_name": "Schema (or dataset/database) of the asset described.",
        "table_name": "Table or view name of the asset described.",
        "column_name": (
            "Column name when asset_kind='column'. NULL when asset_kind='table' "
            "(the alternative describes the table itself)."
        ),
        "asset_kind": (
            "What this row describes: table | view | materialized_view | "
            "column. Drives which COMMENT ON variant is emitted on apply."
        ),
        "source": (
            "Which agent produced this alternative: profile | doc | code | "
            "combined | manual. Lets /compare and /review filter by source "
            "of evidence."
        ),
        "confidence": (
            "Bucketed quality label derived from logprob_score against the "
            "/logprob-thresholds settings: high | medium | low. Used by "
            "/review filters and the human-review UI."
        ),
        "logprob_score": (
            "Normalized average log-probability of the description tokens "
            "(-1..0). Higher = the LLM was more confident in the wording."
        ),
        "raw_logprob": "Sum of log-probabilities (unnormalized) backing logprob_score.",
        "token_count": "Token length of the alternative description.",
        "model_version": (
            "Specific model id that produced this alternative (e.g. "
            "'gpt-4o-2024-11-20'). May differ from analysis_runs.llm_model "
            "when the user switches mid-run."
        ),
        "reasoning": (
            "Optional reasoning trace from a reasoning model (o-series, "
            "claude with thinking, deepseek-reasoner). NULL for normal "
            "chat models."
        ),
        "alternatives_json": (
            "Ordered JSON list of alternative description strings the LLM "
            "produced for this asset. The /review picker presents these; "
            "chosen_description records which one was selected."
        ),
        "evaluated_at": (
            "UTC timestamp when a human (or auto-best) picked one of the "
            "alternatives. NULL = pending review."
        ),
        "applied_at": (
            "UTC timestamp when chosen_description was written to the live "
            "DB via COMMENT ON. NULL = approved but not yet applied (or "
            "never approved)."
        ),
        "chosen_description": (
            "The specific alternative selected at evaluation time. Empty "
            "string when evaluation='rejected'."
        ),
        "evaluation": (
            "Outcome of human/auto review: approved | rejected | edited. "
            "'edited' means the user accepted an alternative but modified "
            "the wording before apply."
        ),
        "catalog_status": (
            "Sync state with the /search catalog: pending | indexed | stale "
            "| skipped. Drives /search rebuild incremental updates."
        ),
        "catalog_indexed_at": "UTC timestamp of the last /search-catalog index for this row.",
        "db_applied_status": (
            "Result of the COMMENT ON write to the live DB: success | "
            "skipped | failed. Empty string until apply is attempted."
        ),
        "effective_source_kind": (
            "What actually became the column's description after evaluation: "
            "same labels as `source` plus 'manual-edit'. Distinct from "
            "`source` because a user may approve a doc-sourced alternative "
            "after a manual edit."
        ),
        "superseded_at": (
            "Set when a newer run produces a better description for the same "
            "asset. Lets /history filters hide stale rows without deleting "
            "them — full audit trail preserved."
        ),
        "rejection_reason": (
            "Free-text reason captured at evaluation time when "
            "evaluation='rejected'. Surfaced by /review for retrospectives."
        ),
        "hostname": ATTRIBUTION_HOSTNAME,
        "local_id": ATTRIBUTION_LOCAL_ID,  # shared-only
        "created_by": ATTRIBUTION_CREATED_BY,  # local-only on run_results
        "shared_uuid": (  # local-only
            "UUID PK of the corresponding shared-store row when this row was "
            "pulled down via /history-store pull-from-shared. NULL for rows "
            "created locally. Indexed to keep re-pulls idempotent."
        ),
        "parent_result_id": (  # local-only
            "When this row is a re-run of an earlier one, points back to the "
            "original run_results.id. NULL for original (rerun_seq=0) rows."
        ),
        "rerun_seq": (  # local-only
            "0 for the original row, 1+ for successive re-runs in the chain. "
            "Lets the history drawer order re-run alternatives chronologically."
        ),
        "user_instructions": (  # local-only
            "Optional free-text addendum the user typed in the re-run modal. "
            "Surfaced next to the alternatives in /history and Studio so the "
            "audit trail explains why the re-run was requested."
        ),
        "citations_json": (  # local-only
            "JSON-encoded list[{source, chunk_idx, score, snippet}] of "
            "RAG-derived citations backing this alternative. NULL on legacy "
            "rows and non-RAG sources — callers treat NULL and [] alike."
        ),
    },
    # ── app_events (local + shared) ───────────────────────────────────────
    "app_events": {
        "__table__": (
            "Append-only structured event log surfaced by /events. Records CLI "
            "lifecycle events (connection tests, syncs, doctor checks, errors) "
            "for audit and debugging. Distinct from analysis_runs which logs "
            "LLM agent invocations."
        ),
        "id": "UUID v4 primary key for the event.",
        "created_at": "UTC timestamp the event fired. Indexed for /events recent-first ordering.",
        "event_type": (
            "Coarse event family: cli | db | llm | doc | code | error | "
            "history-store. Lets /events filter by subsystem."
        ),
        "status": "Severity of the event: info | warn | error.",
        "command": "CLI command (or sub-action) that was running when the event fired.",
        "details_json": (
            "Free-form JSON payload — varies per event_type. Examples: "
            "{profile, backend} for db connect, {model, latency_ms} for "
            "llm calls, {error_class, traceback} for errors."
        ),
        "created_by": ATTRIBUTION_CREATED_BY,  # shared-only
        "hostname": ATTRIBUTION_HOSTNAME,  # shared-only
        "client_version": ATTRIBUTION_CLIENT_VERSION,  # shared-only
    },
    # ── session_state (local + shared) ────────────────────────────────────
    "session_state": {
        "__table__": (
            "Namespaced key/value storage used by StateManager for inter-turn "
            "agent memory within /ask conversational sessions. Composite primary "
            "key (namespace, key_name, hostname) so a teammate's state under "
            "the same namespace does not clobber yours in shared mode."
        ),
        "namespace": (
            "Logical grouping for related keys (e.g. 'ask_session_42' or "
            "'review_state'). Lets multiple StateManager instances share a "
            "table without colliding."
        ),
        "key_name": "Key within the namespace.",
        "hostname": (  # NOTE: not generic ATTRIBUTION_HOSTNAME — session_state
            # has its own semantics because hostname participates in the
            # composite primary key.
            "Writer machine. Part of the composite PK so a teammate's state "
            "under the same (namespace, key_name) does not clobber yours in "
            "shared mode. Empty string when running in single-user mode."
        ),
        "value_json": "JSON-serialized value associated with (namespace, key_name, hostname).",
        "updated_at": "UTC timestamp of the last write to this row.",
        "created_by": ATTRIBUTION_CREATED_BY,  # shared-only
    },
    # ── schema_meta (shared only) ─────────────────────────────────────────
    "schema_meta": {
        "__table__": (
            "Single-row version stamp written at /history-store enable bootstrap. "
            "Newer AMX clients bump schema_version on first write; older clients "
            "refuse to write into a schema bumped beyond what they know about, "
            "mirroring the AMXConfig schema_version guard."
        ),
        "id": (
            "Singleton sentinel — always 1. The PK exists only because every "
            "table needs one; this table holds at most one row."
        ),
        "schema_version": (
            "Current version of the AMX shared-store schema. Older clients "
            "refuse to write when this is higher than what they were built "
            "against, mirroring the AMXConfig schema_version compatibility "
            "guard."
        ),
        "created_at": "UTC timestamp when /history-store enable first bootstrapped this schema.",
        "created_by_client_version": "AMX version string of the client that ran the bootstrap.",
    },
    # ── style_profiles (local + shared) ───────────────────────────────────
    "style_profiles": {
        "__table__": (
            "One row per LLM profile containing the derived StyleProfile that "
            "summarises the user's description writing conventions. The profile "
            "is injected into run-time prompts to keep generated descriptions "
            "consistent with the existing corpus style."
        ),
        "id": "Surrogate primary key.",
        "llm_profile": (
            "Name of the LLM profile this style record belongs to. "
            "Matches the llm_profile key used in AMXConfig."
        ),
        "source_ref": (
            "Fully-qualified reference to the table or column set whose "
            "comments were used to derive this profile (e.g. "
            "'warehouse.sales.orders')."
        ),
        "source_db_kind": (
            "Database kind identifier of the source (e.g. 'snowflake', "
            "'duckdb', 'bigquery'). Stored for diagnostic and audit purposes."
        ),
        "profile_json": (
            "JSON-serialised StyleProfile dataclass. Deserialised by "
            "StyleProfile.from_json() on read."
        ),
        "enabled": (
            "Whether this style profile is active (1) or disabled (0). "
            "Disabled profiles are stored but not injected into prompts."
        ),
        "sample_count": (
            "Number of source description samples that were analysed to "
            "produce the profile. Used as a confidence indicator."
        ),
        "created_at": (
            "Stored as TEXT representation of a Unix timestamp float, "
            "mirroring the local SQLite schema."
        ),
        "updated_at": (
            "Stored as TEXT representation of a Unix timestamp float, "
            "mirroring the local SQLite schema."
        ),
    },
    # ── rerun_context_snapshots (local only) ──────────────────────────────
    "rerun_context_snapshots": {
        "__table__": (
            "Short-lived per-target snapshots written when a re-run job starts. "
            "Each row freezes the AgentContext for one target item so every "
            "parallel agent in the job sees identical inputs. Garbage-collected "
            "by the orchestrator when the parent job reaches a terminal state "
            "(done | failed | cancelled)."
        ),
        "snapshot_id": "Opaque text identifier for this snapshot. Primary key.",
        "job_id": (
            "Identifier of the re-run job that owns this snapshot. Indexed so "
            "the orchestrator can sweep every snapshot for a finished job in "
            "one query."
        ),
        "target_result_id": (
            "run_results.id of the row this re-run targets. The snapshot's "
            "payload_json freezes the AgentContext built around this asset."
        ),
        "payload_json": (
            "JSON-encoded AgentContext for this target — DB schema, prior "
            "descriptions, RAG citations, prompt template, settings. Read by "
            "every parallel agent in the job; never mutated after write."
        ),
        "created_at": (
            "UTC epoch seconds the snapshot was written. Indexed for the "
            "orchestrator's stale-snapshot sweep."
        ),
    },
    # ── run_context_cache (local only) ────────────────────────────────────
    "run_context_cache": {
        "__table__": (
            "Persistent table-level context built at first run and reused on "
            "subsequent re-runs to skip live profile_table introspection. "
            "Keyed on (db_profile, database, schema, table). Entries are "
            "dropped after a row's COMMENT lands on the live DB, and a 24h "
            "TTL guards against silently serving stale schema after the user "
            "altered the table out-of-band."
        ),
        "cache_key": (
            "Deterministic hash of (db_profile, database, schema, table). "
            "Primary key. Lets lookups skip a multi-column WHERE."
        ),
        "db_profile": "DB profile under which the context was gathered.",
        "database_name": (
            "Database (or catalog name on backends that distinguish them) the "
            "cached table lives in. Empty string on backends without that level."
        ),
        "schema_name": "Schema name of the cached table.",
        "table_name": "Table name being cached.",
        "payload_json": (
            "JSON-encoded table context: column types, sample values, profile "
            "histograms, FK edges, prior descriptions. Consumed by the re-run "
            "orchestrator to skip live introspection."
        ),
        "source_run_id": (
            "analysis_runs.id of the run that built this context entry. "
            "Lets /history trace where a cached payload originated."
        ),
        "created_at": "UTC epoch seconds the entry was written.",
        "expires_at": (
            "UTC epoch seconds at which the entry should be treated as "
            "stale. Indexed so the periodic eviction pass is O(log n)."
        ),
    },
    # ── column_comments_cache (local only) ────────────────────────────────
    "column_comments_cache": {
        "__table__": (
            "Per-table cache of existing COMMENT values fetched from the live "
            "DB. Avoids the per-table DESCRIBE EXTENDED loop that hit 30s+ on "
            "large Databricks warehouses. A single COMMENT write invalidates "
            "just the row that changed; TTL is the second line of defence "
            "against DBA-edited comments that AMX never sees."
        ),
        "cache_key": ("Deterministic hash of (db_profile, database, schema, table). Primary key."),
        "db_profile": "DB profile the cached comments belong to.",
        "database_name": "Database/catalog the cached table is in. Empty string on flat backends.",
        "schema_name": "Schema name of the cached table.",
        "table_name": "Table name being cached.",
        "table_comment": (
            "Existing COMMENT ON TABLE value at fetch time, or NULL if the table had no comment."
        ),
        "columns_json": (
            "JSON map of {column_name: existing_comment_or_null} captured at "
            "fetch time. Drives the missing-only filter so AMX does not "
            "re-describe assets that already have a comment."
        ),
        "kind": (
            "Asset kind on the live DB: TABLE | VIEW | MATERIALIZED_VIEW. "
            "Distinguishes how to emit COMMENT ON on apply."
        ),
        "fetched_at": "UTC epoch seconds when the snapshot was fetched.",
        "expires_at": "UTC epoch seconds at which the entry becomes stale. Indexed.",
        "bulk_filled": (
            "1 when this entry came from a bulk_schema_metadata call that, by "
            "contract, returned every table in the schema (so list_assets can "
            "read from the cache directly). 0 when it came from a per-table "
            "fallback; the cache is then incomplete for the schema."
        ),
    },
    # ── schemas_cache (local only) ────────────────────────────────────────
    "schemas_cache": {
        "__table__": (
            "Per-catalog cache of schema-level metadata (schema name + "
            "comment). Absorbs the slow per-schema DESCRIBE/pg_namespace "
            "loop the sidebar used to fire on catalog expand. Filled in one "
            "round-trip by bulk_catalog_metadata."
        ),
        "cache_key": (
            "Deterministic hash of (db_profile, database, catalog, schema). Primary key."
        ),
        "db_profile": "DB profile the cached schema belongs to.",
        "database_name": "Database name. Empty string on flat backends.",
        "catalog_name": "Catalog name on three-tier backends. Empty string elsewhere.",
        "schema_name": "Schema name being cached.",
        "schema_comment": "Existing COMMENT ON SCHEMA value, or NULL if the schema has none.",
        "bulk_filled": (
            "1 when filled by a bulk_catalog_metadata call covering the whole "
            "catalog (cache can be trusted for list_schemas). 0 when filled "
            "from a per-schema fallback."
        ),
        "fetched_at": "UTC epoch seconds the entry was fetched.",
        "expires_at": "UTC epoch seconds at which the entry becomes stale. Indexed.",
    },
    # ── catalog_entities (local only) ─────────────────────────────────────
    "catalog_entities": {
        "__table__": (
            "Search index of every database asset AMX has seen across all DB "
            "profiles. One row per (profile, database, schema, table, column?) "
            "tuple. Powers /search, /ask asset lookups, and the Studio catalog "
            "tree. Synced from the live DB via sync_profile_skeleton."
        ),
        "id": "Surrogate INT primary key. Foreign-key target for catalog_descriptions.",
        "db_profile": "DB profile this entity belongs to.",
        "db_backend": (
            "Backend of the owning DB profile (postgresql | snowflake | …). "
            "Mirrors analysis_runs.db_backend so /search filters by backend "
            "without a join."
        ),
        "database_name": "Database/catalog name. Empty string on flat backends.",
        "schema_name": "Schema name. Empty string when the entity is database-level.",
        "table_name": "Table name. Empty string when the entity is schema-level.",
        "column_name": "Column name. NULL for non-column entities (database/schema/table).",
        "entity_kind": (
            "Granularity: database | schema | table | column. Drives how "
            "/search interprets the row and which COMMENT ON variant to emit."
        ),
        "asset_kind": (
            "Specific kind when entity_kind='table': table | view | "
            "materialized_view. Empty/'table' default for non-table entities."
        ),
        "dtype": "Column data type when entity_kind='column'. Empty string otherwise.",
        "nullable": "1 if the column accepts NULLs, 0 otherwise. Meaningless for non-columns.",
        "pk_flag": "1 if the column is part of the primary key, 0 otherwise.",
        "fk_flag": "1 if the column is part of a foreign key, 0 otherwise.",
        "row_count": (
            "Approximate row count when entity_kind='table'. 0 for non-tables "
            "and for backends that don't expose the stat cheaply."
        ),
        "search_text": (
            "Synthesised search blob (path + dtype + descriptions + "
            "relationships) used as the FTS payload. Empty until first sync."
        ),
        "current_confidence": (
            "Confidence bucket of the effective description: high | medium | "
            "low. Empty when no description has been written."
        ),
        "effective_status": (
            "Lifecycle status of the effective description: pending | "
            "applied | superseded | rejected. Empty when no description exists."
        ),
        "effective_source_kind": (
            "Source of the effective description: profile | doc | code | "
            "combined | manual | manual-edit. Empty when no description exists."
        ),
        "effective_description_id": (
            "catalog_descriptions.id of the row currently treated as the "
            "effective description for this entity. NULL when there is none."
        ),
        "updated_at": "UTC epoch seconds of the last write to this row.",
        "last_synced_at": "UTC epoch seconds of the last sync_profile_skeleton touch.",
        "last_code_sync_at": (
            "UTC epoch seconds of the last code-context sync (lineage usage, "
            "test references). NULL on entities never touched by /code."
        ),
    },
    # ── catalog_descriptions (local only) ─────────────────────────────────
    "catalog_descriptions": {
        "__table__": (
            "All descriptions ever generated or imported for a catalog_entities "
            "row, with full audit trail. The 'effective' one is pointed at by "
            "catalog_entities.effective_description_id; superseded ones stay "
            "for rollback and /history."
        ),
        "id": "Surrogate INT primary key.",
        "entity_id": (
            "Foreign key to catalog_entities.id. Indexed jointly with "
            "created_at DESC for fast 'latest descriptions for this entity' "
            "lookups."
        ),
        "description_text": "The description string itself.",
        "source_kind": (
            "How the description was produced: profile | doc | code | "
            "combined | manual | manual-edit | imported."
        ),
        "source_agent": (
            "Agent identifier when source_kind is LLM-produced "
            "(e.g. 'profile-agent-v2'). Empty string for manual / imported "
            "rows."
        ),
        "confidence": ("Quality bucket: high | medium | low. Empty for manual / imported."),
        "logprob_score": (
            "Normalized log-probability of the LLM-produced tokens (-1..0). "
            "NULL for manual / imported."
        ),
        "reasoning": (
            "Optional reasoning trace from a reasoning model. Empty string "
            "for non-reasoning models."
        ),
        "run_id": (
            "analysis_runs.id of the run that produced this description. "
            "NULL for imported / manual rows."
        ),
        "result_id": (
            "run_results.id of the alternative this description came from. "
            "NULL for imported / manual rows."
        ),
        "created_at": "UTC epoch seconds when the description was created.",
        "superseded": (
            "1 once a newer description for the same entity takes over the "
            "effective slot. 0 while still candidate or effective. Lets "
            "/history hide stale rows without deleting them."
        ),
        "indexed": "1 once the description has been folded into search_text. 0 until then.",
        "indexed_at": "UTC epoch seconds of the last indexing. NULL if never indexed.",
        "applied_to_db": (
            "1 once the description has been written to the live DB via COMMENT ON. 0 otherwise."
        ),
        "applied_at": "UTC epoch seconds of the COMMENT ON write. NULL if not applied.",
        "chosen_description": (
            "1 marks the row currently treated as the effective description "
            "for the entity. At most one row per entity has this flag set."
        ),
    },
    # ── catalog_entities_fts (local only, FTS5 virtual table) ─────────────
    "catalog_entities_fts": {
        "__table__": (
            "SQLite FTS5 virtual table mirroring the search-relevant columns "
            "of catalog_entities. Rows are kept in sync by sync.py at every "
            "catalog_entities write site. content='' (contentless) so no "
            "triggers are needed. Falls back gracefully when the host SQLite "
            "lacks FTS5."
        ),
        "db_profile": (
            "DB profile the indexed entity belongs to. UNINDEXED so MATCH "
            "queries don't try to tokenize profile names."
        ),
        "column_name": "Column name of the indexed entity. Tokenized for MATCH.",
        "table_name": "Table name of the indexed entity. Tokenized for MATCH.",
        "schema_name": "Schema name of the indexed entity. Tokenized for MATCH.",
        "search_text": (
            "Synthesised search blob mirroring catalog_entities.search_text. "
            "The dominant signal source for concept-search MATCH ranking."
        ),
    },
    # ── catalog_profile_state (local only) ────────────────────────────────
    "catalog_profile_state": {
        "__table__": (
            "Per-DB-profile completeness state for the catalog skeleton sync. "
            "The cache-first read path (sidebar, schedule pickers, Ask tools) "
            "gates on state='done' so a partially-synced catalog never "
            "surfaces to the user as the full picture. Upserted by "
            "sync_profile_skeleton at start / progress / finish."
        ),
        "db_profile": "DB profile this state row tracks. Primary key.",
        "state": (
            "Sync lifecycle: none | running | done | failed. Cache-first reads only trust 'done'."
        ),
        "total_tables": "Total tables the current sync intends to touch.",
        "processed_tables": "Tables synced so far in the current sync.",
        "started_at": "UTC epoch seconds the current sync began. NULL when state='none'.",
        "finished_at": "UTC epoch seconds the current sync completed. NULL while running.",
        "last_full_sync_at": (
            "UTC epoch seconds of the last successful 'done' transition. "
            "Drives /sync recency hints."
        ),
        "last_error": (
            "Error message captured when state='failed'. Empty string for successful syncs."
        ),
    },
    # ── catalog_relationships (local only) ────────────────────────────────
    "catalog_relationships": {
        "__table__": (
            "Edges between catalog_entities rows — FK references, lineage, "
            "co-occurrence, semantic similarity. Drives 'related to' lookups "
            "in /ask and the Studio entity drawer."
        ),
        "id": "Surrogate INT primary key.",
        "from_entity_id": "Source entity. Indexed jointly with to_entity_id and relationship_type.",
        "to_entity_id": "Destination entity.",
        "relationship_type": (
            "Edge kind: fk | lineage_source | lineage_target | co_occurs | "
            "semantic_similar. Drives how /ask presents the edge in narratives."
        ),
        "score": (
            "Strength of the edge in 0..1. 1.0 for hard FKs, lower for inferred relationships."
        ),
        "source": (
            "How the edge was discovered: db_introspect | code_static | "
            "code_runtime | doc_inferred | embedding. Empty until populated."
        ),
        "details_json": (
            "JSON payload with edge-specific evidence — referenced columns "
            "for FKs, source files/lines for code edges, similarity scores "
            "for embeddings."
        ),
        "last_seen": (
            "UTC epoch seconds of the last sync that observed this edge. "
            "Edges not seen for >30 days are eligible for pruning."
        ),
    },
    # ── catalog_usage_evidence (local only) ───────────────────────────────
    "catalog_usage_evidence": {
        "__table__": (
            "Per-entity usage evidence collected from code scans, log "
            "analysis, and dashboard introspection. Drives 'used by' panels "
            "in Studio and confidence boosters in the LLM prompt."
        ),
        "id": "Surrogate INT primary key.",
        "db_profile": (
            "DB profile the evidence belongs to. Empty string for profile-agnostic evidence."
        ),
        "entity_id": (
            "catalog_entities.id this evidence row attaches to. NULL when "
            "the asset has been observed but does not (yet) exist in the "
            "catalog."
        ),
        "source_kind": (
            "Where the evidence came from: code_static | code_runtime | dashboard | log | doc."
        ),
        "evidence_type": (
            "What kind of usage: select | join | filter | groupby | aggregate | reference."
        ),
        "source_path": (
            "File path or URL where the evidence was found. Empty string when not applicable."
        ),
        "count_value": "Integer count when the evidence is quantitative (e.g. # of query references).",
        "score_value": "Numeric score 0..1 when the evidence is qualitative (e.g. relevance).",
        "sample_snippets_json": (
            "JSON list of representative snippets (e.g. SQL fragments) "
            "captured for surfacing in the UI. Defaults to [] when none."
        ),
        "last_seen": (
            "UTC epoch seconds the evidence was last observed. Old evidence "
            "is pruned by the catalog GC pass."
        ),
    },
    # ── catalog_sync_jobs (local only) ────────────────────────────────────
    "catalog_sync_jobs": {
        "__table__": (
            "Audit log of every catalog sync invocation. One row per "
            "/search sync (full or incremental) and per scheduled background "
            "sync. Powers /search status reports and post-mortems."
        ),
        "id": "Surrogate INT primary key.",
        "db_profile": "DB profile that was synced.",
        "job_type": (
            "Sync kind: full | incremental | code_sync | usage_sync. Drives "
            "how /search status interprets the counters."
        ),
        "scope_json": (
            "JSON describing the scope: schemas/tables targeted. Empty "
            "object for whole-profile syncs."
        ),
        "started_at": (
            "UTC epoch seconds the job started. Indexed jointly with "
            "db_profile DESC for fast 'latest sync per profile' lookups."
        ),
        "completed_at": "UTC epoch seconds the job finished. NULL while in flight.",
        "status": "Lifecycle: running | done | failed | cancelled.",
        "inserted_count": "Catalog rows inserted by the job.",
        "updated_count": "Catalog rows updated by the job.",
        "error_text": "Error message when status='failed'. Empty string on success.",
    },
    # ── search_settings (local only) ──────────────────────────────────────
    "search_settings": {
        "__table__": (
            "Per-DB-profile key/value store for search-related knobs "
            "(threshold weights, boost lists, opt-outs). One row per "
            "(profile, key) — settings are scoped so different profiles "
            "can be tuned independently."
        ),
        "id": "Surrogate INT primary key.",
        "db_profile": "DB profile this setting applies to.",
        "key_name": "Setting key. Uniqueness is (db_profile, key_name).",
        "value_text": "Setting value, serialized as text. Callers parse per-key.",
        "updated_at": "UTC epoch seconds of the last write.",
    },
    # ── chat_sessions (local only) ────────────────────────────────────────
    "chat_sessions": {
        "__table__": (
            "One row per /ask conversational session. Holds the session "
            "metadata; individual turns live in chat_turns joined on "
            "session_id. Read by /ask --resume and the Studio chat tab."
        ),
        "id": "Surrogate INT primary key.",
        "db_profile": "DB profile the session is bound to.",
        "llm_profile": "LLM profile used for the session.",
        "started_at": "UTC epoch seconds the session began.",
        "last_active_at": (
            "UTC epoch seconds of the most recent turn. Indexed jointly "
            "with (db_profile, llm_profile) so /ask --resume can find "
            "the most recent session per (profile, model)."
        ),
        "ended_at": "UTC epoch seconds when the session was explicitly closed. NULL while open.",
        "title": (
            "Optional human-friendly title for the session. NULL until the "
            "user (or the auto-titler) sets one."
        ),
        "turn_count": "Number of chat_turns rows attached to this session.",
        "total_tokens": (
            "Running total of LLM tokens consumed by this session — sum of "
            "chat_turns.tokens_json totals."
        ),
        "compaction_state_json": (
            "JSON state of the conversation compactor (which turns were "
            "summarised, residual context, etc.). NULL when no compaction "
            "has happened yet."
        ),
        "scope_profiles_json": (
            "JSON list of additional DB profiles the session has access "
            "to beyond db_profile. NULL on legacy single-profile sessions."
        ),
        "focus_profile": (
            "DB profile the session is currently focused on (subset of "
            "scope_profiles_json + db_profile). NULL on legacy sessions."
        ),
    },
    # ── chat_turns (local only) ───────────────────────────────────────────
    "chat_turns": {
        "__table__": (
            "Individual turns within a chat_sessions row. One row per user "
            "question + agent response cycle, plus tool-call subturns. "
            "Joined to analysis_runs when a turn triggered a /run-style "
            "agent invocation."
        ),
        "id": "Surrogate INT primary key.",
        "session_id": (
            "Foreign key to chat_sessions.id. ON DELETE CASCADE so closing "
            "a session sweeps its turns."
        ),
        "run_id": (
            "analysis_runs.id when the turn invoked an agent run. NULL for "
            "plain Q&A turns that did not touch the metadata pipeline."
        ),
        "turn_index": (
            "0-based ordinal of the turn within the session. Indexed "
            "jointly with session_id for ORDER BY turn_index."
        ),
        "role": "Speaker: user | assistant | tool.",
        "question": "Verbatim user question for role='user'. NULL otherwise.",
        "answer_summary": (
            "Short summary of the assistant response for role='assistant'. "
            "Full response is reconstructable from plan_json + tool outputs."
        ),
        "intent": (
            "Classified intent of the turn (e.g. 'describe_column', "
            "'explain_lineage', 'compare_runs'). NULL when the classifier "
            "did not run."
        ),
        "topic": (
            "Detected topic (e.g. 'orders', 'auth'). NULL when the topic tagger did not run."
        ),
        "confidence": "Confidence bucket for the intent classification: high | medium | low.",
        "tables_json": (
            "JSON list of fully-qualified table refs the turn touched. "
            "Defaults to [] when no tables were referenced."
        ),
        "columns_json": (
            "JSON list of fully-qualified column refs the turn touched. Defaults to []."
        ),
        "plan_json": (
            "JSON of the agent's plan/trace for the turn — tool calls, "
            "intermediate thoughts, citations. NULL on plain Q&A."
        ),
        "tokens_json": (
            "JSON of token usage for this turn — {prompt, completion, "
            "cached, total}. NULL when token accounting was unavailable."
        ),
        "request_id": (
            "Opaque request id correlating this turn with provider-side "
            "logs (OpenAI/Anthropic request id). NULL when unknown."
        ),
        "created_at": "UTC epoch seconds the turn was recorded.",
        "estimated_tokens": (
            "Token count estimated by the local tokenizer (used for "
            "compaction triggers). 0 until estimated."
        ),
        "compacted_at": (
            "UTC epoch seconds when the conversation compactor folded "
            "this turn into a summary. NULL while still verbatim."
        ),
    },
    # ── scheduled_runs (local only) ───────────────────────────────────────
    "scheduled_runs": {
        "__table__": (
            "One row per one-shot scheduled metadata run, created via "
            "`amx schedule` (CLI) and the Studio Schedules page. The tick "
            "engine reads pending rows whose fire_at_utc has elapsed and "
            "transitions them through the documented state machine."
        ),
        "id": "Surrogate INT primary key.",
        "name": "Human-friendly schedule name shown in /schedule list and Studio.",
        "fire_at_utc": "Canonical UTC epoch seconds when the run should fire. Indexed.",
        "fire_at_tz": (
            "IANA tz id the user picked for display and DST handling. The "
            "scheduler always uses fire_at_utc; this field is presentational."
        ),
        "status": (
            "Lifecycle: pending | firing | fired | failed | cancelled. "
            "Indexed jointly with fire_at_utc for the tick engine sweep."
        ),
        "db_profile": "DB profile the scheduled run will analyze. Indexed.",
        "database": (
            "Database/catalog overlay for the scheduled run. NULL means "
            "'use the profile's default'. Required for backends that "
            "distinguish multiple databases per profile."
        ),
        "catalog": (
            "Catalog overlay for three-tier backends (Databricks, Snowflake). "
            "NULL means 'use the profile's default'."
        ),
        "scope_json": (
            "JSON describing the high-level scope (schema/table names). "
            "Resolved against the live DB at fire time, so new tables "
            "under a scheduled schema are picked up automatically; "
            "missing entities surface as a clean failure with last_error "
            "populated."
        ),
        "llm_profile": "LLM profile to use for the run.",
        "review_strategy": "How alternatives should be evaluated: human | auto-best | confidence-threshold.",
        "extra_args_json": (
            "JSON of additional CLI flags to pass at fire time. NULL when no overrides are needed."
        ),
        "created_at": "UTC epoch seconds the schedule was created.",
        "updated_at": "UTC epoch seconds of the last edit.",
        "fired_at": "UTC epoch seconds the tick engine fired the schedule. NULL until firing.",
        "triggered_run_id": (
            "analysis_runs.id of the run created by this schedule. NULL until "
            "fired; populated even on failed runs so the audit trail is intact."
        ),
        "last_error": (
            "Error message when status='failed'. NULL on success and on "
            "schedules that have not fired yet."
        ),
    },
    # ── apply_events (local only) ─────────────────────────────────────────
    "apply_events": {
        "__table__": (
            "Audit trail of every COMMENT actually written to a live DB. "
            "One row per successful COMMENT ON. Old comments are stored "
            "verbatim so /history rollback can restore them "
            "character-for-character. Powers Studio's Recent Applies panel."
        ),
        "id": "Surrogate INT primary key.",
        "applied_at": "UTC epoch seconds the COMMENT ON was executed. Indexed DESC.",
        "run_id": (
            "analysis_runs.id of the run that owns this apply. NULL on "
            "manual /apply invocations outside a run."
        ),
        "result_id": (
            "run_results.id of the row whose chosen_description was "
            "written. NULL on manual /apply invocations."
        ),
        "profile_name": "DB profile the comment was written against.",
        "schema_name": "Schema of the asset that received the comment.",
        "table_name": "Table of the asset. Empty string for schema-level comments.",
        "column_name": "Column of the asset. NULL for table/schema-level comments.",
        "asset_kind": "Kind of asset: table | view | materialized_view | column | schema.",
        "old_comment": (
            "Previous COMMENT ON value before the overwrite, captured "
            "verbatim. NULL when the asset had no prior comment."
        ),
        "new_comment": "The comment that was written. Source of truth for /history rollback.",
        "applied_by": "OS username (or AMX_USER override) that triggered the apply.",
        "hostname": "Machine that ran the apply.",
        "sql_template": (
            "Templated COMMENT ON SQL emitted for the apply, with the "
            "comment body redacted (used for /doctor and post-mortems)."
        ),
    },
    # ── _amx_schema_descriptions (the sidecar itself) ─────────────────────
    # Self-describing: the sidecar records its own description rows too.
    "_amx_schema_descriptions": {
        "__table__": (
            "AMX's own metadata sidecar. One row per (object_kind, schema, "
            "table, column) recording the canonical description of every "
            "table and column in this database. Populated idempotently from "
            "amx.storage.schema_descriptions on every SQLiteHistoryStore "
            "init() so descriptions stay in lock-step with the schema. "
            "Equivalent to pg_description for backends without native "
            "COMMENT ON support."
        ),
        "object_kind": (
            "What this row describes: database | table | column. The three "
            "form a hierarchy reflected in the (schema_name, table_name, "
            "column_name) composite key, with empty strings standing in for "
            "the absent levels."
        ),
        "schema_name": (
            "SQLite schema name. Always 'main' for the local store; the "
            "column exists so the sidecar shape mirrors pg_description and "
            "stays portable if AMX ever uses ATTACH DATABASE."
        ),
        "table_name": (
            "Table the row describes (column rows reuse their owning table's "
            "name). Empty string for the database-level row."
        ),
        "column_name": (
            "Column the row describes. Empty string for database- and table-level rows."
        ),
        "description": (
            "Canonical description text from amx.storage.schema_descriptions. "
            "Never empty: a missing description fails CI before the row "
            "could be written."
        ),
        "updated_at": (
            "UTC epoch seconds of the last (re-)populate. Bumped on every "
            "SQLiteHistoryStore.init() so /doctor can detect descriptions "
            "that never refresh."
        ),
    },
}


__all__ = [
    "ATTRIBUTION_CREATED_BY",
    "ATTRIBUTION_HOSTNAME",
    "ATTRIBUTION_CLIENT_VERSION",
    "ATTRIBUTION_LOCAL_ID",
    "LOCAL_DATABASE_DESCRIPTION",
    "SHARED_SCHEMA_COMMENT",
    "SCHEMA_DESCRIPTIONS",
]
