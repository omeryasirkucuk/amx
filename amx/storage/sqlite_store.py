"""SQLite-backed local history store for AMX runs."""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("storage.sqlite")


class SQLiteHistoryStore:
    """Persist run history and metadata in a local SQLite database."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._lock = threading.Lock()

    def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analysis_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at REAL NOT NULL,
                    ended_at REAL,
                    duration_sec REAL,
                    status TEXT NOT NULL,
                    command TEXT NOT NULL,
                    mode TEXT,
                    db_backend TEXT,
                    db_profile TEXT,
                    llm_provider TEXT,
                    llm_model TEXT,
                    scope_json TEXT,
                    metrics_json TEXT,
                    tokens_json TEXT,
                    results_json TEXT,
                    error_text TEXT,
                    -- Reporting columns added in 0.5.2 to make /history honest
                    -- about partial / interrupted runs:
                    -- ``selected_count``: assets the user originally selected
                    -- (pre missing-only filter).
                    -- ``planned_count``: assets remaining after the missing-
                    --   only filter — what we actually intended to process.
                    -- ``processed_count``: assets that have started processing
                    --   (incremented per-table; survives Ctrl+C).
                    -- ``applied_count``: results successfully written to live
                    --   DB via apply_review_results_to_db.
                    -- ``review_strategy``: individual / deferred / auto-apply,
                    --   used by the status logic so auto-apply runs never
                    --   land in 'ready_for_review'.
                    selected_count INTEGER NOT NULL DEFAULT 0,
                    planned_count INTEGER NOT NULL DEFAULT 0,
                    processed_count INTEGER NOT NULL DEFAULT 0,
                    applied_count INTEGER NOT NULL DEFAULT 0,
                    review_strategy TEXT
                )
                """
            )
            # Migration runs on every init() AND defensively at the top
            # of create_run (see ``_ensure_run_columns``). This catches the
            # case where users upgraded AMX while ``_store`` was already
            # initialised in a long-running process, or where init() ran
            # before the upgraded code was loaded (pipx editable mode
            # quirks).
            self._ensure_run_columns(conn)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_analysis_runs_started_at "
                "ON analysis_runs(started_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL,
                    event_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    command TEXT NOT NULL,
                    details_json TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_app_events_created_at "
                "ON app_events(created_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS session_state (
                    namespace TEXT NOT NULL,
                    key_name TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (namespace, key_name)
                )
                """
            )
            # ── run_results: all alternatives produced per column per run ──────
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    saved_at REAL NOT NULL,
                    schema_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    column_name TEXT,
                    asset_kind TEXT NOT NULL DEFAULT 'table',
                    source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    logprob_score REAL,
                    raw_logprob REAL,
                    token_count INTEGER,
                    model_version TEXT NOT NULL DEFAULT '',
                    reasoning TEXT,
                    alternatives_json TEXT NOT NULL,
                    evaluated_at REAL,
                    applied_at REAL,
                    chosen_description TEXT,
                    evaluation TEXT,
                    catalog_status TEXT NOT NULL DEFAULT '',
                    catalog_indexed_at REAL,
                    db_applied_status TEXT NOT NULL DEFAULT '',
                    effective_source_kind TEXT NOT NULL DEFAULT '',
                    superseded_at REAL,
                    rejection_reason TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY (run_id) REFERENCES analysis_runs(id)
                )
                """
            )
            # Backward-compatible migration for older history DBs.
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "ALTER TABLE run_results ADD COLUMN asset_kind TEXT NOT NULL DEFAULT 'table'"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute("ALTER TABLE run_results ADD COLUMN applied_at REAL")
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute("ALTER TABLE run_results ADD COLUMN logprob_score REAL")
            for stmt in (
                "ALTER TABLE run_results ADD COLUMN raw_logprob REAL",
                "ALTER TABLE run_results ADD COLUMN token_count INTEGER",
                "ALTER TABLE run_results ADD COLUMN model_version TEXT NOT NULL DEFAULT ''",
            ):
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(stmt)
            for stmt in (
                "ALTER TABLE run_results ADD COLUMN catalog_status TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE run_results ADD COLUMN catalog_indexed_at REAL",
                "ALTER TABLE run_results ADD COLUMN db_applied_status TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE run_results ADD COLUMN effective_source_kind TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE run_results ADD COLUMN superseded_at REAL",
                "ALTER TABLE run_results ADD COLUMN rejection_reason TEXT NOT NULL DEFAULT ''",
                # 0.12.x — attribution + shared-store provenance on
                # run_results so a row pulled down from the shared
                # store via /history-store pull-from-shared can carry
                # the originating user/host through to /history show
                # and dedupe on re-pull via shared_uuid.
                "ALTER TABLE run_results ADD COLUMN created_by TEXT",
                "ALTER TABLE run_results ADD COLUMN hostname TEXT",
                "ALTER TABLE run_results ADD COLUMN shared_uuid TEXT",
                # Re-Run versioning (v0.13). ``parent_result_id`` links a
                # re-run row back to the original run_results row it was
                # spawned from; ``rerun_seq`` is 0 for originals, 1+ for
                # successive re-runs in the chain. ``user_instructions``
                # records the optional free-text addendum the user typed
                # in the re-run modal so the audit trail / history drawer
                # can show it next to the alternatives.
                "ALTER TABLE run_results ADD COLUMN parent_result_id INTEGER",
                "ALTER TABLE run_results ADD COLUMN rerun_seq INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE run_results ADD COLUMN user_instructions TEXT",
            ):
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(stmt)
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_run_results_parent "
                    "ON run_results(parent_result_id)"
                )
            # ── rerun_context_snapshots: short-lived, GC'd when the worker
            # finishes (job.done / failed / cancelled). One row per target
            # item per re-run job; payload_json is the AgentContext frozen
            # at job start so all parallel agents see identical inputs.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rerun_context_snapshots (
                    snapshot_id      TEXT PRIMARY KEY,
                    job_id           TEXT NOT NULL,
                    target_result_id INTEGER NOT NULL,
                    payload_json     TEXT NOT NULL,
                    created_at       REAL NOT NULL
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_rerun_snap_job "
                    "ON rerun_context_snapshots(job_id)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_rerun_snap_created "
                    "ON rerun_context_snapshots(created_at)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_run_results_shared_uuid "
                    "ON run_results(shared_uuid)"
                )
            # ── run_context_cache: persistent table-level context produced
            # at first run, reused on subsequent re-runs to skip live
            # ``profile_table`` introspection. Keyed on
            # (db_profile, database, schema, table) so a re-run of any
            # column on the same table can hit the same row. The cache
            # is dropped from ``_record_audit`` after the row's COMMENT
            # lands on the live database (i.e. we trust the table the
            # user just touched is no longer a re-run target). A 24h
            # TTL guards against silently serving stale schema after
            # the user altered the table out-of-band.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_context_cache (
                    cache_key       TEXT PRIMARY KEY,
                    db_profile      TEXT NOT NULL,
                    database_name   TEXT NOT NULL DEFAULT '',
                    schema_name     TEXT NOT NULL,
                    table_name      TEXT NOT NULL,
                    payload_json    TEXT NOT NULL,
                    source_run_id   INTEGER,
                    created_at      REAL NOT NULL,
                    expires_at      REAL
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_run_context_cache_table "
                    "ON run_context_cache(db_profile, database_name, schema_name, table_name)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_run_context_cache_expires "
                    "ON run_context_cache(expires_at)"
                )
            # ── column_comments_cache: per-table existing-comment cache ──
            # On large warehouses (Databricks especially) the per-table
            # DESCRIBE EXTENDED loop the sidebar and CLI inspect flows used
            # to hit became 30s+. The connector now folds the whole schema
            # into one bulk INFORMATION_SCHEMA-style query and stashes the
            # result here, keyed per-table so a single COMMENT write can
            # invalidate just the row that changed. TTL is the second line
            # of defence for DBA-edited comments that AMX never sees.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS column_comments_cache (
                    cache_key      TEXT PRIMARY KEY,
                    db_profile     TEXT NOT NULL,
                    database_name  TEXT NOT NULL DEFAULT '',
                    schema_name    TEXT NOT NULL,
                    table_name     TEXT NOT NULL,
                    table_comment  TEXT,
                    columns_json   TEXT NOT NULL,
                    kind           TEXT NOT NULL DEFAULT 'TABLE',
                    fetched_at     REAL NOT NULL,
                    expires_at     REAL NOT NULL
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ccc_profile_schema "
                    "ON column_comments_cache(db_profile, database_name, schema_name)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ccc_expires "
                    "ON column_comments_cache(expires_at)"
                )
            # ``bulk_filled`` differentiates two write paths:
            #   - ``1`` — entry came from a successful
            #     ``adapter.bulk_schema_metadata`` call, which by
            #     contract returns EVERY table in the schema. Presence
            #     of any such row for a schema means the cache covers
            #     the whole schema and ``list_assets`` can read from it
            #     directly without re-issuing SHOW TABLES.
            #   - ``0`` — entry came from the per-table inspector
            #     fallback. The schema may have other uncached tables;
            #     ``list_assets`` must NOT trust this state.
            # Added as a migration so existing histories pick it up.
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "ALTER TABLE column_comments_cache "
                    "ADD COLUMN bulk_filled INTEGER NOT NULL DEFAULT 0"
                )
            # ── schemas_cache: per-catalog schema-level metadata ──
            # Catalog expand in the sidebar fires ``list_schemas`` (one
            # query — fast) and then ``get_schema_comment`` per schema
            # (DESCRIBE SCHEMA / pg_namespace lookup — slow loop).
            # This table absorbs both: a single ``bulk_catalog_metadata``
            # query fills schema names + comments for the whole catalog
            # in one round-trip, and the result lives here under a
            # ``(profile, database, catalog)`` scope. The freshness
            # marker on individual rows mirrors the column cache.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schemas_cache (
                    cache_key       TEXT PRIMARY KEY,
                    db_profile      TEXT NOT NULL,
                    database_name   TEXT NOT NULL DEFAULT '',
                    catalog_name    TEXT NOT NULL DEFAULT '',
                    schema_name     TEXT NOT NULL,
                    schema_comment  TEXT,
                    bulk_filled     INTEGER NOT NULL DEFAULT 0,
                    fetched_at      REAL NOT NULL,
                    expires_at      REAL NOT NULL
                )
                """
            )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_sc_profile_catalog "
                    "ON schemas_cache(db_profile, database_name, catalog_name)"
                )
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_sc_expires ON schemas_cache(expires_at)"
                )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_run_results_run_id ON run_results(run_id)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_results_asset "
                "ON run_results(schema_name, table_name, column_name)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_profile TEXT NOT NULL,
                    db_backend TEXT NOT NULL DEFAULT '',
                    database_name TEXT NOT NULL DEFAULT '',
                    schema_name TEXT NOT NULL DEFAULT '',
                    table_name TEXT NOT NULL DEFAULT '',
                    column_name TEXT,
                    entity_kind TEXT NOT NULL,
                    asset_kind TEXT NOT NULL DEFAULT 'table',
                    dtype TEXT NOT NULL DEFAULT '',
                    nullable INTEGER NOT NULL DEFAULT 1,
                    pk_flag INTEGER NOT NULL DEFAULT 0,
                    fk_flag INTEGER NOT NULL DEFAULT 0,
                    row_count INTEGER NOT NULL DEFAULT 0,
                    search_text TEXT NOT NULL DEFAULT '',
                    current_confidence TEXT NOT NULL DEFAULT '',
                    effective_status TEXT NOT NULL DEFAULT '',
                    effective_source_kind TEXT NOT NULL DEFAULT '',
                    effective_description_id INTEGER,
                    updated_at REAL NOT NULL DEFAULT 0,
                    last_synced_at REAL NOT NULL DEFAULT 0,
                    last_code_sync_at REAL
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_entities_identity
                ON catalog_entities(db_profile, schema_name, table_name, COALESCE(column_name, ''), entity_kind)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_descriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id INTEGER NOT NULL,
                    description_text TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_agent TEXT NOT NULL DEFAULT '',
                    confidence TEXT NOT NULL DEFAULT '',
                    logprob_score REAL,
                    reasoning TEXT NOT NULL DEFAULT '',
                    run_id INTEGER,
                    result_id INTEGER,
                    created_at REAL NOT NULL,
                    superseded INTEGER NOT NULL DEFAULT 0,
                    indexed INTEGER NOT NULL DEFAULT 0,
                    indexed_at REAL,
                    applied_to_db INTEGER NOT NULL DEFAULT 0,
                    applied_at REAL,
                    chosen_description INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (entity_id) REFERENCES catalog_entities(id),
                    FOREIGN KEY (run_id) REFERENCES analysis_runs(id),
                    FOREIGN KEY (result_id) REFERENCES run_results(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_catalog_descriptions_entity_id ON catalog_descriptions(entity_id, created_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_relationships (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_entity_id INTEGER NOT NULL,
                    to_entity_id INTEGER NOT NULL,
                    relationship_type TEXT NOT NULL,
                    score REAL NOT NULL DEFAULT 0,
                    source TEXT NOT NULL DEFAULT '',
                    details_json TEXT NOT NULL DEFAULT '{}',
                    last_seen REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY (from_entity_id) REFERENCES catalog_entities(id),
                    FOREIGN KEY (to_entity_id) REFERENCES catalog_entities(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_catalog_relationships_from_to ON catalog_relationships(from_entity_id, to_entity_id, relationship_type)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_usage_evidence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_profile TEXT NOT NULL DEFAULT '',
                    entity_id INTEGER,
                    source_kind TEXT NOT NULL,
                    evidence_type TEXT NOT NULL,
                    source_path TEXT NOT NULL DEFAULT '',
                    count_value INTEGER NOT NULL DEFAULT 0,
                    score_value REAL NOT NULL DEFAULT 0,
                    sample_snippets_json TEXT NOT NULL DEFAULT '[]',
                    last_seen REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY (entity_id) REFERENCES catalog_entities(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_catalog_usage_entity ON catalog_usage_evidence(db_profile, entity_id, source_kind, evidence_type)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_sync_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_profile TEXT NOT NULL,
                    job_type TEXT NOT NULL,
                    scope_json TEXT NOT NULL DEFAULT '{}',
                    started_at REAL NOT NULL,
                    completed_at REAL,
                    status TEXT NOT NULL,
                    inserted_count INTEGER NOT NULL DEFAULT 0,
                    updated_count INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_catalog_sync_jobs_profile_started ON catalog_sync_jobs(db_profile, started_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS search_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_profile TEXT NOT NULL,
                    key_name TEXT NOT NULL,
                    value_text TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_search_settings_profile_key ON search_settings(db_profile, key_name)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    db_profile TEXT NOT NULL,
                    llm_profile TEXT NOT NULL,
                    started_at REAL NOT NULL,
                    last_active_at REAL NOT NULL,
                    ended_at REAL,
                    title TEXT,
                    turn_count INTEGER NOT NULL DEFAULT 0,
                    total_tokens INTEGER NOT NULL DEFAULT 0,
                    compaction_state_json TEXT,
                    scope_profiles_json TEXT,
                    focus_profile TEXT
                )
                """
            )
            # Backwards-compatible migration for older history DBs created
            # before multi-profile ask shipped (PR ask-multi-profile-A).
            for stmt in (
                "ALTER TABLE chat_sessions ADD COLUMN scope_profiles_json TEXT",
                "ALTER TABLE chat_sessions ADD COLUMN focus_profile TEXT",
            ):
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(stmt)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_chat_sessions_profile_active "
                "ON chat_sessions(db_profile, llm_profile, last_active_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_turns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER NOT NULL,
                    run_id INTEGER,
                    turn_index INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    question TEXT,
                    answer_summary TEXT,
                    intent TEXT,
                    topic TEXT,
                    confidence TEXT,
                    tables_json TEXT NOT NULL DEFAULT '[]',
                    columns_json TEXT NOT NULL DEFAULT '[]',
                    plan_json TEXT,
                    tokens_json TEXT,
                    request_id TEXT,
                    created_at REAL NOT NULL,
                    estimated_tokens INTEGER NOT NULL DEFAULT 0,
                    compacted_at REAL,
                    FOREIGN KEY (session_id) REFERENCES chat_sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (run_id) REFERENCES analysis_runs(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_chat_turns_session_index "
                "ON chat_turns(session_id, turn_index)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_turns_run ON chat_turns(run_id)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS style_profiles (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    llm_profile     TEXT    NOT NULL,
                    source_ref      TEXT    NOT NULL,
                    source_db_kind  TEXT    NOT NULL,
                    profile_json    TEXT    NOT NULL,
                    enabled         INTEGER NOT NULL DEFAULT 1,
                    sample_count    INTEGER NOT NULL,
                    created_at      TEXT    NOT NULL,
                    updated_at      TEXT    NOT NULL,
                    UNIQUE (llm_profile)
                )
                """
            )

    def _ensure_run_columns(self, conn: Any) -> None:
        """Idempotently add the v0.5.2 reporting columns to analysis_runs.

        Called from both ``init()`` (the normal path) and the top of
        ``create_run`` (safety net for users who upgraded AMX without a
        clean restart, or whose init() ran on stale code under a pipx
        editable install). Each ALTER is wrapped so per-column failures
        are isolated and visible in the logs.
        """
        try:
            rows = conn.execute("PRAGMA table_info(analysis_runs)").fetchall()
            existing_cols = {str(r[1]) for r in rows}
        except Exception as exc:
            log.warning("Could not introspect analysis_runs schema: %s", exc)
            existing_cols = set()
        for col_name, col_type in (
            ("selected_count", "INTEGER NOT NULL DEFAULT 0"),
            ("planned_count", "INTEGER NOT NULL DEFAULT 0"),
            ("processed_count", "INTEGER NOT NULL DEFAULT 0"),
            ("applied_count", "INTEGER NOT NULL DEFAULT 0"),
            ("review_strategy", "TEXT"),
            # 0.11.x — profile names captured for /compare so runs that
            # differ only by LLM/doc/code profile can be told apart.
            # Older rows stay NULL and render as '—' in comparisons.
            ("llm_profile", "TEXT"),
            ("doc_profile", "TEXT"),
            ("code_profile", "TEXT"),
            # 0.11.x — full LLM/run config snapshot at run-start time
            # (prompt_detail, language, column_batch_size, n_alternatives,
            # completion_mode, description_verbosity, temperature,
            # batch_context_column_names, dedup_used, missing_only).
            # Stored as JSON so future settings can be added without
            # another schema migration. /history compare surfaces these
            # so users can see exactly which knobs varied between runs.
            ("settings_json", "TEXT"),
            # 0.12.x — attribution + shared-store provenance. Populated
            # for runs created on this machine (so /history list can
            # render "by alice@laptop-A" once shared mode is on) AND
            # for runs pulled down from the team's shared store via
            # /history-store pull-from-shared. ``shared_uuid`` is NULL
            # for runs created locally; for pulled rows it's the UUID
            # PK of the corresponding shared row, so re-running pull
            # is idempotent (we look up by shared_uuid before inserting).
            ("created_by", "TEXT"),
            ("hostname", "TEXT"),
            ("client_version", "TEXT"),
            ("shared_uuid", "TEXT"),
        ):
            if col_name in existing_cols:
                continue
            try:
                conn.execute(f"ALTER TABLE analysis_runs ADD COLUMN {col_name} {col_type}")
                log.info(
                    "Migrated analysis_runs: added column %s %s",
                    col_name,
                    col_type,
                )
            except Exception as exc:
                log.warning(
                    "Could not add analysis_runs.%s: %s  --  partial-progress "
                    "reporting in /history will show '—'.",
                    col_name,
                    exc,
                )
        # Index for the dedup lookup on pull-from-shared. SQLite skips
        # creation when it already exists. (run_results indexes live
        # next to its CREATE TABLE in init().)
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_analysis_runs_shared_uuid "
                "ON analysis_runs(shared_uuid)"
            )
        # ── apply_events: audit trail of every COMMENT actually written ──
        #
        # ``run_results.applied_at`` already says "this row was applied"
        # but cannot answer "what was the comment before we overwrote
        # it" or "who applied it on which host". The apply_events table
        # records one row per successful COMMENT write so /history
        # rollback (PR-12b) and Studio's Recent Applies panel (PR-12c)
        # have a stable replay log. Old comments are stored verbatim so
        # rollback can restore them character-for-character.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS apply_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                applied_at REAL NOT NULL,
                run_id INTEGER,
                result_id INTEGER,
                profile_name TEXT NOT NULL DEFAULT '',
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL DEFAULT '',
                column_name TEXT,
                asset_kind TEXT NOT NULL DEFAULT 'table',
                old_comment TEXT,
                new_comment TEXT NOT NULL,
                applied_by TEXT NOT NULL DEFAULT '',
                hostname TEXT NOT NULL DEFAULT '',
                sql_template TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (run_id) REFERENCES analysis_runs(id),
                FOREIGN KEY (result_id) REFERENCES run_results(id)
            )
            """
        )
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_apply_events_applied_at "
                "ON apply_events(applied_at DESC)"
            )
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_apply_events_run_id ON apply_events(run_id)"
            )
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_apply_events_asset "
                "ON apply_events(profile_name, schema_name, table_name, column_name)"
            )

    def create_run(
        self,
        *,
        command: str,
        mode: str,
        db_backend: str,
        db_profile: str,
        llm_provider: str,
        llm_model: str,
        scope: dict[str, list[str]],
        selected_count: int = 0,
        planned_count: int = 0,
        review_strategy: str | None = None,
        llm_profile: str | None = None,
        doc_profile: str | None = None,
        code_profile: str | None = None,
        settings: dict[str, Any] | None = None,
    ) -> int:
        started = time.time()
        # Sensible defaults if caller didn't pass counts explicitly: derive
        # from the scope dict (unique asset count across all schemas).
        if selected_count <= 0:
            try:
                selected_count = sum(len(v or []) for v in (scope or {}).values())
            except Exception:
                selected_count = 0
        if planned_count <= 0:
            planned_count = selected_count
        with self._lock, self._connect() as conn:
            # Belt-and-suspenders: ensure the v0.5.2 reporting columns
            # exist before we try to INSERT into them. Without this, a user
            # whose ``init()`` somehow ran on stale code (pipx editable mode
            # quirks, in-process upgrade) would silently fail the INSERT
            # and end up with /history showing 'Processed: —' forever.
            self._ensure_run_columns(conn)
            # Recover stale rows left as 'running' after an unclean shutdown/crash.
            conn.execute(
                """
                UPDATE analysis_runs
                SET ended_at = ?,
                    duration_sec = CASE
                        WHEN started_at IS NOT NULL THEN MAX(0.0, ? - started_at)
                        ELSE 0.0
                    END,
                    status = 'failed',
                    error_text = CASE
                        WHEN error_text IS NULL OR error_text = ''
                        THEN 'Recovered stale running run during new run start'
                        ELSE error_text
                    END
                WHERE status = 'running'
                  AND ended_at IS NULL
                """,
                (started, started),
            )
            cur = conn.execute(
                """
                INSERT INTO analysis_runs (
                    started_at, status, command, mode,
                    db_backend, db_profile, llm_provider, llm_model, scope_json,
                    selected_count, planned_count, processed_count, applied_count,
                    review_strategy,
                    llm_profile, doc_profile, code_profile,
                    settings_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?)
                """,
                (
                    started,
                    "running",
                    command,
                    mode,
                    db_backend,
                    db_profile,
                    llm_provider,
                    llm_model,
                    json.dumps(scope, ensure_ascii=True),
                    int(selected_count),
                    int(planned_count),
                    str(review_strategy or ""),
                    (llm_profile or None),
                    (doc_profile or None),
                    (code_profile or None),
                    (json.dumps(settings, ensure_ascii=True) if settings else None),
                ),
            )
            return int(cur.lastrowid)

    def update_run_planned_count(self, run_id: int, planned_count: int) -> None:
        """Set planned_count after the missing-only filter has dropped already-commented assets."""
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE analysis_runs SET planned_count = ? WHERE id = ?",
                (int(planned_count), int(run_id)),
            )

    def increment_run_processed(self, run_id: int, by: int = 1) -> None:
        """Bump processed_count for one (or more) tables that started processing."""
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE analysis_runs SET processed_count = COALESCE(processed_count, 0) + ? WHERE id = ?",
                (int(by), int(run_id)),
            )

    def increment_run_applied(self, run_id: int, by: int = 1) -> None:
        """Bump applied_count for results successfully written to the live DB."""
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE analysis_runs SET applied_count = COALESCE(applied_count, 0) + ? WHERE id = ?",
                (int(by), int(run_id)),
            )

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        metrics: dict[str, Any],
        tokens: dict[str, Any],
        results: dict[str, Any],
        error_text: str = "",
    ) -> None:
        ended = time.time()
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT started_at FROM analysis_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            started = float(row[0]) if row and row[0] is not None else ended
            conn.execute(
                """
                UPDATE analysis_runs
                SET ended_at = ?,
                    duration_sec = ?,
                    status = ?,
                    metrics_json = ?,
                    tokens_json = ?,
                    results_json = ?,
                    error_text = ?
                WHERE id = ?
                """,
                (
                    ended,
                    max(0.0, ended - started),
                    status,
                    json.dumps(metrics, ensure_ascii=True),
                    json.dumps(tokens, ensure_ascii=True),
                    json.dumps(results, ensure_ascii=True),
                    error_text[:4000],
                    run_id,
                ),
            )

    # ── run_results helpers ────────────────────────────────────────────────

    def save_run_results(
        self,
        run_id: int,
        suggestions: list[dict[str, Any]],
    ) -> list[int]:
        """Persist all LLM alternatives produced for a run before human review.

        Each *suggestion* dict should contain:
          schema, table, column (or None), asset_kind, source, confidence,
          reasoning, alternatives (list[str])

        Optional re-run fields:
          parent_result_id (int | None) — original run_results.id this row
            re-runs; ``rerun_seq`` (int, default 0) — versioned position in
            the chain (0 = original, 1+ = ordered re-runs);
          ``user_instructions`` (str | None) — free-text addendum the user
            typed in the re-run modal.

        Returns the inserted row IDs.
        """
        now = time.time()
        ids: list[int] = []
        with self._lock, self._connect() as conn:
            for s in suggestions:
                cur = conn.execute(
                    """
                    INSERT INTO run_results (
                        run_id, saved_at, schema_name, table_name, column_name,
                        asset_kind, source, confidence, logprob_score, raw_logprob,
                        token_count, model_version, reasoning, alternatives_json,
                        parent_result_id, rerun_seq, user_instructions
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        now,
                        s.get("schema", ""),
                        s.get("table", ""),
                        s.get("column"),
                        s.get("asset_kind", "table"),
                        s.get("source", "unknown"),
                        s.get("confidence", "medium"),
                        s.get("logprob_score"),
                        s.get("raw_logprob", s.get("logprob_score")),
                        s.get("token_count"),
                        s.get("model_version", ""),
                        s.get("reasoning", ""),
                        json.dumps(s.get("alternatives", []), ensure_ascii=True),
                        s.get("parent_result_id"),
                        int(s.get("rerun_seq", 0) or 0),
                        s.get("user_instructions"),
                    ),
                )
                ids.append(int(cur.lastrowid))
        return ids

    def set_session_state(self, namespace: str, key: str, value: Any) -> None:
        """Write-through session/agent state storage."""
        payload = json.dumps(value, ensure_ascii=True)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO session_state (namespace, key_name, value_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(namespace, key_name) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (namespace, key, payload, time.time()),
            )

    def get_session_state(self, namespace: str, key: str, default: Any = None) -> Any:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value_json FROM session_state WHERE namespace = ? AND key_name = ?",
                (namespace, key),
            ).fetchone()
        if not row:
            return default
        try:
            return json.loads(str(row["value_json"] or ""))
        except Exception:
            return default

    def record_evaluation(
        self,
        result_id: int,
        *,
        chosen_description: str,
        evaluation: str,  # 'accepted' | 'skipped' | 'custom'
    ) -> None:
        """Record the user's evaluation decision for one run_result row."""
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE run_results
                SET evaluated_at = ?,
                    chosen_description = ?,
                    evaluation = ?
                WHERE id = ?
                """,
                (now, chosen_description, evaluation, result_id),
            )

    def record_applied(self, result_id: int) -> None:
        """Record when a reviewed description was successfully applied to DB."""
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE run_results
                SET applied_at = ?,
                    db_applied_status = 'applied',
                    rejection_reason = ''
                WHERE id = ?
                """,
                (now, result_id),
            )

    def record_db_apply_failure(self, result_id: int, error_text: str = "") -> None:
        """Record when a reviewed description failed during DB write-back."""
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE run_results
                SET db_applied_status = 'failed',
                    rejection_reason = CASE
                        WHEN ? != '' THEN ?
                        ELSE rejection_reason
                    END
                WHERE id = ?
                """,
                (error_text, error_text, result_id),
            )

    def record_apply_event(
        self,
        *,
        schema_name: str,
        new_comment: str,
        run_id: int | None = None,
        result_id: int | None = None,
        profile_name: str = "",
        table_name: str = "",
        column_name: str | None = None,
        asset_kind: str = "table",
        old_comment: str | None = None,
        applied_by: str = "",
        hostname: str = "",
        sql_template: str = "",
    ) -> int:
        """Append one ``apply_events`` row for a successful COMMENT write.

        ``new_comment`` is the comment text actually written to the
        database. ``old_comment`` (when supplied) lets a future
        rollback step restore the prior state byte-for-byte. Every
        other field is optional so callers that don't yet propagate
        full attribution can still record a basic audit trail.

        Returns the inserted row id so callers (e.g. Studio SSE) can
        link a UI event back to the audit row.
        """
        now = time.time()
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO apply_events (
                    applied_at, run_id, result_id, profile_name,
                    schema_name, table_name, column_name, asset_kind,
                    old_comment, new_comment, applied_by, hostname,
                    sql_template
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    run_id,
                    result_id,
                    profile_name,
                    schema_name,
                    table_name,
                    column_name,
                    asset_kind,
                    old_comment,
                    new_comment,
                    applied_by,
                    hostname,
                    sql_template,
                ),
            )
            return int(cursor.lastrowid or 0)

    def list_apply_events(
        self,
        *,
        run_id: int | None = None,
        profile_name: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return apply events newest-first, optionally filtered by run / profile.

        Used by ``/history rollback`` (PR-12b) to find the events to
        replay in reverse, and by Studio's Recent Applies panel
        (PR-12c) to render the timeline.
        """
        clauses: list[str] = []
        params: list[Any] = []
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if profile_name is not None:
            clauses.append("profile_name = ?")
            params.append(profile_name)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            "SELECT id, applied_at, run_id, result_id, profile_name, "
            "schema_name, table_name, column_name, asset_kind, "
            "old_comment, new_comment, applied_by, hostname, sql_template "
            "FROM apply_events" + where + " ORDER BY applied_at DESC LIMIT ?"
        )
        params.append(int(limit))
        with self._lock, self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [
            {
                "id": int(r[0]),
                "applied_at": float(r[1]),
                "run_id": r[2],
                "result_id": r[3],
                "profile_name": str(r[4]),
                "schema_name": str(r[5]),
                "table_name": str(r[6]),
                "column_name": r[7],
                "asset_kind": str(r[8]),
                "old_comment": r[9],
                "new_comment": str(r[10]),
                "applied_by": str(r[11]),
                "hostname": str(r[12]),
                "sql_template": str(r[13]),
            }
            for r in rows
        ]

    def update_run_status(self, run_id: int, status: str, error_text: str = "") -> None:
        """Update run status without overwriting metrics/tokens/results payloads."""
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE analysis_runs
                SET status = ?,
                    error_text = CASE
                        WHEN ? = 'success' THEN ''
                        WHEN ? != '' THEN ?
                        ELSE error_text
                    END
                WHERE id = ?
                """,
                (status, status, error_text, error_text, int(run_id)),
            )

    def get_run_results(
        self,
        run_id: int,
        *,
        unevaluated_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Return all (or unevaluated) result rows for a given run."""
        query = "SELECT * FROM run_results WHERE run_id = ?"
        if unevaluated_only:
            query += " AND (evaluation IS NULL OR evaluation = '')"
        query += " ORDER BY id"
        with self._connect() as conn:
            rows = conn.execute(query, (int(run_id),)).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            raw = d.get("alternatives_json")
            if isinstance(raw, str) and raw:
                with contextlib.suppress(Exception):
                    d["alternatives_json"] = json.loads(raw)
            out.append(d)
        return out

    # ── Re-Run helpers ─────────────────────────────────────────────────────

    def get_run_result(self, result_id: int) -> dict[str, Any] | None:
        """Return one ``run_results`` row by id, alternatives parsed."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM run_results WHERE id = ?",
                (int(result_id),),
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        raw = d.get("alternatives_json")
        if isinstance(raw, str) and raw:
            with contextlib.suppress(Exception):
                d["alternatives_json"] = json.loads(raw)
        return d

    def get_result_chain(self, result_id: int) -> list[dict[str, Any]]:
        """Return the full version chain (original + all re-runs) for an item.

        Walks ``parent_result_id`` upward to find the chain root, then
        fetches every row whose ``parent_result_id`` matches that root
        (plus the root itself), ordered by ``rerun_seq`` ASC. Used by
        the Studio history drawer + ``GET /api/history/runs/...?include_history=true``.
        """
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, parent_result_id FROM run_results WHERE id = ?",
                (int(result_id),),
            ).fetchone()
            if row is None:
                return []
            root = int(row["id"])
            seen: set[int] = set()
            while True:
                if root in seen:
                    break
                seen.add(root)
                parent_row = conn.execute(
                    "SELECT parent_result_id FROM run_results WHERE id = ?",
                    (root,),
                ).fetchone()
                parent = (
                    int(parent_row["parent_result_id"])
                    if parent_row and parent_row["parent_result_id"] is not None
                    else None
                )
                if parent is None or parent == root:
                    break
                root = parent
            chain_rows = conn.execute(
                """
                SELECT * FROM run_results
                WHERE id = ? OR parent_result_id = ?
                ORDER BY rerun_seq ASC, id ASC
                """,
                (root, root),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in chain_rows:
            d = dict(r)
            raw = d.get("alternatives_json")
            if isinstance(raw, str) and raw:
                with contextlib.suppress(Exception):
                    d["alternatives_json"] = json.loads(raw)
            out.append(d)
        return out

    def next_rerun_seq(self, parent_result_id: int) -> int:
        """Return the next ``rerun_seq`` to use for a re-run targeting this item.

        Looks at the chain root (parent_result_id, which already points at
        the original) and returns ``max(rerun_seq) + 1`` so concurrent
        re-runs receive monotonically increasing sequence numbers.
        """
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(MAX(rerun_seq), 0) AS mx
                FROM run_results
                WHERE id = ? OR parent_result_id = ?
                """,
                (int(parent_result_id), int(parent_result_id)),
            ).fetchone()
        return int((row["mx"] if row else 0) or 0) + 1

    def save_rerun_snapshot(
        self,
        *,
        snapshot_id: str,
        job_id: str,
        target_result_id: int,
        payload: dict[str, Any],
    ) -> None:
        """Persist a frozen ``AgentContext`` for the re-run worker to read.

        Snapshots are short-lived: the worker deletes them in its
        ``finally`` block once the job terminates (done / failed /
        cancelled), and ``gc_orphan_rerun_snapshots`` sweeps anything
        left over from a crash on next process startup.
        """
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO rerun_context_snapshots
                    (snapshot_id, job_id, target_result_id, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(snapshot_id),
                    str(job_id),
                    int(target_result_id),
                    json.dumps(payload, ensure_ascii=True),
                    time.time(),
                ),
            )

    def read_rerun_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        """Return the deserialized snapshot payload, or ``None`` when missing."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json, target_result_id, job_id "
                "FROM rerun_context_snapshots WHERE snapshot_id = ?",
                (str(snapshot_id),),
            ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["payload_json"])
        except Exception:
            return None
        return {
            "snapshot_id": str(snapshot_id),
            "job_id": str(row["job_id"]),
            "target_result_id": int(row["target_result_id"]),
            "payload": payload,
        }

    def list_rerun_snapshots_for_job(self, job_id: str) -> list[dict[str, Any]]:
        """Return all snapshot rows for one job (ordered by created_at)."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT snapshot_id, target_result_id, payload_json, created_at
                FROM rerun_context_snapshots
                WHERE job_id = ?
                ORDER BY created_at ASC
                """,
                (str(job_id),),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            try:
                payload = json.loads(r["payload_json"])
            except Exception:
                continue
            out.append(
                {
                    "snapshot_id": str(r["snapshot_id"]),
                    "target_result_id": int(r["target_result_id"]),
                    "payload": payload,
                    "created_at": float(r["created_at"]),
                }
            )
        return out

    def delete_rerun_snapshots_for_job(self, job_id: str) -> int:
        """Drop every snapshot row owned by a finished job. Returns count."""
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM rerun_context_snapshots WHERE job_id = ?",
                (str(job_id),),
            )
            return int(cur.rowcount or 0)

    def gc_orphan_rerun_snapshots(self, *, max_age_seconds: float = 3600.0) -> int:
        """Sweep snapshots older than ``max_age_seconds`` (default 1h).

        Called once at AMX Studio startup (and at CLI bootstrap) so
        any rows left behind by a crashed worker don't accumulate.
        Returns the number of rows deleted.
        """
        cutoff = time.time() - float(max_age_seconds)
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM rerun_context_snapshots WHERE created_at < ?",
                (cutoff,),
            )
            return int(cur.rowcount or 0)

    # ── run_context_cache: first-run table profiles reused on rerun ──

    @staticmethod
    def _context_cache_key(
        *,
        db_profile: str,
        database: str,
        schema: str,
        table: str,
    ) -> str:
        return f"{db_profile}|{database or ''}|{schema}|{table}"

    def save_run_context_cache(
        self,
        *,
        db_profile: str,
        database: str,
        schema: str,
        table: str,
        payload: dict[str, Any],
        source_run_id: int | None = None,
        ttl_seconds: float = 86400.0,
    ) -> None:
        """Persist a table-level context snapshot for re-use on re-run.

        ``payload`` is the JSON-serialisable dict the rerun executor
        normally rebuilds via ``_build_db_profile_dict`` — keys at
        minimum: ``db_profile`` (the column-aware profile dict) and
        ``existing_metadata``.  ``ttl_seconds`` defaults to 24 hours so
        a stale schema can't silently produce wrong descriptions when
        the user re-runs a week later.

        Uses ``INSERT OR REPLACE`` keyed on
        (db_profile, database, schema, table) so a re-analyze of the
        same table refreshes the cache rather than appending duplicates.
        """
        cache_key = self._context_cache_key(
            db_profile=str(db_profile or ""),
            database=str(database or ""),
            schema=str(schema or ""),
            table=str(table or ""),
        )
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO run_context_cache
                    (cache_key, db_profile, database_name, schema_name, table_name,
                     payload_json, source_run_id, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    source_run_id = excluded.source_run_id,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at
                """,
                (
                    cache_key,
                    str(db_profile or ""),
                    str(database or ""),
                    str(schema or ""),
                    str(table or ""),
                    json.dumps(payload, ensure_ascii=True),
                    int(source_run_id) if source_run_id is not None else None,
                    now,
                    (now + float(ttl_seconds)) if ttl_seconds > 0 else None,
                ),
            )

    def lookup_run_context_cache(
        self,
        *,
        db_profile: str,
        database: str,
        schema: str,
        table: str,
    ) -> dict[str, Any] | None:
        """Return the cached payload for a table, or ``None`` if missing/expired.

        Expired rows are kept on disk (cheaper than rewriting) but the
        lookup pretends they're absent so callers always rebuild from
        the live database. ``gc_run_context_cache`` reaps them.
        """
        cache_key = self._context_cache_key(
            db_profile=str(db_profile or ""),
            database=str(database or ""),
            schema=str(schema or ""),
            table=str(table or ""),
        )
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT payload_json, expires_at, source_run_id, created_at
                FROM run_context_cache
                WHERE cache_key = ?
                """,
                (cache_key,),
            ).fetchone()
        if row is None:
            return None
        expires_at = row["expires_at"]
        if expires_at is not None and float(expires_at) < time.time():
            return None
        try:
            payload = json.loads(row["payload_json"])
        except Exception:
            return None
        return {
            "payload": payload,
            "source_run_id": row["source_run_id"],
            "created_at": float(row["created_at"]),
        }

    def delete_run_context_cache(
        self,
        *,
        db_profile: str,
        database: str,
        schema: str,
        table: str,
    ) -> int:
        """Drop the cache row for a single table; returns rowcount.

        Called from the apply path after a successful COMMENT write so
        we don't keep stale-but-valid context around for a row the
        user has already accepted.
        """
        cache_key = self._context_cache_key(
            db_profile=str(db_profile or ""),
            database=str(database or ""),
            schema=str(schema or ""),
            table=str(table or ""),
        )
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM run_context_cache WHERE cache_key = ?",
                (cache_key,),
            )
            return int(cur.rowcount or 0)

    def gc_run_context_cache(self) -> int:
        """Sweep cache rows past their TTL; called at process startup."""
        now = time.time()
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM run_context_cache WHERE expires_at IS NOT NULL AND expires_at < ?",
                (now,),
            )
            return int(cur.rowcount or 0)

    # ── column_comments_cache: per-table existing-comment cache ──

    @staticmethod
    def _ccc_key(*, db_profile: str, database: str, schema: str, table: str) -> str:
        return f"{db_profile}|{database or ''}|{schema}|{table}"

    def save_column_comments_cache(
        self,
        *,
        db_profile: str,
        database: str,
        schema: str,
        entries: dict[str, dict[str, Any]],
        ttl_seconds: float = 3600.0,
        bulk_filled: bool = False,
    ) -> int:
        """Bulk upsert per-table entries after one ``bulk_schema_metadata`` call.

        ``entries`` maps each ``table_name`` to a dict with keys:
        ``table_comment`` (str | None), ``columns`` (dict[col_name, comment_or_none]),
        ``kind`` ("TABLE" | "VIEW" | "MATERIALIZED VIEW"). Missing keys default
        to ``None`` / empty / "TABLE" so callers can pass partial payloads
        when the backend only returns column-level data.

        ``bulk_filled`` records *how* the entries arrived: ``True`` for a
        successful bulk-adapter call (the dict covers every table in the
        schema by contract), ``False`` for per-table fallback writes. The
        flag is what lets ``list_assets`` know whether the cache is safe
        to read instead of re-issuing SHOW TABLES.
        """
        if not entries:
            return 0
        now = time.time()
        # ``ttl_seconds == 0`` defaults to one hour to match the helper's
        # default kwarg; negative values are honoured verbatim so tests
        # can stamp rows as already-expired.
        if ttl_seconds == 0:
            ttl_seconds = 3600.0
        expires_at = now + float(ttl_seconds)
        flag = 1 if bulk_filled else 0
        rows = [
            (
                self._ccc_key(
                    db_profile=str(db_profile or ""),
                    database=str(database or ""),
                    schema=str(schema or ""),
                    table=str(table or ""),
                ),
                str(db_profile or ""),
                str(database or ""),
                str(schema or ""),
                str(table or ""),
                payload.get("table_comment"),
                json.dumps(payload.get("columns") or {}, ensure_ascii=True),
                str(payload.get("kind") or "TABLE"),
                now,
                expires_at,
                flag,
            )
            for table, payload in entries.items()
        ]
        with self._lock, self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO column_comments_cache
                    (cache_key, db_profile, database_name, schema_name, table_name,
                     table_comment, columns_json, kind, fetched_at, expires_at, bulk_filled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    table_comment = excluded.table_comment,
                    columns_json  = excluded.columns_json,
                    kind          = excluded.kind,
                    fetched_at    = excluded.fetched_at,
                    expires_at    = excluded.expires_at,
                    bulk_filled   = MAX(column_comments_cache.bulk_filled, excluded.bulk_filled)
                """,
                rows,
            )
        return len(rows)

    def schema_has_bulk_filled_cache(
        self,
        *,
        db_profile: str,
        database: str,
        schema: str,
    ) -> bool:
        """``True`` when at least one fresh ``bulk_filled=1`` row exists
        for ``(profile, database, schema)``.

        Presence of one bulk-filled row implies the whole schema is
        covered by the cache (bulk_schema_metadata returns every table
        in the schema by contract). ``list_assets`` keys off this flag
        to decide whether reading from cache is safe — partial caches
        produced by the per-table fallback path are not.
        """
        now = time.time()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM column_comments_cache
                WHERE db_profile = ? AND database_name = ? AND schema_name = ?
                  AND bulk_filled = 1 AND expires_at >= ?
                LIMIT 1
                """,
                (
                    str(db_profile or ""),
                    str(database or ""),
                    str(schema or ""),
                    now,
                ),
            ).fetchone()
        return row is not None

    def lookup_column_comments_cache(
        self,
        *,
        db_profile: str,
        database: str,
        schema: str,
        table: str,
    ) -> dict[str, Any] | None:
        """Return a single fresh cache entry, or ``None`` if missing/expired.

        Returned shape: ``{"table_comment": ..., "columns": {...}, "kind": ...,
        "fetched_at": ..., "expires_at": ...}``. Expired rows are kept on disk
        (cheaper than rewriting) but the lookup pretends they're absent.
        """
        cache_key = self._ccc_key(
            db_profile=str(db_profile or ""),
            database=str(database or ""),
            schema=str(schema or ""),
            table=str(table or ""),
        )
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT table_comment, columns_json, kind, fetched_at, expires_at
                FROM column_comments_cache
                WHERE cache_key = ?
                """,
                (cache_key,),
            ).fetchone()
        if row is None:
            return None
        if float(row["expires_at"]) < time.time():
            return None
        try:
            columns = json.loads(row["columns_json"])
        except Exception:
            columns = {}
        return {
            "table_comment": row["table_comment"],
            "columns": columns,
            "kind": row["kind"] or "TABLE",
            "fetched_at": float(row["fetched_at"]),
            "expires_at": float(row["expires_at"]),
        }

    def lookup_column_comments_cache_bulk(
        self,
        *,
        db_profile: str,
        database: str,
        schema: str,
    ) -> dict[str, dict[str, Any]]:
        """Return ``{table_name: cached_entry}`` for every fresh row in a schema.

        Used by the connector's bulk path to decide whether a refetch is
        needed at all. Expired rows are skipped.
        """
        now = time.time()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT table_name, table_comment, columns_json, kind, fetched_at, expires_at
                FROM column_comments_cache
                WHERE db_profile = ? AND database_name = ? AND schema_name = ?
                  AND expires_at >= ?
                """,
                (
                    str(db_profile or ""),
                    str(database or ""),
                    str(schema or ""),
                    now,
                ),
            ).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            try:
                columns = json.loads(row["columns_json"])
            except Exception:
                columns = {}
            out[str(row["table_name"])] = {
                "table_comment": row["table_comment"],
                "columns": columns,
                "kind": row["kind"] or "TABLE",
                "fetched_at": float(row["fetched_at"]),
                "expires_at": float(row["expires_at"]),
            }
        return out

    def invalidate_column_comments_cache(
        self,
        *,
        db_profile: str,
        database: str = "",
        schema: str | None = None,
        table: str | None = None,
    ) -> int:
        """Drop cached rows at one of three granularities.

        * ``schema`` + ``table`` set → single row (column/table comment write).
        * ``schema`` set, ``table`` ``None`` → whole schema (schema comment write).
        * Both ``None`` → whole profile (database comment write, profile reset).

        Returns rowcount. Always safe — a no-op on a cold cache returns 0.
        """
        params: list[Any] = [str(db_profile or "")]
        sql = "DELETE FROM column_comments_cache WHERE db_profile = ?"
        # ``database_name`` is empty string when the profile is single-db;
        # we filter on it whenever a schema is named so a multi-db profile
        # only wipes the affected database.
        if schema is not None:
            sql += " AND database_name = ? AND schema_name = ?"
            params.append(str(database or ""))
            params.append(str(schema or ""))
        if table is not None:
            sql += " AND table_name = ?"
            params.append(str(table or ""))
        with self._lock, self._connect() as conn:
            cur = conn.execute(sql, params)
            return int(cur.rowcount or 0)

    def gc_column_comments_cache(self) -> int:
        """Sweep expired rows; called at process startup alongside other GC."""
        now = time.time()
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM column_comments_cache WHERE expires_at < ?",
                (now,),
            )
            return int(cur.rowcount or 0)

    # ── schemas_cache: per-catalog schema-level cache ─────────────

    @staticmethod
    def _sc_key(*, db_profile: str, database: str, catalog: str, schema: str) -> str:
        return f"{db_profile}|{database or ''}|{catalog or ''}|{schema}"

    def save_schemas_cache(
        self,
        *,
        db_profile: str,
        database: str,
        catalog: str,
        entries: dict[str, str | None],
        ttl_seconds: float = 3600.0,
        bulk_filled: bool = False,
    ) -> int:
        """Bulk upsert schema-level entries for one catalog.

        ``entries`` maps schema name → schema comment (``None`` when the
        schema has no comment). ``bulk_filled`` mirrors the column
        cache's flag: ``True`` when a single ``bulk_catalog_metadata``
        call produced the dict (covers every schema in the catalog),
        ``False`` for per-schema fallback writes.
        """
        if not entries:
            return 0
        now = time.time()
        if ttl_seconds == 0:
            ttl_seconds = 3600.0
        expires_at = now + float(ttl_seconds)
        flag = 1 if bulk_filled else 0
        rows = [
            (
                self._sc_key(
                    db_profile=str(db_profile or ""),
                    database=str(database or ""),
                    catalog=str(catalog or ""),
                    schema=str(schema or ""),
                ),
                str(db_profile or ""),
                str(database or ""),
                str(catalog or ""),
                str(schema or ""),
                comment,
                flag,
                now,
                expires_at,
            )
            for schema, comment in entries.items()
        ]
        with self._lock, self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO schemas_cache
                    (cache_key, db_profile, database_name, catalog_name, schema_name,
                     schema_comment, bulk_filled, fetched_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    schema_comment = excluded.schema_comment,
                    bulk_filled    = MAX(schemas_cache.bulk_filled, excluded.bulk_filled),
                    fetched_at     = excluded.fetched_at,
                    expires_at     = excluded.expires_at
                """,
                rows,
            )
        return len(rows)

    def lookup_schemas_cache(
        self,
        *,
        db_profile: str,
        database: str,
        catalog: str,
        schema: str,
    ) -> dict[str, Any] | None:
        """Return one fresh schema entry or ``None`` if missing/expired."""
        cache_key = self._sc_key(
            db_profile=str(db_profile or ""),
            database=str(database or ""),
            catalog=str(catalog or ""),
            schema=str(schema or ""),
        )
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT schema_comment, bulk_filled, fetched_at, expires_at
                FROM schemas_cache WHERE cache_key = ?
                """,
                (cache_key,),
            ).fetchone()
        if row is None or float(row["expires_at"]) < time.time():
            return None
        return {
            "schema_comment": row["schema_comment"],
            "bulk_filled": bool(row["bulk_filled"]),
            "fetched_at": float(row["fetched_at"]),
            "expires_at": float(row["expires_at"]),
        }

    def catalog_has_bulk_filled_cache(
        self,
        *,
        db_profile: str,
        database: str,
        catalog: str,
    ) -> bool:
        """``True`` when at least one fresh ``bulk_filled=1`` row exists
        for ``(profile, database, catalog)``.

        ``list_schemas`` keys off this flag to decide whether reading
        schema names from the cache is safe instead of re-issuing
        SHOW SCHEMAS / pg_namespace lookups.
        """
        now = time.time()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM schemas_cache
                WHERE db_profile = ? AND database_name = ? AND catalog_name = ?
                  AND bulk_filled = 1 AND expires_at >= ?
                LIMIT 1
                """,
                (
                    str(db_profile or ""),
                    str(database or ""),
                    str(catalog or ""),
                    now,
                ),
            ).fetchone()
        return row is not None

    def list_schemas_from_cache(
        self,
        *,
        db_profile: str,
        database: str,
        catalog: str,
    ) -> list[tuple[str, str | None]]:
        """Return ``[(schema_name, schema_comment), …]`` for every fresh
        row of this catalog. Caller is responsible for checking
        ``catalog_has_bulk_filled_cache`` first if it needs to know
        whether the list is exhaustive.
        """
        now = time.time()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT schema_name, schema_comment FROM schemas_cache
                WHERE db_profile = ? AND database_name = ? AND catalog_name = ?
                  AND expires_at >= ?
                ORDER BY schema_name
                """,
                (
                    str(db_profile or ""),
                    str(database or ""),
                    str(catalog or ""),
                    now,
                ),
            ).fetchall()
        return [(str(r[0]), r[1]) for r in rows]

    def invalidate_schemas_cache(
        self,
        *,
        db_profile: str,
        database: str = "",
        catalog: str | None = None,
        schema: str | None = None,
    ) -> int:
        """Drop schema-cache rows at one of three granularities.

        * ``catalog`` + ``schema`` set → single schema row.
        * ``catalog`` only → whole catalog.
        * Both ``None`` → whole profile.
        """
        params: list[Any] = [str(db_profile or "")]
        sql = "DELETE FROM schemas_cache WHERE db_profile = ?"
        if catalog is not None:
            sql += " AND database_name = ? AND catalog_name = ?"
            params.append(str(database or ""))
            params.append(str(catalog or ""))
        if schema is not None:
            sql += " AND schema_name = ?"
            params.append(str(schema or ""))
        with self._lock, self._connect() as conn:
            cur = conn.execute(sql, params)
            return int(cur.rowcount or 0)

    def gc_schemas_cache(self) -> int:
        """Sweep expired schemas_cache rows."""
        now = time.time()
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM schemas_cache WHERE expires_at < ?",
                (now,),
            )
            return int(cur.rowcount or 0)

    def list_runs_with_result_counts(self, limit: int = 20) -> list[dict[str, Any]]:
        """List recent runs augmented with pending evaluation count."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    r.id,
                    r.started_at,
                    r.ended_at,
                    r.duration_sec,
                    r.status,
                    r.mode,
                    r.db_backend,
                    r.db_profile,
                    r.llm_provider,
                    r.llm_model,
                    r.llm_profile,
                    r.doc_profile,
                    r.code_profile,
                    r.scope_json,
                    COUNT(rr.id)          AS total_alternatives,
                    SUM(CASE WHEN rr.evaluation IS NULL OR rr.evaluation = ''
                             THEN 1 ELSE 0 END) AS pending_count
                FROM analysis_runs r
                LEFT JOIN run_results rr ON rr.run_id = r.id
                GROUP BY r.id
                ORDER BY r.started_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            raw = d.get("scope_json")
            if isinstance(raw, str) and raw:
                with contextlib.suppress(Exception):
                    d["scope_json"] = json.loads(raw)
            out.append(d)
        return out

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def log_event(
        self,
        *,
        event_type: str,
        status: str,
        command: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO app_events (created_at, event_type, status, command, details_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    time.time(),
                    event_type,
                    status,
                    command,
                    json.dumps(details or {}, ensure_ascii=True),
                ),
            )

    def list_recent_runs(
        self,
        limit: int = 20,
        *,
        command_filter: str | None = "analyze.run",
    ) -> list[dict[str, Any]]:
        """Return the most-recent runs, optionally filtered by ``command``.

        ``command_filter`` defaults to ``"analyze.run"`` so ``/history list``
        shows only ``/run`` invocations — the historical "what data
        analyses have I executed" question. Pass ``None`` to include
        ``/ask`` sessions too (which are stored as ``search.ask``
        rows). Per the 2026-05-02 user feedback: ``/ask`` chat sessions
        belong in ``/session list`` (with resume), not in the
        analyze-run history list.
        """
        clauses: list[str] = []
        params: list[Any] = []
        if command_filter:
            clauses.append("command = ?")
            params.append(str(command_filter))
        where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(max(1, int(limit)))
        # ``tokens_json`` joins the SELECT so /api/usage's aggregator
        # (and any future caller that reasons about per-run cost) can
        # read the per-call records without a second round-trip. The
        # column is small (~400-500B per run) and the SPA's runs list
        # already pulls it via /api/history/runs/{id}, so the marginal
        # bandwidth cost on a 50-row list query is in the same ballpark.
        # Without this column on the SELECT, _aggregate_runs found
        # ``run.get("tokens_json")`` to be ``None`` on every row and
        # silently skipped them all -- the Overview cards rendered
        # "--" even with months of usage in the history database.
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, started_at, ended_at, duration_sec, status, command, mode,
                       db_backend, db_profile, llm_provider, llm_model,
                       llm_profile, doc_profile, code_profile,
                       scope_json, metrics_json, tokens_json,
                       created_by, hostname, client_version, shared_uuid
                FROM analysis_runs
                {where_sql}
                ORDER BY started_at DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            # Enrich each row with the actual (schema, table, column)
            # tuples processed by the run. ``scope_json`` only carries
            # the schema-level scope the user originally picked, so a
            # column-level run (``/rerun --column x`` or analyze with
            # ``column_overrides``) was indistinguishable from a full
            # table run in the listing — users saw "sales · 1 table"
            # for both and had to click into the run-detail view to
            # find out what was actually processed. The aggregate below
            # uses ``run_results`` (the per-asset write log) as the
            # ground truth and surfaces a compact summary the SPA's
            # Runs / Compare pages render as e.g. "sales.orders.status"
            # or "sales.orders (3 columns)".
            run_ids = [r["id"] for r in rows]
            assets_by_run: dict[int, dict[str, Any]] = {}
            if run_ids:
                placeholders = ",".join("?" for _ in run_ids)
                agg_rows = conn.execute(
                    f"""
                    SELECT run_id,
                           COUNT(DISTINCT schema_name) AS schemas_count,
                           COUNT(DISTINCT schema_name || '\\x1f' || table_name)
                               AS tables_count,
                           SUM(CASE
                                   WHEN column_name IS NULL OR column_name = ''
                                   THEN 0 ELSE 1
                               END) AS column_rows
                    FROM run_results
                    WHERE run_id IN ({placeholders})
                    GROUP BY run_id
                    """,
                    tuple(run_ids),
                ).fetchall()
                for ar in agg_rows:
                    assets_by_run[int(ar["run_id"])] = {
                        "schemas": int(ar["schemas_count"] or 0),
                        "tables": int(ar["tables_count"] or 0),
                        "columns": int(ar["column_rows"] or 0),
                        "sample": [],
                    }
                # Pull a small sample of asset tuples per run so the
                # SPA can render "schema.table.column" labels without
                # a second roundtrip. Cap at 6 distinct tuples per run
                # — enough to drive the headline label + a tooltip
                # without bloating the payload on long-history lists.
                sample_rows = conn.execute(
                    f"""
                    SELECT run_id, schema_name, table_name, column_name
                    FROM run_results
                    WHERE run_id IN ({placeholders})
                    GROUP BY run_id, schema_name, table_name, COALESCE(column_name, '')
                    ORDER BY run_id, schema_name, table_name, column_name
                    """,
                    tuple(run_ids),
                ).fetchall()
                for sr in sample_rows:
                    rid = int(sr["run_id"])
                    bucket = assets_by_run.setdefault(
                        rid,
                        {"schemas": 0, "tables": 0, "columns": 0, "sample": []},
                    )
                    if len(bucket["sample"]) >= 6:
                        continue
                    bucket["sample"].append(
                        {
                            "schema": sr["schema_name"],
                            "table": sr["table_name"],
                            "column": sr["column_name"] or None,
                        }
                    )
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            # ``scope_json`` + ``metrics_json`` are eagerly JSON-decoded
            # so the SPA's run list can render scope / metrics chips
            # without re-parsing. ``tokens_json`` deliberately stays a
            # raw string -- the aggregator handles either shape, and
            # most readers (recent-runs feed, runs list table) only
            # care that the field is non-empty, never its contents.
            for key in ("scope_json", "metrics_json"):
                raw = d.get(key)
                if isinstance(raw, str) and raw:
                    with contextlib.suppress(Exception):
                        d[key] = json.loads(raw)
            d["processed_assets"] = assets_by_run.get(
                int(d["id"]),
                {"schemas": 0, "tables": 0, "columns": 0, "sample": []},
            )
            out.append(d)
        return out

    def find_runs_for_scope(
        self,
        *,
        schema: str | None = None,
        table: str | None = None,
        command_filter: str | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """Return runs whose scope_json mentions ``schema`` (and optionally ``table``).

        Used by ``/compare`` to auto-pick the last N runs touching a
        given asset. Filter is a LIKE match on the JSON-encoded scope —
        not a full JSON search, but adequate for the typical
        ``{"<schema>": ["<table>", ...]}`` shape produced by AMX.
        ``command_filter`` accepts ``"analyze.run"``, ``"search.ask"``,
        or ``None`` (any).
        """
        clauses: list[str] = []
        params: list[Any] = []
        if command_filter:
            clauses.append("command = ?")
            params.append(command_filter)
        if schema:
            clauses.append("scope_json LIKE ?")
            params.append(f'%"{schema}"%')
        if table:
            clauses.append("scope_json LIKE ?")
            params.append(f'%"{table}"%')
        where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(max(1, int(limit)))
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, started_at, ended_at, duration_sec, status, command, mode,
                       db_backend, db_profile, llm_provider, llm_model,
                       llm_profile, doc_profile, code_profile,
                       scope_json, metrics_json, tokens_json, settings_json,
                       selected_count, planned_count, processed_count, applied_count
                FROM analysis_runs
                {where_sql}
                ORDER BY started_at DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            for key in ("scope_json", "metrics_json", "tokens_json", "settings_json"):
                raw = d.get(key)
                if isinstance(raw, str) and raw:
                    with contextlib.suppress(Exception):
                        d[key] = json.loads(raw)
            out.append(d)
        return out

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM analysis_runs WHERE id = ?",
                (int(run_id),),
            ).fetchone()
        if not row:
            return None
        out = dict(row)
        for key in ("scope_json", "metrics_json", "tokens_json", "results_json", "settings_json"):
            raw = out.get(key)
            if isinstance(raw, str) and raw:
                with contextlib.suppress(Exception):
                    out[key] = json.loads(raw)
        return out

    def stats(self, command_filter: str | None = "analyze.run") -> dict[str, Any]:
        """Aggregate counters for the dashboard.

        ``command_filter`` defaults to ``"analyze.run"`` so AMX
        Studio's "Total runs" / "Success rate" tiles reflect the
        same scope the Recent runs feed (which only lists `/run`
        invocations). Pass ``None`` to include every command kind.
        """
        where = "WHERE command = ?" if command_filter else ""
        params: tuple[Any, ...] = (command_filter,) if command_filter else ()
        with self._connect() as conn:
            total_runs = conn.execute(
                f"SELECT COUNT(*) AS n FROM analysis_runs {where}", params
            ).fetchone()["n"]
            ok_runs = conn.execute(
                "SELECT COUNT(*) AS n FROM analysis_runs WHERE status = 'success'"
                + (" AND command = ?" if command_filter else ""),
                params,
            ).fetchone()["n"]
            fail_runs = conn.execute(
                "SELECT COUNT(*) AS n FROM analysis_runs WHERE status = 'failed'"
                + (" AND command = ?" if command_filter else ""),
                params,
            ).fetchone()["n"]
            review_runs = conn.execute(
                "SELECT COUNT(*) AS n FROM analysis_runs WHERE status = 'ready_for_review'"
                + (" AND command = ?" if command_filter else ""),
                params,
            ).fetchone()["n"]
            avg_duration = conn.execute(
                "SELECT AVG(duration_sec) AS v FROM analysis_runs WHERE duration_sec IS NOT NULL"
                + (" AND command = ?" if command_filter else ""),
                params,
            ).fetchone()["v"]
            last_started = conn.execute(
                f"SELECT MAX(started_at) AS v FROM analysis_runs {where}", params
            ).fetchone()["v"]
            total_events = conn.execute("SELECT COUNT(*) AS n FROM app_events").fetchone()["n"]
            metrics_rows = conn.execute(
                "SELECT metrics_json FROM analysis_runs WHERE metrics_json IS NOT NULL"
                + (" AND command = ?" if command_filter else ""),
                params,
            ).fetchall()

        model_durations: list[float] = []
        for row in metrics_rows:
            raw = row[0]
            if not isinstance(raw, str) or not raw:
                continue
            try:
                parsed = json.loads(raw)
            except Exception:
                continue
            val = float((parsed or {}).get("model_processing_sec") or 0.0)
            if val > 0:
                model_durations.append(val)

        avg_model_duration = sum(model_durations) / len(model_durations) if model_durations else 0.0

        return {
            "total_runs": int(total_runs or 0),
            "success_runs": int(ok_runs or 0),
            "failed_runs": int(fail_runs or 0),
            "ready_for_review_runs": int(review_runs or 0),
            "avg_duration_sec": float(avg_duration or 0.0),
            "avg_model_processing_sec": float(avg_model_duration),
            "last_started_at": float(last_started or 0.0),
            "total_events": int(total_events or 0),
        }

    def list_recent_events(self, limit: int = 30) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, created_at, event_type, status, command, details_json
                FROM app_events
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            raw = d.get("details_json")
            if isinstance(raw, str) and raw:
                with contextlib.suppress(Exception):
                    d["details_json"] = json.loads(raw)
            out.append(d)
        return out


# The singleton is typed as ``Any`` so v0.12.0+ shared-history mode
# can store a :class:`amx.storage.dual_write.DualWriteHistoryStore`
# (which implements :class:`amx.storage.protocol.IHistoryStore` but is
# not a SQLiteHistoryStore subclass). All call sites use Protocol-
# compatible methods, so the loose typing does not hurt them.
_store: Any | None = None


def init_history_store(config_dir: str) -> SQLiteHistoryStore:
    """Initialize the local-only history store (legacy entry point).

    .. deprecated:: 0.12.0
        Prefer :func:`amx.storage.factory.init_history_store(cfg)` —
        the new entry takes an :class:`AMXConfig` and dispatches to
        the dual-write store when shared mode is enabled. This shim
        is kept so the headless application path (which never calls
        the CLI directly) continues to work.
    """
    global _store
    if _store is None:
        db_path = Path(config_dir) / "history.db"
        _store = SQLiteHistoryStore(db_path)
        try:
            _store.init()
            log.info("SQLite history initialized at %s", db_path)
        except Exception as exc:
            log.warning("Could not initialize SQLite history store: %s", exc)
    return _store


def history_store() -> Any | None:
    """Return the active singleton.

    May be ``SQLiteHistoryStore`` (local-only) or ``DualWriteHistoryStore``
    (shared mode) — both implement the :class:`IHistoryStore` Protocol.
    """
    return _store
