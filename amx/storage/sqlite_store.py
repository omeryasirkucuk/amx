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
            ):
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(stmt)
            with contextlib.suppress(sqlite3.OperationalError):
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_run_results_shared_uuid "
                    "ON run_results(shared_uuid)"
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
                    compaction_state_json TEXT
                )
                """
            )
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
                        token_count, model_version, reasoning, alternatives_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, started_at, ended_at, duration_sec, status, command, mode,
                       db_backend, db_profile, llm_provider, llm_model,
                       llm_profile, doc_profile, code_profile,
                       scope_json, metrics_json,
                       created_by, hostname, client_version, shared_uuid
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
            for key in ("scope_json", "metrics_json"):
                raw = d.get(key)
                if isinstance(raw, str) and raw:
                    with contextlib.suppress(Exception):
                        d[key] = json.loads(raw)
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

        ``command_filter`` defaults to ``"analyze.run"`` so the
        visualizer's "Total runs" / "Success rate" tiles reflect the
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
                f"SELECT COUNT(*) AS n FROM analysis_runs WHERE status = 'success'"
                + (" AND command = ?" if command_filter else ""),
                params,
            ).fetchone()["n"]
            fail_runs = conn.execute(
                f"SELECT COUNT(*) AS n FROM analysis_runs WHERE status = 'failed'"
                + (" AND command = ?" if command_filter else ""),
                params,
            ).fetchone()["n"]
            review_runs = conn.execute(
                f"SELECT COUNT(*) AS n FROM analysis_runs WHERE status = 'ready_for_review'"
                + (" AND command = ?" if command_filter else ""),
                params,
            ).fetchone()["n"]
            avg_duration = conn.execute(
                f"SELECT AVG(duration_sec) AS v FROM analysis_runs WHERE duration_sec IS NOT NULL"
                + (" AND command = ?" if command_filter else ""),
                params,
            ).fetchone()["v"]
            last_started = conn.execute(
                f"SELECT MAX(started_at) AS v FROM analysis_runs {where}", params
            ).fetchone()["v"]
            total_events = conn.execute("SELECT COUNT(*) AS n FROM app_events").fetchone()["n"]
            metrics_rows = conn.execute(
                f"SELECT metrics_json FROM analysis_runs WHERE metrics_json IS NOT NULL"
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
