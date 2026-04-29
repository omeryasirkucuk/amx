"""SQLite-backed local history store for AMX runs."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
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
                    error_text TEXT
                )
                """
            )
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
            try:
                conn.execute("ALTER TABLE run_results ADD COLUMN asset_kind TEXT NOT NULL DEFAULT 'table'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE run_results ADD COLUMN applied_at REAL")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE run_results ADD COLUMN logprob_score REAL")
            except sqlite3.OperationalError:
                pass
            for stmt in (
                "ALTER TABLE run_results ADD COLUMN raw_logprob REAL",
                "ALTER TABLE run_results ADD COLUMN token_count INTEGER",
                "ALTER TABLE run_results ADD COLUMN model_version TEXT NOT NULL DEFAULT ''",
            ):
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass
            for stmt in (
                "ALTER TABLE run_results ADD COLUMN catalog_status TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE run_results ADD COLUMN catalog_indexed_at REAL",
                "ALTER TABLE run_results ADD COLUMN db_applied_status TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE run_results ADD COLUMN effective_source_kind TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE run_results ADD COLUMN superseded_at REAL",
                "ALTER TABLE run_results ADD COLUMN rejection_reason TEXT NOT NULL DEFAULT ''",
            ):
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError:
                    pass
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_results_run_id "
                "ON run_results(run_id)"
            )
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
    ) -> int:
        started = time.time()
        with self._lock, self._connect() as conn:
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
                    db_backend, db_profile, llm_provider, llm_model, scope_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )
            return int(cur.lastrowid)

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
                try:
                    d["alternatives_json"] = json.loads(raw)
                except Exception:
                    pass
            out.append(d)
        return out

    def list_runs_with_result_counts(
        self, limit: int = 20
    ) -> list[dict[str, Any]]:
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
                try:
                    d["scope_json"] = json.loads(raw)
                except Exception:
                    pass
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

    def list_recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, started_at, ended_at, duration_sec, status, command, mode,
                       db_backend, db_profile, llm_provider, llm_model, scope_json, metrics_json
                FROM analysis_runs
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            for key in ("scope_json", "metrics_json"):
                raw = d.get(key)
                if isinstance(raw, str) and raw:
                    try:
                        d[key] = json.loads(raw)
                    except Exception:
                        pass
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
        for key in ("scope_json", "metrics_json", "tokens_json", "results_json"):
            raw = out.get(key)
            if isinstance(raw, str) and raw:
                try:
                    out[key] = json.loads(raw)
                except Exception:
                    pass
        return out

    def stats(self) -> dict[str, Any]:
        with self._connect() as conn:
            total_runs = conn.execute(
                "SELECT COUNT(*) AS n FROM analysis_runs"
            ).fetchone()["n"]
            ok_runs = conn.execute(
                "SELECT COUNT(*) AS n FROM analysis_runs WHERE status = 'success'"
            ).fetchone()["n"]
            fail_runs = conn.execute(
                "SELECT COUNT(*) AS n FROM analysis_runs WHERE status = 'failed'"
            ).fetchone()["n"]
            review_runs = conn.execute(
                "SELECT COUNT(*) AS n FROM analysis_runs WHERE status = 'ready_for_review'"
            ).fetchone()["n"]
            avg_duration = conn.execute(
                "SELECT AVG(duration_sec) AS v FROM analysis_runs WHERE duration_sec IS NOT NULL"
            ).fetchone()["v"]
            last_started = conn.execute(
                "SELECT MAX(started_at) AS v FROM analysis_runs"
            ).fetchone()["v"]
            total_events = conn.execute(
                "SELECT COUNT(*) AS n FROM app_events"
            ).fetchone()["n"]
            metrics_rows = conn.execute(
                "SELECT metrics_json FROM analysis_runs WHERE metrics_json IS NOT NULL"
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

        avg_model_duration = (
            sum(model_durations) / len(model_durations)
            if model_durations
            else 0.0
        )

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
                try:
                    d["details_json"] = json.loads(raw)
                except Exception:
                    pass
            out.append(d)
        return out


_store: SQLiteHistoryStore | None = None


def init_history_store(config_dir: str) -> SQLiteHistoryStore:
    """Initialize and return the singleton history store."""
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


def history_store() -> SQLiteHistoryStore | None:
    return _store
