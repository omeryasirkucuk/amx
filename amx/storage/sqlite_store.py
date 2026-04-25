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
                    reasoning TEXT,
                    alternatives_json TEXT NOT NULL,
                    evaluated_at REAL,
                    chosen_description TEXT,
                    evaluation TEXT,
                    FOREIGN KEY (run_id) REFERENCES analysis_runs(id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_results_run_id "
                "ON run_results(run_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_results_asset "
                "ON run_results(schema_name, table_name, column_name)"
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
                        asset_kind, source, confidence, reasoning, alternatives_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        s.get("reasoning", ""),
                        json.dumps(s.get("alternatives", []), ensure_ascii=True),
                    ),
                )
                ids.append(int(cur.lastrowid))
        return ids

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
                       db_backend, db_profile, llm_provider, llm_model, scope_json
                FROM analysis_runs
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            raw = d.get("scope_json")
            if isinstance(raw, str) and raw:
                try:
                    d["scope_json"] = json.loads(raw)
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
            avg_duration = conn.execute(
                "SELECT AVG(duration_sec) AS v FROM analysis_runs WHERE duration_sec IS NOT NULL"
            ).fetchone()["v"]
            last_started = conn.execute(
                "SELECT MAX(started_at) AS v FROM analysis_runs"
            ).fetchone()["v"]
            total_events = conn.execute(
                "SELECT COUNT(*) AS n FROM app_events"
            ).fetchone()["n"]
        return {
            "total_runs": int(total_runs or 0),
            "success_runs": int(ok_runs or 0),
            "failed_runs": int(fail_runs or 0),
            "avg_duration_sec": float(avg_duration or 0.0),
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

