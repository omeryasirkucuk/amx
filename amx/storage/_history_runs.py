"""Run-lifecycle methods extracted from :mod:`amx.storage.sqlite_store`.

A5 of the sqlite_store decomposition — the highest-blast-radius
extraction. Fifteen methods drive every analysis-run lifecycle event
from create_run through finish_run, plus the read aggregates
(list_recent_runs, stats, find_runs_for_scope) that every dashboard
and CLI history view consumes.

Each function takes the store as ``hs`` and uses its ``_lock`` +
``_connect()`` plumbing. ``_ensure_run_columns`` and any DDL stays
in ``SQLiteHistoryStore.init()`` untouched.
"""

from __future__ import annotations

import contextlib
import json
import time
from typing import TYPE_CHECKING, Any

from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.storage.sqlite_store import SQLiteHistoryStore

log = get_logger("storage.sqlite.runs")


def create_run(
    hs: SQLiteHistoryStore,
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
    with hs._lock, hs._connect() as conn:
        # Belt-and-suspenders: ensure the v0.5.2 reporting columns
        # exist before we try to INSERT into them. Without this, a user
        # whose ``init()`` somehow ran on stale code (pipx editable mode
        # quirks, in-process upgrade) would silently fail the INSERT
        # and end up with /history showing 'Processed: —' forever.
        hs._ensure_run_columns(conn)
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


def update_run_planned_count(hs: SQLiteHistoryStore, run_id: int, planned_count: int) -> None:
    """Set planned_count after the missing-only filter has dropped already-commented assets."""
    with hs._lock, hs._connect() as conn:
        conn.execute(
            "UPDATE analysis_runs SET planned_count = ? WHERE id = ?",
            (int(planned_count), int(run_id)),
        )


def increment_run_processed(hs: SQLiteHistoryStore, run_id: int, by: int = 1) -> None:
    """Bump processed_count for one (or more) tables that started processing."""
    with hs._lock, hs._connect() as conn:
        conn.execute(
            "UPDATE analysis_runs SET processed_count = COALESCE(processed_count, 0) + ? WHERE id = ?",
            (int(by), int(run_id)),
        )


def increment_run_applied(hs: SQLiteHistoryStore, run_id: int, by: int = 1) -> None:
    """Bump applied_count for results successfully written to the live DB."""
    with hs._lock, hs._connect() as conn:
        conn.execute(
            "UPDATE analysis_runs SET applied_count = COALESCE(applied_count, 0) + ? WHERE id = ?",
            (int(by), int(run_id)),
        )


def finish_run(
    hs: SQLiteHistoryStore,
    run_id: int,
    *,
    status: str,
    metrics: dict[str, Any],
    tokens: dict[str, Any],
    results: dict[str, Any],
    error_text: str = "",
) -> None:
    ended = time.time()
    with hs._lock, hs._connect() as conn:
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
                error_text = ?,
                current_step_label = NULL
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


def update_run_status(
    hs: SQLiteHistoryStore, run_id: int, status: str, error_text: str = ""
) -> None:
    """Update run status without overwriting metrics/tokens/results payloads."""
    with hs._lock, hs._connect() as conn:
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


def list_recent_runs(
    hs: SQLiteHistoryStore,
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
    with hs._connect() as conn:
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
    hs: SQLiteHistoryStore,
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
    with hs._connect() as conn:
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


def get_run(hs: SQLiteHistoryStore, run_id: int) -> dict[str, Any] | None:
    with hs._connect() as conn:
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


def stats(hs: SQLiteHistoryStore, command_filter: str | None = "analyze.run") -> dict[str, Any]:
    """Aggregate counters for the dashboard.

    ``command_filter`` defaults to ``"analyze.run"`` so AMX
    Studio's "Total runs" / "Success rate" tiles reflect the
    same scope the Recent runs feed (which only lists `/run`
    invocations). Pass ``None`` to include every command kind.
    """
    where = "WHERE command = ?" if command_filter else ""
    params: tuple[Any, ...] = (command_filter,) if command_filter else ()
    with hs._connect() as conn:
        total_runs = conn.execute(
            f"SELECT COUNT(*) AS n FROM analysis_runs {where}", params
        ).fetchone()["n"]
        # "Success rate" reads as "did the analysis finish without
        # erroring?" — not "did the user apply the suggestions?".
        # ``ready_for_review`` and ``applied_partial`` both mean the
        # pipeline ran to completion; treating them as non-success
        # made the Overview tile stick at 0% for users who reviewed
        # rather than auto-applied.
        ok_runs = conn.execute(
            "SELECT COUNT(*) AS n FROM analysis_runs WHERE status IN"
            " ('success', 'applied_partial', 'ready_for_review', 'completed')"
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


def count_pending_review_runs(
    hs: SQLiteHistoryStore, command_filter: str | None = "analyze.run"
) -> int:
    """Global count of runs that still have unreviewed result rows.

    Covers both ``ready_for_review`` (nothing applied yet) and
    ``applied_partial`` (some applied, some still pending) — the
    two terminal states where human review work remains. Used by
    the Studio Landing "pending review" chip so the badge reflects
    the whole table, not just the most-recent feed slice.
    """
    where = "status IN ('ready_for_review','applied_partial')"
    params: tuple[Any, ...] = ()
    if command_filter:
        where += " AND command = ?"
        params = (command_filter,)
    with hs._connect() as conn:
        row = conn.execute(
            f"SELECT COUNT(*) AS n FROM analysis_runs WHERE {where}", params
        ).fetchone()
    return int((row["n"] if row is not None else 0) or 0)


def list_recent_events(hs: SQLiteHistoryStore, limit: int = 30) -> list[dict[str, Any]]:
    with hs._connect() as conn:
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


def recover_stale_runs(
    hs: SQLiteHistoryStore,
    *,
    threshold_sec: float = 300.0,
    now_utc: float | None = None,
) -> list[int]:
    """Mark stale running rows as failed and return their ids.

    A run is "stale" when ``status='running'`` and ``ended_at IS NULL``
    AND either:
      * ``last_heartbeat_at`` is older than ``now - threshold_sec``, or
      * ``last_heartbeat_at IS NULL`` (back-compat path -- runs that
        started before this column existed are assumed dead if
        recovered after a process restart).

    Idempotent: callers can invoke at every bootstrap tick without
    worrying about double-marking. Tightening the threshold over
    time will sweep up more rows on the next call.
    """
    now = now_utc if now_utc is not None else time.time()
    cutoff = now - threshold_sec
    with hs._lock, hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT id FROM analysis_runs
            WHERE status = 'running'
              AND ended_at IS NULL
              AND (
                   last_heartbeat_at IS NULL
                   OR last_heartbeat_at < ?
              )
            """,
            (cutoff,),
        ).fetchall()
        recovered = [int(r[0]) for r in rows]
        if recovered:
            placeholders = ",".join("?" for _ in recovered)
            conn.execute(
                f"""
                UPDATE analysis_runs
                SET ended_at = ?,
                    duration_sec = CASE
                        WHEN started_at IS NOT NULL THEN MAX(0.0, ? - started_at)
                        ELSE 0.0
                    END,
                    status = 'failed',
                    error_text = CASE
                        WHEN error_text IS NULL OR error_text = ''
                        THEN 'Recovered stale running run (heartbeat threshold exceeded)'
                        ELSE error_text
                    END
                WHERE id IN ({placeholders})
                """,
                [now, now, *recovered],
            )
    return recovered


def update_run_heartbeat(
    hs: SQLiteHistoryStore,
    run_id: int,
    *,
    now_utc: float | None = None,
) -> None:
    """Bump ``last_heartbeat_at`` for an in-flight analysis_runs row.

    Used by the orchestrator while a run is in flight; consumed by
    the stale-run recovery path (Phase 2) to detect interrupted
    runs after an unclean shutdown. No-ops for unknown run_ids so
    the caller can be defensive without checking first.
    """
    ts = now_utc if now_utc is not None else time.time()
    with hs._lock, hs._connect() as conn:
        conn.execute(
            "UPDATE analysis_runs SET last_heartbeat_at = ? WHERE id = ?",
            (ts, run_id),
        )


def update_run_current_step(hs: SQLiteHistoryStore, run_id: int, label: str) -> None:
    """Persist the most recent phase label for a live run.

    Cold-load progress: a Studio page refresh has no SSE replay,
    so without a persisted phase label the run-detail card falls
    back to "Waiting for the worker to begin…" even when the
    worker has been busy for half a minute. The web worker calls
    this at each startup/per-table boundary; ``finish_run`` clears
    the field when the run ends so the persisted view doesn't
    keep advertising a stale phase. No-ops for unknown run_ids.
    """
    trimmed = (label or "").strip()
    with hs._lock, hs._connect() as conn:
        conn.execute(
            "UPDATE analysis_runs SET current_step_label = ? WHERE id = ?",
            (trimmed or None, run_id),
        )
