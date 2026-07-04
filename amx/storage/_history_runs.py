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


# Allow-list mapping a DataTable column id -> the SQL column it sorts by.
# Anything outside this map falls back to ``started_at`` so a crafted
# ``sort_by`` can never inject SQL.
_RUNS_SORT_COLUMNS: dict[str, str] = {
    "id": "id",
    "status": "status",
    "duration": "duration_sec",
    "duration_sec": "duration_sec",
    "started": "started_at",
    "started_at": "started_at",
    "db": "db_profile",
    "db_profile": "db_profile",
    "model": "llm_model",
    "llm_model": "llm_model",
    "command": "command",
}

# Status filter id -> the stored status values it covers. "running" also
# covers freshly-queued workers, matching the frontend status chip.
_STATUS_GROUPS: dict[str, tuple[str, ...]] = {
    "success": ("success",),
    "failed": ("failed",),
    "running": ("running", "queued"),
    "cancelled": ("cancelled",),
}


def _runs_where(
    *,
    q: str | None = None,
    status: str | None = None,
    kind: str | None = None,
    command_filter: str | None = None,
    comparable_only: bool = False,
) -> tuple[str, list[Any]]:
    """Build the shared WHERE body (no ``WHERE`` keyword) for the Runs list
    and its facet counts. Returns ``(sql, params)``; ``sql`` is the AND-joined
    clause body, empty when no filter applies. Reused by ``list_recent_runs``
    and ``runs_facets`` so the page and its counts stay consistent."""
    from amx.storage.run_kinds import comparable_sql, kind_bucket_sql

    clauses: list[str] = []
    params: list[Any] = []
    if comparable_only:
        frag, frag_params = comparable_sql("command")
        clauses.append(frag)
        params.extend(frag_params)
    if command_filter:
        clauses.append("command = ?")
        params.append(str(command_filter))
    if kind:
        frag, frag_params = kind_bucket_sql(kind, "command")
        if frag:
            clauses.append(frag)
            params.extend(frag_params)
    if status:
        values = _STATUS_GROUPS.get(str(status).strip().lower())
        if values:
            placeholders = ",".join("?" for _ in values)
            clauses.append(f"status IN ({placeholders})")
            params.extend(values)
    if q and str(q).strip():
        needle = f"%{str(q).strip()}%"
        clauses.append(
            "(CAST(id AS TEXT) LIKE ? OR command LIKE ? OR scope_json LIKE ? OR status LIKE ?)"
        )
        params.extend([needle, needle, needle, needle])
    return " AND ".join(clauses), params


def list_recent_runs(
    hs: SQLiteHistoryStore,
    limit: int = 20,
    *,
    offset: int = 0,
    command_filter: str | None = "analyze.run",
    comparable_only: bool = False,
    q: str | None = None,
    status: str | None = None,
    kind: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = "desc",
) -> list[dict[str, Any]]:
    """Return one page of runs, optionally filtered by ``command``.

    ``command_filter`` defaults to ``"analyze.run"`` so ``/history list``
    shows only ``/run`` invocations — the historical "what data
    analyses have I executed" question. Pass ``None`` to include
    ``/ask`` sessions too (which are stored as ``search.ask``
    rows). Per the 2026-05-02 user feedback: ``/ask`` chat sessions
    belong in ``/session list`` (with resume), not in the
    analyze-run history list.

    ``comparable_only=True`` restricts the result to the
    description-producing commands the Compare picker can pivot (see
    :mod:`amx.storage.run_kinds`). It combines (AND) with
    ``command_filter`` when both are given.

    The Runs page drives server-side pagination through ``offset`` plus the
    ``q`` (free-text over id / command / scope / status), ``status``,
    ``kind`` (bucket), ``sort_by`` (allow-listed column id), and ``sort_dir``
    (``"asc"``/``"desc"``) arguments. All default to the legacy
    newest-first-by-``started_at`` behavior so other callers are unchanged.
    """
    where_body, params = _runs_where(
        q=q,
        status=status,
        kind=kind,
        command_filter=command_filter,
        comparable_only=comparable_only,
    )
    where_sql = ("WHERE " + where_body) if where_body else ""
    sort_col = _RUNS_SORT_COLUMNS.get((sort_by or "").strip().lower(), "started_at")
    direction = "ASC" if (sort_dir or "").strip().lower() == "asc" else "DESC"
    # Tie-break by id so rows with equal sort keys (same status, same
    # duration) page deterministically instead of drifting between fetches.
    order_sql = f"ORDER BY {sort_col} {direction}, id DESC"
    params.append(max(1, int(limit)))
    params.append(max(0, int(offset)))
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
            {order_sql}
            LIMIT ? OFFSET ?
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


def runs_facets(
    hs: SQLiteHistoryStore,
    *,
    q: str | None = None,
    status: str | None = None,
    kind: str | None = None,
) -> dict[str, Any]:
    """Full-dataset facets for the Runs page so its chips stay accurate
    regardless of the current page.

    Returns ``{"total", "kind_counts", "status_counts"}`` where:

    * ``total`` — rows matching the active ``q + kind + status`` (drives the
      pager).
    * ``kind_counts`` — per-bucket counts over the ``q``-filtered set only, so
      each KIND chip shows its own total and responds to search independently
      of the selected kind. Includes an ``"all"`` total.
    * ``status_counts`` — per-status-group counts over the ``q + kind``-filtered
      set, so the status chips reflect the active kind (as the page did when it
      computed badges client-side).
    """
    from amx.storage.run_kinds import KIND_BUCKETS, command_bucket

    def _count_where(**kw: Any) -> tuple[str, list[Any]]:
        body, params = _runs_where(**kw)
        return (("WHERE " + body) if body else ""), params

    with hs._connect() as conn:
        # total — every active filter applied.
        total_where, total_params = _count_where(q=q, status=status, kind=kind)
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM analysis_runs {total_where}",
            tuple(total_params),
        ).fetchone()["n"]

        # kind_counts — group by raw command over the q-filtered set, then
        # bucket in Python so the map matches the frontend exactly.
        kind_where, kind_params = _count_where(q=q)
        kind_rows = conn.execute(
            f"SELECT command, COUNT(*) AS n FROM analysis_runs {kind_where} GROUP BY command",
            tuple(kind_params),
        ).fetchall()
        kind_counts: dict[str, int] = dict.fromkeys((*KIND_BUCKETS, "other"), 0)
        kind_total = 0
        for r in kind_rows:
            n = int(r["n"] or 0)
            kind_counts[command_bucket(r["command"])] += n
            kind_total += n
        kind_counts["all"] = kind_total

        # status_counts — group by status over the q + kind-filtered set,
        # collapsing into the four chip groups (running ⊃ queued).
        status_where, status_params = _count_where(q=q, kind=kind)
        status_rows = conn.execute(
            f"SELECT status, COUNT(*) AS n FROM analysis_runs {status_where} GROUP BY status",
            tuple(status_params),
        ).fetchall()
        status_counts: dict[str, int] = dict.fromkeys(_STATUS_GROUPS, 0)
        for r in status_rows:
            st = str(r["status"] or "").strip().lower()
            n = int(r["n"] or 0)
            for group, members in _STATUS_GROUPS.items():
                if st in members:
                    status_counts[group] += n
                    break

    return {
        "total": int(total or 0),
        "kind_counts": kind_counts,
        "status_counts": status_counts,
    }


def find_runs_for_scope(
    hs: SQLiteHistoryStore,
    *,
    schema: str | None = None,
    table: str | None = None,
    command_filter: str | None = None,
    comparable_only: bool = False,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Return runs whose scope_json mentions ``schema`` (and optionally ``table``).

    Used by ``/compare`` to auto-pick the last N runs touching a
    given asset. Filter is a LIKE match on the JSON-encoded scope —
    not a full JSON search, but adequate for the typical
    ``{"<schema>": ["<table>", ...]}`` shape produced by AMX.
    ``command_filter`` accepts ``"analyze.run"``, ``"search.ask"``,
    or ``None`` (any).

    ``comparable_only=True`` restricts the result to the
    description-producing commands the Compare picker can pivot (see
    :mod:`amx.storage.run_kinds`) so ``/compare --last N`` never
    auto-resolves a non-comparable run. It combines (AND) with
    ``command_filter`` when both are given.
    """
    clauses: list[str] = []
    params: list[Any] = []
    if comparable_only:
        from amx.storage.run_kinds import comparable_sql

        frag, frag_params = comparable_sql("command")
        clauses.append(frag)
        params.extend(frag_params)
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


def delete_run(hs: SQLiteHistoryStore, run_id: int) -> dict[str, int]:
    """Hard-delete one run: its ``analysis_runs`` master row plus every
    ``run_results`` child row.

    Returns a counts dict ``{"runs": n, "results": m}`` so callers can
    confirm what was removed. A missing run yields ``{"runs": 0,
    "results": 0}`` (idempotent — deleting an already-gone run is not an
    error).

    The ``apply_events`` audit trail is intentionally left untouched:
    those rows record comments actually written to a live database and
    are cleared through the separate "clear a table's reviews" path
    (:func:`amx.storage._history_apply_audit.delete_apply_events_for_table`),
    never as a side effect of pruning run history.
    """
    rid = int(run_id)
    with hs._lock, hs._connect() as conn:
        results = conn.execute("DELETE FROM run_results WHERE run_id = ?", (rid,)).rowcount or 0
        runs = conn.execute("DELETE FROM analysis_runs WHERE id = ?", (rid,)).rowcount or 0
    return {"runs": runs, "results": results}


def delete_runs(hs: SQLiteHistoryStore, run_ids: list[int]) -> dict[str, int]:
    """Hard-delete a batch of runs in a single transaction.

    Returns aggregate counts ``{"runs": n, "results": m}``. Ids that
    don't exist are silently skipped. See :func:`delete_run` for the
    ``apply_events`` boundary.
    """
    ids = [int(r) for r in run_ids if r is not None]
    if not ids:
        return {"runs": 0, "results": 0}
    placeholders = ",".join("?" for _ in ids)
    with hs._lock, hs._connect() as conn:
        results = (
            conn.execute(f"DELETE FROM run_results WHERE run_id IN ({placeholders})", ids).rowcount
            or 0
        )
        runs = (
            conn.execute(f"DELETE FROM analysis_runs WHERE id IN ({placeholders})", ids).rowcount
            or 0
        )
    return {"runs": runs, "results": results}


def delete_runs_matching(
    hs: SQLiteHistoryStore,
    *,
    q: str | None = None,
    status: str | None = None,
    kind: str | None = None,
    command_filter: str | None = None,
    comparable_only: bool = False,
) -> dict[str, int]:
    """Hard-delete every run matching the same filter the Runs list uses.

    Reuses :func:`_runs_where` so "delete all matching filter" removes
    exactly the rows the current filtered view shows (and their
    ``run_results`` children) in one transaction — no id round-trip, no
    SQLite variable-count ceiling. See :func:`delete_run` for the
    ``apply_events`` boundary.

    A guard: an empty filter body would delete the entire history, so
    the caller must pass at least one constraint. With no filter this
    raises ``ValueError`` rather than silently wiping everything.
    """
    where_body, params = _runs_where(
        q=q,
        status=status,
        kind=kind,
        command_filter=command_filter,
        comparable_only=comparable_only,
    )
    if not where_body:
        raise ValueError(
            "delete_runs_matching requires at least one filter "
            "(q/status/kind/command_filter/comparable_only)."
        )
    with hs._lock, hs._connect() as conn:
        results = (
            conn.execute(
                f"DELETE FROM run_results WHERE run_id IN "
                f"(SELECT id FROM analysis_runs WHERE {where_body})",
                params,
            ).rowcount
            or 0
        )
        runs = conn.execute(f"DELETE FROM analysis_runs WHERE {where_body}", params).rowcount or 0
    return {"runs": runs, "results": results}


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
    hs: SQLiteHistoryStore,
    command_filter: str | None = "analyze.run",
    *,
    comparable_only: bool = False,
) -> int:
    """Global count of runs that still have unreviewed result rows.

    Covers both ``ready_for_review`` (nothing applied yet) and
    ``applied_partial`` (some applied, some still pending) — the
    two terminal states where human review work remains. Used by
    the Studio Landing "pending review" chip so the badge reflects
    the whole table, not just the most-recent feed slice.

    ``comparable_only=True`` mirrors the comparable-runs filter used by
    the Compare picker feed so the ``/api/history/runs`` endpoint can
    report a count consistent with the rows it returns. It combines (AND)
    with ``command_filter`` when both are given.
    """
    where = "status IN ('ready_for_review','applied_partial')"
    params: tuple[Any, ...] = ()
    if comparable_only:
        from amx.storage.run_kinds import comparable_sql

        frag, frag_params = comparable_sql("command")
        where += f" AND {frag}"
        params = params + tuple(frag_params)
    if command_filter:
        where += " AND command = ?"
        params = params + (command_filter,)
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
