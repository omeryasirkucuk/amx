"""Apply-event audit trail extracted from :mod:`amx.storage.sqlite_store`.

Six methods that drive the run_results write + apply_events audit log:
- save_run_results (batch INSERT into run_results)
- record_evaluation (UPDATE run_results after the user reviews)
- record_applied / record_db_apply_failure (DB writeback signals)
- record_apply_event / list_apply_events (apply_events audit table)

Each function takes the store as ``hs`` and reuses its ``_lock`` +
``_connect()`` plumbing. No DDL touched.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from amx.storage.sqlite_store import build_alternatives_json

if TYPE_CHECKING:
    from amx.storage.sqlite_store import SQLiteHistoryStore


def save_run_results(
    hs: SQLiteHistoryStore,
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
    with hs._lock, hs._connect() as conn:
        for s in suggestions:
            cur = conn.execute(
                """
                INSERT INTO run_results (
                    run_id, saved_at, schema_name, table_name, column_name,
                    asset_kind, source, confidence, logprob_score, raw_logprob,
                    token_count, model_version, reasoning, alternatives_json,
                    parent_result_id, rerun_seq, user_instructions, citations_json,
                    alternatives_mode, seed_alternative_id, seed_alternative_text,
                    parent_run_id, model, provider, production_warning
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    build_alternatives_json(s),
                    s.get("parent_result_id"),
                    int(s.get("rerun_seq", 0) or 0),
                    s.get("user_instructions"),
                    # ``citations`` is a list of plain dicts at
                    # this layer; the orchestrator serializer
                    # converted dataclasses upstream so SQLite
                    # only sees JSON-safe values. ``None`` on
                    # non-RAG suggestions keeps the column space
                    # cheap for legacy rows.
                    (
                        json.dumps(s.get("citations") or [], ensure_ascii=True)
                        if s.get("citations")
                        else None
                    ),
                    # ``alternatives_mode`` captures the diversity mode
                    # active when this row's alternatives were generated,
                    # per Definition 1: ``semantic`` ⇒ paraphrases of
                    # DESCRIPTION_1; ``lexical`` ⇒ shared vocabulary with
                    # DESCRIPTION_1, meaning may drift. ``None`` on
                    # legacy / non-LLM rows is treated as "not recorded"
                    # by the review UI.
                    s.get("alternatives_mode"),
                    # Variations audit (NULL on non-variations rows).
                    s.get("seed_alternative_id"),
                    s.get("seed_alternative_text"),
                    s.get("parent_run_id"),
                    # Per-row LLM identity — captures the effective model /
                    # provider in use when the alternatives were generated.
                    # Needed when a per-run profile override was applied,
                    # since analysis_runs.llm_model / llm_provider would
                    # still report the base profile's values.
                    s.get("model"),
                    s.get("provider"),
                    # Under-production audit (NULL on success path).
                    s.get("production_warning"),
                ),
            )
            ids.append(int(cur.lastrowid))
    return ids


def record_evaluation(
    hs: SQLiteHistoryStore,
    result_id: int,
    *,
    chosen_description: str,
    evaluation: str,  # 'accepted' | 'skipped' | 'custom'
) -> None:
    """Record the user's evaluation decision for one run_result row."""
    now = time.time()
    with hs._lock, hs._connect() as conn:
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


def record_applied(
    hs: SQLiteHistoryStore,
    result_id: int,
    *,
    chosen_description: str | None = None,
) -> None:
    """Record when a reviewed description was successfully applied to DB.

    When ``chosen_description`` is provided it backfills
    ``run_results.chosen_description`` *only* if the column is still empty.
    The interactive review path (``record_evaluation``) wins when both ran,
    but non-interactive apply paths (Studio "Apply pending", CLI
    ``/history apply``) finally persist the text actually written to the
    live DB, so ``describe_run`` / chat answers can quote it later.
    """
    now = time.time()
    with hs._lock, hs._connect() as conn:
        if chosen_description:
            conn.execute(
                """
                UPDATE run_results
                SET applied_at = ?,
                    db_applied_status = 'applied',
                    rejection_reason = '',
                    chosen_description = COALESCE(NULLIF(chosen_description, ''), ?)
                WHERE id = ?
                """,
                (now, chosen_description, result_id),
            )
        else:
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


def record_db_apply_failure(hs: SQLiteHistoryStore, result_id: int, error_text: str = "") -> None:
    """Record when a reviewed description failed during DB write-back."""
    with hs._lock, hs._connect() as conn:
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
    hs: SQLiteHistoryStore,
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
    with hs._lock, hs._connect() as conn:
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
    hs: SQLiteHistoryStore,
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
    with hs._lock, hs._connect() as conn:
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
