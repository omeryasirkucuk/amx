"""Usage / history / manual-record bookkeeping for ``SearchCatalog``.

Six methods that track WHO touched WHAT and persist explicit
manual/dedup decisions:

* ``record_manual_description`` — write-through for
  ``/metadata edit`` (manual comment on a column / table).
* ``record_dedup_decision`` — write-through for the equivalence
  dedup pass (one description fanned out to multiple members).
* ``mark_applied`` / ``_mark_run_result_state`` — flag /run-apply
  review picks as applied / rejected / skipped in the catalog.
* ``_store_query_usage`` — log /ask query terms for analytics.
* ``history_counts`` — render-side aggregation for /history.
"""

from __future__ import annotations

import json
import sqlite3
import time

from amx.db.connector import TableProfile
from amx.utils.logging import get_logger

log = get_logger("search.catalog.usage")


class UsageMixin:
    """Usage / history / manual-record methods for ``SearchCatalog``."""

    def _mark_run_result_state(
        self,
        conn: sqlite3.Connection,
        result_id: int,
        *,
        catalog_status: str,
        effective_source_kind: str,
        db_applied_status: str = "",
        rejection_reason: str = "",
        superseded_at: float | None = None,
    ) -> None:
        conn.execute(
            """
            UPDATE run_results
            SET catalog_status = ?,
                catalog_indexed_at = ?,
                db_applied_status = COALESCE(NULLIF(?, ''), db_applied_status),
                effective_source_kind = ?,
                rejection_reason = COALESCE(NULLIF(?, ''), rejection_reason),
                superseded_at = COALESCE(?, superseded_at)
            WHERE id = ?
            """,
            (
                catalog_status,
                time.time(),
                db_applied_status,
                effective_source_kind,
                rejection_reason,
                superseded_at,
                result_id,
            ),
        )

    def _store_query_usage(
        self,
        conn: sqlite3.Connection,
        table_entity_id: int,
        profile: TableProfile,
        query_usage: dict[str, object],
        *,
        db_profile: str,
        schema_name: str,
    ) -> None:
        now = time.time()
        conn.execute(
            "DELETE FROM catalog_usage_evidence WHERE entity_id = ? AND source_kind = 'query_usage'",
            (table_entity_id,),
        )
        table_mentions = int(query_usage.get("table_mentions") or 0)
        sql_like_mentions = int(query_usage.get("sql_like_table_mentions") or 0)
        conn.execute(
            """
            INSERT INTO catalog_usage_evidence (
                db_profile, entity_id, source_kind, evidence_type, count_value, score_value, sample_snippets_json, last_seen
            ) VALUES (?, ?, 'query_usage', ?, ?, ?, ?, ?)
            """,
            (
                db_profile,
                table_entity_id,
                "table_usage",
                table_mentions,
                float(table_mentions) + float(sql_like_mentions) * 0.5,
                json.dumps([], ensure_ascii=True),
                now,
            ),
        )
        top_usage = query_usage.get("top_column_usage") or []
        for item in top_usage:
            column = str((item or {}).get("column") or "")
            if not column:
                continue
            row = conn.execute(
                """
                SELECT id FROM catalog_entities
                WHERE id != ? AND db_profile = ? AND schema_name = ? AND table_name = ?
                  AND COALESCE(column_name, '') = ? AND entity_kind = 'column'
                """,
                (table_entity_id, db_profile, schema_name, profile.name, column),
            ).fetchone()
            if not row:
                continue
            conn.execute(
                """
                INSERT INTO catalog_usage_evidence (
                    db_profile, entity_id, source_kind, evidence_type, count_value, score_value, sample_snippets_json, last_seen
                ) VALUES (?, ?, 'query_usage', ?, ?, ?, ?, ?)
                """,
                (
                    db_profile,
                    int(row["id"]),
                    "column_usage",
                    int((item or {}).get("mentions") or 0),
                    float((item or {}).get("mentions") or 0),
                    json.dumps((item or {}).get("sample_sql_lines") or [], ensure_ascii=True),
                    now,
                ),
            )

    def mark_applied(self, result_id: int) -> None:
        with self._connect() as conn:
            desc = conn.execute(
                "SELECT id FROM catalog_descriptions WHERE result_id = ? ORDER BY created_at DESC LIMIT 1",
                (result_id,),
            ).fetchone()
            if desc:
                conn.execute(
                    """
                    UPDATE catalog_descriptions
                    SET applied_to_db = 1, applied_at = ?
                    WHERE id = ?
                    """,
                    (time.time(), int(desc["id"])),
                )
            self._mark_run_result_state(
                conn,
                result_id,
                catalog_status="applied",
                effective_source_kind="reviewed",
                db_applied_status="applied",
            )

    def record_manual_description(
        self,
        *,
        db_profile: str,
        db_backend: str,
        database_name: str,
        schema_name: str,
        table_name: str,
        column_name: str | None,
        entity_kind: str,
        asset_kind: str,
        description: str,
    ) -> None:
        with self._connect() as conn:
            entity_id = self._upsert_entity(
                conn,
                db_profile=db_profile,
                db_backend=db_backend,
                database_name=database_name,
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                entity_kind=entity_kind,
                asset_kind=asset_kind,
            )
            self._insert_description(
                conn,
                entity_id=entity_id,
                description_text=description,
                source_kind="manual",
                source_agent="manual.edit",
                confidence="high",
                chosen=True,
            )
            self._resolve_effective_description(conn, entity_id)
            self._index_entity(conn, entity_id)

    def record_dedup_decision(
        self,
        *,
        db_profile: str,
        db_backend: str,
        run_id: int,
        schema_name: str,
        table_name: str,
        column_name: str,
        description: str,
        equivalence_key: str,
        member_count: int,
    ) -> None:
        """Persist an equivalence-class decision for one column member.

        The dedup pass produces a single description per class and applies
        it to every member; this method records the decision in the
        catalog (so /ask sees the new description) and tags it with
        ``source_kind='dedup'`` plus a ``source_agent`` string carrying
        the equivalence key + run id, so /history reporting can later
        count "12 classes (145 columns)".
        """
        with self._connect() as conn:
            entity_id = self._upsert_entity(
                conn,
                db_profile=db_profile,
                db_backend=db_backend,
                database_name="",
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                entity_kind="column",
                asset_kind="column",
            )
            self._insert_description(
                conn,
                entity_id=entity_id,
                description_text=description,
                source_kind="dedup",
                source_agent=f"equivalence:{equivalence_key}:run={run_id}:n={member_count}",
                confidence="high",
                chosen=True,
            )
            self._resolve_effective_description(conn, entity_id)
            self._index_entity(conn, entity_id)

    def history_counts(self) -> dict[str, int]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN source_kind = 'reviewed' THEN 1 ELSE 0 END) AS reviewed_count,
                    SUM(CASE WHEN source_kind = 'rejected' THEN 1 ELSE 0 END) AS rejected_count,
                    SUM(CASE WHEN source_kind = 'manual' THEN 1 ELSE 0 END) AS manual_count,
                    SUM(CASE WHEN indexed = 1 THEN 1 ELSE 0 END) AS indexed_count,
                    SUM(CASE WHEN applied_to_db = 1 THEN 1 ELSE 0 END) AS applied_count
                FROM catalog_descriptions
                """
            ).fetchone()
            stale_row = conn.execute(
                "SELECT COUNT(*) AS stale_count FROM catalog_entities WHERE effective_status = 'stale'"
            ).fetchone()
        out = dict(row or {})
        out["stale_count"] = int(stale_row["stale_count"] if stale_row else 0)
        return {key: int(value or 0) for key, value in out.items()}


__all__ = ["UsageMixin"]
