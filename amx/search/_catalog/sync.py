"""Catalog sync orchestration for ``SearchCatalog``.

Sync = "import or update" rows from external sources (live DB
metadata, /run-apply review decisions, codebase analyzer output).
The cluster covers ingestion + status + housekeeping:

* ``sync_table_profile`` / ``_sync_table_profile_conn`` — main entry
  point used by /search sync.
* ``sync_review_decision`` — record /run-apply human picks.
* ``sync_generated_suggestions`` — bulk-import LLM proposals.
* ``sync_code_report`` — wire the codebase analyzer's results.
* ``start_sync_job`` / ``finish_sync_job`` / ``sync_status`` /
  ``sources_status`` — job tracking + UI rendering.
* ``rebuild_profile`` — drop and re-import a single table profile.
* ``clear_code_evidence`` — wipe code-source evidence rows for a
  profile.

Calls back into ``EntityCrudMixin`` (``_upsert_entity``,
``_insert_description``, ``_resolve_effective_description``,
``_index_entity``, ``_update_search_text``) for the actual row
writes.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable
from typing import Any

from amx.agents.base import MetadataSuggestion
from amx.codebase.analyzer import CodebaseReport
from amx.db.connector import TableProfile
from amx.utils.logging import get_logger

log = get_logger("search.catalog.sync")


class SyncMixin:
    """Catalog sync orchestration for ``SearchCatalog``."""

    def start_sync_job(self, db_profile: str, job_type: str, scope: dict[str, Any] | None = None) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO catalog_sync_jobs (db_profile, job_type, scope_json, started_at, status)
                VALUES (?, ?, ?, ?, ?)
                """,
                (db_profile, job_type, json.dumps(scope or {}, ensure_ascii=True), time.time(), "running"),
            )
            return int(cur.lastrowid)
    def finish_sync_job(
        self,
        job_id: int,
        *,
        status: str,
        inserted_count: int = 0,
        updated_count: int = 0,
        error_text: str = "",
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE catalog_sync_jobs
                SET completed_at = ?,
                    status = ?,
                    inserted_count = ?,
                    updated_count = ?,
                    error_text = ?
                WHERE id = ?
                """,
                (time.time(), status, inserted_count, updated_count, error_text[:4000], int(job_id)),
            )
    def sync_table_profile(
        self,
        *,
        db_profile: str,
        db_backend: str,
        database_name: str,
        profile: TableProfile,
        query_usage: dict[str, object] | None = None,
    ) -> None:
        with self._connect() as conn:
            self._sync_table_profile_conn(
                conn,
                db_profile=db_profile,
                db_backend=db_backend,
                database_name=database_name,
                profile=profile,
                query_usage=query_usage,
            )
    def _sync_table_profile_conn(
        self,
        conn: sqlite3.Connection,
        *,
        db_profile: str,
        db_backend: str,
        database_name: str,
        profile: TableProfile,
        query_usage: dict[str, object] | None = None,
    ) -> int:
        table_entity_id = self._upsert_entity(
            conn,
            db_profile=db_profile,
            db_backend=db_backend,
            database_name=database_name,
            schema_name=profile.schema,
            table_name=profile.name,
            column_name=None,
            entity_kind="table",
            asset_kind=profile.asset_kind.value,
            dtype="",
            nullable=1,
            pk_flag=1 if profile.primary_key else 0,
            fk_flag=1 if profile.foreign_keys or profile.referenced_by else 0,
            row_count=profile.row_count,
        )
        for column in profile.columns:
            entity_id = self._upsert_entity(
                conn,
                db_profile=db_profile,
                db_backend=db_backend,
                database_name=database_name,
                schema_name=profile.schema,
                table_name=profile.name,
                column_name=column.name,
                entity_kind="column",
                asset_kind=profile.asset_kind.value,
                dtype=column.dtype,
                nullable=1 if column.nullable else 0,
                pk_flag=1 if column.name in profile.primary_key else 0,
                fk_flag=1 if any(column.name in (fk.get("constrained_columns") or []) for fk in profile.foreign_keys) else 0,
                row_count=profile.row_count,
            )
            if column.existing_comment:
                self._insert_description(
                    conn,
                    entity_id=entity_id,
                    description_text=str(column.existing_comment),
                    source_kind="imported",
                    source_agent="database",
                    confidence="medium",
                )
                self._resolve_effective_description(conn, entity_id)
        if profile.existing_comment:
            self._insert_description(
                conn,
                entity_id=table_entity_id,
                description_text=str(profile.existing_comment),
                source_kind="imported",
                source_agent="database",
                confidence="medium",
            )
        self._resolve_effective_description(conn, table_entity_id)
        conn.execute("DELETE FROM catalog_relationships WHERE from_entity_id = ?", (table_entity_id,))
        for fk in profile.foreign_keys:
            target_id = self._upsert_entity(
                conn,
                db_profile=db_profile,
                db_backend=db_backend,
                database_name=database_name,
                schema_name=str(fk.get("referred_schema") or profile.schema),
                table_name=str(fk.get("referred_table") or ""),
                column_name=None,
                entity_kind="table",
                asset_kind="table",
            )
            conn.execute(
                """
                INSERT INTO catalog_relationships (
                    from_entity_id, to_entity_id, relationship_type, score, source, details_json, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    table_entity_id,
                    target_id,
                    "foreign_key",
                    10.0,
                    "database",
                    json.dumps(fk, ensure_ascii=True),
                    time.time(),
                ),
            )
        for fk in profile.referenced_by:
            source_id = self._upsert_entity(
                conn,
                db_profile=db_profile,
                db_backend=db_backend,
                database_name=database_name,
                schema_name=str(fk.get("source_schema") or profile.schema),
                table_name=str(fk.get("source_table") or ""),
                column_name=None,
                entity_kind="table",
                asset_kind="table",
            )
            conn.execute(
                """
                INSERT INTO catalog_relationships (
                    from_entity_id, to_entity_id, relationship_type, score, source, details_json, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    table_entity_id,
                    "incoming_foreign_key",
                    10.0,
                    "database",
                    json.dumps(fk, ensure_ascii=True),
                    time.time(),
                ),
            )
        if query_usage:
            self._store_query_usage(conn, table_entity_id, profile, query_usage, db_profile=db_profile, schema_name=profile.schema)
        self._update_search_text(conn, table_entity_id)
        self._index_entity(conn, table_entity_id)
        for row in conn.execute(
            "SELECT id FROM catalog_entities WHERE db_profile = ? AND schema_name = ? AND table_name = ?",
            (db_profile, profile.schema, profile.name),
        ).fetchall():
            self._update_search_text(conn, int(row["id"]))
            self._index_entity(conn, int(row["id"]))
        return table_entity_id
    def sync_generated_suggestions(
        self,
        *,
        db_profile: str,
        db_backend: str,
        database_name: str,
        run_id: int | None,
        profile: TableProfile,
        suggestions: list[MetadataSuggestion],
        result_id_map: dict[str | None, int],
        query_usage: dict[str, object] | None = None,
    ) -> None:
        with self._connect() as conn:
            self._sync_table_profile_conn(
                conn,
                db_profile=db_profile,
                db_backend=db_backend,
                database_name=database_name,
                profile=profile,
                query_usage=query_usage,
            )
            for suggestion in suggestions:
                entity_id = self._upsert_entity(
                    conn,
                    db_profile=db_profile,
                    db_backend=db_backend,
                    database_name=database_name,
                    schema_name=suggestion.schema,
                    table_name=suggestion.table,
                    column_name=suggestion.column,
                    entity_kind="column" if suggestion.column else "table",
                    asset_kind=profile.asset_kind.value,
                )
                result_id = result_id_map.get(suggestion.column)
                for idx, alternative in enumerate(suggestion.suggestions):
                    self._insert_description(
                        conn,
                        entity_id=entity_id,
                        description_text=alternative,
                        source_kind="generated",
                        source_agent=suggestion.source,
                        confidence=suggestion.confidence.value,
                        logprob_score=suggestion.logprob_score,
                        reasoning=suggestion.reasoning,
                        run_id=run_id,
                        result_id=result_id,
                        chosen=idx == 0,
                    )
                winner = self._resolve_effective_description(conn, entity_id)
                self._index_entity(conn, entity_id)
                if result_id is not None:
                    self._mark_run_result_state(
                        conn,
                        result_id,
                        catalog_status="generated",
                        effective_source_kind="generated" if winner else "",
                    )
    def sync_review_decision(self, result_id: int, *, chosen_description: str, evaluation: str) -> None:
        source_kind = "reviewed" if evaluation in {"accepted", "custom"} else "rejected"
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT rr.*, ar.db_profile, ar.db_backend
                FROM run_results rr
                LEFT JOIN analysis_runs ar ON ar.id = rr.run_id
                WHERE rr.id = ?
                """,
                (result_id,),
            ).fetchone()
            if not row:
                return
            entity = conn.execute(
                """
                SELECT id FROM catalog_entities
                WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND
                      COALESCE(column_name, '') = COALESCE(?, '') AND entity_kind = ?
                """,
                (
                    str(row["db_profile"] or "default"),
                    str(row["schema_name"] or ""),
                    str(row["table_name"] or ""),
                    row["column_name"],
                    "column" if row["column_name"] else "table",
                ),
            ).fetchone()
            if not entity:
                return
            entity_id = int(entity["id"])
            if chosen_description:
                self._insert_description(
                    conn,
                    entity_id=entity_id,
                    description_text=chosen_description,
                    source_kind=source_kind,
                    source_agent="history.review",
                    confidence=str(row["confidence"] or "medium"),
                    logprob_score=row["logprob_score"],
                    reasoning=str(row["reasoning"] or ""),
                    run_id=int(row["run_id"]),
                    result_id=result_id,
                    chosen=True,
                )
            winner = self._resolve_effective_description(conn, entity_id)
            self._index_entity(conn, entity_id)
            self._mark_run_result_state(
                conn,
                result_id,
                catalog_status=source_kind,
                effective_source_kind=source_kind if winner else "",
                rejection_reason="" if source_kind != "rejected" else "skipped during review",
            )
    def clear_code_evidence(self, db_profile: str, source_path: str | None = None) -> None:
        with self._connect() as conn:
            if source_path:
                conn.execute(
                    "DELETE FROM catalog_usage_evidence WHERE db_profile = ? AND source_path = ? AND source_kind = 'code'",
                    (db_profile, source_path),
                )
            else:
                conn.execute(
                    "DELETE FROM catalog_usage_evidence WHERE db_profile = ? AND source_kind = 'code'",
                    (db_profile,),
                )
    def sync_code_report(
        self,
        *,
        db_profile: str,
        db_backend: str,
        database_name: str,
        schema_name: str,
        source_path: str,
        report: CodebaseReport,
    ) -> tuple[int, int]:
        inserted = 0
        updated = 0
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM catalog_usage_evidence WHERE db_profile = ? AND source_kind = 'code' AND source_path = ?",
                (db_profile, source_path),
            )
            ref_keys = report.references or {}
            for key, refs in ref_keys.items():
                matches = conn.execute(
                    """
                    SELECT id, entity_kind FROM catalog_entities
                    WHERE db_profile = ? AND schema_name = ?
                      AND (LOWER(table_name) = ? OR LOWER(COALESCE(column_name, '')) = ?)
                    """,
                    (db_profile, schema_name, key.lower(), key.lower()),
                ).fetchall()
                if not matches:
                    # Bootstrap table entities from code references when possible.
                    if "." not in key:
                        entity_id = self._upsert_entity(
                            conn,
                            db_profile=db_profile,
                            db_backend=db_backend,
                            database_name=database_name,
                            schema_name=schema_name,
                            table_name=key,
                            column_name=None,
                            entity_kind="table",
                            asset_kind="table",
                        )
                        row = conn.execute(
                            "SELECT id, entity_kind FROM catalog_entities WHERE id = ?",
                            (entity_id,),
                        ).fetchone()
                        matches = [row] if row else []
                for match in matches:
                    snippets = [ref.line_text[:240] for ref in refs[:3]]
                    evidence_type = "table_usage" if match["entity_kind"] == "table" else "column_usage"
                    conn.execute(
                        """
                        INSERT INTO catalog_usage_evidence (
                            db_profile, entity_id, source_kind, evidence_type, source_path,
                            count_value, score_value, sample_snippets_json, last_seen
                        ) VALUES (?, ?, 'code', ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            db_profile,
                            int(match["id"]),
                            evidence_type,
                            source_path,
                            len(refs),
                            float(len(refs)),
                            json.dumps(snippets, ensure_ascii=True),
                            time.time(),
                        ),
                    )
                    self._update_search_text(conn, int(match["id"]))
                    self._index_entity(conn, int(match["id"]))
                    updated += 1
            join_pairs = self._extract_join_pairs(ref_keys)
            for left, right, score, evidence in join_pairs:
                left_entity = conn.execute(
                    """
                    SELECT id FROM catalog_entities
                    WHERE db_profile = ? AND schema_name = ? AND LOWER(table_name) = ? AND entity_kind = 'table'
                    """,
                    (db_profile, schema_name, left.lower()),
                ).fetchone()
                right_entity = conn.execute(
                    """
                    SELECT id FROM catalog_entities
                    WHERE db_profile = ? AND schema_name = ? AND LOWER(table_name) = ? AND entity_kind = 'table'
                    """,
                    (db_profile, schema_name, right.lower()),
                ).fetchone()
                if not left_entity or not right_entity:
                    continue
                conn.execute(
                    """
                    INSERT INTO catalog_relationships (
                        from_entity_id, to_entity_id, relationship_type, score, source, details_json, last_seen
                    ) VALUES (?, ?, 'code_observed_join', ?, 'code', ?, ?)
                    """,
                    (
                        int(left_entity["id"]),
                        int(right_entity["id"]),
                        score,
                        json.dumps({"evidence": evidence}, ensure_ascii=True),
                        time.time(),
                    ),
                )
                inserted += 1
        return inserted, updated
    def rebuild_profile(
        self,
        db_profile: str,
        *,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> tuple[int, int]:
        job_id = self.start_sync_job(db_profile, "rebuild", {"db_profile": db_profile})
        inserted = 0
        updated = 0
        try:
            with self._connect() as conn:
                entities = conn.execute(
                    "SELECT id FROM catalog_entities WHERE db_profile = ? ORDER BY id",
                    (db_profile,),
                ).fetchall()
                self.index.reset_profile(db_profile)
                total = len(entities)
                for index, row in enumerate(entities, start=1):
                    entity_id = int(row["id"])
                    self._resolve_effective_description(conn, entity_id)
                    self._index_entity(conn, entity_id)
                    updated += 1
                    if on_progress is not None:
                        on_progress(index, total)
                self.finish_sync_job(job_id, status="success", inserted_count=inserted, updated_count=updated)
            return inserted, updated
        except Exception as exc:
            self.finish_sync_job(job_id, status="failed", inserted_count=inserted, updated_count=updated, error_text=str(exc))
            raise
    def sync_status(self, db_profile: str) -> dict[str, Any]:
        settings = self.get_settings(db_profile)
        with self._connect() as conn:
            entities = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_entities,
                    SUM(CASE WHEN effective_description_id IS NOT NULL THEN 1 ELSE 0 END) AS effective_entities,
                    MAX(last_synced_at) AS last_synced_at
                FROM catalog_entities
                WHERE db_profile = ?
                """,
                (db_profile,),
            ).fetchone()
            descriptions = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_descriptions,
                    SUM(CASE WHEN source_kind = 'manual' THEN 1 ELSE 0 END) AS manual_count,
                    SUM(CASE WHEN source_kind = 'reviewed' THEN 1 ELSE 0 END) AS reviewed_count,
                    SUM(CASE WHEN source_kind = 'generated' THEN 1 ELSE 0 END) AS generated_count,
                    SUM(CASE WHEN source_kind = 'rejected' THEN 1 ELSE 0 END) AS rejected_count
                FROM catalog_descriptions cd
                JOIN catalog_entities ce ON ce.id = cd.entity_id
                WHERE ce.db_profile = ?
                """,
                (db_profile,),
            ).fetchone()
            jobs = conn.execute(
                """
                SELECT job_type, status, started_at, completed_at, inserted_count, updated_count
                FROM catalog_sync_jobs
                WHERE db_profile = ?
                ORDER BY started_at DESC
                LIMIT 5
                """,
                (db_profile,),
            ).fetchall()
        return {
            "entities": dict(entities or {}),
            "descriptions": dict(descriptions or {}),
            "jobs": [dict(row) for row in jobs],
            "settings": settings,
        }
    def sources_status(self, db_profile: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT source_kind, evidence_type, COUNT(*) AS count_rows, MAX(last_seen) AS last_seen
                FROM catalog_usage_evidence
                WHERE db_profile = ?
                GROUP BY source_kind, evidence_type
                ORDER BY source_kind, evidence_type
                """,
                (db_profile,),
            ).fetchall()
        return [dict(row) for row in rows]


__all__ = ["SyncMixin"]
