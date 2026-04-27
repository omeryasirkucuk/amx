"""SQLite-backed search catalog built on top of the AMX history DB."""

from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from amx.agents.base import MetadataSuggestion
from amx.codebase.analyzer import CodebaseReport, CodeReference
from amx.db.connector import AssetKind, TableProfile
from amx.search.index import SearchIndex
from amx.storage.sqlite_store import history_store
from amx.utils.logging import get_logger

log = get_logger("search.catalog")

SOURCE_PRIORITY = {
    "manual": 4,
    "reviewed": 3,
    "generated": 2,
    "imported": 1,
    "rejected": 0,
}

DEFAULT_SETTINGS: dict[str, str] = {
    "auto_sync_on_writeback": "true",
    "enable_generated_metadata": "true",
    "enable_manual_metadata": "true",
    "enable_reviewed_metadata": "true",
    "enable_code_evidence": "true",
    "enable_vector_search": "true",
    "enable_exact_search": "true",
    "manual_weight": "6.0",
    "reviewed_weight": "4.5",
    "generated_weight": "3.0",
    "code_evidence_weight": "2.0",
    "freshness_weight": "1.0",
    "show_provenance": "true",
    "show_confidence": "true",
    "max_results": "8",
}


def _json_loads(raw: Any, default: Any) -> Any:
    if not isinstance(raw, str) or not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _database_name(database_name: str | None, catalog_name: str | None, project: str | None) -> str:
    for value in (database_name, catalog_name, project):
        if value:
            return str(value)
    return ""


@dataclass
class SearchAnswer:
    intent: str
    question: str
    rows: list[dict[str, Any]]
    confidence: str
    summary: str
    provenance: list[str]
    details: dict[str, Any]


class SearchCatalog:
    """Manage catalog rows and sync/search operations."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.index = SearchIndex()

    @classmethod
    def from_history_store(cls) -> "SearchCatalog | None":
        hs = history_store()
        if hs is None:
            return None
        return cls(hs.db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def get_settings(self, db_profile: str) -> dict[str, str]:
        out = dict(DEFAULT_SETTINGS)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT key_name, value_text FROM search_settings WHERE db_profile = ?",
                (db_profile,),
            ).fetchall()
        for row in rows:
            out[str(row["key_name"])] = str(row["value_text"])
        return out

    def set_setting(self, db_profile: str, key: str, value: str) -> None:
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO search_settings (db_profile, key_name, value_text, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(db_profile, key_name) DO UPDATE SET
                    value_text = excluded.value_text,
                    updated_at = excluded.updated_at
                """,
                (db_profile, key, value, now),
            )

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

    def _upsert_entity(
        self,
        conn: sqlite3.Connection,
        *,
        db_profile: str,
        db_backend: str,
        database_name: str,
        schema_name: str,
        table_name: str,
        column_name: str | None,
        entity_kind: str,
        asset_kind: str = "table",
        dtype: str = "",
        nullable: int = 1,
        pk_flag: int = 0,
        fk_flag: int = 0,
        row_count: int = 0,
    ) -> int:
        now = time.time()
        row = conn.execute(
            """
            SELECT id FROM catalog_entities
            WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND
                  COALESCE(column_name, '') = COALESCE(?, '') AND entity_kind = ?
            """,
            (db_profile, schema_name, table_name, column_name, entity_kind),
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE catalog_entities
                SET db_backend = ?, database_name = ?, asset_kind = ?, dtype = ?, nullable = ?,
                    pk_flag = ?, fk_flag = ?, row_count = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    db_backend,
                    database_name,
                    asset_kind,
                    dtype,
                    nullable,
                    pk_flag,
                    fk_flag,
                    row_count,
                    now,
                    int(row["id"]),
                ),
            )
            return int(row["id"])

        cur = conn.execute(
            """
            INSERT INTO catalog_entities (
                db_profile, db_backend, database_name, schema_name, table_name, column_name,
                entity_kind, asset_kind, dtype, nullable, pk_flag, fk_flag, row_count,
                updated_at, last_synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                db_profile,
                db_backend,
                database_name,
                schema_name,
                table_name,
                column_name,
                entity_kind,
                asset_kind,
                dtype,
                nullable,
                pk_flag,
                fk_flag,
                row_count,
                now,
                now,
            ),
        )
        return int(cur.lastrowid)

    def _insert_description(
        self,
        conn: sqlite3.Connection,
        *,
        entity_id: int,
        description_text: str,
        source_kind: str,
        source_agent: str,
        confidence: str,
        logprob_score: float | None = None,
        reasoning: str = "",
        run_id: int | None = None,
        result_id: int | None = None,
        chosen: bool = False,
    ) -> int:
        now = time.time()
        cur = conn.execute(
            """
            INSERT INTO catalog_descriptions (
                entity_id, description_text, source_kind, source_agent, confidence,
                logprob_score, reasoning, run_id, result_id, created_at,
                superseded, indexed, chosen_description
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?)
            """,
            (
                entity_id,
                description_text,
                source_kind,
                source_agent,
                confidence,
                logprob_score,
                reasoning,
                run_id,
                result_id,
                now,
                1 if chosen else 0,
            ),
        )
        return int(cur.lastrowid)

    def _update_search_text(self, conn: sqlite3.Connection, entity_id: int) -> None:
        entity = conn.execute(
            """
            SELECT ce.*, cd.description_text AS effective_description
            FROM catalog_entities ce
            LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
            WHERE ce.id = ?
            """,
            (entity_id,),
        ).fetchone()
        if not entity:
            return
        history = conn.execute(
            """
            SELECT description_text, source_kind, confidence
            FROM catalog_descriptions
            WHERE entity_id = ? AND superseded = 0
            ORDER BY created_at DESC
            LIMIT 5
            """,
            (entity_id,),
        ).fetchall()
        rels = conn.execute(
            """
            SELECT relationship_type, details_json, score
            FROM catalog_relationships
            WHERE from_entity_id = ? OR to_entity_id = ?
            ORDER BY score DESC, last_seen DESC
            LIMIT 8
            """,
            (entity_id, entity_id),
        ).fetchall()
        evidence = conn.execute(
            """
            SELECT evidence_type, count_value, sample_snippets_json
            FROM catalog_usage_evidence
            WHERE entity_id = ?
            ORDER BY count_value DESC, last_seen DESC
            LIMIT 6
            """,
            (entity_id,),
        ).fetchall()
        parts = [
            f"path={entity['db_profile']}.{entity['schema_name']}.{entity['table_name']}"
            + (f".{entity['column_name']}" if entity["column_name"] else ""),
            f"kind={entity['entity_kind']}",
            f"asset_kind={entity['asset_kind']}",
            f"dtype={entity['dtype'] or ''}",
            f"nullable={entity['nullable']}",
            f"effective_status={entity['effective_status'] or ''}",
            f"effective_source_kind={entity['effective_source_kind'] or ''}",
            f"effective_description={entity['effective_description'] or ''}",
        ]
        for row in history:
            parts.append(
                f"{row['source_kind']}:{row['confidence']}:{row['description_text']}"
            )
        for row in rels:
            details = _json_loads(row["details_json"], {})
            parts.append(
                f"relationship={row['relationship_type']} score={row['score']} details={json.dumps(details, ensure_ascii=True)}"
            )
        for row in evidence:
            snippets = _json_loads(row["sample_snippets_json"], [])
            parts.append(
                f"usage={row['evidence_type']} count={row['count_value']} snippets={' | '.join(snippets[:2])}"
            )
        conn.execute(
            """
            UPDATE catalog_entities
            SET search_text = ?, last_synced_at = ?
            WHERE id = ?
            """,
            ("\n".join(parts), time.time(), entity_id),
        )

    def _resolve_effective_description(self, conn: sqlite3.Connection, entity_id: int) -> int | None:
        rows = conn.execute(
            """
            SELECT id, source_kind, confidence, created_at, description_text
            FROM catalog_descriptions
            WHERE entity_id = ? AND superseded = 0
            ORDER BY created_at DESC
            """,
            (entity_id,),
        ).fetchall()
        winner: sqlite3.Row | None = None
        best_score = -1
        for row in rows:
            priority = SOURCE_PRIORITY.get(str(row["source_kind"]), 0)
            if priority <= 0:
                continue
            score = priority * 1000000 + int(float(row["created_at"] or 0))
            if score > best_score:
                best_score = score
                winner = row
        if winner is None:
            conn.execute(
                """
                UPDATE catalog_entities
                SET effective_description_id = NULL,
                    effective_status = 'stale',
                    effective_source_kind = '',
                    current_confidence = ''
                WHERE id = ?
                """,
                (entity_id,),
            )
            self._update_search_text(conn, entity_id)
            return None
        conn.execute(
            """
            UPDATE catalog_entities
            SET effective_description_id = ?,
                effective_status = ?,
                effective_source_kind = ?,
                current_confidence = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                int(winner["id"]),
                str(winner["source_kind"]),
                str(winner["source_kind"]),
                str(winner["confidence"]),
                time.time(),
                entity_id,
            ),
        )
        conn.execute(
            "UPDATE catalog_descriptions SET indexed = 0 WHERE entity_id = ?",
            (entity_id,),
        )
        self._update_search_text(conn, entity_id)
        return int(winner["id"])

    def _index_entity(self, conn: sqlite3.Connection, entity_id: int) -> None:
        row = conn.execute("SELECT * FROM catalog_entities WHERE id = ?", (entity_id,)).fetchone()
        if not row:
            return
        self.index.upsert_entities([dict(row)])
        conn.execute(
            """
            UPDATE catalog_descriptions
            SET indexed = 1, indexed_at = ?
            WHERE id = (SELECT effective_description_id FROM catalog_entities WHERE id = ?)
            """,
            (time.time(), entity_id),
        )

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

    def _extract_join_pairs(
        self,
        refs: dict[str, list[CodeReference]],
    ) -> list[tuple[str, str, float, list[str]]]:
        file_map: dict[str, set[str]] = {}
        evidence: dict[tuple[str, str], list[str]] = {}
        for key, key_refs in refs.items():
            for ref in key_refs:
                tokens = file_map.setdefault(f"{ref.file}:{ref.line_no}", set())
                tokens.add(key.lower())
                evidence.setdefault((key.lower(), key.lower()), []).append(ref.line_text[:200])
        pairs: dict[tuple[str, str], list[str]] = {}
        for file_line, tokens in file_map.items():
            table_tokens = sorted(t for t in tokens if "." not in t)
            for idx, left in enumerate(table_tokens):
                for right in table_tokens[idx + 1 :]:
                    key = (left, right)
                    pairs.setdefault(key, []).append(file_line)
        out: list[tuple[str, str, float, list[str]]] = []
        for (left, right), lines in pairs.items():
            out.append((left, right, float(len(lines)), lines[:3]))
        return out

    def rebuild_profile(self, db_profile: str) -> tuple[int, int]:
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
                for row in entities:
                    entity_id = int(row["id"])
                    self._resolve_effective_description(conn, entity_id)
                    self._index_entity(conn, entity_id)
                    updated += 1
                self.finish_sync_job(job_id, status="success", inserted_count=inserted, updated_count=updated)
            return inserted, updated
        except Exception as exc:
            self.finish_sync_job(job_id, status="failed", inserted_count=inserted, updated_count=updated, error_text=str(exc))
            raise

    def sync_status(self, db_profile: str) -> dict[str, Any]:
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

    def _tokens(self, text: str) -> list[str]:
        return [token for token in re.split(r"[^a-zA-Z0-9_]+", text.lower()) if token]

    def _exact_candidates(self, db_profile: str, question: str, limit: int = 20) -> list[dict[str, Any]]:
        tokens = self._tokens(question)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE ce.db_profile = ? AND ce.search_text != ''
                """,
                (db_profile,),
            ).fetchall()
        hits: list[dict[str, Any]] = []
        for row in rows:
            search_text = str(row["search_text"] or "").lower()
            score = 0.0
            for token in tokens:
                if token in search_text:
                    score += 1.0
                if token == str(row["column_name"] or "").lower():
                    score += 2.0
                if token == str(row["table_name"] or "").lower():
                    score += 1.5
            if score <= 0:
                continue
            item = dict(row)
            item["match_score"] = score
            hits.append(item)
        hits.sort(key=lambda item: item["match_score"], reverse=True)
        return hits[:limit]

    def search_columns(self, db_profile: str, question: str, limit: int = 8) -> list[dict[str, Any]]:
        settings = self.get_settings(db_profile)
        exact_hits = self._exact_candidates(db_profile, question, limit=max(limit * 2, 10))
        by_id: dict[int, dict[str, Any]] = {}
        for row in exact_hits:
            if row.get("entity_kind") != "column":
                continue
            by_id[int(row["id"])] = row
        if settings.get("enable_vector_search", "true").lower() == "true":
            for hit in self.index.query(question, db_profile=db_profile, n_results=max(limit * 2, 10)):
                entity_id = int((hit.get("metadata") or {}).get("entity_id") or 0)
                if not entity_id:
                    continue
                row = by_id.get(entity_id)
                if row is None:
                    with self._connect() as conn:
                        fetched = conn.execute(
                            "SELECT * FROM catalog_entities WHERE id = ?",
                            (entity_id,),
                        ).fetchone()
                    if not fetched or fetched["entity_kind"] != "column":
                        continue
                    row = dict(fetched)
                    row["match_score"] = 0.0
                    by_id[entity_id] = row
                dist = hit.get("distance")
                if dist is not None:
                    row["match_score"] = float(row.get("match_score") or 0.0) + max(0.0, 3.0 - float(dist))
        return self._rank_rows(list(by_id.values()), settings, limit)

    def _rank_rows(self, rows: list[dict[str, Any]], settings: dict[str, str], limit: int) -> list[dict[str, Any]]:
        weight_map = {
            "manual": float(settings.get("manual_weight", "6.0")),
            "reviewed": float(settings.get("reviewed_weight", "4.5")),
            "generated": float(settings.get("generated_weight", "3.0")),
            "imported": 2.0,
            "rejected": 0.0,
        }
        scored: list[dict[str, Any]] = []
        for row in rows:
            total = float(row.get("match_score") or 0.0)
            total += weight_map.get(str(row.get("effective_source_kind") or ""), 0.0)
            confidence = str(row.get("current_confidence") or "").lower()
            if confidence == "high":
                total += 1.0
            elif confidence == "medium":
                total += 0.5
            row = dict(row)
            row["rank_score"] = total
            scored.append(row)
        scored.sort(key=lambda item: item["rank_score"], reverse=True)
        return scored[:limit]

    def join_candidates(self, db_profile: str, left_path: str, right_path: str, limit: int = 8) -> list[dict[str, Any]]:
        left_parts = left_path.split(".")
        right_parts = right_path.split(".")
        if len(left_parts) != 2 or len(right_parts) != 2:
            raise ValueError("Use schema.table format for join candidates.")
        with self._connect() as conn:
            left = conn.execute(
                """
                SELECT * FROM catalog_entities
                WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND entity_kind = 'table'
                """,
                (db_profile, left_parts[0], left_parts[1]),
            ).fetchone()
            right = conn.execute(
                """
                SELECT * FROM catalog_entities
                WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND entity_kind = 'table'
                """,
                (db_profile, right_parts[0], right_parts[1]),
            ).fetchone()
            if not left or not right:
                return []
            rels = conn.execute(
                """
                SELECT * FROM catalog_relationships
                WHERE (from_entity_id = ? AND to_entity_id = ?)
                   OR (from_entity_id = ? AND to_entity_id = ?)
                ORDER BY score DESC, last_seen DESC
                """,
                (int(left["id"]), int(right["id"]), int(right["id"]), int(left["id"])),
            ).fetchall()
            left_cols = conn.execute(
                "SELECT * FROM catalog_entities WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND entity_kind = 'column'",
                (db_profile, left_parts[0], left_parts[1]),
            ).fetchall()
            right_cols = conn.execute(
                "SELECT * FROM catalog_entities WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND entity_kind = 'column'",
                (db_profile, right_parts[0], right_parts[1]),
            ).fetchall()
        results: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for rel in rels:
            details = _json_loads(rel["details_json"], {})
            constrained = details.get("constrained_columns") or details.get("source_columns") or []
            referred = details.get("referred_columns") or details.get("target_columns") or []
            if constrained and referred:
                for lcol, rcol in zip(constrained, referred):
                    key = (str(lcol), str(rcol))
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append(
                        {
                            "left_column": str(lcol),
                            "right_column": str(rcol),
                            "relationship_type": str(rel["relationship_type"]),
                            "score": float(rel["score"] or 0.0),
                            "source": str(rel["source"] or ""),
                            "details": details,
                        }
                    )
        right_by_name = {str(row["column_name"]).lower(): dict(row) for row in right_cols}
        for left_col in left_cols:
            name = str(left_col["column_name"] or "")
            right_col = right_by_name.get(name.lower())
            if not right_col or (name, name) in seen:
                continue
            score = 6.0
            if str(left_col["dtype"] or "") == str(right_col["dtype"] or ""):
                score += 1.5
            if left_col["pk_flag"] or right_col["pk_flag"]:
                score += 1.0
            results.append(
                {
                    "left_column": name,
                    "right_column": name,
                    "relationship_type": "same_name_candidate",
                    "score": score,
                    "source": "heuristic",
                    "details": {
                        "left_dtype": str(left_col["dtype"] or ""),
                        "right_dtype": str(right_col["dtype"] or ""),
                    },
                }
            )
        results.sort(key=lambda item: item["score"], reverse=True)
        return results[:limit]

    def explain_table(self, db_profile: str, table_path: str) -> dict[str, Any] | None:
        parts = table_path.split(".")
        if len(parts) != 2:
            raise ValueError("Use schema.table format.")
        with self._connect() as conn:
            table = conn.execute(
                """
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE ce.db_profile = ? AND ce.schema_name = ? AND ce.table_name = ? AND ce.entity_kind = 'table'
                """,
                (db_profile, parts[0], parts[1]),
            ).fetchone()
            if not table:
                return None
            cols = conn.execute(
                """
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE ce.db_profile = ? AND ce.schema_name = ? AND ce.table_name = ? AND ce.entity_kind = 'column'
                ORDER BY ce.column_name
                """,
                (db_profile, parts[0], parts[1]),
            ).fetchall()
            rels = conn.execute(
                """
                SELECT cr.*, target.schema_name AS target_schema, target.table_name AS target_table
                FROM catalog_relationships cr
                LEFT JOIN catalog_entities target ON target.id = cr.to_entity_id
                WHERE cr.from_entity_id = ?
                ORDER BY cr.score DESC, cr.last_seen DESC
                """,
                (int(table["id"]),),
            ).fetchall()
        return {
            "table": dict(table),
            "columns": [dict(row) for row in cols],
            "relationships": [dict(row) for row in rels],
        }

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
