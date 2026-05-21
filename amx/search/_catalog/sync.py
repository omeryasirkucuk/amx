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
from datetime import datetime, timezone
from typing import Any

from amx.agents.base import MetadataSuggestion
from amx.codebase.analyzer import CodebaseReport
from amx.db.connector import TableProfile
from amx.utils.logging import get_logger

log = get_logger("search.catalog.sync")


class SyncMixin:
    """Catalog sync orchestration for ``SearchCatalog``."""

    def start_sync_job(
        self, db_profile: str, job_type: str, scope: dict[str, Any] | None = None
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO catalog_sync_jobs (db_profile, job_type, scope_json, started_at, status)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    db_profile,
                    job_type,
                    json.dumps(scope or {}, ensure_ascii=True),
                    time.time(),
                    "running",
                ),
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
                (
                    time.time(),
                    status,
                    inserted_count,
                    updated_count,
                    error_text[:4000],
                    int(job_id),
                ),
            )

    # ── Profile-level skeleton sync ──────────────────────────────────────
    #
    # The skeleton sync exists to give the cache-first read path a
    # trustworthy "this profile is fully synced" signal without paying
    # the per-table profile_table cost of the full /search sync. Three
    # state-machine helpers gate every skeleton sync; the
    # ``catalog_profile_state`` table is the single source of truth.

    def start_skeleton_sync(self, db_profile: str, total_tables: int) -> None:
        """Flip ``catalog_profile_state`` to ``state='syncing'`` and
        clear the previous progress / error. Idempotent — a profile
        that was already in ``syncing`` from a stale process gets its
        counters reset so the new run starts from zero.
        """
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO catalog_profile_state (
                    db_profile, state, total_tables, processed_tables,
                    started_at, finished_at, last_full_sync_at, last_error
                ) VALUES (?, 'syncing', ?, 0, ?, NULL, NULL, '')
                ON CONFLICT(db_profile) DO UPDATE SET
                    state='syncing',
                    total_tables=excluded.total_tables,
                    processed_tables=0,
                    started_at=excluded.started_at,
                    finished_at=NULL,
                    last_error=''
                """,
                (db_profile, int(total_tables), now),
            )

    def record_skeleton_progress(
        self,
        db_profile: str,
        processed_tables: int,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        """Update the ``processed_tables`` counter mid-sync. Called
        once per schema (not per table) so the daemon thread doesn't
        thrash the SQLite WAL.

        ``conn`` lets the caller share the open connection from the
        skeleton-sync upsert loop. Without sharing, the progress
        update would race the upsert transaction on SQLite WAL and
        surface as ``database is locked``.
        """
        if conn is not None:
            conn.execute(
                "UPDATE catalog_profile_state SET processed_tables = ? WHERE db_profile = ?",
                (int(processed_tables), db_profile),
            )
            return
        with self._connect() as own_conn:
            own_conn.execute(
                "UPDATE catalog_profile_state SET processed_tables = ? WHERE db_profile = ?",
                (int(processed_tables), db_profile),
            )

    def finish_skeleton_sync(self, db_profile: str, *, ok: bool, error: str = "") -> None:
        """Terminal state for a skeleton sync. ``ok=True`` flips
        ``state='done'`` and stamps ``last_full_sync_at`` so the
        cache-first gate trusts the catalog. ``ok=False`` flips to
        ``state='failed'`` and records ``last_error`` for the
        freshness pill's Retry surface — the catalog stays usable as
        a fallback but readers know to treat it as partial."""
        now = time.time()
        if ok:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE catalog_profile_state
                    SET state='done', finished_at=?, last_full_sync_at=?, last_error=''
                    WHERE db_profile = ?
                    """,
                    (now, now, db_profile),
                )
        else:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE catalog_profile_state
                    SET state='failed', finished_at=?, last_error=?
                    WHERE db_profile = ?
                    """,
                    (now, str(error)[:4000], db_profile),
                )

    def get_profile_state(self, db_profile: str) -> dict[str, Any]:
        """Read the current state row. Returns the default ``none``
        shape when the profile has never been synced."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT state, total_tables, processed_tables,
                       started_at, finished_at, last_full_sync_at, last_error
                FROM catalog_profile_state
                WHERE db_profile = ?
                """,
                (db_profile,),
            ).fetchone()
        if not row:
            return {
                "state": "none",
                "total_tables": 0,
                "processed_tables": 0,
                "started_at": None,
                "finished_at": None,
                "last_full_sync_at": None,
                "last_error": "",
            }
        return {
            "state": str(row["state"] or "none"),
            "total_tables": int(row["total_tables"] or 0),
            "processed_tables": int(row["processed_tables"] or 0),
            "started_at": float(row["started_at"]) if row["started_at"] else None,
            "finished_at": float(row["finished_at"]) if row["finished_at"] else None,
            "last_full_sync_at": (
                float(row["last_full_sync_at"]) if row["last_full_sync_at"] else None
            ),
            "last_error": str(row["last_error"] or ""),
        }

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
                fk_flag=1
                if any(
                    column.name in (fk.get("constrained_columns") or [])
                    for fk in profile.foreign_keys
                )
                else 0,
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
        conn.execute(
            "DELETE FROM catalog_relationships WHERE from_entity_id = ?", (table_entity_id,)
        )
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
            self._store_query_usage(
                conn,
                table_entity_id,
                profile,
                query_usage,
                db_profile=db_profile,
                schema_name=profile.schema,
            )
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

    def sync_review_decision(
        self, result_id: int, *, chosen_description: str, evaluation: str
    ) -> None:
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
                    evidence_type = (
                        "table_usage" if match["entity_kind"] == "table" else "column_usage"
                    )
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
                self.finish_sync_job(
                    job_id, status="success", inserted_count=inserted, updated_count=updated
                )
            return inserted, updated
        except Exception as exc:
            self.finish_sync_job(
                job_id,
                status="failed",
                inserted_count=inserted,
                updated_count=updated,
                error_text=str(exc),
            )
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

    # ── Task 26: sync_remote_assets ──────────────────────────────────────────

    def sync_remote_assets(
        self,
        *,
        profile_name: str,
        notebooks: list | None = None,
        jobs: list | None = None,
        pipelines: list | None = None,
        streamlit_apps: list | None = None,
        streams: list | None = None,
        queries: list | None = None,
        task_dependencies: list[tuple[str, str]] | None = None,
    ) -> dict[str, int]:
        """Upsert remote-ingested assets into the ``remote_*`` tables.

        Returns a per-asset-type count of rows newly inserted or content-updated.
        Rows whose source hashes already match the stored row are touched only
        on ``ingested_at`` and not counted (the catalog stays accurate without
        re-embedding work upstream).
        """
        counts: dict[str, int] = {}
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            if notebooks:
                counts["notebooks"] = self._upsert_remote_notebooks(
                    conn, profile_name, notebooks, now_iso
                )
            if jobs:
                counts["jobs"] = self._upsert_remote_jobs(
                    conn, profile_name, jobs, now_iso
                )
            if pipelines:
                counts["pipelines"] = self._upsert_remote_pipelines(
                    conn, profile_name, pipelines, now_iso
                )
            if streamlit_apps:
                counts["streamlit_apps"] = self._upsert_remote_streamlit_apps(
                    conn, profile_name, streamlit_apps, now_iso
                )
            if streams:
                counts["streams"] = self._upsert_remote_streams(
                    conn, profile_name, streams, now_iso
                )
            if queries:
                counts["queries"] = self._upsert_remote_queries(
                    conn, profile_name, queries, now_iso
                )
            if task_dependencies:
                counts["task_dependencies"] = self._upsert_remote_task_dependencies(
                    conn, profile_name, task_dependencies
                )
            # After every notebook+job pass, opportunistically resolve task→notebook FKs.
            if (notebooks is not None) or (jobs is not None):
                self._resolve_job_task_notebook_fks(conn, profile_name)
            conn.commit()
        return counts

    def _upsert_remote_notebooks(self, conn, profile, items, now_iso):
        n = 0
        for nb in items:
            existing = conn.execute(
                "SELECT id, source_hash FROM remote_notebooks "
                "WHERE profile_name = ? AND platform = ? AND external_id = ?",
                (profile, nb.platform, nb.external_id),
            ).fetchone()
            if existing and existing[1] == nb.source_hash:
                conn.execute(
                    "UPDATE remote_notebooks SET ingested_at = ? WHERE id = ?",
                    (now_iso, existing[0]),
                )
                continue
            if existing:
                conn.execute(
                    """UPDATE remote_notebooks SET
                           name = ?, workspace_path = ?, qualified_name = ?,
                           language = ?, source_text = ?, source_hash = ?,
                           last_modified_at = ?, last_modified_by = ?, owner = ?,
                           cell_count = ?, ingested_at = ?
                       WHERE id = ?""",
                    (
                        nb.name, nb.workspace_path, nb.qualified_name,
                        nb.language, nb.source_text, nb.source_hash,
                        nb.last_modified_at.isoformat() if nb.last_modified_at else None,
                        nb.last_modified_by, nb.owner, nb.cell_count, now_iso,
                        existing[0],
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO remote_notebooks
                           (profile_name, platform, external_id, name,
                            workspace_path, qualified_name, language,
                            source_text, source_hash, last_modified_at,
                            last_modified_by, owner, cell_count, ingested_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        profile, nb.platform, nb.external_id, nb.name,
                        nb.workspace_path, nb.qualified_name, nb.language,
                        nb.source_text, nb.source_hash,
                        nb.last_modified_at.isoformat() if nb.last_modified_at else None,
                        nb.last_modified_by, nb.owner, nb.cell_count, now_iso,
                    ),
                )
            n += 1
        return n

    def _upsert_remote_jobs(self, conn, profile, items, now_iso):
        n = 0
        for j in items:
            last_run = j.recent_runs[0] if j.recent_runs else None
            cur = conn.execute(
                "SELECT id FROM remote_jobs WHERE profile_name = ? AND job_id = ?",
                (profile, j.job_id),
            ).fetchone()
            if cur:
                row_id = cur[0]
                conn.execute(
                    """UPDATE remote_jobs SET
                           name = ?, creator_user_name = ?, schedule_cron = ?,
                           schedule_timezone = ?, schedule_pause_status = ?,
                           max_concurrent_runs = ?, email_notifications_json = ?,
                           tags_json = ?, last_run_status = ?, last_run_started_at = ?,
                           success_rate_30d = ?, ingested_at = ?
                       WHERE id = ?""",
                    (
                        j.name, j.creator_user_name, j.schedule_cron,
                        j.schedule_timezone, j.schedule_pause_status,
                        j.max_concurrent_runs,
                        json.dumps(j.email_notifications),
                        json.dumps(j.tags),
                        last_run.state_result if last_run else None,
                        last_run.start_time.isoformat() if last_run else None,
                        j.success_rate(window_days=30), now_iso,
                        row_id,
                    ),
                )
            else:
                cur2 = conn.execute(
                    """INSERT INTO remote_jobs
                           (profile_name, job_id, name, creator_user_name,
                            schedule_cron, schedule_timezone, schedule_pause_status,
                            max_concurrent_runs, email_notifications_json, tags_json,
                            last_run_status, last_run_started_at, success_rate_30d,
                            ingested_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        profile, j.job_id, j.name, j.creator_user_name,
                        j.schedule_cron, j.schedule_timezone, j.schedule_pause_status,
                        j.max_concurrent_runs,
                        json.dumps(j.email_notifications),
                        json.dumps(j.tags),
                        last_run.state_result if last_run else None,
                        last_run.start_time.isoformat() if last_run else None,
                        j.success_rate(window_days=30), now_iso,
                    ),
                )
                row_id = cur2.lastrowid
            # Replace child rows for this job — simplest correct semantics.
            conn.execute("DELETE FROM remote_job_tasks WHERE job_id_fk = ?", (row_id,))
            for t in j.tasks:
                conn.execute(
                    """INSERT INTO remote_job_tasks
                           (job_id_fk, task_key, task_type, notebook_path,
                            sql_query_id, sql_warehouse_id, pipeline_id_fk,
                            depends_on_json, raw_definition_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row_id, t.task_key, t.task_type, t.notebook_path,
                        t.sql_query_id, t.sql_warehouse_id, None,  # pipeline_id_fk resolved later
                        json.dumps(list(t.depends_on)),
                        json.dumps(t.raw_definition),
                    ),
                )
            conn.execute("DELETE FROM remote_job_runs WHERE job_id_fk = ?", (row_id,))
            for r in j.recent_runs:
                conn.execute(
                    """INSERT INTO remote_job_runs
                           (job_id_fk, run_id, state_result, start_time, end_time,
                            setup_duration_ms, execution_duration_ms)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row_id, r.run_id, r.state_result,
                        r.start_time.isoformat(),
                        r.end_time.isoformat() if r.end_time else None,
                        r.setup_duration_ms, r.execution_duration_ms,
                    ),
                )
            n += 1
        return n

    def _upsert_remote_pipelines(self, conn, profile, items, now_iso):
        n = 0
        for p in items:
            cur = conn.execute(
                "SELECT id FROM remote_pipelines WHERE profile_name = ? AND pipeline_id = ?",
                (profile, p.pipeline_id),
            ).fetchone()
            if cur:
                conn.execute(
                    """UPDATE remote_pipelines SET
                           name = ?, target_schema = ?, edition = ?, continuous = ?,
                           photon = ?, libraries_json = ?, latest_update_state = ?,
                           latest_update_creation_time = ?, ingested_at = ?
                       WHERE id = ?""",
                    (
                        p.name, p.target_schema, p.edition, int(p.continuous),
                        int(p.photon), json.dumps(p.libraries), p.latest_update_state,
                        p.latest_update_creation_time.isoformat()
                        if p.latest_update_creation_time else None,
                        now_iso, cur[0],
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO remote_pipelines
                           (profile_name, pipeline_id, name, target_schema, edition,
                            continuous, photon, libraries_json, latest_update_state,
                            latest_update_creation_time, ingested_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        profile, p.pipeline_id, p.name, p.target_schema, p.edition,
                        int(p.continuous), int(p.photon),
                        json.dumps(p.libraries), p.latest_update_state,
                        p.latest_update_creation_time.isoformat()
                        if p.latest_update_creation_time else None,
                        now_iso,
                    ),
                )
            n += 1
        return n

    def _upsert_remote_streamlit_apps(self, conn, profile, items, now_iso):
        n = 0
        for s in items:
            cur = conn.execute(
                "SELECT id FROM remote_streamlit_apps "
                "WHERE profile_name = ? AND qualified_name = ?",
                (profile, s.qualified_name),
            ).fetchone()
            if cur:
                conn.execute(
                    """UPDATE remote_streamlit_apps SET
                           main_file = ?, query_warehouse = ?, root_location = ?,
                           owner = ?, last_altered_at = ?, ingested_at = ?
                       WHERE id = ?""",
                    (
                        s.main_file, s.query_warehouse, s.root_location, s.owner,
                        s.last_altered_at.isoformat() if s.last_altered_at else None,
                        now_iso, cur[0],
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO remote_streamlit_apps
                           (profile_name, qualified_name, main_file, query_warehouse,
                            root_location, owner, last_altered_at, ingested_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        profile, s.qualified_name, s.main_file, s.query_warehouse,
                        s.root_location, s.owner,
                        s.last_altered_at.isoformat() if s.last_altered_at else None,
                        now_iso,
                    ),
                )
            n += 1
        return n

    def _upsert_remote_streams(self, conn, profile, items, now_iso):
        n = 0
        for s in items:
            cur = conn.execute(
                "SELECT id FROM remote_streams "
                "WHERE profile_name = ? AND qualified_name = ?",
                (profile, s.qualified_name),
            ).fetchone()
            if cur:
                conn.execute(
                    """UPDATE remote_streams SET
                           source_table_fqn = ?, mode = ?, stale_after = ?,
                           owner = ?, ingested_at = ?
                       WHERE id = ?""",
                    (
                        s.source_table_fqn, s.mode,
                        s.stale_after.isoformat() if s.stale_after else None,
                        s.owner, now_iso, cur[0],
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO remote_streams
                           (profile_name, qualified_name, source_table_fqn,
                            source_entity_id, mode, stale_after, owner, ingested_at)
                       VALUES (?, ?, ?, NULL, ?, ?, ?, ?)""",
                    (
                        profile, s.qualified_name, s.source_table_fqn,
                        s.mode,
                        s.stale_after.isoformat() if s.stale_after else None,
                        s.owner, now_iso,
                    ),
                )
            n += 1
        return n

    def _upsert_remote_queries(self, conn, profile, items, now_iso):
        n = 0
        for q in items:
            cur = conn.execute(
                "SELECT id FROM remote_queries "
                "WHERE profile_name = ? AND platform = ? AND kind = ? AND external_id = ?",
                (profile, q.platform, q.kind, q.external_id),
            ).fetchone()
            if cur:
                conn.execute(
                    """UPDATE remote_queries SET
                           name = ?, sql_text = ?, sql_hash = ?, warehouse = ?,
                           user_name = ?, executed_at = ?, duration_ms = ?,
                           ingested_at = ?
                       WHERE id = ?""",
                    (
                        q.name, q.sql_text, q.sql_hash, q.warehouse, q.user_name,
                        q.executed_at.isoformat() if q.executed_at else None,
                        q.duration_ms, now_iso, cur[0],
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO remote_queries
                           (profile_name, platform, kind, external_id, name,
                            sql_text, sql_hash, warehouse, user_name,
                            executed_at, duration_ms, ingested_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        profile, q.platform, q.kind, q.external_id, q.name,
                        q.sql_text, q.sql_hash, q.warehouse, q.user_name,
                        q.executed_at.isoformat() if q.executed_at else None,
                        q.duration_ms, now_iso,
                    ),
                )
            n += 1
        return n

    def _upsert_remote_task_dependencies(self, conn, profile, edges):
        conn.execute(
            "DELETE FROM remote_task_dependencies WHERE profile_name = ?", (profile,)
        )
        n = 0
        for parent, child in edges:
            conn.execute(
                """INSERT OR IGNORE INTO remote_task_dependencies
                       (profile_name, parent_task_fqn, child_task_fqn)
                   VALUES (?, ?, ?)""",
                (profile, parent, child),
            )
            n += 1
        return n

    def _resolve_job_task_notebook_fks(self, conn, profile):
        """Fill remote_job_tasks.notebook_id_fk by matching notebook_path to remote_notebooks.workspace_path."""
        conn.execute(
            """
            UPDATE remote_job_tasks
            SET notebook_id_fk = (
                SELECT id FROM remote_notebooks
                WHERE remote_notebooks.profile_name = ?
                  AND remote_notebooks.workspace_path = remote_job_tasks.notebook_path
                LIMIT 1
            )
            WHERE notebook_path IS NOT NULL
              AND notebook_id_fk IS NULL
              AND job_id_fk IN (SELECT id FROM remote_jobs WHERE profile_name = ?)
            """,
            (profile, profile),
        )


__all__ = ["SyncMixin"]
