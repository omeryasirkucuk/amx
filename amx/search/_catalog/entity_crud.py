"""Entity + description CRUD for ``SearchCatalog``.

These are the low-level row builders that every other mixin calls
when it needs to read or write a catalog entity (database / schema /
table / column) and its associated description rows. The cluster
covers six methods that share private state on ``catalog_entities``
and ``catalog_descriptions``:

* ``_entity_row`` — minimal selector returning the columns the rest
  of the codebase expects.
* ``_upsert_entity`` — get-or-create by (db_profile, schema, table,
  column) tuple.
* ``_insert_description`` — append a candidate description row with
  source attribution.
* ``_index_entity`` — refresh the FTS5 / vector indices for one
  entity.
* ``_resolve_effective_description`` — pick the canonical description
  among manual / reviewed / auto / dedup candidates.
* ``_update_search_text`` — keep the FTS5 ``catalog_search`` table in
  sync with the canonical description.

Reads from ``self._connect()``; otherwise pure SQL. No higher-level
dependencies on other mixins.
"""

from __future__ import annotations

import json
import sqlite3
import time

from amx.search._catalog._constants import SOURCE_PRIORITY, _json_loads
from amx.utils.logging import get_logger

log = get_logger("search.catalog.entity_crud")


class EntityCrudMixin:
    """Entity + description CRUD methods for ``SearchCatalog``."""

    def _entity_row(self, conn: sqlite3.Connection, entity_id: int) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT ce.*, cd.description_text AS effective_description
            FROM catalog_entities ce
            LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
            WHERE ce.id = ?
            """,
            (entity_id,),
        ).fetchone()

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
            parts.append(f"{row['source_kind']}:{row['confidence']}:{row['description_text']}")
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

    def _resolve_effective_description(
        self, conn: sqlite3.Connection, entity_id: int
    ) -> int | None:
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


__all__ = ["EntityCrudMixin"]
