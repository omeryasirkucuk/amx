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
        # Lookup must include ``database_name`` — without it, a re-sync
        # of profile X against a different database would UPDATE the
        # wrong entity row (the post-v0.16 unique index keys on
        # database_name; the legacy lookup didn't). See the migration
        # block in sqlite_store.py.
        row = conn.execute(
            """
            SELECT id FROM catalog_entities
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND table_name = ?
              AND COALESCE(column_name, '') = COALESCE(?, '')
              AND entity_kind = ?
            """,
            (
                db_profile,
                database_name,
                schema_name,
                table_name,
                column_name,
                entity_kind,
            ),
        ).fetchone()
        if row:
            # Bump ``last_synced_at`` on every re-upsert. The
            # ``catalog_freshness`` pill (and the user) treats this
            # timestamp as the canonical "last time AMX touched this
            # row"; without the bump a Sync-all over an unchanged
            # catalog leaves the pill stuck on the prior timestamp
            # even though the sync actually ran.
            conn.execute(
                """
                UPDATE catalog_entities
                SET db_backend = ?, database_name = ?, asset_kind = ?, dtype = ?, nullable = ?,
                    pk_flag = ?, fk_flag = ?, row_count = ?, updated_at = ?, last_synced_at = ?
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
        search_text_value = "\n".join(parts)
        conn.execute(
            """
            UPDATE catalog_entities
            SET search_text = ?, last_synced_at = ?
            WHERE id = ?
            """,
            (search_text_value, time.time(), entity_id),
        )
        # Mirror into the FTS5 index. ``catalog_entities_fts`` is a
        # contentless virtual table keyed by ``rowid = catalog_entities.id``,
        # so DELETE + INSERT keeps the FTS row matching the live entity
        # row exactly. The concept-search path (``_exact_candidates``)
        # uses MATCH instead of the legacy O(n) scan; without this
        # mirror the FTS would stay empty and concept search would
        # return zero rows on any catalog written under v0.15+.
        try:
            conn.execute(
                "DELETE FROM catalog_entities_fts WHERE rowid = ?",
                (entity_id,),
            )
            conn.execute(
                """
                INSERT INTO catalog_entities_fts (
                    rowid, db_profile, column_name, table_name, schema_name, search_text
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    entity_id,
                    str(entity["db_profile"] or ""),
                    str(entity["column_name"] or ""),
                    str(entity["table_name"] or ""),
                    str(entity["schema_name"] or ""),
                    search_text_value,
                ),
            )
        except sqlite3.OperationalError:
            # FTS5 may be unavailable on a very old SQLite shipping
            # with the host python. Leave the legacy scan path as the
            # fallback rather than failing every catalog write.
            pass

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

    def fetch_table_metadata(
        self,
        db_profile: str,
        schema_name: str,
        table_name: str,
    ) -> dict | None:
        """Read a fully-resolved table+column snapshot from the catalog.

        Returns ``None`` when no ``entity_kind='table'`` row exists for
        the (profile, schema, table) tuple — caller falls back to a
        live query. Otherwise returns::

            {
                "table_comment": str,
                "row_count": int,
                "last_synced_at": float,
                "columns": [
                    {"name", "dtype", "nullable", "comment"}, ...
                ],
            }

        ``comment`` on each column is the catalog's currently effective
        description (joined through ``effective_description_id``). The
        cache-first agent tools consume this to skip live ``profile_table``
        calls whenever ``/search sync`` has already covered the table.
        """
        with self._connect() as conn:
            table_row = conn.execute(
                """
                SELECT ce.row_count, ce.last_synced_at,
                       cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE ce.db_profile = ? AND ce.schema_name = ? AND ce.table_name = ?
                  AND ce.entity_kind = 'table'
                LIMIT 1
                """,
                (db_profile, schema_name, table_name),
            ).fetchone()
            if table_row is None:
                return None
            col_rows = conn.execute(
                """
                SELECT ce.column_name, ce.dtype, ce.nullable,
                       cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE ce.db_profile = ? AND ce.schema_name = ? AND ce.table_name = ?
                  AND ce.entity_kind = 'column'
                ORDER BY ce.id
                """,
                (db_profile, schema_name, table_name),
            ).fetchall()
        columns: list[dict] = []
        for r in col_rows:
            name = str(r["column_name"] or "").strip()
            if not name:
                continue
            columns.append(
                {
                    "name": name,
                    "dtype": str(r["dtype"] or ""),
                    "nullable": bool(int(r["nullable"] or 0)),
                    "comment": str(r["effective_description"] or ""),
                }
            )
        return {
            "table_comment": str(table_row["effective_description"] or ""),
            "row_count": int(table_row["row_count"] or 0),
            "last_synced_at": float(table_row["last_synced_at"] or 0.0),
            "columns": columns,
        }

    def is_profile_fully_synced(self, db_profile: str) -> bool:
        """``True`` iff every cache surface for *db_profile* has been
        warmed by a successful ``Sync all``. Tightened from "skeleton
        done" to "skeleton + schemas + columns all stamped" so the
        cache-only read mode in
        :meth:`amx.db.connector.DatabaseConnector._is_cache_warm`
        only kicks in when the next read can actually be served from
        cache. Previously a profile with only the skeleton populated
        (catalog_entities) would surface as fully synced and the
        cache-only gate would route ``get_column_comments`` to an
        empty cache row — starving the caller — because
        ``column_comments_cache`` had never been warmed.

        Reads ``catalog_profile_state``. The cache NEVER auto-expires
        — pre-PR the helper rejected snapshots older than 7 days,
        which forced sidebar / Ask / drift surfaces to fall through
        to the live DB on any week-old profile. The user's contract
        now: keep cached data forever, surface a UI staleness pill
        instead of invalidating.

        Returns ``False`` when:
        - the table doesn't exist (legacy catalog from a pre-v0.15
          install — keeps existing users on the live DB until they
          run a fresh skeleton sync)
        - the state row exists but `state != 'done'` (sync still
          running, failed, or never started)
        - any of ``last_skeleton_sync_at`` / ``last_schemas_sync_at``
          / ``last_columns_sync_at`` is NULL (a partial sync — the
          caller must keep the live-DB fallback armed)

        Use :meth:`get_profile_state` from ``SyncMixin`` to read the
        individual timestamps when a UI surface wants to render a
        "synced N days ago" pill or a per-surface staleness chip.
        """
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT state, last_full_sync_at,
                           last_skeleton_sync_at,
                           last_schemas_sync_at,
                           last_columns_sync_at
                    FROM catalog_profile_state
                    WHERE db_profile = ?
                    """,
                    (db_profile,),
                ).fetchone()
        except sqlite3.OperationalError:
            return False
        if not row:
            return False
        state = str(row["state"] or "")
        if state != "done":
            return False
        # All four timestamps are required. ``last_full_sync_at``
        # remains a hard gate for back-compat (any pre-existing logic
        # that reads it directly stays consistent); the three
        # per-surface stamps are the new contract.
        return all(
            row[col] is not None
            for col in (
                "last_full_sync_at",
                "last_skeleton_sync_at",
                "last_schemas_sync_at",
                "last_columns_sync_at",
            )
        )

    def fetch_distinct_databases(self, db_profile: str) -> list[dict]:
        """Return distinct ``database_name`` rows for *db_profile* with
        each database's freshest ``last_synced_at``. Catalog-style
        backends (Databricks, BigQuery) populate ``database_name``
        with their catalog/project value at sync time so the same
        query serves every backend uniformly. Empty / legacy rows
        without a recorded database are skipped — the caller relies on
        a non-empty name to scope subsequent schema and table reads.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT database_name, MAX(last_synced_at) AS last_synced_at
                FROM catalog_entities
                WHERE db_profile = ?
                  AND entity_kind = 'table'
                  AND database_name IS NOT NULL AND database_name != ''
                GROUP BY database_name
                ORDER BY database_name
                """,
                (db_profile,),
            ).fetchall()
        return [
            {
                "name": str(r["database_name"] or ""),
                "last_synced_at": float(r["last_synced_at"] or 0.0),
            }
            for r in rows
            if r["database_name"]
        ]

    def fetch_distinct_schemas(
        self, db_profile: str, database_name: str | None = None
    ) -> list[dict]:
        """Return distinct ``schema_name`` rows for *db_profile* with
        each schema's freshest ``last_synced_at``. When *database_name*
        is provided, scope the result to that database — required for
        2-level backends (Postgres, MySQL…) where a single profile
        can reach multiple databases and each has its own schemas.
        Without the filter, the cache would leak schemas across
        databases under the same profile.

        Matches both ``entity_kind='table'`` rows (the historical
        marker — present whenever any table under the schema has been
        synced) and ``entity_kind='schema'`` rows (the lighter marker
        the sidebar's live-fallback path writes when the user expands
        a catalog and the connector returned schemas but no tables
        yet). Without the schema marker, a sidebar that fetched
        schemas live on a cache miss would never serve them from
        cache on the next expand.
        """
        with self._connect() as conn:
            if database_name is None:
                rows = conn.execute(
                    """
                    SELECT schema_name, MAX(last_synced_at) AS last_synced_at
                    FROM catalog_entities
                    WHERE db_profile = ? AND entity_kind IN ('table', 'schema')
                      AND schema_name != ''
                    GROUP BY schema_name
                    ORDER BY schema_name
                    """,
                    (db_profile,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT schema_name, MAX(last_synced_at) AS last_synced_at
                    FROM catalog_entities
                    WHERE db_profile = ? AND database_name = ?
                      AND entity_kind IN ('table', 'schema')
                      AND schema_name != ''
                    GROUP BY schema_name
                    ORDER BY schema_name
                    """,
                    (db_profile, database_name),
                ).fetchall()
        return [
            {
                "name": str(r["schema_name"] or ""),
                "last_synced_at": float(r["last_synced_at"] or 0.0),
            }
            for r in rows
            if r["schema_name"]
        ]

    def fetch_distinct_tables_in_schema(
        self,
        db_profile: str,
        schema_name: str,
        database_name: str | None = None,
    ) -> list[dict]:
        """Return distinct ``table_name`` rows under (profile, schema)
        with each table's freshest ``last_synced_at``. ``database_name``
        scopes to a single database under the profile when provided —
        the same database-level guard ``fetch_distinct_schemas``
        applies.
        """
        with self._connect() as conn:
            if database_name is None:
                rows = conn.execute(
                    """
                    SELECT table_name, MAX(last_synced_at) AS last_synced_at
                    FROM catalog_entities
                    WHERE db_profile = ? AND schema_name = ?
                      AND entity_kind = 'table' AND table_name != ''
                    GROUP BY table_name
                    ORDER BY table_name
                    """,
                    (db_profile, schema_name),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT table_name, MAX(last_synced_at) AS last_synced_at
                    FROM catalog_entities
                    WHERE db_profile = ? AND database_name = ?
                      AND schema_name = ?
                      AND entity_kind = 'table' AND table_name != ''
                    GROUP BY table_name
                    ORDER BY table_name
                    """,
                    (db_profile, database_name, schema_name),
                ).fetchall()
        return [
            {
                "name": str(r["table_name"] or ""),
                "last_synced_at": float(r["last_synced_at"] or 0.0),
            }
            for r in rows
            if r["table_name"]
        ]

    def fetch_coverage_summary(
        self,
        db_profile: str | list[str],
        *,
        schema_name: str | None = None,
    ) -> list[dict]:
        """Per-profile + per-schema description-coverage counts.

        Powers the ``catalog_coverage_summary`` Ask tool: the agent
        used to chain ``describe_table`` for every entity to answer
        coverage questions, which is why "how many tables don't have
        comments" used to take 70+ seconds. A single GROUP BY against
        ``catalog_entities`` answers it in sub-50 ms because the
        canonical effective-description pointer
        (``effective_description_id``) is exactly the field we need
        to count for "missing" — NULL means no comment.

        Returns one row per (db_profile, database_name, schema_name)
        with ``total_tables``, ``undocumented_tables``,
        ``total_columns``, ``undocumented_columns`` and the freshest
        ``last_synced_at`` from the underlying entity rows. Empty
        schemas are omitted so the LLM doesn't waste a sentence
        narrating zero-sized groups.

        ``db_profile`` accepts the scalar / list shape the rest of
        the catalog helpers use (matches ``DBProfileFilter`` from
        the search module). ``schema_name`` filters to one schema
        for "is THIS schema covered?" questions.
        """
        from amx.search._catalog._db_profile_clause import build_db_profile_clause

        clause, binds = build_db_profile_clause(db_profile, column="db_profile")
        sql_parts = [
            "SELECT db_profile, database_name, schema_name, "
            "       SUM(CASE WHEN entity_kind = 'table' THEN 1 ELSE 0 END) AS total_tables, "
            "       SUM(CASE WHEN entity_kind = 'table' "
            "                AND effective_description_id IS NULL THEN 1 ELSE 0 END) "
            "         AS undocumented_tables, "
            "       SUM(CASE WHEN entity_kind = 'column' THEN 1 ELSE 0 END) AS total_columns, "
            "       SUM(CASE WHEN entity_kind = 'column' "
            "                AND effective_description_id IS NULL THEN 1 ELSE 0 END) "
            "         AS undocumented_columns, "
            "       MAX(last_synced_at) AS last_synced_at "
            "FROM catalog_entities "
            f"WHERE {clause} "
            "  AND schema_name != ''",
        ]
        params: list[object] = list(binds)
        if schema_name:
            sql_parts.append("  AND schema_name = ?")
            params.append(schema_name)
        sql_parts.append(
            " GROUP BY db_profile, database_name, schema_name "
            " HAVING total_tables > 0 OR total_columns > 0 "
            " ORDER BY db_profile, database_name, schema_name"
        )
        sql = "\n".join(sql_parts)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        out: list[dict] = []
        for row in rows:
            total_tables = int(row["total_tables"] or 0)
            undocumented_tables = int(row["undocumented_tables"] or 0)
            total_columns = int(row["total_columns"] or 0)
            undocumented_columns = int(row["undocumented_columns"] or 0)
            documented_tables = max(0, total_tables - undocumented_tables)
            documented_columns = max(0, total_columns - undocumented_columns)
            out.append(
                {
                    "db_profile": str(row["db_profile"] or ""),
                    "database": str(row["database_name"] or "") or None,
                    "schema": str(row["schema_name"] or ""),
                    "total_tables": total_tables,
                    "undocumented_tables": undocumented_tables,
                    "documented_tables": documented_tables,
                    "total_columns": total_columns,
                    "undocumented_columns": undocumented_columns,
                    "documented_columns": documented_columns,
                    "table_coverage_pct": (
                        round(100.0 * documented_tables / total_tables, 1)
                        if total_tables > 0
                        else None
                    ),
                    "column_coverage_pct": (
                        round(100.0 * documented_columns / total_columns, 1)
                        if total_columns > 0
                        else None
                    ),
                    "last_synced_at": float(row["last_synced_at"] or 0.0) or None,
                }
            )
        return out

    def fetch_columns_for_table(
        self,
        db_profile: str,
        *,
        schema_name: str,
        table_name: str,
        database_name: str | None = None,
    ) -> list[dict]:
        """Return every column the catalog has recorded for one table.

        Powers the Studio sidebar's column list — used to be a live DB
        round-trip on every "expand this table" click, even though
        ``/search sync`` already wrote every column row into the
        catalog. Single SQLite read.
        """
        with self._connect() as conn:
            if database_name:
                rows = conn.execute(
                    """
                    SELECT column_name, dtype, nullable, pk_flag, fk_flag,
                           last_synced_at
                    FROM catalog_entities
                    WHERE db_profile = ? AND database_name = ?
                      AND lower(schema_name) = lower(?)
                      AND lower(table_name) = lower(?)
                      AND entity_kind = 'column'
                      AND column_name IS NOT NULL AND column_name != ''
                    ORDER BY column_name
                    """,
                    (db_profile, database_name, schema_name, table_name),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT column_name, dtype, nullable, pk_flag, fk_flag,
                           last_synced_at
                    FROM catalog_entities
                    WHERE db_profile = ?
                      AND lower(schema_name) = lower(?)
                      AND lower(table_name) = lower(?)
                      AND entity_kind = 'column'
                      AND column_name IS NOT NULL AND column_name != ''
                    ORDER BY column_name
                    """,
                    (db_profile, schema_name, table_name),
                ).fetchall()
        return [
            {
                "name": str(r["column_name"] or ""),
                "dtype": str(r["dtype"] or ""),
                "nullable": bool(r["nullable"]),
                "pk_flag": bool(r["pk_flag"]),
                "fk_flag": bool(r["fk_flag"]),
                "last_synced_at": float(r["last_synced_at"] or 0.0) or None,
            }
            for r in rows
        ]

    def fetch_column_detail(
        self,
        db_profile: str | list[str],
        *,
        schema_name: str,
        table_name: str,
        column_name: str,
    ) -> list[dict]:
        """Return matching ``catalog_entities`` column row(s) with
        their joined description text, for the ``describe_column``
        Ask tool. Multi-profile fan-out returns one row per profile
        that has the (schema, table, column) tuple — the agent can
        disambiguate when the same column exists across profiles.
        """
        from amx.search._catalog._db_profile_clause import build_db_profile_clause

        clause, binds = build_db_profile_clause(db_profile, column="ce.db_profile")
        sql = (
            "SELECT ce.db_profile, ce.database_name, ce.schema_name, "
            "       ce.table_name, ce.column_name, ce.dtype, ce.nullable, "
            "       ce.pk_flag, ce.fk_flag, ce.last_synced_at, "
            "       cd.description_text AS description "
            "FROM catalog_entities ce "
            "LEFT JOIN catalog_descriptions cd "
            "       ON cd.id = ce.effective_description_id "
            f"WHERE {clause} "
            "  AND ce.entity_kind = 'column' "
            "  AND lower(ce.schema_name) = lower(?) "
            "  AND lower(ce.table_name) = lower(?) "
            "  AND lower(ce.column_name) = lower(?) "
            "ORDER BY ce.db_profile, ce.database_name"
        )
        params: list[object] = list(binds) + [schema_name, table_name, column_name]
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            {
                "db_profile": str(r["db_profile"] or ""),
                "database": str(r["database_name"] or "") or None,
                "schema": str(r["schema_name"] or ""),
                "table": str(r["table_name"] or ""),
                "column": str(r["column_name"] or ""),
                "dtype": str(r["dtype"] or ""),
                "nullable": bool(r["nullable"]),
                "pk_flag": bool(r["pk_flag"]),
                "fk_flag": bool(r["fk_flag"]),
                "description": str(r["description"] or ""),
                "last_synced_at": float(r["last_synced_at"] or 0.0) or None,
            }
            for r in rows
        ]

    def fetch_inventory(
        self,
        db_profile: str | list[str],
        *,
        scope: str = "schemas",
    ) -> list[dict]:
        """Distinct catalog / database / schema rows from the cache.

        Replaces three live-only inventory tools (``list_catalogs``,
        ``list_server_databases``, ``list_databases``) with a single
        cache-served query the agent can call in cache-only mode.
        Each row carries a freshness signal (``last_synced_at``)
        and the parent profile so multi-profile fan-out questions
        ("which databases do we have across both profiles?")
        answer in one shot.

        ``scope`` is one of:

        * ``"databases"`` — distinct (profile, database_name).
        * ``"schemas"`` — distinct (profile, database_name, schema_name).

        Empty database / schema names are filtered out; the catalog
        carries them only for higher-level entity-kind rows.
        """
        from amx.search._catalog._db_profile_clause import build_db_profile_clause

        scope_norm = (scope or "schemas").strip().lower()
        if scope_norm not in {"databases", "schemas"}:
            scope_norm = "schemas"
        clause, binds = build_db_profile_clause(db_profile, column="db_profile")
        if scope_norm == "databases":
            sql = (
                "SELECT db_profile, database_name, "
                "       MAX(last_synced_at) AS last_synced_at, "
                "       COUNT(DISTINCT schema_name) AS schema_count, "
                "       SUM(CASE WHEN entity_kind = 'table' THEN 1 ELSE 0 END) "
                "         AS table_count "
                "FROM catalog_entities "
                f"WHERE {clause} "
                "  AND database_name != '' "
                "GROUP BY db_profile, database_name "
                "ORDER BY db_profile, database_name"
            )
            with self._connect() as conn:
                rows = conn.execute(sql, binds).fetchall()
            return [
                {
                    "db_profile": str(r["db_profile"] or ""),
                    "database": str(r["database_name"] or ""),
                    "schema_count": int(r["schema_count"] or 0),
                    "table_count": int(r["table_count"] or 0),
                    "last_synced_at": float(r["last_synced_at"] or 0.0) or None,
                }
                for r in rows
                if r["database_name"]
            ]
        # scope == "schemas"
        sql = (
            "SELECT db_profile, database_name, schema_name, "
            "       MAX(last_synced_at) AS last_synced_at, "
            "       SUM(CASE WHEN entity_kind = 'table' THEN 1 ELSE 0 END) "
            "         AS table_count "
            "FROM catalog_entities "
            f"WHERE {clause} "
            "  AND schema_name != '' "
            "GROUP BY db_profile, database_name, schema_name "
            "ORDER BY db_profile, database_name, schema_name"
        )
        with self._connect() as conn:
            rows = conn.execute(sql, binds).fetchall()
        return [
            {
                "db_profile": str(r["db_profile"] or ""),
                "database": str(r["database_name"] or "") or None,
                "schema": str(r["schema_name"] or ""),
                "table_count": int(r["table_count"] or 0),
                "last_synced_at": float(r["last_synced_at"] or 0.0) or None,
            }
            for r in rows
            if r["schema_name"]
        ]

    def search_entities(
        self,
        query: str,
        *,
        db_profile: str | None = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, object]], bool]:
        """Substring search across ``catalog_entities`` for schema /
        table / column names. Powers the Studio sidebar's column-level
        search box.

        Returns ``(results, truncated)``. Each result is a flat dict
        with the breadcrumb fields::

            {
                "profile", "db_backend", "database",
                "schema", "table", "column",
                "match_field": "schema" | "table" | "column",
            }

        ``table`` / ``column`` are ``None`` for higher-level matches.
        Results are ranked ``schema → table → column`` so structural
        hits surface above the long tail of column matches, then
        alphabetical for stable ordering. ``truncated`` flips True
        when the catalog row count exceeds ``limit`` — the SPA shows
        a "refine your search" hint in that case.

        Only fully-synced profiles contribute rows: a half-finished
        skeleton sync could surface schemas / columns that no longer
        exist on the live DB and burn the user's trust. When
        ``db_profile`` is given but the profile isn't fully synced,
        returns ``([], False)``; when ``db_profile`` is None, the
        synced set is computed via ``catalog_profile_state`` and used
        to gate every UNION branch.

        Queries shorter than two characters return immediately — the
        SPA enforces this too but the server is the authoritative
        gate.
        """
        q = (query or "").strip()
        if len(q) < 2:
            return [], False
        if limit <= 0:
            return [], False
        needle = f"%{q.lower()}%"

        with self._connect() as conn:
            if db_profile is not None:
                if not self.is_profile_fully_synced(db_profile):
                    return [], False
                synced_profiles: list[str] = [db_profile]
            else:
                try:
                    rows = conn.execute(
                        """
                        SELECT db_profile, last_full_sync_at
                        FROM catalog_profile_state
                        WHERE state = 'done' AND last_full_sync_at IS NOT NULL
                        """,
                    ).fetchall()
                except sqlite3.OperationalError:
                    return [], False
                cutoff = time.time() - 7 * 24 * 60 * 60
                synced_profiles = [
                    str(r["db_profile"])
                    for r in rows
                    if float(r["last_full_sync_at"] or 0.0) >= cutoff
                ]
            if not synced_profiles:
                return [], False

            placeholder = ",".join("?" * len(synced_profiles))
            cap = limit + 1
            schema_rows = conn.execute(
                f"""
                SELECT db_profile, db_backend, database_name, schema_name
                FROM catalog_entities
                WHERE LOWER(schema_name) LIKE ?
                  AND entity_kind = 'table'
                  AND schema_name != ''
                  AND db_profile IN ({placeholder})
                GROUP BY db_profile, db_backend, database_name, schema_name
                ORDER BY schema_name, db_profile, database_name
                LIMIT ?
                """,
                (needle, *synced_profiles, cap),
            ).fetchall()
            table_rows = conn.execute(
                f"""
                SELECT db_profile, db_backend, database_name, schema_name, table_name
                FROM catalog_entities
                WHERE LOWER(table_name) LIKE ?
                  AND entity_kind = 'table'
                  AND table_name != ''
                  AND db_profile IN ({placeholder})
                GROUP BY db_profile, db_backend, database_name, schema_name, table_name
                ORDER BY table_name, schema_name, db_profile, database_name
                LIMIT ?
                """,
                (needle, *synced_profiles, cap),
            ).fetchall()
            column_rows = conn.execute(
                f"""
                SELECT db_profile, db_backend, database_name, schema_name,
                       table_name, column_name
                FROM catalog_entities
                WHERE LOWER(column_name) LIKE ?
                  AND entity_kind = 'column'
                  AND column_name IS NOT NULL AND column_name != ''
                  AND db_profile IN ({placeholder})
                ORDER BY column_name, table_name, schema_name, db_profile, database_name
                LIMIT ?
                """,
                (needle, *synced_profiles, cap),
            ).fetchall()

        results: list[dict[str, object]] = []
        for r in schema_rows:
            results.append(
                {
                    "profile": str(r["db_profile"] or ""),
                    "db_backend": str(r["db_backend"] or ""),
                    "database": str(r["database_name"] or ""),
                    "schema": str(r["schema_name"] or ""),
                    "table": None,
                    "column": None,
                    "match_field": "schema",
                }
            )
        for r in table_rows:
            results.append(
                {
                    "profile": str(r["db_profile"] or ""),
                    "db_backend": str(r["db_backend"] or ""),
                    "database": str(r["database_name"] or ""),
                    "schema": str(r["schema_name"] or ""),
                    "table": str(r["table_name"] or ""),
                    "column": None,
                    "match_field": "table",
                }
            )
        for r in column_rows:
            results.append(
                {
                    "profile": str(r["db_profile"] or ""),
                    "db_backend": str(r["db_backend"] or ""),
                    "database": str(r["database_name"] or ""),
                    "schema": str(r["schema_name"] or ""),
                    "table": str(r["table_name"] or ""),
                    "column": str(r["column_name"] or ""),
                    "match_field": "column",
                }
            )

        truncated = (
            len(schema_rows) > limit
            or len(table_rows) > limit
            or len(column_rows) > limit
            or len(results) > limit
        )
        return results[:limit], truncated

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
