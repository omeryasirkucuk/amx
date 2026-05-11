"""MySQL / MariaDB backend adapter.

Driven by the PyMySQL SQLAlchemy dialect. The adapter reads metadata
out of ``information_schema`` (tables, views, routines, triggers,
events, indexes, FKs, partitions) and surfaces:

* Tables (BASE TABLE) and views.
* Stored procedures and functions via ``information_schema.ROUTINES``.
* Triggers (table-scoped DML triggers).
* Events — MySQL's scheduled jobs, no equivalent on most warehouses.
* Storage engine, row-count estimate, partition info, and on-disk
  size in :meth:`get_analytics_metadata`.

MySQL does NOT support:

* Schema-level ``COMMENT ON SCHEMA`` (no syntax). Schema comments
  are dropped at write time — capability flag is False.
* Materialized views.
* Sequences (``AUTO_INCREMENT`` is the equivalent and isn't a
  first-class object).

Comments are written via ``ALTER TABLE`` for table-level and
``ALTER TABLE ... MODIFY COLUMN`` for column-level. The latter
requires re-stating the full column definition, so the connector
only calls it after fetching the current type — handled below by
overriding :meth:`comment_sql_with_params` to inject the cached
column type.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


class MySQLAdapter(DatabaseAdapter):
    name = "mysql"
    capabilities = BackendCapabilities(
        database_comments=False,  # MySQL has no DATABASE-level comment
        schema_comments=False,  # MySQL schemas == databases; no separate COMMENT
        materialized_views=False,
        materialized_view_comments=False,
        relationships=True,
        row_count_stats=True,
        stored_procedures=True,
        functions=True,
        triggers=True,
        events=True,
        comment_asset_keywords=frozenset({"TABLE", "VIEW"}),
        supports_shared_history=True,
    )

    def create_history_schema_ddl(self, schema_name: str) -> str:
        # In MySQL, ``SCHEMA`` and ``DATABASE`` are synonyms; the DDL
        # below works on both MySQL 5.7+ and MariaDB 10.x. CHARSET pinning
        # makes JSON columns and `created_by` text portable across server
        # default-charset variations.
        return (
            f"CREATE DATABASE IF NOT EXISTS {self.quote_identifier(schema_name)} "
            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )

    def create_engine(self) -> Engine:
        return create_engine(self.cfg.url, pool_pre_ping=True)

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "access denied for user" in msg:
            return (
                "MySQL refused the credentials. Check the username/password "
                "in the profile, or grant the required privileges with "
                "`GRANT SELECT ON <db>.* TO '<user>'@'<host>';`."
            )
        if "unknown database" in msg:
            return (
                "MySQL refused: the `database` field on this profile points "
                "at a database that doesn't exist on this server. Edit the "
                "profile, or leave the field blank to pick a database at "
                "command time."
            )
        if "can't connect to mysql server" in msg:
            return (
                "MySQL refused the TCP connection. Check the host/port, "
                "and that the server is configured to accept remote logins "
                "(skip-networking must be off)."
            )
        if "1142" in msg or "command denied" in msg:
            return (
                "MySQL denied the introspection query. The user needs SELECT "
                "on `information_schema` and the target schema, plus PROCESS "
                "privilege for some `SHOW` commands."
            )
        return None

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"mysql", "information_schema", "performance_schema", "sys"})

    def quote_identifier(self, name: str) -> str:
        # MySQL uses backticks. Doubling escapes embedded backticks.
        escaped = name.replace("`", "``")
        return f"`{escaped}`"

    def list_databases(self, engine: Engine) -> list[str]:
        # In MySQL, "database" and "schema" are synonyms; this lists the
        # user-visible top-level scopes (excluding the system DBs).
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA "
                    "WHERE SCHEMA_NAME NOT IN "
                    "('mysql','information_schema','performance_schema','sys') "
                    "ORDER BY SCHEMA_NAME"
                )
            ).fetchall()
        return [str(r[0]) for r in rows]

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        # MySQL's CHAR coercion is forgiving for most types. Wrap MIN/MAX
        # in CAST for binary / spatial types where the implicit cast
        # would error.
        return (
            f"SELECT "
            f"  SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS null_cnt, "
            f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
            f"  CAST(MIN({quoted_col}) AS CHAR) AS min_val, "
            f"  CAST(MAX({quoted_col}) AS CHAR) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT CAST({quoted_col} AS CHAR) FROM {fqn} "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    def _null_count_expr(self, quoted_col: str) -> str:
        # MySQL doesn't accept FILTER. SUM(CASE) is portable across
        # 5.7 / 8.x and MariaDB.
        return f"SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END)"

    def _aggregate_text_expr(self, agg: str, quoted_col: str) -> str:
        # MySQL's existing per-column path uses the outer-cast form
        # ``CAST(MIN(col) AS CHAR)``. Mirror it so bulk and per-column
        # paths produce identical MIN/MAX values.
        return f"CAST({agg}({quoted_col}) AS CHAR)"

    def _value_text_expr(self, quoted_col: str) -> str:
        return f"CAST({quoted_col} AS CHAR)"

    # MySQL has no TABLESAMPLE; bulk_sample_sql relies on bare ``LIMIT``
    # which scans the first N rows. For perf-sensitive cases the
    # per-column escalation fallback in the connector still runs on
    # any column that didn't get enough distinct values from the
    # sequential sample.

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
        # ``TABLE_ROWS`` is precise for MyISAM and an estimate for InnoDB
        # (so it should be treated as a hint, not a count). Surface it
        # under ``n_live_tup`` to match the PG vocabulary.
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT TABLE_ROWS FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
                ),
                {"schema": schema, "table": table},
            ).fetchone()
        n = int(row[0]) if row and row[0] is not None else 0
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n}

    def stats_label(self) -> str:
        return "information_schema.TABLES (estimate for InnoDB)"

    # ── Incoming foreign keys ─────────────────────────────────────────────

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        kcu.TABLE_SCHEMA   AS source_schema,
                        kcu.TABLE_NAME     AS source_table,
                        kcu.COLUMN_NAME    AS source_column,
                        kcu.REFERENCED_COLUMN_NAME AS target_column
                    FROM information_schema.KEY_COLUMN_USAGE kcu
                    WHERE kcu.REFERENCED_TABLE_SCHEMA = :schema
                      AND kcu.REFERENCED_TABLE_NAME   = :table
                    ORDER BY kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.COLUMN_NAME
                    """
                ),
                {"schema": schema, "table": table},
            ).fetchall()
        return [
            {
                "source_schema": str(r[0]),
                "source_table": str(r[1]),
                "source_column": str(r[2]),
                "target_column": str(r[3]),
            }
            for r in rows
        ]

    # ── Extended object types ─────────────────────────────────────────────

    def list_stored_procedures(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT ROUTINE_NAME, ROUTINE_DEFINITION, ROUTINE_COMMENT, "
                    "DATA_TYPE, CREATED, LAST_ALTERED "
                    "FROM information_schema.ROUTINES "
                    "WHERE ROUTINE_SCHEMA = :schema AND ROUTINE_TYPE = 'PROCEDURE' "
                    "ORDER BY ROUTINE_NAME"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "procedure",
                "definition": str(r[1]) if r[1] else None,
                "comment": str(r[2]) if r[2] else None,
                "metadata": {
                    "return_type": str(r[3]) if r[3] else None,
                    "created": str(r[4]) if r[4] else None,
                    "last_altered": str(r[5]) if r[5] else None,
                },
            }
            for r in rows
        ]

    def list_functions(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT ROUTINE_NAME, ROUTINE_DEFINITION, ROUTINE_COMMENT, "
                    "DATA_TYPE, CREATED, LAST_ALTERED "
                    "FROM information_schema.ROUTINES "
                    "WHERE ROUTINE_SCHEMA = :schema AND ROUTINE_TYPE = 'FUNCTION' "
                    "ORDER BY ROUTINE_NAME"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "function",
                "definition": str(r[1]) if r[1] else None,
                "comment": str(r[2]) if r[2] else None,
                "metadata": {
                    "return_type": str(r[3]) if r[3] else None,
                    "created": str(r[4]) if r[4] else None,
                    "last_altered": str(r[5]) if r[5] else None,
                },
            }
            for r in rows
        ]

    def list_triggers(
        self, engine: Engine, schema: str, table: str | None = None
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT TRIGGER_NAME, EVENT_MANIPULATION, ACTION_TIMING, "
            "EVENT_OBJECT_TABLE, ACTION_STATEMENT "
            "FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA = :schema"
        )
        params: dict[str, Any] = {"schema": schema}
        if table:
            sql += " AND EVENT_OBJECT_TABLE = :table"
            params["table"] = table
        sql += " ORDER BY TRIGGER_NAME"
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "trigger",
                "definition": str(r[4]) if r[4] else None,
                "comment": None,
                "metadata": {
                    "event": str(r[1]) if r[1] else None,
                    "timing": str(r[2]) if r[2] else None,
                    "table": str(r[3]) if r[3] else None,
                },
            }
            for r in rows
        ]

    def list_events(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT EVENT_NAME, EVENT_BODY, EVENT_DEFINITION, EVENT_TYPE, "
                    "STATUS, EVENT_COMMENT, INTERVAL_VALUE, INTERVAL_FIELD, "
                    "STARTS, ENDS "
                    "FROM information_schema.EVENTS "
                    "WHERE EVENT_SCHEMA = :schema "
                    "ORDER BY EVENT_NAME"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": str(r[3]) if r[3] else "event",
                "definition": str(r[2]) if r[2] else None,
                "comment": str(r[5]) if r[5] else None,
                "metadata": {
                    "body": str(r[1]) if r[1] else None,
                    "status": str(r[4]) if r[4] else None,
                    "interval_value": str(r[6]) if r[6] is not None else None,
                    "interval_field": str(r[7]) if r[7] else None,
                    "starts": str(r[8]) if r[8] else None,
                    "ends": str(r[9]) if r[9] else None,
                },
            }
            for r in rows
        ]

    # ── Analytics metadata ────────────────────────────────────────────────

    def get_analytics_metadata(self, engine: Engine, schema: str, table: str) -> dict[str, Any]:
        out: dict[str, Any] = {}
        warnings: list[str] = []

        with engine.connect() as conn:
            try:
                row = conn.execute(
                    text(
                        "SELECT ENGINE, TABLE_TYPE, DATA_LENGTH, INDEX_LENGTH, "
                        "UPDATE_TIME, CREATE_OPTIONS, TABLE_COMMENT "
                        "FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
                    ),
                    {"schema": schema, "table": table},
                ).fetchone()
                if row:
                    engine_name = (str(row[0]) if row[0] else "").lower()
                    table_type = (str(row[1]) if row[1] else "").lower()
                    data_len = int(row[2] or 0)
                    idx_len = int(row[3] or 0)
                    if data_len or idx_len:
                        out["storage_bytes"] = data_len + idx_len
                    if row[4]:
                        out["last_modified"] = str(row[4])
                    if engine_name:
                        out.setdefault("storage_format", engine_name)
                    if table_type == "view":
                        out["table_type"] = "view"
                    elif "system" in table_type:
                        out["table_type"] = "system"
                    else:
                        out["table_type"] = "managed"
            except Exception as exc:
                warnings.append(f"table info: {exc}")

            # Partition info — only present on partitioned tables.
            try:
                rows = conn.execute(
                    text(
                        "SELECT DISTINCT PARTITION_METHOD, PARTITION_EXPRESSION "
                        "FROM information_schema.PARTITIONS "
                        "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table "
                        "AND PARTITION_NAME IS NOT NULL"
                    ),
                    {"schema": schema, "table": table},
                ).fetchall()
                if rows:
                    method = str(rows[0][0] or "").lower()
                    expr = str(rows[0][1] or "")
                    if method:
                        out["partition_strategy"] = method
                    if expr:
                        # Best-effort: split by comma if it's a column list.
                        out["partition_keys"] = [
                            c.strip().strip("`") for c in expr.split(",") if c.strip()
                        ]
            except Exception as exc:
                warnings.append(f"partitions: {exc}")

            # Indexes from STATISTICS (one row per (index, column)).
            try:
                rows = conn.execute(
                    text(
                        "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE, SEQ_IN_INDEX "
                        "FROM information_schema.STATISTICS "
                        "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table "
                        "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
                    ),
                    {"schema": schema, "table": table},
                ).fetchall()
                idx_map: dict[str, dict[str, Any]] = {}
                for r in rows:
                    name = str(r[0])
                    bucket = idx_map.setdefault(
                        name, {"name": name, "columns": [], "unique": int(r[2] or 0) == 0}
                    )
                    bucket["columns"].append(str(r[1]))
                if idx_map:
                    out["indexes"] = list(idx_map.values())
            except Exception as exc:
                warnings.append(f"indexes: {exc}")

        if warnings:
            out["warnings"] = warnings
        return out

    # ── Bulk schema metadata ──────────────────────────────────────────────

    def bulk_schema_metadata(
        self,
        engine: Engine,
        schema: str,
        *,
        catalog: str = "",
    ) -> dict[str, dict[str, Any]] | None:
        """One ``INFORMATION_SCHEMA`` round-trip per schema.

        MySQL's ``TABLE_COMMENT`` column carries the user-supplied
        comment AND any innodb internal stats ("InnoDB free: 12345 kB").
        We keep it verbatim — those internal-stat strings have a stable
        prefix the SPA can strip if it ever matters, and the typical
        case is a clean user comment.
        """
        try:
            out: dict[str, dict[str, Any]] = {}
            with engine.connect() as conn:
                table_rows = conn.execute(
                    text(
                        "SELECT TABLE_NAME, TABLE_TYPE, TABLE_COMMENT "
                        "FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_SCHEMA = :schema"
                    ),
                    {"schema": schema},
                ).fetchall()
                for r in table_rows:
                    raw_kind = str(r[1] or "").upper()
                    kind = "VIEW" if "VIEW" in raw_kind else "TABLE"
                    out[str(r[0])] = {
                        "table_comment": str(r[2]) if r[2] else None,
                        "columns": {},
                        "kind": kind,
                    }
                col_rows = conn.execute(
                    text(
                        "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_COMMENT "
                        "FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_SCHEMA = :schema "
                        "ORDER BY TABLE_NAME, ORDINAL_POSITION"
                    ),
                    {"schema": schema},
                ).fetchall()
            for r in col_rows:
                entry = out.setdefault(
                    str(r[0]),
                    {"table_comment": None, "columns": {}, "kind": "TABLE"},
                )
                entry["columns"][str(r[1])] = str(r[2]) if r[2] else None
            return out or None
        except Exception:
            return None

    # ── Comment writing ───────────────────────────────────────────────────
    #
    # MySQL has no ``COMMENT ON`` statement. Instead:
    # * ``ALTER TABLE <fqn> COMMENT = '...'`` for table comments.
    # * ``ALTER TABLE <fqn> MODIFY COLUMN <col> <type> COMMENT '...'``
    #   for column comments — and the type clause is mandatory.
    #
    # The connector calls ``set_table_comment_sql`` / ``set_column_comment_sql``
    # to get a *template* and fills the ``:cmt`` bind via
    # ``comment_sql_with_params``. MySQL doesn't accept named binds in
    # DDL, so we override ``comment_sql_with_params`` to inline the
    # comment as a quoted literal.

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        # ``asset_keyword`` is "TABLE" or "VIEW"; both use the same
        # ALTER-with-COMMENT shape on MySQL.
        fqn = self.fully_qualified_name(schema, table)
        return f"ALTER {asset_keyword} {fqn} COMMENT = :cmt"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        # We don't know the column's type at template-build time. The
        # caller is expected to fetch the type and stitch it in via
        # ``comment_sql_with_params`` (which we override to also accept
        # a ``column_type`` key in extras). Placeholder ``__TYPE__``
        # gets replaced when params are bound.
        fqn = self.fully_qualified_name(schema, table)
        col = self.quote_identifier(column)
        return f"ALTER TABLE {fqn} MODIFY COLUMN {col} __TYPE__ COMMENT :cmt"

    def set_schema_comment_sql(self, schema: str) -> str:
        # MySQL has no schema-level COMMENT; the matching capability
        # flag is False so the connector should never call this.
        raise self.unsupported("set_schema_comment_sql")

    def set_database_comment_sql(self) -> str:
        raise self.unsupported("set_database_comment_sql")

    def comment_sql_with_params(
        self, stmt_template: str, comment: str
    ) -> tuple[str, dict[str, Any]]:
        # Inline the comment as a quoted literal — MySQL doesn't accept
        # ``?`` / ``:cmt`` binds in DDL on every driver/version combo.
        # Type-stitching for column comments is handled by the caller
        # before this is invoked (see :meth:`set_column_comment_sql`).
        literal = self.quote_literal(comment)
        return stmt_template.replace(":cmt", literal), {}
