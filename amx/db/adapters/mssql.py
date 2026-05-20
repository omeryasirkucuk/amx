"""SQL Server (Microsoft SQL Server / Azure SQL) backend adapter.

Driven by the pyodbc SQLAlchemy dialect. Connection requires a
locally-installed ODBC driver (typically ``ODBC Driver 18 for SQL
Server``) — the wizard prompts for the driver name and the URL
builder defaults to ODBC 18 when blank.

What this adapter exposes:

* Tables, views, indexed views (the closest equivalent to a
  materialized view).
* Stored procedures (``sys.procedures``).
* Functions — scalar (FN), table-valued (TF), inline TVF (IF) — all
  surfaced as ``functions`` with ``metadata.subtype`` to distinguish.
* Triggers (``sys.triggers``).
* Sequences (``sys.sequences``).
* Synonyms (``sys.synonyms`` — SQL Server's named aliases for
  remote/local objects).
* Extended properties as comments — see write-back notes below.
* Partition strategy, row-count estimate, and on-disk size in
  :meth:`get_analytics_metadata`.

Write-back: SQL Server has no ``COMMENT ON`` statement. Object
descriptions live in ``sys.extended_properties`` under the
``MS_Description`` name, written by ``sp_addextendedproperty`` and
updated by ``sp_updateextendedproperty``. The two are not
interchangeable — adding twice errors, and updating a non-existent
property errors. We use a single ``MERGE``-style block that
``IF EXISTS`` checks before picking which proc to call.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


class MSSQLAdapter(DatabaseAdapter):
    name = "mssql"
    capabilities = BackendCapabilities(
        # SQL Server stores all comments as extended properties, including
        # at the database level. Setting True so the connector exposes
        # them, but write-back is special-cased (see comment_sql_with_params).
        database_comments=True,
        schema_comments=True,
        materialized_view_comments=False,
        materialized_views=False,  # use list_external_tables for indexed views? No — they're regular views
        relationships=True,
        row_count_stats=True,
        stored_procedures=True,
        functions=True,
        sequences=True,
        triggers=True,
        synonyms=True,
        comment_asset_keywords=frozenset({"TABLE", "VIEW"}),
        supports_shared_history=True,
    )

    def create_history_schema(self, engine: Engine, schema_name: str) -> None:
        # SQL Server does not support ``IF NOT EXISTS`` on ``CREATE SCHEMA``
        # in older versions, so we use a self-contained EXEC pattern that
        # works from SQL Server 2008+. Equivalent to ``CREATE SCHEMA IF
        # NOT EXISTS`` on other backends.
        check = (
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = :schema_name) "
            f"EXEC('CREATE SCHEMA {self.quote_identifier(schema_name)}')"
        )
        with engine.begin() as conn:
            conn.execute(text(check), {"schema_name": schema_name})

    def create_history_schema_ddl(self, schema_name: str) -> str:
        return (
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = "
            f"{self.quote_literal(schema_name)}) "
            f"EXEC('CREATE SCHEMA {self.quote_identifier(schema_name)}');"
        )

    def create_history_database(self, engine: Engine, name: str) -> None:
        """Create the SQL Server database hosting the AMX schema."""
        sanitized = (name or "").strip()
        if not sanitized:
            return
        check = (
            "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = :db_name) "
            f"EXEC('CREATE DATABASE {self.quote_identifier(sanitized)}')"
        )
        with engine.begin() as conn:
            conn.execute(text(check), {"db_name": sanitized})

    def create_engine(self) -> Engine:
        return create_engine(self.cfg.url, pool_pre_ping=True)

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "im002" in msg or "data source name not found" in msg:
            return (
                "ODBC driver not found. Install Microsoft's ODBC Driver 18 for "
                "SQL Server (or set the `driver` field on the profile to a "
                "driver you do have). On macOS: `brew install msodbcsql18`. "
                "On Linux: see Microsoft's installation guide."
            )
        if "login failed for user" in msg:
            return (
                "SQL Server refused the credentials. Check the username/password, "
                "and that the login is mapped to a user in the target database "
                "with at least VIEW DEFINITION + SELECT permissions."
            )
        if "cannot open database" in msg:
            return (
                "SQL Server refused: the `database` field on this profile points "
                "at a database that doesn't exist or that the login can't access. "
                "Edit the profile, or grant CONNECT on the target database."
            )
        if "ssl provider" in msg or "encryption" in msg:
            return (
                "TLS handshake failed. Set `trust_server_certificate=true` on the "
                "profile if you're using a self-signed certificate, or set "
                "`encrypt=false` for legacy on-prem servers without TLS."
            )
        return None

    def system_schemas(self) -> frozenset[str]:
        # ``sys`` and ``INFORMATION_SCHEMA`` are server-defined; ``guest``
        # and the ``db_*`` role-mapping schemas are noise. ``dbo`` is the
        # user-visible default and stays.
        return frozenset(
            {
                "sys",
                "INFORMATION_SCHEMA",
                "guest",
                "db_owner",
                "db_accessadmin",
                "db_securityadmin",
                "db_ddladmin",
                "db_backupoperator",
                "db_datareader",
                "db_datawriter",
                "db_denydatareader",
                "db_denydatawriter",
            }
        )

    def quote_identifier(self, name: str) -> str:
        # SQL Server brackets. Bracket-in-name is escaped by doubling
        # the closing bracket per T-SQL rules.
        escaped = name.replace("]", "]]")
        return f"[{escaped}]"

    def list_databases(self, engine: Engine) -> list[str]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT name FROM sys.databases "
                    "WHERE database_id > 4 "  # exclude master/tempdb/model/msdb
                    "AND state_desc = 'ONLINE' "
                    "ORDER BY name"
                )
            ).fetchall()
        return [str(r[0]) for r in rows]

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        # SQL Server doesn't have FILTER. Use SUM(CASE ...) for null
        # count and CONVERT for the text cast (some types like
        # geography / geometry need special handling, but CONVERT to
        # NVARCHAR(MAX) covers the common cases).
        return (
            f"SELECT "
            f"  SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS null_cnt, "
            f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
            f"  CONVERT(NVARCHAR(MAX), MIN({quoted_col})) AS min_val, "
            f"  CONVERT(NVARCHAR(MAX), MAX({quoted_col})) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        # ``LIMIT`` doesn't exist on SQL Server. Use ``TOP`` with a
        # subquery so the parameter binds. pyodbc binds via ``?``, but
        # SQLAlchemy translates ``:lim`` to the right driver style.
        return (
            f"SELECT DISTINCT TOP (:lim) CONVERT(NVARCHAR(MAX), {quoted_col}) "
            f"FROM {fqn} WHERE {quoted_col} IS NOT NULL"
        )

    def _null_count_expr(self, quoted_col: str) -> str:
        return f"SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END)"

    def _aggregate_text_expr(self, agg: str, quoted_col: str) -> str:
        # MSSQL existing path uses outer ``CONVERT(NVARCHAR(MAX), MIN(col))``
        # — mirror it.
        return f"CONVERT(NVARCHAR(MAX), {agg}({quoted_col}))"

    def _value_text_expr(self, quoted_col: str) -> str:
        return f"CONVERT(NVARCHAR(MAX), {quoted_col})"

    def bulk_sample_sql(
        self,
        fqn: str,
        quoted_cols: list[str],
        row_cap: int,
    ) -> str:
        # SQL Server has no LIMIT — use ``SELECT TOP (n)`` instead.
        # ``TABLESAMPLE`` is supported but row-level (each page either
        # all-in or all-out), and on tables small enough to fit on a few
        # pages it can return zero rows. For sample collection we want
        # determinism, so we stick with TOP and skip TABLESAMPLE.
        if not quoted_cols:
            raise ValueError("bulk_sample_sql requires at least one column")
        cols_sql = ", ".join(self._value_text_expr(qc) for qc in quoted_cols)
        return f"SELECT TOP ({int(row_cap)}) {cols_sql} FROM {fqn}"

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
        # ``sys.dm_db_partition_stats.row_count`` is a fast estimate that
        # avoids a full scan. Sum across partitions for the in-row data
        # index (index_id IN (0,1)).
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT COALESCE(SUM(p.row_count), 0) "
                    "FROM sys.dm_db_partition_stats p "
                    "JOIN sys.objects o ON o.object_id = p.object_id "
                    "JOIN sys.schemas s ON s.schema_id = o.schema_id "
                    "WHERE s.name = :schema AND o.name = :table "
                    "AND p.index_id IN (0, 1)"
                ),
                {"schema": schema, "table": table},
            ).fetchone()
        n = int(row[0]) if row and row[0] is not None else 0
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n}

    def stats_label(self) -> str:
        return "sys.dm_db_partition_stats (estimate)"

    # ── Bulk catalog metadata ─────────────────────────────────────────────

    def bulk_catalog_metadata(
        self,
        engine: Engine,
        catalog: str = "",
    ) -> dict[str, str | None] | None:
        """``sys.schemas`` joined to ``sys.extended_properties`` for the
        ``MS_Description`` extended property on schema-level (class=3).
        Skips the built-in ``dbo``-adjacent system schemas the sidebar
        already filters.
        """
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT s.name, CAST(ep.value AS NVARCHAR(MAX)) "
                        "FROM sys.schemas s "
                        "LEFT JOIN sys.extended_properties ep "
                        "  ON ep.major_id = s.schema_id "
                        "  AND ep.class = 3 AND ep.name = 'MS_Description' "
                        "WHERE s.name NOT IN ("
                        "'sys','INFORMATION_SCHEMA','guest','db_accessadmin',"
                        "'db_backupoperator','db_datareader','db_datawriter',"
                        "'db_ddladmin','db_denydatareader','db_denydatawriter',"
                        "'db_owner','db_securityadmin')"
                    )
                ).fetchall()
            return {str(r[0]): (str(r[1]) if r[1] else None) for r in rows}
        except Exception:
            return None

    # ── Bulk schema metadata ──────────────────────────────────────────────

    def bulk_schema_metadata(
        self,
        engine: Engine,
        schema: str,
        *,
        catalog: str = "",
    ) -> dict[str, dict[str, Any]] | None:
        """One ``sys.extended_properties`` join per schema.

        MSSQL stashes comments as extended properties named
        ``MS_Description``. Table-level rows have ``class = 1`` and
        ``minor_id = 0``; column rows share ``class = 1`` with a
        non-zero ``minor_id`` pointing at the column id. We pull both
        in a single query and split client-side.

        The ``sys.tables`` + ``sys.views`` joins surface tables that
        have no MS_Description property at all so they still appear in
        the result (with ``table_comment = None``).
        """
        try:
            out: dict[str, dict[str, Any]] = {}
            with engine.connect() as conn:
                table_rows = conn.execute(
                    text(
                        "SELECT t.name, 'TABLE' AS kind, "
                        "       CAST(ep.value AS NVARCHAR(MAX)) AS comment "
                        "FROM sys.tables t "
                        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
                        "LEFT JOIN sys.extended_properties ep "
                        "  ON ep.major_id = t.object_id "
                        "  AND ep.minor_id = 0 AND ep.class = 1 "
                        "  AND ep.name = 'MS_Description' "
                        "WHERE s.name = :schema "
                        "UNION ALL "
                        "SELECT v.name, 'VIEW' AS kind, "
                        "       CAST(ep.value AS NVARCHAR(MAX)) AS comment "
                        "FROM sys.views v "
                        "JOIN sys.schemas s ON s.schema_id = v.schema_id "
                        "LEFT JOIN sys.extended_properties ep "
                        "  ON ep.major_id = v.object_id "
                        "  AND ep.minor_id = 0 AND ep.class = 1 "
                        "  AND ep.name = 'MS_Description' "
                        "WHERE s.name = :schema"
                    ),
                    {"schema": schema},
                ).fetchall()
                for r in table_rows:
                    out[str(r[0])] = {
                        "table_comment": str(r[2]) if r[2] else None,
                        "columns": {},
                        "kind": str(r[1]),
                    }
                col_rows = conn.execute(
                    text(
                        "SELECT o.name AS table_name, c.name AS column_name, "
                        "       CAST(ep.value AS NVARCHAR(MAX)) AS comment "
                        "FROM sys.columns c "
                        "JOIN sys.objects o ON o.object_id = c.object_id "
                        "JOIN sys.schemas s ON s.schema_id = o.schema_id "
                        "LEFT JOIN sys.extended_properties ep "
                        "  ON ep.major_id = c.object_id "
                        "  AND ep.minor_id = c.column_id AND ep.class = 1 "
                        "  AND ep.name = 'MS_Description' "
                        "WHERE s.name = :schema "
                        "ORDER BY o.name, c.column_id"
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

    # ── Schema / database comments ────────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT CAST(value AS NVARCHAR(MAX)) "
                    "FROM sys.extended_properties ep "
                    "JOIN sys.schemas s ON s.schema_id = ep.major_id "
                    "WHERE ep.name = 'MS_Description' "
                    "AND ep.class = 3 "  # 3 = schema
                    "AND s.name = :schema"
                ),
                {"schema": schema},
            ).fetchone()
        return str(row[0]) if row and row[0] else None

    def get_database_comment(self, engine: Engine) -> str | None:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT CAST(value AS NVARCHAR(MAX)) "
                    "FROM sys.extended_properties "
                    "WHERE name = 'MS_Description' AND class = 0"
                )
            ).fetchone()
        return str(row[0]) if row and row[0] else None

    # ── Incoming foreign keys ─────────────────────────────────────────────

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        src_s.name AS source_schema,
                        src_t.name AS source_table,
                        src_c.name AS source_column,
                        tgt_c.name AS target_column
                    FROM sys.foreign_keys fk
                    JOIN sys.foreign_key_columns fkc
                         ON fkc.constraint_object_id = fk.object_id
                    JOIN sys.objects src_t ON src_t.object_id = fk.parent_object_id
                    JOIN sys.schemas src_s ON src_s.schema_id = src_t.schema_id
                    JOIN sys.columns src_c
                         ON src_c.object_id = fk.parent_object_id
                        AND src_c.column_id  = fkc.parent_column_id
                    JOIN sys.objects tgt_t ON tgt_t.object_id = fk.referenced_object_id
                    JOIN sys.schemas tgt_s ON tgt_s.schema_id = tgt_t.schema_id
                    JOIN sys.columns tgt_c
                         ON tgt_c.object_id = fk.referenced_object_id
                        AND tgt_c.column_id  = fkc.referenced_column_id
                    WHERE tgt_s.name = :schema
                      AND tgt_t.name = :table
                    ORDER BY src_s.name, src_t.name, src_c.name
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

    def list_views_with_definitions(
        self,
        engine: Engine,
        schema: str,
    ) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text(
                        "SELECT v.name, m.definition, "
                        "       ep.value AS comment "
                        "FROM sys.views v "
                        "JOIN sys.sql_modules m ON m.object_id = v.object_id "
                        "LEFT JOIN sys.extended_properties ep "
                        "       ON ep.major_id = v.object_id "
                        "       AND ep.minor_id = 0 "
                        "       AND ep.name = 'MS_Description' "
                        "WHERE SCHEMA_NAME(v.schema_id) = :schema "
                        "ORDER BY v.name"
                    ),
                    {"schema": schema},
                ).fetchall()
            except Exception:
                return []
        return [
            {
                "name": str(r[0]),
                "type": "view",
                "definition": str(r[1]) if r[1] else None,
                "comment": str(r[2]) if r[2] else None,
                "metadata": {},
            }
            for r in rows
        ]

    def list_stored_procedures(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT p.name, OBJECT_DEFINITION(p.object_id) AS body, "
                    "p.create_date, p.modify_date, "
                    "(SELECT CAST(value AS NVARCHAR(MAX)) "
                    "  FROM sys.extended_properties ep "
                    "  WHERE ep.major_id = p.object_id "
                    "    AND ep.minor_id = 0 "
                    "    AND ep.name = 'MS_Description') AS comment "
                    "FROM sys.procedures p "
                    "JOIN sys.schemas s ON s.schema_id = p.schema_id "
                    "WHERE s.name = :schema "
                    "ORDER BY p.name"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "procedure",
                "definition": str(r[1]) if r[1] else None,
                "comment": str(r[4]) if r[4] else None,
                "metadata": {
                    "created": str(r[2]) if r[2] else None,
                    "modified": str(r[3]) if r[3] else None,
                },
            }
            for r in rows
        ]

    def list_functions(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # FN = scalar, TF = multi-statement table-valued, IF = inline TVF.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT o.name, o.type, OBJECT_DEFINITION(o.object_id) AS body, "
                    "o.create_date, o.modify_date "
                    "FROM sys.objects o "
                    "JOIN sys.schemas s ON s.schema_id = o.schema_id "
                    "WHERE s.name = :schema AND o.type IN ('FN','TF','IF') "
                    "ORDER BY o.name"
                ),
                {"schema": schema},
            ).fetchall()
        type_map = {"FN": "scalar", "TF": "table_valued", "IF": "inline_tvf"}
        return [
            {
                "name": str(r[0]),
                "type": "function",
                "definition": str(r[2]) if r[2] else None,
                "comment": None,
                "metadata": {
                    "subtype": type_map.get(str(r[1]).strip(), str(r[1]).strip()),
                    "created": str(r[3]) if r[3] else None,
                    "modified": str(r[4]) if r[4] else None,
                },
            }
            for r in rows
        ]

    def list_sequences(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT seq.name, t.name AS data_type, seq.start_value, "
                    "seq.increment, seq.minimum_value, seq.maximum_value, seq.is_cycling "
                    "FROM sys.sequences seq "
                    "JOIN sys.schemas s ON s.schema_id = seq.schema_id "
                    "JOIN sys.types t ON t.user_type_id = seq.user_type_id "
                    "WHERE s.name = :schema "
                    "ORDER BY seq.name"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "sequence",
                "definition": None,
                "comment": None,
                "metadata": {
                    "data_type": str(r[1]),
                    "start": int(r[2]) if r[2] is not None else None,
                    "increment": int(r[3]) if r[3] is not None else None,
                    "min": int(r[4]) if r[4] is not None else None,
                    "max": int(r[5]) if r[5] is not None else None,
                    "cycle": bool(r[6]),
                },
            }
            for r in rows
        ]

    def list_triggers(
        self, engine: Engine, schema: str, table: str | None = None
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT t.name, parent.name AS table_name, "
            "OBJECT_DEFINITION(t.object_id) AS body, t.is_disabled "
            "FROM sys.triggers t "
            "JOIN sys.objects parent ON parent.object_id = t.parent_id "
            "JOIN sys.schemas s ON s.schema_id = parent.schema_id "
            "WHERE t.parent_class = 1 AND s.name = :schema"  # 1 = OBJECT_OR_COLUMN
        )
        params: dict[str, Any] = {"schema": schema}
        if table:
            sql += " AND parent.name = :table"
            params["table"] = table
        sql += " ORDER BY t.name"
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "trigger",
                "definition": str(r[2]) if r[2] else None,
                "comment": None,
                "metadata": {
                    "table": str(r[1]),
                    "disabled": bool(r[3]),
                },
            }
            for r in rows
        ]

    def list_synonyms(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT syn.name, syn.base_object_name "
                    "FROM sys.synonyms syn "
                    "JOIN sys.schemas s ON s.schema_id = syn.schema_id "
                    "WHERE s.name = :schema "
                    "ORDER BY syn.name"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "synonym",
                "definition": str(r[1]) if r[1] else None,
                "comment": None,
                "metadata": {"target": str(r[1]) if r[1] else None},
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
                        """
                        SELECT
                            o.create_date,
                            o.modify_date,
                            o.type_desc,
                            (SELECT SUM(au.total_pages) * 8 * 1024
                             FROM sys.allocation_units au
                             JOIN sys.partitions p
                                  ON (au.container_id = p.partition_id AND au.type IN (1,3))
                             WHERE p.object_id = o.object_id) AS storage_bytes
                        FROM sys.objects o
                        JOIN sys.schemas s ON s.schema_id = o.schema_id
                        WHERE s.name = :schema AND o.name = :table
                        """
                    ),
                    {"schema": schema, "table": table},
                ).fetchone()
                if row:
                    out["last_modified"] = str(row[1]) if row[1] else ""
                    type_desc = (str(row[2]) if row[2] else "").lower()
                    if "user_table" in type_desc:
                        out["table_type"] = "managed"
                    elif "view" in type_desc:
                        out["table_type"] = "view"
                    if row[3] is not None:
                        out["storage_bytes"] = int(row[3])
            except Exception as exc:
                warnings.append(f"table info: {exc}")

            # Partition columns — present when the table is partitioned by a
            # partition function. Returns the leading partition column name.
            try:
                rows = conn.execute(
                    text(
                        """
                        SELECT c.name, ps.name AS partition_scheme
                        FROM sys.indexes i
                        JOIN sys.objects o ON o.object_id = i.object_id
                        JOIN sys.schemas s ON s.schema_id = o.schema_id
                        JOIN sys.index_columns ic
                             ON ic.object_id = i.object_id
                            AND ic.index_id = i.index_id
                            AND ic.partition_ordinal > 0
                        JOIN sys.columns c
                             ON c.object_id = i.object_id AND c.column_id = ic.column_id
                        JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
                        WHERE s.name = :schema AND o.name = :table
                        ORDER BY ic.partition_ordinal
                        """
                    ),
                    {"schema": schema, "table": table},
                ).fetchall()
                if rows:
                    out["partition_keys"] = [str(r[0]) for r in rows]
                    out["partition_strategy"] = "range"
            except Exception as exc:
                warnings.append(f"partitions: {exc}")

            # Indexes
            try:
                rows = conn.execute(
                    text(
                        """
                        SELECT i.name, i.is_unique, c.name
                        FROM sys.indexes i
                        JOIN sys.objects o ON o.object_id = i.object_id
                        JOIN sys.schemas s ON s.schema_id = o.schema_id
                        JOIN sys.index_columns ic
                             ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                        JOIN sys.columns c
                             ON c.object_id = i.object_id AND c.column_id = ic.column_id
                        WHERE s.name = :schema AND o.name = :table
                          AND i.name IS NOT NULL
                        ORDER BY i.name, ic.key_ordinal
                        """
                    ),
                    {"schema": schema, "table": table},
                ).fetchall()
                idx_map: dict[str, dict[str, Any]] = {}
                for r in rows:
                    name = str(r[0])
                    bucket = idx_map.setdefault(
                        name, {"name": name, "columns": [], "unique": bool(r[1])}
                    )
                    bucket["columns"].append(str(r[2]))
                if idx_map:
                    out["indexes"] = list(idx_map.values())
            except Exception as exc:
                warnings.append(f"indexes: {exc}")

        out.setdefault("storage_format", "native")
        if warnings:
            out["warnings"] = warnings
        return out

    # ── Comment writing ───────────────────────────────────────────────────
    #
    # SQL Server uses ``sp_addextendedproperty`` to create and
    # ``sp_updateextendedproperty`` to update — adding twice errors and
    # updating a non-existent property errors. We emit a single block
    # that branches on existence via ``IF EXISTS``, so the same SQL
    # works for both create and update paths.
    #
    # The :cmt placeholder bind is supported by pyodbc because
    # ``sp_addextendedproperty`` takes ``@value`` as a bound parameter.
    # We use named binds for readability; SQLAlchemy translates them
    # to the driver's positional-bind style.

    def _build_extended_property_sql(
        self,
        level0type: str | None,
        level0name: str | None,
        level1type: str | None = None,
        level1name: str | None = None,
        level2type: str | None = None,
        level2name: str | None = None,
    ) -> str:
        """Return a T-SQL block that adds-or-updates ``MS_Description``
        on the targeted object, using sp_addextendedproperty when the
        property is missing and sp_updateextendedproperty when present.

        Levels follow Microsoft's nomenclature: level0 is the schema,
        level1 is the table/view/proc, level2 is the column.
        """

        def _q(v: str | None) -> str:
            if v is None:
                return "NULL"
            return self.quote_literal(v)

        params = (
            f"@name = N'MS_Description', @value = :cmt, "
            f"@level0type = {_q(level0type)}, @level0name = {_q(level0name)}, "
            f"@level1type = {_q(level1type)}, @level1name = {_q(level1name)}, "
            f"@level2type = {_q(level2type)}, @level2name = {_q(level2name)}"
        )

        # Build the EXISTS predicate against sys.fn_listextendedproperty.
        # Empty string for ``default`` arg means "not specified" — we
        # have to pass the literal positional args matching the levels
        # we filled.
        exists_args = (
            f"N'MS_Description', "
            f"{_q(level0type)}, {_q(level0name)}, "
            f"{_q(level1type)}, {_q(level1name)}, "
            f"{_q(level2type)}, {_q(level2name)}"
        )

        return (
            f"IF EXISTS (SELECT 1 FROM sys.fn_listextendedproperty({exists_args})) "
            f"EXEC sp_updateextendedproperty {params} "
            f"ELSE EXEC sp_addextendedproperty {params}"
        )

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        # asset_keyword is "TABLE" or "VIEW"; sys.fn_listextendedproperty
        # accepts both as level1 type names.
        return self._build_extended_property_sql(
            level0type="SCHEMA",
            level0name=schema,
            level1type=asset_keyword,
            level1name=table,
        )

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        return self._build_extended_property_sql(
            level0type="SCHEMA",
            level0name=schema,
            level1type="TABLE",  # works for views too in this proc
            level1name=table,
            level2type="COLUMN",
            level2name=column,
        )

    def set_schema_comment_sql(self, schema: str) -> str:
        return self._build_extended_property_sql(
            level0type="SCHEMA",
            level0name=schema,
        )

    def set_database_comment_sql(self) -> str:
        # Database-level extended properties have no level filters.
        return self._build_extended_property_sql(level0type=None, level0name=None)
