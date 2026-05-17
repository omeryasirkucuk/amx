"""Snowflake backend adapter."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


class SnowflakeAdapter(DatabaseAdapter):
    name = "snowflake"
    capabilities = BackendCapabilities(
        materialized_view_comments=True,
        materialized_views=True,
        relationships=True,
        row_count_stats=True,
        full_scan_when_row_count_unknown=False,
        stored_procedures=True,
        functions=True,
        sequences=True,
        events=True,  # SHOW TASKS — Snowflake's scheduled jobs
        volumes=True,  # SHOW STAGES — Snowflake's file-storage primitive
        datashares=True,
        external_tables=True,
        supports_shared_history=True,
        comment_asset_keywords=frozenset({"TABLE", "VIEW", "MATERIALIZED VIEW"}),
    )

    def create_history_schema_ddl(self, schema_name: str) -> str:
        # Snowflake fully-qualifies on a database; if the active profile
        # has a database pinned we emit a ``"DB"."AMX"`` qualified DDL
        # so the schema lands in the expected database. Otherwise the
        # connection's default database is used (the user picked it via
        # /database) and the unqualified form below works.
        db = getattr(self.cfg, "database", "") or ""
        if db:
            return (
                f"CREATE SCHEMA IF NOT EXISTS "
                f"{self.quote_identifier(db)}.{self.quote_identifier(schema_name)}"
            )
        return f"CREATE SCHEMA IF NOT EXISTS {self.quote_identifier(schema_name)}"

    def create_engine(self) -> Engine:
        try:
            from sqlalchemy import create_engine
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "SQLAlchemy is required for Snowflake. Install with: pip install 'amx-cli[snowflake]'"
            ) from exc
        try:
            import snowflake.sqlalchemy  # noqa: F401 — registers dialect
        except ImportError as exc:
            raise ImportError(
                "snowflake-sqlalchemy is required for the Snowflake backend. "
                "Install the extra: pip install 'amx-cli[snowflake]'"
            ) from exc
        # No pool_pre_ping — every checkout would issue a `SELECT 1` that
        # keeps the Snowflake warehouse warm and bills credits. Use
        # pool_recycle to refresh stale connections on a time basis; the
        # connector handles real session expiry on the next query.
        return create_engine(self.cfg.url, pool_recycle=1800)

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA", "information_schema"})

    def normalize_identifier(self, value: str) -> str:
        if not value:
            return value
        if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
            return value
        return value.upper()

    def list_databases(self, engine: Engine) -> list[str]:
        """Return user-visible Snowflake databases via ``SHOW DATABASES``.

        Filters Snowflake-managed databases (``SNOWFLAKE``,
        ``SNOWFLAKE_SAMPLE_DATA``) when the user has any of their own —
        otherwise the runtime picker would surface nothing but
        managed metadata DBs. If those *are* the only ones visible the
        list is returned as-is so the user still gets a non-empty
        choice (a sandbox account with sample data only is a real
        case worth supporting).
        """
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("SHOW DATABASES")).fetchall()
        except Exception:
            return []
        # SHOW DATABASES exposes ``name`` as column 1 (column 0 is
        # ``created_on``).
        names: list[str] = []
        for r in rows:
            try:
                mapping = r._mapping if hasattr(r, "_mapping") else None
                if mapping is not None and "name" in mapping:
                    name = str(mapping["name"])
                else:
                    name = str(r[1])
            except Exception:
                continue
            if name:
                names.append(name)
        managed = {"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"}
        user_dbs = [n for n in names if n.upper() not in managed]
        return user_dbs or names

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "insufficient privileges" in msg or "not authorized" in msg:
            return "Insufficient Snowflake privileges. Grant USAGE on database/schema and SELECT on the object."
        if "does not exist" in msg or "not exist or not authorized" in msg:
            return "Snowflake object is missing or not visible to the active role."
        if "warehouse" in msg and ("suspended" in msg or "not running" in msg):
            return "Snowflake warehouse is unavailable. Start the warehouse or select an active warehouse."
        return None

    # ── Materialized views ────────────────────────────────────────────────

    def list_materialized_views(self, engine: Engine, schema: str) -> list[str]:
        stmt = f"SHOW MATERIALIZED VIEWS IN SCHEMA {self.quote_identifier(schema)}"
        with engine.connect() as conn:
            rows = conn.execute(text(stmt)).fetchall()
        out: list[str] = []
        for row in rows:
            mapping = row._mapping if hasattr(row, "_mapping") else {}
            name = mapping.get("name") or mapping.get("NAME")
            if name:
                out.append(str(name))
            elif len(row) > 1 and row[1]:
                out.append(str(row[1]))
        return out

    # ── Identifier quoting ────────────────────────────────────────────────

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'

    def fully_qualified_name(self, schema: str, table: str) -> str:
        """Emit a 3-level ``"db"."schema"."table"`` name when the profile
        pins a database.

        Snowflake binds the active database at connection time, so
        ``"schema"."table"`` is valid for sample / profiling queries that
        run against the current database. Comment-write DDL
        (``COMMENT ON TABLE``) is also fine without the database when
        the writer is connected with the right DB. The 3-level form is
        required only when a query references an object in a *different*
        database than the active one — but emitting it whenever the
        profile names a database is a no-op for the active-DB case and
        avoids "object does not exist" errors when the user runs
        cross-database lookups via the agent.
        """
        db = getattr(self.cfg, "database", "") or ""
        if db:
            return (
                f"{self.quote_identifier(db)}."
                f"{self.quote_identifier(schema)}."
                f"{self.quote_identifier(table)}"
            )
        return f"{self.quote_identifier(schema)}.{self.quote_identifier(table)}"

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT "
            f"  SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS null_cnt, "
            f"  {self._distinct_count_expr(quoted_col)} AS dist_cnt, "
            f"  MIN({quoted_col}::VARCHAR) AS min_val, "
            f"  MAX({quoted_col}::VARCHAR) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT {quoted_col}::VARCHAR FROM {fqn} SAMPLE (1) "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    def _null_count_expr(self, quoted_col: str) -> str:
        # Snowflake doesn't accept ``COUNT(*) FILTER (WHERE …)``; use the
        # SUM(CASE) form that the per-column path also uses.
        return f"SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END)"

    def _distinct_count_expr(self, quoted_col: str) -> str:
        # Snowflake's APPROX_COUNT_DISTINCT is an HLL implementation and
        # bills a small fraction of an exact COUNT(DISTINCT) on wide
        # / high-cardinality columns. Default behaviour unchanged
        # (cfg flag defaults to False).
        if getattr(self.cfg, "profiling_approximate", False):
            return f"APPROX_COUNT_DISTINCT({quoted_col})"
        return f"COUNT(DISTINCT {quoted_col})"

    def _aggregate_text_expr(self, agg: str, quoted_col: str) -> str:
        return f"{agg}({quoted_col}::VARCHAR)"

    def _value_text_expr(self, quoted_col: str) -> str:
        return f"{quoted_col}::VARCHAR"

    def _bulk_sample_clause(self) -> str:
        # Snowflake row sampling: ``SAMPLE (n)`` where n is the percentage.
        # Same as the per-column path — a 1% slice of the table.
        return "SAMPLE (1)"

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
        row = self._fetch_table_row(engine, schema, table, "ROW_COUNT")
        n_live = int(row[0] or 0) if row else 0
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n_live}

    def stats_label(self) -> str:
        return "INFORMATION_SCHEMA.TABLES"

    # ── Bulk catalog metadata ─────────────────────────────────────────────

    def bulk_catalog_metadata(
        self,
        engine: Engine,
        catalog: str = "",
    ) -> dict[str, str | None] | None:
        """``INFORMATION_SCHEMA.SCHEMATA`` is database-scoped on
        Snowflake; the active connection's database implicitly bounds
        the result. Skips the always-empty ``INFORMATION_SCHEMA``
        schema itself so the sidebar doesn't show it.
        """
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT SCHEMA_NAME, COMMENT "
                        "FROM INFORMATION_SCHEMA.SCHEMATA "
                        "WHERE SCHEMA_NAME <> 'INFORMATION_SCHEMA'"
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
        """Snowflake INFORMATION_SCHEMA queries are database-scoped, so
        the active connection's database implicitly bounds the result —
        no explicit ``catalog`` arg needed unless the caller crossed
        warehouses, which AMX doesn't.

        Snowflake folds identifiers to uppercase; ``schema`` is normalised
        by the connector before reaching the adapter so a quoted match
        works as expected.
        """
        try:
            out: dict[str, dict[str, Any]] = {}
            with engine.connect() as conn:
                table_rows = conn.execute(
                    text(
                        "SELECT TABLE_NAME, TABLE_TYPE, COMMENT "
                        "FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_SCHEMA = :schema"
                    ),
                    {"schema": schema},
                ).fetchall()
                for r in table_rows:
                    raw_kind = str(r[1] or "").upper()
                    if "VIEW" in raw_kind and "MATERIALIZED" in raw_kind:
                        kind = "MATERIALIZED VIEW"
                    elif "VIEW" in raw_kind:
                        kind = "VIEW"
                    else:
                        kind = "TABLE"
                    out[str(r[0])] = {
                        "table_comment": str(r[2]) if r[2] else None,
                        "columns": {},
                        "kind": kind,
                    }
                col_rows = conn.execute(
                    text(
                        "SELECT TABLE_NAME, COLUMN_NAME, COMMENT "
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

    # ── Schema / database comments ────────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        row = self._fetch_schema_row(engine, schema, "COMMENT")
        return row[0] if row and row[0] else None

    def get_database_comment(self, engine: Engine) -> str | None:
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(f"SHOW DATABASES LIKE {self.quote_literal(self.cfg.database)}")
                ).fetchall()
            if rows:
                for r in rows:
                    mapping = r._mapping if hasattr(r, "_mapping") else {}
                    comment = mapping.get("comment") or mapping.get("COMMENT")
                    if comment:
                        return str(comment)
        except Exception:
            pass
        return None

    def _fetch_schema_row(self, engine: Engine, schema: str, column: str):
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    f"SELECT {column} FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :schema"
                ),
                {"schema": schema},
            ).fetchone()
            if row or schema.upper() == schema:
                return row
            return conn.execute(
                text(
                    f"SELECT {column} FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :schema"
                ),
                {"schema": schema.upper()},
            ).fetchone()

    def _fetch_table_row(self, engine: Engine, schema: str, table: str, column: str):
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    f"SELECT {column} FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
                ),
                {"schema": schema, "table": table},
            ).fetchone()
            if row or (schema.upper() == schema and table.upper() == table):
                return row
            return conn.execute(
                text(
                    f"SELECT {column} FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
                ),
                {"schema": schema.upper(), "table": table.upper()},
            ).fetchone()

    # ── Incoming foreign keys ─────────────────────────────────────────────

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT "
                        "  FK_TABLE_SCHEMA, FK_TABLE_NAME, FK_COLUMN_NAME, "
                        "  PK_COLUMN_NAME "
                        "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc "
                        "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk "
                        "  ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME "
                        "     AND rc.CONSTRAINT_SCHEMA = fk.CONSTRAINT_SCHEMA "
                        "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk "
                        "  ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME "
                        "     AND rc.UNIQUE_CONSTRAINT_SCHEMA = pk.CONSTRAINT_SCHEMA "
                        "     AND fk.ORDINAL_POSITION = pk.ORDINAL_POSITION "
                        "WHERE pk.TABLE_SCHEMA = :schema "
                        "  AND pk.TABLE_NAME = :table"
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
        except Exception as exc:
            actionable = self.actionable_profile_error(exc)
            raise RuntimeError(actionable or str(exc)) from exc

    # ── Extended object types ─────────────────────────────────────────────

    def _show_to_dicts(
        self,
        engine: Engine,
        sql: str,
        type_label: str,
        name_keys: tuple[str, ...] = ("name", "NAME"),
        comment_keys: tuple[str, ...] = ("comment", "COMMENT"),
        extra_keys: tuple[str, ...] = (),
    ) -> list[dict[str, Any]]:
        """Run a Snowflake ``SHOW`` statement and reshape the rows into the
        adapter's uniform list-result dict shape. ``SHOW`` returns rows
        with mixed-case column names that depend on the object type;
        we look at both upper- and lower-case variants to be safe.
        """
        with engine.connect() as conn:
            rows = conn.execute(text(sql)).fetchall()

        def _pick(mapping: Any, keys: tuple[str, ...]) -> Any:
            for k in keys:
                if k in mapping and mapping[k] is not None:
                    return mapping[k]
            return None

        out: list[dict[str, Any]] = []
        for row in rows:
            mapping = row._mapping if hasattr(row, "_mapping") else {}
            name = _pick(mapping, name_keys)
            if name is None:
                continue
            extras: dict[str, Any] = {}
            for k in extra_keys:
                v = mapping.get(k)
                if v is None:
                    v = mapping.get(k.upper())
                if v is not None:
                    extras[k.lower()] = v if isinstance(v, (str, int, float, bool)) else str(v)
            comment = _pick(mapping, comment_keys)
            out.append(
                {
                    "name": str(name),
                    "type": type_label,
                    "definition": None,
                    "comment": str(comment) if comment else None,
                    "metadata": extras,
                }
            )
        return out

    def list_views_with_definitions(
        self,
        engine: Engine,
        schema: str,
    ) -> list[dict[str, Any]]:
        from sqlalchemy import text

        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text(
                        "SELECT TABLE_NAME, VIEW_DEFINITION, COMMENT "
                        "FROM INFORMATION_SCHEMA.VIEWS "
                        "WHERE TABLE_SCHEMA = :schema "
                        "ORDER BY TABLE_NAME"
                    ),
                    {"schema": schema.upper()},
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
        sql = f"SHOW PROCEDURES IN SCHEMA {self.quote_identifier(schema)}"
        return self._show_to_dicts(engine, sql, "procedure", extra_keys=("arguments", "language"))

    def list_functions(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        sql = f"SHOW USER FUNCTIONS IN SCHEMA {self.quote_identifier(schema)}"
        return self._show_to_dicts(engine, sql, "function", extra_keys=("arguments", "language"))

    def list_sequences(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        sql = f"SHOW SEQUENCES IN SCHEMA {self.quote_identifier(schema)}"
        return self._show_to_dicts(engine, sql, "sequence", extra_keys=("interval", "next_value"))

    def list_events(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # Snowflake "tasks" are scheduled SQL statements — the closest
        # analogue to MySQL events / SQL Server Agent jobs.
        sql = f"SHOW TASKS IN SCHEMA {self.quote_identifier(schema)}"
        return self._show_to_dicts(
            engine, sql, "task", extra_keys=("schedule", "state", "warehouse", "definition")
        )

    def list_volumes(self, engine: Engine, catalog: str, schema: str) -> list[dict[str, Any]]:
        # Snowflake stages — internal or external file-storage. ``catalog``
        # arg is unused (Snowflake stages are schema-scoped).
        sql = f"SHOW STAGES IN SCHEMA {self.quote_identifier(schema)}"
        return self._show_to_dicts(
            engine, sql, "stage", extra_keys=("type", "url", "cloud", "region")
        )

    def list_datashares(self, engine: Engine) -> list[dict[str, Any]]:
        # SHOW SHARES is account-level. Listing both inbound and
        # outbound shares; the ``kind`` column distinguishes.
        return self._show_to_dicts(
            engine,
            "SHOW SHARES",
            "share",
            name_keys=("name", "NAME"),
            extra_keys=("kind", "owner_account", "to_accounts", "listing_global_name"),
        )

    def list_external_tables(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        sql = f"SHOW EXTERNAL TABLES IN SCHEMA {self.quote_identifier(schema)}"
        return self._show_to_dicts(
            engine,
            sql,
            "external_table",
            extra_keys=("location", "file_format_name", "auto_refresh"),
        )

    # ── Analytics metadata ────────────────────────────────────────────────

    def get_analytics_metadata(self, engine: Engine, schema: str, table: str) -> dict[str, Any]:
        """Snowflake analytics metadata.

        Pulls clustering keys / size / row count / last_altered /
        table_type from ``INFORMATION_SCHEMA.TABLES``. Tag references
        come from ``INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS``
        when available; columns whose tag NAME contains "PII" or
        "SENSITIVE" are surfaced as ``pii_columns``.

        Snowflake stores all managed tables in proprietary micropartitions
        — ``storage_format`` is set to ``native`` for managed tables and
        ``external`` for external tables.
        """
        out: dict[str, Any] = {}
        warnings: list[str] = []

        with engine.connect() as conn:
            try:
                row = conn.execute(
                    text(
                        """
                        SELECT
                            CLUSTERING_KEY,
                            BYTES,
                            ROW_COUNT,
                            CAST(LAST_ALTERED AS VARCHAR) AS last_altered,
                            TABLE_TYPE,
                            IS_TRANSIENT
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
                        LIMIT 1
                        """
                    ),
                    {"schema": schema.upper(), "table": table.upper()},
                ).fetchone()
                if row:
                    if row[0]:
                        # CLUSTERING_KEY is the literal "LINEAR(col1, col2)" string
                        # Snowflake reports. Strip the wrapper.
                        ck = str(row[0])
                        if ck.upper().startswith("LINEAR(") and ck.endswith(")"):
                            inner = ck[len("LINEAR(") : -1]
                            out["clustering_keys"] = [
                                c.strip() for c in inner.split(",") if c.strip()
                            ]
                        else:
                            out["clustering_keys"] = [ck]
                    if row[1] is not None:
                        out["storage_bytes"] = int(row[1])
                    if row[3]:
                        out["last_modified"] = str(row[3])
                    raw_type = str(row[4] or "").lower()
                    type_map = {
                        "base table": "managed",
                        "view": "view",
                        "materialized view": "materialized_view",
                        "external table": "external",
                        "temporary table": "temporary",
                    }
                    out["table_type"] = type_map.get(raw_type, raw_type)
                    out["storage_format"] = "external" if "external" in raw_type else "native"
            except Exception as exc:
                warnings.append(f"INFORMATION_SCHEMA.TABLES: {exc}")

            # ── Tags / PII columns ──
            # TAG_REFERENCES_ALL_COLUMNS may not be readable to all
            # roles; soft-fail and leave the field empty when blocked.
            try:
                rows = conn.execute(
                    text(
                        """
                        SELECT COLUMN_NAME, TAG_NAME, TAG_VALUE
                        FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                            :fqn, 'TABLE'
                        ))
                        """
                    ),
                    {"fqn": f"{schema}.{table}"},
                ).fetchall()
                tags: dict[str, str] = {}
                pii_cols: list[str] = []
                for r in rows:
                    column_name = str(r[0] or "")
                    tag_name = str(r[1] or "")
                    tag_value = str(r[2] or "")
                    if not tag_name:
                        continue
                    tags[f"{column_name}:{tag_name}" if column_name else tag_name] = tag_value
                    upper = tag_name.upper()
                    if column_name and ("PII" in upper or "SENSITIVE" in upper or "GDPR" in upper):
                        if column_name not in pii_cols:
                            pii_cols.append(column_name)
                if tags:
                    out["tags"] = tags
                if pii_cols:
                    out["pii_columns"] = pii_cols
            except Exception as exc:
                warnings.append(f"TAG_REFERENCES_ALL_COLUMNS: {exc}")

        if warnings:
            out["warnings"] = warnings
        return out

    # ── Comment writing ───────────────────────────────────────────────────

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON {asset_keyword} {fqn} IS :cmt"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON COLUMN {fqn}.{self.quote_identifier(column)} IS :cmt"

    def set_schema_comment_sql(self, schema: str) -> str:
        return f"COMMENT ON SCHEMA {self.quote_identifier(schema)} IS :cmt"

    def set_database_comment_sql(self) -> str:
        return f"COMMENT ON DATABASE {self.quote_identifier(self.cfg.database)} IS :cmt"
