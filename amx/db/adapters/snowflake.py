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
        comment_asset_keywords=frozenset({"TABLE", "VIEW", "MATERIALIZED VIEW"}),
    )

    def create_engine(self) -> Engine:
        try:
            from sqlalchemy import create_engine
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "SQLAlchemy is required for Snowflake. "
                "Install with: pip install 'amx[snowflake]'"
            ) from exc
        try:
            import snowflake.sqlalchemy  # noqa: F401 — registers dialect
        except ImportError as exc:
            raise ImportError(
                "snowflake-sqlalchemy is required for the Snowflake backend. "
                "Reinstall AMX: pip install -U amx"
            ) from exc
        return create_engine(self.cfg.url, pool_pre_ping=True)

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA", "information_schema"})

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

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT "
            f"  SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS null_cnt, "
            f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
            f"  MIN({quoted_col}::VARCHAR) AS min_val, "
            f"  MAX({quoted_col}::VARCHAR) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT {quoted_col}::VARCHAR FROM {fqn} SAMPLE (1) "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(
        self, engine: Engine, schema: str, table: str
    ) -> dict[str, int]:
        row = self._fetch_table_row(engine, schema, table, "ROW_COUNT")
        n_live = int(row[0] or 0) if row else 0
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n_live}

    def stats_label(self) -> str:
        return "INFORMATION_SCHEMA.TABLES"

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
                    f"SELECT {column} FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME = :schema"
                ),
                {"schema": schema},
            ).fetchone()
            if row or schema.upper() == schema:
                return row
            return conn.execute(
                text(
                    f"SELECT {column} FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME = :schema"
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

    # ── Comment writing ───────────────────────────────────────────────────

    def set_table_comment_sql(
        self, schema: str, table: str, asset_keyword: str
    ) -> str:
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON {asset_keyword} {fqn} IS :cmt"

    def set_column_comment_sql(
        self, schema: str, table: str, column: str
    ) -> str:
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON COLUMN {fqn}.{self.quote_identifier(column)} IS :cmt"

    def set_schema_comment_sql(self, schema: str) -> str:
        return f"COMMENT ON SCHEMA {self.quote_identifier(schema)} IS :cmt"

    def set_database_comment_sql(self) -> str:
        return f"COMMENT ON DATABASE {self.quote_identifier(self.cfg.database)} IS :cmt"
