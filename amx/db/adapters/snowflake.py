"""Snowflake backend adapter."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import DatabaseAdapter


class SnowflakeAdapter(DatabaseAdapter):
    name = "snowflake"

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

    # ── Materialized views ────────────────────────────────────────────────

    def list_materialized_views(self, engine: Engine, schema: str) -> list[str]:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SHOW MATERIALIZED VIEWS IN SCHEMA :schema"),
                {"schema": schema},
            ).fetchall()
        return [r[1] for r in rows] if rows else []

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
            f"SELECT DISTINCT {quoted_col}::VARCHAR FROM {fqn} "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(
        self, engine: Engine, schema: str, table: str
    ) -> dict[str, int]:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT ROW_COUNT FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
                ),
                {"schema": schema.upper(), "table": table.upper()},
            ).fetchone()
        n_live = int(row[0] or 0) if row else 0
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n_live}

    def stats_label(self) -> str:
        return "INFORMATION_SCHEMA.TABLES"

    # ── Schema / database comments ────────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT COMMENT FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME = :schema"
                ),
                {"schema": schema.upper()},
            ).fetchone()
        return row[0] if row and row[0] else None

    def get_database_comment(self, engine: Engine) -> str | None:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("SHOW DATABASES LIKE :db"), {"db": self.cfg.database}).fetchall()
            if rows:
                for r in rows:
                    comment_idx = 4
                    if len(r) > comment_idx and r[comment_idx]:
                        return str(r[comment_idx])
        except Exception:
            pass
        return None

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
                    {"schema": schema.upper(), "table": table.upper()},
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
        except Exception:
            return []

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
