"""PostgreSQL backend adapter."""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import DatabaseAdapter


class PostgreSQLAdapter(DatabaseAdapter):
    name = "postgresql"

    def create_engine(self) -> Engine:
        return create_engine(self.cfg.url, pool_pre_ping=True)

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"information_schema", "pg_catalog", "pg_toast"})

    # ── Materialized views ────────────────────────────────────────────────

    def list_materialized_views(self, engine: Engine, schema: str) -> list[str]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT c.relname FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relkind = 'm' "
                    "ORDER BY c.relname"
                ),
                {"schema": schema},
            ).fetchall()
        return [r[0] for r in rows]

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT "
            f"  COUNT(*) FILTER (WHERE {quoted_col} IS NULL) AS null_cnt, "
            f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
            f"  MIN({quoted_col}::text) AS min_val, "
            f"  MAX({quoted_col}::text) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT {quoted_col}::text FROM {fqn} "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(
        self, engine: Engine, schema: str, table: str
    ) -> dict[str, int]:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT COALESCE(seq_scan, 0), COALESCE(idx_scan, 0), "
                    "COALESCE(n_live_tup, 0) "
                    "FROM pg_stat_user_tables "
                    "WHERE schemaname = :schema AND relname = :table"
                ),
                {"schema": schema, "table": table},
            ).fetchone()
        if row:
            return {
                "seq_scan": int(row[0] or 0),
                "idx_scan": int(row[1] or 0),
                "n_live_tup": int(row[2] or 0),
            }
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": 0}

    def stats_label(self) -> str:
        return "pg_stat_user_tables"

    # ── Schema / database comments ────────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT obj_description(n.oid, 'pg_namespace') "
                    "FROM pg_namespace n WHERE n.nspname = :schema"
                ),
                {"schema": schema},
            ).fetchone()
        return row[0] if row else None

    def get_database_comment(self, engine: Engine) -> str | None:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT shobj_description(d.oid, 'pg_database') "
                    "FROM pg_database d WHERE d.datname = current_database()"
                )
            ).fetchone()
        return row[0] if row else None

    # ── Incoming foreign keys ─────────────────────────────────────────────

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        src_ns.nspname  AS source_schema,
                        src.relname     AS source_table,
                        src_col.attname AS source_column,
                        tgt_col.attname AS target_column
                    FROM pg_constraint con
                    JOIN pg_class src ON src.oid = con.conrelid
                    JOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace
                    JOIN pg_class tgt ON tgt.oid = con.confrelid
                    JOIN pg_namespace tgt_ns ON tgt_ns.oid = tgt.relnamespace
                    JOIN unnest(con.conkey)  WITH ORDINALITY AS src_key(attnum, ord) ON TRUE
                    JOIN unnest(con.confkey) WITH ORDINALITY AS tgt_key(attnum, ord)
                         ON src_key.ord = tgt_key.ord
                    JOIN pg_attribute src_col
                         ON src_col.attrelid = src.oid AND src_col.attnum = src_key.attnum
                    JOIN pg_attribute tgt_col
                         ON tgt_col.attrelid = tgt.oid AND tgt_col.attnum = tgt_key.attnum
                    WHERE con.contype = 'f'
                      AND tgt_ns.nspname = :schema
                      AND tgt.relname    = :table
                    ORDER BY src_ns.nspname, src.relname, src_col.attname
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
