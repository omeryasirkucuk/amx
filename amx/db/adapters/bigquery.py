"""Google BigQuery backend adapter."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import DatabaseAdapter


class BigQueryAdapter(DatabaseAdapter):
    name = "bigquery"

    def create_engine(self) -> Engine:
        try:
            from sqlalchemy import create_engine
        except ImportError as exc:  # pragma: no cover
            raise ImportError("SQLAlchemy is required.") from exc
        try:
            import sqlalchemy_bigquery  # noqa: F401 — registers dialect
        except ImportError as exc:
            raise ImportError(
                "sqlalchemy-bigquery is required for the BigQuery backend. "
                "Reinstall AMX: pip install -U amx"
            ) from exc
        return create_engine(self.cfg.url, pool_pre_ping=True)

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA", "information_schema"})

    # ── Identifier quoting ────────────────────────────────────────────────

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"

    def fully_qualified_name(self, schema: str, table: str) -> str:
        project = getattr(self.cfg, "project", "") or ""
        if project:
            return f"`{project}`.`{schema}`.`{table}`"
        return f"`{schema}`.`{table}`"

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT "
            f"  COUNTIF({quoted_col} IS NULL) AS null_cnt, "
            f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
            f"  MIN(CAST({quoted_col} AS STRING)) AS min_val, "
            f"  MAX(CAST({quoted_col} AS STRING)) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT CAST({quoted_col} AS STRING) FROM {fqn} "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(
        self, engine: Engine, schema: str, table: str
    ) -> dict[str, int]:
        project = getattr(self.cfg, "project", "") or ""
        dataset = schema
        info_schema = (
            f"`{project}`.`{dataset}`.INFORMATION_SCHEMA.TABLES"
            if project
            else f"`{dataset}`.INFORMATION_SCHEMA.TABLES"
        )
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        f"SELECT row_count FROM {info_schema} "
                        "WHERE table_name = :table"
                    ),
                    {"table": table},
                ).fetchone()
            n = int(row[0] or 0) if row else 0
            return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n}
        except Exception:
            return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": 0}

    def stats_label(self) -> str:
        return "INFORMATION_SCHEMA.TABLES"

    # ── Schema / database comments ────────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        project = getattr(self.cfg, "project", "") or ""
        info_path = (
            f"`{project}`.`{schema}`.INFORMATION_SCHEMA.SCHEMATA"
            if project
            else f"`{schema}`.INFORMATION_SCHEMA.SCHEMATA"
        )
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        f"SELECT option_value "
                        f"FROM {info_path}_OPTIONS "
                        "WHERE option_name = 'description'"
                    )
                ).fetchone()
            return str(row[0]) if row and row[0] else None
        except Exception:
            return None

    def get_database_comment(self, engine: Engine) -> str | None:
        return None

    # ── Incoming foreign keys ─────────────────────────────────────────────
    # BigQuery has informational constraints (not enforced) via
    # INFORMATION_SCHEMA.TABLE_CONSTRAINTS / KEY_COLUMN_USAGE.

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
        project = getattr(self.cfg, "project", "") or ""
        prefix = f"`{project}`." if project else ""
        tc_path = f"{prefix}`{schema}`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
        kcu_path = f"{prefix}`{schema}`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
        ccu_path = f"{prefix}`{schema}`.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE"
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SELECT "
                        f"  kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.COLUMN_NAME, "
                        f"  ccu.COLUMN_NAME "
                        f"FROM {tc_path} tc "
                        f"JOIN {kcu_path} kcu "
                        f"  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
                        f"JOIN {ccu_path} ccu "
                        f"  ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME "
                        "WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY' "
                        "  AND ccu.TABLE_SCHEMA = :schema "
                        "  AND ccu.TABLE_NAME = :table"
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
        except Exception:
            return []

    # ── Comment writing ───────────────────────────────────────────────────

    def set_table_comment_sql(
        self, schema: str, table: str, asset_keyword: str
    ) -> str:
        fqn = self.fully_qualified_name(schema, table)
        return f"ALTER TABLE {fqn} SET OPTIONS(description = :cmt)"

    def set_column_comment_sql(
        self, schema: str, table: str, column: str
    ) -> str:
        fqn = self.fully_qualified_name(schema, table)
        col = self.quote_identifier(column)
        return f"ALTER TABLE {fqn} ALTER COLUMN {col} SET OPTIONS(description = :cmt)"
