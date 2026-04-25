"""Databricks (Unity Catalog) backend adapter."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import DatabaseAdapter


class DatabricksAdapter(DatabaseAdapter):
    name = "databricks"

    def create_engine(self) -> Engine:
        try:
            from sqlalchemy import create_engine
        except ImportError as exc:  # pragma: no cover
            raise ImportError("SQLAlchemy is required.") from exc
        try:
            import databricks.sqlalchemy  # noqa: F401 — registers dialect
        except ImportError as exc:
            raise ImportError(
                "databricks-sqlalchemy is required for the Databricks backend. "
                "Reinstall AMX: pip install -U amx"
            ) from exc
        return create_engine(self.cfg.url, pool_pre_ping=True)

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"information_schema", "default"})

    # ── Identifier quoting ────────────────────────────────────────────────

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"

    def fully_qualified_name(self, schema: str, table: str) -> str:
        catalog = getattr(self.cfg, "catalog", "") or ""
        if catalog:
            return f"`{catalog}`.`{schema}`.`{table}`"
        return f"`{schema}`.`{table}`"

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT "
            f"  SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS null_cnt, "
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
        fqn = self.fully_qualified_name(schema, table)
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(f"DESCRIBE DETAIL {fqn}")).fetchall()
            if rows:
                row = rows[0]
                n_rows = int(row._mapping.get("numFiles", 0)) if hasattr(row, "_mapping") else 0
                return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n_rows}
        except Exception:
            pass
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": 0}

    def stats_label(self) -> str:
        return "DESCRIBE DETAIL"

    # ── Schema / database comments ────────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        catalog = getattr(self.cfg, "catalog", "") or ""
        qualified = f"`{catalog}`.`{schema}`" if catalog else f"`{schema}`"
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(f"DESCRIBE SCHEMA {qualified}")
                ).fetchall()
            for r in rows:
                if str(r[0]).lower() == "comment" and r[1]:
                    return str(r[1])
        except Exception:
            pass
        return None

    def get_database_comment(self, engine: Engine) -> str | None:
        catalog = getattr(self.cfg, "catalog", "") or ""
        if not catalog:
            return None
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(f"DESCRIBE CATALOG `{catalog}`")
                ).fetchall()
            for r in rows:
                if str(r[0]).lower() == "comment" and r[1]:
                    return str(r[1])
        except Exception:
            pass
        return None

    # ── Incoming foreign keys ─────────────────────────────────────────────

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
        fqn = self.fully_qualified_name(schema, table)
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT "
                        "  fk_schema, fk_table, fk_columns, pk_columns "
                        f"FROM system.information_schema.table_constraints "
                        "WHERE constraint_type = 'FOREIGN KEY' "
                        "  AND pk_table_schema = :schema "
                        "  AND pk_table_name = :table"
                    ),
                    {"schema": schema, "table": table},
                ).fetchall()
            results: list[dict[str, Any]] = []
            for r in rows:
                fk_cols = str(r[2]).split(",") if r[2] else []
                pk_cols = str(r[3]).split(",") if r[3] else []
                for fc, pc in zip(fk_cols, pk_cols):
                    results.append({
                        "source_schema": str(r[0]),
                        "source_table": str(r[1]),
                        "source_column": fc.strip(),
                        "target_column": pc.strip(),
                    })
            return results
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
        col = self.quote_identifier(column)
        return f"ALTER TABLE {fqn} ALTER COLUMN {col} COMMENT :cmt"

    def set_schema_comment_sql(self, schema: str) -> str:
        catalog = getattr(self.cfg, "catalog", "") or ""
        qualified = f"`{catalog}`.`{schema}`" if catalog else f"`{schema}`"
        return f"COMMENT ON SCHEMA {qualified} IS :cmt"

    def set_database_comment_sql(self) -> str:
        catalog = getattr(self.cfg, "catalog", "") or ""
        if not catalog:
            return "SELECT 1 -- No catalog configured"
        return f"COMMENT ON CATALOG `{catalog}` IS :cmt"
