"""Databricks (Unity Catalog) backend adapter."""

from __future__ import annotations

from typing import Any

import urllib3
from sqlalchemy import text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


class DatabricksAdapter(DatabaseAdapter):
    name = "databricks"
    connect_timeout_seconds = 15
    connect_retry_attempts = 3
    connect_retry_duration_seconds = 20
    capabilities = BackendCapabilities(
        database_comments=True,
        materialized_view_comments=False,
        materialized_views=False,
        relationships=False,
        row_count_stats=True,
        full_scan_when_row_count_unknown=False,
        comment_asset_keywords=frozenset({"TABLE", "VIEW"}),
    )

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
        connect_args: dict[str, object] = {
            "user_agent_entry": "amx",
            "_socket_timeout": self.connect_timeout_seconds,
            "_retry_stop_after_attempts_count": self.connect_retry_attempts,
            "_retry_stop_after_attempts_duration": self.connect_retry_duration_seconds,
        }
        if getattr(self.cfg, "tls_no_verify", False):
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            connect_args["_tls_no_verify"] = True
        trusted_ca = str(getattr(self.cfg, "tls_trusted_ca_file", "") or "").strip()
        if trusted_ca:
            connect_args["_tls_trusted_ca_file"] = trusted_ca
        return create_engine(
            self.cfg.url,
            pool_pre_ping=True,
            connect_args=connect_args,
        )

    def comment_sql_with_params(
        self,
        stmt_template: str,
        comment: str,
    ) -> tuple[str, dict[str, Any]]:
        return stmt_template.replace(":cmt", self.quote_literal(comment)), {}

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"information_schema", "default"})

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "permission denied" in msg or "not authorized" in msg or "privilege" in msg:
            return "Insufficient Databricks privileges. Grant USE CATALOG/SCHEMA and SELECT on the object."
        if "not found" in msg or "does not exist" in msg or "table_or_view_not_found" in msg:
            return "Databricks object is missing or not visible in the active catalog/schema."
        if "certificate_verify_failed" in msg or "self-signed certificate" in msg:
            return (
                "TLS certificate validation failed. If your company uses a proxy or private CA, "
                "set a Databricks trusted CA bundle path in the DB profile or, as a last resort, "
                "disable TLS verification for that profile."
            )
        if "http_path" in msg or "warehouse" in msg:
            return "Databricks SQL warehouse connection is unavailable. Check host, HTTP path, and token."
        return None

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
            f"SELECT DISTINCT CAST({quoted_col} AS STRING) FROM {fqn} TABLESAMPLE (1 PERCENT) "
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
                mapping = row._mapping if hasattr(row, "_mapping") else {}
                n_rows = int(mapping.get("numRows") or mapping.get("rowCount") or 0)
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
        return []

    # ── Comment writing ───────────────────────────────────────────────────

    def set_table_comment_sql(
        self, schema: str, table: str, asset_keyword: str
    ) -> str:
        if asset_keyword not in self.capabilities.comment_asset_keywords:
            raise self.unsupported(f"Comment write-back for {asset_keyword.lower()} assets")
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
            raise self.unsupported("Database/catalog comment write-back without a Databricks catalog")
        return f"COMMENT ON CATALOG `{catalog}` IS :cmt"
