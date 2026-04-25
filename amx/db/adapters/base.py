"""Abstract base class for database backend adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from sqlalchemy.engine import Engine


class DatabaseAdapter(ABC):
    """Each backend (PostgreSQL, Snowflake, Databricks, BigQuery) subclasses this."""

    name: str = "base"

    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg

    # ── Connection ────────────────────────────────────────────────────────

    @abstractmethod
    def create_engine(self) -> Engine:
        """Build a SQLAlchemy Engine from the stored config."""
        ...

    def test_connection_sql(self) -> str:
        """Simple SQL to validate connectivity."""
        return "SELECT 1"

    # ── Schema filtering ──────────────────────────────────────────────────

    @abstractmethod
    def system_schemas(self) -> frozenset[str]:
        """Schema names to exclude from user-facing listings."""
        ...

    # ── Materialized views ────────────────────────────────────────────────

    def list_materialized_views(self, engine: Engine, schema: str) -> list[str]:
        """Override when the backend supports materialized views."""
        return []

    # ── Identifier quoting ────────────────────────────────────────────────

    def quote_identifier(self, name: str) -> str:
        """Quote a single identifier for use in raw SQL."""
        return f'"{name}"'

    def fully_qualified_name(self, schema: str, table: str) -> str:
        return f"{self.quote_identifier(schema)}.{self.quote_identifier(table)}"

    # ── Column profiling SQL ──────────────────────────────────────────────

    @abstractmethod
    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        """SQL returning (null_count, distinct_count, min_text, max_text)."""
        ...

    @abstractmethod
    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        """SQL returning up to :lim distinct non-null text samples."""
        ...

    # ── Table-level statistics ────────────────────────────────────────────

    def get_table_stats(
        self, engine: Engine, schema: str, table: str
    ) -> dict[str, int]:
        """Return backend-specific usage stats (seq_scan, idx_scan, n_live_tup, …).

        Keys that don't apply to the backend may be omitted or zero.
        """
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": 0}

    def stats_label(self) -> str:
        """Human-readable label for the stats source (used in LLM prompts)."""
        return "usage statistics"

    # ── Schema / database comments ────────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        return None

    def get_database_comment(self, engine: Engine) -> str | None:
        return None

    # ── Incoming foreign keys ─────────────────────────────────────────────

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
        return []

    # ── Comment writing ───────────────────────────────────────────────────

    @abstractmethod
    def set_table_comment_sql(
        self, schema: str, table: str, asset_keyword: str
    ) -> str:
        """Return a SQL template with a ``:cmt`` bind parameter for the comment text."""
        ...

    @abstractmethod
    def set_column_comment_sql(
        self, schema: str, table: str, column: str
    ) -> str:
        """Return a SQL template with a ``:cmt`` bind parameter for the comment text."""
        ...

    @abstractmethod
    def set_schema_comment_sql(self, schema: str) -> str:
        """Return a SQL template with a ``:cmt`` bind parameter for the comment text."""
        ...

    @abstractmethod
    def set_database_comment_sql(self) -> str:
        """Return a SQL template with a ``:cmt`` bind parameter for the comment text."""
        ...
