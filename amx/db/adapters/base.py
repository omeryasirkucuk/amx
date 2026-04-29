"""Abstract base class for database backend adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


class UnsupportedDatabaseOperation(NotImplementedError):
    """Raised when a backend cannot perform a requested metadata operation."""


@dataclass(frozen=True)
class BackendCapabilities:
    """Advertised backend behavior used by the connector and CLI flows."""

    database_comments: bool = True
    schema_comments: bool = True
    table_comments: bool = True
    view_comments: bool = True
    materialized_view_comments: bool = False
    column_comments: bool = True
    materialized_views: bool = False
    relationships: bool = False
    row_count_stats: bool = False
    full_profiling: bool = True
    sampled_profiling: bool = True
    full_scan_when_row_count_unknown: bool = True
    comment_asset_keywords: frozenset[str] = field(
        default_factory=lambda: frozenset({"TABLE", "VIEW"})
    )


class DatabaseAdapter(ABC):
    """Each backend (PostgreSQL, Snowflake, Databricks, BigQuery) subclasses this."""

    name: str = "base"
    capabilities = BackendCapabilities()

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

    def test_connection(self, engine: Engine | None = None) -> None:
        """Run a minimal connectivity check for this backend."""
        active_engine = engine or self.create_engine()
        with active_engine.connect() as conn:
            conn.execute(text(self.test_connection_sql()))

    # ── Schema filtering ──────────────────────────────────────────────────

    @abstractmethod
    def system_schemas(self) -> frozenset[str]:
        """Schema names to exclude from user-facing listings."""
        ...

    # ── Materialized views ────────────────────────────────────────────────

    def list_materialized_views(self, engine: Engine, schema: str) -> list[str]:
        """Override when the backend supports materialized views."""
        return []

    def actionable_profile_error(self, exc: Exception) -> str | None:
        """Return backend-specific remediation text for profiling failures."""
        return None

    def unsupported(self, operation: str) -> UnsupportedDatabaseOperation:
        return UnsupportedDatabaseOperation(
            f"{operation} is not supported for the {self.name} backend."
        )

    # ── Identifier quoting ────────────────────────────────────────────────

    def quote_identifier(self, name: str) -> str:
        """Quote a single identifier for use in raw SQL."""
        return f'"{name}"'

    def quote_literal(self, value: str) -> str:
        """Quote a SQL string literal for dialects that do not allow binds in metadata commands."""
        return "'" + str(value).replace("'", "''") + "'"

    def comment_sql_with_params(
        self,
        stmt_template: str,
        comment: str,
    ) -> tuple[str, dict[str, Any]]:
        """Return the final SQL plus execute params for comment DDL."""
        return stmt_template, {"cmt": comment}

    def set_multi_column_comments_sql(
        self,
        schema: str,
        table: str,
        comments: list[tuple[str, str]],
    ) -> str | None:
        """Return a backend-specific SQL statement for updating multiple column comments."""
        return None

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

    def column_comments_probe_query(self, schema: str, table: str) -> str:
        """Return the query or metadata operation used to inspect column comments."""
        return f"SQLAlchemy inspector get_columns(table={table!r}, schema={schema!r})"

    def table_metadata_probe_query(self, schema: str, table: str) -> str:
        """Return the query or metadata operation used to inspect table structure."""
        return f"SQLAlchemy inspector get_columns/get_table_comment(table={table!r}, schema={schema!r})"

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
        """Return a SQL template for comment text write-back."""
        ...

    @abstractmethod
    def set_column_comment_sql(
        self, schema: str, table: str, column: str
    ) -> str:
        """Return a SQL template for comment text write-back."""
        ...

    @abstractmethod
    def set_schema_comment_sql(self, schema: str) -> str:
        """Return a SQL template for comment text write-back."""
        ...

    @abstractmethod
    def set_database_comment_sql(self) -> str:
        """Return a SQL template for comment text write-back."""
        ...
