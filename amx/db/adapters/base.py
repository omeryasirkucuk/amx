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
    # Extended object-type capabilities. Each flag gates a corresponding
    # ``list_<object>()`` call on :class:`DatabaseAdapter`. Defaulting to
    # False keeps existing adapters intact — they opt in by setting the
    # flags True after implementing the matching list method.
    stored_procedures: bool = False
    functions: bool = False
    sequences: bool = False
    triggers: bool = False
    events: bool = False
    packages: bool = False
    synonyms: bool = False
    user_defined_types: bool = False
    dictionaries: bool = False
    macros: bool = False
    volumes: bool = False
    datashares: bool = False
    external_tables: bool = False
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

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
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

    # ── Catalog hierarchy (Unity Catalog / BigQuery projects) ─────────────

    def supports_catalogs(self) -> bool:
        """True for backends with a 3-level catalog → schema → table hierarchy.

        Databricks Unity Catalog and BigQuery (project = catalog) need
        the user to pick a catalog/project BEFORE schemas / tables can
        be listed unambiguously. PostgreSQL and Snowflake either bind
        the catalog at connection time or use database == catalog so
        this is False for them.
        """
        return False

    def list_catalogs(self, engine: Engine) -> list[str]:
        """Catalogs (or projects) visible to the active connection.

        Default returns an empty list. Override on backends where the
        connection can switch between catalogs without reconnecting
        (Unity Catalog Databricks, BigQuery via job-level project).
        """
        return []

    def list_databases(self, engine: Engine) -> list[str]:
        """User-visible databases on this server (2-level backends only).

        Default returns an empty list. Override on backends where the
        same server hosts multiple databases and switching between them
        requires reconnecting with a different ``database`` field
        (PostgreSQL ``pg_database``, Snowflake ``SHOW DATABASES``).
        Used by the runtime database picker in ``cli_support`` so the
        user can choose at ``/run`` / ``/sync`` time when their profile
        has no ``database`` pinned.
        """
        return []

    def list_schemas(self, engine: Engine, catalog: str = "") -> list[str] | None:
        """Backend-specific schema listing.

        Returning ``None`` tells the connector to fall back to the
        SQLAlchemy ``inspect().get_schema_names()`` path. Override on
        backends where the schema list is catalog-scoped (Databricks
        ``SHOW SCHEMAS IN <catalog>``).
        """
        return None

    def list_tables(
        self,
        engine: Engine,
        schema: str,
        catalog: str = "",
    ) -> list[str] | None:
        """Backend-specific table listing.

        Returning ``None`` tells the connector to fall back to the
        SQLAlchemy ``inspect().get_table_names(schema=...)`` path.
        Override on backends where the table list is
        catalog-and-schema-scoped (Databricks
        ``SHOW TABLES IN <catalog>.<schema>``).
        """
        return None

    def list_views(
        self,
        engine: Engine,
        schema: str,
        catalog: str = "",
    ) -> list[str] | None:
        """Backend-specific view listing. ``None`` → SQLAlchemy fallback."""
        return None

    # ── Extended object types ─────────────────────────────────────────────
    #
    # Each ``list_<object>()`` returns an empty list by default. Adapters
    # that support the object type override the method AND set the
    # matching ``BackendCapabilities`` flag to True. The connector layer
    # gates calls on the flag so unsupported backends never run a query.
    #
    # All return shapes are uniform: a list of dicts with the keys
    # ``name`` (required), ``type``, ``definition``, ``comment``, and
    # ``metadata``. Adapters fill what they can — extra keys in
    # ``metadata`` are passed through to downstream search/indexing.

    def list_stored_procedures(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        """Stored procedures in *schema*. Override when the backend supports them."""
        return []

    def list_functions(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        """User-defined functions / UDFs in *schema*."""
        return []

    def list_sequences(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        """Sequences in *schema*. (Most warehouses skip this; OLTP backends use it heavily.)"""
        return []

    def list_triggers(
        self, engine: Engine, schema: str, table: str | None = None
    ) -> list[dict[str, Any]]:
        """Triggers in *schema*. Pass *table* to scope to one table."""
        return []

    def list_events(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        """Scheduled events / tasks / jobs (MySQL events, Snowflake tasks, BigQuery
        scheduled queries, SQL Server Agent jobs).
        """
        return []

    def list_packages(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        """PL/SQL packages — Oracle-specific."""
        return []

    def list_synonyms(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        """Synonyms — Oracle and SQL Server expose these as object aliases."""
        return []

    def list_user_defined_types(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        """Composite / domain / enum types declared in *schema*."""
        return []

    def list_dictionaries(self, engine: Engine, database: str) -> list[dict[str, Any]]:
        """ClickHouse dictionaries — external lookup tables refreshed from a source."""
        return []

    def list_macros(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        """DuckDB macros — parameterized SQL or table-returning functions."""
        return []

    def list_volumes(
        self, engine: Engine, catalog: str, schema: str
    ) -> list[dict[str, Any]]:
        """Unity Catalog volumes (Databricks) or stages (Snowflake) — file-storage assets."""
        return []

    def list_datashares(self, engine: Engine) -> list[dict[str, Any]]:
        """Datashares (Redshift) / shares (Snowflake) / Delta Sharing recipients (Databricks)."""
        return []

    def list_external_tables(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        """External tables — Redshift Spectrum, BigQuery external tables, DuckDB Parquet/S3."""
        return []

    # ── Analytics metadata ────────────────────────────────────────────────

    def get_analytics_metadata(self, engine: Engine, schema: str, table: str) -> dict[str, Any]:
        """Return analytics-aware metadata for a single table.

        Each adapter overrides this with backend-specific queries that
        fill the analytics-relevant fields the user may ask about:

        * ``partition_keys`` / ``partition_strategy`` — for performance
          optimization questions.
        * ``clustering_keys`` — Snowflake / BigQuery / Databricks Delta.
        * ``storage_format`` — native / parquet / delta / iceberg / csv.
        * ``storage_bytes`` / ``storage_files_count`` — size analysis.
        * ``last_modified`` — freshness check.
        * ``table_type`` — managed / external / view / mat-view.
        * ``tags`` / ``pii_columns`` — governance / compliance lookup.
        * ``indexes`` — performance-tuning input.
        * ``warnings`` — list of strings explaining which fields the
          adapter could NOT populate (permission errors, view limits
          etc.) so the search agent can be honest in its answer.

        The default returns an empty dict — backends that can't expose
        analytics metadata at all just inherit this. Returning a dict
        rather than the dataclass keeps the contract loose so adapters
        can fill subsets without coupling to the full schema.

        Returns:
            A dict whose keys match :class:`AnalyticsMetadata` field
            names. Unknown / inapplicable fields are simply omitted.
        """
        return {}

    # ── Comment writing ───────────────────────────────────────────────────

    @abstractmethod
    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        """Return a SQL template for comment text write-back."""
        ...

    @abstractmethod
    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
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
