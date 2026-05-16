"""Shared types + retry policy for :class:`DatabaseConnector`.

Extracted from :mod:`amx.db.connector` so the value types and the
transient-error classifier live in one focused module independent
from the big ``DatabaseConnector`` class. ``connector.py`` re-exports
every public name so the ~90 external imports (``from amx.db.connector
import AssetKind, ColumnProfile, TableProfile, AnalyticsMetadata,
ConnectionTestResult, ProfilingError``) keep working unchanged.

The dataclasses describe the connector's data contract:

* :class:`ColumnProfile` — per-column metadata + sample values used by
  the agents to ground description generation.
* :class:`TableProfile` — table-level metadata + columns + analytics
  metadata.
* :class:`AnalyticsMetadata` — backend-specific physical-layout signals
  (partition keys, clustering, storage format, tags, PII flags,
  last-modified time).
* :class:`ConnectionTestResult` — ok / error message pair returned by
  ``DatabaseConnector.test_connection``.

The retry policy is:

* :data:`MAX_CONNECTION_RETRIES` + :data:`CONNECTION_RETRY_BACKOFF_SEC`
  control the inline retry loop inside ``test_connection``.
* :data:`_TRANSIENT_DB_PATTERNS` and :data:`_NON_TRANSIENT_DB_PATTERNS`
  match against the lowered error string.
* :func:`_is_transient_db_connection_error` is the predicate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ProfilingError(RuntimeError):
    """Profiling failed for a specific asset, but the run can continue."""

    def __init__(self, schema: str, table: str, message: str):
        super().__init__(message)
        self.schema = schema
        self.table = table


class AssetKind(Enum):
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    SCHEMA = "schema"
    DATABASE = "database"

    @property
    def label(self) -> str:
        return self.value.replace("_", " ")

    @property
    def comment_keyword(self) -> str:
        """SQL keyword for COMMENT ON <keyword>."""
        return {
            AssetKind.TABLE: "TABLE",
            AssetKind.VIEW: "VIEW",
            AssetKind.MATERIALIZED_VIEW: "MATERIALIZED VIEW",
        }[self]


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    nullable: bool
    row_count: int = 0
    null_count: int = 0
    distinct_count: int = 0
    cardinality_ratio: float = 0.0
    min_val: Any = None
    max_val: Any = None
    samples: list[Any] = field(default_factory=list)
    existing_comment: str | None = None


@dataclass
class AnalyticsMetadata:
    """Per-table metadata that's useful for analytical/warehouse questions.

    Populated by :meth:`AdapterBase.get_analytics_metadata` — each
    backend fills only the fields it can. Empty/zero defaults mean
    "this backend doesn't expose this signal" rather than "the value
    is zero" so the search agent can be honest in its answer ("no
    partition info available for this DB" vs "this table has no
    partitions").

    The fields cover the questions analytical DB users actually ask:
    performance optimization (``partition_keys`` / ``clustering_keys``
    / ``indexes``), data freshness (``last_modified``), storage
    footprint (``storage_bytes`` / ``storage_files_count``), table
    physical layout (``storage_format``, ``table_type``), and
    governance (``tags`` / ``pii_columns``).

    Each backend's ``get_analytics_metadata`` is best-effort: if a
    query fails (permissions, view-not-supported, etc.) the field is
    just left empty. The ``warnings`` list records why so the agent
    can mention "I couldn't read partition metadata; you may need
    SELECT on INFORMATION_SCHEMA".
    """

    partition_keys: list[str] = field(default_factory=list)
    partition_strategy: str = ""  # range | list | hash | bucket | time | none | ""
    clustering_keys: list[str] = field(default_factory=list)
    storage_format: str = ""  # native | parquet | delta | iceberg | csv | json | external | ""
    storage_bytes: int = 0
    storage_files_count: int = 0
    last_modified: str = ""  # ISO 8601 timestamp; empty when unknown
    table_type: str = ""  # managed | external | view | materialized_view | temporary | foreign | ""
    tags: dict[str, str] = field(default_factory=dict)  # tag_name -> value
    pii_columns: list[str] = field(default_factory=list)  # column names flagged as PII
    indexes: list[dict[str, Any]] = field(default_factory=list)  # {name, columns, unique}
    warnings: list[str] = field(default_factory=list)


@dataclass
class TableProfile:
    schema: str
    name: str
    asset_kind: AssetKind = AssetKind.TABLE
    row_count: int = 0
    columns: list[ColumnProfile] = field(default_factory=list)
    existing_comment: str | None = None
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: list[dict[str, Any]] = field(default_factory=list)
    referenced_by: list[dict[str, Any]] = field(default_factory=list)
    unique_constraints: list[list[str]] = field(default_factory=list)
    check_constraints: list[str] = field(default_factory=list)
    stats_seq_scan: int = 0
    stats_idx_scan: int = 0
    stats_n_live_tup: int = 0
    schema_comment: str | None = None
    database_comment: str | None = None
    related_comments: list[dict[str, str]] = field(default_factory=list)
    # Analytical DB metadata — partition / clustering / size / format /
    # freshness / tags. Populated by the active adapter via
    # ``get_analytics_metadata``. Backends fill what they can; the
    # search agent surfaces only the non-empty fields when answering
    # analytics-aware questions.
    analytics: AnalyticsMetadata = field(default_factory=lambda: AnalyticsMetadata())


@dataclass
class ConnectionTestResult:
    ok: bool
    message: str = ""
    exception: Exception | None = None


MAX_CONNECTION_RETRIES = 1


CONNECTION_RETRY_BACKOFF_SEC = 1.5


_TRANSIENT_DB_PATTERNS: tuple[str, ...] = (
    "could not connect",
    "connection refused",
    "connection reset",
    "connection aborted",
    "name or service not known",
    "could not translate host name",
    "could not resolve host",
    "no route to host",
    "network is unreachable",
    "temporary failure in name resolution",
    "broken pipe",
    "getaddrinfo",
    "timed out",
    "timeout",
    "503 service",
    "502 bad gateway",
    "504 gateway",
)


_NON_TRANSIENT_DB_PATTERNS: tuple[str, ...] = (
    "password authentication failed",
    "authentication failed",
    "permission denied",
    "insufficient privilege",
    "401 unauthorized",
    "403 forbidden",
    "invalid token",
    "invalid api key",
    "does not exist",
    "not exist or not authorized",
    "unknown database",
    "no such database",
    "certificate_verify_failed",
    "self-signed certificate",
)


def _is_transient_db_connection_error(exc: BaseException) -> bool:
    """Return True for connection errors worth retrying once.

    Auth, permission, missing-database, and SSL-trust errors are
    explicitly NOT transient — retrying them just delays the
    categorised actionable message the user actually wants to see.
    """
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    msg = str(exc).lower()
    if any(token in msg for token in _NON_TRANSIENT_DB_PATTERNS):
        return False
    return any(token in msg for token in _TRANSIENT_DB_PATTERNS)
