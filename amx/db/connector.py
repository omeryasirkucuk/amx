"""Database introspection and metadata extraction.

Supports multiple backends via the adapter layer in ``amx.db.adapters``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from dataclasses import fields as dc_fields
from enum import Enum
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

from amx.config import DBConfig
from amx.core.errors import actionable_error_message
from amx.db.adapters.base import BackendCapabilities, UnsupportedDatabaseOperation
from amx.utils.logging import get_logger

log = get_logger("db.connector")


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


class DatabaseConnector:
    """Unified database connector that delegates backend-specific work to adapters."""

    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self._engine: Engine | None = None

        # Auto-install the backend's driver(s) before constructing the
        # adapter so a user with a saved profile (e.g. ``local-postgre``
        # backed by Postgres) on a fresh slim install never sees the
        # raw ``ModuleNotFoundError: No module named 'psycopg2'`` —
        # they see the same one-time pip-progress UX every other
        # feature uses. Idempotent / cached after first hit.
        from amx.db.drivers import ensure_backend_driver

        ensure_backend_driver(cfg.backend)

        from amx.db.adapters import get_adapter

        self._adapter = get_adapter(cfg)

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = self._adapter.create_engine()
            url_tail = self.cfg.url.split("@")[-1] if "@" in self.cfg.url else self.cfg.url
            log.info("Connected via %s to %s", self._adapter.name, url_tail)
        return self._engine

    @property
    def backend(self) -> str:
        return self._adapter.name

    @property
    def capabilities(self) -> BackendCapabilities:
        return getattr(self._adapter, "capabilities", BackendCapabilities())

    def test_connection_result(self) -> ConnectionTestResult:
        """Test the active connection, retrying once on transient failures.

        Mirrors :func:`amx.llm.provider._is_transient_llm_error` — DNS
        glitches, connection resets, and timeouts are retried once with
        a short backoff; auth / permission / missing-DB / SSL-trust
        errors propagate immediately so the user sees the categorised
        actionable message from :class:`amx.core.errors.ErrorMapper`
        without an artificial delay.
        """
        last_exc: Exception | None = None
        for attempt in range(MAX_CONNECTION_RETRIES + 1):
            try:
                self._adapter.test_connection(self._engine)
                return ConnectionTestResult(ok=True)
            except Exception as exc:
                last_exc = exc
                if attempt < MAX_CONNECTION_RETRIES and _is_transient_db_connection_error(exc):
                    wait = CONNECTION_RETRY_BACKOFF_SEC * (2**attempt)
                    log.warning(
                        "DB connection failed (attempt %d/%d)  --  retrying in %.1fs: %s",
                        attempt + 1,
                        MAX_CONNECTION_RETRIES + 1,
                        wait,
                        exc,
                    )
                    time.sleep(wait)
                    continue
                break

        assert last_exc is not None  # the loop must have raised at least once
        actionable = self._adapter.actionable_profile_error(last_exc) or actionable_error_message(
            last_exc, backend=self.backend
        )
        log.error("Connection failed: %s", actionable)
        return ConnectionTestResult(ok=False, message=actionable, exception=last_exc)

    def test_connection(self) -> bool:
        return self.test_connection_result().ok

    # ── Schema / asset listing ────────────────────────────────────────────

    def list_catalogs(self) -> list[str]:
        """Catalogs visible to the active connection.

        Empty list for backends without a 3-level
        catalog/schema/table hierarchy. Used by the manual-edit
        wizard on Databricks Unity Catalog so the user picks a
        catalog before the schema picker fires.

        ``ImportError`` propagates so the missing-driver case (user
        ran ``pip install amx-cli`` without ``[databricks]``) reaches
        the catalog picker as an actionable hint instead of
        masquerading as "empty workspace".
        """
        try:
            return list(self._adapter.list_catalogs(self.engine))
        except ImportError:
            raise
        except Exception:
            return []

    def supports_catalogs(self) -> bool:
        """True when ``list_catalogs`` is meaningful for this adapter."""
        try:
            return bool(self._adapter.supports_catalogs())
        except Exception:
            return False

    def list_databases(self) -> list[str]:
        """User-visible databases on this server (2-level backends only).

        Used by the runtime database picker when the profile has
        ``database=""``. Returns ``[]`` for backends that don't expose a
        multi-database server (Databricks catalogs, BigQuery datasets —
        those use ``list_catalogs``).

        ``ImportError`` propagates so the missing-driver case (a fresh
        ``pip install amx-cli`` without the right extra) reaches the
        runtime database picker as an actionable hint instead of
        masquerading as "no databases visible". Mirrors the symmetric
        behaviour of :meth:`list_catalogs`.
        """
        try:
            return list(self._adapter.list_databases(self.engine))
        except ImportError:
            raise
        except Exception as exc:
            log.debug("list_databases failed: %s", exc)
            return []

    def reconnect(self) -> None:
        """Dispose the active engine so the next ``self.engine`` access
        rebuilds it from the current ``self.cfg``.

        The runtime database picker mutates ``self.cfg.database``
        in-memory; without a reconnect, ``self._engine`` would still be
        bound to the old database and every subsequent listing query
        would target the wrong DB.
        """
        if self._engine is not None:
            try:
                self._engine.dispose()
            except Exception as exc:
                log.debug("engine.dispose() raised during reconnect: %s", exc)
        self._engine = None

    def list_schemas(self) -> list[str]:
        # Adapter-specific override (e.g. Databricks ``SHOW SCHEMAS IN
        # <catalog>``) takes precedence so catalog-scoped backends
        # don't fall through to the SQLAlchemy inspector — which
        # ignores catalog and returns ambiguous results.
        catalog = getattr(self.cfg, "catalog", "") or ""
        try:
            adapter_result = self._adapter.list_schemas(self.engine, catalog)
        except Exception:
            adapter_result = None
        if adapter_result is not None:
            return list(adapter_result)
        insp = inspect(self.engine)
        system = self._adapter.system_schemas()
        return [s for s in insp.get_schema_names() if s not in system]

    def list_tables(self, schema: str) -> list[str]:
        # Adapter override path for catalog-scoped backends (Databricks
        # Unity Catalog ``SHOW TABLES IN <catalog>.<schema>``). When
        # the override returns None we fall back to the SQLAlchemy
        # inspector — same contract as ``list_schemas``.
        catalog = getattr(self.cfg, "catalog", "") or ""
        try:
            adapter_result = self._adapter.list_tables(
                self.engine,
                schema,
                catalog,
            )
        except Exception:
            adapter_result = None
        if adapter_result is not None:
            return list(adapter_result)
        insp = inspect(self.engine)
        return insp.get_table_names(schema=schema)

    def list_views(self, schema: str) -> list[str]:
        catalog = getattr(self.cfg, "catalog", "") or ""
        try:
            adapter_result = self._adapter.list_views(
                self.engine,
                schema,
                catalog,
            )
        except Exception:
            adapter_result = None
        if adapter_result is not None:
            return list(adapter_result)
        insp = inspect(self.engine)
        return insp.get_view_names(schema=schema)

    def list_materialized_views(self, schema: str) -> list[str]:
        if not self.capabilities.materialized_views:
            return []
        return self._adapter.list_materialized_views(self.engine, schema)

    # ── Extended object types ─────────────────────────────────────────────
    #
    # Each accessor is gated by the matching ``BackendCapabilities`` flag
    # so unsupported backends short-circuit without firing a query.
    # Adapter exceptions degrade to ``[]`` with a debug-level log entry,
    # mirroring the ``list_databases`` resilience pattern — a single
    # permission failure should never tank a wider listing operation.

    def _list_extended(
        self,
        flag_name: str,
        method_name: str,
        *args: Any,
    ) -> list[dict[str, Any]]:
        if not getattr(self.capabilities, flag_name, False):
            return []
        try:
            return list(getattr(self._adapter, method_name)(self.engine, *args))
        except Exception as exc:
            log.debug("%s failed: %s", method_name, exc)
            return []

    def list_stored_procedures(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("stored_procedures", "list_stored_procedures", schema)

    def list_functions(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("functions", "list_functions", schema)

    def list_sequences(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("sequences", "list_sequences", schema)

    def list_triggers(self, schema: str, table: str | None = None) -> list[dict[str, Any]]:
        return self._list_extended("triggers", "list_triggers", schema, table)

    def list_events(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("events", "list_events", schema)

    def list_packages(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("packages", "list_packages", schema)

    def list_synonyms(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("synonyms", "list_synonyms", schema)

    def list_user_defined_types(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("user_defined_types", "list_user_defined_types", schema)

    def list_dictionaries(self, database: str | None = None) -> list[dict[str, Any]]:
        # ClickHouse exposes dictionaries by *database* — defaults to the
        # connection's current database when not passed.
        db = database if database is not None else getattr(self.cfg, "database", "") or ""
        return self._list_extended("dictionaries", "list_dictionaries", db)

    def list_macros(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("macros", "list_macros", schema)

    def list_volumes(self, schema: str, catalog: str | None = None) -> list[dict[str, Any]]:
        cat = catalog if catalog is not None else getattr(self.cfg, "catalog", "") or ""
        return self._list_extended("volumes", "list_volumes", cat, schema)

    def list_volumes_bulk(
        self,
        catalog: str | None = None,
    ) -> list[dict[str, Any]] | None:
        """One INFORMATION_SCHEMA query for every volume in the catalog.

        Returns ``None`` when the active backend has no bulk implementation
        (caller falls back to a per-schema loop) or when the
        ``volumes`` capability is off. Callers must handle the
        ``None`` case explicitly.
        """
        if not self.capabilities.volumes:
            return None
        cat = catalog if catalog is not None else getattr(self.cfg, "catalog", "") or ""
        if not cat:
            return None
        try:
            return self._adapter.list_volumes_bulk(self.engine, cat)
        except Exception as exc:
            log.debug("list_volumes_bulk failed for %s: %s", cat, exc)
            return None

    def list_assets_bulk(
        self,
        catalog: str | None = None,
    ) -> list[tuple[str, str, AssetKind]] | None:
        """Bulk asset enumeration across every schema in ``catalog``.

        Returns triples ``(schema, name, AssetKind)`` or ``None`` when the
        active backend has no bulk implementation.
        """
        cat = catalog if catalog is not None else getattr(self.cfg, "catalog", "") or ""
        if not cat:
            return None
        try:
            raw = self._adapter.list_assets_bulk(self.engine, cat)
        except Exception as exc:
            log.debug("list_assets_bulk failed for %s: %s", cat, exc)
            return None
        if raw is None:
            return None
        # Normalise the backend's raw asset-kind string to AssetKind.
        out: list[tuple[str, str, AssetKind]] = []
        for sch, name, raw_kind in raw:
            kind_norm = (raw_kind or "").strip().upper()
            if kind_norm == "VIEW":
                kind = AssetKind.VIEW
            elif kind_norm in {"MATERIALIZED VIEW", "MATERIALIZED_VIEW"}:
                kind = AssetKind.MATERIALIZED_VIEW
            else:
                kind = AssetKind.TABLE
            out.append((sch, name, kind))
        return out

    def list_datashares(self) -> list[dict[str, Any]]:
        # No schema / catalog argument — datashares live at the cluster /
        # account level on every backend that supports them.
        if not self.capabilities.datashares:
            return []
        try:
            return list(self._adapter.list_datashares(self.engine))
        except Exception as exc:
            log.debug("list_datashares failed: %s", exc)
            return []

    def list_external_tables(self, schema: str) -> list[dict[str, Any]]:
        return self._list_extended("external_tables", "list_external_tables", schema)

    def list_assets(self, schema: str) -> list[tuple[str, AssetKind]]:
        """All analyzable assets (tables, views, materialized views) in a schema."""
        assets: list[tuple[str, AssetKind]] = []
        for t in self.list_tables(schema):
            assets.append((t, AssetKind.TABLE))
        for v in self.list_views(schema):
            assets.append((v, AssetKind.VIEW))
        for mv in self.list_materialized_views(schema):
            assets.append((mv, AssetKind.MATERIALIZED_VIEW))
        assets.sort(key=lambda x: x[0])
        return assets

    def list_column_profiles(self, schema: str, table: str) -> list[ColumnProfile]:
        """Return column names/types/nullability without scanning table data."""
        insp = inspect(self.engine)
        return [
            ColumnProfile(
                name=str(c["name"]),
                dtype=str(c["type"]),
                nullable=bool(c.get("nullable", True)),
            )
            for c in insp.get_columns(table, schema=schema)
        ]

    def resolve_asset_kind(self, schema: str, name: str) -> AssetKind:
        """Determine whether *name* is a table, view, or materialized view."""
        tables = set(self.list_tables(schema))
        if name in tables:
            return AssetKind.TABLE
        views = set(self.list_views(schema))
        if name in views:
            return AssetKind.VIEW
        matviews = set(self.list_materialized_views(schema))
        if name in matviews:
            return AssetKind.MATERIALIZED_VIEW
        return AssetKind.TABLE

    # ── Comments (read) ───────────────────────────────────────────────────

    def get_table_comment(self, schema: str, table: str) -> str | None:
        if not self.capabilities.table_comments and not self.capabilities.view_comments:
            return None
        insp = inspect(self.engine)
        try:
            info = insp.get_table_comment(table, schema=schema)
            return info.get("text")
        except Exception:
            return None

    def get_column_comments(self, schema: str, table: str) -> dict[str, str | None]:
        if not self.capabilities.column_comments:
            return {}
        insp = inspect(self.engine)
        cols = insp.get_columns(table, schema=schema)
        return {c["name"]: c.get("comment") for c in cols}

    def column_comments_probe_query(self, schema: str, table: str) -> str:
        return self._adapter.column_comments_probe_query(schema, table)

    def table_metadata_probe_query(self, schema: str, table: str) -> str:
        return self._adapter.table_metadata_probe_query(schema, table)

    def get_table_metadata_snapshot(self, schema: str, table: str) -> dict[str, Any]:
        columns = self.list_column_profiles(schema, table)
        comments = self.get_column_comments(schema, table)
        return {
            "schema": schema,
            "table": table,
            "table_comment": self.get_table_comment(schema, table) or "",
            "columns": [
                {
                    "name": column.name,
                    "dtype": column.dtype,
                    "nullable": column.nullable,
                    "comment": comments.get(column.name) or "",
                }
                for column in columns
            ],
        }

    def get_schema_comment(self, schema: str) -> str | None:
        if not self.capabilities.schema_comments:
            return None
        return self._adapter.get_schema_comment(self.engine, schema)

    def get_database_comment(self) -> str | None:
        if not self.capabilities.database_comments:
            return None
        return self._adapter.get_database_comment(self.engine)

    # ── Profiling ─────────────────────────────────────────────────────────

    def profile_table(
        self,
        schema: str,
        table: str,
        sample_size: int | None = None,
        asset_kind: AssetKind | None = None,
    ) -> TableProfile:
        if asset_kind is None:
            asset_kind = self.resolve_asset_kind(schema, table)
        log.info("Profiling %s.%s (%s) via %s", schema, table, asset_kind.label, self.backend)

        adapter = self._adapter
        fqn = adapter.fully_qualified_name(schema, table)
        mode = str(getattr(self.cfg, "profiling_mode", "full") or "full").lower().strip()
        if mode not in {"full", "sampled", "metadata"}:
            mode = "full"
        max_rows = max(0, int(getattr(self.cfg, "profiling_max_rows", 1_000_000) or 0))
        effective_sample_size = max(
            0,
            int(
                sample_size
                if sample_size is not None
                else getattr(self.cfg, "profiling_sample_size", 5) or 0
            ),
        )
        profile = TableProfile(
            schema=schema,
            name=table,
            asset_kind=asset_kind,
            existing_comment=self.get_table_comment(schema, table),
            schema_comment=self.get_schema_comment(schema),
            database_comment=self.get_database_comment(),
        )

        try:
            stats = adapter.get_table_stats(self.engine, schema, table)
            profile.stats_seq_scan = stats.get("seq_scan", 0)
            profile.stats_idx_scan = stats.get("idx_scan", 0)
            profile.stats_n_live_tup = stats.get("n_live_tup", 0)
        except Exception as exc:
            actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
                exc, backend=self.backend
            )
            msg = f"Profiling failed for {schema}.{table}: {actionable}"
            raise ProfilingError(schema, table, msg) from exc
        estimated_rows = int(profile.stats_n_live_tup or 0)
        full_scan_blocked = bool(max_rows and estimated_rows > max_rows)
        if (
            mode == "full"
            and max_rows
            and estimated_rows <= 0
            and not self.capabilities.full_scan_when_row_count_unknown
        ):
            full_scan_blocked = True

        if mode == "full" and not full_scan_blocked:
            try:
                with self.engine.connect() as conn:
                    row_count = conn.execute(text(f"SELECT COUNT(*) FROM {fqn}")).scalar() or 0
                    profile.row_count = int(row_count or 0)
            except Exception as exc:
                # Demoted from WARNING to DEBUG in v0.10.9: the exact
                # COUNT(*) failure is fully recovered by falling back
                # to the estimated row count, so the user sees no
                # functional regression. The previous WARNING leaked
                # through the live-display panel during /ask answers
                # ("[WARNING] amx.db.connector — Exact row count
                # failed for public.bkpf: ...") which alarmed users
                # despite being a no-op recovery. Operators who want
                # to investigate slow / blocked counts can still get
                # the line via ``AMX_LOG_LEVEL=debug``.
                log.debug(
                    "Exact row count failed for %s.%s; falling back to "
                    "estimated row count (%d). Detail: %s",
                    schema,
                    table,
                    estimated_rows,
                    exc,
                )
                profile.row_count = estimated_rows
        else:
            profile.row_count = estimated_rows

        if mode == "full" and max_rows and profile.row_count > max_rows:
            full_scan_blocked = True
        scan_column_stats = mode == "full" and not full_scan_blocked
        scan_samples = mode in {"full", "sampled"} and effective_sample_size > 0

        insp = inspect(self.engine)

        try:
            pk = insp.get_pk_constraint(table, schema=schema) or {}
            profile.primary_key = list(pk.get("constrained_columns") or [])
        except Exception:
            profile.primary_key = []

        try:
            profile.foreign_keys = list(insp.get_foreign_keys(table, schema=schema) or [])
        except Exception:
            profile.foreign_keys = []

        try:
            profile.unique_constraints = [
                list((u or {}).get("column_names") or [])
                for u in (insp.get_unique_constraints(table, schema=schema) or [])
            ]
        except Exception:
            profile.unique_constraints = []

        try:
            profile.check_constraints = [
                str((c or {}).get("sqltext") or "")
                for c in (insp.get_check_constraints(table, schema=schema) or [])
                if (c or {}).get("sqltext")
            ]
        except Exception:
            profile.check_constraints = []

        profile.referenced_by = self.get_incoming_foreign_keys(schema, table)
        profile.related_comments = self.get_related_table_comments(
            profile.foreign_keys, profile.referenced_by
        )

        try:
            raw_cols = insp.get_columns(table, schema=schema)
        except Exception as exc:
            actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
                exc, backend=self.backend
            )
            msg = f"Profiling failed for {schema}.{table}: {actionable}"
            raise ProfilingError(schema, table, msg) from exc

        # Build the ColumnProfile list first; we need it indexed before
        # the bulk stats query so we can map result-row positions back
        # to columns.
        for col_info in raw_cols:
            col_name = col_info["name"]
            cp = ColumnProfile(
                name=col_name,
                dtype=str(col_info["type"]),
                nullable=col_info.get("nullable", True),
                row_count=profile.row_count,
            )
            cp.existing_comment = col_info.get("comment")
            profile.columns.append(cp)

        if (scan_column_stats or scan_samples) and profile.columns:
            self._collect_column_stats_and_samples(
                schema=schema,
                table=table,
                fqn=fqn,
                adapter=adapter,
                column_profiles=profile.columns,
                row_count=profile.row_count,
                scan_column_stats=scan_column_stats,
                scan_samples=scan_samples,
                effective_sample_size=effective_sample_size,
            )

        # Analytics metadata — best-effort populate of partition /
        # clustering / size / format / freshness / tags. Each adapter
        # ships a backend-specific implementation; the default is an
        # empty dict so old call sites (and adapters that don't need
        # this) keep working unchanged.
        try:
            am = self._adapter.get_analytics_metadata(self.engine, schema, table)
            if am:
                # Whitelisted assignment so unknown keys don't blow up
                # the dataclass when an adapter passes extra fields.
                allowed = {f.name for f in dc_fields(AnalyticsMetadata)}
                for key, value in am.items():
                    if key in allowed:
                        setattr(profile.analytics, key, value)
        except Exception as exc:
            # Analytics metadata is purely additive — never let a
            # failure here prevent the user from seeing the basic
            # profile they asked for.
            log.debug(
                "Analytics metadata fetch failed for %s.%s: %s",
                schema,
                table,
                exc,
            )

        return profile

    def _collect_column_stats_and_samples(
        self,
        *,
        schema: str,
        table: str,
        fqn: str,
        adapter: Any,
        column_profiles: list[ColumnProfile],
        row_count: int,
        scan_column_stats: bool,
        scan_samples: bool,
        effective_sample_size: int,
    ) -> None:
        """Populate stats / samples on ``column_profiles`` in place.

        Stats path uses one bulk query (``column_stats_bulk_sql``) per
        chunk of N columns instead of one query per column — on a
        300-column table this collapses 300 queries to 6 (at the
        default batch size of 50). On a warehouse-billed backend
        (Databricks/Snowflake/BigQuery) every query saved is one fewer
        full-table scan. If the bulk query fails (rare — usually a
        single column with a type the bulk cast can't handle),
        per-column-fallback runs only for the unprofiled columns of
        that batch, so a single bad column never masks the rest of
        the table.

        Sample path is still per-column for now — Phase 2 will collapse
        it the same way (one bounded sample of the table, distill per
        column in Python).
        """
        if scan_column_stats:
            batch_size = max(
                1,
                int(getattr(self.cfg, "profiling_stats_batch_size", 50) or 50),
            )
            self._collect_bulk_stats(
                schema=schema,
                table=table,
                fqn=fqn,
                adapter=adapter,
                column_profiles=column_profiles,
                row_count=row_count,
                batch_size=batch_size,
            )

        if scan_samples and effective_sample_size > 0:
            self._collect_bulk_samples(
                schema=schema,
                table=table,
                fqn=fqn,
                adapter=adapter,
                column_profiles=column_profiles,
                effective_sample_size=effective_sample_size,
            )

    def _collect_bulk_stats(
        self,
        *,
        schema: str,
        table: str,
        fqn: str,
        adapter: Any,
        column_profiles: list[ColumnProfile],
        row_count: int,
        batch_size: int,
    ) -> None:
        """Run bulk stats query in chunks; fall back per-column on failure."""
        for batch_start in range(0, len(column_profiles), batch_size):
            batch = column_profiles[batch_start : batch_start + batch_size]
            quoted_cols = [adapter.quote_identifier(cp.name) for cp in batch]
            try:
                bulk_sql = adapter.column_stats_bulk_sql(fqn, quoted_cols)
                with self.engine.connect() as conn:
                    row = conn.execute(text(bulk_sql)).fetchone()
                if row is None:
                    continue
                for j, cp in enumerate(batch):
                    base = j * 4
                    cp.null_count = row[base] or 0
                    cp.distinct_count = row[base + 1] or 0
                    cp.min_val = row[base + 2]
                    cp.max_val = row[base + 3]
                    cp.cardinality_ratio = (
                        float(cp.distinct_count) / float(row_count) if row_count > 0 else 0.0
                    )
            except Exception as exc:
                # Bulk failed — most likely one column in this batch has
                # a type the cast can't handle. Retry per-column for
                # this batch only; columns that fail individually get
                # logged and skipped (the original behavior).
                actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
                    exc, backend=self.backend
                )
                log.debug(
                    "Bulk column stats failed for %s.%s (batch %d-%d), "
                    "falling back to per-column: %s",
                    schema,
                    table,
                    batch_start,
                    batch_start + len(batch) - 1,
                    actionable or exc,
                )
                self._collect_per_column_stats_fallback(
                    schema=schema,
                    table=table,
                    fqn=fqn,
                    adapter=adapter,
                    batch=batch,
                    row_count=row_count,
                )

    def _collect_per_column_stats_fallback(
        self,
        *,
        schema: str,
        table: str,
        fqn: str,
        adapter: Any,
        batch: list[ColumnProfile],
        row_count: int,
    ) -> None:
        """Original one-query-per-column path. Used only on bulk failure."""
        for cp in batch:
            quoted_col = adapter.quote_identifier(cp.name)
            try:
                stats_sql = adapter.column_stats_sql(fqn, quoted_col)
                with self.engine.connect() as conn:
                    col_stats = conn.execute(text(stats_sql)).fetchone()
                if col_stats:
                    cp.null_count = col_stats[0] or 0
                    cp.distinct_count = col_stats[1] or 0
                    cp.min_val = col_stats[2]
                    cp.max_val = col_stats[3]
                    cp.cardinality_ratio = (
                        float(cp.distinct_count) / float(row_count) if row_count > 0 else 0.0
                    )
            except Exception as exc:
                actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
                    exc, backend=self.backend
                )
                log.warning(
                    "Skipping profile stats for %s.%s.%s: %s",
                    schema,
                    table,
                    cp.name,
                    actionable or exc,
                )

    def _collect_bulk_samples(
        self,
        *,
        schema: str,
        table: str,
        fqn: str,
        adapter: Any,
        column_profiles: list[ColumnProfile],
        effective_sample_size: int,
    ) -> None:
        """One bulk sample query for all columns; escalate per-column only
        for columns that didn't get enough distinct values.

        Wide-table win: a 300-column table at the default sample size
        of 5 distincts/col used to issue 300 separate
        ``SELECT DISTINCT col FROM big_table TABLESAMPLE … LIMIT 5``
        queries. We now issue one ``SELECT col1, col2, …, colN FROM
        big_table TABLESAMPLE … LIMIT row_cap`` and distill per-column
        distincts in Python. row_cap is adaptive — 1000 baseline plus
        50 × column_count so very wide tables get a deeper sample.

        Quality safety net: if a column emerged from the bulk sample
        with fewer than ``min(target, 3)`` distinct values, the
        connector escalates to a per-column query for *that column
        only*. This catches the rare case of a billion-row table
        whose 1000-row TABLESAMPLE happened to land on a near-constant
        slice for some skewed column.
        """
        n_cols = len(column_profiles)
        row_cap = max(1000, 50 * n_cols)
        quoted_cols = [adapter.quote_identifier(cp.name) for cp in column_profiles]

        try:
            bulk_sql = adapter.bulk_sample_sql(fqn, quoted_cols, row_cap)
            with self.engine.connect() as conn:
                rows = conn.execute(text(bulk_sql)).fetchall()
        except Exception as exc:
            # Bulk failed entirely — fall back to per-column for all
            # columns so the user still gets samples.
            actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
                exc, backend=self.backend
            )
            log.debug(
                "Bulk sample failed for %s.%s, falling back to per-column: %s",
                schema,
                table,
                actionable or exc,
            )
            self._collect_per_column_samples(
                schema=schema,
                table=table,
                fqn=fqn,
                adapter=adapter,
                column_profiles=column_profiles,
                effective_sample_size=effective_sample_size,
            )
            return

        # Distill per-column distinct values from the wide row set.
        # ``rows`` may be empty (very small / heavily filtered table);
        # in that case every column ends up needing escalation.
        short_columns: list[ColumnProfile] = []
        threshold = min(effective_sample_size, 3)
        for col_idx, cp in enumerate(column_profiles):
            seen: set[Any] = set()
            samples: list[Any] = []
            for row in rows:
                v = row[col_idx]
                if v is None or v in seen:
                    continue
                seen.add(v)
                samples.append(v)
                if len(samples) >= effective_sample_size:
                    break
            cp.samples = samples
            if len(samples) < threshold:
                short_columns.append(cp)

        if short_columns:
            log.debug(
                "Escalating sample collection for %d/%d columns of %s.%s "
                "(bulk row_cap=%d returned <%d distincts)",
                len(short_columns),
                n_cols,
                schema,
                table,
                row_cap,
                threshold,
            )
            self._collect_per_column_samples(
                schema=schema,
                table=table,
                fqn=fqn,
                adapter=adapter,
                column_profiles=short_columns,
                effective_sample_size=effective_sample_size,
            )

    def _collect_per_column_samples(
        self,
        *,
        schema: str,
        table: str,
        fqn: str,
        adapter: Any,
        column_profiles: list[ColumnProfile],
        effective_sample_size: int,
    ) -> None:
        """Per-column sample fetch. Used as the fallback path when the
        bulk query fails or as escalation for columns that didn't get
        enough distinct values from the bulk sample.
        """
        for cp in column_profiles:
            quoted_col = adapter.quote_identifier(cp.name)
            try:
                sample_sql = adapter.column_sample_sql(fqn, quoted_col)
                with self.engine.connect() as conn:
                    samples_row = conn.execute(
                        text(sample_sql), {"lim": effective_sample_size}
                    ).fetchall()
                cp.samples = [r[0] for r in samples_row]
            except Exception as exc:
                actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
                    exc, backend=self.backend
                )
                log.warning(
                    "Skipping sample collection for %s.%s.%s: %s",
                    schema,
                    table,
                    cp.name,
                    actionable or exc,
                )

    def profile_entities(
        self,
        schema: str,
        table: str,
        sample_size: int | None = None,
        asset_kind: AssetKind | None = None,
    ):
        """Return profiled metadata normalized to Universal Metadata Interface objects."""
        from amx.core.metadata import UniversalMetadataAdapter

        return UniversalMetadataAdapter.from_table_profile(
            self.profile_table(schema, table, sample_size=sample_size, asset_kind=asset_kind)
        )

    # ── Relationships ─────────────────────────────────────────────────────

    def get_incoming_foreign_keys(self, schema: str, table: str) -> list[dict[str, Any]]:
        if not self.capabilities.relationships:
            return []
        try:
            return self._adapter.get_incoming_foreign_keys(self.engine, schema, table)
        except Exception as exc:
            actionable = self._adapter.actionable_profile_error(exc) or actionable_error_message(
                exc, backend=self.backend
            )
            log.warning(
                "Incoming foreign key introspection failed for %s.%s via %s: %s",
                schema,
                table,
                self.backend,
                actionable or exc,
            )
            return []

    def get_related_table_comments(
        self,
        outgoing_fks: list[dict[str, Any]],
        incoming_fks: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        """Fetch comments for tables connected through FK relationships.

        Uses :meth:`amx.db.adapters.base.BaseAdapter.batch_get_table_comments`
        when the active adapter implements the hook (PostgreSQL today)
        so a tabel with N foreign-key neighbours costs one round-trip
        instead of N. Adapters that have not overridden the hook fall
        back to the per-table ``get_table_comment`` path; behaviour is
        unchanged for them.
        """
        related: set[tuple[str, str]] = set()
        for fk in outgoing_fks:
            rs = str(fk.get("referred_schema") or "")
            rt = str(fk.get("referred_table") or "")
            if rs and rt:
                related.add((rs, rt))
        for fk in incoming_fks:
            rs = str(fk.get("source_schema") or "")
            rt = str(fk.get("source_table") or "")
            if rs and rt:
                related.add((rs, rt))

        if not related:
            return []

        ordered_pairs = sorted(related)

        # Try the batch path first. ``batch_get_table_comments`` returns
        # ``None`` on adapters that have not opted in; a dict (possibly
        # empty) means the adapter handled the request and we should
        # not fall back per-table.
        comments_by_pair: dict[tuple[str, str], str | None] | None = None
        try:
            comments_by_pair = self._adapter.batch_get_table_comments(self.engine, ordered_pairs)
        except Exception as exc:
            # A misbehaving batch implementation must not break callers
            # — log and fall back to the historical per-table path.
            log.warning(
                "Adapter %s batch_get_table_comments failed (%s); falling back to per-table fetch.",
                type(self._adapter).__name__,
                exc,
            )
            comments_by_pair = None

        out: list[dict[str, str]] = []
        for rs, rt in ordered_pairs:
            if comments_by_pair is not None:
                cmt = comments_by_pair.get((rs, rt)) or ""
            else:
                cmt = self.get_table_comment(rs, rt) or ""
            out.append({"schema": rs, "table": rt, "comment": cmt})
        return out

    # ── Comments (write) ──────────────────────────────────────────────────

    def _execute_comment_sql(self, conn: Connection, stmt: str, comment: str) -> None:
        final_sql, params = self._adapter.comment_sql_with_params(stmt, comment)
        conn.execute(text(final_sql), params)

    def apply_column_comments_batch(
        self,
        schema: str,
        table: str,
        comments: list[tuple[str, str]],
        *,
        conn: Connection | None = None,
    ) -> bool:
        if not self.capabilities.column_comments:
            raise UnsupportedDatabaseOperation(f"{self.backend} does not support column comments.")
        stmt = self._adapter.set_multi_column_comments_sql(schema, table, comments)
        if not stmt:
            return False
        if conn is None:
            with self.engine.begin() as local_conn:
                local_conn.execute(text(stmt))
        else:
            conn.execute(text(stmt))
        log.info("Set %d column comments on %s.%s", len(comments), schema, table)
        return True

    def apply_comment(
        self,
        *,
        schema: str,
        table: str = "",
        column: str | None = None,
        comment: str,
        asset_kind: AssetKind = AssetKind.TABLE,
        conn: Connection | None = None,
    ) -> None:
        if asset_kind == AssetKind.SCHEMA:
            if not self.capabilities.schema_comments:
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support schema comments."
                )
            stmt = self._adapter.set_schema_comment_sql(schema)
            if conn is None:
                with self.engine.begin() as local_conn:
                    self._execute_comment_sql(local_conn, stmt, comment)
            else:
                self._execute_comment_sql(conn, stmt, comment)
            log.info("Set comment on schema %s", schema)
            return

        if asset_kind == AssetKind.DATABASE:
            if not self.capabilities.database_comments:
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support database comments."
                )
            stmt = self._adapter.set_database_comment_sql()
            if conn is None:
                with self.engine.begin() as local_conn:
                    self._execute_comment_sql(local_conn, stmt, comment)
            else:
                self._execute_comment_sql(conn, stmt, comment)
            log.info("Set comment on database")
            return

        if column is None:
            keyword = asset_kind.comment_keyword
            if keyword not in self.capabilities.comment_asset_keywords:
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support comment write-back for {asset_kind.label} assets."
                )
            if asset_kind == AssetKind.VIEW and not self.capabilities.view_comments:
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support view comments."
                )
            if (
                asset_kind == AssetKind.MATERIALIZED_VIEW
                and not self.capabilities.materialized_view_comments
            ):
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support materialized view comments."
                )
            if asset_kind == AssetKind.TABLE and not self.capabilities.table_comments:
                raise UnsupportedDatabaseOperation(
                    f"{self.backend} does not support table comments."
                )
            stmt = self._adapter.set_table_comment_sql(schema, table, keyword)
            if conn is None:
                with self.engine.begin() as local_conn:
                    self._execute_comment_sql(local_conn, stmt, comment)
            else:
                self._execute_comment_sql(conn, stmt, comment)
            log.info("Set comment on %s.%s (%s)", schema, table, asset_kind.label)
            return

        if not self.capabilities.column_comments:
            raise UnsupportedDatabaseOperation(f"{self.backend} does not support column comments.")
        stmt = self._adapter.set_column_comment_sql(schema, table, column)
        if conn is None:
            with self.engine.begin() as local_conn:
                self._execute_comment_sql(local_conn, stmt, comment)
        else:
            self._execute_comment_sql(conn, stmt, comment)
        log.info("Set comment on %s.%s.%s", schema, table, column)

    def set_table_comment(
        self,
        schema: str,
        table: str,
        comment: str,
        asset_kind: AssetKind = AssetKind.TABLE,
    ) -> None:
        self.apply_comment(schema=schema, table=table, comment=comment, asset_kind=asset_kind)

    def set_column_comment(self, schema: str, table: str, column: str, comment: str) -> None:
        self.apply_comment(schema=schema, table=table, column=column, comment=comment)

    def set_schema_comment(self, schema: str, comment: str) -> None:
        self.apply_comment(schema=schema, comment=comment, asset_kind=AssetKind.SCHEMA)

    def set_database_comment(self, comment: str) -> None:
        self.apply_comment(schema="", comment=comment, asset_kind=AssetKind.DATABASE)

    # ── Adapter metadata ──────────────────────────────────────────────────

    @property
    def stats_label(self) -> str:
        """Human-readable label for the stats source (passed to LLM prompts)."""
        return self._adapter.stats_label()

    # ── Cleanup ───────────────────────────────────────────────────────────

    def close(self) -> None:
        if self._engine:
            self._engine.dispose()
