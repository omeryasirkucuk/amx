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
                        "DB connection failed (attempt %d/%d) — retrying in %.1fs: %s",
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
        """
        try:
            return list(self._adapter.list_catalogs(self.engine))
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

        Used by the runtime database picker for PostgreSQL / Snowflake
        when the profile has ``database=""``. Returns ``[]`` for
        backends that don't expose a multi-database server (Databricks
        catalogs, BigQuery datasets — those use ``list_catalogs``).
        Suppresses adapter exceptions so the picker can degrade
        gracefully.
        """
        try:
            return list(self._adapter.list_databases(self.engine))
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

        for col_info in raw_cols:
            col_name = col_info["name"]
            quoted_col = adapter.quote_identifier(col_name)
            cp = ColumnProfile(
                name=col_name,
                dtype=str(col_info["type"]),
                nullable=col_info.get("nullable", True),
                row_count=profile.row_count,
            )

            if scan_column_stats or scan_samples:
                try:
                    with self.engine.connect() as conn:
                        if scan_column_stats:
                            stats_sql = adapter.column_stats_sql(fqn, quoted_col)
                            col_stats = conn.execute(text(stats_sql)).fetchone()
                            if col_stats:
                                cp.null_count = col_stats[0] or 0
                                cp.distinct_count = col_stats[1] or 0
                                cp.min_val = col_stats[2]
                                cp.max_val = col_stats[3]
                                cp.cardinality_ratio = (
                                    float(cp.distinct_count) / float(cp.row_count)
                                    if cp.row_count > 0
                                    else 0.0
                                )

                        if scan_samples:
                            sample_sql = adapter.column_sample_sql(fqn, quoted_col)
                            samples_row = conn.execute(
                                text(sample_sql), {"lim": effective_sample_size}
                            ).fetchall()
                            cp.samples = [r[0] for r in samples_row]
                except Exception as exc:
                    actionable = adapter.actionable_profile_error(exc) or actionable_error_message(
                        exc, backend=self.backend
                    )
                    if actionable:
                        log.warning(
                            "Skipping profile stats for %s.%s.%s: %s",
                            schema,
                            table,
                            col_name,
                            actionable,
                        )
                    else:
                        log.warning(
                            "Skipping profile stats for %s.%s.%s: %s",
                            schema,
                            table,
                            col_name,
                            exc,
                        )

            cp.existing_comment = col_info.get("comment")
            profile.columns.append(cp)

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
        """Fetch comments for tables connected through FK relationships."""
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

        out: list[dict[str, str]] = []
        for rs, rt in sorted(related):
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
