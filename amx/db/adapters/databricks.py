"""Databricks (Unity Catalog) backend adapter."""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import urllib3
from sqlalchemy import text
from sqlalchemy.engine import Engine

from amx.codebase.notebook_normalize import normalize_source
from amx.db.adapters._databricks_workspace import DatabricksWorkspaceClient
from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter
from amx.db.adapters.remote_asset_types import (
    RemoteJob,
    RemoteJobRun,
    RemoteJobTask,
    RemoteNotebook,
    RemotePipeline,
    RemoteQuery,
)

log = logging.getLogger(__name__)


class DatabricksAdapter(DatabaseAdapter):
    name = "databricks"
    connect_timeout_seconds = 15
    connect_retry_attempts = 3
    # ``_retry_stop_after_attempts_duration`` is the total budget the
    # databricks-sql-connector spends retrying a single request. It
    # caps connection-establishment retries AND query retries with the
    # same value, so on a slow/serverless warehouse a real-world bulk
    # metadata query (e.g. ``system.information_schema.columns`` over
    # a 1000-table schema) was hitting the previous 20s cap and
    # surfacing as a noisy "Retry request would exceed Retry policy
    # max retry duration" log line. 120s covers transient hiccups
    # without masking a genuinely dead warehouse — the per-attempt
    # socket timeout above (15s) still bites individual round-trips.
    connect_retry_duration_seconds = 120
    capabilities = BackendCapabilities(
        database_comments=True,
        materialized_view_comments=False,
        materialized_views=False,
        relationships=False,
        row_count_stats=True,
        full_scan_when_row_count_unknown=False,
        functions=True,
        volumes=True,  # ★ Unity Catalog volumes — distinctively Databricks
        external_tables=True,
        supports_shared_history=True,
        # Databricks DDL does NOT accept parameter markers like ``:cmt``;
        # the base ``set_schema_comment_sql`` builds a parameterized
        # ``COMMENT ON SCHEMA … IS :cmt`` statement which Databricks
        # rejects with UNEXPECTED_USE_OF_PARAMETER_MARKER. Skip the
        # schema-level comment on this backend — table / column comments
        # still ship through inline ``CREATE TABLE … COMMENT`` clauses.
        schema_comments=False,
        comment_asset_keywords=frozenset({"TABLE", "VIEW"}),
        remote_notebooks=True,
        remote_jobs=True,
        remote_pipelines=True,
        remote_queries=True,
        # Databricks SQL has no SAVEPOINT statement; SQLAlchemy's
        # nested-tx path emits ``SAVEPOINT sa_savepoint_N`` which the
        # server rejects. Flip the capability off so the writeback
        # path uses per-row separate transactions instead.
        supports_savepoints=False,
    )

    def create_history_schema_ddl(self, schema_name: str) -> str:
        # Unity Catalog requires ``catalog.schema``. Fall back to the
        # connection's default catalog (also implicit in DBConfig.url)
        # so the DDL is portable across both Hive and UC workspaces.
        catalog = (getattr(self.cfg, "catalog", "") or "").strip()
        if catalog:
            return (
                f"CREATE SCHEMA IF NOT EXISTS "
                f"{self.quote_identifier(catalog)}.{self.quote_identifier(schema_name)}"
            )
        return f"CREATE SCHEMA IF NOT EXISTS {self.quote_identifier(schema_name)}"

    def create_history_database(self, engine: Engine, name: str) -> None:
        """Create the Unity Catalog catalog hosting the AMX schema.

        Required because without an explicit catalog Databricks lands
        the schema in the workspace default (``workspace`` on UC, hive
        metastore otherwise) — surfacing AMX internal tables to every
        Databricks user instead of keeping them under the team's own
        catalog. Issues ``CREATE CATALOG IF NOT EXISTS`` so an existing
        catalog is left alone.

        Permission: needs Unity Catalog ``CREATE CATALOG`` on the
        metastore. Without it, Databricks returns ``PERMISSION_DENIED``
        which propagates up to the Studio enable endpoint and surfaces
        in the UI as ``schema_bootstrap_warning``.
        """
        from sqlalchemy import text

        sanitized = (name or "").strip()
        if not sanitized:
            return
        ddl = f"CREATE CATALOG IF NOT EXISTS {self.quote_identifier(sanitized)}"
        with engine.begin() as conn:
            conn.execute(text(ddl))

    trusted_ca_env_vars = (
        "AMX_DATABRICKS_TRUSTED_CA_FILE",
        "DATABRICKS_TRUSTED_CA_FILE",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_FILE",
    )

    def _trusted_ca_file(self) -> str:
        raw = str(getattr(self.cfg, "tls_trusted_ca_file", "") or "").strip()
        if not raw:
            for env_name in self.trusted_ca_env_vars:
                raw = os.environ.get(env_name, "").strip()
                if raw:
                    break
        if not raw:
            return ""

        resolved = Path(os.path.expandvars(os.path.expanduser(raw)))
        if not resolved.is_file():
            raise FileNotFoundError(f"Databricks trusted CA bundle file was not found: {resolved}")
        return str(resolved)

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
                "Install the extra: pip install 'amx-cli[databricks]'"
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
        trusted_ca = self._trusted_ca_file()
        if trusted_ca:
            connect_args["_tls_trusted_ca_file"] = trusted_ca
        # Deliberately NOT using pool_pre_ping=True. SQLAlchemy's pre-ping
        # issues a `SELECT 1` on every connection checkout from the pool —
        # cheap on a self-hosted PostgreSQL but on a Databricks SQL warehouse
        # each one keeps the warehouse warm and bills DBUs. A `/run` that
        # checks out 200 connections used to add 200 extra `SELECT 1`s on top
        # of the actual introspection workload. Use pool_recycle instead so
        # SQLAlchemy refreshes connections idle longer than the warehouse's
        # auto-stop window without issuing any keepalive query; if a stale
        # connection slips through, the next real query will trigger a
        # reconnect through SQLAlchemy's native error handling.
        return create_engine(
            self.cfg.url,
            pool_recycle=1800,
            connect_args=connect_args,
        )

    @property
    def _workspace_client(self) -> DatabricksWorkspaceClient:
        """Lazy Workspace API client built from the active profile.

        Tests can override by setting ``self._workspace_client_override`` to
        a mock before calling list_remote_*; if present, that takes precedence.
        """
        override = getattr(self, "_workspace_client_override", None)
        if override is not None:
            return override
        token = getattr(self.cfg, "workspace_token", None) or self.cfg.access_token
        return DatabricksWorkspaceClient(host=self.cfg.host, token=token)

    def test_connection(self, engine: Engine | None = None) -> None:
        try:
            from databricks import sql
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "databricks-sql-connector is required for the Databricks backend. "
                "Install the extra: pip install 'amx-cli[databricks]'"
            ) from exc

        connect_args: dict[str, object] = {
            "_socket_timeout": self.connect_timeout_seconds,
            "_retry_stop_after_attempts_count": self.connect_retry_attempts,
            "_retry_stop_after_attempts_duration": self.connect_retry_duration_seconds,
            "user_agent_entry": "amx",
        }
        if getattr(self.cfg, "tls_no_verify", False):
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            connect_args["_tls_no_verify"] = True
        trusted_ca = self._trusted_ca_file()
        if trusted_ca:
            connect_args["_tls_trusted_ca_file"] = trusted_ca

        with (
            sql.connect(
                server_hostname=self.cfg.host,
                http_path=self.cfg.http_path,
                access_token=self.cfg.access_token or self.cfg.password,
                **connect_args,
            ) as conn,
            conn.cursor() as cursor,
        ):
            cursor.execute("SELECT 1")
            cursor.fetchall()

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
        if "trusted ca bundle file was not found" in msg:
            return (
                "Databricks trusted CA bundle file was not found. Check the profile's "
                "tls_trusted_ca_file path or the AMX_DATABRICKS_TRUSTED_CA_FILE, "
                "DATABRICKS_TRUSTED_CA_FILE, REQUESTS_CA_BUNDLE, or SSL_CERT_FILE environment variable."
            )
        if "not found" in msg or "does not exist" in msg or "table_or_view_not_found" in msg:
            return "Databricks object is missing or not visible in the active catalog/schema."
        if "certificate_verify_failed" in msg or "self-signed certificate" in msg:
            return (
                "TLS certificate validation failed. If your company uses a proxy or private CA, "
                "set a Databricks trusted CA bundle path in the DB profile or, as a last resort, "
                "disable TLS verification for that profile."
            )
        if "invalid access token" in msg or "access token" in msg and "invalid" in msg:
            return (
                "Databricks access token is invalid. Check that the active profile uses a valid "
                "Databricks PAT or supported auth token for this workspace and SQL warehouse."
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
            f"  {self._distinct_count_expr(quoted_col)} AS dist_cnt, "
            f"  MIN(CAST({quoted_col} AS STRING)) AS min_val, "
            f"  MAX(CAST({quoted_col} AS STRING)) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT CAST({quoted_col} AS STRING) FROM {fqn} TABLESAMPLE (1 PERCENT) "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    def _null_count_expr(self, quoted_col: str) -> str:
        # Databricks Spark SQL has no ``FILTER (WHERE …)`` aggregate
        # modifier — use SUM(CASE) like the per-column path.
        return f"SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END)"

    def _distinct_count_expr(self, quoted_col: str) -> str:
        # Databricks Spark SQL exposes ``approx_count_distinct`` (HLL).
        # Same billing logic as the BigQuery / Snowflake overrides — see
        # ``DBConfig.profiling_approximate``. Default unchanged.
        if getattr(self.cfg, "profiling_approximate", False):
            return f"approx_count_distinct({quoted_col})"
        return f"COUNT(DISTINCT {quoted_col})"

    def _aggregate_text_expr(self, agg: str, quoted_col: str) -> str:
        # Databricks uses ``STRING`` rather than ``VARCHAR``.
        return f"{agg}(CAST({quoted_col} AS STRING))"

    def _value_text_expr(self, quoted_col: str) -> str:
        return f"CAST({quoted_col} AS STRING)"

    def _bulk_sample_clause(self) -> str:
        # Spark SQL ``TABLESAMPLE (n PERCENT)``. Mirrors the per-column
        # sample path which already uses ``TABLESAMPLE (1 PERCENT)`` to
        # avoid full-table scans on a billion-row Delta table.
        return "TABLESAMPLE (1 PERCENT)"

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
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

    # ── Catalog hierarchy (Unity Catalog) ─────────────────────────────────

    def supports_catalogs(self) -> bool:
        return True

    def list_catalogs(self, engine: Engine) -> list[str]:
        """Unity Catalog catalogs visible to the active workspace token.

        Runs ``SHOW CATALOGS`` and returns the catalog names. Filters
        out the legacy ``hive_metastore`` and ``samples`` reflections
        only when they're empty / unused — for now we keep them so
        users on hive-metastore-only workspaces can still see their
        databases.
        """
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("SHOW CATALOGS")).fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception:
            return []

    def list_schemas(self, engine: Engine, catalog: str = "") -> list[str] | None:
        """``SHOW SCHEMAS IN <catalog>`` when catalog is set.

        Without an explicit catalog the SQLAlchemy fallback (which
        runs ``SHOW SCHEMAS`` against whatever the connection's USE
        CATALOG default is) returns ambiguous / wrong results on
        Databricks Unity Catalog — that's the bug the v0.10.11
        catalog picker addresses.
        """
        cat = (catalog or "").strip()
        if not cat:
            return None  # let connector fall back to SQLAlchemy inspector
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(f"SHOW SCHEMAS IN `{cat}`")).fetchall()
            system = self.system_schemas()
            return [str(r[0]) for r in rows if r and r[0] and str(r[0]).lower() not in system]
        except Exception:
            return None

    def list_tables(
        self,
        engine: Engine,
        schema: str,
        catalog: str = "",
    ) -> list[str] | None:
        """``SHOW TABLES IN <catalog>.<schema>`` — catalog-aware.

        SQLAlchemy's ``inspect().get_table_names(schema=schema)`` on
        Databricks issues ``SHOW TABLES FROM <schema>`` without
        catalog context, which fails as ``SHOW TABLES FROM None.dev``
        when no USE CATALOG was issued — the v0.10.11 catalog picker
        sets ``cfg.catalog`` but doesn't run USE CATALOG on the
        engine, so this override carries the catalog explicitly.

        When no catalog is configured we return ``[]`` (NOT ``None``)
        so the ``DatabaseConnector`` short-circuits without falling
        through to the SQLAlchemy inspector, which would issue the
        broken ``SHOW TABLES FROM `None`.<schema>`` against the
        warehouse. The empty list is the correct semantic answer for
        "this profile is mis-configured" — pair it with a warning so
        the operator notices.
        """
        cat = (catalog or "").strip()
        sch = (schema or "").strip()
        if not sch:
            return None
        if not cat:
            log.warning(
                "Databricks list_tables called with no catalog for "
                "schema=%s; profile is missing 'catalog'. Returning [] "
                "instead of falling back to SQLAlchemy's None-catalog "
                "SHOW TABLES — configure the profile catalog and retry.",
                sch,
            )
            return []
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(f"SHOW TABLES IN `{cat}`.`{sch}`")).fetchall()
            # SHOW TABLES in Databricks returns at least:
            # (database, tableName, isTemporary, [information])
            # tableName is column index 1.
            return [str(r[1]) for r in rows if r and len(r) >= 2 and r[1]]
        except Exception:
            return None

    def list_views(
        self,
        engine: Engine,
        schema: str,
        catalog: str = "",
    ) -> list[str] | None:
        """``SHOW VIEWS IN <catalog>.<schema>`` — same pattern as list_tables.

        Returns ``[]`` when no catalog is configured (same reasoning as
        ``list_tables``) so we don't fall back to SQLAlchemy's
        ``SHOW VIEWS FROM `None`.<schema>``.
        """
        cat = (catalog or "").strip()
        sch = (schema or "").strip()
        if not sch:
            return None
        if not cat:
            log.warning(
                "Databricks list_views called with no catalog for "
                "schema=%s; profile is missing 'catalog'. Returning [].",
                sch,
            )
            return []
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(f"SHOW VIEWS IN `{cat}`.`{sch}`")).fetchall()
            # SHOW VIEWS rows: (namespace, viewName, isTemporary)
            return [str(r[1]) for r in rows if r and len(r) >= 2 and r[1]]
        except Exception:
            return None

    def list_views_with_definitions(
        self,
        engine: Engine,
        schema: str,
    ) -> list[dict[str, Any]]:
        # Unity Catalog: ``system.information_schema.views`` carries the
        # view definition. Hive-metastore profiles return ``[]`` since the
        # legacy metastore exposes only ``SHOW VIEWS`` (name list).
        if not schema:
            return []
        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text(
                        "SELECT table_name, view_definition "
                        "FROM system.information_schema.views "
                        "WHERE table_schema = :schema "
                        "ORDER BY table_name"
                    ),
                    {"schema": schema},
                ).fetchall()
            except Exception:
                return []
        return [
            {
                "name": str(r[0]),
                "type": "view",
                "definition": str(r[1]) if r[1] else None,
                "comment": None,
                "metadata": {},
            }
            for r in rows
        ]

    # ── Bulk catalog metadata ─────────────────────────────────────────────

    def bulk_catalog_metadata(
        self,
        engine: Engine,
        catalog: str = "",
    ) -> dict[str, str | None] | None:
        """Unity Catalog: every schema in ``catalog`` + its comment in
        one ``system.information_schema.schemata`` query.

        Replaces the sidebar's per-schema ``DESCRIBE SCHEMA`` loop on
        Databricks, which is the single most expensive bit of metadata
        polish when expanding a catalog with many schemas. Legacy Hive
        metastore profiles return ``None`` (no ``system.information_
        schema``) and fall back to per-schema fetch.
        """
        cat = (catalog or getattr(self.cfg, "catalog", "") or "").strip()
        if not cat:
            return None
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT schema_name, comment "
                        "FROM system.information_schema.schemata "
                        "WHERE catalog_name = :cat AND schema_name <> 'information_schema'"
                    ),
                    {"cat": cat},
                ).fetchall()
            return {str(r[0]): (str(r[1]) if r[1] else None) for r in rows}
        except Exception:
            return None

    # ── Bulk schema metadata ──────────────────────────────────────────────

    def bulk_schema_metadata(
        self,
        engine: Engine,
        schema: str,
        *,
        catalog: str = "",
    ) -> dict[str, dict[str, Any]] | None:
        """Bulk fetch via Unity Catalog's ``system.information_schema``.

        Two queries — one for tables/views, one for columns — both
        filtered on ``table_catalog`` + ``table_schema``. This is the
        single biggest perf win in the entire AMX-Databricks story: a
        200-table schema collapses from 200 sequential ``DESCRIBE TABLE
        EXTENDED`` calls to two ``INFORMATION_SCHEMA`` queries (~1s).

        Legacy Hive metastore profiles have no ``system.information_
        schema`` so the query raises and we return ``None`` — the
        connector then falls back to the per-table inspector path that
        Hive callers have always used. No regression for that case.
        """
        cat = (catalog or getattr(self.cfg, "catalog", "") or "").strip()
        if not cat:
            return None
        try:
            out: dict[str, dict[str, Any]] = {}
            with engine.connect() as conn:
                table_rows = conn.execute(
                    text(
                        "SELECT table_name, table_type, comment "
                        "FROM system.information_schema.tables "
                        "WHERE table_catalog = :cat AND table_schema = :schema"
                    ),
                    {"cat": cat, "schema": schema},
                ).fetchall()
                for r in table_rows:
                    raw_kind = str(r[1] or "").upper()
                    if "MATERIALIZED" in raw_kind:
                        kind = "MATERIALIZED VIEW"
                    elif "VIEW" in raw_kind:
                        kind = "VIEW"
                    else:
                        kind = "TABLE"
                    out[str(r[0])] = {
                        "table_comment": str(r[2]) if r[2] else None,
                        "columns": {},
                        "kind": kind,
                    }
                col_rows = conn.execute(
                    text(
                        "SELECT table_name, column_name, comment "
                        "FROM system.information_schema.columns "
                        "WHERE table_catalog = :cat AND table_schema = :schema "
                        "ORDER BY table_name, ordinal_position"
                    ),
                    {"cat": cat, "schema": schema},
                ).fetchall()
            for r in col_rows:
                entry = out.setdefault(
                    str(r[0]),
                    {"table_comment": None, "columns": {}, "kind": "TABLE"},
                )
                entry["columns"][str(r[1])] = str(r[2]) if r[2] else None
            return out or None
        except Exception:
            return None

    # ── Schema / database comments ────────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        catalog = getattr(self.cfg, "catalog", "") or ""
        qualified = f"`{catalog}`.`{schema}`" if catalog else f"`{schema}`"
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(f"DESCRIBE SCHEMA {qualified}")).fetchall()
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
                rows = conn.execute(text(f"DESCRIBE CATALOG `{catalog}`")).fetchall()
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

    # ── Analytics metadata ────────────────────────────────────────────────

    def get_analytics_metadata(self, engine: Engine, schema: str, table: str) -> dict[str, Any]:
        """Databricks analytics metadata via ``DESCRIBE DETAIL`` + ``DESCRIBE TABLE EXTENDED``.

        Pulls partition columns, storage format (delta / parquet / iceberg),
        size in bytes, file count, last modified, table type, and ZORDER
        clustering keys when present. Soft-fails on permission errors —
        the DESCRIBE statements work for any role with USAGE on the
        catalog/schema, but listing files may require additional ACLs.
        """
        out: dict[str, Any] = {}
        warnings: list[str] = []

        with engine.connect() as conn:
            # ── DESCRIBE DETAIL — Delta-style metadata (size, format, partition, clustering) ──
            try:
                fqn = self.fully_qualified_name(schema, table)
                row = conn.execute(text(f"DESCRIBE DETAIL {fqn}")).fetchone()
                if row:
                    rd = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
                    fmt = str(rd.get("format") or "").lower()
                    if fmt:
                        out["storage_format"] = fmt  # delta, parquet, iceberg, csv...
                    if rd.get("sizeInBytes") is not None:
                        out["storage_bytes"] = int(rd["sizeInBytes"])
                    if rd.get("numFiles") is not None:
                        out["storage_files_count"] = int(rd["numFiles"])
                    if rd.get("lastModified"):
                        out["last_modified"] = str(rd["lastModified"])
                    pcols = rd.get("partitionColumns") or []
                    if pcols:
                        out["partition_keys"] = list(pcols)
                        out["partition_strategy"] = "list"  # Databricks partitions are list-style.
                    # ZORDER columns — only present if user ran OPTIMIZE ZORDER BY.
                    zorder = rd.get("clusteringColumns") or []
                    if zorder:
                        out["clustering_keys"] = list(zorder)
            except Exception as exc:
                warnings.append(f"DESCRIBE DETAIL: {exc}")

            # ── Table type from DESCRIBE TABLE EXTENDED Type field ──
            try:
                fqn = self.fully_qualified_name(schema, table)
                rows = conn.execute(text(f"DESCRIBE TABLE EXTENDED {fqn}")).fetchall()
                for r in rows:
                    rd = dict(r._mapping) if hasattr(r, "_mapping") else dict(r)
                    name = str(rd.get("col_name") or "").strip()
                    if name == "Type":
                        raw_type = str(rd.get("data_type") or "").lower()
                        type_map = {
                            "managed": "managed",
                            "external": "external",
                            "view": "view",
                            "materialized_view": "materialized_view",
                        }
                        out["table_type"] = type_map.get(raw_type, raw_type)
                        break
            except Exception as exc:
                warnings.append(f"DESCRIBE TABLE EXTENDED: {exc}")

        if warnings:
            out["warnings"] = warnings
        return out

    # ── Extended object types ─────────────────────────────────────────────

    def _qualify(self, catalog: str, schema: str) -> str:
        # Build a Unity Catalog 3-level reference for SHOW commands.
        cat = catalog or getattr(self.cfg, "catalog", "") or ""
        if cat:
            return f"`{cat}`.`{schema}`"
        return f"`{schema}`"

    def list_functions(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        catalog = getattr(self.cfg, "catalog", "") or ""
        sql = f"SHOW USER FUNCTIONS IN {self._qualify(catalog, schema)}"
        with engine.connect() as conn:
            try:
                rows = conn.execute(text(sql)).fetchall()
            except Exception:
                return []
        out: list[dict[str, Any]] = []
        for row in rows:
            mapping = row._mapping if hasattr(row, "_mapping") else {}
            # SHOW USER FUNCTIONS returns a single-column ``function`` field
            # carrying the fully-qualified name.
            full = mapping.get("function") or (row[0] if len(row) else None)
            if not full:
                continue
            name = str(full).split(".")[-1]
            out.append(
                {
                    "name": name,
                    "type": "function",
                    "definition": None,
                    "comment": None,
                    "metadata": {"qualified_name": str(full)},
                }
            )
        return out

    def list_volumes(self, engine: Engine, catalog: str, schema: str) -> list[dict[str, Any]]:
        # ★ Unity Catalog volumes — managed or external file storage,
        # the headline Databricks-distinctive object type.
        sql = f"SHOW VOLUMES IN {self._qualify(catalog, schema)}"
        with engine.connect() as conn:
            try:
                rows = conn.execute(text(sql)).fetchall()
            except Exception:
                return []
        out: list[dict[str, Any]] = []
        for row in rows:
            mapping = row._mapping if hasattr(row, "_mapping") else {}
            name = mapping.get("volume_name") or mapping.get("name")
            if name is None and len(row):
                name = row[0]
            if name is None:
                continue
            out.append(
                {
                    "name": str(name),
                    "type": str(mapping.get("volume_type") or "volume").lower(),
                    "definition": None,
                    "comment": str(mapping.get("comment")) if mapping.get("comment") else None,
                    "metadata": {
                        k: str(v)
                        for k, v in mapping.items()
                        if k not in ("volume_name", "name", "comment") and v is not None
                    },
                }
            )
        return out

    def list_volumes_bulk(
        self,
        engine: Engine,
        catalog: str,
    ) -> list[dict[str, Any]] | None:
        """One ``system.information_schema.volumes`` query covers every schema.

        The ``/ask`` ``list_volumes`` tool used to issue one
        ``SHOW VOLUMES IN cat.schema`` per schema — 50 schemas =
        50 round-trips. Unity Catalog exposes the same data through
        a queryable view, so we can fetch everything in one go.
        Returns ``None`` on permission denial / older runtimes that
        don't expose this view, and the caller falls back to the
        per-schema loop.
        """
        if not catalog:
            return None
        sql = (
            "SELECT volume_schema, volume_name, volume_type, comment "
            "FROM system.information_schema.volumes "
            "WHERE volume_catalog = :cat"
        )
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(sql), {"cat": catalog}).fetchall()
        except Exception as exc:
            log.debug("list_volumes_bulk failed for %s: %s", catalog, exc)
            return None
        out: list[dict[str, Any]] = []
        for row in rows:
            mapping = row._mapping if hasattr(row, "_mapping") else {}
            sch = mapping.get("volume_schema")
            name = mapping.get("volume_name")
            if not sch or not name:
                continue
            out.append(
                {
                    "schema": str(sch),
                    "name": str(name),
                    "type": str(mapping.get("volume_type") or "volume").lower(),
                    "comment": str(mapping.get("comment")) if mapping.get("comment") else None,
                }
            )
        return out

    def list_assets_bulk(
        self,
        engine: Engine,
        catalog: str,
    ) -> list[tuple[str, str, str]] | None:
        """One ``system.information_schema.tables`` query for the whole catalog.

        ``find_table_by_name`` and similar fuzzy-search tools previously
        called ``list_assets(schema)`` per schema, paying for one
        ``SHOW TABLES`` per schema. This bulk path returns one row per
        asset (table / view / materialized view) across every schema in
        ``catalog`` in a single query. Returns ``None`` when the
        information_schema view isn't accessible — caller falls back.
        """
        if not catalog:
            return None
        # Filter out system_schemas at the SQL level so we don't ship
        # information_schema's own table list back to the tool layer.
        sys_schemas = self.system_schemas()
        sql = (
            "SELECT table_schema, table_name, table_type "
            "FROM system.information_schema.tables "
            "WHERE table_catalog = :cat"
        )
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(sql), {"cat": catalog}).fetchall()
        except Exception as exc:
            log.debug("list_assets_bulk failed for %s: %s", catalog, exc)
            return None
        out: list[tuple[str, str, str]] = []
        for row in rows:
            mapping = row._mapping if hasattr(row, "_mapping") else {}
            sch = str(mapping.get("table_schema") or "")
            name = str(mapping.get("table_name") or "")
            kind = str(mapping.get("table_type") or "").upper()
            if not sch or not name or sch in sys_schemas:
                continue
            out.append((sch, name, kind))
        return out

    def list_external_tables(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # No clean per-schema "show external tables" — we filter the
        # standard SHOW TABLES output by DESCRIBE TABLE EXTENDED Type.
        catalog = getattr(self.cfg, "catalog", "") or ""
        out: list[dict[str, Any]] = []
        try:
            tables = self.list_tables(engine, schema, catalog) or []
        except Exception:
            return []
        for t in tables:
            try:
                fqn = self.fully_qualified_name(schema, t)
                with engine.connect() as conn:
                    rows = conn.execute(text(f"DESCRIBE TABLE EXTENDED {fqn}")).fetchall()
                location = None
                kind = None
                for r in rows:
                    rd = dict(r._mapping) if hasattr(r, "_mapping") else dict(r)
                    name = str(rd.get("col_name") or "").strip()
                    if name == "Type":
                        kind = str(rd.get("data_type") or "").lower()
                    elif name == "Location":
                        location = str(rd.get("data_type") or "")
                if kind == "external":
                    out.append(
                        {
                            "name": t,
                            "type": "external_table",
                            "definition": None,
                            "comment": None,
                            "metadata": {"location": location},
                        }
                    )
            except Exception:
                continue
        return out

    # ── Comment writing ───────────────────────────────────────────────────

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        if asset_keyword not in self.capabilities.comment_asset_keywords:
            raise self.unsupported(f"Comment write-back for {asset_keyword.lower()} assets")
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON {asset_keyword} {fqn} IS :cmt"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        fqn = self.fully_qualified_name(schema, table)
        col = self.quote_identifier(column)
        return f"ALTER TABLE {fqn} ALTER COLUMN {col} COMMENT :cmt"

    def set_multi_column_comments_sql(
        self,
        schema: str,
        table: str,
        comments: list[tuple[str, str]],
    ) -> str | None:
        if not comments:
            return None
        fqn = self.fully_qualified_name(schema, table)
        clauses = [
            f"{self.quote_identifier(column)} COMMENT {self.quote_literal(comment)}"
            for column, comment in comments
        ]
        return f"ALTER TABLE {fqn} ALTER COLUMN " + ", ".join(clauses)

    def set_schema_comment_sql(self, schema: str) -> str:
        catalog = getattr(self.cfg, "catalog", "") or ""
        qualified = f"`{catalog}`.`{schema}`" if catalog else f"`{schema}`"
        return f"COMMENT ON SCHEMA {qualified} IS :cmt"

    def set_database_comment_sql(self) -> str:
        catalog = getattr(self.cfg, "catalog", "") or ""
        if not catalog:
            raise self.unsupported(
                "Database/catalog comment write-back without a Databricks catalog"
            )
        return f"COMMENT ON CATALOG `{catalog}` IS :cmt"

    # ------------------------------------------------------------------
    # Remote executable assets
    # ------------------------------------------------------------------

    @staticmethod
    def _databricks_lang_to_amx(raw: str | None) -> str:
        if not raw:
            return "python"
        return raw.lower()  # PYTHON|SQL|SCALA|R -> python|sql|scala|r

    @staticmethod
    def _count_cells(ipynb_json: str) -> int | None:
        import json

        try:
            return len(json.loads(ipynb_json).get("cells", []))
        except (json.JSONDecodeError, AttributeError):
            return None

    def list_workspace_children(self, engine=None, *, parent_path: str, kind: str):
        """Yield :class:`WorkspaceEntry` rows immediately under ``parent_path``.

        PR-E lazy discover. Behaviour per kind:

        * ``notebook`` — one ``/api/2.0/workspace/list`` call per
          expand. FILE / REPO entries are skipped.
        * ``job`` — flat list at ``parent_path=''`` (Databricks
          jobs have no folder hierarchy). Subfolder requests yield
          nothing.
        * ``pipeline`` — same flat-on-root pattern as jobs.
        """
        from amx.db.adapters.remote_asset_types import WorkspaceEntry

        del engine
        if kind == "job":
            if parent_path:
                return
            for meta in self.list_remote_jobs_metadata():
                yield WorkspaceEntry(
                    kind="job",
                    path=meta.path or meta.external_id,
                    name=meta.name,
                    is_directory=False,
                    external_id=meta.external_id,
                    owner=meta.owner,
                    last_modified=meta.last_modified,
                )
            return
        if kind == "pipeline":
            if parent_path:
                return
            for meta in self.list_remote_pipelines_metadata():
                yield WorkspaceEntry(
                    kind="pipeline",
                    path=meta.path or meta.external_id,
                    name=meta.name,
                    is_directory=False,
                    external_id=meta.external_id,
                    owner=meta.owner,
                    last_modified=meta.last_modified,
                )
            return
        if kind != "notebook":
            return
        for obj in self._workspace_client.list_workspace_objects_immediate(path=parent_path or "/"):
            object_type = obj.get("object_type")
            full_path = obj.get("path") or ""
            name = full_path.rsplit("/", 1)[-1] or full_path
            modified_ms = obj.get("modified_at")
            last_modified = (
                datetime.fromtimestamp(modified_ms / 1000, tz=timezone.utc) if modified_ms else None
            )
            if object_type == "DIRECTORY":
                yield WorkspaceEntry(
                    kind="notebook",
                    path=full_path,
                    name=name,
                    is_directory=True,
                    external_id=None,
                    owner=obj.get("creator_user_name"),
                    last_modified=last_modified,
                )
            elif object_type == "NOTEBOOK":
                yield WorkspaceEntry(
                    kind="notebook",
                    path=full_path,
                    name=name,
                    is_directory=False,
                    external_id=str(obj.get("object_id") or ""),
                    owner=obj.get("creator_user_name"),
                    last_modified=last_modified,
                )

    def list_remote_notebooks_metadata(self, engine=None):
        """Yield :class:`AssetMetadata` for every notebook in the workspace.

        The cheap cousin of :meth:`list_remote_notebooks`. Skips the
        per-notebook ``export_notebook_source`` round-trip so the
        Studio "browse and pick" wizard can populate its table in
        seconds even against a 5,000-notebook workspace. Consumers
        that need the actual source text feed the chosen
        ``external_id`` values back into ``list_remote_notebooks(
        external_id_filter=…)`` once the user has confirmed.
        """
        from amx.db.adapters.remote_asset_types import AssetMetadata

        del engine
        client = self._workspace_client
        for obj in client.list_workspace_objects(path="/"):
            if obj.get("object_type") != "NOTEBOOK":
                continue
            modified_ms = obj.get("modified_at")
            last_modified = (
                datetime.fromtimestamp(modified_ms / 1000, tz=timezone.utc) if modified_ms else None
            )
            yield AssetMetadata(
                kind="notebook",
                external_id=str(obj["object_id"]),
                name=obj["path"].rsplit("/", 1)[-1],
                path=obj["path"],
                owner=obj.get("creator_user_name"),
                last_modified=last_modified,
            )

    def list_remote_jobs_metadata(self, engine=None):
        """Yield :class:`AssetMetadata` for every job (header only)."""
        from amx.db.adapters.remote_asset_types import AssetMetadata

        del engine
        # _workspace_client exposes a job listing that does not fetch
        # ``recent_runs`` per job — the heavy ``runs_per_job`` fan-out
        # only fires inside the full ``list_remote_jobs``.
        for raw in self._workspace_client.list_jobs_headers():
            s = raw.get("settings", {})
            yield AssetMetadata(
                kind="job",
                external_id=str(raw["job_id"]),
                name=s.get("name", f"job_{raw['job_id']}"),
                path="",
                owner=raw.get("creator_user_name"),
                last_modified=None,
            )

    def list_remote_pipelines_metadata(self, engine=None):
        """Yield :class:`AssetMetadata` for every DLT pipeline.

        Uses the thin ``list_pipelines_headers`` cousin so the wizard
        avoids the per-pipeline ``/api/2.0/pipelines/<id>`` round-trip
        — only the picked ids hit ``list_remote_pipelines`` later.
        """
        from amx.db.adapters.remote_asset_types import AssetMetadata

        del engine
        for raw in self._workspace_client.list_pipelines_headers():
            yield AssetMetadata(
                kind="pipeline",
                external_id=str(raw.get("pipeline_id") or ""),
                name=raw.get("name", ""),
                path=raw.get("target") or "",
                owner=raw.get("creator_user_name"),
                last_modified=None,
            )

    def list_remote_notebooks(self, engine=None, *, external_id_filter=None):
        # ``engine`` is accepted for signature uniformity with the ABC and
        # warehouse-backed adapters (Snowflake). Databricks Workspace assets
        # are fetched over REST, not via SQLAlchemy.
        # ``external_id_filter`` (PR-A): when provided, only the listed
        # workspace object ids are exported. Skips the per-notebook
        # export call on everything outside the set, so a "browse +
        # pick 50 of 5,000" round-trip stays cheap.
        del engine
        wanted = set(external_id_filter) if external_id_filter is not None else None
        client = self._workspace_client
        for obj in client.list_workspace_objects(path="/"):
            if obj.get("object_type") != "NOTEBOOK":
                continue
            if wanted is not None and str(obj["object_id"]) not in wanted:
                continue
            try:
                raw_source = client.export_notebook_source(workspace_path=obj["path"])
            except Exception as exc:  # noqa: BLE001 — skip one bad notebook, keep going
                import logging

                logging.getLogger(__name__).warning(
                    "Failed to export Databricks notebook %s: %s", obj["path"], exc
                )
                continue
            language = self._databricks_lang_to_amx(obj.get("language"))
            normalized = normalize_source(
                raw_source, hint="databricks_source", default_language=language
            )
            modified_ms = obj.get("modified_at")
            last_modified = (
                datetime.fromtimestamp(modified_ms / 1000, tz=timezone.utc) if modified_ms else None
            )
            yield RemoteNotebook(
                external_id=str(obj["object_id"]),
                name=obj["path"].rsplit("/", 1)[-1],
                platform="databricks",
                language=language,
                workspace_path=obj["path"],
                qualified_name=None,
                source_text=normalized,
                source_hash=hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
                last_modified_at=last_modified,
                last_modified_by=obj.get("modified_by"),
                owner=obj.get("creator_user_name"),
                cell_count=self._count_cells(normalized),
            )

    def fetch_remote_notebook_source(self, engine=None, external_id: str = "") -> str:
        del engine
        client = self._workspace_client
        path = (
            external_id if external_id.startswith("/") else client.path_for_object_id(external_id)
        )
        raw = client.export_notebook_source(workspace_path=path)
        return normalize_source(raw, hint="databricks_source", default_language="python")

    def list_remote_jobs(self, engine=None, *, runs_per_job: int = 20, external_id_filter=None):
        # ``external_id_filter`` (PR-A): keep the page-by-page listing
        # for cheap discovery, but skip the expensive per-job
        # ``/jobs/get`` + ``/runs/list`` fan-out for ids outside the
        # set so a "pick 50 of 5,000" round-trip stays linear in the
        # selection size.
        del engine
        wanted = set(external_id_filter) if external_id_filter is not None else None
        for raw in self._workspace_client.list_jobs_full(runs_per_job=runs_per_job):
            if wanted is not None and str(raw.get("job_id")) not in wanted:
                continue
            s = raw.get("settings", {})
            schedule = s.get("schedule") or {}
            tasks = tuple(self._map_remote_task(t) for t in s.get("tasks", []))
            runs = tuple(self._map_remote_run(r) for r in raw.get("recent_runs", []))
            yield RemoteJob(
                job_id=raw["job_id"],
                name=s.get("name", f"job_{raw['job_id']}"),
                creator_user_name=raw.get("creator_user_name"),
                schedule_cron=schedule.get("quartz_cron_expression"),
                schedule_timezone=schedule.get("timezone_id"),
                schedule_pause_status=schedule.get("pause_status"),
                max_concurrent_runs=s.get("max_concurrent_runs"),
                email_notifications=s.get("email_notifications") or {},
                tags=s.get("tags") or {},
                tasks=tasks,
                recent_runs=runs,
            )

    @staticmethod
    def _map_remote_task(t: dict) -> RemoteJobTask:
        type_keys = {
            "notebook_task": "notebook_task",
            "python_wheel_task": "python_wheel_task",
            "sql_task": "sql_task",
            "dbt_task": "dbt_task",
            "pipeline_task": "pipeline_task",
            "spark_jar_task": "spark_jar_task",
            "spark_python_task": "spark_python_task",
            "spark_submit_task": "spark_submit_task",
            "run_job_task": "run_job_task",
        }
        task_type = next((v for k, v in type_keys.items() if k in t), "unknown")
        notebook_path = (t.get("notebook_task") or {}).get("notebook_path")
        sql = t.get("sql_task") or {}
        sql_query_id = (sql.get("query") or {}).get("query_id")
        sql_warehouse_id = sql.get("warehouse_id")
        pipeline = t.get("pipeline_task") or {}
        pipeline_id = pipeline.get("pipeline_id")
        depends_on = tuple(d["task_key"] for d in t.get("depends_on", []))
        return RemoteJobTask(
            task_key=t["task_key"],
            task_type=task_type,
            notebook_path=notebook_path,
            sql_query_id=sql_query_id,
            sql_warehouse_id=sql_warehouse_id,
            pipeline_id=pipeline_id,
            depends_on=depends_on,
            raw_definition=t,
        )

    def list_remote_pipelines(self, engine=None, *, external_id_filter=None):
        # ``external_id_filter`` (PR-A): restrict the yielded pipelines
        # to the given pipeline_id set. The header listing is cheap;
        # filtering at the source avoids downstream churn.
        del engine
        wanted = set(external_id_filter) if external_id_filter is not None else None
        for raw in self._workspace_client.list_pipelines():
            if wanted is not None and str(raw.get("pipeline_id") or "") not in wanted:
                continue
            spec = raw.get("spec") or {}
            latest_list = raw.get("latest_updates") or []
            latest = latest_list[0] if latest_list else {}
            creation_ms = latest.get("creation_time")
            creation = (
                datetime.fromtimestamp(creation_ms / 1000, tz=timezone.utc) if creation_ms else None
            )
            yield RemotePipeline(
                pipeline_id=raw["pipeline_id"],
                name=raw.get("name", raw["pipeline_id"]),
                target_schema=spec.get("target"),
                edition=spec.get("edition"),
                continuous=bool(spec.get("continuous", False)),
                photon=bool(spec.get("photon", False)),
                libraries=spec.get("libraries") or [],
                latest_update_state=latest.get("state"),
                latest_update_creation_time=creation,
            )

    @staticmethod
    def _map_remote_run(r: dict) -> RemoteJobRun:
        def _ms_to_dt(ms):
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc) if ms else None

        return RemoteJobRun(
            run_id=r["run_id"],
            state_result=(r.get("state") or {}).get("result_state") or "UNKNOWN",
            start_time=_ms_to_dt(r.get("start_time")) or datetime.now(timezone.utc),
            end_time=_ms_to_dt(r.get("end_time")),
            setup_duration_ms=r.get("setup_duration"),
            execution_duration_ms=r.get("execution_duration"),
        )

    def list_remote_queries(self, engine=None, *, history_days: int = 7, limit: int = 1000):
        del engine
        for sq in self._workspace_client.list_saved_queries():
            text = sq.get("query") or ""
            yield RemoteQuery(
                platform="databricks",
                kind="saved",
                external_id=sq["id"],
                name=sq.get("name"),
                sql_text=text,
                sql_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                warehouse=sq.get("data_source_id"),
                user_name=(sq.get("user") or {}).get("email"),
                executed_at=None,
                duration_ms=None,
            )
        for h in self._workspace_client.list_query_history(history_days=history_days, limit=limit):
            text = h.get("query_text") or ""
            start_ms = h.get("query_start_time_ms")
            executed = (
                datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc) if start_ms else None
            )
            yield RemoteQuery(
                platform="databricks",
                kind="history",
                external_id=h["query_id"],
                name=None,
                sql_text=text,
                sql_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                warehouse=h.get("warehouse_id"),
                user_name=h.get("user_name"),
                executed_at=executed,
                duration_ms=h.get("duration"),
            )
