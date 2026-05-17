"""Databricks (Unity Catalog) backend adapter."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import urllib3
from sqlalchemy import text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter

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
        comment_asset_keywords=frozenset({"TABLE", "VIEW"}),
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
