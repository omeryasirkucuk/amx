"""Trino (and Presto) backend adapter.

Trino and Presto share the same wire protocol — this adapter targets
``trino-python-client`` and the ``sqlalchemy-trino`` dialect, which also
accepts a ``presto://`` URL by configuring an alternative entrypoint.
Most lakehouse deployments today are on Trino; Presto users connect
through the same code path.

The adapter follows the same shape as the Databricks one because both
talk to a remote distributed SQL engine over HTTP with bearer-style
auth, support 3-part naming (catalog.schema.table), and have the same
inlined-literal idiosyncrasy on the ``COMMENT`` DDL path.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter

log = logging.getLogger(__name__)


class TrinoAdapter(DatabaseAdapter):
    name = "trino"
    capabilities = BackendCapabilities(
        # Trino has no "database" object above ``catalog`` — catalog
        # descriptions are connector-config, not DDL — so the
        # database-comment surface is off. Schema / table / view /
        # column / materialized-view comments are all live.
        database_comments=False,
        schema_comments=True,
        table_comments=True,
        view_comments=True,
        materialized_view_comments=True,
        column_comments=True,
        materialized_views=True,
        relationships=False,
        row_count_stats=False,
        full_profiling=True,
        sampled_profiling=True,
        full_scan_when_row_count_unknown=False,
        external_tables=True,
        functions=False,
        # Trino is a query engine; row UPDATE for the shared-history
        # ``finish_run`` lifecycle is connector-specific (Iceberg ✓,
        # Hive raw ✗). Leave the shared-history switch off so users
        # don't get a half-broken setup when they pair Trino with a
        # connector that lacks row mutation.
        supports_shared_history=False,
        comment_asset_keywords=frozenset({"TABLE", "VIEW", "MATERIALIZED VIEW"}),
    )

    # ── Engine / connection ───────────────────────────────────────────────

    def create_engine(self) -> Engine:
        try:
            from sqlalchemy import create_engine
        except ImportError as exc:  # pragma: no cover
            raise ImportError("SQLAlchemy is required.") from exc
        try:
            import trino  # noqa: F401 — HTTP transport
            import trino.sqlalchemy  # noqa: F401 — registers the ``trino://`` dialect
        except ImportError as exc:
            raise ImportError(
                "The 'trino' package (with the sqlalchemy submodule) is required for "
                "the Trino backend. Install the AMX extra: pip install 'amx-cli[trino]'"
            ) from exc

        connect_args: dict[str, Any] = {}
        # JWT auth — supported via the trino client's JWTAuthentication
        # transport. When the user supplied a jwt_token we route over
        # JWT; otherwise the URL's ``user:password`` triggers Basic
        # auth at the driver level.
        jwt_token = getattr(self.cfg, "jwt_token", "") or ""
        if jwt_token:
            from trino.auth import JWTAuthentication

            connect_args["auth"] = JWTAuthentication(jwt_token)

        # TLS — when ``verify`` is False, drop cert validation. When a
        # CA path is set, point requests at it. Mirrors the Databricks
        # ``tls_trusted_ca_file`` / ``tls_no_verify`` surface so users
        # behind a corporate proxy with a private root can configure
        # both backends the same way.
        verify = bool(getattr(self.cfg, "verify", True))
        ca_path = self._trusted_ca_file()
        if not verify:
            connect_args["http_scheme"] = "https"
            connect_args["verify"] = False
        elif ca_path:
            connect_args["verify"] = ca_path

        return create_engine(
            self.cfg.url,
            pool_pre_ping=True,
            connect_args=connect_args,
        )

    trusted_ca_env_vars = (
        "AMX_TRINO_TRUSTED_CA_FILE",
        "TRINO_TRUSTED_CA_FILE",
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
            raise FileNotFoundError(f"Trino trusted CA bundle file was not found: {resolved}")
        return str(resolved)

    def system_schemas(self) -> frozenset[str]:
        # Trino exposes ``information_schema`` inside every catalog and
        # the ``system`` catalog holds runtime metadata. The runtime
        # ``$system`` schema (3-part name ``<cat>.information_schema``)
        # is filtered at listing time; this set covers the legacy
        # schema-level filters used by the connector.
        return frozenset({"information_schema", "sys"})

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "access denied" in msg or "not authorized" in msg or "denied" in msg:
            return (
                "Insufficient Trino privileges. Grant SELECT on the table and "
                "USE on the catalog/schema."
            )
        if "catalog" in msg and "not found" in msg:
            return (
                "Trino catalog is not registered on the coordinator. "
                "Check ``etc/catalog/`` on the server, or update the profile catalog."
            )
        if "table not found" in msg or "schema not found" in msg:
            return "Trino object is missing or not visible in the active catalog/schema."
        return None

    # ── Identifier quoting ────────────────────────────────────────────────

    def quote_identifier(self, name: str) -> str:
        # Trino uses ANSI double-quotes; embedded quotes double up.
        return '"' + str(name).replace('"', '""') + '"'

    def fully_qualified_name(self, schema: str, table: str) -> str:
        catalog = (getattr(self.cfg, "catalog", "") or "").strip()
        if catalog:
            return (
                f"{self.quote_identifier(catalog)}."
                f"{self.quote_identifier(schema)}."
                f"{self.quote_identifier(table)}"
            )
        return f"{self.quote_identifier(schema)}.{self.quote_identifier(table)}"

    # ── Comment writeback ─────────────────────────────────────────────────
    #
    # Trino's DB-API exposes named-parameter binding for DML, but
    # named binds inside DDL ``COMMENT ON …`` statements have historic
    # gaps across connectors (hive / iceberg / delta each parse the
    # DDL slightly differently). Inlining the literal — same pattern
    # the Databricks adapter uses for the same reason — sidesteps the
    # variance. ``quote_literal`` escapes single quotes via the
    # ANSI-standard ``''`` doubling.

    def comment_sql_with_params(
        self,
        stmt_template: str,
        comment: str,
    ) -> tuple[str, dict[str, Any]]:
        return stmt_template.replace(":cmt", self.quote_literal(comment)), {}

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        if asset_keyword not in self.capabilities.comment_asset_keywords:
            raise self.unsupported(f"Comment write-back for {asset_keyword.lower()} assets")
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON {asset_keyword} {fqn} IS :cmt"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        fqn = self.fully_qualified_name(schema, table)
        col = self.quote_identifier(column)
        return f"COMMENT ON COLUMN {fqn}.{col} IS :cmt"

    def set_schema_comment_sql(self, schema: str) -> str:
        catalog = (getattr(self.cfg, "catalog", "") or "").strip()
        if catalog:
            qualified = f"{self.quote_identifier(catalog)}.{self.quote_identifier(schema)}"
        else:
            qualified = self.quote_identifier(schema)
        return f"COMMENT ON SCHEMA {qualified} IS :cmt"

    def set_database_comment_sql(self) -> str:
        # Trino has no "database" object above catalog (catalog
        # descriptions live in connector config, not DDL). The
        # capability flag is False, so the connector raises
        # UnsupportedDatabaseOperation before reaching this path —
        # but we still must implement the abstract method, so we
        # raise with the same actionable message.
        raise self.unsupported(
            "Database comment write-back on Trino — catalog descriptions are "
            "configured in ``etc/catalog/<name>.properties`` on the coordinator, not via DDL"
        )

    # ── Profiling SQL ─────────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        # Trino is ANSI on FILTER clauses and CAST AS VARCHAR.
        return (
            f"SELECT "
            f"  {self._null_count_expr(quoted_col)} AS null_cnt, "
            f"  {self._distinct_count_expr(quoted_col)} AS dist_cnt, "
            f"  CAST(MIN({quoted_col}) AS VARCHAR) AS min_val, "
            f"  CAST(MAX({quoted_col}) AS VARCHAR) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        # Trino supports ``TABLESAMPLE BERNOULLI`` and ``SYSTEM``. We
        # prefer BERNOULLI 1 PERCENT for parity with Databricks and
        # because it's connector-agnostic (SYSTEM is best-effort on
        # some connectors).
        return (
            f"SELECT DISTINCT CAST({quoted_col} AS VARCHAR) "
            f"FROM {fqn} TABLESAMPLE BERNOULLI (1) "
            f"WHERE {quoted_col} IS NOT NULL "
            f"LIMIT :lim"
        )

    def _distinct_count_expr(self, quoted_col: str) -> str:
        # Trino has ``approx_distinct`` (HLL). Mirrors the
        # Databricks / BigQuery / Snowflake billing-aware path.
        if getattr(self.cfg, "profiling_approximate", False):
            return f"approx_distinct({quoted_col})"
        return f"COUNT(DISTINCT {quoted_col})"

    def _bulk_sample_clause(self) -> str:
        return "TABLESAMPLE BERNOULLI (1)"

    # ── Catalog hierarchy (Trino is 3-level) ──────────────────────────────

    def supports_catalogs(self) -> bool:
        return True

    def list_catalogs(self, engine: Engine) -> list[str]:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("SHOW CATALOGS")).fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception:
            return []

    def list_schemas(self, engine: Engine, catalog: str = "") -> list[str] | None:
        cat = (catalog or getattr(self.cfg, "catalog", "") or "").strip()
        if not cat:
            return None  # fall back to SQLAlchemy inspector
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(f"SHOW SCHEMAS FROM {self.quote_identifier(cat)}")
                ).fetchall()
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
        cat = (catalog or getattr(self.cfg, "catalog", "") or "").strip()
        sch = (schema or "").strip()
        if not sch:
            return None
        # SHOW TABLES needs the catalog context; if neither the call
        # nor the profile carries one, defer to the SQLAlchemy
        # inspector (it can resolve via the engine's default catalog
        # set by ``USE`` at connect time).
        if not cat:
            return None
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SHOW TABLES FROM {self.quote_identifier(cat)}.{self.quote_identifier(sch)}"
                    )
                ).fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception:
            return None

    def list_views(
        self,
        engine: Engine,
        schema: str,
        catalog: str = "",
    ) -> list[str] | None:
        # Trino exposes views through ``information_schema.views``; the
        # ``SHOW TABLES`` listing already includes views, so for
        # AMX-listing purposes we keep the explicit query here so the
        # connector knows which entries are views vs tables.
        cat = (catalog or getattr(self.cfg, "catalog", "") or "").strip()
        sch = (schema or "").strip()
        if not (cat and sch):
            return None
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SELECT table_name FROM "
                        f"{self.quote_identifier(cat)}.information_schema.views "
                        f"WHERE table_schema = :schema "
                        f"ORDER BY table_name"
                    ),
                    {"schema": sch},
                ).fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception:
            return None

    def list_views_with_definitions(
        self,
        engine: Engine,
        schema: str,
    ) -> list[dict[str, Any]]:
        cat = (getattr(self.cfg, "catalog", "") or "").strip()
        if not (cat and schema):
            return []
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SELECT table_name, view_definition FROM "
                        f"{self.quote_identifier(cat)}.information_schema.views "
                        f"WHERE table_schema = :schema "
                        f"ORDER BY table_name"
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

    # ── Bulk metadata (the cache-perf hot path) ───────────────────────────

    def bulk_catalog_metadata(
        self,
        engine: Engine,
        catalog: str = "",
    ) -> dict[str, str | None] | None:
        """One query → ``{schema_name: schema_comment_or_none}`` for a catalog.

        Trino's ``<catalog>.information_schema.schemata`` exposes the
        schema name; comments live in ``…schemata.comment`` on Trino
        351+. On older clusters the comment column is missing and the
        query fails — we soft-fall to ``None`` so the connector
        re-runs the per-schema ``get_schema_comment`` path without a
        regression.
        """
        cat = (catalog or getattr(self.cfg, "catalog", "") or "").strip()
        if not cat:
            return None
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SELECT schema_name, comment FROM "
                        f"{self.quote_identifier(cat)}.information_schema.schemata "
                        f"WHERE schema_name <> 'information_schema'"
                    )
                ).fetchall()
            return {str(r[0]): (str(r[1]) if r[1] else None) for r in rows}
        except Exception:
            # Try without the comment column on older clusters.
            try:
                with engine.connect() as conn:
                    rows = conn.execute(
                        text(
                            f"SELECT schema_name FROM "
                            f"{self.quote_identifier(cat)}.information_schema.schemata "
                            f"WHERE schema_name <> 'information_schema'"
                        )
                    ).fetchall()
                return {str(r[0]): None for r in rows}
            except Exception:
                return None

    def bulk_schema_metadata(
        self,
        engine: Engine,
        schema: str,
        *,
        catalog: str = "",
    ) -> dict[str, dict[str, Any]] | None:
        """Two queries → comments + columns for every table in *schema*.

        Mirrors the Databricks ``system.information_schema`` bulk path
        — the single biggest cache-fill win on big Trino deployments
        because every per-table ``DESCRIBE`` would otherwise hit the
        underlying connector independently.
        """
        cat = (catalog or getattr(self.cfg, "catalog", "") or "").strip()
        if not cat:
            return None
        try:
            out: dict[str, dict[str, Any]] = {}
            with engine.connect() as conn:
                table_rows = conn.execute(
                    text(
                        f"SELECT table_name, table_type, comment FROM "
                        f"{self.quote_identifier(cat)}.information_schema.tables "
                        f"WHERE table_schema = :schema"
                    ),
                    {"schema": schema},
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
                        f"SELECT table_name, column_name, comment FROM "
                        f"{self.quote_identifier(cat)}.information_schema.columns "
                        f"WHERE table_schema = :schema "
                        f"ORDER BY table_name, ordinal_position"
                    ),
                    {"schema": schema},
                ).fetchall()
            for r in col_rows:
                entry = out.setdefault(
                    str(r[0]),
                    {"table_comment": None, "columns": {}, "kind": "TABLE"},
                )
                entry["columns"][str(r[1])] = str(r[2]) if r[2] else None
            return out or None
        except Exception:
            # Older Trino releases (≤ 350) and some connectors don't
            # expose ``comment`` in information_schema. Retry without
            # the comment columns so the cache still gets a populated
            # ``bulk_filled=True`` entry — comments stay as ``None``
            # until the user picks the table.
            try:
                out = {}
                with engine.connect() as conn:
                    table_rows = conn.execute(
                        text(
                            f"SELECT table_name, table_type FROM "
                            f"{self.quote_identifier(cat)}.information_schema.tables "
                            f"WHERE table_schema = :schema"
                        ),
                        {"schema": schema},
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
                            "table_comment": None,
                            "columns": {},
                            "kind": kind,
                        }
                    col_rows = conn.execute(
                        text(
                            f"SELECT table_name, column_name FROM "
                            f"{self.quote_identifier(cat)}.information_schema.columns "
                            f"WHERE table_schema = :schema "
                            f"ORDER BY table_name, ordinal_position"
                        ),
                        {"schema": schema},
                    ).fetchall()
                for r in col_rows:
                    entry = out.setdefault(
                        str(r[0]),
                        {"table_comment": None, "columns": {}, "kind": "TABLE"},
                    )
                    entry["columns"][str(r[1])] = None
                return out or None
            except Exception:
                return None

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        cat = (getattr(self.cfg, "catalog", "") or "").strip()
        if not cat:
            return None
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        f"SELECT comment FROM "
                        f"{self.quote_identifier(cat)}.information_schema.schemata "
                        f"WHERE schema_name = :schema"
                    ),
                    {"schema": schema},
                ).fetchone()
            if row and row[0]:
                return str(row[0])
        except Exception:
            pass
        return None

    # ── Asset bulk listing (find_table_by_name) ──────────────────────────

    def list_assets_bulk(
        self,
        engine: Engine,
        catalog: str,
    ) -> list[tuple[str, str, str]] | None:
        """Every table / view across every schema in *catalog* in one query."""
        cat = (catalog or getattr(self.cfg, "catalog", "") or "").strip()
        if not cat:
            return None
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SELECT table_schema, table_name, table_type FROM "
                        f"{self.quote_identifier(cat)}.information_schema.tables "
                        f"WHERE table_schema <> 'information_schema'"
                    )
                ).fetchall()
            return [
                (str(r[0]), str(r[1]), str(r[2] or "BASE TABLE"))
                for r in rows
                if r and r[0] and r[1]
            ]
        except Exception:
            return None
