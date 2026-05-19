"""Hive (HiveServer2) backend adapter.

Targets the HiveServer2 SQL gateway over Thrift via the ``pyhive`` driver
and the bundled ``pyhive.sqlalchemy_hive`` dialect (registered under the
``hive://`` scheme). HiveServer2 is the right surface for AMX because the
adapter contract requires SQL execution (``column_stats_sql``,
``column_sample_sql``) — pure-Metastore-Thrift cannot run queries.

Hive lives in many places; the wizard accepts all of them:

* **Local / dev** — ``apache/hive`` Docker, NOSASL on port 10000.
* **On-premises Hadoop** — HiveServer2 with SASL PLAIN (often LDAP-backed)
  or Kerberos.
* **AWS EMR** — HiveServer2 with PLAIN+LDAP on the master node.
* **Cloudera CDH / CDP** — Kerberos by default. The wizard does not
  collect a Kerberos keytab; power users hand-edit ``config.yml`` and
  the adapter forwards the auth mode without crashing.
* **Standalone Hive Metastore (HMS Thrift only)** — *not* a target.
  Users wanting HMS access without HiveServer2 should configure the
  Trino backend with the ``hive`` connector.
* **Databricks legacy ``hive_metastore`` catalog** — handled by the
  Databricks backend; that workspace is reachable through the
  Databricks SQL warehouse adapter, not this one.

Comment writeback story (partial, per :attr:`BackendCapabilities`):

* **Table / View** — ``ALTER {TABLE|VIEW} <db>.<t> SET TBLPROPERTIES
  ('comment' = '...')``. Works against every Hive 2.x / 3.x release.
* **Database / Schema** — ``ALTER DATABASE <db> SET DBPROPERTIES
  ('comment' = '...')``. Hive treats database == schema.
* **Column comments — INTENTIONALLY DISABLED.** Hive's only path is
  ``ALTER TABLE <db>.<t> CHANGE col col <TYPE> COMMENT '...'`` which
  requires re-declaring the original column type. Complex types
  (``struct``, ``map``, ``array``, ``uniontype``, generics nested
  inside other complex types) round-trip lossily through Hive's
  catalog representation and a wrong type re-emission corrupts the
  table. Capability flag ``column_comments=False`` makes the
  connector raise ``UnsupportedDatabaseOperation`` cleanly upstream
  so the user gets a clear error instead of a silently-broken
  schema.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter

log = logging.getLogger(__name__)


# Auth modes the wizard / PyHive accept. KERBEROS / CUSTOM are not in
# the wizard but the adapter forwards them so a hand-edited config.yml
# works against a Kerberos-only Cloudera CDP cluster.
_HIVE_AUTH_MODES = frozenset({"NOSASL", "NONE", "PLAIN", "LDAP", "KERBEROS", "CUSTOM"})


class HiveAdapter(DatabaseAdapter):
    name = "hive"
    capabilities = BackendCapabilities(
        database_comments=True,
        schema_comments=True,
        table_comments=True,
        view_comments=True,
        materialized_view_comments=False,
        # See module docstring — full column-redefinition would corrupt
        # complex types on round-trip. Capability OFF so the connector
        # raises ``UnsupportedDatabaseOperation`` cleanly.
        column_comments=False,
        materialized_views=False,
        relationships=False,
        row_count_stats=False,
        full_profiling=True,
        sampled_profiling=True,
        full_scan_when_row_count_unknown=False,
        external_tables=True,
        # Hive row-level UPDATE is partition-/transactional-table-only,
        # so it cannot safely host AMX's run-history schema.
        supports_shared_history=False,
        comment_asset_keywords=frozenset({"TABLE", "VIEW"}),
    )

    # ── Engine / connection ───────────────────────────────────────────────

    def create_engine(self) -> Engine:
        try:
            from sqlalchemy import create_engine
        except ImportError as exc:  # pragma: no cover
            raise ImportError("SQLAlchemy is required.") from exc
        try:
            import pyhive.sqlalchemy_hive  # noqa: F401 — registers the ``hive://`` dialect
        except ImportError as exc:
            raise ImportError(
                "The 'pyhive' package is required for the Hive backend. "
                "Install the AMX extra: pip install 'amx-cli[hive]'"
            ) from exc

        auth = (getattr(self.cfg, "auth_mode", "") or "PLAIN").upper()
        if auth not in _HIVE_AUTH_MODES:
            raise ValueError(
                f"Unknown Hive auth mode: {auth!r}. Supported: {sorted(_HIVE_AUTH_MODES)}"
            )
        connect_args: dict[str, Any] = {}
        return create_engine(
            self.cfg.url,
            pool_pre_ping=True,
            connect_args=connect_args,
        )

    def system_schemas(self) -> frozenset[str]:
        # Hive's metastore exposes ``information_schema`` on 3.x and
        # ``sys`` (transaction tables) on workloads with ACID enabled.
        return frozenset({"information_schema", "sys"})

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "permission denied" in msg or "authorization failed" in msg:
            return (
                "Insufficient Hive privileges. Grant SELECT on the table "
                "and ALTER on the database (or rely on Ranger / Sentry / "
                "Lake Formation policies as your stack requires)."
            )
        if "table not found" in msg or "does not exist" in msg or "noviewmatch" in msg:
            return "Hive object is missing or not visible in the active database."
        if "sasl" in msg or "kerberos" in msg or "thrift transport" in msg:
            return (
                "Hive SASL / Kerberos handshake failed. Verify ``auth_mode`` "
                "in the profile matches the HiveServer2 configuration "
                "(check ``hive.server2.authentication`` on the cluster) and "
                "that the local ``pure-sasl`` package is installed."
            )
        return None

    # ── Identifier quoting ────────────────────────────────────────────────

    def quote_identifier(self, name: str) -> str:
        # HiveQL identifier-quoting uses backticks; embedded backticks double up.
        return "`" + str(name).replace("`", "``") + "`"

    # ── Comment writeback ─────────────────────────────────────────────────
    #
    # Hive's Thrift DDL path does not honor named bind parameters inside
    # ALTER … SET TBLPROPERTIES / DBPROPERTIES. Inline the literal — same
    # pattern Databricks + Trino already use — so a comment containing
    # apostrophes ("the customer's address") round-trips correctly via
    # the ANSI ``''`` doubling in ``quote_literal``.

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
        return f"ALTER {asset_keyword} {fqn} SET TBLPROPERTIES ('comment' = :cmt)"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        # Capability flag is False, so this path is gated upstream in
        # ``DatabaseConnector.apply_comment`` and never reached with the
        # current contract. We still must implement the abstract method
        # — raise with the rationale so anyone bypassing the connector
        # layer (preview / dry-run / hand-rolled tooling) gets a clear
        # error instead of generating dangerous DDL.
        raise self.unsupported(
            "Column comment write-back on Hive — disabled by design. Hive's "
            "only path requires re-declaring the original column type, which "
            "is lossy for complex types (struct / map / array). Apply column "
            "comments through your data-definition tooling (dbt, Atlas) "
            "instead, or migrate the table to a backend with native column "
            "comment DDL (Trino / Databricks Unity Catalog / PostgreSQL)."
        )

    def set_schema_comment_sql(self, schema: str) -> str:
        return f"ALTER DATABASE {self.quote_identifier(schema)} SET DBPROPERTIES ('comment' = :cmt)"

    def set_database_comment_sql(self) -> str:
        db = (getattr(self.cfg, "database", "") or "").strip()
        if not db:
            raise self.unsupported(
                "Database comment write-back without a pinned ``database`` in the profile"
            )
        return self.set_schema_comment_sql(db)

    def set_multi_column_comments_sql(
        self,
        schema: str,
        table: str,
        comments: list[tuple[str, str]],
    ) -> str | None:
        # Column comments are disabled (capability flag); no bulk variant.
        return None

    # ── Profiling SQL ─────────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        # HiveQL: no ANSI ``FILTER (WHERE …)`` aggregate clause — use
        # ``SUM(CASE WHEN …)``. String cast goes to ``STRING``.
        return (
            f"SELECT "
            f"  SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS null_cnt, "
            f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
            f"  CAST(MIN({quoted_col}) AS STRING) AS min_val, "
            f"  CAST(MAX({quoted_col}) AS STRING) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        # Hive supports TABLESAMPLE (n PERCENT) since 0.11; on
        # non-bucketed tables the simple LIMIT path is the safer
        # default — Tez / MapReduce both handle it without an extra
        # map stage.
        return (
            f"SELECT DISTINCT CAST({quoted_col} AS STRING) FROM {fqn} "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    def _null_count_expr(self, quoted_col: str) -> str:
        return f"SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END)"

    def _aggregate_text_expr(self, agg: str, quoted_col: str) -> str:
        return f"{agg}(CAST({quoted_col} AS STRING))"

    def _value_text_expr(self, quoted_col: str) -> str:
        return f"CAST({quoted_col} AS STRING)"

    # ── Listings ──────────────────────────────────────────────────────────
    #
    # PyHive's SQLAlchemy inspector is incomplete on some Hive releases
    # (especially 2.x). Explicit ``SHOW`` listings work consistently
    # across the deployment matrix.

    def list_databases(self, engine: Engine) -> list[str]:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("SHOW DATABASES")).fetchall()
            system = self.system_schemas()
            return [str(r[0]) for r in rows if r and r[0] and str(r[0]).lower() not in system]
        except Exception:
            return []

    def list_schemas(self, engine: Engine, catalog: str = "") -> list[str] | None:
        # Hive's "database" IS its "schema". The ``catalog`` parameter is
        # unused — Hive has no catalog object above the database.
        databases = self.list_databases(engine)
        if not databases:
            return None
        return databases

    def list_tables(
        self,
        engine: Engine,
        schema: str,
        catalog: str = "",
    ) -> list[str] | None:
        sch = (schema or "").strip()
        if not sch:
            return None
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(f"SHOW TABLES IN {self.quote_identifier(sch)}")).fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception:
            return None

    def list_views(
        self,
        engine: Engine,
        schema: str,
        catalog: str = "",
    ) -> list[str] | None:
        sch = (schema or "").strip()
        if not sch:
            return None
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(f"SHOW VIEWS IN {self.quote_identifier(sch)}")).fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception:
            # Hive ≤ 2.1 has no ``SHOW VIEWS`` — return None so the
            # connector tries the inspector path.
            return None

    # ── Bulk metadata (the cache-perf hot path) ───────────────────────────

    def bulk_catalog_metadata(
        self,
        engine: Engine,
        catalog: str = "",
    ) -> dict[str, str | None] | None:
        """One query → ``{database_name: description}`` across the cluster.

        Hive 3+ exposes ``information_schema.schemata.comment``; older
        clusters need a per-database ``DESCRIBE DATABASE`` loop.
        """
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT schema_name, comment FROM information_schema.schemata "
                        "WHERE schema_name NOT IN ('information_schema', 'sys')"
                    )
                ).fetchall()
            return {str(r[0]): (str(r[1]) if r[1] else None) for r in rows}
        except Exception:
            pass
        out: dict[str, str | None] = {}
        try:
            for db_name in self.list_databases(engine):
                out[db_name] = self.get_schema_comment(engine, db_name)
            return out or None
        except Exception:
            return None

    def bulk_schema_metadata(
        self,
        engine: Engine,
        schema: str,
        *,
        catalog: str = "",
    ) -> dict[str, dict[str, Any]] | None:
        """Bulk fetch tables + columns + comments for *schema*.

        Hive 3.x ``information_schema.{tables,columns}`` is the fast
        path. Older clusters fall back to per-table ``DESCRIBE
        FORMATTED`` parsing — slower but produces the same dict shape
        so the connector cache stays ``bulk_filled=True``.
        """
        sch = (schema or "").strip()
        if not sch:
            return None
        try:
            out: dict[str, dict[str, Any]] = {}
            with engine.connect() as conn:
                table_rows = conn.execute(
                    text(
                        "SELECT table_name, table_type, comment FROM "
                        "information_schema.tables WHERE table_schema = :schema"
                    ),
                    {"schema": sch},
                ).fetchall()
                for r in table_rows:
                    raw_kind = str(r[1] or "").upper()
                    kind = "VIEW" if "VIEW" in raw_kind else "TABLE"
                    out[str(r[0])] = {
                        "table_comment": str(r[2]) if r[2] else None,
                        "columns": {},
                        "kind": kind,
                    }
                col_rows = conn.execute(
                    text(
                        "SELECT table_name, column_name, comment FROM "
                        "information_schema.columns WHERE table_schema = :schema "
                        "ORDER BY table_name, ordinal_position"
                    ),
                    {"schema": sch},
                ).fetchall()
            for r in col_rows:
                entry = out.setdefault(
                    str(r[0]),
                    {"table_comment": None, "columns": {}, "kind": "TABLE"},
                )
                entry["columns"][str(r[1])] = str(r[2]) if r[2] else None
            if out:
                return out
        except Exception:
            pass
        return self._bulk_schema_metadata_via_describe(engine, sch)

    def _bulk_schema_metadata_via_describe(
        self,
        engine: Engine,
        schema: str,
    ) -> dict[str, dict[str, Any]] | None:
        """Per-table ``DESCRIBE FORMATTED`` fallback for older Hive.

        Slow (N round-trips for N tables) but reliable across every
        HiveServer2 release back to 2.0. Returns the same dict shape
        as the information_schema fast path.
        """
        tables = self.list_tables(engine, schema) or []
        if not tables:
            return None
        views = set(self.list_views(engine, schema) or [])
        out: dict[str, dict[str, Any]] = {}
        with engine.connect() as conn:
            for tbl in tables:
                try:
                    raw_rows = conn.execute(
                        text(
                            f"DESCRIBE FORMATTED "
                            f"{self.quote_identifier(schema)}.{self.quote_identifier(tbl)}"
                        )
                    ).fetchall()
                except Exception:
                    continue
                table_comment, columns = self._parse_describe_formatted(raw_rows)
                out[tbl] = {
                    "table_comment": table_comment,
                    "columns": columns,
                    "kind": "VIEW" if tbl in views else "TABLE",
                }
        return out or None

    @staticmethod
    def _parse_describe_formatted(
        rows: list[Any],
    ) -> tuple[str | None, dict[str, str | None]]:
        """Parse ``DESCRIBE FORMATTED`` output across Hive 2.x / 3.x / 4.x.

        The output is positional and section-headed but the section
        headers themselves shift between releases:

        * Hive 2.x — opens with a literal ``# col_name | data_type
          | comment`` header row, columns follow, blank-row separator,
          then ``# Detailed Table Information`` etc.
        * Hive 3.x / 4.x — drops the ``# col_name`` header entirely;
          column rows are first, blank row, then ``# Detailed Table
          Information``; the table-parameter sub-section header is
          plain ``Table Parameters:`` (no ``#`` prefix).

        The parser switches into "section header" mode whenever it
        encounters a row whose c1 starts with ``#`` OR ends with
        ``:`` (the Hive 3+ convention) AND c3 is empty. Inside the
        ``Table Parameters`` section the rows have an empty c1, the
        key in c2, and the value in c3.

        Returns ``(table_comment_or_None, {column_name: comment_or_None})``.
        """
        columns: dict[str, str | None] = {}
        table_comment: str | None = None
        in_columns = True
        in_table_params = False

        def _looks_like_section_header(c1: str, c3: str) -> bool:
            # Hive 2.x uses ``# col_name`` and ``# Detailed Table …``
            # both prefixed with ``#``. Hive 3+ uses ``Table
            # Parameters:`` and ``# Detailed Table Information`` —
            # mixed. Treat any row whose c1 ends with ``:`` and c3
            # is empty as a section header (e.g. ``Table Parameters:``
            # or ``Storage Information:``), and any row starting with
            # ``#`` also as a header.
            return c1.startswith("#") or (c1.endswith(":") and not c3)

        for r in rows:
            try:
                c1 = (str(r[0]) if r[0] is not None else "").strip()
                c2 = (str(r[1]) if r[1] is not None else "").strip()
                c3 = (str(r[2]) if r[2] is not None else "").strip()
            except Exception:
                continue
            if not c1 and not c2 and not c3:
                in_columns = False
                in_table_params = False
                continue
            if _looks_like_section_header(c1, c3):
                lowered = c1.lower().lstrip("#").strip()
                # Hive 2.x columns-block header — stay in column mode.
                if lowered.startswith("col_name"):
                    in_columns = True
                    in_table_params = False
                    continue
                in_columns = False
                in_table_params = lowered.startswith("table parameters")
                continue
            # Field rows that look like "Database:   amx_smoke" (c1
            # ends with ':' but c2 is non-empty value, c3 empty) are
            # NOT section headers — they're scalar fields inside the
            # detailed-info block. Skip them silently.
            if c1.endswith(":") and c2 and not c3:
                in_columns = False
                continue
            if in_columns:
                if c1:
                    columns[c1] = c3 or None
                continue
            if in_table_params:
                # Table-parameter sub-rows: c2=key, c3=value (c1 empty).
                key = c2.lower()
                if key == "comment" and c3:
                    table_comment = c3
        return table_comment, columns

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(f"DESCRIBE DATABASE EXTENDED {self.quote_identifier(schema)}")
                ).fetchall()
            for r in rows:
                cells = [str(c) if c is not None else "" for c in r]
                for cell in cells:
                    m = re.search(r"comment\s*[:=]\s*(.+)", cell, re.IGNORECASE)
                    if m:
                        candidate = m.group(1).strip().strip("'\"")
                        if candidate:
                            return candidate
                if len(cells) >= 2 and cells[1] and not cells[1].startswith("hdfs://"):
                    return cells[1]
        except Exception:
            pass
        return None

    def get_database_comment(self, engine: Engine) -> str | None:
        db = (getattr(self.cfg, "database", "") or "").strip()
        if not db:
            return None
        return self.get_schema_comment(engine, db)

    # ── Asset bulk listing ────────────────────────────────────────────────

    def list_assets_bulk(
        self,
        engine: Engine,
        catalog: str,
    ) -> list[tuple[str, str, str]] | None:
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT table_schema, table_name, table_type FROM "
                        "information_schema.tables "
                        "WHERE table_schema NOT IN ('information_schema', 'sys')"
                    )
                ).fetchall()
            return [
                (str(r[0]), str(r[1]), str(r[2] or "BASE TABLE"))
                for r in rows
                if r and r[0] and r[1]
            ]
        except Exception:
            return None

    # ── Trusted CA bundle (used by future HiveServer2-behind-TLS-proxy
    #     deployments) ───────────────────────────────────────────────────

    trusted_ca_env_vars = (
        "AMX_HIVE_TRUSTED_CA_FILE",
        "HIVE_TRUSTED_CA_FILE",
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
        return raw
