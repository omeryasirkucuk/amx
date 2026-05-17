"""Amazon Redshift backend adapter.

Driven by ``sqlalchemy-redshift`` over ``redshift_connector``.
PostgreSQL wire-compatible — standard ``COMMENT ON``, the same
``pg_catalog`` shape — but the analytically-interesting metadata
lives in Redshift-specific ``SVV_*`` / ``STV_*`` views:

* Distribution style and sort key (``SVV_TABLE_INFO``) — central to
  Redshift performance tuning. Both surface in ``get_analytics_metadata``.
* Column compression / encoding (``SVV_COLUMNS.compression``).
* Materialized views (``STV_MV_INFO``).
* External tables / Spectrum (``SVV_EXTERNAL_TABLES``,
  ``SVV_EXTERNAL_SCHEMAS``) — querying S3 via the AWS Glue catalog.
* Datashares (``SVV_DATASHARES``) — Redshift's cross-cluster sharing
  primitive, no equivalent on PG.
* Stored procedures + UDFs via ``pg_proc`` (PG-compatible catalogs).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


class RedshiftAdapter(DatabaseAdapter):
    name = "redshift"
    capabilities = BackendCapabilities(
        materialized_view_comments=True,
        materialized_views=True,
        relationships=True,
        row_count_stats=True,
        stored_procedures=True,
        functions=True,
        external_tables=True,
        datashares=True,
        supports_shared_history=True,
        comment_asset_keywords=frozenset({"TABLE", "VIEW", "MATERIALIZED VIEW"}),
    )

    def create_engine(self) -> Engine:
        return create_engine(self.cfg.url, pool_pre_ping=True)

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "password authentication failed" in msg:
            return (
                "Redshift refused the credentials. Check the username/password, "
                "and that the user has CONNECT on the target database."
            )
        if "could not translate host name" in msg or "name or service not known" in msg:
            return (
                "Redshift cluster endpoint did not resolve. Check the host "
                "(should look like `<id>.<region>.redshift.amazonaws.com`) "
                "and your VPC/security group allows the connection."
            )
        if "permission denied" in msg or "must be owner" in msg:
            return (
                "Redshift denied access to a system view. Some `SVV_*` views "
                "require SELECT on system tables — grant SELECT on "
                "PG_CATALOG.* or run profiling as a higher-privileged role."
            )
        if "database" in msg and "does not exist" in msg:
            return (
                "Redshift refused: the `database` field on this profile points "
                "at a database that doesn't exist on the cluster. Edit the "
                "profile or create the database first."
            )
        return None

    def system_schemas(self) -> frozenset[str]:
        return frozenset(
            {"pg_catalog", "pg_toast", "pg_internal", "information_schema", "catalog_history"}
        )

    def list_databases(self, engine: Engine) -> list[str]:
        # Redshift Serverless / RA3 list visible databases via pg_database
        # (PG-compatible) PLUS datashare-attached databases via
        # SVV_REDSHIFT_DATABASES on newer versions.
        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text("SELECT database_name FROM SVV_REDSHIFT_DATABASES ORDER BY database_name")
                ).fetchall()
            except Exception:
                rows = conn.execute(
                    text(
                        "SELECT datname FROM pg_database "
                        "WHERE datistemplate = false ORDER BY datname"
                    )
                ).fetchall()
        return [str(r[0]) for r in rows]

    # ── Column profiling (PG-compatible) ─────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        # Redshift accepts FILTER and ::text casts identically to PG.
        return (
            f"SELECT "
            f"  COUNT(*) FILTER (WHERE {quoted_col} IS NULL) AS null_cnt, "
            f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
            f"  MIN({quoted_col}::text) AS min_val, "
            f"  MAX({quoted_col}::text) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT {quoted_col}::text FROM {fqn} "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    def _aggregate_text_expr(self, agg: str, quoted_col: str) -> str:
        # Redshift mirrors Postgres: ``::text`` is the idiomatic cast.
        return f"{agg}({quoted_col}::text)"

    def _value_text_expr(self, quoted_col: str) -> str:
        return f"{quoted_col}::text"

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
        # SVV_TABLE_INFO.tbl_rows is the cluster-managed estimate.
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT COALESCE(tbl_rows, 0) FROM SVV_TABLE_INFO "
                    'WHERE schema = :schema AND "table" = :table'
                ),
                {"schema": schema, "table": table},
            ).fetchone()
        n = int(row[0]) if row and row[0] is not None else 0
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n}

    def stats_label(self) -> str:
        return "SVV_TABLE_INFO.tbl_rows"

    def list_materialized_views(self, engine: Engine, schema: str) -> list[str]:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT name FROM STV_MV_INFO WHERE schema = :schema ORDER BY name"),
                {"schema": schema},
            ).fetchall()
        return [str(r[0]) for r in rows]

    # ── Bulk catalog metadata ─────────────────────────────────────────────

    def bulk_catalog_metadata(
        self,
        engine: Engine,
        catalog: str = "",
    ) -> dict[str, str | None] | None:
        """Same shape as Postgres' ``bulk_catalog_metadata`` since
        Redshift forks ``pg_namespace`` / ``obj_description``."""
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT n.nspname, "
                        "       obj_description(n.oid, 'pg_namespace') "
                        "FROM pg_namespace n "
                        "WHERE n.nspname NOT LIKE 'pg_%' "
                        "  AND n.nspname <> 'information_schema'"
                    )
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
        """Postgres-compatible ``pg_description`` join — Redshift forks
        from PG 8 and keeps the same catalog views. Tables that live in
        external schemas (Spectrum, federated) don't appear in
        ``pg_class`` so they're transparently skipped here; ``list_
        external_tables`` handles them separately.
        """
        sql = (
            "SELECT c.relname AS table_name, "
            "       c.relkind AS relkind, "
            "       obj_description(c.oid, 'pg_class') AS table_comment, "
            "       a.attname AS column_name, "
            "       col_description(c.oid, a.attnum) AS column_comment "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "LEFT JOIN pg_attribute a ON a.attrelid = c.oid "
            "  AND a.attnum > 0 AND NOT a.attisdropped "
            "WHERE n.nspname = :schema "
            "  AND c.relkind IN ('r', 'v', 'm') "
            "ORDER BY c.relname, a.attnum"
        )
        try:
            out: dict[str, dict[str, Any]] = {}
            with engine.connect() as conn:
                rows = conn.execute(text(sql), {"schema": schema}).fetchall()
            for r in rows:
                tname = str(r[0])
                kind = {"v": "VIEW", "m": "MATERIALIZED VIEW"}.get(str(r[1]), "TABLE")
                entry = out.setdefault(
                    tname,
                    {"table_comment": r[2], "columns": {}, "kind": kind},
                )
                entry["table_comment"] = r[2]
                entry["kind"] = kind
                if r[3] is not None:
                    entry["columns"][str(r[3])] = r[4]
            return out or None
        except Exception:
            return None

    # ── Schema comments (PG-compatible) ──────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT obj_description(n.oid, 'pg_namespace') "
                    "FROM pg_namespace n WHERE n.nspname = :schema"
                ),
                {"schema": schema},
            ).fetchone()
        return str(row[0]) if row and row[0] else None

    def get_database_comment(self, engine: Engine) -> str | None:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT shobj_description(d.oid, 'pg_database') "
                    "FROM pg_database d WHERE d.datname = current_database()"
                )
            ).fetchone()
        return str(row[0]) if row and row[0] else None

    # ── Incoming foreign keys (informational on Redshift) ────────────────

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
        # Redshift accepts FK declarations but doesn't enforce them.
        # The PG metadata catalogs still carry the relationships, so the
        # PG query shape works.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        src_ns.nspname  AS source_schema,
                        src.relname     AS source_table,
                        src_col.attname AS source_column,
                        tgt_col.attname AS target_column
                    FROM pg_constraint con
                    JOIN pg_class src ON src.oid = con.conrelid
                    JOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace
                    JOIN pg_class tgt ON tgt.oid = con.confrelid
                    JOIN pg_namespace tgt_ns ON tgt_ns.oid = tgt.relnamespace
                    JOIN pg_attribute src_col
                         ON src_col.attrelid = src.oid AND src_col.attnum = ANY(con.conkey)
                    JOIN pg_attribute tgt_col
                         ON tgt_col.attrelid = tgt.oid AND tgt_col.attnum = ANY(con.confkey)
                    WHERE con.contype = 'f'
                      AND tgt_ns.nspname = :schema
                      AND tgt.relname    = :table
                    ORDER BY src_ns.nspname, src.relname, src_col.attname
                    """
                ),
                {"schema": schema, "table": table},
            ).fetchall()
        return [
            {
                "source_schema": str(r[0]),
                "source_table": str(r[1]),
                "source_column": str(r[2]),
                "target_column": str(r[3]),
            }
            for r in rows
        ]

    # ── Extended object types ─────────────────────────────────────────────

    def list_views_with_definitions(
        self,
        engine: Engine,
        schema: str,
    ) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text(
                        "SELECT viewname, definition "
                        "FROM pg_views WHERE schemaname = :schema "
                        "ORDER BY viewname"
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

    def list_stored_procedures(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT p.proname, pg_get_functiondef(p.oid) AS body, "
                    "obj_description(p.oid, 'pg_proc') AS comment "
                    "FROM pg_proc p "
                    "JOIN pg_namespace n ON n.oid = p.pronamespace "
                    "WHERE n.nspname = :schema "
                    "AND p.prokind = 'p' "  # 'p' = procedure (Redshift adopted PG 11+ semantics)
                    "ORDER BY p.proname"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "procedure",
                "definition": str(r[1]) if r[1] else None,
                "comment": str(r[2]) if r[2] else None,
                "metadata": {},
            }
            for r in rows
        ]

    def list_functions(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # Includes both Python UDFs and SQL UDFs. Filter out aggregates
        # and window functions since they don't map cleanly.
        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text(
                        "SELECT p.proname, pg_get_functiondef(p.oid) AS body, "
                        "obj_description(p.oid, 'pg_proc') AS comment, "
                        "p.prolang::regtype AS language "
                        "FROM pg_proc p "
                        "JOIN pg_namespace n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = :schema "
                        "AND p.prokind = 'f' "
                        "ORDER BY p.proname"
                    ),
                    {"schema": schema},
                ).fetchall()
            except Exception:
                # Some Redshift versions don't expose prokind. Fall back
                # to the prouserdefined flag.
                rows = conn.execute(
                    text(
                        "SELECT p.proname, NULL, NULL, NULL "
                        "FROM pg_proc p "
                        "JOIN pg_namespace n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = :schema "
                        "AND p.prouserdefined = true "
                        "ORDER BY p.proname"
                    ),
                    {"schema": schema},
                ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "function",
                "definition": str(r[1]) if r[1] else None,
                "comment": str(r[2]) if r[2] else None,
                "metadata": {"language": str(r[3]) if r[3] else None},
            }
            for r in rows
        ]

    def list_external_tables(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # External tables are tied to external schemas (Glue / Hive /
        # Redshift Spectrum). SVV_EXTERNAL_TABLES is the right view.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT tablename, location, input_format, output_format, "
                    "serialization_lib "
                    "FROM SVV_EXTERNAL_TABLES "
                    "WHERE schemaname = :schema "
                    "ORDER BY tablename"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "external_table",
                "definition": None,
                "comment": None,
                "metadata": {
                    "location": str(r[1]) if r[1] else None,
                    "input_format": str(r[2]) if r[2] else None,
                    "output_format": str(r[3]) if r[3] else None,
                    "serde": str(r[4]) if r[4] else None,
                },
            }
            for r in rows
        ]

    def list_datashares(self, engine: Engine) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT share_name, share_type, share_owner, source_database, "
                    "consumer_namespace, status "
                    "FROM SVV_DATASHARES "
                    "ORDER BY share_name"
                )
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": str(r[1]) if r[1] else "datashare",
                "definition": None,
                "comment": None,
                "metadata": {
                    "owner": str(r[2]) if r[2] else None,
                    "source_database": str(r[3]) if r[3] else None,
                    "consumer_namespace": str(r[4]) if r[4] else None,
                    "status": str(r[5]) if r[5] else None,
                },
            }
            for r in rows
        ]

    # ── Analytics metadata (★ Redshift-specific dist/sort/encoding) ──────

    def get_analytics_metadata(self, engine: Engine, schema: str, table: str) -> dict[str, Any]:
        out: dict[str, Any] = {}
        warnings: list[str] = []

        with engine.connect() as conn:
            try:
                row = conn.execute(
                    text(
                        "SELECT diststyle, sortkey1, encoded, "
                        "size, tbl_rows, unsorted, stats_off, "
                        "vacuum_sort_benefit "
                        "FROM SVV_TABLE_INFO "
                        'WHERE schema = :schema AND "table" = :table'
                    ),
                    {"schema": schema, "table": table},
                ).fetchone()
                if row:
                    if row[0]:
                        # diststyle is e.g. 'EVEN', 'KEY(<col>)', 'ALL', 'AUTO(...)'
                        out["partition_strategy"] = (
                            "key" if str(row[0]).upper().startswith("KEY") else str(row[0]).lower()
                        )
                    if row[1]:
                        out["clustering_keys"] = [str(row[1])]
                    if row[3] is not None:
                        # SVV_TABLE_INFO.size is in 1MB blocks.
                        out["storage_bytes"] = int(row[3]) * 1024 * 1024
                    out["table_type"] = "managed"
                    out["storage_format"] = "native"
                    metadata_extras = {
                        "diststyle": str(row[0]) if row[0] else None,
                        "sortkey1": str(row[1]) if row[1] else None,
                        "encoded": str(row[2]) if row[2] else None,
                        "unsorted_pct": float(row[5]) if row[5] is not None else None,
                        "stats_off": float(row[6]) if row[6] is not None else None,
                        "vacuum_sort_benefit": float(row[7]) if row[7] is not None else None,
                    }
                    out["redshift_table_info"] = {
                        k: v for k, v in metadata_extras.items() if v is not None
                    }
            except Exception as exc:
                warnings.append(f"SVV_TABLE_INFO: {exc}")

            # Per-column compression / encoding.
            try:
                rows = conn.execute(
                    text(
                        "SELECT column_name, encoding "
                        "FROM SVV_COLUMNS "
                        "WHERE table_schema = :schema AND table_name = :table "
                        "ORDER BY ordinal_position"
                    ),
                    {"schema": schema, "table": table},
                ).fetchall()
                if rows:
                    out["column_encodings"] = {str(r[0]): str(r[1]) for r in rows if r[1]}
            except Exception as exc:
                warnings.append(f"column encodings: {exc}")

        if warnings:
            out["warnings"] = warnings
        return out

    # ── Comment writing (PG-compatible) ──────────────────────────────────

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON {asset_keyword} {fqn} IS :cmt"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON COLUMN {fqn}.{self.quote_identifier(column)} IS :cmt"

    def set_schema_comment_sql(self, schema: str) -> str:
        return f"COMMENT ON SCHEMA {self.quote_identifier(schema)} IS :cmt"

    def set_database_comment_sql(self) -> str:
        return f"COMMENT ON DATABASE {self.quote_identifier(self.cfg.database)} IS :cmt"
