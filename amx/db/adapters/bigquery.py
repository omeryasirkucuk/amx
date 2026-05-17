"""Google BigQuery backend adapter."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


class BigQueryAdapter(DatabaseAdapter):
    name = "bigquery"
    capabilities = BackendCapabilities(
        database_comments=False,
        materialized_view_comments=True,
        materialized_views=False,
        relationships=True,
        row_count_stats=True,
        full_scan_when_row_count_unknown=False,
        stored_procedures=True,
        functions=True,
        external_tables=True,
        supports_shared_history=True,
        comment_asset_keywords=frozenset({"TABLE", "VIEW", "MATERIALIZED VIEW"}),
    )

    def create_history_schema_ddl(self, schema_name: str) -> str:
        # BigQuery uses backtick-quoted ``project.dataset`` identifiers.
        # The dataset (= "schema" in AMX nomenclature) lives under a
        # specific project; we use the project pinned on the profile.
        project = (getattr(self.cfg, "project", "") or "").strip()
        if not project:
            raise ValueError(
                "BigQuery shared-history bootstrap requires `project` to be set on the DB profile."
            )
        return f"CREATE SCHEMA IF NOT EXISTS `{project}.{schema_name}`"

    def create_engine(self) -> Engine:
        try:
            from sqlalchemy import create_engine
        except ImportError as exc:  # pragma: no cover
            raise ImportError("SQLAlchemy is required.") from exc
        try:
            import sqlalchemy_bigquery  # noqa: F401 — registers dialect
        except ImportError as exc:
            raise ImportError(
                "sqlalchemy-bigquery is required for the BigQuery backend. "
                "Install the extra: pip install 'amx-cli[bigquery]'"
            ) from exc
        # No pool_pre_ping — every checkout would issue a `SELECT 1` against
        # BigQuery, which is per-query billed (each pre-ping = $0.01-$0.02
        # if the on-demand minimum applies, multiplied by the checkout count
        # of a single /run). pool_recycle keeps connections fresh without
        # any keepalive query.
        return create_engine(self.cfg.url, pool_recycle=1800)

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"INFORMATION_SCHEMA", "information_schema"})

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "access denied" in msg or "permission" in msg or "forbidden" in msg:
            return "Insufficient BigQuery permissions. Grant metadata read and table data viewer permissions for profiling."
        if "not found" in msg:
            return "BigQuery dataset/table is missing or not visible in the configured project."
        if "quota" in msg or "rate limit" in msg:
            return "BigQuery quota or rate limit was reached. Retry later or switch profiling to metadata/sampled mode."
        return None

    # ── Identifier quoting ────────────────────────────────────────────────

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"

    def fully_qualified_name(self, schema: str, table: str) -> str:
        project = getattr(self.cfg, "project", "") or ""
        if project:
            return f"`{project}`.`{schema}`.`{table}`"
        return f"`{schema}`.`{table}`"

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT "
            f"  COUNTIF({quoted_col} IS NULL) AS null_cnt, "
            f"  {self._distinct_count_expr(quoted_col)} AS dist_cnt, "
            f"  MIN(CAST({quoted_col} AS STRING)) AS min_val, "
            f"  MAX(CAST({quoted_col} AS STRING)) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT CAST({quoted_col} AS STRING) FROM {fqn} TABLESAMPLE SYSTEM (1 PERCENT) "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    def _null_count_expr(self, quoted_col: str) -> str:
        return f"COUNTIF({quoted_col} IS NULL)"

    def _distinct_count_expr(self, quoted_col: str) -> str:
        # BigQuery's APPROX_COUNT_DISTINCT uses HyperLogLog++ and bills a
        # tiny fraction of an exact COUNT(DISTINCT) on wide / high-
        # cardinality columns. The exact aggregate scans every row in
        # the sampled slice — TABLESAMPLE SYSTEM (1 PERCENT) narrows
        # the input but the distinct-hash itself still touches each
        # sampled row, which is what produces the surprise credit
        # spikes the profiling-billing fix targets. Default behaviour
        # is unchanged (cfg flag defaults to False).
        if getattr(self.cfg, "profiling_approximate", False):
            return f"APPROX_COUNT_DISTINCT({quoted_col})"
        return f"COUNT(DISTINCT {quoted_col})"

    def _aggregate_text_expr(self, agg: str, quoted_col: str) -> str:
        return f"{agg}(CAST({quoted_col} AS STRING))"

    def _value_text_expr(self, quoted_col: str) -> str:
        return f"CAST({quoted_col} AS STRING)"

    def _bulk_sample_clause(self) -> str:
        # BigQuery only supports block sampling (``TABLESAMPLE SYSTEM``)
        # and the percentage must be at the table-reference level.
        return "TABLESAMPLE SYSTEM (1 PERCENT)"

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
        project = getattr(self.cfg, "project", "") or ""
        dataset = schema
        info_schema = (
            f"`{project}`.`{dataset}`.INFORMATION_SCHEMA.TABLES"
            if project
            else f"`{dataset}`.INFORMATION_SCHEMA.TABLES"
        )
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(f"SELECT row_count FROM {info_schema} WHERE table_name = :table"),
                    {"table": table},
                ).fetchone()
            n = int(row[0] or 0) if row else 0
            return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n}
        except Exception as exc:
            actionable = self.actionable_profile_error(exc)
            raise RuntimeError(actionable or str(exc)) from exc

    def stats_label(self) -> str:
        return "INFORMATION_SCHEMA.TABLES"

    # ── Bulk catalog metadata ─────────────────────────────────────────────

    def bulk_catalog_metadata(
        self,
        engine: Engine,
        catalog: str = "",
    ) -> dict[str, str | None] | None:
        """BigQuery doesn't store a free-text dataset description in
        ``INFORMATION_SCHEMA.SCHEMATA`` — descriptions live on the
        dataset resource itself, fetched via the Google client. The
        bulk query still wins because it replaces one
        ``SELECT … FROM `project`.INFORMATION_SCHEMA.SCHEMATA`` for
        the schema list with N per-dataset client calls the legacy
        path would make.
        """
        project = (catalog or getattr(self.cfg, "project", "") or "").strip()
        info_path = (
            f"`{project}`.INFORMATION_SCHEMA.SCHEMATA" if project else "INFORMATION_SCHEMA.SCHEMATA"
        )
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(f"SELECT schema_name FROM {info_path}")).fetchall()
            return {str(r[0]): None for r in rows}
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
        """One ``INFORMATION_SCHEMA`` round-trip per dataset.

        BigQuery exposes a per-dataset ``INFORMATION_SCHEMA`` view; the
        catalog is the GCP project and the schema is the dataset name.
        Nested STRUCT fields live in ``COLUMN_FIELD_PATHS`` rather than
        ``COLUMNS``, but for the sidebar / inspect UX we only need
        top-level columns — STRUCT introspection is a separate feature.
        """
        project = (catalog or getattr(self.cfg, "project", "") or "").strip()
        info_path_tables = (
            f"`{project}`.`{schema}`.INFORMATION_SCHEMA.TABLES"
            if project
            else f"`{schema}`.INFORMATION_SCHEMA.TABLES"
        )
        info_path_columns = (
            f"`{project}`.`{schema}`.INFORMATION_SCHEMA.COLUMNS"
            if project
            else f"`{schema}`.INFORMATION_SCHEMA.COLUMNS"
        )
        try:
            out: dict[str, dict[str, Any]] = {}
            with engine.connect() as conn:
                table_rows = conn.execute(
                    text(
                        f"SELECT table_name, table_type, "
                        f"  (SELECT option_value FROM "
                        f"   `{project}`.`{schema}`.INFORMATION_SCHEMA.TABLE_OPTIONS o "
                        f"   WHERE o.table_name = t.table_name AND option_name = 'description' "
                        f"   LIMIT 1) AS table_comment "
                        f"FROM {info_path_tables} t"
                    )
                ).fetchall()
                for r in table_rows:
                    raw_kind = str(r[1] or "").upper()
                    if "MATERIALIZED" in raw_kind:
                        kind = "MATERIALIZED VIEW"
                    elif "VIEW" in raw_kind:
                        kind = "VIEW"
                    else:
                        kind = "TABLE"
                    # BigQuery wraps the option_value in double-quotes
                    # ("foo") — strip them for a clean string.
                    raw_comment = str(r[2]) if r[2] is not None else ""
                    table_comment = (
                        raw_comment[1:-1]
                        if raw_comment.startswith('"') and raw_comment.endswith('"')
                        else raw_comment
                    )
                    out[str(r[0])] = {
                        "table_comment": table_comment or None,
                        "columns": {},
                        "kind": kind,
                    }
                col_rows = conn.execute(
                    text(
                        f"SELECT table_name, column_name, description "
                        f"FROM {info_path_columns} "
                        f"ORDER BY table_name, ordinal_position"
                    )
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
        project = getattr(self.cfg, "project", "") or ""
        info_path = (
            f"`{project}`.`{schema}`.INFORMATION_SCHEMA.SCHEMATA"
            if project
            else f"`{schema}`.INFORMATION_SCHEMA.SCHEMATA"
        )
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        f"SELECT option_value "
                        f"FROM {info_path}_OPTIONS "
                        "WHERE option_name = 'description'"
                    )
                ).fetchone()
            return str(row[0]) if row and row[0] else None
        except Exception:
            return None

    def get_database_comment(self, engine: Engine) -> str | None:
        return None

    # ── Incoming foreign keys ─────────────────────────────────────────────
    # BigQuery has informational constraints (not enforced) via
    # INFORMATION_SCHEMA.TABLE_CONSTRAINTS / KEY_COLUMN_USAGE.

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
        project = getattr(self.cfg, "project", "") or ""
        prefix = f"`{project}`." if project else ""
        tc_path = f"{prefix}`{schema}`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
        kcu_path = f"{prefix}`{schema}`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
        ccu_path = f"{prefix}`{schema}`.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE"
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SELECT "
                        f"  kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.COLUMN_NAME, "
                        f"  ccu.COLUMN_NAME "
                        f"FROM {tc_path} tc "
                        f"JOIN {kcu_path} kcu "
                        f"  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
                        f"JOIN {ccu_path} ccu "
                        f"  ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME "
                        "WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY' "
                        "  AND ccu.TABLE_SCHEMA = :schema "
                        "  AND ccu.TABLE_NAME = :table"
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
        except Exception as exc:
            actionable = self.actionable_profile_error(exc)
            raise RuntimeError(actionable or str(exc)) from exc

    # ── Extended object types ─────────────────────────────────────────────

    def _routine_query(self, schema: str, kind: str) -> str:
        # ``kind`` is BigQuery's ROUTINE_TYPE: 'PROCEDURE' or 'FUNCTION'
        # (the latter covers SQL UDFs and JS UDFs both).
        return (
            f"SELECT routine_name, routine_definition, ddl, language, "
            f"data_type, last_altered "
            f"FROM `{getattr(self.cfg, 'project', '')}`.`{schema}`.INFORMATION_SCHEMA.ROUTINES "
            f"WHERE routine_type = '{kind}' "
            f"ORDER BY routine_name"
        )

    def list_views_with_definitions(
        self,
        engine: Engine,
        schema: str,
    ) -> list[dict[str, Any]]:
        # BigQuery views live in `<project>.<dataset>.INFORMATION_SCHEMA.VIEWS`.
        # Project comes from the engine URL; SQLAlchemy puts it in the dialect
        # at run time. The dataset is the AMX-level "schema". Fully qualifying
        # avoids relying on a session-level default project.
        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text(
                        f"SELECT table_name, view_definition "
                        f"FROM `{schema}`.INFORMATION_SCHEMA.VIEWS "
                        f"ORDER BY table_name"
                    )
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
            try:
                rows = conn.execute(text(self._routine_query(schema, "PROCEDURE"))).fetchall()
            except Exception:
                return []
        return [
            {
                "name": str(r[0]),
                "type": "procedure",
                "definition": str(r[2] or r[1]) if (r[2] or r[1]) else None,
                "comment": None,
                "metadata": {
                    "language": str(r[3]) if r[3] else None,
                    "last_altered": str(r[5]) if r[5] else None,
                },
            }
            for r in rows
        ]

    def list_functions(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            try:
                rows = conn.execute(text(self._routine_query(schema, "FUNCTION"))).fetchall()
            except Exception:
                return []
        return [
            {
                "name": str(r[0]),
                "type": "function",
                "definition": str(r[2] or r[1]) if (r[2] or r[1]) else None,
                "comment": None,
                "metadata": {
                    "language": str(r[3]) if r[3] else None,
                    "return_type": str(r[4]) if r[4] else None,
                    "last_altered": str(r[5]) if r[5] else None,
                },
            }
            for r in rows
        ]

    def list_external_tables(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # BigQuery external tables (EXTERNAL) and BigLake tables both
        # show table_type='EXTERNAL'. Storage URIs / format come from
        # INFORMATION_SCHEMA.EXTERNAL_TABLE_OPTIONS as JSON-ish strings.
        project = getattr(self.cfg, "project", "")
        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text(
                        f"SELECT table_name "
                        f"FROM `{project}`.`{schema}`.INFORMATION_SCHEMA.TABLES "
                        f"WHERE table_type = 'EXTERNAL' "
                        f"ORDER BY table_name"
                    )
                ).fetchall()
            except Exception:
                return []
            opts_by_table: dict[str, dict[str, str]] = {}
            try:
                opt_rows = conn.execute(
                    text(
                        f"SELECT table_name, option_name, option_value "
                        f"FROM `{project}`.`{schema}`.INFORMATION_SCHEMA.EXTERNAL_TABLE_OPTIONS"
                    )
                ).fetchall()
                for orow in opt_rows:
                    bucket = opts_by_table.setdefault(str(orow[0]), {})
                    bucket[str(orow[1])] = str(orow[2])
            except Exception:
                pass
        return [
            {
                "name": str(r[0]),
                "type": "external_table",
                "definition": None,
                "comment": None,
                "metadata": opts_by_table.get(str(r[0]), {}),
            }
            for r in rows
        ]

    # ── Analytics metadata ────────────────────────────────────────────────

    def get_analytics_metadata(self, engine: Engine, schema: str, table: str) -> dict[str, Any]:
        """BigQuery analytics metadata.

        Pulls partition / cluster / size / freshness / type from
        ``INFORMATION_SCHEMA.TABLES`` (and ``COLUMNS`` for partition
        column type). Each query is wrapped so a single permission
        denied or unsupported-region failure leaves the affected
        field empty and records a warning.

        BigQuery storage format is always ``native`` for managed
        tables; external tables are flagged via ``table_type``.
        """
        out: dict[str, Any] = {}
        warnings: list[str] = []

        with engine.connect() as conn:
            # Partition + cluster + last_modified + type from INFORMATION_SCHEMA.TABLES.
            try:
                row = conn.execute(
                    text(
                        f"""
                        SELECT
                            table_type,
                            ddl,
                            CAST(creation_time AS STRING) AS creation_time
                        FROM `{schema}`.INFORMATION_SCHEMA.TABLES
                        WHERE table_name = @tname
                        LIMIT 1
                        """
                    ).bindparams(tname=table),
                ).fetchone()
                if row:
                    raw_type = str(row[0] or "").lower()
                    type_map = {
                        "base table": "managed",
                        "view": "view",
                        "materialized view": "materialized_view",
                        "external": "external",
                    }
                    out["table_type"] = type_map.get(raw_type, raw_type)
                    if "external" in raw_type:
                        out["storage_format"] = "external"
                    else:
                        out["storage_format"] = "native"
                    # Parse partition + cluster from the DDL — INFORMATION_SCHEMA
                    # doesn't expose them as structured columns in standard SQL.
                    ddl = str(row[1] or "")
                    if "PARTITION BY" in ddl.upper():
                        # Extract whatever's between "PARTITION BY" and the next clause.
                        upper = ddl.upper()
                        start = upper.index("PARTITION BY") + len("PARTITION BY")
                        end_candidates = [
                            upper.find("CLUSTER BY", start),
                            upper.find("OPTIONS(", start),
                            upper.find(" AS ", start),
                            len(ddl),
                        ]
                        end = min((e for e in end_candidates if e > 0), default=len(ddl))
                        partition_expr = ddl[start:end].strip().rstrip(",")
                        out["partition_keys"] = [partition_expr]
                        if (
                            "_PARTITIONDATE" in partition_expr.upper()
                            or "DATE(" in partition_expr.upper()
                            or "_PARTITIONTIME" in partition_expr.upper()
                        ):
                            out["partition_strategy"] = "time"
                        elif "RANGE_BUCKET" in partition_expr.upper():
                            out["partition_strategy"] = "range"
                        else:
                            out["partition_strategy"] = "time"
                    if "CLUSTER BY" in ddl.upper():
                        upper = ddl.upper()
                        start = upper.index("CLUSTER BY") + len("CLUSTER BY")
                        end_candidates = [
                            upper.find("OPTIONS(", start),
                            upper.find(" AS ", start),
                            len(ddl),
                        ]
                        end = min((e for e in end_candidates if e > 0), default=len(ddl))
                        cluster_expr = ddl[start:end].strip().rstrip(",")
                        out["clustering_keys"] = [
                            c.strip() for c in cluster_expr.split(",") if c.strip()
                        ]
                    if row[2]:
                        out["last_modified"] = str(row[2])
            except Exception as exc:
                warnings.append(f"INFORMATION_SCHEMA.TABLES: {exc}")

            # storage_bytes from __TABLES__ (legacy SQL — may require permissions).
            try:
                row = conn.execute(
                    text(
                        f"""
                        SELECT size_bytes, last_modified_time
                        FROM `{schema}.__TABLES__`
                        WHERE table_id = @tname
                        LIMIT 1
                        """
                    ).bindparams(tname=table),
                ).fetchone()
                if row:
                    if row[0] is not None:
                        out["storage_bytes"] = int(row[0])
                    if row[1] is not None and not out.get("last_modified"):
                        # last_modified_time is unix millis in __TABLES__.
                        try:
                            from datetime import datetime, timezone

                            ts = datetime.fromtimestamp(int(row[1]) / 1000.0, tz=timezone.utc)
                            out["last_modified"] = ts.isoformat()
                        except Exception:
                            pass
            except Exception as exc:
                warnings.append(f"__TABLES__: {exc}")

        if warnings:
            out["warnings"] = warnings
        return out

    # ── Comment writing ───────────────────────────────────────────────────

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        if asset_keyword not in self.capabilities.comment_asset_keywords:
            raise self.unsupported(f"Comment write-back for {asset_keyword.lower()} assets")
        fqn = self.fully_qualified_name(schema, table)
        return f"ALTER {asset_keyword} {fqn} SET OPTIONS(description = :cmt)"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        fqn = self.fully_qualified_name(schema, table)
        col = self.quote_identifier(column)
        return f"ALTER TABLE {fqn} ALTER COLUMN {col} SET OPTIONS(description = :cmt)"

    def set_schema_comment_sql(self, schema: str) -> str:
        project = getattr(self.cfg, "project", "") or ""
        ds = f"`{project}`.`{schema}`" if project else f"`{schema}`"
        return f"ALTER SCHEMA {ds} SET OPTIONS(description = :cmt)"

    def set_database_comment_sql(self) -> str:
        raise self.unsupported(
            "BigQuery project descriptions are not supported through SQL write-back."
        )
