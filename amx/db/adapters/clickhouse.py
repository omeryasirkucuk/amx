"""ClickHouse backend adapter.

Driven by ``clickhouse-sqlalchemy`` over the HTTP transport (the
default port 8123, or 8443 with ``secure=True``). ClickHouse calls
its top-level scope a "database", not a "schema" — the adapter
treats them interchangeably so the rest of AMX doesn't have to
care.

Object types exposed:

* Tables, views, materialized views — first-class on ClickHouse and
  surfaced through ``system.tables`` filtered by engine name.
* User-defined functions (SQL UDFs and executable UDFs) via
  ``system.functions``.
* Dictionaries (``system.dictionaries``) — ClickHouse's external
  lookup primitive, no equivalent on any other backend.
* Data-skipping indices (``system.data_skipping_indices``) — these
  are not B-tree indexes; they prune granules during scans.
* Storage engine, partition info, on-disk size, and TTL settings
  surface in :meth:`get_analytics_metadata`.

ClickHouse does NOT enforce foreign keys (the constraint declaration
exists for documentation only), so ``relationships=False``. There
are no triggers, no stored procedures, and no sequences. Comment
write-back is supported on table/view/column/database since
ClickHouse 21.x.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


class ClickHouseAdapter(DatabaseAdapter):
    name = "clickhouse"
    capabilities = BackendCapabilities(
        # CH stores DB-level comments since 22.x. Keep True; users on
        # earlier versions see a soft failure logged as warning.
        database_comments=True,
        # CH databases ARE the schema scope — no separate schema-level
        # COMMENT.
        schema_comments=False,
        materialized_view_comments=True,
        materialized_views=True,
        relationships=False,  # FKs declared but not enforced
        row_count_stats=True,
        functions=True,
        dictionaries=True,
        comment_asset_keywords=frozenset({"TABLE", "VIEW", "MATERIALIZED VIEW"}),
    )

    def create_engine(self) -> Engine:
        return create_engine(self.cfg.url, pool_pre_ping=True)

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "code: 516" in msg or "authentication failed" in msg:
            return (
                "ClickHouse refused the credentials (Code 516). Check the "
                "username/password — note ClickHouse defaults the user to "
                "`default` with a blank password."
            )
        if "code: 81" in msg or "database" in msg and "doesn't exist" in msg:
            return (
                "ClickHouse refused: the `database` field on this profile "
                "points at a database that doesn't exist on this server. "
                "Edit the profile or create the database first."
            )
        if "code: 192" in msg or "connection refused" in msg:
            return (
                "ClickHouse server is not reachable. Check the host/port — "
                "HTTP defaults to 8123, HTTPS to 8443."
            )
        if "code: 497" in msg or "not enough privileges" in msg:
            return (
                "ClickHouse denied access. Grant the user SELECT on the "
                "target database and on `system.*` tables for introspection."
            )
        return None

    def system_schemas(self) -> frozenset[str]:
        # CH ships ``system`` (user-visible read-only catalogs),
        # ``information_schema`` / ``INFORMATION_SCHEMA`` (mirrored
        # under both casings), and an internal ``__inner__`` namespace
        # for materialized-view backing tables.
        return frozenset({"system", "information_schema", "INFORMATION_SCHEMA"})

    def quote_identifier(self, name: str) -> str:
        # CH supports both backticks and double quotes; backticks are
        # idiomatic and don't conflict with string literals.
        escaped = name.replace("`", "\\`")
        return f"`{escaped}`"

    def list_databases(self, engine: Engine) -> list[str]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT name FROM system.databases "
                    "WHERE name NOT IN ('system','information_schema','INFORMATION_SCHEMA') "
                    "ORDER BY name"
                )
            ).fetchall()
        return [str(r[0]) for r in rows]

    def list_schemas(self, engine: Engine, catalog: str = "") -> list[str] | None:
        # CH's "schema" IS its "database". Surface them as schemas so
        # the rest of AMX (which thinks in schemas) works without a
        # special case.
        return self.list_databases(engine)

    def list_tables(self, engine: Engine, schema: str, catalog: str = "") -> list[str] | None:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT name FROM system.tables "
                    "WHERE database = :db "
                    "AND engine NOT LIKE '%View' "
                    "ORDER BY name"
                ),
                {"db": schema},
            ).fetchall()
        return [str(r[0]) for r in rows]

    def list_views(self, engine: Engine, schema: str, catalog: str = "") -> list[str] | None:
        # ``View`` engine is a non-materialized view; the materialized
        # variants are ``MaterializedView`` and ``WindowView`` — picked
        # up separately by ``list_materialized_views``.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT name FROM system.tables "
                    "WHERE database = :db AND engine = 'View' "
                    "ORDER BY name"
                ),
                {"db": schema},
            ).fetchall()
        return [str(r[0]) for r in rows]

    def list_materialized_views(self, engine: Engine, schema: str) -> list[str]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT name FROM system.tables "
                    "WHERE database = :db "
                    "AND engine IN ('MaterializedView','WindowView','LiveView') "
                    "ORDER BY name"
                ),
                {"db": schema},
            ).fetchall()
        return [str(r[0]) for r in rows]

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        # CH counts NULLs with countIf and offers exact distinct via
        # uniqExact (uniq is approximate). For wide cardinality columns
        # the connector should switch to sampled mode at the gate level.
        return (
            f"SELECT "
            f"  countIf({quoted_col} IS NULL) AS null_cnt, "
            f"  uniqExact({quoted_col}) AS dist_cnt, "
            f"  toString(min({quoted_col})) AS min_val, "
            f"  toString(max({quoted_col})) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT toString({quoted_col}) FROM {fqn} "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    def _null_count_expr(self, quoted_col: str) -> str:
        return f"countIf({quoted_col} IS NULL)"

    def _distinct_count_expr(self, quoted_col: str) -> str:
        # CH's exact-distinct primitive — matches the per-column path.
        return f"uniqExact({quoted_col})"

    def _aggregate_text_expr(self, agg: str, quoted_col: str) -> str:
        # CH per-column path uses ``toString(min(col))`` (outer cast).
        # CH is case-sensitive: lowercase ``min`` / ``max`` for built-ins.
        return f"toString({agg.lower()}({quoted_col}))"

    def _value_text_expr(self, quoted_col: str) -> str:
        return f"toString({quoted_col})"

    # ClickHouse ``SAMPLE`` only works on tables that declare a ``SAMPLE
    # BY`` clause at CREATE TABLE time. We can't assume that — leave
    # the bulk sample as a plain LIMIT; the user can opt in to SAMPLE
    # by overriding _bulk_sample_clause in their own adapter wrapper.

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
        # ``system.parts`` aggregates rows / bytes per active part.
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT sum(rows) FROM system.parts "
                    "WHERE database = :db AND table = :tbl AND active = 1"
                ),
                {"db": schema, "tbl": table},
            ).fetchone()
        n = int(row[0]) if row and row[0] is not None else 0
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n}

    def stats_label(self) -> str:
        return "system.parts (active parts only)"

    # ── Bulk schema metadata ──────────────────────────────────────────────

    def bulk_schema_metadata(
        self,
        engine: Engine,
        schema: str,
        *,
        catalog: str = "",
    ) -> dict[str, dict[str, Any]] | None:
        """Bulk fetch via ``system.tables`` + ``system.columns``.

        In ClickHouse, "database" is the equivalent of a schema in
        other backends. The caller passes the user-facing schema name
        as ``schema`` and we map it to ``database`` for the query.
        """
        try:
            out: dict[str, dict[str, Any]] = {}
            with engine.connect() as conn:
                table_rows = conn.execute(
                    text(
                        "SELECT name, engine, comment "
                        "FROM system.tables WHERE database = :db"
                    ),
                    {"db": schema},
                ).fetchall()
                for r in table_rows:
                    engine_name = str(r[1] or "").lower()
                    # MaterializedView is a CH-specific engine; the
                    # generic View engine covers regular views.
                    if "materializedview" in engine_name.replace(" ", ""):
                        kind = "MATERIALIZED VIEW"
                    elif "view" in engine_name:
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
                        "SELECT table, name, comment "
                        "FROM system.columns WHERE database = :db "
                        "ORDER BY table, position"
                    ),
                    {"db": schema},
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

    # ── Schema (database) comments ───────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        # CH stores DB comments in system.databases.comment (22.5+). On
        # older versions the column doesn't exist — degrade silently.
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT comment FROM system.databases WHERE name = :db"),
                    {"db": schema},
                ).fetchone()
            return str(row[0]) if row and row[0] else None
        except Exception:
            return None

    def get_database_comment(self, engine: Engine) -> str | None:
        # Same field — CH conflates "database" and "schema".
        return self.get_schema_comment(engine, self.cfg.database)

    # ── Extended object types ─────────────────────────────────────────────

    def list_functions(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # CH UDFs are global, not schema-scoped — but ``schema`` is
        # accepted for API parity with PG/SF/etc. The list comes from
        # system.functions filtered to user-defined entries.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT name, create_query "
                    "FROM system.functions "
                    "WHERE origin = 'SQLUserDefined' OR origin = 'ExecutableUserDefined' "
                    "ORDER BY name"
                )
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "function",
                "definition": str(r[1]) if r[1] else None,
                "comment": None,
                "metadata": {"scope": "global"},
            }
            for r in rows
        ]

    def list_dictionaries(self, engine: Engine, database: str) -> list[dict[str, Any]]:
        # ClickHouse dictionaries — external lookup tables refreshed
        # from a source (file, MySQL, MongoDB, S3, ...). They live in a
        # database scope; an empty ``database`` arg lists across all
        # user-visible databases.
        sql = (
            "SELECT name, database, source, layout_type, key_types, status, "
            "element_count, bytes_allocated "
            "FROM system.dictionaries"
        )
        params: dict[str, Any] = {}
        if database:
            sql += " WHERE database = :db"
            params["db"] = database
        sql += " ORDER BY database, name"
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "dictionary",
                "definition": None,
                "comment": None,
                "metadata": {
                    "database": str(r[1]) if r[1] else None,
                    "source": str(r[2]) if r[2] else None,
                    "layout": str(r[3]) if r[3] else None,
                    "key_types": list(r[4]) if r[4] else [],
                    "status": str(r[5]) if r[5] else None,
                    "element_count": int(r[6]) if r[6] is not None else None,
                    "bytes_allocated": int(r[7]) if r[7] is not None else None,
                },
            }
            for r in rows
        ]

    # ── Analytics metadata (★ ClickHouse engine + parts surface) ─────────

    def get_analytics_metadata(self, engine: Engine, schema: str, table: str) -> dict[str, Any]:
        out: dict[str, Any] = {}
        warnings: list[str] = []

        with engine.connect() as conn:
            try:
                row = conn.execute(
                    text(
                        "SELECT engine, partition_key, sorting_key, primary_key, "
                        "sampling_key, total_rows, total_bytes, metadata_modification_time "
                        "FROM system.tables "
                        "WHERE database = :db AND name = :tbl"
                    ),
                    {"db": schema, "tbl": table},
                ).fetchone()
                if row:
                    eng = str(row[0]) if row[0] else ""
                    out["storage_format"] = eng or "native"
                    if "MaterializedView" in eng:
                        out["table_type"] = "materialized_view"
                    elif "View" in eng:
                        out["table_type"] = "view"
                    else:
                        out["table_type"] = "managed"
                    if row[1]:
                        out["partition_keys"] = [
                            c.strip() for c in str(row[1]).split(",") if c.strip()
                        ]
                        out["partition_strategy"] = "expression"
                    if row[2]:
                        out["clustering_keys"] = [
                            c.strip() for c in str(row[2]).split(",") if c.strip()
                        ]
                    if row[6] is not None:
                        out["storage_bytes"] = int(row[6])
                    if row[7]:
                        out["last_modified"] = str(row[7])
                    out["clickhouse"] = {
                        "engine": eng,
                        "primary_key": str(row[3]) if row[3] else None,
                        "sampling_key": str(row[4]) if row[4] else None,
                        "total_rows": int(row[5]) if row[5] is not None else None,
                    }
            except Exception as exc:
                warnings.append(f"system.tables: {exc}")

            # Skipping indices — different concept from B-tree indexes
            # but the closest analogue.
            try:
                rows = conn.execute(
                    text(
                        "SELECT name, type, expr "
                        "FROM system.data_skipping_indices "
                        "WHERE database = :db AND table = :tbl"
                    ),
                    {"db": schema, "tbl": table},
                ).fetchall()
                if rows:
                    out["indexes"] = [
                        {
                            "name": str(r[0]),
                            "columns": [str(r[2])] if r[2] else [],
                            "unique": False,  # skipping indices aren't unique
                            "type": str(r[1]) if r[1] else None,
                        }
                        for r in rows
                    ]
            except Exception as exc:
                warnings.append(f"data_skipping_indices: {exc}")

            # Files / part count
            try:
                row = conn.execute(
                    text(
                        "SELECT count(), sum(bytes_on_disk) "
                        "FROM system.parts "
                        "WHERE database = :db AND table = :tbl AND active = 1"
                    ),
                    {"db": schema, "tbl": table},
                ).fetchone()
                if row and row[0]:
                    out["storage_files_count"] = int(row[0])
                    if row[1] is not None:
                        out["storage_bytes"] = int(row[1])
            except Exception as exc:
                warnings.append(f"system.parts: {exc}")

        if warnings:
            out["warnings"] = warnings
        return out

    # ── Comment writing ───────────────────────────────────────────────────

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        # CH 21+ supports ALTER TABLE ... MODIFY COMMENT for tables and
        # views. Materialized views use the same syntax with the
        # MATERIALIZED VIEW keyword.
        fqn = self.fully_qualified_name(schema, table)
        return f"ALTER {asset_keyword} {fqn} MODIFY COMMENT :cmt"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        fqn = self.fully_qualified_name(schema, table)
        return f"ALTER TABLE {fqn} COMMENT COLUMN {self.quote_identifier(column)} :cmt"

    def set_schema_comment_sql(self, schema: str) -> str:
        # CH conflates schema and database; capability flag is False.
        raise self.unsupported("set_schema_comment_sql")

    def set_database_comment_sql(self) -> str:
        return f"ALTER DATABASE {self.quote_identifier(self.cfg.database)} MODIFY COMMENT :cmt"

    def comment_sql_with_params(
        self, stmt_template: str, comment: str
    ) -> tuple[str, dict[str, Any]]:
        # CH's HTTP interface handles named binds inconsistently for
        # DDL; safer to inline the literal.
        literal = self.quote_literal(comment)
        return stmt_template.replace(":cmt", literal), {}
