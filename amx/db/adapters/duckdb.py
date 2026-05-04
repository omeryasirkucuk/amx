"""DuckDB backend adapter.

DuckDB is a single-file (or in-memory) embedded analytical database.
The connection target lives in ``DBConfig.database`` — either a path
to a ``.duckdb`` file or the literal string ``:memory:``. There is no
host / port / user / password.

What this adapter exposes:

* Tables and views via SQLAlchemy inspector + ``duckdb_tables()`` /
  ``duckdb_views()`` for fully-qualified names (DuckDB supports
  ``catalog.schema.table`` once a database is attached).
* Sequences via ``duckdb_sequences()``.
* Functions and macros — DuckDB's macros are parameterized SQL or
  table-returning expressions and have no equivalent in any other
  backend, so they get their own list method.
* External tables (Parquet / CSV files exposed as views via
  ``read_parquet`` / ``read_csv``) — surfaced as views since DuckDB
  doesn't carry a separate ``EXTERNAL`` table type.
* Standard ``COMMENT ON TABLE / COLUMN / SCHEMA`` write-back.

DuckDB has no row-level FK enforcement, no triggers, no stored
procedures, and no concept of materialized views, so those
capabilities stay False.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


class DuckDBAdapter(DatabaseAdapter):
    name = "duckdb"
    capabilities = BackendCapabilities(
        # DuckDB has no DATABASE-level comment ("database" is a file)
        # and no schema-level comment yet (NotImplemented as of 1.5).
        database_comments=False,
        schema_comments=False,
        materialized_views=False,
        relationships=False,  # no FK enforcement
        row_count_stats=True,
        functions=True,
        sequences=True,
        macros=True,
    )

    def create_engine(self) -> Engine:
        # ``DBConfig.url`` already produces ``duckdb:///<path>`` or
        # ``duckdb:///:memory:``. ``pool_pre_ping`` is irrelevant for
        # the embedded driver but harmless.
        return create_engine(self.cfg.url)

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "io error" in msg and "cannot open file" in msg:
            return (
                "DuckDB cannot open the database file. Check that the path in "
                "the profile exists and is readable, or use ':memory:' for an "
                "ephemeral database."
            )
        if "is being used by another process" in msg:
            return (
                "Another process holds an exclusive lock on the DuckDB file. "
                "Close the other connection (or use a separate file) and retry."
            )
        if "table" in msg and "does not exist" in msg:
            return "Referenced table is missing or hasn't been created in this database."
        return None

    def system_schemas(self) -> frozenset[str]:
        # DuckDB ships ``information_schema``, ``pg_catalog``, ``system``,
        # and the per-connection ``temp`` schema. ``main`` is the user-
        # visible default.
        return frozenset({"information_schema", "pg_catalog", "system", "temp"})

    def list_schemas(self, engine: Engine, catalog: str = "") -> list[str] | None:
        # SQLAlchemy's DuckDB dialect returns catalog-qualified schema
        # names (e.g. ``mydb.analytics``, ``system.information_schema``)
        # which the system_schemas filter doesn't know how to match.
        # Query ``duckdb_schemas()`` directly and filter on the
        # ``internal`` flag so user-visible schemas across all attached
        # databases come back as bare names.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT schema_name FROM duckdb_schemas() "
                    "WHERE NOT internal ORDER BY schema_name"
                )
            ).fetchall()
        return [str(r[0]) for r in rows]

    def list_databases(self, engine: Engine) -> list[str]:
        # DuckDB lets you ATTACH multiple files (Parquet datasets, the
        # Postgres scanner, SQLite, etc.). Each shows up in
        # ``duckdb_databases()`` and is a legitimate top-level scope to
        # browse.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT database_name FROM duckdb_databases() "
                    "WHERE NOT internal ORDER BY database_name"
                )
            ).fetchall()
        return [str(r[0]) for r in rows]

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        # DuckDB casts most types to VARCHAR cleanly; for binary blobs
        # the cast is best-effort. Same shape as PG.
        return (
            f"SELECT "
            f"  COUNT(*) FILTER (WHERE {quoted_col} IS NULL) AS null_cnt, "
            f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
            f"  CAST(MIN({quoted_col}) AS VARCHAR) AS min_val, "
            f"  CAST(MAX({quoted_col}) AS VARCHAR) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        return (
            f"SELECT DISTINCT CAST({quoted_col} AS VARCHAR) FROM {fqn} "
            f"WHERE {quoted_col} IS NOT NULL LIMIT :lim"
        )

    def _aggregate_text_expr(self, agg: str, quoted_col: str) -> str:
        # DuckDB per-column path uses outer-cast ``CAST(MIN(col) AS VARCHAR)``;
        # mirror to keep bulk and per-column outputs aligned.
        return f"CAST({agg}({quoted_col}) AS VARCHAR)"

    def _value_text_expr(self, quoted_col: str) -> str:
        return f"CAST({quoted_col} AS VARCHAR)"

    # DuckDB has ``USING SAMPLE n%`` which requires a separate clause
    # position; LIMIT alone is fine for sample collection on local /
    # in-memory analytical workloads.

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
        # DuckDB doesn't track scan counters the way PG's
        # ``pg_stat_user_tables`` does. Return a row-count estimate so
        # the connector at least has something to gate full scans on.
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(f"SELECT COUNT(*) FROM {self.fully_qualified_name(schema, table)}")
                ).fetchone()
            n = int(row[0]) if row else 0
        except Exception:
            n = 0
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n}

    def stats_label(self) -> str:
        return "row count (no scan counters)"

    # ── Schema comments ───────────────────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT comment FROM duckdb_schemas() WHERE schema_name = :schema"),
                {"schema": schema},
            ).fetchone()
        return str(row[0]) if row and row[0] else None

    # ── Extended object types ─────────────────────────────────────────────

    def list_sequences(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT sequence_name, comment, start_value, increment_by, "
                    "min_value, max_value, cycle "
                    "FROM duckdb_sequences() WHERE schema_name = :schema "
                    "ORDER BY sequence_name"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "sequence",
                "definition": None,
                "comment": str(r[1]) if r[1] else None,
                "metadata": {
                    "start": int(r[2]) if r[2] is not None else None,
                    "increment": int(r[3]) if r[3] is not None else None,
                    "min": int(r[4]) if r[4] is not None else None,
                    "max": int(r[5]) if r[5] is not None else None,
                    "cycle": bool(r[6]) if r[6] is not None else False,
                },
            }
            for r in rows
        ]

    def list_functions(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # DuckDB exposes ~1000 built-in functions; restrict to user-defined
        # ones (function_type = 'macro' is handled separately by
        # list_macros). Internal functions live in ``main`` / ``pg_catalog``
        # and are noisy — filter by schema and skip the ``internal`` flag.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT function_name, function_type, return_type, parameters, comment "
                    "FROM duckdb_functions() "
                    "WHERE schema_name = :schema AND NOT internal "
                    "AND function_type IN ('scalar','aggregate','table','pragma') "
                    "ORDER BY function_name"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": str(r[1]),
                "definition": None,
                "comment": str(r[4]) if r[4] else None,
                "metadata": {
                    "return_type": str(r[2]) if r[2] else None,
                    "parameters": list(r[3]) if r[3] else [],
                },
            }
            for r in rows
        ]

    def list_macros(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # ``duckdb_functions()`` reports macros via ``function_type='macro'``
        # and ``function_type='table_macro'``. They get their own listing
        # so downstream tooling can distinguish "real UDF" from "stored
        # SQL snippet" — a DuckDB-distinctive feature.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT function_name, function_type, macro_definition, parameters, comment "
                    "FROM duckdb_functions() "
                    "WHERE schema_name = :schema "
                    "AND function_type IN ('macro','table_macro') "
                    "ORDER BY function_name"
                ),
                {"schema": schema},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": str(r[1]),
                "definition": str(r[2]) if r[2] else None,
                "comment": str(r[4]) if r[4] else None,
                "metadata": {"parameters": list(r[3]) if r[3] else []},
            }
            for r in rows
        ]

    # ── Comment writing ───────────────────────────────────────────────────

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        # DuckDB accepts standard ``COMMENT ON TABLE/VIEW``. It does NOT
        # accept ``MATERIALIZED VIEW`` (no support), but the connector
        # already gates that via capabilities so we never get called with
        # that keyword.
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON {asset_keyword} {fqn} IS :cmt"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON COLUMN {fqn}.{self.quote_identifier(column)} IS :cmt"

    def set_schema_comment_sql(self, schema: str) -> str:
        # DuckDB 1.x raises ``NotImplementedException`` for COMMENT ON
        # SCHEMA; the matching capability flag is False so the connector
        # never calls this, but we still raise here for safety.
        raise self.unsupported("set_schema_comment_sql")

    def set_database_comment_sql(self) -> str:
        # DuckDB has no DATABASE-level COMMENT — a "database" is a file.
        raise self.unsupported("set_database_comment_sql")
