"""PostgreSQL backend adapter."""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


class PostgreSQLAdapter(DatabaseAdapter):
    name = "postgresql"
    capabilities = BackendCapabilities(
        materialized_view_comments=True,
        materialized_views=True,
        relationships=True,
        row_count_stats=True,
        stored_procedures=True,
        functions=True,
        sequences=True,
        triggers=True,
        user_defined_types=True,
        comment_asset_keywords=frozenset({"TABLE", "VIEW", "MATERIALIZED VIEW"}),
        supports_shared_history=True,
    )

    def create_engine(self) -> Engine:
        return create_engine(self.cfg.url, pool_pre_ping=True)

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "pg_stat_statements must be loaded via shared_preload_libraries" in msg:
            return (
                "pg_stat_statements view is unavailable in this session. "
                "Enable it in postgresql.conf (shared_preload_libraries='pg_stat_statements') "
                "and restart PostgreSQL, or skip telemetry/system views from AMX scope."
            )
        if "permission denied" in msg:
            return "Insufficient privileges for profiling. Grant SELECT on this object or use a higher-privileged role."
        # libpq surfaces a wrong/missing password as ``FATAL: password
        # authentication failed for user "<name>"`` (or
        # ``28P01`` SQLSTATE). Without this branch the user sees the raw
        # SQLAlchemy ``OperationalError`` traceback — the same UX gap
        # the wizard's TLS-cert branch closes for Databricks.
        if (
            "password authentication failed" in msg
            or "no password supplied" in msg
            or "28p01" in msg
        ):
            return (
                "PostgreSQL refused the credentials. Check the username and "
                "password on this profile (open it with /edit). If the server "
                "uses peer/ident auth, the user must match the OS account or "
                "be remapped in pg_hba.conf."
            )
        # Catch a missing-database error from the server. When the
        # ``database`` profile field is blank, AMX falls back to the
        # ``postgres`` system database (see ``DBConfig.url`` for
        # postgresql), so this branch only fires when the user
        # explicitly pinned a database name that the server doesn't
        # have, OR when their role lacks ``CONNECT`` on the requested
        # database. In both cases the actionable next step is the same:
        # fix the name (or grant the privilege).
        if 'database "' in msg and "does not exist" in msg:
            return (
                "PostgreSQL refused: the `database` field on this profile points "
                "at a database that does not exist on this server. Open the "
                "profile with /edit and correct the name, or create the database "
                "first. (Tip: leave the `database` field blank to connect to the "
                "default `postgres` system database and pick a real database "
                "later with /database <name>.)"
            )
        if "undefined_table" in msg or "does not exist" in msg:
            return (
                "Referenced relation is missing or inaccessible in the current schema search path."
            )
        return None

    def system_schemas(self) -> frozenset[str]:
        return frozenset({"information_schema", "pg_catalog", "pg_toast"})

    def list_databases(self, engine: Engine) -> list[str]:
        """Return user-visible databases on this PostgreSQL server.

        Excludes templates (``datistemplate = false``) and the system
        ``postgres`` maintenance database itself unless the user has
        nothing else — when a fresh server has only ``postgres``,
        returning an empty list would be misleading and block AMX
        bootstrap. The default ordering is alphabetical for stable
        picker UX.
        """
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
            ).fetchall()
        names = [str(r[0]) for r in rows]
        # Drop the ``postgres`` maintenance DB when real user databases
        # exist. It otherwise shows up in the /history-store enable
        # picker as a tempting but wrong target — users connected to a
        # data DB (e.g. SAP) sometimes pick it without realising it's
        # the empty system database. Keep it as the sole option only
        # when the server has nothing else (fresh install).
        non_system = [n for n in names if n != "postgres"]
        return non_system if non_system else names

    # ── Materialized views ────────────────────────────────────────────────

    def list_materialized_views(self, engine: Engine, schema: str) -> list[str]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT c.relname FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relkind = 'm' "
                    "ORDER BY c.relname"
                ),
                {"schema": schema},
            ).fetchall()
        return [r[0] for r in rows]

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
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
        # PG-idiomatic ``col::text`` rather than ``CAST(col AS VARCHAR)``.
        # Behaviour identical, but matches existing column_stats_sql so
        # the bulk and per-column paths produce the same MIN/MAX values.
        return f"{agg}({quoted_col}::text)"

    def _value_text_expr(self, quoted_col: str) -> str:
        return f"{quoted_col}::text"

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT COALESCE(seq_scan, 0), COALESCE(idx_scan, 0), "
                    "COALESCE(n_live_tup, 0) "
                    "FROM pg_stat_user_tables "
                    "WHERE schemaname = :schema AND relname = :table"
                ),
                {"schema": schema, "table": table},
            ).fetchone()
        if row:
            return {
                "seq_scan": int(row[0] or 0),
                "idx_scan": int(row[1] or 0),
                "n_live_tup": int(row[2] or 0),
            }
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": 0}

    def stats_label(self) -> str:
        return "pg_stat_user_tables"

    # ── Bulk catalog metadata ─────────────────────────────────────────────

    def bulk_catalog_metadata(
        self,
        engine: Engine,
        catalog: str = "",
    ) -> dict[str, str | None] | None:
        """Pull every user schema + its comment in one ``pg_namespace`` scan.

        Skips the system catalogs (``pg_*`` + ``information_schema``)
        that ``DatabaseConnector.list_schemas`` would already filter
        out — keeps the bulk result aligned with what the sidebar
        actually shows.
        """
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
        """Single ``pg_catalog`` query for every table + column comment.

        Joins ``pg_class`` to ``pg_attribute`` so the result already has
        the column list per table; uses ``obj_description`` for the
        table-level comment and ``col_description`` for each column.
        Tables that have no columns still appear because of the LEFT
        JOIN — important for empty placeholders the user may have left
        behind during a half-finished migration.
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
            "  AND c.relkind IN ('r', 'v', 'm', 'p', 'f') "
            "ORDER BY c.relname, a.attnum"
        )
        try:
            out: dict[str, dict[str, Any]] = {}
            with engine.connect() as conn:
                rows = conn.execute(text(sql), {"schema": schema}).fetchall()
            for r in rows:
                tname = str(r[0])
                relkind = str(r[1])
                kind = {
                    "r": "TABLE",
                    "p": "TABLE",
                    "f": "TABLE",
                    "v": "VIEW",
                    "m": "MATERIALIZED VIEW",
                }.get(relkind, "TABLE")
                entry = out.setdefault(
                    tname,
                    {"table_comment": r[2], "columns": {}, "kind": kind},
                )
                entry["kind"] = kind
                entry["table_comment"] = r[2]
                if r[3] is not None:
                    entry["columns"][str(r[3])] = r[4]
            return out or None
        except Exception:
            return None

    # ── Schema / database comments ────────────────────────────────────────

    def get_schema_comment(self, engine: Engine, schema: str) -> str | None:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT obj_description(n.oid, 'pg_namespace') "
                    "FROM pg_namespace n WHERE n.nspname = :schema"
                ),
                {"schema": schema},
            ).fetchone()
        return row[0] if row else None

    def get_database_comment(self, engine: Engine) -> str | None:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT shobj_description(d.oid, 'pg_database') "
                    "FROM pg_database d WHERE d.datname = current_database()"
                )
            ).fetchone()
        return row[0] if row else None

    def batch_get_table_comments(
        self,
        engine: Engine,
        pairs: list[tuple[str, str]],
    ) -> dict[tuple[str, str], str | None] | None:
        """Resolve all (schema, table) → comment pairs in one SQL round-trip.

        Reads from ``pg_description`` joined to ``pg_class`` /
        ``pg_namespace``, restricted to the exact pairs the caller
        asked for. Tables without a comment are still present in the
        return dict with a ``None`` value so the caller can
        distinguish "no comment" from "table not found".

        Replaces the per-table ``inspect(engine).get_table_comment``
        fan-out used by ``Connector.get_related_table_comments``;
        with ~50 unique FK targets the round-trip count drops from
        ~50 to 1.
        """
        if not pairs:
            return {}
        # De-duplicate while preserving deterministic iteration so
        # tests can pin a stable parameter expansion.
        unique_pairs = sorted({(s, t) for s, t in pairs if s and t})
        # Build (schema, table) tuple parameters using SQLAlchemy
        # ``expanding`` semantics so the driver handles quoting and the
        # number of placeholders varies safely with the input length.
        # We fall back to a CTE join on a VALUES table because the
        # PostgreSQL driver doesn't support tuple expansion for IN
        # directly via SQLAlchemy's ``expanding`` flag.
        values_sql = ", ".join(f"(:s{idx}, :t{idx})" for idx in range(len(unique_pairs)))
        params: dict[str, str] = {}
        for idx, (schema, table) in enumerate(unique_pairs):
            params[f"s{idx}"] = schema
            params[f"t{idx}"] = table
        sql = (
            "WITH wanted(schema_name, table_name) AS (VALUES "
            f"{values_sql})\n"
            "SELECT n.nspname AS schema_name, c.relname AS table_name, "
            "obj_description(c.oid, 'pg_class') AS comment\n"
            "FROM wanted w "
            "JOIN pg_namespace n ON n.nspname = w.schema_name "
            "JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = w.table_name"
        )
        result: dict[tuple[str, str], str | None] = dict.fromkeys(unique_pairs)
        with engine.connect() as conn:
            for row in conn.execute(text(sql), params):
                key = (str(row[0]), str(row[1]))
                result[key] = row[2] if row[2] is not None else None
        return result

    def column_comments_probe_query(self, schema: str, table: str) -> str:
        return (
            "SELECT a.attname AS column_name, col_description(c.oid, a.attnum) AS comment "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid "
            "WHERE n.nspname = :schema AND c.relname = :table "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum"
        )

    def table_metadata_probe_query(self, schema: str, table: str) -> str:
        return (
            "SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS data_type, "
            "a.attnotnull AS not_null, col_description(c.oid, a.attnum) AS comment, "
            "obj_description(c.oid, 'pg_class') AS table_comment "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid "
            "WHERE n.nspname = :schema AND c.relname = :table "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum"
        )

    # ── Incoming foreign keys ─────────────────────────────────────────────

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
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
                    JOIN unnest(con.conkey)  WITH ORDINALITY AS src_key(attnum, ord) ON TRUE
                    JOIN unnest(con.confkey) WITH ORDINALITY AS tgt_key(attnum, ord)
                         ON src_key.ord = tgt_key.ord
                    JOIN pg_attribute src_col
                         ON src_col.attrelid = src.oid AND src_col.attnum = src_key.attnum
                    JOIN pg_attribute tgt_col
                         ON tgt_col.attrelid = tgt.oid AND tgt_col.attnum = tgt_key.attnum
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
                        "SELECT viewname, definition, "
                        "obj_description(("
                        "  quote_ident(schemaname) || '.' || quote_ident(viewname)"
                        ")::regclass) "
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
                "comment": str(r[2]) if r[2] else None,
                "metadata": {},
            }
            for r in rows
        ]

    def list_stored_procedures(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # PG 11+ distinguishes procedures (prokind='p') from functions
        # ('f') and aggregates ('a'). Older PG only has functions, so
        # this returns [] there — capability flag is True regardless.
        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text(
                        "SELECT p.proname, pg_get_functiondef(p.oid), "
                        "obj_description(p.oid, 'pg_proc') "
                        "FROM pg_proc p "
                        "JOIN pg_namespace n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = :schema AND p.prokind = 'p' "
                        "ORDER BY p.proname"
                    ),
                    {"schema": schema},
                ).fetchall()
            except Exception:
                return []
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
        with engine.connect() as conn:
            try:
                rows = conn.execute(
                    text(
                        "SELECT p.proname, pg_get_functiondef(p.oid), "
                        "obj_description(p.oid, 'pg_proc'), l.lanname "
                        "FROM pg_proc p "
                        "JOIN pg_namespace n ON n.oid = p.pronamespace "
                        "JOIN pg_language l ON l.oid = p.prolang "
                        "WHERE n.nspname = :schema AND p.prokind = 'f' "
                        "ORDER BY p.proname"
                    ),
                    {"schema": schema},
                ).fetchall()
            except Exception:
                # Pre-PG 11: prokind doesn't exist; fall back to all
                # functions in the schema.
                rows = conn.execute(
                    text(
                        "SELECT p.proname, NULL, obj_description(p.oid, 'pg_proc'), 'sql' "
                        "FROM pg_proc p "
                        "JOIN pg_namespace n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = :schema "
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

    def list_sequences(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT c.relname, "
                    "  obj_description(c.oid, 'pg_class') AS comment, "
                    "  s.seqstart, s.seqincrement, s.seqmin, s.seqmax, s.seqcycle "
                    "FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "LEFT JOIN pg_sequence s ON s.seqrelid = c.oid "
                    "WHERE n.nspname = :schema AND c.relkind = 'S' "
                    "ORDER BY c.relname"
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

    def list_triggers(
        self, engine: Engine, schema: str, table: str | None = None
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT t.tgname, c.relname AS table_name, "
            "  pg_get_triggerdef(t.oid) AS def, t.tgenabled "
            "FROM pg_trigger t "
            "JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :schema AND NOT t.tgisinternal"
        )
        params: dict[str, Any] = {"schema": schema}
        if table:
            sql += " AND c.relname = :table"
            params["table"] = table
        sql += " ORDER BY t.tgname"
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "trigger",
                "definition": str(r[2]) if r[2] else None,
                "comment": None,
                "metadata": {
                    "table": str(r[1]),
                    "enabled": str(r[3]) != "D",  # 'D' = disabled
                },
            }
            for r in rows
        ]

    def list_user_defined_types(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # typtype: 'c'=composite, 'd'=domain, 'e'=enum, 'r'=range,
        # 'm'=multirange. Skip table-row composites by filtering on
        # typrelid=0 OR the relation isn't a table.
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT t.typname, t.typtype, obj_description(t.oid, 'pg_type') "
                    "FROM pg_type t "
                    "JOIN pg_namespace n ON n.oid = t.typnamespace "
                    "WHERE n.nspname = :schema "
                    "AND t.typtype IN ('c','d','e','r','m') "
                    "AND (t.typrelid = 0 OR (SELECT relkind FROM pg_class WHERE oid = t.typrelid) != 'r') "
                    "ORDER BY t.typname"
                ),
                {"schema": schema},
            ).fetchall()
        type_map = {"c": "composite", "d": "domain", "e": "enum", "r": "range", "m": "multirange"}
        return [
            {
                "name": str(r[0]),
                "type": type_map.get(str(r[1]), str(r[1])),
                "definition": None,
                "comment": str(r[2]) if r[2] else None,
                "metadata": {},
            }
            for r in rows
        ]

    # ── Analytics metadata ────────────────────────────────────────────────

    def get_analytics_metadata(self, engine: Engine, schema: str, table: str) -> dict[str, Any]:
        """PostgreSQL analytics metadata.

        Pulls partition info from ``pg_partitioned_table`` /
        ``pg_inherits``, indexes from ``pg_indexes``, on-disk size
        from ``pg_relation_size`` (including TOAST + indexes), table
        type from ``pg_class.relkind``, and freshness from
        ``pg_stat_user_tables.last_*``. Each query is wrapped so a
        single permission failure doesn't drop the whole result; the
        affected field is left empty and a warning is recorded.
        """
        out: dict[str, Any] = {}
        warnings: list[str] = []

        with engine.connect() as conn:
            # ── partition_keys / partition_strategy ──
            try:
                row = conn.execute(
                    text(
                        """
                        SELECT
                            pp.partstrat,
                            (
                                SELECT array_agg(att.attname ORDER BY ord.idx)
                                FROM unnest(pp.partattrs) WITH ORDINALITY AS ord(attnum, idx)
                                JOIN pg_attribute att
                                  ON att.attrelid = pp.partrelid
                                 AND att.attnum   = ord.attnum
                            ) AS partition_columns
                        FROM pg_partitioned_table pp
                        JOIN pg_class c ON c.oid = pp.partrelid
                        JOIN pg_namespace ns ON ns.oid = c.relnamespace
                        WHERE ns.nspname = :schema AND c.relname = :table
                        """
                    ),
                    {"schema": schema, "table": table},
                ).fetchone()
                if row and row[0]:
                    strat_map = {"r": "range", "l": "list", "h": "hash"}
                    out["partition_strategy"] = strat_map.get(str(row[0]), str(row[0]))
                    out["partition_keys"] = [str(c) for c in (row[1] or []) if c]
            except Exception as exc:
                warnings.append(f"partition info: {exc}")

            # ── indexes ──
            try:
                rows = conn.execute(
                    text(
                        """
                        SELECT indexname, indexdef, indisunique
                        FROM pg_indexes idx
                        LEFT JOIN pg_class ic ON ic.relname = idx.indexname
                        LEFT JOIN pg_index pgi ON pgi.indexrelid = ic.oid
                        WHERE idx.schemaname = :schema AND idx.tablename = :table
                        ORDER BY indexname
                        """
                    ),
                    {"schema": schema, "table": table},
                ).fetchall()
                indexes: list[dict[str, Any]] = []
                for r in rows:
                    name = str(r[0] or "")
                    indexdef = str(r[1] or "")
                    unique = bool(r[2]) if r[2] is not None else "UNIQUE" in indexdef.upper()
                    # Extract column list from the indexdef tail (... USING btree (col1, col2)).
                    cols: list[str] = []
                    if "(" in indexdef and indexdef.rstrip().endswith(")"):
                        col_str = indexdef.rsplit("(", 1)[1].rstrip(")")
                        cols = [
                            c.strip().strip('"').split()[0] for c in col_str.split(",") if c.strip()
                        ]
                    indexes.append({"name": name, "columns": cols, "unique": unique})
                out["indexes"] = indexes
            except Exception as exc:
                warnings.append(f"indexes: {exc}")

            # ── storage_bytes (table + TOAST + indexes) ──
            try:
                fqn = self.fully_qualified_name(schema, table)
                size_bytes = conn.execute(
                    text(f"SELECT pg_total_relation_size('{fqn}'::regclass)")
                ).scalar()
                if size_bytes is not None:
                    out["storage_bytes"] = int(size_bytes)
            except Exception as exc:
                warnings.append(f"storage size: {exc}")

            # ── last_modified (best-effort: max of last_analyze / vacuum / autoanalyze) ──
            try:
                row = conn.execute(
                    text(
                        """
                        SELECT GREATEST(
                            COALESCE(last_analyze, '1970-01-01'::timestamptz),
                            COALESCE(last_autoanalyze, '1970-01-01'::timestamptz),
                            COALESCE(last_vacuum, '1970-01-01'::timestamptz),
                            COALESCE(last_autovacuum, '1970-01-01'::timestamptz)
                        ) AS lm
                        FROM pg_stat_user_tables
                        WHERE schemaname = :schema AND relname = :table
                        """
                    ),
                    {"schema": schema, "table": table},
                ).fetchone()
                if row and row[0] is not None and str(row[0]) != "1970-01-01 00:00:00+00:00":
                    out["last_modified"] = str(row[0])
            except Exception as exc:
                warnings.append(f"last_modified: {exc}")

            # ── table_type from pg_class.relkind ──
            try:
                row = conn.execute(
                    text(
                        """
                        SELECT c.relkind
                        FROM pg_class c
                        JOIN pg_namespace ns ON ns.oid = c.relnamespace
                        WHERE ns.nspname = :schema AND c.relname = :table
                        LIMIT 1
                        """
                    ),
                    {"schema": schema, "table": table},
                ).fetchone()
                if row:
                    kind_map = {
                        "r": "managed",
                        "v": "view",
                        "m": "materialized_view",
                        "f": "foreign",
                        "t": "toast",
                        "p": "partitioned",
                    }
                    out["table_type"] = kind_map.get(str(row[0]), str(row[0]))
            except Exception as exc:
                warnings.append(f"table_type: {exc}")

        # PostgreSQL is always native heap storage (or partitioned heap);
        # no Parquet / Delta / Iceberg.
        out.setdefault("storage_format", "native")

        if warnings:
            out["warnings"] = warnings
        return out

    # ── Comment writing ───────────────────────────────────────────────────

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
