"""Oracle Database backend adapter.

Driven by python-oracledb in thin mode (no Instant Client needed).
Connection accepts either ``service_name`` (preferred for Oracle Cloud
/ RAC / modern setups) or falls back to ``database`` as a SID.

Object types exposed beyond the standard tables/views/columns:

* Materialized views (``ALL_MVIEWS``) — Oracle has first-class support.
* Stored procedures and functions (``ALL_PROCEDURES`` filtered by
  ``OBJECT_TYPE``).
* Packages (``ALL_OBJECTS WHERE OBJECT_TYPE='PACKAGE'``) — Oracle
  groups related procedures and functions into named packages, a
  feature with no equivalent on most other backends.
* Triggers (``ALL_TRIGGERS``).
* Sequences (``ALL_SEQUENCES``).
* Synonyms (``ALL_SYNONYMS``) — Oracle's named aliases for objects.
* User-defined types (``ALL_TYPES``) — composite, collection, REF
  types.
* Partition info, tablespace, and storage size in
  :meth:`get_analytics_metadata`.

Schema = user in Oracle. There is no ``COMMENT ON SCHEMA`` and no
database-level COMMENT — both raise ``unsupported``.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from amx.db.adapters.base import BackendCapabilities, DatabaseAdapter


# Oracle ships with a long list of system schemas. Filtering them out
# of the schema picker is the difference between "12 user schemas" and
# "120 schemas, 90% of which are XDB / APEX / OLAP support".
_ORACLE_SYSTEM_SCHEMAS = frozenset(
    {
        "SYS",
        "SYSTEM",
        "OUTLN",
        "MDSYS",
        "CTXSYS",
        "XDB",
        "WMSYS",
        "OLAPSYS",
        "DBSNMP",
        "GSMADMIN_INTERNAL",
        "LBACSYS",
        "OJVMSYS",
        "ORDSYS",
        "ORDDATA",
        "ORDPLUGINS",
        "SI_INFORMTN_SCHEMA",
        "DVF",
        "DVSYS",
        "AUDSYS",
        "APPQOSSYS",
        "DIP",
        "ORACLE_OCM",
        "ANONYMOUS",
        "REMOTE_SCHEDULER_AGENT",
        "SYSBACKUP",
        "SYSDG",
        "SYSKM",
        "SYSRAC",
        "SYS$UMF",
        "GGSYS",
        "GSMCATUSER",
        "GSMUSER",
        "GSMROOTUSER",
        "MDDATA",
        "FLOWS_FILES",
    }
)


class OracleAdapter(DatabaseAdapter):
    name = "oracle"
    capabilities = BackendCapabilities(
        database_comments=False,
        schema_comments=False,
        materialized_view_comments=True,
        materialized_views=True,
        relationships=True,
        row_count_stats=True,
        stored_procedures=True,
        functions=True,
        sequences=True,
        triggers=True,
        packages=True,
        synonyms=True,
        user_defined_types=True,
        comment_asset_keywords=frozenset({"TABLE", "VIEW", "MATERIALIZED VIEW"}),
    )

    def create_engine(self) -> Engine:
        return create_engine(self.cfg.url, pool_pre_ping=True)

    def actionable_profile_error(self, exc: Exception) -> str | None:
        msg = str(exc).lower()
        if "ora-01017" in msg or "invalid username/password" in msg:
            return (
                "Oracle refused the credentials (ORA-01017). Check the "
                "username/password and the case-sensitivity setting on the "
                "server (12c+ defaults to case-sensitive passwords)."
            )
        if "ora-12541" in msg or "no listener" in msg:
            return (
                "Oracle listener is not reachable (ORA-12541). Check the "
                "host/port and that the listener is running on the server."
            )
        if "ora-12514" in msg or "service" in msg and "not currently known" in msg:
            return (
                "Oracle listener doesn't know the service (ORA-12514). Set "
                "`service_name` on the profile to match the value from "
                "`lsnrctl status` on the server."
            )
        if "ora-00942" in msg or "table or view does not exist" in msg:
            return (
                "Oracle reported that a referenced table or view doesn't "
                "exist (ORA-00942). The user may need SELECT_CATALOG_ROLE "
                "for full introspection, or the object lives in a schema "
                "the user can't see."
            )
        if "ora-01918" in msg or "user does not exist" in msg:
            return (
                "Oracle reported that the user doesn't exist (ORA-01918). "
                "Check the username — Oracle stores it in upper case unless "
                "quoted at create time."
            )
        return None

    def system_schemas(self) -> frozenset[str]:
        return _ORACLE_SYSTEM_SCHEMAS

    def list_databases(self, engine: Engine) -> list[str]:
        # Oracle's "schema = user" model means there's no separate
        # database list; surface the user-visible schemas (owners with
        # at least one table) as the closest analogue. The system-schema
        # IN list is built from a static frozenset, not user input, so
        # the literal interpolation is safe.
        in_list = ", ".join(f"'{s}'" for s in sorted(_ORACLE_SYSTEM_SCHEMAS))
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"SELECT DISTINCT OWNER FROM ALL_TABLES "
                    f"WHERE OWNER NOT IN ({in_list}) "
                    f"ORDER BY OWNER"
                )
            ).fetchall()
        return [str(r[0]) for r in rows]

    # ── Column profiling ──────────────────────────────────────────────────

    def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
        # Oracle's TO_CHAR coerces NUMBER/DATE/TIMESTAMP cleanly. CLOB
        # and LONG types need DBMS_LOB.SUBSTR but those are rare in
        # day-to-day OLTP — we accept the failure mode and let the
        # profiler skip those columns.
        return (
            f"SELECT "
            f"  SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS null_cnt, "
            f"  COUNT(DISTINCT {quoted_col}) AS dist_cnt, "
            f"  TO_CHAR(MIN({quoted_col})) AS min_val, "
            f"  TO_CHAR(MAX({quoted_col})) AS max_val "
            f"FROM {fqn}"
        )

    def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
        # ROWNUM is the cross-version Oracle equivalent of LIMIT.
        return (
            f"SELECT * FROM ("
            f"  SELECT DISTINCT TO_CHAR({quoted_col}) AS v FROM {fqn} "
            f"  WHERE {quoted_col} IS NOT NULL"
            f") WHERE ROWNUM <= :lim"
        )

    # ── Table stats ───────────────────────────────────────────────────────

    def get_table_stats(self, engine: Engine, schema: str, table: str) -> dict[str, int]:
        # NUM_ROWS is from the optimiser stats, refreshed by ANALYZE or
        # DBMS_STATS. It can be stale — surface it anyway, the search
        # agent should treat it as an estimate.
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT NUM_ROWS FROM ALL_TABLES "
                    "WHERE OWNER = :owner AND TABLE_NAME = :tname"
                ),
                {"owner": schema.upper(), "tname": table.upper()},
            ).fetchone()
        n = int(row[0]) if row and row[0] is not None else 0
        return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": n}

    def stats_label(self) -> str:
        return "ALL_TABLES.NUM_ROWS (optimiser stats; may be stale)"

    # ── Materialized views ────────────────────────────────────────────────

    def list_materialized_views(self, engine: Engine, schema: str) -> list[str]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT MVIEW_NAME FROM ALL_MVIEWS "
                    "WHERE OWNER = :owner ORDER BY MVIEW_NAME"
                ),
                {"owner": schema.upper()},
            ).fetchall()
        return [str(r[0]) for r in rows]

    # ── Incoming foreign keys ─────────────────────────────────────────────

    def get_incoming_foreign_keys(
        self, engine: Engine, schema: str, table: str
    ) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        src_cc.OWNER     AS source_schema,
                        src_cc.TABLE_NAME AS source_table,
                        src_cc.COLUMN_NAME AS source_column,
                        tgt_cc.COLUMN_NAME AS target_column
                    FROM ALL_CONSTRAINTS src
                    JOIN ALL_CONS_COLUMNS src_cc
                         ON src_cc.OWNER = src.OWNER
                        AND src_cc.CONSTRAINT_NAME = src.CONSTRAINT_NAME
                    JOIN ALL_CONS_COLUMNS tgt_cc
                         ON tgt_cc.OWNER = src.R_OWNER
                        AND tgt_cc.CONSTRAINT_NAME = src.R_CONSTRAINT_NAME
                        AND tgt_cc.POSITION = src_cc.POSITION
                    WHERE src.CONSTRAINT_TYPE = 'R'
                      AND tgt_cc.OWNER = :owner
                      AND tgt_cc.TABLE_NAME = :tname
                    ORDER BY src_cc.OWNER, src_cc.TABLE_NAME, src_cc.COLUMN_NAME
                    """
                ),
                {"owner": schema.upper(), "tname": table.upper()},
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

    def list_stored_procedures(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # ALL_PROCEDURES rows are duplicated when a procedure lives
        # inside a package — restrict to standalone procedures
        # (OBJECT_TYPE='PROCEDURE', PROCEDURE_NAME IS NULL).
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT OBJECT_NAME "
                    "FROM ALL_PROCEDURES "
                    "WHERE OWNER = :owner AND OBJECT_TYPE = 'PROCEDURE' "
                    "AND PROCEDURE_NAME IS NULL "
                    "ORDER BY OBJECT_NAME"
                ),
                {"owner": schema.upper()},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "procedure",
                "definition": None,
                "comment": None,
                "metadata": {},
            }
            for r in rows
        ]

    def list_functions(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT OBJECT_NAME "
                    "FROM ALL_PROCEDURES "
                    "WHERE OWNER = :owner AND OBJECT_TYPE = 'FUNCTION' "
                    "AND PROCEDURE_NAME IS NULL "
                    "ORDER BY OBJECT_NAME"
                ),
                {"owner": schema.upper()},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "function",
                "definition": None,
                "comment": None,
                "metadata": {},
            }
            for r in rows
        ]

    def list_packages(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        # Packages are first-class — list them with their member
        # procedure/function names rolled into the metadata so consumers
        # get the contained-procedure surface for free.
        with engine.connect() as conn:
            pkg_rows = conn.execute(
                text(
                    "SELECT OBJECT_NAME, STATUS "
                    "FROM ALL_OBJECTS "
                    "WHERE OWNER = :owner AND OBJECT_TYPE = 'PACKAGE' "
                    "ORDER BY OBJECT_NAME"
                ),
                {"owner": schema.upper()},
            ).fetchall()
            members_by_pkg: dict[str, list[dict[str, str]]] = {}
            if pkg_rows:
                pkg_names = [str(r[0]) for r in pkg_rows]
                # ALL_PROCEDURES rows where OBJECT_TYPE=PACKAGE and
                # PROCEDURE_NAME IS NOT NULL list the package members.
                for pkg_name in pkg_names:
                    member_rows = conn.execute(
                        text(
                            "SELECT PROCEDURE_NAME "
                            "FROM ALL_PROCEDURES "
                            "WHERE OWNER = :owner AND OBJECT_NAME = :pkg "
                            "AND PROCEDURE_NAME IS NOT NULL "
                            "ORDER BY PROCEDURE_NAME"
                        ),
                        {"owner": schema.upper(), "pkg": pkg_name},
                    ).fetchall()
                    members_by_pkg[pkg_name] = [
                        {"name": str(m[0])} for m in member_rows
                    ]
        return [
            {
                "name": str(r[0]),
                "type": "package",
                "definition": None,
                "comment": None,
                "metadata": {
                    "status": str(r[1]),
                    "members": members_by_pkg.get(str(r[0]), []),
                },
            }
            for r in pkg_rows
        ]

    def list_sequences(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, "
                    "CYCLE_FLAG, CACHE_SIZE, LAST_NUMBER "
                    "FROM ALL_SEQUENCES "
                    "WHERE SEQUENCE_OWNER = :owner "
                    "ORDER BY SEQUENCE_NAME"
                ),
                {"owner": schema.upper()},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "sequence",
                "definition": None,
                "comment": None,
                "metadata": {
                    "min": int(r[1]) if r[1] is not None else None,
                    "max": int(r[2]) if r[2] is not None else None,
                    "increment": int(r[3]) if r[3] is not None else None,
                    "cycle": str(r[4]) == "Y",
                    "cache_size": int(r[5]) if r[5] is not None else None,
                    "last_number": int(r[6]) if r[6] is not None else None,
                },
            }
            for r in rows
        ]

    def list_triggers(
        self, engine: Engine, schema: str, table: str | None = None
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT TRIGGER_NAME, TRIGGER_TYPE, TRIGGERING_EVENT, "
            "TABLE_NAME, STATUS, TRIGGER_BODY "
            "FROM ALL_TRIGGERS "
            "WHERE OWNER = :owner"
        )
        params: dict[str, Any] = {"owner": schema.upper()}
        if table:
            sql += " AND TABLE_NAME = :tname"
            params["tname"] = table.upper()
        sql += " ORDER BY TRIGGER_NAME"
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "trigger",
                "definition": str(r[5]) if r[5] else None,
                "comment": None,
                "metadata": {
                    "trigger_type": str(r[1]) if r[1] else None,
                    "event": str(r[2]) if r[2] else None,
                    "table": str(r[3]) if r[3] else None,
                    "status": str(r[4]) if r[4] else None,
                },
            }
            for r in rows
        ]

    def list_synonyms(self, engine: Engine, schema: str) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT SYNONYM_NAME, TABLE_OWNER, TABLE_NAME, DB_LINK "
                    "FROM ALL_SYNONYMS "
                    "WHERE OWNER = :owner "
                    "ORDER BY SYNONYM_NAME"
                ),
                {"owner": schema.upper()},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": "synonym",
                "definition": None,
                "comment": None,
                "metadata": {
                    "target_owner": str(r[1]) if r[1] else None,
                    "target": str(r[2]) if r[2] else None,
                    "db_link": str(r[3]) if r[3] else None,
                },
            }
            for r in rows
        ]

    def list_user_defined_types(
        self, engine: Engine, schema: str
    ) -> list[dict[str, Any]]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT TYPE_NAME, TYPECODE, ATTRIBUTES "
                    "FROM ALL_TYPES "
                    "WHERE OWNER = :owner "
                    "AND PREDEFINED = 'NO' "
                    "ORDER BY TYPE_NAME"
                ),
                {"owner": schema.upper()},
            ).fetchall()
        return [
            {
                "name": str(r[0]),
                "type": str(r[1]).lower() if r[1] else "udt",
                "definition": None,
                "comment": None,
                "metadata": {
                    "attribute_count": int(r[2]) if r[2] is not None else None,
                },
            }
            for r in rows
        ]

    # ── Analytics metadata ────────────────────────────────────────────────

    def get_analytics_metadata(self, engine: Engine, schema: str, table: str) -> dict[str, Any]:
        out: dict[str, Any] = {}
        warnings: list[str] = []

        with engine.connect() as conn:
            try:
                row = conn.execute(
                    text(
                        "SELECT TABLESPACE_NAME, NUM_ROWS, LAST_ANALYZED, "
                        "TEMPORARY, EXTERNAL "
                        "FROM ALL_TABLES "
                        "WHERE OWNER = :owner AND TABLE_NAME = :tname"
                    ),
                    {"owner": schema.upper(), "tname": table.upper()},
                ).fetchone()
                if row:
                    if row[2]:
                        out["last_modified"] = str(row[2])
                    if str(row[3]) == "Y":
                        out["table_type"] = "temporary"
                    elif str(row[4]) == "YES":
                        out["table_type"] = "external"
                    else:
                        out["table_type"] = "managed"
                    out["storage_format"] = "native"
            except Exception as exc:
                warnings.append(f"table info: {exc}")

            # Partition info
            try:
                rows = conn.execute(
                    text(
                        "SELECT PARTITIONING_TYPE, "
                        "(SELECT LISTAGG(COLUMN_NAME, ',') WITHIN GROUP (ORDER BY COLUMN_POSITION) "
                        " FROM ALL_PART_KEY_COLUMNS k "
                        " WHERE k.OWNER = pt.OWNER AND k.NAME = pt.TABLE_NAME) AS pkeys "
                        "FROM ALL_PART_TABLES pt "
                        "WHERE OWNER = :owner AND TABLE_NAME = :tname"
                    ),
                    {"owner": schema.upper(), "tname": table.upper()},
                ).fetchone()
                if rows and rows[0]:
                    out["partition_strategy"] = str(rows[0]).lower()
                    if rows[1]:
                        out["partition_keys"] = [
                            c.strip() for c in str(rows[1]).split(",") if c.strip()
                        ]
            except Exception as exc:
                warnings.append(f"partitions: {exc}")

            # Storage size
            try:
                row = conn.execute(
                    text(
                        "SELECT SUM(BYTES) FROM DBA_SEGMENTS "
                        "WHERE OWNER = :owner AND SEGMENT_NAME = :tname"
                    ),
                    {"owner": schema.upper(), "tname": table.upper()},
                ).fetchone()
                if row and row[0] is not None:
                    out["storage_bytes"] = int(row[0])
            except Exception as exc:
                # DBA_SEGMENTS often requires SELECT_CATALOG_ROLE — non-fatal.
                warnings.append(f"storage size (DBA_SEGMENTS access?): {exc}")

            # Indexes
            try:
                rows = conn.execute(
                    text(
                        "SELECT i.INDEX_NAME, i.UNIQUENESS, ic.COLUMN_NAME, ic.COLUMN_POSITION "
                        "FROM ALL_INDEXES i "
                        "JOIN ALL_IND_COLUMNS ic "
                        "  ON ic.INDEX_OWNER = i.OWNER AND ic.INDEX_NAME = i.INDEX_NAME "
                        "WHERE i.TABLE_OWNER = :owner AND i.TABLE_NAME = :tname "
                        "ORDER BY i.INDEX_NAME, ic.COLUMN_POSITION"
                    ),
                    {"owner": schema.upper(), "tname": table.upper()},
                ).fetchall()
                idx_map: dict[str, dict[str, Any]] = {}
                for r in rows:
                    name = str(r[0])
                    bucket = idx_map.setdefault(
                        name, {"name": name, "columns": [], "unique": str(r[1]) == "UNIQUE"}
                    )
                    bucket["columns"].append(str(r[2]))
                if idx_map:
                    out["indexes"] = list(idx_map.values())
            except Exception as exc:
                warnings.append(f"indexes: {exc}")

        if warnings:
            out["warnings"] = warnings
        return out

    # ── Comment writing ───────────────────────────────────────────────────

    def set_table_comment_sql(self, schema: str, table: str, asset_keyword: str) -> str:
        # Oracle's COMMENT statement: ``COMMENT ON TABLE <fqn> IS '...'``.
        # MATERIALIZED VIEW is also accepted.
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON {asset_keyword} {fqn} IS :cmt"

    def set_column_comment_sql(self, schema: str, table: str, column: str) -> str:
        fqn = self.fully_qualified_name(schema, table)
        return f"COMMENT ON COLUMN {fqn}.{self.quote_identifier(column)} IS :cmt"

    def set_schema_comment_sql(self, schema: str) -> str:
        # Oracle has no schema-level COMMENT — schema = user.
        raise self.unsupported("set_schema_comment_sql")

    def set_database_comment_sql(self) -> str:
        raise self.unsupported("set_database_comment_sql")

    def comment_sql_with_params(
        self, stmt_template: str, comment: str
    ) -> tuple[str, dict[str, Any]]:
        # Oracle's COMMENT ON DDL doesn't accept binds in every driver
        # mode — inline the literal to be safe.
        literal = self.quote_literal(comment)
        return stmt_template.replace(":cmt", literal), {}
