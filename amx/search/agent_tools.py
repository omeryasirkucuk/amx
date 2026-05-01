"""Tool definitions for the tool-calling ``/ask`` agent.

The tool-calling agent (``amx/search/tool_agent.py``) hands the LLM a small
fixed set of tools that wrap the existing catalog / live-DB / SchemaExplorer
infrastructure. The LLM picks which tool to call (and with what arguments)
to answer the user's question — instead of us trying to classify the
question via regex up-front.

Each tool is described by:
* ``name``      — JSON-schema friendly identifier (snake_case) the LLM emits.
* ``schema``    — OpenAI-compatible function-calling JSON schema.
* ``run(args)`` — Python callable invoked when the LLM picks this tool.

The ``ToolBox`` class holds the catalog/DB references and exposes ``schemas``
(passed to the LLM) plus ``invoke(name, json_args_string)``.
"""

from __future__ import annotations

import json
from typing import Any, Callable

from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector, ProfilingError
from amx.search.catalog import SearchCatalog


class _ToolError(RuntimeError):
    """Raised by a tool when it can't fulfil the request — surfaced verbatim
    to the LLM so it can adjust and try a different tool."""


def _safe_json(value: Any, *, max_len: int = 6000) -> str:
    """Serialize a tool result; truncate so the prompt stays manageable."""
    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:  # pragma: no cover - JSON of catalog rows always works
        text = str(value)
    if len(text) > max_len:
        text = text[: max_len - 18] + "...<truncated>"
    return text


class ToolBox:
    """Concrete tool implementations the agent loop dispatches into."""

    def __init__(
        self,
        cfg: AMXConfig,
        catalog: SearchCatalog,
        *,
        db_factory: Callable[[], DatabaseConnector] | None = None,
    ) -> None:
        self.cfg = cfg
        self.catalog = catalog
        self.db_profile = cfg.active_db_profile or "default"
        self._db_factory = db_factory or (lambda: DatabaseConnector(cfg.db))
        # Only build the live DB connector lazily — many tools never need it.
        self._db: DatabaseConnector | None = None

    # ------------------------------------------------------------------ helpers
    def _live_db(self) -> DatabaseConnector:
        if self._db is None:
            self._db = self._db_factory()
        return self._db

    def close(self) -> None:
        """Dispose the live DB connector. Each ``/ask`` question instantiates a
        fresh ``ToolBox``; without this call the SQLAlchemy engine + connection
        pool stay alive across REPL turns, leaking file descriptors until
        macOS / Linux ulimit kicks in (the user-reported
        ``OSError: [Errno 24] Too many open files`` after several turns).
        """
        if self._db is not None:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None

    def __enter__(self) -> "ToolBox":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------------ schemas
    @staticmethod
    def schemas() -> list[dict[str, Any]]:
        """JSON schemas for every tool — passed to the LLM as the ``tools``
        parameter. Names are stable; argument names match the Python kwargs
        of the corresponding ``ToolBox`` method."""
        return [
            {
                "type": "function",
                "function": {
                    "name": "list_schemas",
                    "description": (
                        "Return the list of schema names (namespaces) visible in the active "
                        "database. Use this when the user asks 'which schemas do we have?', "
                        "'what schemas exist?', 'sap_test ne tür bir şema?', or as a discovery "
                        "step before drilling into one specific schema."
                    ),
                    "parameters": {"type": "object", "properties": {}, "required": []},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_tables_in_schema",
                    "description": (
                        "Return the tables, views, and materialized views inside a given "
                        "schema. Use this when the user asks 'what tables are under sap_test?', "
                        "'list all tables in sap_s6p', or to disambiguate a bare table name."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "schema": {
                                "type": "string",
                                "description": "Exact schema name. Case-insensitive.",
                            },
                        },
                        "required": ["schema"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "find_table_by_name",
                    "description": (
                        "Return every (schema, table) pair where the given table name exists "
                        "exactly. Use this to find which schema contains a table the user "
                        "named ('which schema have vbrk table?', 'where is adrc?'). When the "
                        "name lives in multiple schemas, the result lets you ask the user to "
                        "disambiguate."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "Exact table name. Case-insensitive.",
                            },
                        },
                        "required": ["name"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "describe_table",
                    "description": (
                        "Return the table comment, column count, and column metadata "
                        "(name + dtype + comment) for a fully-qualified table. Use this to "
                        "answer 'what's the vbrk table?', 'describe sap_s6p.adrc', "
                        "'X tablosunda hangi kolonlar var?'."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "schema": {"type": "string", "description": "Schema name."},
                            "table": {"type": "string", "description": "Table name."},
                        },
                        "required": ["schema", "table"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_tables_by_concept",
                    "description": (
                        "Semantic / lexical search over the catalog for tables whose names or "
                        "comments relate to a business concept (pricing, customer, address, "
                        "billing, ...). Use this for 'tables about pricing', "
                        "'müşteri ile ilgili tablolar', 'find tables that store invoices'."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "concept": {
                                "type": "string",
                                "description": "The business concept to search for.",
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Max rows to return (default 10).",
                                "default": 10,
                            },
                        },
                        "required": ["concept"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_columns_by_concept",
                    "description": (
                        "Lexical search over indexed catalog columns. Use this for 'where is "
                        "the customer_id column?', 'which tables have an email column?', "
                        "'address related columns'."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "concept": {
                                "type": "string",
                                "description": "Column name fragment or concept term.",
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Max rows to return (default 10).",
                                "default": 10,
                            },
                        },
                        "required": ["concept"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_join_candidates",
                    "description": (
                        "Return likely join columns between two tables (verified foreign keys "
                        "first, semantic-similarity candidates after). Use this for "
                        "'how do X and Y join?', 'X ile Y nasıl birleşir?'."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "left": {
                                "type": "string",
                                "description": "First table as schema.table.",
                            },
                            "right": {
                                "type": "string",
                                "description": "Second table as schema.table.",
                            },
                        },
                        "required": ["left", "right"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_databases",
                    "description": (
                        "List the databases the agent currently has access to (one per active "
                        "DB profile). Use this only when the user explicitly asks 'which "
                        "databases do we have?' / 'hangi veritabanları var?'."
                    ),
                    "parameters": {"type": "object", "properties": {}, "required": []},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "find_columns_by_dtype",
                    "description": (
                        "Return columns whose dtype matches the given SQL data type "
                        "('boolean', 'int', 'integer', 'text', 'date', 'timestamp', 'numeric', "
                        "etc.). Use this for 'which tables have boolean columns?', "
                        "'all date columns', 'tables with bigint primary keys'. Matches by "
                        "dtype FAMILY when possible (e.g. 'int' covers BIGINT/INTEGER/SMALLINT)."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "dtype": {
                                "type": "string",
                                "description": "Data type token. Case-insensitive.",
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Max rows (default 30).",
                                "default": 30,
                            },
                        },
                        "required": ["dtype"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "find_assets_missing_comment",
                    "description": (
                        "Return tables and/or columns that have NO comment in the live "
                        "database (queries the DB directly, NOT the catalog). Use this for "
                        "'are there any tables without a description?', 'which tables are "
                        "missing comments?', 'açıklaması olmayan tablolar', 'eksik comment'. "
                        "Catalog data may be stale right after a /run-apply, so always use "
                        "this live-DB check for coverage questions instead of the concept "
                        "search tools. By default, system / extension assets (e.g. "
                        "pg_stat_statements, pg_statio_*) are filtered out — same rule "
                        "the /run flow uses; AMX never describes those. Set "
                        "``include_system=True`` only if the user EXPLICITLY asks about "
                        "system tables (e.g. 'including system views')."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "schema": {
                                "type": "string",
                                "description": "Optional schema filter. Omit to scan every schema.",
                            },
                            "scope": {
                                "type": "string",
                                "description": (
                                    "What to check: 'tables' (table-level comments only), "
                                    "'columns' (column-level only), or 'both' (default)."
                                ),
                                "default": "both",
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Max rows per scope (default 50).",
                                "default": 50,
                            },
                            "include_system": {
                                "type": "boolean",
                                "description": (
                                    "Include PostgreSQL extension / system assets like "
                                    "pg_stat_statements. Default false — only set true "
                                    "when the user explicitly asks about system tables."
                                ),
                                "default": False,
                            },
                        },
                        "required": [],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "find_joinable_tables",
                    "description": (
                        "Given ONE table, return the tables it can be joined with — verified "
                        "foreign keys first, then semantic-similarity candidates. Use this for "
                        "'which tables can I join with adr6?', 'X ile birleşebilecek tablolar', "
                        "'find tables related to vbrk'. Different from get_join_candidates "
                        "which needs both sides upfront."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table": {
                                "type": "string",
                                "description": "Table as schema.table or just table_name (we'll resolve via find_table_by_name first).",
                            },
                        },
                        "required": ["table"],
                    },
                },
            },
        ]

    # ------------------------------------------------------------------ invoke
    def invoke(self, name: str, raw_arguments: str) -> str:
        """Dispatch a tool by name; return the result as a JSON string for the
        LLM. All tools return a string for direct embedding in the next
        ``role=tool`` message."""
        try:
            args = json.loads(raw_arguments) if raw_arguments else {}
        except json.JSONDecodeError as exc:
            return _safe_json({"error": f"Invalid arguments JSON: {exc}"})
        try:
            handler = getattr(self, f"_tool_{name}", None)
            if handler is None:
                return _safe_json({"error": f"Unknown tool: {name}"})
            payload = handler(**args)
            return _safe_json(payload)
        except _ToolError as exc:
            return _safe_json({"error": str(exc)})
        except Exception as exc:  # surface to LLM but don't crash
            return _safe_json({"error": f"Tool {name} failed: {exc}"})

    # ------------------------------------------------------------------ implementations
    def _tool_list_schemas(self) -> dict[str, Any]:
        try:
            schemas = [str(s) for s in self._live_db().list_schemas()]
        except Exception as exc:
            raise _ToolError(f"Could not list schemas live: {exc}") from exc
        database = (
            self.cfg.db.database
            or self.cfg.db.catalog
            or self.cfg.db.project
            or "(active database)"
        )
        return {"database": database, "schemas": schemas, "count": len(schemas)}

    def _tool_list_tables_in_schema(self, schema: str) -> dict[str, Any]:
        target = (schema or "").strip()
        if not target:
            raise _ToolError("Argument 'schema' is required.")
        db = self._live_db()
        # Resolve case-insensitively against the live schema list.
        try:
            available = list(db.list_schemas())
        except Exception as exc:
            raise _ToolError(f"Could not list schemas: {exc}") from exc
        match = next((s for s in available if str(s).lower() == target.lower()), None)
        if match is None:
            return {
                "schema": target,
                "found": False,
                "available_schemas": [str(s) for s in available],
                "message": (
                    f"No schema named '{target}'. Available schemas: "
                    + ", ".join(str(s) for s in available)
                ),
            }
        items: list[dict[str, str]] = []
        try:
            if hasattr(db, "list_assets"):
                for name, kind in db.list_assets(match):
                    items.append({"name": str(name), "kind": str(kind)})
            else:
                for name in db.list_tables(match):
                    items.append({"name": str(name), "kind": "table"})
        except Exception as exc:
            raise _ToolError(f"Could not list tables in {match}: {exc}") from exc
        return {"schema": match, "found": True, "tables": items, "count": len(items)}

    def _tool_find_table_by_name(self, name: str) -> dict[str, Any]:
        target = (name or "").strip()
        if not target:
            raise _ToolError("Argument 'name' is required.")
        # Check both catalog (cheap) and live DB (broader).
        catalog_rows = self.catalog.find_tables_by_exact_name(self.db_profile, target, limit=20)
        catalog_paths: list[str] = []
        for row in catalog_rows:
            schema_name = str(row.get("schema_name") or "")
            table_name = str(row.get("table_name") or "")
            if schema_name and table_name:
                catalog_paths.append(f"{schema_name}.{table_name}")
        live_paths: list[str] = []
        try:
            db = self._live_db()
            for schema in db.list_schemas():
                # Prefer ``list_assets`` when available — single round trip per schema.
                if hasattr(db, "list_assets"):
                    asset_iter = ((str(n), str(k)) for n, k in db.list_assets(schema))
                else:
                    asset_iter = ((str(n), "table") for n in db.list_tables(schema))
                for asset_name, _kind in asset_iter:
                    if asset_name.lower() == target.lower():
                        live_paths.append(f"{schema}.{asset_name}")
        except Exception:
            # Live discovery is best-effort. Fall back to whatever the catalog had.
            pass
        merged = list(dict.fromkeys(catalog_paths + live_paths))
        return {
            "name": target,
            "matches": merged,
            "match_count": len(merged),
            "from_catalog": catalog_paths,
            "from_live_db": live_paths,
        }

    def _tool_describe_table(self, schema: str, table: str) -> dict[str, Any]:
        schema_name = (schema or "").strip()
        table_name = (table or "").strip()
        if not schema_name or not table_name:
            raise _ToolError("Both 'schema' and 'table' are required.")
        try:
            profile = self._live_db().profile_table(schema_name, table_name, sample_size=0)
        except ProfilingError as exc:
            return {
                "schema": schema_name,
                "table": table_name,
                "found": False,
                "error": str(exc),
            }
        except Exception as exc:
            return {
                "schema": schema_name,
                "table": table_name,
                "found": False,
                "error": str(exc),
            }
        cols = [
            {
                "name": c.name,
                "type": c.dtype,
                "nullable": bool(c.nullable),
                "comment": str(c.existing_comment or ""),
            }
            for c in profile.columns
        ]
        return {
            "schema": schema_name,
            "table": table_name,
            "found": True,
            "table_comment": str(profile.existing_comment or ""),
            "row_count": int(profile.row_count or 0),
            "column_count": len(cols),
            "columns": cols[:60],  # cap so the prompt doesn't explode on wide tables
        }

    def _tool_search_tables_by_concept(self, concept: str, limit: int = 10) -> dict[str, Any]:
        rows = self.catalog.search_tables(self.db_profile, concept or "", limit=int(limit))
        return {
            "concept": concept,
            "count": len(rows),
            "matches": [
                {
                    "schema": str(r.get("schema_name") or ""),
                    "table": str(r.get("table_name") or ""),
                    "score": float(r.get("rank_score") or r.get("score") or 0.0),
                    "description": str(r.get("effective_description") or ""),
                }
                for r in rows
            ],
        }

    def _tool_search_columns_by_concept(self, concept: str, limit: int = 10) -> dict[str, Any]:
        rows = self.catalog.search_columns(self.db_profile, concept or "", limit=int(limit))
        return {
            "concept": concept,
            "count": len(rows),
            "matches": [
                {
                    "schema": str(r.get("schema_name") or ""),
                    "table": str(r.get("table_name") or ""),
                    "column": str(r.get("column_name") or ""),
                    "score": float(r.get("rank_score") or r.get("score") or 0.0),
                    "description": str(r.get("effective_description") or ""),
                }
                for r in rows
            ],
        }

    def _tool_get_join_candidates(self, left: str, right: str) -> dict[str, Any]:
        verified = self.catalog.join_candidates(self.db_profile, left, right, limit=8)
        return {
            "left": left,
            "right": right,
            "candidates": [
                {
                    "left_column": str(r.get("left_column") or ""),
                    "right_column": str(r.get("right_column") or ""),
                    "type": str(r.get("relationship_type") or ""),
                    "score": float(r.get("score") or 0.0),
                }
                for r in verified
            ],
        }

    def _tool_find_assets_missing_comment(
        self,
        schema: str = "",
        scope: str = "both",
        limit: int = 50,
        include_system: bool = False,
    ) -> dict[str, Any]:
        """Return tables/columns with no comment, queried from the LIVE DB.

        The catalog can lag behind the live DB right after a ``/run-apply``,
        so coverage-type questions ("which tables are missing a comment?")
        must NOT come from ``catalog_entities`` rows — they must come from
        the source of truth. This tool calls ``get_table_comment`` /
        ``get_column_comments`` per asset and reports anything blank.

        System / telemetry assets (PostgreSQL extension views like
        ``pg_stat_statements``) are filtered out by default — the same
        filter the ``/run`` flow uses — because they aren't user data and
        AMX never describes them. Set ``include_system=True`` only when the
        user explicitly asks about system tables.
        """
        scope = (scope or "both").strip().lower()
        if scope not in {"tables", "columns", "both"}:
            scope = "both"
        limit = max(1, int(limit or 50))
        db = self._live_db()
        # Resolve schema list (case-insensitive when the user passed one).
        try:
            available = [str(s) for s in db.list_schemas()]
        except Exception as exc:
            raise _ToolError(f"Could not list schemas: {exc}") from exc
        target_schemas: list[str]
        target = (schema or "").strip()
        if target:
            match = next((s for s in available if s.lower() == target.lower()), None)
            if match is None:
                return {
                    "schema": target,
                    "found": False,
                    "available_schemas": available,
                    "message": (
                        f"No schema named '{target}'. Available schemas: "
                        + ", ".join(available)
                    ),
                    "tables_missing_comment": [],
                    "columns_missing_comment": [],
                }
            target_schemas = [match]
        else:
            target_schemas = available

        # Reuse the same system-asset filter the /run flow uses so /ask
        # doesn't surface PostgreSQL extension views (pg_stat_statements,
        # pg_statio_*, etc.) as gaps. Users can ask about system tables
        # explicitly via include_system=True if needed.
        try:
            from amx.services.analyze_scope import is_non_business_asset
        except Exception:
            def is_non_business_asset(_name: str) -> bool:  # type: ignore[misc]
                return False

        tables_missing: list[dict[str, str]] = []
        columns_missing: list[dict[str, str]] = []
        skipped_system: list[str] = []
        for sch in target_schemas:
            try:
                if hasattr(db, "list_assets"):
                    asset_iter = [(str(n), str(k)) for n, k in db.list_assets(sch)]
                else:
                    asset_iter = [(str(n), "table") for n in db.list_tables(sch)]
            except Exception:
                continue
            for asset_name, asset_kind in asset_iter:
                if not include_system and is_non_business_asset(asset_name):
                    skipped_system.append(f"{sch}.{asset_name}")
                    continue
                if scope in {"tables", "both"} and len(tables_missing) < limit:
                    try:
                        tcom = db.get_table_comment(sch, asset_name)
                    except Exception:
                        tcom = None
                    if not (tcom or "").strip():
                        tables_missing.append(
                            {"schema": sch, "table": asset_name, "kind": asset_kind}
                        )
                if scope in {"columns", "both"} and len(columns_missing) < limit:
                    try:
                        col_comments = db.get_column_comments(sch, asset_name)
                    except Exception:
                        col_comments = {}
                    for col_name, col_comment in col_comments.items():
                        if not (col_comment or "").strip():
                            columns_missing.append(
                                {
                                    "schema": sch,
                                    "table": asset_name,
                                    "column": col_name,
                                }
                            )
                            if len(columns_missing) >= limit:
                                break
                if len(tables_missing) >= limit and len(columns_missing) >= limit:
                    break
            if len(tables_missing) >= limit and len(columns_missing) >= limit:
                break

        return {
            "scope": scope,
            "schemas_scanned": target_schemas,
            "tables_missing_comment": tables_missing if scope != "columns" else [],
            "tables_missing_count": len(tables_missing) if scope != "columns" else 0,
            "columns_missing_comment": columns_missing if scope != "tables" else [],
            "columns_missing_count": len(columns_missing) if scope != "tables" else 0,
            # Surfaced so the LLM knows we filtered system objects and can
            # mention it in the answer ("4 system views were excluded;
            # they aren't user data and AMX doesn't describe them").
            "system_assets_skipped": skipped_system,
            "system_assets_skipped_count": len(skipped_system),
            "include_system": bool(include_system),
        }

    def _tool_list_databases(self) -> dict[str, Any]:
        rows = []
        for profile_name, db_cfg in sorted(self.cfg.db_profiles.items()):
            db_name = db_cfg.database or db_cfg.catalog or db_cfg.project or ""
            rows.append(
                {
                    "profile": profile_name,
                    "database": db_name,
                    "backend": db_cfg.backend or "",
                    "is_active": profile_name == (self.cfg.active_db_profile or "default"),
                }
            )
        return {"databases": rows, "count": len(rows)}

    # ------------------------------------------------------------ dtype family
    # Map a user-supplied dtype token to a concrete SQL-LIKE pattern set so
    # 'boolean' covers BOOL/BOOLEAN, 'int' covers BIGINT/INTEGER/SMALLINT,
    # 'date' covers DATE/TIMESTAMP/TIMESTAMPTZ, etc. Any unknown token is
    # passed through verbatim and matched as a substring against the column's
    # dtype field.
    _DTYPE_FAMILIES: dict[str, list[str]] = {
        "boolean": ["bool", "boolean"],
        "bool": ["bool", "boolean"],
        "int": ["int", "integer", "bigint", "smallint", "tinyint", "mediumint"],
        "integer": ["int", "integer", "bigint", "smallint", "tinyint", "mediumint"],
        "bigint": ["bigint"],
        "smallint": ["smallint", "int2"],
        "float": ["float", "double", "real", "numeric", "decimal"],
        "double": ["double", "float8"],
        "numeric": ["numeric", "decimal"],
        "decimal": ["numeric", "decimal"],
        "text": ["text", "varchar", "char", "string"],
        "varchar": ["varchar", "text", "char"],
        "string": ["text", "varchar", "char", "string"],
        "char": ["char", "varchar"],
        "date": ["date"],
        "timestamp": ["timestamp", "timestamptz", "datetime"],
        "datetime": ["timestamp", "timestamptz", "datetime"],
        "json": ["json", "jsonb"],
        "jsonb": ["jsonb"],
        "uuid": ["uuid"],
        "bytea": ["bytea", "blob", "binary"],
    }

    def _tool_find_columns_by_dtype(self, dtype: str, limit: int = 30) -> dict[str, Any]:
        token = (dtype or "").strip().lower()
        if not token:
            raise _ToolError("Argument 'dtype' is required.")
        family = self._DTYPE_FAMILIES.get(token, [token])
        # Build a single SQL OR-set so we run one query.
        with self.catalog._connect() as conn:  # noqa: SLF001 — internal helper
            placeholders = ", ".join(["?"] * len(family))
            query = f"""
                SELECT schema_name, table_name, column_name, dtype, effective_description
                FROM (
                    SELECT
                        ce.schema_name,
                        ce.table_name,
                        ce.column_name,
                        ce.dtype,
                        cd.description_text AS effective_description
                    FROM catalog_entities ce
                    LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                    WHERE ce.db_profile = ?
                      AND ce.entity_kind = 'column'
                      AND ce.dtype IS NOT NULL
                ) WHERE LOWER(dtype) IN ({placeholders})
                   OR {' OR '.join(['LOWER(dtype) LIKE ?'] * len(family))}
                ORDER BY schema_name, table_name, column_name
                LIMIT ?
            """
            params: list[Any] = [self.db_profile]
            params.extend(family)
            params.extend([f"%{f}%" for f in family])
            params.append(int(limit))
            rows = conn.execute(query, tuple(params)).fetchall()
        results = [
            {
                "schema": str(r["schema_name"] or ""),
                "table": str(r["table_name"] or ""),
                "column": str(r["column_name"] or ""),
                "dtype": str(r["dtype"] or ""),
                "description": str(r["effective_description"] or ""),
            }
            for r in rows
        ]
        # Roll up to (schema, table) so the LLM gets a clean per-table view.
        by_table: dict[tuple[str, str], list[dict[str, str]]] = {}
        for entry in results:
            key = (entry["schema"], entry["table"])
            by_table.setdefault(key, []).append(
                {
                    "column": entry["column"],
                    "dtype": entry["dtype"],
                    "description": entry["description"],
                }
            )
        tables = [
            {
                "schema": schema,
                "table": table,
                "matching_columns": cols,
                "match_count": len(cols),
            }
            for (schema, table), cols in by_table.items()
        ]
        return {
            "dtype": token,
            "matched_family": family,
            "table_count": len(tables),
            "column_count": len(results),
            "tables": tables,
        }

    def _tool_find_joinable_tables(self, table: str) -> dict[str, Any]:
        target = (table or "").strip()
        if not target:
            raise _ToolError("Argument 'table' is required.")
        # Resolve to schema.table when only the table name was provided.
        if "." not in target:
            exact = self.catalog.find_tables_by_exact_name(self.db_profile, target, limit=5)
            if not exact:
                return {
                    "table": target,
                    "found": False,
                    "message": (
                        f"No table named '{target}' is in the catalog. Try find_table_by_name "
                        "first, or qualify the target as schema.table."
                    ),
                    "joinable_tables": [],
                }
            if len(exact) > 1:
                paths = [
                    f"{str(r.get('schema_name') or '')}.{str(r.get('table_name') or '')}"
                    for r in exact
                ]
                return {
                    "table": target,
                    "found": False,
                    "ambiguous": True,
                    "candidates": paths,
                    "message": (
                        f"'{target}' lives in multiple schemas: {', '.join(paths)}. "
                        "Re-call with the fully-qualified schema.table."
                    ),
                    "joinable_tables": [],
                }
            row = exact[0]
            target = f"{row.get('schema_name') or ''}.{row.get('table_name') or ''}"
        rows = self.catalog.joinable_tables(self.db_profile, target, limit=12)
        joinable = [
            {
                "target_schema": str(r.get("target_schema_name") or ""),
                "target_table": str(r.get("target_table_name") or ""),
                "left_column": str(r.get("left_column") or ""),
                "right_column": str(r.get("right_column") or ""),
                "type": str(r.get("relationship_type") or ""),
                "score": float(r.get("score") or 0.0),
            }
            for r in rows
        ]
        return {
            "table": target,
            "found": True,
            "joinable_tables": joinable,
            "count": len(joinable),
        }
