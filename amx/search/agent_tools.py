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

import contextlib
import json
from collections.abc import Callable
from difflib import SequenceMatcher
from typing import Any

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
            with contextlib.suppress(Exception):
                self._db.close()
            self._db = None

    def __enter__(self) -> ToolBox:
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
                        "step before drilling into one specific schema.\n"
                        "Pass ``catalog`` to scope the listing to a Unity-Catalog catalog or "
                        "BigQuery project the active profile has not pinned. When the active "
                        "profile is a 3-level backend (Databricks UC) and no catalog is pinned "
                        "AND no ``catalog`` argument is given, the tool returns the visible "
                        "catalog list with a hint instead of failing — call ``list_catalogs`` "
                        "(or pass ``catalog`` here) to drill in."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "catalog": {
                                "type": "string",
                                "description": (
                                    "Optional Unity-Catalog catalog (Databricks) or BigQuery "
                                    "project to scope the listing to. Omit to use whatever the "
                                    "active profile has pinned."
                                ),
                            },
                        },
                        "required": [],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_tables_in_schema",
                    "description": (
                        "Return the tables, views, and materialized views inside a given "
                        "schema. Use this when the user asks 'what tables are under sap_test?', "
                        "'list all tables in sap_s6p', or to disambiguate a bare table name. "
                        "Pass ``catalog`` to scope the listing to a Unity-Catalog catalog the "
                        "active profile has not pinned."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "schema": {
                                "type": "string",
                                "description": "Exact schema name. Case-insensitive.",
                            },
                            "catalog": {
                                "type": "string",
                                "description": (
                                    "Optional Unity-Catalog catalog (Databricks) or BigQuery "
                                    "project. Omit to use whatever the active profile has "
                                    "pinned."
                                ),
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
                        "Locate tables by name with progressive fallback:\n"
                        "  • ``matches`` — exact-name hits (case-insensitive) across catalog "
                        "+ live DB. Authoritative when populated.\n"
                        "  • ``fuzzy_matches`` — list of ``{path, match_kind}`` where "
                        "match_kind is ``prefix`` / ``suffix`` / ``contains`` / ``fuzzy``. "
                        "Populated when the user gives a PARTIAL or APPROXIMATE name (e.g. "
                        "'I don't remember the whole name, just trog'). NEVER empty unless "
                        "the catalog and live DB truly have no related table.\n"
                        "RULE: when ``matches`` is empty, ALWAYS surface ``fuzzy_matches`` to "
                        "the user — never say 'no table found'. Order them by match_kind "
                        "(prefix > suffix > contains > fuzzy) and let the user pick. "
                        "Examples: 'where is adrc?', 'I think it was called trog…', "
                        "'tablo adı vbap mı vbpa mı?'."
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
                        "Return the table comment, column count, and three complementary "
                        "views of the column list:\n"
                        "  • ``dtype_summary`` — {family: count} across ALL columns. "
                        "Authoritative source for 'how many of dtype X are there'.\n"
                        "  • ``columns_by_dtype`` — {family: [column_names]} across ALL "
                        "columns. NEVER truncated. AUTHORITATIVE SOURCE for 'which "
                        "columns of dtype X exist on this table'.\n"
                        "  • ``columns`` — list of {name, dtype, nullable, comment} "
                        "objects. Sorted by 'interestingness' (commented first, then "
                        "rare dtypes) and TRUNCATED to 60 entries on wide tables. Use "
                        "this for description / nullability / comment access, NOT for "
                        "answering dtype questions.\n"
                        "Family vocabulary: bool, int, float, string, date, timestamp, "
                        "time, json, uuid, binary (or the lowered raw dtype for exotics).\n"
                        "RULE: when the user asks 'which columns are dtype X in TABLE' "
                        "(int, double, bool, string, date, timestamp, …), the COMPLETE "
                        "answer is in ``columns_by_dtype`` — read it directly and list "
                        "the names. Do NOT say 'no X columns' unless the family key is "
                        "absent or the list is empty. SAP / legacy boolean SEMANTICS "
                        "live in the ``string`` family (char(1)/varchar(1) flags like "
                        "'X'/'' or 'Y'/'N') — surface those alongside any native bool "
                        "instead of saying 'no boolean columns'. Use ``columns_truncated`` "
                        "to caveat the answer ('showing X of Y rows in details') only "
                        "when the user wants comments / examples per column.\n"
                        "ANALYTICS METADATA — the response also includes an ``analytics`` "
                        "object with backend-aware fields when the active DB exposes them: "
                        "``partition_keys`` / ``partition_strategy`` (range / list / hash / "
                        "time / bucket), ``clustering_keys`` (Snowflake / BigQuery / "
                        "Databricks ZORDER), ``storage_format`` (native / parquet / delta / "
                        "iceberg / external), ``storage_bytes`` + ``storage_files_count``, "
                        "``last_modified`` (ISO timestamp), ``table_type`` (managed / "
                        "external / view / materialized_view), ``tags`` + ``pii_columns`` "
                        "(governance), and ``indexes`` (PostgreSQL). Use these to answer "
                        "questions like 'is there a performance optimization opportunity', "
                        "'when was X last updated', 'which tables are larger than N GB', "
                        "'is there any PII column', 'is X partitioned'. Empty / absent "
                        "fields mean the active backend doesn't expose that signal — say "
                        "'this DB doesn't surface partition info' instead of 'this table "
                        "has no partition'."
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
                        "billing, ...). Returns a CANDIDATE SET — read each row's description "
                        "and filter false positives before composing your answer. Use for "
                        "'tables about pricing', 'müşteri ile ilgili tablolar', 'find tables "
                        "that store invoices'."
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
                        "Lexical search over indexed catalog columns. Returns a CANDIDATE "
                        "SET ranked by name/description similarity — many matches are FALSE "
                        "POSITIVES. For example, searching 'phone' returns every column with "
                        "'number' in the name (addrnumber, consnumber, persnumber, ...) even "
                        "though only tel_number / fax_number are actual phone numbers. After "
                        "calling, you MUST read each row's description text and filter the "
                        "list to ones that genuinely match the user's concept. Don't echo the "
                        "raw tool output. Use for 'where is the customer_id column?', 'which "
                        "tables have an email column?', 'address related columns'."
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
                    "name": "list_catalogs",
                    "description": (
                        "Run ``SHOW CATALOGS`` (or the equivalent) on the active live "
                        "connection and return every catalog / project visible to the role. "
                        "Use this on 3-level backends (Databricks Unity Catalog, BigQuery) "
                        "when the active DB profile has no catalog pinned and the user asks "
                        "to see tables / schemas — call this first, then pass the chosen "
                        "catalog to ``list_schemas`` or ``list_tables_in_schema``. Returns "
                        "``supports_catalogs=false`` for 2-level backends so the LLM can "
                        "fall back to ``list_server_databases`` instead."
                    ),
                    "parameters": {"type": "object", "properties": {}, "required": []},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_server_databases",
                    "description": (
                        "Run ``SHOW DATABASES`` (or the equivalent) on the active live "
                        "connection and return every database visible to the role. Use this "
                        "on 2-level backends (PostgreSQL, Snowflake, MySQL, MSSQL, Redshift, "
                        "ClickHouse) when the user asks 'which databases live on this "
                        "server?' or when the active profile has no database pinned and you "
                        "need to discover it. Different from ``list_databases`` — that lists "
                        "AMX DB profiles, this lists databases on the live server. Returns "
                        "an empty list with a hint for backends that don't expose a multi-"
                        "database server."
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
                        "('boolean', 'int', 'integer', 'text', 'date', 'timestamp', 'time', "
                        "'temporal', 'numeric', etc.). Each result row carries a 'kind' field "
                        "so the LLM can be honest about how the match was found.\n"
                        "SEMANTIC BUCKETS — when token is 'boolean' / 'date' / 'timestamp' / "
                        "'time' / 'temporal', the tool ALSO surfaces columns where the "
                        "SEMANTICS match even if the dtype doesn't:\n"
                        "  • 'boolean' → native bool/boolean dtype (kind=native_boolean) AND "
                        "single-char fixed-width strings char(1)/varchar(1) which SAP / "
                        "legacy schemas use as 'X'/'' or 'Y'/'N' flags (kind=flag_candidate).\n"
                        "  • 'date' / 'timestamp' / 'time' / 'temporal' → all native temporal "
                        "dtypes (kind=native_temporal) AND varchar/text columns whose NAME "
                        "looks like a date (suffix _date/_dt/_at/_time, prefix dat_/date_, "
                        "names like erdat/audat/created_at/valid_from/valid_to/begda/endda; "
                        "kind=name_inferred_temporal).\n"
                        "OTHER DTYPES — 'int' covers BIGINT/INTEGER/SMALLINT etc., "
                        "kind=exact_dtype_match.\n"
                        "ANSWERING RULE — when the user asks 'which tables have date / "
                        "boolean / timestamp columns', surface BOTH native AND name_inferred / "
                        "flag_candidate rows. NEVER say 'no date columns' when "
                        "name_inferred_temporal rows are present — say 'no native date dtype, "
                        "but the schema stores dates as varchar with names like X, Y, Z; "
                        "their format would need inspect_data_quality to confirm'. The user "
                        "usually means 'columns with date SEMANTICS', not 'columns whose "
                        "stored type is literally DATE'."
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
                    "name": "check_uniqueness",
                    "description": (
                        "Verify whether a column or column tuple uniquely "
                        "identifies rows in a table. Runs ``SELECT COUNT(*), "
                        "COUNT(DISTINCT (col1, col2, ...))`` against the live "
                        "DB and reports ``total_rows``, ``distinct_rows``, "
                        "``duplicate_rows``, ``uniqueness_ratio``, ``is_unique``. "
                        "Use this for 'is X a primary key?', 'are there duplicate "
                        "PK values?', '(id, time, op) tuple unique mi?', "
                        "'do I need composite PK or is `id` enough?'. The "
                        "``columns`` argument defaults to the table's declared "
                        "primary_key when omitted."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "schema": {"type": "string", "description": "Schema name."},
                            "table": {"type": "string", "description": "Table name."},
                            "columns": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": (
                                    "Column names whose tuple uniqueness will "
                                    "be tested. Omit to use the table's "
                                    "declared primary key."
                                ),
                            },
                        },
                        "required": ["schema", "table"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "detect_dimensional_role",
                    "description": (
                        "Classify a SINGLE table's role in a dimensional model "
                        "(fact / dimension / bridge / lookup / staging / "
                        "transactional / unknown), or — when ``table`` is "
                        "omitted — rank EVERY table in the schema by role and "
                        "say whether the layout looks like a STAR or "
                        "SNOWFLAKE schema. Detection blends naming patterns "
                        "(``fact_*`` / ``dim_*`` / ``stg_*`` / ``bridge_*`` / "
                        "``_facts`` / ``_dim`` / etc.) with structural "
                        "signals (row count percentile, count of outgoing "
                        "vs. incoming foreign keys, partition/clustering "
                        "presence, temporal columns). Each result row carries "
                        "``role_hypothesis``, ``confidence``, ``evidence`` "
                        "(human-readable bullets — ALWAYS quote in answer), "
                        "and ``indicators`` (the structured signals that "
                        "fired). Schema-level result also includes "
                        "``pattern_hypothesis`` (``star_schema`` / "
                        "``snowflake_schema`` / ``flat`` / ``unknown``) "
                        "derived from whether dimensions reference other "
                        "dimensions (snowflake) or only the fact (star). Use "
                        "this for 'what's the main/fact table here?', "
                        "'which tables look like dimensions?', 'is this a "
                        "star schema?', 'this schema'ın ana tablosu nedir?', "
                        "'fact ve dimension tabloları?'."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "schema": {"type": "string", "description": "Schema name."},
                            "table": {
                                "type": "string",
                                "description": (
                                    "Table to classify. Omit to rank every "
                                    "table in the schema and infer the "
                                    "overall star-vs-snowflake pattern."
                                ),
                            },
                        },
                        "required": ["schema"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "detect_scd_pattern",
                    "description": (
                        "Infer the table's slowly-changing-dimension (SCD) "
                        "history pattern from DATA SIGNALS — NOT from "
                        "comments. Returns ``scd_type_hypothesis`` "
                        "(``type_1`` / ``type_2`` / ``type_3`` / ``type_4`` / "
                        "``append_only`` / ``unknown``), ``confidence`` "
                        "(``high`` / ``medium`` / ``low``), ``evidence`` (a "
                        "human-readable bullet list of why), and "
                        "``indicators`` (the structured signals that fired):\n"
                        "  • Type 2 (history-as-rows) signals: valid_from/"
                        "valid_to column pair, is_current/active/current_flag "
                        "boolean column, version/revision/seq_no column.\n"
                        "  • Type 3 (history-as-columns) signals: paired "
                        "columns like (status, prev_status) / "
                        "(address, old_address) / (price, previous_price).\n"
                        "  • Type 4 (separate history table) signals: a "
                        "companion table ``X_history`` / ``X_hist`` / "
                        "``X_audit`` / ``X_log`` exists in the same schema.\n"
                        "  • Type 1 vs 2 row-count probe: when "
                        "``business_key`` is provided, counts rows per key — "
                        "1.0 means current-only (Type 1); >1 average means "
                        "history rows are kept (Type 2).\n"
                        "Use this for 'how does X hold history?', 'is this "
                        "SCD2 mi?', 'eski değerler nasıl tutuluyor?', "
                        "'değişiklik aynı satırda mı yeni satır mı?'. "
                        "ALWAYS surface the evidence list to the user — the "
                        "hypothesis alone (without evidence) is misleading."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "schema": {"type": "string", "description": "Schema name."},
                            "table": {"type": "string", "description": "Table name."},
                            "business_key": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": (
                                    "Business-key column(s) for the Type 1 vs "
                                    "Type 2 row-per-key probe. Optional — when "
                                    "omitted, the tool relies on column-name "
                                    "and sibling-table signals only."
                                ),
                            },
                        },
                        "required": ["schema", "table"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "sample_column_values",
                    "description": (
                        "Return a few non-null example values from a single column "
                        "via a direct ``SELECT col FROM schema.table WHERE col IS "
                        "NOT NULL LIMIT N``. Cheap (no profile, no full-table scan, "
                        "no catalog round-trip) and ground-truth (live DB). "
                        "Use this for 'give me a sample / example value', 'what "
                        "does column X look like', 'show me a value from aedat', "
                        "'date format YYYYMMDD mı, bir örnek görelim', 'kolon "
                        "değerleri nasıl'. ALWAYS resolve the table via "
                        "find_table_by_name first if the user didn't qualify the "
                        "schema — running this tool with the wrong schema will "
                        "fail with a misleading 'table not found' error. The "
                        "result includes 'samples' (list of distinct non-null "
                        "values, up to ``limit``) and 'distinct_count' so the "
                        "LLM can say 'here are 5 of 12 distinct values'."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "schema": {"type": "string", "description": "Schema name."},
                            "table": {"type": "string", "description": "Table name."},
                            "column": {
                                "type": "string",
                                "description": "Column to pull example values from.",
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Max distinct values to return (default 5).",
                                "default": 5,
                            },
                        },
                        "required": ["schema", "table", "column"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "inspect_data_quality",
                    "description": (
                        "Per-column data-quality probe against the live DB. "
                        "Returns row_count, null_count, distinct_count, "
                        "null_ratio, distinct_ratio, min_value, max_value, "
                        "and (for varchar/text columns that look like dates) "
                        "``detected_format`` — recognises common patterns "
                        "(YYYYMMDD / YYYY-MM-DD / DD/MM/YYYY / DD-MM-YYYY / "
                        "DDMMYYYY / ISO 8601). Use this for 'how many nulls "
                        "in email column?', 'date format ddmmyyyy mı?', 'is "
                        "the data continuous since when?' (read min_value of "
                        "the date column), 'çoklama oranı', 'are there gaps "
                        "in created_at?'. ``columns`` defaults to all columns "
                        "of the table; pass a subset to limit the scan on "
                        "very wide tables."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "schema": {"type": "string", "description": "Schema name."},
                            "table": {"type": "string", "description": "Table name."},
                            "columns": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": (
                                    "Column names to probe. Omit to scan all columns of the table."
                                ),
                            },
                        },
                        "required": ["schema", "table"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "find_joinable_tables",
                    "description": (
                        "Given ONE table, return the tables it can be joined with using a "
                        "three-tier fallback: (1) declared foreign keys, (2) name-overlap "
                        "heuristic (rarity-weighted shared column names — works WITHOUT FK "
                        "constraints, ideal for SAP-style schemas), (3) semantic similarity on "
                        "column descriptions. The result includes ``inference_source`` "
                        "(``foreign_key`` / ``name_overlap`` / ``semantic_similarity``) — when "
                        "you compose the final answer, ALWAYS state the inference tier "
                        "explicitly so the user knows whether the join is FK-verified or "
                        "name-inferred. Use for 'which tables can I join with vbrk?', "
                        "'X ile birleşebilecek tablolar', 'find tables related to vbrk'. "
                        "Different from get_join_candidates which needs both sides upfront."
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
            {
                "type": "function",
                "function": {
                    "name": "list_past_runs",
                    "description": (
                        "List the user's past ``/run`` invocations (analyze.run) from the "
                        "local SQLite history (``~/.amx/history.db``). Each row carries the "
                        "captured settings snapshot (LLM model, prompt detail, batch size, "
                        "dedup, etc.), scope, timing, and token usage. Defaults to "
                        "``analyze.run`` only — call ``list_chat_sessions`` for ``/ask`` "
                        "conversation history (those are resumable threads, not runs). "
                        "Use this when the user asks 'what runs have I done on sales.orders', "
                        "'compare my last 3 runs', 'which settings did I use yesterday', "
                        "'has this table been analyzed before'. NEVER reply 'I don't have "
                        "access to your past runs' — you DO via this tool. The returned rows "
                        "include human-readable ``started_at`` and ``duration_human`` "
                        "fields; use those (not the raw epoch / float) when rendering "
                        "tables or text answers."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "schema": {
                                "type": "string",
                                "description": "Optional: limit to runs that touched this schema.",
                            },
                            "table": {
                                "type": "string",
                                "description": "Optional: limit to runs that touched this table.",
                            },
                            "command": {
                                "type": "string",
                                "description": (
                                    "Filter mode: omit (or 'analyze.run' / 'run') for /run "
                                    "history (default — almost always what the user wants). "
                                    "Pass 'search.ask' / 'ask' to list /ask invocations as "
                                    "audit-log rows (per-turn). Pass 'all' to include both. "
                                    "For resumable /ask conversation threads use the "
                                    "list_chat_sessions tool instead."
                                ),
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Max runs to return (default 10, max 50).",
                            },
                        },
                        "required": [],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "describe_run",
                    "description": (
                        "Return the full record for one past run by ID — settings snapshot, "
                        "every per-column suggestion the LLM produced (top description + "
                        "alternatives, confidence band, logprob_score, token_count), and the "
                        "review decisions the user made. Use this AFTER list_past_runs has "
                        "narrowed the candidate set, when the user wants details on a specific "
                        "run ('show me run 42', 'what did the LLM suggest for adr6 in run 17', "
                        "'why is run 13's avg logprob higher than run 12's')."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "run_id": {
                                "type": "integer",
                                "description": "The numeric run id from list_past_runs.",
                            },
                            "include_results": {
                                "type": "boolean",
                                "description": (
                                    "When true (default), include every saved per-column "
                                    "result. Set false for a lightweight settings + scope "
                                    "view when results aren't needed."
                                ),
                            },
                        },
                        "required": ["run_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "list_chat_sessions",
                    "description": (
                        "List the user's past ``/ask`` chat sessions (resumable conversation "
                        "threads). Each row carries session id, started/last-active "
                        "timestamps, whether the session is still open, turn count, total "
                        "tokens, and the first user question as a preview. Use this — NOT "
                        "``list_past_runs(command='search.ask')`` — when the user asks "
                        "'show me my past chats', 'my ask history', 'previous /ask "
                        "conversations', 'continue our last chat'. The two surfaces store "
                        "the same conceptual data differently: analysis_runs rows for "
                        "``search.ask`` are PER-TURN audit log entries; chat_sessions rows "
                        "are PER-CONVERSATION threads. Users almost always want the latter. "
                        "Tell the user they can resume any ended session in the CLI via "
                        "``/session resume <id>``."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "limit": {
                                "type": "integer",
                                "description": "Max sessions to return (default 20, max 100).",
                            },
                            "include_ended": {
                                "type": "boolean",
                                "description": (
                                    "When true (default), include sessions that the user "
                                    "has /session end-ed. Set false to show only currently-"
                                    "active threads."
                                ),
                            },
                        },
                        "required": [],
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
    @contextlib.contextmanager
    def _scoped_catalog(self, db: DatabaseConnector, catalog: str | None):
        """Temporarily pin ``cfg.catalog`` so connector methods route by it.

        Used by ``list_schemas`` / ``list_tables_in_schema`` when the LLM
        passes a catalog argument to drill into a Unity-Catalog catalog
        the active profile has not pinned. Empty / None ``catalog`` is a
        no-op so callers can pass through unconditionally.
        """
        cat = (catalog or "").strip()
        if not cat:
            yield
            return
        cfg = getattr(db, "cfg", None)
        if cfg is None:
            yield
            return
        previous = getattr(cfg, "catalog", "")
        try:
            cfg.catalog = cat
            yield
        finally:
            cfg.catalog = previous

    # Names that are system / built-in catalogs across the catalog-aware
    # backends AMX supports. Filtered out before the LLM sees the catalog
    # list so a Databricks workspace with one user catalog and three
    # system catalogs auto-routes instead of asking the user to pick.
    # Lower-cased — we compare case-insensitively.
    _SYSTEM_CATALOG_NAMES: frozenset[str] = frozenset(
        {
            # Databricks Unity Catalog system catalogs.
            "system",
            "samples",
            "workspace",
            "hive_metastore",
            "spark_catalog",
            "__databricks_internal",
        }
    )

    @classmethod
    def _user_catalogs(cls, catalogs: list[str]) -> list[str]:
        """Drop well-known system / built-in catalogs from a candidate list.

        Used to disambiguate the no-catalog-pinned auto-pick path: if
        the only non-system catalog is a user catalog, we can route
        listings to it without asking the LLM to pick.
        """
        return [c for c in catalogs if c and c.lower() not in cls._SYSTEM_CATALOG_NAMES]

    def _tool_list_schemas(self, catalog: str = "") -> dict[str, Any]:
        db = self._live_db()
        cat_arg = (catalog or "").strip()
        pinned_catalog = str(getattr(self.cfg.db, "catalog", "") or "").strip()
        supports_catalogs = False
        try:
            supports_catalogs = bool(db.supports_catalogs())
        except Exception:
            supports_catalogs = False
        # 3-level backend (Databricks UC, BigQuery): when neither the
        # active profile nor the LLM has named a catalog, listing schemas
        # against the SQLAlchemy default surfaces the literal "Catalog
        # 'none' was not found" error from the warehouse. We try to
        # auto-pick here when the workspace exposes exactly one user
        # catalog (Databricks samples/system/workspace are filtered out).
        # When the choice is ambiguous, surface the filtered list so the
        # LLM can recurse — but the auto-pick path resolves the
        # user-reported infinite loop where kimi-thinking would compose
        # an answer like "I see 4 catalogs, let me check amx_test…"
        # without ever calling list_schemas a second time.
        if supports_catalogs and not cat_arg and not pinned_catalog:
            try:
                all_catalogs = [str(c) for c in db.list_catalogs()]
            except Exception as exc:
                raise _ToolError(
                    f"Active DB profile has no catalog pinned and SHOW CATALOGS failed: {exc}. "
                    "Edit the profile to pin a catalog or pass `catalog` to this tool."
                ) from exc
            user_catalogs = self._user_catalogs(all_catalogs)
            if len(user_catalogs) == 1:
                # Unambiguous — auto-pick and recurse so the LLM gets
                # actual schemas instead of having to choose.
                cat_arg = user_catalogs[0]
            else:
                # Ambiguous (multiple user catalogs) or empty (only
                # system catalogs). Surface the filtered list and the
                # full list so the LLM can pick the right one. The
                # ``message`` is phrased as a directive ("pick one
                # below and recurse") rather than a question to
                # discourage models from just composing prose at the
                # user.
                surface = user_catalogs or all_catalogs
                return {
                    "database": "(no catalog pinned)",
                    "schemas": [],
                    "count": 0,
                    "catalogs": surface,
                    "all_catalogs": all_catalogs,
                    "needs_catalog": True,
                    "message": (
                        "The active DB profile has no catalog pinned and the workspace "
                        "has multiple user catalogs. Pick the most likely one from the "
                        "`catalogs` list and IMMEDIATELY call this tool again with the "
                        "`catalog` argument set — do NOT just narrate the choice to the "
                        "user. If you genuinely cannot tell which catalog the user means, "
                        "answer in one short sentence asking them to pick."
                    ),
                }

        try:
            with self._scoped_catalog(db, cat_arg):
                schemas = [str(s) for s in db.list_schemas()]
        except Exception as exc:
            raise _ToolError(f"Could not list schemas live: {exc}") from exc
        database = (
            cat_arg
            or self.cfg.db.database
            or self.cfg.db.catalog
            or self.cfg.db.project
            or "(active database)"
        )
        payload: dict[str, Any] = {
            "database": database,
            "schemas": schemas,
            "count": len(schemas),
        }
        if cat_arg:
            payload["catalog"] = cat_arg
            if not pinned_catalog:
                # We auto-resolved the catalog (single user catalog
                # heuristic). Surface it so the LLM mentions which
                # catalog the schemas live in instead of pretending
                # the profile already had one pinned.
                payload["auto_picked_catalog"] = cat_arg
        return payload

    def _tool_list_tables_in_schema(self, schema: str, catalog: str = "") -> dict[str, Any]:
        target = (schema or "").strip()
        if not target:
            raise _ToolError("Argument 'schema' is required.")
        db = self._live_db()
        cat_arg = (catalog or "").strip()
        # Resolve case-insensitively against the live schema list within
        # the chosen catalog (when one was supplied). The same scoping
        # applies to the listing call below — without it, a Databricks UC
        # backend without a pinned catalog would issue ``SHOW TABLES FROM
        # None.<schema>`` and fail with NO_SUCH_CATALOG_EXCEPTION.
        with self._scoped_catalog(db, cat_arg):
            try:
                available = list(db.list_schemas())
            except Exception as exc:
                raise _ToolError(f"Could not list schemas: {exc}") from exc
            match = next((s for s in available if str(s).lower() == target.lower()), None)
            if match is None:
                return {
                    "schema": target,
                    "catalog": cat_arg or None,
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
        return {
            "schema": match,
            "catalog": cat_arg or None,
            "found": True,
            "tables": items,
            "count": len(items),
        }

    def _tool_list_catalogs(self) -> dict[str, Any]:
        db = self._live_db()
        try:
            supports = bool(db.supports_catalogs())
        except Exception:
            supports = False
        if not supports:
            return {
                "supports_catalogs": False,
                "catalogs": [],
                "count": 0,
                "message": (
                    "The active backend does not expose multiple catalogs. Use "
                    "`list_server_databases` for 2-level backends (PostgreSQL, "
                    "Snowflake, MySQL, MSSQL, Redshift, ClickHouse)."
                ),
            }
        try:
            catalogs = [str(c) for c in db.list_catalogs()]
        except Exception as exc:
            raise _ToolError(f"SHOW CATALOGS failed: {exc}") from exc
        pinned = str(getattr(self.cfg.db, "catalog", "") or "").strip()
        return {
            "supports_catalogs": True,
            "catalogs": catalogs,
            "count": len(catalogs),
            "active_catalog": pinned or None,
        }

    def _tool_list_server_databases(self) -> dict[str, Any]:
        db = self._live_db()
        try:
            databases = [str(d) for d in db.list_databases()]
        except Exception as exc:
            raise _ToolError(f"Listing databases failed: {exc}") from exc
        pinned = str(getattr(self.cfg.db, "database", "") or "").strip()
        if not databases:
            return {
                "databases": [],
                "count": 0,
                "active_database": pinned or None,
                "message": (
                    "The active backend does not expose multiple databases on this server, "
                    "or the role has no privilege to list them. For 3-level backends "
                    "(Databricks Unity Catalog, BigQuery) use `list_catalogs`."
                ),
            }
        return {
            "databases": databases,
            "count": len(databases),
            "active_database": pinned or None,
        }

    def _tool_find_table_by_name(self, name: str) -> dict[str, Any]:
        target = (name or "").strip()
        if not target:
            raise _ToolError("Argument 'name' is required.")
        # ── Stage 1 — exact match in both catalog + live DB ──
        catalog_rows = self.catalog.find_tables_by_exact_name(self.db_profile, target, limit=20)
        catalog_paths: list[str] = []
        for row in catalog_rows:
            schema_name = str(row.get("schema_name") or "")
            table_name = str(row.get("table_name") or "")
            if schema_name and table_name:
                catalog_paths.append(f"{schema_name}.{table_name}")
        live_paths: list[str] = []
        # Walk live DB once and remember every table name we see; the
        # exact-match check happens here, fuzzy fallback (Stage 2)
        # reuses the same list so we don't pay for two passes.
        all_live_tables: list[str] = []
        try:
            db = self._live_db()
            for schema in db.list_schemas():
                # Prefer ``list_assets`` when available — single round trip per schema.
                if hasattr(db, "list_assets"):
                    asset_iter = ((str(n), str(k)) for n, k in db.list_assets(schema))
                else:
                    asset_iter = ((str(n), "table") for n in db.list_tables(schema))
                for asset_name, _kind in asset_iter:
                    full_path = f"{schema}.{asset_name}"
                    all_live_tables.append(full_path)
                    if asset_name.lower() == target.lower():
                        live_paths.append(full_path)
        except Exception:
            # Live discovery is best-effort. Fall back to whatever the catalog had.
            pass
        merged = list(dict.fromkeys(catalog_paths + live_paths))

        # ── Stage 2 — substring + fuzzy fallback ──
        # When the user only remembers PART of the table name ("trog"
        # for "trogr_v"), exact match returns nothing and the LLM
        # honestly says "no such table". Give it a wider net: any
        # table where the target is a substring, prefix, suffix, OR
        # within edit distance ≤ 2. The LLM gets each match tagged
        # with ``match_kind`` so it can rank / present them
        # transparently. Same design fix as v0.9.10's columns_by_dtype:
        # complete coverage, no whack-a-mole per question phrasing.
        target_lower = target.lower()
        fuzzy_matches: list[dict[str, str]] = []
        seen = {p.lower() for p in merged}
        for path in all_live_tables:
            if path.lower() in seen:
                continue
            asset_name = path.split(".", 1)[1] if "." in path else path
            asset_lower = asset_name.lower()
            kind: str | None = None
            if target_lower == asset_lower:
                continue  # already in merged via Stage 1
            if target_lower in asset_lower:
                kind = "contains"
            elif asset_lower.startswith(target_lower):
                kind = "prefix"
            elif asset_lower.endswith(target_lower):
                kind = "suffix"
            else:
                # Edit-distance fallback. Use SequenceMatcher's ratio
                # as a cheap proxy: 0.7+ ≈ 1-2 edits on short SAP-style
                # names (4-8 chars).
                ratio = SequenceMatcher(
                    None,
                    target_lower,
                    asset_lower,
                ).ratio()
                if ratio >= 0.7 and abs(len(target_lower) - len(asset_lower)) <= 3:
                    kind = "fuzzy"
            if kind is not None:
                fuzzy_matches.append({"path": path, "match_kind": kind})
                seen.add(path.lower())

        # Catalog-side fuzzy: also scan catalog entities so we catch
        # tables that exist in the catalog but aren't in the live DB
        # listing yet (or live discovery failed).
        try:
            with self.catalog._connect() as conn:  # noqa: SLF001
                catalog_all = conn.execute(
                    "SELECT schema_name, table_name FROM catalog_entities "
                    "WHERE db_profile = ? AND entity_kind = 'table'",
                    (self.db_profile,),
                ).fetchall()
            for r in catalog_all:
                schema_name = str(r["schema_name"] or "")
                table_name = str(r["table_name"] or "")
                if not schema_name or not table_name:
                    continue
                path = f"{schema_name}.{table_name}"
                if path.lower() in seen:
                    continue
                asset_lower = table_name.lower()
                if target_lower == asset_lower:
                    continue
                kind: str | None = None
                if target_lower in asset_lower:
                    kind = "contains"
                elif asset_lower.startswith(target_lower):
                    kind = "prefix"
                elif asset_lower.endswith(target_lower):
                    kind = "suffix"
                else:
                    ratio = SequenceMatcher(
                        None,
                        target_lower,
                        asset_lower,
                    ).ratio()
                    if ratio >= 0.7 and abs(len(target_lower) - len(asset_lower)) <= 3:
                        kind = "fuzzy"
                if kind is not None:
                    fuzzy_matches.append({"path": path, "match_kind": kind})
                    seen.add(path.lower())
        except Exception:
            pass

        # Rank fuzzy matches: prefix/suffix > contains > fuzzy. Within
        # each tier, shorter table names rank first (assumption: a
        # 5-char table name containing "trog" is a closer hit than a
        # 30-char one).
        order = {"prefix": 0, "suffix": 1, "contains": 2, "fuzzy": 3}
        fuzzy_matches.sort(
            key=lambda r: (
                order.get(r["match_kind"], 99),
                len(r["path"].split(".", 1)[1] if "." in r["path"] else r["path"]),
                r["path"].lower(),
            )
        )
        # Cap so the prompt stays tight on huge schemas.
        fuzzy_matches = fuzzy_matches[:25]

        return {
            "name": target,
            "matches": merged,
            "match_count": len(merged),
            "from_catalog": catalog_paths,
            "from_live_db": live_paths,
            # Substring + fuzzy fallback. ALWAYS populated (empty list
            # when nothing matches) so the LLM has one shape to reason
            # over. Each entry is ``{path, match_kind}`` where
            # match_kind is one of: prefix / suffix / contains / fuzzy.
            "fuzzy_matches": fuzzy_matches,
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
        all_cols = [
            {
                "name": c.name,
                "type": c.dtype,
                "nullable": bool(c.nullable),
                "comment": str(c.existing_comment or ""),
            }
            for c in profile.columns
        ]

        # ── Per-dtype family summary + complete coverage map ──
        # The summary gives the LLM the complete dtype picture of the
        # table even when the columns list below is truncated.
        # ``columns_by_dtype`` carries the actual column NAMES grouped
        # by family (NOT truncated, regardless of total table width)
        # so the LLM can answer "which columns are int / double / bool /
        # string / date / … in TABLE" by reading one map instead of
        # asking AMX for one tool-call per dtype. This is the design
        # fix for the false-negative loop ("we can't enumerate every
        # dtype question one by one"): give the LLM the complete
        # picture and trust it to reason.
        dtype_summary: dict[str, int] = {}
        columns_by_dtype: dict[str, list[str]] = {}
        for c in all_cols:
            family = self._dtype_family_label(c["type"])
            dtype_summary[family] = dtype_summary.get(family, 0) + 1
            columns_by_dtype.setdefault(family, []).append(c["name"])

        # ── Smart truncation order ──
        # When wide tables get capped, the truncation should leave the
        # MOST INTERESTING columns visible: rare dtypes (bool / date /
        # uuid / json — usually one or two per table) and columns that
        # already have a comment (someone curated them, so they're
        # worth seeing). Numeric / varchar columns without comments
        # cluster at the bottom because there are many of them and
        # they're typically interchangeable.
        rarity = dict(dtype_summary.items())

        def _sort_key(col: dict[str, Any]) -> tuple[int, int, str]:
            family = self._dtype_family_label(col["type"])
            commented = 1 if col.get("comment") else 0
            # rarity rank — fewer columns of this dtype family => earlier
            return (
                -commented,  # comments first
                rarity.get(family, 999),  # rare dtypes next (lower count first)
                col["name"],  # alphabetical tiebreak
            )

        sorted_cols = sorted(all_cols, key=_sort_key)

        # ── Analytics metadata ──
        # v0.10.0 introduced AnalyticsMetadata on TableProfile; pull
        # the non-empty fields here so the LLM can answer
        # performance-optimization / freshness / governance questions
        # without an extra tool round-trip. Empty fields are dropped to
        # keep the prompt tight.
        analytics_payload: dict[str, Any] = {}
        am = getattr(profile, "analytics", None)
        if am is not None:
            for attr in (
                "partition_keys",
                "partition_strategy",
                "clustering_keys",
                "storage_format",
                "storage_bytes",
                "storage_files_count",
                "last_modified",
                "table_type",
                "tags",
                "pii_columns",
                "indexes",
                "warnings",
            ):
                value = getattr(am, attr, None)
                if value:  # drop empty list / "" / 0 / {}
                    analytics_payload[attr] = value

        return {
            "schema": schema_name,
            "table": table_name,
            "found": True,
            "table_comment": str(profile.existing_comment or ""),
            "row_count": int(profile.row_count or 0),
            "column_count": len(all_cols),
            "dtype_summary": dtype_summary,
            # Complete coverage — no truncation. Authoritative source
            # for "which columns of dtype X exist on this table".
            "columns_by_dtype": columns_by_dtype,
            "columns_truncated": len(all_cols) > 60,
            "columns": sorted_cols[:60],
            # Analytics-aware metadata — partition / cluster / size /
            # format / freshness / tags. Per-backend coverage varies;
            # only non-empty fields are included.
            "analytics": analytics_payload,
        }

    @staticmethod
    def _dtype_family_label(dtype: str) -> str:
        """Coarse dtype family label used in ``dtype_summary``.

        Mirrors the agent_tools dtype-family map but compresses to one
        label per column (``bool`` / ``int`` / ``float`` / ``string`` /
        ``date`` / ``timestamp`` / ``json`` / ``uuid`` / etc.). Returns
        the lowered raw dtype when no family matches so exotic types
        still appear in the summary instead of silently merging into
        a generic bucket.
        """
        raw = (dtype or "").strip().lower()
        if not raw:
            return "unknown"
        # Strip array suffix and length/precision parens.
        base = raw.rstrip("[]")
        base = base.split("(", 1)[0].strip()
        head = base.split()[0] if base else raw
        if head in {"bool", "boolean"}:
            return "bool"
        if head in {
            "int",
            "integer",
            "int4",
            "int8",
            "int2",
            "bigint",
            "smallint",
            "serial",
            "bigserial",
        }:
            return "int"
        if head in {"float", "float4", "float8", "double", "real", "numeric", "decimal", "money"}:
            return "float"
        if head in {
            "char",
            "varchar",
            "text",
            "string",
            "nchar",
            "nvarchar",
            "character",
            "bpchar",
        }:
            return "string"
        if head in {"date"}:
            return "date"
        if head in {"timestamp", "timestamptz", "datetime", "datetime2", "smalldatetime"}:
            return "timestamp"
        if head in {"time", "timetz"}:
            return "time"
        if head in {"json", "jsonb"}:
            return "json"
        if head in {"uuid"}:
            return "uuid"
        if head in {"bytea", "blob", "binary", "varbinary"}:
            return "binary"
        return head

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
                        f"No schema named '{target}'. Available schemas: " + ", ".join(available)
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
        # ``boolean`` matches the literal PG ``bool``/``boolean`` types
        # AND single-character fixed-width strings (``char(1)`` /
        # ``varchar(1)`` / ``character(1)``) which SAP and many legacy
        # schemas use as boolean flags ("X" / "" or "Y" / "N"). Without
        # the char(1) family, /ask "are there any boolean columns in
        # vbak?" would say "no" with confidence even though SAP vbak
        # has dozens of single-char flags (autlf, faksk, lifsk, ...).
        "boolean": [
            "bool",
            "boolean",
            "char(1)",
            "varchar(1)",
            "character(1)",
            "character varying(1)",
        ],
        "bool": [
            "bool",
            "boolean",
            "char(1)",
            "varchar(1)",
            "character(1)",
            "character varying(1)",
        ],
        "int": ["int", "integer", "bigint", "smallint", "tinyint", "mediumint"],
        # ``date`` is a SEMANTIC bucket — it covers every temporal
        # native type (``date``, ``timestamp``, ``timestamptz``,
        # ``datetime``, ``time``) so /ask "which tables have date
        # related columns" returns one set instead of forcing the LLM
        # to call once per dtype. Name-inferred date matches
        # (varchar columns whose NAME suggests date semantics —
        # ``erdat``, ``audat``, ``*_date``, ``created_at``, etc.) are
        # added in ``_tool_find_columns_by_dtype`` via a separate
        # name-pattern query, NOT as additional dtype tokens here.
        "date": [
            "date",
            "timestamp",
            "timestamptz",
            "datetime",
            "datetime2",
            "smalldatetime",
            "time",
            "timetz",
            "timestamp_ntz",
            "timestamp_ltz",
        ],
        "timestamp": [
            "timestamp",
            "timestamptz",
            "datetime",
            "datetime2",
            "smalldatetime",
            "timestamp_ntz",
            "timestamp_ltz",
        ],
        "time": ["time", "timetz"],
        "temporal": [
            "date",
            "timestamp",
            "timestamptz",
            "datetime",
            "datetime2",
            "smalldatetime",
            "time",
            "timetz",
            "timestamp_ntz",
            "timestamp_ltz",
        ],
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
                   OR {" OR ".join(["LOWER(dtype) LIKE ?"] * len(family))}
                ORDER BY schema_name, table_name, column_name
                LIMIT ?
            """
            params: list[Any] = [self.db_profile]
            params.extend(family)
            params.extend([f"%{f}%" for f in family])
            params.append(int(limit))
            rows = conn.execute(query, tuple(params)).fetchall()
        # Classify each match so the LLM can be honest in its
        # answer: native_boolean vs flag_candidate (single-char
        # fixed-width fields used as boolean flags by SAP / legacy
        # schemas). For non-boolean queries this is always
        # ``exact_dtype_match``.
        is_boolean_query = token in {"bool", "boolean"}
        is_temporal_query = token in {"date", "timestamp", "time", "temporal"}
        results: list[dict[str, Any]] = []
        for r in rows:
            dtype_raw = str(r["dtype"] or "")
            dtype_lower = dtype_raw.lower()
            if is_boolean_query:
                if dtype_lower in {"bool", "boolean"}:
                    kind = "native_boolean"
                elif "(1)" in dtype_lower and any(
                    base in dtype_lower for base in ("char", "varchar", "character")
                ):
                    kind = "flag_candidate"
                else:
                    kind = "exact_dtype_match"
            elif is_temporal_query:
                # Native temporal dtype hits.
                kind = "native_temporal"
            else:
                kind = "exact_dtype_match"
            results.append(
                {
                    "schema": str(r["schema_name"] or ""),
                    "table": str(r["table_name"] or ""),
                    "column": str(r["column_name"] or ""),
                    "dtype": dtype_raw,
                    "description": str(r["effective_description"] or ""),
                    "kind": kind,
                }
            )

        # ── Name-pattern inference for semantic buckets ──
        # When the user asks about "date" (semantic) and the catalog
        # has SAP-style dates stored as varchar(8) / text, the
        # native-dtype query above misses them. Run a second query
        # against the same catalog that matches column names against
        # well-known temporal naming conventions, restricted to
        # string-family dtypes so we don't tag a numeric column as
        # date just because its name happens to contain "date".
        if is_temporal_query:
            seen_keys = {(r["schema"], r["table"], r["column"]) for r in results}
            name_patterns = [
                "%_date",
                "%_dt",
                "%_at",
                "%_time",
                "%_ts",
                "dat_%",
                "date_%",
                "time_%",
                "erdat",
                "audat",
                "ernam_dat",
                "letzd",
                "valid_from",
                "valid_to",
                "created%",
                "updated%",
                "modified%",
                "deleted%",
                "begda",
                "endda",
                "rldat",
                "psotg",
                "tzonso",
            ]
            string_dtypes_like = ["%char%", "%text%", "%string%", "%varchar%"]
            with self.catalog._connect() as conn:  # noqa: SLF001
                # OR-of name LIKE patterns AND OR-of string dtype LIKE patterns
                name_like_clause = " OR ".join("LOWER(column_name) LIKE ?" for _ in name_patterns)
                dtype_like_clause = " OR ".join("LOWER(dtype) LIKE ?" for _ in string_dtypes_like)
                q = f"""
                    SELECT ce.schema_name, ce.table_name, ce.column_name,
                           ce.dtype,
                           cd.description_text AS effective_description
                    FROM catalog_entities ce
                    LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                    WHERE ce.db_profile = ?
                      AND ce.entity_kind = 'column'
                      AND ce.dtype IS NOT NULL
                      AND ({name_like_clause})
                      AND ({dtype_like_clause})
                    ORDER BY ce.schema_name, ce.table_name, ce.column_name
                    LIMIT ?
                """
                params2: list[Any] = [self.db_profile]
                params2.extend(name_patterns)
                params2.extend(string_dtypes_like)
                params2.append(int(limit))
                try:
                    name_rows = conn.execute(q, tuple(params2)).fetchall()
                except Exception:
                    name_rows = []
            for r in name_rows:
                schema_n = str(r["schema_name"] or "")
                table_n = str(r["table_name"] or "")
                column_n = str(r["column_name"] or "")
                if (schema_n, table_n, column_n) in seen_keys:
                    continue
                results.append(
                    {
                        "schema": schema_n,
                        "table": table_n,
                        "column": column_n,
                        "dtype": str(r["dtype"] or ""),
                        "description": str(r["effective_description"] or ""),
                        "kind": "name_inferred_temporal",
                    }
                )
        # Roll up to (schema, table) so the LLM gets a clean per-table view.
        by_table: dict[tuple[str, str], list[dict[str, str]]] = {}
        for entry in results:
            key = (entry["schema"], entry["table"])
            by_table.setdefault(key, []).append(
                {
                    "column": entry["column"],
                    "dtype": entry["dtype"],
                    "description": entry["description"],
                    "kind": entry["kind"],
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
        # Three-tier fallback chain (v0.9.7):
        # 1. Symbolic FK relationships from catalog (best — explicit
        #    referential integrity). Empty when the DB has no FK
        #    constraints, which is typical of SAP / legacy schemas
        #    where joins are managed at the application layer.
        # 2. Name-overlap heuristic — same column name on both sides,
        #    weighted by rarity so ``mandt`` (in every table) doesn't
        #    drown out a high-signal shared name. Works WITHOUT FK
        #    constraints AND WITHOUT per-column descriptions.
        # 3. Semantic similarity — vector match on column descriptions.
        #    Requires the catalog to have been ``/run``-populated.
        # The first non-empty tier wins; ``inference_source`` is
        # surfaced so the LLM can be honest in the answer ("via FK"
        # vs "via shared column name" vs "via semantic similarity").
        rows = self.catalog.joinable_tables(self.db_profile, target, limit=12)
        inference_source = "foreign_key"
        if not rows:
            rows = self.catalog.name_overlap_joinable_tables(
                self.db_profile,
                target,
                limit=12,
            )
            if rows:
                inference_source = "name_overlap"
        if not rows:
            try:
                rows = self.catalog.semantic_joinable_tables(
                    self.db_profile,
                    target,
                    limit=12,
                )
            except Exception:
                rows = []
            if rows:
                inference_source = "semantic_similarity"
        joinable = [
            {
                "target_schema": str(r.get("target_schema_name") or ""),
                "target_table": str(r.get("target_table_name") or ""),
                "left_column": str(r.get("left_column") or ""),
                "right_column": str(r.get("right_column") or ""),
                "type": str(r.get("relationship_type") or ""),
                "score": float(r.get("score") or 0.0),
                "shared_column_count": int(r.get("shared_column_count") or 0),
            }
            for r in rows
        ]
        return {
            "table": target,
            "found": True,
            "joinable_tables": joinable,
            "count": len(joinable),
            "inference_source": inference_source,
        }

    # ── Data-quality / uniqueness probes (v0.10.2) ─────────────────────────

    _DATE_FORMAT_PATTERNS: list[tuple[str, str]] = [
        # (regex, label) — matched against samples; first match wins.
        (r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", "ISO 8601 timestamp"),
        (r"^\d{4}-\d{2}-\d{2}$", "YYYY-MM-DD"),
        (r"^\d{4}/\d{2}/\d{2}$", "YYYY/MM/DD"),
        (r"^\d{8}$", "YYYYMMDD"),
        (r"^\d{2}-\d{2}-\d{4}$", "DD-MM-YYYY"),
        (r"^\d{2}/\d{2}/\d{4}$", "DD/MM/YYYY"),
        (r"^\d{2}\.\d{2}\.\d{4}$", "DD.MM.YYYY"),
        (r"^\d{6}$", "YYMMDD or YYYYMM"),
        (r"^\d{2}/\d{2}/\d{2}$", "DD/MM/YY or MM/DD/YY"),
        (r"^\d{4}-\d{2}$", "YYYY-MM"),
    ]

    @staticmethod
    def _detect_date_format(samples: list[Any]) -> str:
        """Return the dominant date-format label across non-null samples.

        Returns the empty string when no pattern matches a majority of
        the samples. Used by ``inspect_data_quality`` for varchar/text
        columns whose stored type doesn't reveal the temporal format.
        """
        import re as _re

        clean = [str(s).strip() for s in samples if s is not None and str(s).strip()]
        if not clean:
            return ""
        counts: dict[str, int] = {}
        for value in clean:
            for pattern, label in ToolBox._DATE_FORMAT_PATTERNS:
                if _re.match(pattern, value):
                    counts[label] = counts.get(label, 0) + 1
                    break
        if not counts:
            return ""
        # Pick the most common; require at least 60% confidence so we
        # don't slap a date label on a column that just happens to
        # have a few date-shaped values.
        best_label, best_count = max(counts.items(), key=lambda kv: kv[1])
        if best_count / len(clean) >= 0.6:
            return best_label
        return ""

    def _tool_check_uniqueness(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Verify whether (col1, col2, ...) is unique across the table.

        Runs ``SELECT COUNT(*), COUNT(DISTINCT (cols))`` against the
        live DB. When ``columns`` is omitted, falls back to the
        table's declared primary key.
        """
        from sqlalchemy import text as _text

        schema_name = (schema or "").strip()
        table_name = (table or "").strip()
        if not schema_name or not table_name:
            raise _ToolError("Both 'schema' and 'table' are required.")

        # Resolve columns: explicit > primary key.
        target_cols = list(columns or [])
        if not target_cols:
            try:
                profile = self._live_db().profile_table(
                    schema_name,
                    table_name,
                    sample_size=0,
                )
                target_cols = list(profile.primary_key or [])
            except Exception as exc:
                return {
                    "schema": schema_name,
                    "table": table_name,
                    "found": False,
                    "error": f"Could not load table profile: {exc}",
                }
        if not target_cols:
            # No PK declared and the caller didn't pass columns. Don't
            # bounce back with "give me columns" — that's the literal
            # answer the user is trying to escape. Instead, run
            # inspect_data_quality so the LLM sees per-column distinct
            # ratios + can name the most likely candidate keys
            # (columns where distinct_ratio ≈ 1.0). The LLM can then
            # follow up with a targeted check_uniqueness call once it
            # has a hypothesis.
            try:
                quality = self._tool_inspect_data_quality(
                    schema_name,
                    table_name,
                    columns=None,
                )
            except Exception as exc:
                quality = {"error": str(exc)}
            candidate_cols = []
            if isinstance(quality, dict) and quality.get("found"):
                # Likely-unique columns first (distinct_ratio close to 1.0).
                for entry in quality.get("columns", []):
                    if entry.get("distinct_ratio", 0) >= 0.99:
                        candidate_cols.append(entry["column"])
            return {
                "schema": schema_name,
                "table": table_name,
                "columns": [],
                "found": False,
                "no_primary_key": True,
                "duplicate_summary": quality,
                "likely_unique_columns": candidate_cols,
                "hint": (
                    "No primary key is declared on this table. The "
                    "duplicate_summary above carries per-column distinct "
                    "ratios; columns with ratio ≈ 1.0 are likely unique. "
                    "Pick a candidate composite key and call "
                    "check_uniqueness again with explicit ``columns``, or "
                    "ask the user which key they care about."
                ),
            }

        db = self._live_db()
        adapter = db._adapter  # noqa: SLF001
        fqn = adapter.fully_qualified_name(schema_name, table_name)
        quoted_cols = [adapter.quote_identifier(c) for c in target_cols]
        col_tuple = ", ".join(quoted_cols)
        try:
            with db.engine.connect() as conn:
                # COUNT(DISTINCT (a, b, c)) is supported by all 4 backends
                # we target; the parens make it a row-tuple comparison.
                row = conn.execute(
                    _text(
                        f"SELECT COUNT(*) AS total, "
                        f"COUNT(DISTINCT ({col_tuple})) AS distinct_count "
                        f"FROM {fqn}"
                    ),
                ).fetchone()
        except Exception as exc:
            return {
                "schema": schema_name,
                "table": table_name,
                "columns": target_cols,
                "found": False,
                "error": f"Uniqueness probe failed: {exc}",
            }
        total = int(row[0] or 0) if row else 0
        distinct = int(row[1] or 0) if row else 0
        duplicate_rows = max(0, total - distinct)
        ratio = (distinct / total) if total else 0.0
        return {
            "schema": schema_name,
            "table": table_name,
            "columns": target_cols,
            "total_rows": total,
            "distinct_rows": distinct,
            "duplicate_rows": duplicate_rows,
            "uniqueness_ratio": round(ratio, 6),
            "is_unique": (total > 0 and total == distinct),
        }

    def _tool_inspect_data_quality(
        self,
        schema: str,
        table: str,
        columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Per-column live-DB stats: nulls, distincts, min/max, date format.

        Loads a single TableProfile (sampled) and returns a per-column
        dict so the LLM has one map for "how nullable is X", "what's
        the min/max of created_at", "is this a date column stored as
        varchar?".
        """
        schema_name = (schema or "").strip()
        table_name = (table or "").strip()
        if not schema_name or not table_name:
            raise _ToolError("Both 'schema' and 'table' are required.")
        try:
            # sample_size>0 enables the column stats collection (null
            # count, distinct count, min/max, samples) the existing
            # profiler already does. Use a small but informative sample
            # so this stays fast even on huge tables.
            profile = self._live_db().profile_table(
                schema_name,
                table_name,
                sample_size=50,
            )
        except Exception as exc:
            return {
                "schema": schema_name,
                "table": table_name,
                "found": False,
                "error": str(exc),
            }

        wanted = {c.lower() for c in (columns or [])}
        per_col: list[dict[str, Any]] = []
        total_rows = int(profile.row_count or 0)
        for cp in profile.columns:
            if wanted and cp.name.lower() not in wanted:
                continue
            non_null = max(0, total_rows - int(cp.null_count or 0))
            null_ratio = (int(cp.null_count or 0) / total_rows) if total_rows else 0.0
            distinct_ratio = (int(cp.distinct_count or 0) / total_rows) if total_rows else 0.0
            entry: dict[str, Any] = {
                "column": cp.name,
                "dtype": str(cp.dtype),
                "nullable": bool(cp.nullable),
                "row_count": total_rows,
                "null_count": int(cp.null_count or 0),
                "non_null_count": non_null,
                "null_ratio": round(null_ratio, 6),
                "distinct_count": int(cp.distinct_count or 0),
                "distinct_ratio": round(distinct_ratio, 6),
                "min_value": (str(cp.min_val) if cp.min_val is not None else ""),
                "max_value": (str(cp.max_val) if cp.max_val is not None else ""),
            }
            # Detected date format — only meaningful for string-family
            # dtypes (varchar / text / char). Native date / timestamp
            # columns advertise their format via dtype itself.
            dtype_low = str(cp.dtype).lower()
            if any(token in dtype_low for token in ("char", "text", "string", "varchar")):
                fmt = self._detect_date_format(cp.samples or [])
                if fmt:
                    entry["detected_format"] = fmt
                    entry["likely_kind"] = "date_or_timestamp_in_string"
            per_col.append(entry)

        return {
            "schema": schema_name,
            "table": table_name,
            "found": True,
            "row_count": total_rows,
            "column_count": len(profile.columns),
            "columns": per_col,
        }

    def _tool_sample_column_values(
        self,
        schema: str,
        table: str,
        column: str,
        limit: int = 5,
    ) -> dict[str, Any]:
        """Pull a few distinct non-null example values from a single column.

        Direct ``SELECT DISTINCT col FROM schema.table WHERE col IS NOT
        NULL LIMIT N`` against the live DB — bypasses ``profile_table``
        (which scans every column + foreign keys + stats) so a "give
        me an example" question doesn't pay for a full table profile.
        """
        from sqlalchemy import text as _text

        schema_name = (schema or "").strip()
        table_name = (table or "").strip()
        column_name = (column or "").strip()
        if not schema_name or not table_name or not column_name:
            raise _ToolError(
                "All of 'schema', 'table', 'column' are required.",
            )
        n = max(1, min(int(limit or 5), 50))

        db = self._live_db()
        adapter = db._adapter  # noqa: SLF001
        fqn = adapter.fully_qualified_name(schema_name, table_name)
        col_q = adapter.quote_identifier(column_name)

        try:
            with db.engine.connect() as conn:
                # DISTINCT keeps the prompt small when the column has
                # repeated values (boolean flags, status codes, etc.).
                rows = conn.execute(
                    _text(
                        f"SELECT DISTINCT {col_q} AS v "
                        f"FROM {fqn} "
                        f"WHERE {col_q} IS NOT NULL "
                        f"LIMIT :n"
                    ),
                    {"n": n},
                ).fetchall()
                samples = [str(r[0]) for r in rows if r and r[0] is not None]
                # Also fetch distinct count when cheap (single-column
                # COUNT(DISTINCT) is fast on indexed tables; soft-fails
                # on big un-indexed columns where the planner gives up).
                try:
                    distinct_row = conn.execute(
                        _text(f"SELECT COUNT(DISTINCT {col_q}) FROM {fqn}"),
                    ).fetchone()
                    distinct_count = (
                        int(distinct_row[0])
                        if distinct_row and distinct_row[0] is not None
                        else None
                    )
                except Exception:
                    distinct_count = None
        except Exception as exc:
            return {
                "schema": schema_name,
                "table": table_name,
                "column": column_name,
                "found": False,
                "error": str(exc),
                "hint": (
                    "If the schema/table didn't resolve, call "
                    "find_table_by_name first — the user may have "
                    "given a bare table name and the agent picked the "
                    "wrong schema."
                ),
            }

        return {
            "schema": schema_name,
            "table": table_name,
            "column": column_name,
            "found": True,
            "samples": samples,
            "sample_count": len(samples),
            "distinct_count": distinct_count,
        }

    # SCD-pattern naming heuristics. Lowered + matched as substring on
    # the column name so suffixes like ``my_valid_from_dt`` still
    # register. Order matters per signal: more-specific names first
    # so a column called ``effective_from_date`` matches the type-2
    # temporal pair before the generic ``_from`` filter.
    _SCD_VALID_FROM_NAMES: tuple[str, ...] = (
        "valid_from",
        "valid_start",
        "effective_from",
        "effective_start",
        "start_date",
        "start_dt",
        "begin_date",
        "begda",
        "from_date",
        "active_from",
        "row_start",
    )
    _SCD_VALID_TO_NAMES: tuple[str, ...] = (
        "valid_to",
        "valid_end",
        "effective_to",
        "effective_end",
        "end_date",
        "end_dt",
        "endda",
        "to_date",
        "active_to",
        "row_end",
    )
    _SCD_CURRENT_FLAG_NAMES: tuple[str, ...] = (
        "is_current",
        "is_active",
        "current_flag",
        "active_flag",
        "is_latest",
        "current_record",
        "is_current_version",
    )
    _SCD_VERSION_NAMES: tuple[str, ...] = (
        "version",
        "revision",
        "rev_no",
        "seq_no",
        "row_version",
        "scd_version",
        "history_seq",
    )
    _SCD_PREV_PREFIXES: tuple[str, ...] = (
        "prev_",
        "previous_",
        "old_",
        "former_",
        "before_",
        "last_",
    )
    _SCD_NEW_PREFIXES: tuple[str, ...] = (
        "new_",
        "current_",
        "now_",
        "after_",
    )
    _SCD_HISTORY_SUFFIXES: tuple[str, ...] = (
        "_history",
        "_hist",
        "_audit",
        "_log",
        "_archive",
        "_versions",
        "_changes",
        "_snapshot",
    )

    def _tool_detect_scd_pattern(
        self,
        schema: str,
        table: str,
        business_key: list[str] | None = None,
    ) -> dict[str, Any]:
        """Infer SCD type from column-name patterns + sibling tables + key cardinality.

        The heuristic stack:

        1. Column-name patterns ⇒ Type 2 / Type 3 hints.
        2. Sibling-table lookup (``X_history`` / ``X_hist`` / ``X_audit``
           / ``X_log``) ⇒ Type 4 hint.
        3. When ``business_key`` is provided: row-per-key avg count ⇒
           Type 1 vs Type 2 (current-only vs history-rows).

        The hypothesis is the strongest signal that fired; ``evidence``
        captures every detected signal so the LLM can quote them
        verbatim instead of asserting the type without justification.
        """
        from sqlalchemy import text as _text

        schema_name = (schema or "").strip()
        table_name = (table or "").strip()
        if not schema_name or not table_name:
            raise _ToolError("Both 'schema' and 'table' are required.")

        # Profile the table once to get column names + dtypes + PK.
        try:
            profile = self._live_db().profile_table(
                schema_name,
                table_name,
                sample_size=0,
            )
        except Exception as exc:
            return {
                "schema": schema_name,
                "table": table_name,
                "found": False,
                "error": str(exc),
                "hint": ("If schema/table didn't resolve, call find_table_by_name first."),
            }

        col_names_lower = [str(c.name).lower() for c in profile.columns]
        col_lookup = {n: profile.columns[i] for i, n in enumerate(col_names_lower)}

        evidence: list[str] = []
        indicators: dict[str, Any] = {}

        # ── Type 2 — temporal row-validity pair ──
        valid_from_hits = [
            n for n in col_names_lower if any(p in n for p in self._SCD_VALID_FROM_NAMES)
        ]
        valid_to_hits = [
            n for n in col_names_lower if any(p in n for p in self._SCD_VALID_TO_NAMES)
        ]
        if valid_from_hits and valid_to_hits:
            indicators["type2_temporal_pair"] = [valid_from_hits[0], valid_to_hits[0]]
            evidence.append(f"Type 2 temporal pair: `{valid_from_hits[0]}` + `{valid_to_hits[0]}`.")
        elif valid_from_hits:
            indicators["type2_open_ended_temporal"] = valid_from_hits[0]
            evidence.append(
                f"Type 2 partial signal: `{valid_from_hits[0]}` exists "
                "but no matching end-of-validity column."
            )

        # ── Type 2 — current/active flag ──
        flag_hits = [
            n
            for n in col_names_lower
            if any(p == n or n.endswith("_" + p) or n == p for p in self._SCD_CURRENT_FLAG_NAMES)
            or n in self._SCD_CURRENT_FLAG_NAMES
        ]
        # Restrict to boolean-shape dtypes so a regular int isn't tagged.
        flag_hits = [
            n
            for n in flag_hits
            if any(
                token in str(col_lookup[n].dtype).lower()
                for token in ("bool", "char(1)", "varchar(1)")
            )
        ]
        if flag_hits:
            indicators["type2_current_flag"] = flag_hits[0]
            evidence.append(
                f"Type 2 current-flag column: `{flag_hits[0]}` "
                f"(dtype={col_lookup[flag_hits[0]].dtype})."
            )

        # ── Type 2 — version / revision column ──
        version_hits = [n for n in col_names_lower if n in self._SCD_VERSION_NAMES]
        if version_hits:
            indicators["type2_version_col"] = version_hits[0]
            evidence.append(f"Type 2 version column: `{version_hits[0]}`.")

        # ── Type 3 — paired (current_X, prev_X) columns ──
        prev_pairs: list[tuple[str, str]] = []
        for col in col_names_lower:
            for prev_p in self._SCD_PREV_PREFIXES:
                if col.startswith(prev_p):
                    base = col[len(prev_p) :]
                    # Look for the canonical sibling in the same table.
                    if base in col_names_lower:
                        prev_pairs.append((base, col))
                        break
                    # Or a new_/current_ prefix sibling.
                    for new_p in self._SCD_NEW_PREFIXES:
                        if (new_p + base) in col_names_lower:
                            prev_pairs.append((new_p + base, col))
                            break
                    break
        if prev_pairs:
            indicators["type3_prev_pairs"] = [
                {"current": cur, "previous": prev} for cur, prev in prev_pairs[:5]
            ]
            evidence.append(
                "Type 3 column pair(s): "
                + ", ".join(f"`{prev}`↔`{cur}`" for cur, prev in prev_pairs[:3])
                + "."
            )

        # ── Type 4 — sibling history table in same schema ──
        sibling_path = ""
        try:
            db = self._live_db()
            assets = (
                db.list_assets(schema_name)
                if hasattr(db, "list_assets")
                else ((n, "table") for n in db.list_tables(schema_name))
            )
            for name, _kind in assets:
                low = str(name).lower()
                for suffix in self._SCD_HISTORY_SUFFIXES:
                    if low == table_name.lower() + suffix:
                        sibling_path = f"{schema_name}.{name}"
                        break
                if sibling_path:
                    break
        except Exception:
            pass
        if sibling_path:
            indicators["type4_history_sibling"] = sibling_path
            evidence.append(
                f"Type 4 sibling history table: `{sibling_path}` exists next to the base table."
            )

        # ── Type 1 vs 2 — row-per-key probe (only if business_key given) ──
        rows_per_key: float | None = None
        if business_key:
            try:
                db = self._live_db()
                adapter = db._adapter  # noqa: SLF001
                fqn = adapter.fully_qualified_name(schema_name, table_name)
                q_cols = ", ".join(adapter.quote_identifier(c) for c in business_key)
                with db.engine.connect() as conn:
                    row = conn.execute(
                        _text(
                            f"SELECT COUNT(*) AS total, "
                            f"COUNT(DISTINCT ({q_cols})) AS distinct_keys "
                            f"FROM {fqn}"
                        ),
                    ).fetchone()
                if row and row[1]:
                    total = int(row[0] or 0)
                    distinct_keys = int(row[1])
                    rows_per_key = total / distinct_keys if distinct_keys else 0.0
                    indicators["business_key"] = list(business_key)
                    indicators["rows_per_key_avg"] = round(rows_per_key, 3)
                    indicators["total_rows"] = total
                    indicators["distinct_business_keys"] = distinct_keys
                    if rows_per_key <= 1.05:
                        evidence.append(
                            f"Avg rows-per-business-key = {rows_per_key:.2f} "
                            "→ current-only (Type 1)."
                        )
                    elif rows_per_key > 1.5:
                        evidence.append(
                            f"Avg rows-per-business-key = {rows_per_key:.2f} "
                            "→ multiple rows per key (likely Type 2)."
                        )
                    else:
                        evidence.append(
                            f"Avg rows-per-business-key = {rows_per_key:.2f} "
                            "→ ambiguous; could be Type 1 with rare history."
                        )
            except Exception as exc:
                evidence.append(f"Could not run rows-per-key probe: {exc}")

        # ── Decide hypothesis ──
        # Strongest signals win; sibling history table is the most
        # specific but we still surface other signals because real
        # systems often combine types (Type 6 = 1+2+3).
        type2_hits = (
            ("type2_temporal_pair" in indicators)
            + ("type2_current_flag" in indicators)
            + ("type2_version_col" in indicators)
            + (1 if rows_per_key is not None and rows_per_key > 1.5 else 0)
        )
        type3_hits = 1 if "type3_prev_pairs" in indicators else 0
        type4_hits = 1 if sibling_path else 0
        type1_signal = (
            rows_per_key is not None
            and rows_per_key <= 1.05
            and type2_hits == 0
            and type3_hits == 0
        )

        if type2_hits >= 2 or (type2_hits >= 1 and rows_per_key is not None and rows_per_key > 1.5):
            hypothesis = "type_2"
            confidence = "high" if type2_hits >= 2 else "medium"
        elif type3_hits and type2_hits == 0:
            hypothesis = "type_3"
            confidence = "medium"
        elif type4_hits and type2_hits == 0 and type3_hits == 0:
            hypothesis = "type_4"
            confidence = "medium"
        elif type1_signal:
            hypothesis = "type_1"
            confidence = "medium"
        elif type2_hits >= 1:
            hypothesis = "type_2"
            confidence = "low"
        else:
            hypothesis = "unknown"
            confidence = "low"
            if not evidence:
                evidence.append(
                    "No SCD-style signals found in column names or sibling "
                    "tables. The table may be append-only, fully overwritten "
                    "(Type 1), or use a custom convention."
                )

        # Alternative hypotheses — surface co-existing signals so the
        # LLM can mention "primarily Type 2 but a sibling history "
        # table also exists (so this is closer to Type 6)".
        alternatives: list[str] = []
        if hypothesis == "type_2" and type4_hits:
            alternatives.append("type_6 (Type 2 in main + Type 4 sibling = hybrid)")
        if hypothesis == "type_4" and type2_hits:
            alternatives.append("type_6 (history sibling + in-table type 2 signals = hybrid)")
        if hypothesis == "type_2" and type3_hits:
            alternatives.append("type_6 (in-table previous-value columns alongside row-history)")

        recommendation = ""
        if hypothesis == "type_2" and "type2_temporal_pair" not in indicators:
            recommendation = (
                "Type 2 inferred without an explicit valid_from/valid_to "
                "pair. To replay history at a point in time you'll need "
                "the version / current_flag column; consider asking for "
                "the load logic from your data team."
            )
        elif hypothesis == "type_1":
            recommendation = (
                "Type 1 inferred — only current values are kept. To get "
                "history you'd need a separate audit log or CDC stream."
            )
        elif hypothesis == "unknown" and not business_key:
            recommendation = (
                "No SCD signals from names/siblings. Re-call this tool "
                "with a candidate ``business_key`` so the rows-per-key "
                "probe can disambiguate Type 1 vs Type 2."
            )

        return {
            "schema": schema_name,
            "table": table_name,
            "found": True,
            "scd_type_hypothesis": hypothesis,
            "confidence": confidence,
            "evidence": evidence,
            "indicators": indicators,
            "alternative_hypotheses": alternatives,
            "recommendation": recommendation,
        }

    # ── Dimensional-role detection (v0.10.7) ───────────────────────────────

    _DIM_ROLE_NAMING: dict[str, tuple[str, ...]] = {
        # Each role lists name patterns; matched as substring on the
        # lowered table name. Order doesn't matter — every role
        # contributes to a separate naming-signal bucket.
        "fact": (
            "fact_",
            "_fact",
            "_facts",
            "fact",
            "f_",
            "_evt",
            "_event",
            "_events",
            "transactions",
            "_trans",
            "_txn",
            "_orders",
            "_sales",
            "_invoice",
            "_invoices",
        ),
        "dimension": (
            "dim_",
            "_dim",
            "_dimension",
            "dimension_",
            "_lookup",
            "lookup_",
        ),
        "staging": (
            "stg_",
            "staging_",
            "_staging",
            "_landing",
            "raw_",
            "_raw",
            "src_",
            "_src",
        ),
        "bridge": (
            "bridge_",
            "_bridge",
            "xref_",
            "_xref",
            "link_",
            "_link",
            "rel_",
            "_rel",
        ),
        "audit": (
            "_audit",
            "audit_",
            "_log",
            "log_",
            "_history",
            "history_",
            "_archive",
            "archive_",
        ),
    }

    def _name_role_signal(self, table_name: str) -> str:
        low = table_name.lower()
        for role, patterns in self._DIM_ROLE_NAMING.items():
            for pat in patterns:
                if pat in low:
                    return role
        return ""

    # ── Column-shape patterns ──
    # Numeric "measure" columns suggest a fact table (financial /
    # quantity values, summable). Both English and SAP-style names.
    _MEASURE_NAME_PATTERNS: tuple[str, ...] = (
        "_amt",
        "_amount",
        "amount_",
        "_value",
        "_qty",
        "_quantity",
        "_total",
        "_sum",
        "_price",
        "_cost",
        "_fee",
        "_rate",
        "_count",
        "_brutto",
        "_netto",
        "_revenue",
        "_profit",
        "_margin",
        "_balance",
        "_credit",
        "_debit",
        "_tax",
        # SAP-specific currency / quantity columns
        "netwr",
        "brtwr",
        "mwsbp",
        "mwsbk",
        "kbetr",
        "kwert",
        "fkimg",
        "fklmg",
        "kpein",
        "kzwi",
        "wavwr",
    )
    # ID / key columns — high count suggests a fact joining out.
    _ID_NAME_PATTERNS: tuple[str, ...] = (
        "_id",
        "id_",
        "_key",
        "_no",
        "_num",
        "_code",
        "_nr",
        "_kod",
        # SAP-specific keys appearing in many tables
        "mandt",
        "vbeln",
        "vgbel",
        "kunag",
        "kunrg",
        "kunwe",
        "lifnr",
        "vkorg",
        "vtweg",
        "spart",
        "matnr",
        "werks",
        "lgort",
        "bukrs",
        "gjahr",
        "belnr",
        "buzei",
        "fkart",
        "auart",
    )
    # Descriptive text columns — high count + low measures suggests
    # a dimension / reference table.
    _DESCRIPTIVE_NAME_PATTERNS: tuple[str, ...] = (
        "_name",
        "name_",
        "_desc",
        "_description",
        "description_",
        "_label",
        "_text",
        "text_",
        "_title",
        "_remark",
        "_note",
        "_comment",
        "_addr",
        "address_",
        "_street",
        "_city",
        # SAP-specific descriptive columns
        "ktokd",
        "kdgrp",
        "klabc",
        "konzs",
        "name1",
        "name2",
    )

    def _count_column_shape(self, profile: Any) -> dict[str, int]:
        """Count measure-like / id-like / descriptive-like columns.

        Used by ``_classify_table_role`` as a structural fallback when
        naming + FK signals are weak. Returns counts by category;
        decision logic lives in the caller.
        """
        measures = 0
        ids = 0
        descriptives = 0
        for c in profile.columns or []:
            name_low = str(c.name).lower()
            dtype_low = str(c.dtype).lower()
            is_numeric = any(
                token in dtype_low
                for token in (
                    "int",
                    "numeric",
                    "decimal",
                    "double",
                    "float",
                    "real",
                    "money",
                )
            )
            is_string = any(token in dtype_low for token in ("char", "varchar", "text", "string"))
            # Measure: numeric AND name suggests value/quantity.
            if is_numeric and any(p in name_low for p in self._MEASURE_NAME_PATTERNS):
                measures += 1
                continue
            # ID-like: any dtype, name suggests key/code (numeric or
            # short-fixed-width strings both count).
            if any(
                p == name_low or name_low.endswith(p) or name_low.startswith(p) or p in name_low
                for p in self._ID_NAME_PATTERNS
            ):
                ids += 1
                continue
            # Descriptive: string AND name suggests label/description.
            if is_string and any(p in name_low for p in self._DESCRIPTIVE_NAME_PATTERNS):
                descriptives += 1
        return {
            "measures": measures,
            "ids": ids,
            "descriptives": descriptives,
        }

    def _classify_table_role(
        self,
        profile: Any,
        peer_row_counts: list[int] | None = None,
    ) -> dict[str, Any]:
        """Classify ONE table's dimensional role from its profile.

        Combines naming signals with structural signals. ``peer_row_counts``
        is the row-count distribution of sibling tables in the same
        schema — used to compute the row-count percentile (high
        percentile → likely fact). When omitted (single-table call
        without schema context), the structural heuristic falls back
        to absolute thresholds.
        """
        from statistics import median

        evidence: list[str] = []
        indicators: dict[str, Any] = {}

        table_name = str(profile.name)
        row_count = int(profile.row_count or 0)
        fk_out = len(profile.foreign_keys or [])
        fk_in = len(profile.referenced_by or [])
        col_count = len(profile.columns or [])
        is_partitioned = bool(getattr(profile.analytics, "partition_keys", []) or [])
        has_clustering = bool(getattr(profile.analytics, "clustering_keys", []) or [])

        indicators["row_count"] = row_count
        indicators["fk_outgoing"] = fk_out
        indicators["fk_incoming"] = fk_in
        indicators["column_count"] = col_count
        indicators["is_partitioned"] = is_partitioned
        indicators["has_clustering"] = has_clustering

        # Has temporal column? (any column with date/timestamp dtype family)
        has_temporal = any(
            any(token in str(c.dtype).lower() for token in ("date", "timestamp", "datetime"))
            for c in profile.columns
        )
        indicators["has_temporal_column"] = has_temporal

        # Naming signal
        naming = self._name_role_signal(table_name)
        if naming:
            indicators["naming_signal"] = naming
            evidence.append(f"Naming pattern matches `{naming}` role.")

        # ── Column-shape signal ──
        # Counts measure-like (numeric financial / quantity) columns,
        # ID-like (key / code) columns, and descriptive (label / name)
        # columns. Lets the classifier handle SAP-style schemas with
        # opaque table names AND no declared FKs — vbrk has no naming
        # signal AND no FK constraints, but it has many numeric measures
        # (netwr / mwsbk / fkimg) + many keys (mandt / vbeln / kunag),
        # which is the column-shape signature of a fact table.
        shape = self._count_column_shape(profile)
        indicators["measure_columns"] = shape["measures"]
        indicators["id_columns"] = shape["ids"]
        indicators["descriptive_columns"] = shape["descriptives"]
        if shape["measures"] >= 3:
            evidence.append(
                f"{shape['measures']} measure-like numeric column(s) "
                "(amount / value / qty / SAP currency or quantity field) "
                "— fact-like column shape."
            )
        if shape["ids"] >= 4:
            evidence.append(
                f"{shape['ids']} ID / key / code column(s) — joins out "
                "to many entities (fact-like) or composite-key (bridge-like)."
            )
        if shape["descriptives"] >= 5 and shape["measures"] == 0:
            evidence.append(
                f"{shape['descriptives']} descriptive (name / label / "
                "description) column(s) and no measures — dimension / "
                "reference shape."
            )

        # Row-count percentile vs peers (if peers provided)
        rc_percentile: float | None = None
        if peer_row_counts and len(peer_row_counts) >= 3:
            sorted_peers = sorted(peer_row_counts)
            rank = sum(1 for n in sorted_peers if n <= row_count)
            rc_percentile = rank / len(sorted_peers)
            indicators["row_count_percentile"] = round(rc_percentile, 3)
            med = median(sorted_peers)
            indicators["peer_row_count_median"] = int(med)
            if row_count > med * 5 and row_count > 1000:
                evidence.append(
                    f"Row count {row_count:,} is >5× the schema median "
                    f"({int(med):,}) — likely fact / transactional."
                )
            elif row_count <= 1000 and col_count <= 10:
                evidence.append(
                    f"Small table ({row_count} rows, {col_count} cols) — likely lookup / reference."
                )

        # FK fan-out / fan-in
        if fk_out >= 3:
            evidence.append(
                f"{fk_out} outgoing FK(s) — likely fact (joins out to many dimensions)."
            )
        if fk_in >= 3:
            evidence.append(
                f"{fk_in} incoming FK(s) — likely dimension (referenced by many tables)."
            )

        # Bridge: roughly equal in/out, both ≥ 2
        is_bridge = fk_out >= 2 and fk_in >= 2 and abs(fk_out - fk_in) <= 1

        # Decide the hypothesis. Naming wins for staging / audit / bridge
        # (strong intent); structural wins for fact / dimension / lookup.
        hypothesis = "unknown"
        confidence = "low"

        if naming == "staging":
            hypothesis = "staging"
            confidence = "high"
        elif naming == "audit":
            hypothesis = "audit"
            confidence = "high"
        elif naming == "bridge" or is_bridge:
            hypothesis = "bridge"
            confidence = "medium" if naming == "bridge" else "low"
            if is_bridge and naming != "bridge":
                evidence.append(
                    f"Roughly equal FK fan-out ({fk_out}) and fan-in "
                    f"({fk_in}) — bridge / link table shape."
                )
        elif naming == "fact":
            hypothesis = "fact"
            confidence = (
                "high"
                if (fk_out >= 2 or rc_percentile is not None and rc_percentile >= 0.75)
                else "medium"
            )
        elif naming == "dimension":
            hypothesis = "dimension"
            confidence = "high" if fk_in >= 1 else "medium"
        else:
            # Pure structural inference. Order matters — the
            # column-shape signal (measures + ids) wins for SAP /
            # FK-free schemas because that's the only ground truth
            # left when naming is opaque AND constraints aren't
            # declared.
            if (
                fk_out >= 3
                and (rc_percentile is None or rc_percentile >= 0.6)
                and (is_partitioned or has_temporal)
            ):
                hypothesis = "fact"
                confidence = "medium"
                evidence.append(
                    "No naming signal; classified by structure (high FK "
                    "fan-out + temporal/partitioned)."
                )
            elif (
                shape["measures"] >= 3
                and shape["ids"] >= 4
                and (has_temporal or row_count >= 10_000)
            ):
                # Column-shape fact heuristic — fires when FK
                # constraints are absent (typical SAP) but the column
                # mix screams "transactional with measures + foreign
                # keys at the application layer".
                hypothesis = "fact"
                confidence = "medium"
                evidence.append(
                    f"No FK / naming signal; column-shape shows "
                    f"{shape['measures']} measure(s) + {shape['ids']} "
                    f"key(s) + temporal — fact-shaped row."
                )
            elif fk_in >= 3 and fk_out <= 1:
                hypothesis = "dimension"
                confidence = "medium"
                evidence.append(
                    "No naming signal; classified by structure (high FK fan-in, low fan-out)."
                )
            elif shape["descriptives"] >= 5 and shape["measures"] == 0 and row_count <= 100_000:
                # Column-shape dimension heuristic — many descriptive
                # columns + no measures + moderate row count.
                hypothesis = "dimension"
                confidence = "medium"
                evidence.append(
                    f"Column-shape dimension: {shape['descriptives']} "
                    "descriptive column(s) + no measures + moderate row "
                    "count."
                )
            elif row_count <= 1000 and col_count <= 12 and fk_in >= 1:
                hypothesis = "lookup"
                confidence = "medium"
                evidence.append("Small + referenced — likely lookup / reference table.")
            elif has_temporal and not (is_partitioned or fk_out):
                hypothesis = "transactional"
                confidence = "low"
                evidence.append(
                    "Temporal column present but no partitioning / FKs out "
                    "— likely raw transactional / event log."
                )

        if not evidence:
            evidence.append(
                "No strong signals — naming, FK structure, and row count "
                "are all ambiguous. Try providing the schema context "
                "(rank-all-tables mode) or run the SCD detector if "
                "history shape matters."
            )

        return {
            "schema": str(profile.schema),
            "table": table_name,
            "role_hypothesis": hypothesis,
            "confidence": confidence,
            "evidence": evidence,
            "indicators": indicators,
        }

    def _tool_detect_dimensional_role(
        self,
        schema: str,
        table: str | None = None,
    ) -> dict[str, Any]:
        """Single-table or schema-wide dimensional-role classifier.

        See the tool description for the full contract; this body just
        dispatches between per-table and schema-level classification.
        """
        schema_name = (schema or "").strip()
        if not schema_name:
            raise _ToolError("Argument 'schema' is required.")

        # ── Single-table mode ──
        if table:
            try:
                profile = self._live_db().profile_table(
                    schema_name,
                    table.strip(),
                    sample_size=0,
                )
            except Exception as exc:
                return {
                    "schema": schema_name,
                    "table": table,
                    "found": False,
                    "error": str(exc),
                }
            return {**self._classify_table_role(profile), "found": True}

        # ── Schema-level mode ──
        # Walk every asset in the schema, profile cheaply (no samples,
        # no large stats), classify, then derive the schema-level
        # pattern (star vs snowflake) from FK relationships among the
        # classified dimensions.
        db = self._live_db()
        try:
            if hasattr(db, "list_assets"):
                assets = [(str(n), str(k)) for n, k in db.list_assets(schema_name)]
            else:
                assets = [(str(n), "table") for n in db.list_tables(schema_name)]
        except Exception as exc:
            return {
                "schema": schema_name,
                "found": False,
                "error": f"Could not list tables in schema: {exc}",
            }
        if not assets:
            return {
                "schema": schema_name,
                "found": False,
                "table_count": 0,
                "tables_by_role": {},
                "pattern_hypothesis": "unknown",
                "evidence": [
                    "Schema has no tables to classify.",
                ],
            }

        # First pass: profile all tables to collect row counts (for
        # percentile) + FK info. Profiles WITHOUT samples are cheap.
        per_table: list[Any] = []
        peer_row_counts: list[int] = []
        for name, _kind in assets:
            try:
                p = db.profile_table(schema_name, name, sample_size=0)
                per_table.append(p)
                peer_row_counts.append(int(p.row_count or 0))
            except Exception:
                continue

        # Second pass: classify each with peer-row-count context.
        classifications: list[dict[str, Any]] = []
        # Build a (schema, table) → role lookup so we can later check
        # whether a dimension references another dimension (snowflake).
        for p in per_table:
            classifications.append(self._classify_table_role(p, peer_row_counts))

        role_to_paths: dict[str, list[str]] = {}
        for c in classifications:
            role = c["role_hypothesis"]
            role_to_paths.setdefault(role, []).append(f"{c['schema']}.{c['table']}")

        # Star vs snowflake — only meaningful if BOTH facts and
        # dimensions exist. Snowflake = at least one dimension references
        # another dimension. Star = dimensions are flat (only referenced
        # by facts, no FKs to other dimensions).
        pattern = "unknown"
        pattern_evidence: list[str] = []
        fact_paths = set(role_to_paths.get("fact", []))
        dim_paths = set(role_to_paths.get("dimension", []))
        if fact_paths and dim_paths:
            dim_to_dim_links = 0
            dim_to_dim_examples: list[str] = []
            for p in per_table:
                if f"{p.schema}.{p.name}" not in dim_paths:
                    continue
                for fk in p.foreign_keys or []:
                    target = (
                        f"{fk.get('referred_schema') or p.schema}.{fk.get('referred_table') or ''}"
                    )
                    if target in dim_paths and target != f"{p.schema}.{p.name}":
                        dim_to_dim_links += 1
                        if len(dim_to_dim_examples) < 3:
                            dim_to_dim_examples.append(f"{p.schema}.{p.name} → {target}")
            if dim_to_dim_links:
                pattern = "snowflake_schema"
                pattern_evidence.append(
                    f"{dim_to_dim_links} dimension-to-dimension FK link(s) "
                    "found (snowflake): " + ", ".join(dim_to_dim_examples)
                )
            else:
                pattern = "star_schema"
                pattern_evidence.append(
                    f"{len(fact_paths)} fact table(s) and "
                    f"{len(dim_paths)} dimension table(s); no "
                    "dimension-to-dimension FKs (star layout)."
                )
        elif not fact_paths and dim_paths:
            pattern = "flat"
            pattern_evidence.append(
                "No fact-shaped tables; the schema looks like a "
                "denormalised dim-only or reference layout."
            )
        elif fact_paths and not dim_paths:
            pattern = "fact_only"
            pattern_evidence.append(
                "Fact tables present but no dimension-shaped tables "
                "found — possibly an OBT (one-big-table) layout."
            )

        return {
            "schema": schema_name,
            "found": True,
            "table_count": len(per_table),
            "pattern_hypothesis": pattern,
            "pattern_evidence": pattern_evidence,
            "tables_by_role": role_to_paths,
            "fact_tables": role_to_paths.get("fact", []),
            "dimension_tables": role_to_paths.get("dimension", []),
            "bridge_tables": role_to_paths.get("bridge", []),
            "lookup_tables": role_to_paths.get("lookup", []),
            "staging_tables": role_to_paths.get("staging", []),
            "audit_tables": role_to_paths.get("audit", []),
            "transactional_tables": role_to_paths.get("transactional", []),
            "unknown_tables": role_to_paths.get("unknown", []),
            "classifications": classifications,
        }

    # ── Past-runs introspection (the /history compare data, but for /ask) ─

    def _tool_list_past_runs(
        self,
        schema: str = "",
        table: str = "",
        command: str = "",
        limit: int = 10,
    ) -> dict[str, Any]:
        """List the user's past ``/run`` invocations from the local SQLite history.

        Defaults to ``analyze.run`` only — matches the user's mental
        model where "runs" means data-analysis invocations and "asks"
        are conversational chats listed by ``list_chat_sessions``.
        Each row carries human-friendly fields (``started_at`` ISO
        string, ``duration_human``) plus the raw epoch / float for
        machine consumption, so the LLM can produce a clean answer
        without doing arithmetic on ``1777675166.705911``.
        """
        import datetime as _dt

        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None:
            return {
                "runs": [],
                "count": 0,
                "note": (
                    "No local history store is initialised in this process. "
                    "This usually means /ask was invoked outside the standard CLI; "
                    "tell the user we can't introspect past runs in that context."
                ),
            }
        clamped_limit = max(1, min(int(limit) if limit else 10, 50))
        # Default to /run history. The LLM (or the user) must explicitly
        # ask for ``search.ask`` or ``all`` to widen the filter — see the
        # tool description and the system prompt's routing rule.
        raw_cmd = command.strip().lower() if command else ""
        if raw_cmd in ("", "analyze.run", "run"):
            cmd: str | None = "analyze.run"
        elif raw_cmd in ("search.ask", "ask"):
            cmd = "search.ask"
        elif raw_cmd == "all":
            cmd = None
        else:
            return {
                "error": (
                    f"Invalid 'command' filter {command!r} — must be 'analyze.run', "
                    "'search.ask', or 'all'. Omit the parameter for /run history "
                    "(the most common case)."
                )
            }

        try:
            rows = hs.find_runs_for_scope(
                schema=(schema.strip() or None) if schema else None,
                table=(table.strip() or None) if table else None,
                command_filter=cmd,
                limit=clamped_limit,
            )
        except Exception as exc:
            return {"error": f"Could not query history: {exc}"}

        def _human_duration(sec: float) -> str:
            if sec is None or sec <= 0:
                return "—"
            s = float(sec)
            if s < 60:
                return f"{s:.1f}s"
            m, rem = divmod(s, 60)
            return f"{int(m)}m {rem:0.0f}s"

        def _iso(epoch: float) -> str:
            try:
                return _dt.datetime.fromtimestamp(float(epoch or 0)).strftime("%Y-%m-%d %H:%M")
            except Exception:
                return "—"

        compact: list[dict[str, Any]] = []
        for r in rows:
            metrics = r.get("metrics_json") if isinstance(r, dict) else None
            if not isinstance(metrics, dict):
                metrics = {}
            tokens = r.get("tokens_json") if isinstance(r, dict) else None
            total_tokens = 0
            if isinstance(tokens, dict):
                try:
                    total_tokens = int(tokens.get("total_tokens") or 0)
                except (TypeError, ValueError):
                    total_tokens = 0
            duration = float(r.get("duration_sec") or 0.0)
            model_proc = float(metrics.get("model_processing_sec") or 0.0)
            compact.append(
                {
                    "run_id": int(r.get("id") or 0),
                    "started_at": _iso(r.get("started_at")),
                    "started_at_epoch": float(r.get("started_at") or 0.0),
                    "duration_human": _human_duration(duration),
                    "duration_sec": round(duration, 2),
                    "model_processing_human": _human_duration(model_proc),
                    "model_processing_sec": round(model_proc, 2),
                    "status": r.get("status") or "",
                    "command": r.get("command") or "",
                    "scope": r.get("scope_json") or {},
                    "db_profile": r.get("db_profile") or "",
                    "llm_profile": r.get("llm_profile") or "",
                    "llm_model": r.get("llm_model") or "",
                    "doc_profile": r.get("doc_profile") or "",
                    "code_profile": r.get("code_profile") or "",
                    "settings": r.get("settings_json") or {},
                    "selected_count": int(r.get("selected_count") or 0),
                    "processed_count": int(r.get("processed_count") or 0),
                    "applied_count": int(r.get("applied_count") or 0),
                    "total_tokens": total_tokens,
                }
            )

        return {
            "runs": compact,
            "count": len(compact),
            "filter": {
                "schema": schema or "",
                "table": table or "",
                "command": cmd or "all",
                "limit": clamped_limit,
            },
            "presentation_hint": (
                "When the user asks for a table, render a Rich-friendly compact table "
                "with at most 6 columns: Run ID, Started, Duration, Status, LLM model, "
                "Total tokens. Use the human-readable fields (started_at, "
                "duration_human) — never the raw epoch or raw float seconds. Quote "
                "longer fields (scope, settings) inline as text below the table."
            ),
        }

    def _tool_describe_run(
        self,
        run_id: int,
        include_results: bool = True,
    ) -> dict[str, Any]:
        """Return the full record for one past run, optionally with results."""
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None:
            return {
                "error": (
                    "No local history store is initialised in this process. "
                    "Cannot describe past runs."
                )
            }
        try:
            rid = int(run_id)
        except (TypeError, ValueError):
            return {"error": f"Invalid run_id {run_id!r} — must be an integer."}

        try:
            row = hs.get_run(rid)
        except Exception as exc:
            return {"error": f"Failed to load run #{rid}: {exc}"}
        if row is None:
            return {"error": f"Run #{rid} not found in history.db."}

        out: dict[str, Any] = {
            "run_id": rid,
            "started_at_epoch": float(row.get("started_at") or 0.0),
            "ended_at_epoch": float(row.get("ended_at") or 0.0),
            "duration_sec": float(row.get("duration_sec") or 0.0),
            "status": row.get("status") or "",
            "command": row.get("command") or "",
            "mode": row.get("mode") or "",
            "scope": row.get("scope_json") or {},
            "db_profile": row.get("db_profile") or "",
            "db_backend": row.get("db_backend") or "",
            "llm_profile": row.get("llm_profile") or "",
            "llm_provider": row.get("llm_provider") or "",
            "llm_model": row.get("llm_model") or "",
            "doc_profile": row.get("doc_profile") or "",
            "code_profile": row.get("code_profile") or "",
            "settings": row.get("settings_json") or {},
            "metrics": row.get("metrics_json") or {},
            "tokens": row.get("tokens_json") or {},
            "selected_count": int(row.get("selected_count") or 0),
            "planned_count": int(row.get("planned_count") or 0),
            "processed_count": int(row.get("processed_count") or 0),
            "applied_count": int(row.get("applied_count") or 0),
            "review_strategy": row.get("review_strategy") or "",
            "error_text": row.get("error_text") or "",
        }

        if include_results:
            try:
                results = hs.get_run_results(rid)
            except Exception as exc:
                results = []
                out["results_warning"] = f"Could not load run_results: {exc}"
            # Compact the results for LLM consumption — drop heavy raw
            # fields the model rarely needs.
            out["results"] = [
                {
                    "schema": r.get("schema_name") or "",
                    "table": r.get("table_name") or "",
                    "column": r.get("column_name") or "",
                    "asset_kind": r.get("asset_kind") or "table",
                    "source": r.get("source") or "",
                    "confidence": r.get("confidence") or "",
                    "logprob_score": r.get("logprob_score"),
                    "token_count": r.get("token_count"),
                    "model_version": r.get("model_version") or "",
                    "chosen_description": r.get("chosen_description") or "",
                    "evaluation": r.get("evaluation") or "",
                    "alternatives": (
                        r.get("alternatives_json")
                        if isinstance(r.get("alternatives_json"), list)
                        else []
                    ),
                }
                for r in results
            ]
            out["results_count"] = len(out["results"])
        return out

    def _tool_list_chat_sessions(
        self,
        limit: int = 20,
        include_ended: bool = True,
    ) -> dict[str, Any]:
        """List the user's past ``/ask`` chat sessions (resumable conversations).

        ``/ask`` invocations form a stateful conversation thread (the
        chat_sessions / chat_turns SQLite tables). Each row here
        carries the session id, when it started, last activity time,
        whether it's still open, turn count, total tokens, and the
        first user question as a preview. Tell the user they can
        resume any ended session via ``/session resume <id>``.

        Use this — NOT ``list_past_runs(command="search.ask")`` —
        when the user asks "show me my past chats" / "my ask history"
        / "previous /ask conversations". The two surfaces store the
        same conceptual data differently: ``analysis_runs`` rows for
        ``search.ask`` are PER-TURN audit log entries (one per
        question asked); ``chat_sessions`` rows are PER-CONVERSATION
        threads. Users almost always want the latter.
        """
        import datetime as _dt

        from amx.search.session_store import ChatSessionStore
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None:
            return {
                "sessions": [],
                "count": 0,
                "note": "No local history store is initialised in this process.",
            }

        clamped_limit = max(1, min(int(limit) if limit else 20, 100))
        try:
            rows = ChatSessionStore(hs).list_sessions(
                limit=clamped_limit,
                include_ended=bool(include_ended),
            )
        except Exception as exc:
            return {"error": f"Could not query chat sessions: {exc}"}

        def _iso(epoch: Any) -> str:
            try:
                v = float(epoch or 0)
                if v <= 0:
                    return "—"
                return _dt.datetime.fromtimestamp(v).strftime("%Y-%m-%d %H:%M")
            except Exception:
                return "—"

        sessions: list[dict[str, Any]] = []
        for row in rows:
            ended_epoch = row.get("ended_at")
            sessions.append(
                {
                    "session_id": int(row.get("id") or 0),
                    "db_profile": row.get("db_profile") or "",
                    "llm_profile": row.get("llm_profile") or "",
                    "started_at": _iso(row.get("started_at")),
                    "last_active_at": _iso(row.get("last_active_at")),
                    "ended_at": _iso(ended_epoch) if ended_epoch else "",
                    "is_active": ended_epoch is None,
                    "title": row.get("title") or "",
                    "turn_count": int(row.get("turn_count") or 0),
                    "total_tokens": int(row.get("total_tokens") or 0),
                    "first_question": row.get("first_question") or "",
                }
            )

        return {
            "sessions": sessions,
            "count": len(sessions),
            "note": (
                "Resume any ended session in the CLI with `/session resume <id>`. "
                "Active sessions (is_active=true) are the currently-open thread."
            ),
        }
