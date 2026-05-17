"""JSON tool schemas passed to the LLM by ``ToolBox.schemas()``.

This module holds the data definition only — names, descriptions, and
JSON-schema argument shapes for every tool the ``/ask`` agent can call.

The list is duplicated (deep-copied) on every access via ``tool_schemas()``
so callers can mutate it without poisoning the shared source. The
existing ``ToolBox.schemas()`` static method delegates here.

Each entry carries a sibling ``freshness`` annotation:

* ``"cache_ok"`` — the tool answers from the catalog cache for the
  common path. Always available, including in cache-only Ask mode.
* ``"live_only"`` — the tool's body can only answer with a live-DB
  round-trip. Hidden from the LLM's menu when Ask is in cache-only
  mode so the agent never proposes a call we'd refuse anyway.

The key is a sibling of ``type`` / ``function`` on each entry. OpenAI's
function-calling JSON schema ignores unknown top-level keys, so this is
pure metadata.
"""

from __future__ import annotations

import copy
from typing import Any

#: Freshness annotation values — exported as a constant so the ToolBox
#: filter and the test suite agree on the literal strings.
FRESHNESS_CACHE_OK = "cache_ok"
FRESHNESS_LIVE_ONLY = "live_only"

# The single source of truth. Defined as a module-level constant so it is
# constructed exactly once at import time; ``tool_schemas()`` returns a
# fresh deep copy on each call to preserve the historical behaviour of
# the in-class literal ``return [...]``.
_TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "type": "function",
        "freshness": FRESHNESS_CACHE_OK,
        "function": {
            "name": "catalog_sync_status",
            "description": (
                "Return the catalog's sync state and last-sync timestamp for "
                "every DB profile in the current Ask scope. Read directly "
                "from ``catalog_profile_state`` — no live database query, "
                "one round-trip, ~30 ms.\n\n"
                "Call this FIRST whenever the user asks any variation of "
                "'are my tables synced / up to date / fresh / stale', 'is my "
                "catalog still good', 'when did we last sync', or any other "
                "freshness question. Answer directly from the response; do "
                "NOT chain ``list_schemas`` / ``list_tables_in_schema`` / "
                "``describe_table`` to 'verify' — the catalog state IS the "
                "answer.\n\n"
                "Response: ``{\"profiles\": [{\"db_profile\", \"state\", "
                "\"last_synced_at\", \"age_seconds\", \"is_fresh_24h\", "
                "\"is_fresh_7d\", \"processed_tables\", \"total_tables\", "
                "\"last_error\"}]}``."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "db_profile": {
                        "type": "string",
                        "description": (
                            "Optional: limit the response to a single "
                            "profile. Omit to get every profile in scope."
                        ),
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "freshness": FRESHNESS_CACHE_OK,
        "function": {
            "name": "list_schemas",
            "description": (
                "Return the list of schema names (namespaces) visible in the active "
                "database. Use this when the user asks 'which schemas do we have?', "
                "'what schemas exist?', 'what kind of schema is sap_test?', or as a discovery "
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
                    "db_profile": {
                        "type": "string",
                        "description": (
                            "Optional DB profile to target. Omit when the scope is "
                            "single-profile, OR when you want a multi-profile fan-out "
                            "(the tool then queries every profile in scope in parallel "
                            "and returns a per-profile breakdown)."
                        ),
                    },
                    "force_fresh": {
                        "type": "boolean",
                        "description": (
                            "Default ``false`` — the tool reads the schema list from "
                            "the catalog when /search sync has covered the profile. "
                            "Set ``true`` when the user explicitly asks for the "
                            "current live state (after creating a new schema, post-"
                            "ALTER, etc.). Response carries ``source`` "
                            "(``catalog``/``live``) and ``age_seconds`` so you can "
                            "judge staleness."
                        ),
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "freshness": "cache_ok",
        "function": {
            "name": "list_tables_in_schema",
            "description": (
                "Return the tables, views, and materialized views inside a given "
                "schema. Use this when the user asks 'what tables are under sap_test?', "
                "'list all tables in sap_s6p', or to disambiguate a bare table name. "
                "Pass ``catalog`` to scope the listing to a Unity-Catalog catalog the "
                "active profile has not pinned. Pass ``db_profile`` to target a "
                "specific profile when scope is multi-profile (otherwise all profiles "
                "in scope are queried in parallel and the result is per-profile)."
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
                    "db_profile": {
                        "type": "string",
                        "description": (
                            "Optional DB profile to target. Omit for multi-profile "
                            "fan-out: every profile in scope is queried in parallel "
                            "and the result groups by profile."
                        ),
                    },
                    "force_fresh": {
                        "type": "boolean",
                        "description": (
                            "Default ``false`` — table list served from the catalog "
                            "when /search sync covered the schema. Set ``true`` for "
                            "the live current state (post-CREATE, post-DROP). "
                            "Response carries ``source`` and ``age_seconds``."
                        ),
                    },
                },
                "required": ["schema"],
            },
        },
    },
    {
        "type": "function",
        "freshness": "cache_ok",
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
                "'is the table called vbap or vbpa?'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Exact table name. Case-insensitive.",
                    },
                    "force_fresh": {
                        "type": "boolean",
                        "description": (
                            "Default ``false``. Catalog hits are always consulted "
                            "first; the cross-DB live sweep runs in addition to "
                            "the catalog and writes discovery rows back to the "
                            "24h cache so a follow-up describe_table doesn't "
                            "sweep again. Set ``true`` to also bypass the "
                            "schemas / databases caches — usually unnecessary, "
                            "only when you suspect the live server changed "
                            "very recently."
                        ),
                    },
                },
                "required": ["name"],
            },
        },
    },
    {
        "type": "function",
        "freshness": "cache_ok",
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
                    "catalog": {
                        "type": "string",
                        "description": (
                            "Optional Unity-Catalog catalog (Databricks) or "
                            "BigQuery project. Omit to use whatever the active "
                            "profile pins; the tool will auto-pick the single "
                            "user catalog when no catalog is pinned."
                        ),
                    },
                    "db_profile": {
                        "type": "string",
                        "description": (
                            "REQUIRED when scope is multi-profile and the table "
                            "exists in more than one profile (resolve ambiguity by "
                            "naming the profile). Optional otherwise."
                        ),
                    },
                    "database": {
                        "type": "string",
                        "description": (
                            "Optional database name (PostgreSQL / MySQL / SQL "
                            "Server). When the active profile has no pinned "
                            "database, set this to the database the table lives "
                            "in (find_table_by_name reports it under "
                            "``resolved_database``). Omit on 3-level backends "
                            "(Databricks / BigQuery) — use ``catalog`` there."
                        ),
                    },
                    "force_fresh": {
                        "type": "boolean",
                        "description": (
                            "Bypass the catalog + 24h live cache and run a fresh "
                            "live profile_table. Default ``false`` — the tool "
                            "serves from cache when warm and writes back on a "
                            "live miss so the next call is free. Set ``true`` "
                            "ONLY when the user explicitly asks for the current "
                            "live state ('what does it look like right now', "
                            "'after my last apply', 'fresh from the warehouse') "
                            "or when the response's ``age_seconds`` is too old "
                            "to trust for the question being asked. The result "
                            "carries ``source`` (``catalog``/``live_cache``/"
                            "``live``) and ``age_seconds`` so you can decide."
                        ),
                    },
                },
                "required": ["schema", "table"],
            },
        },
    },
    {
        "type": "function",
        "freshness": "cache_ok",
        "function": {
            "name": "search_tables_by_concept",
            "description": (
                "Semantic / lexical search over the catalog for tables whose names or "
                "comments relate to a business concept (pricing, customer, address, "
                "billing, ...). Returns a CANDIDATE SET — read each row's description "
                "and filter false positives before composing your answer. Use for "
                "'tables about pricing', 'tables related to customers', 'find tables"
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
        "freshness": "cache_ok",
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
        "freshness": "cache_ok",
        "function": {
            "name": "get_join_candidates",
            "description": (
                "Return likely join columns between two tables (verified foreign keys "
                "first, semantic-similarity candidates after). Use this for "
                "'how do X and Y join?', 'what columns connect X and Y?'."
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
        "freshness": "live_only",
        "function": {
            "name": "list_databases",
            "description": (
                "List EVERY database (or catalog, on 3-level backends) reachable "
                "across the configured DB profiles. Fans out per profile in "
                "parallel and returns ``profiles: {name: {databases|catalogs, "
                "pinned_database, pinned_catalog, supports_catalogs}}``. Use "
                "this when the user asks 'which databases do I have?', 'what "
                "databases exist?', 'show me all databases', 'what's in each "
                "profile?'. The result enumerates the full reach of every "
                "connection — NOT just the database currently pinned in each "
                "profile's config. When composing the answer, list ALL entries "
                "per profile, grouped by profile name; cite per-profile counts. "
                "Profiles that errored / timed out are reported in "
                "``profiles_with_errors`` so the caveat is honest.\n\n"
                "Set ``with_counts=true`` when the user asks 'which tables can "
                "we reach', 'how many tables / schemas do I have', or any "
                "coverage / rollup question — each database/catalog entry is "
                "then ``{name, schema_count, table_count}`` and the result "
                "gains per-profile ``total_schemas`` / ``total_tables`` plus "
                "grand totals ``grand_total_schemas`` / ``grand_total_tables``. "
                "Use these numbers DIRECTLY for the STATS-EXAMPLE-DRILL stats "
                "line — no follow-up tool call needed. Skip ``with_counts`` "
                "when the user only asks for names ('list my databases'); the "
                "count fan-out is per-database and adds latency."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "with_counts": {
                        "type": "boolean",
                        "description": (
                            "If true, enrich every database/catalog entry "
                            "with ``schema_count`` + ``table_count`` and "
                            "compute per-profile and grand totals. Default "
                            "false (names only)."
                        ),
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "freshness": "live_only",
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
        "freshness": "live_only",
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
        "freshness": "live_only",
        "function": {
            "name": "list_volumes",
            "description": (
                "Run ``SHOW VOLUMES`` against a Databricks Unity Catalog schema (or "
                "every schema in the active catalog when ``schema`` is omitted) and "
                "return the user's managed + external volumes. Volumes are a "
                "Databricks-distinctive object type that lives alongside tables in "
                "the catalog/schema namespace and points at managed or external file "
                "storage; AMX's regular table-listing tools do NOT surface them. Use "
                "this when the user asks 'do we have any volumes', 'which volumes "
                "exist under <schema>', 'are there external volumes', 'volumelar "
                "neler', etc. Returns ``supported=false`` for backends without a "
                "volume concept (everything except Databricks). Catalog auto-pick "
                "follows the same rule as list_schemas — pinned profile catalog "
                "wins, otherwise we auto-pick the single non-system user catalog."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": (
                            "Optional schema to scope the listing to. Omit to scan "
                            "every schema in the active catalog (one SHOW VOLUMES "
                            "per schema)."
                        ),
                    },
                    "catalog": {
                        "type": "string",
                        "description": (
                            "Optional Unity-Catalog catalog. Omit to use whatever "
                            "the active profile pins; the tool auto-picks the "
                            "single non-system user catalog when nothing is pinned."
                        ),
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "freshness": "cache_ok",
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
        "freshness": "cache_ok",
        "function": {
            "name": "find_assets_missing_comment",
            "description": (
                "Return tables and/or columns that have NO comment in the live "
                "database (queries the DB directly, NOT the catalog). Use this for "
                "'are there any tables without a description?', 'which tables are "
                "missing comments?', 'tables without descriptions', 'undocumented assets'. "
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
        "freshness": "live_only",
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
        "freshness": "live_only",
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
                "star schema?', 'what is the main table of this schema?', "
                "'which are the fact and dimension tables?'."
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
        "freshness": "live_only",
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
                "SCD2?', 'how are old values stored?', "
                "'are changes kept in the same row or in new rows?'. "
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
        "freshness": "live_only",
        "function": {
            "name": "sample_column_values",
            "description": (
                "Return a few non-null example values from a single column "
                "via a direct ``SELECT col FROM schema.table WHERE col IS "
                "NOT NULL LIMIT N``. Cheap (no profile, no full-table scan, "
                "no catalog round-trip) and ground-truth (live DB). "
                "Use this for 'give me a sample / example value', 'what "
                "does column X look like', 'show me a value from aedat', "
                "'is the date format YYYYMMDD, show me a sample', 'what "
                "do column values look like'. ALWAYS resolve the table via "
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
        "freshness": "live_only",
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
                "in email column?', 'is the date format ddmmyyyy?', 'is "
                "the data continuous since when?' (read min_value of "
                "the date column), 'duplication ratio', 'are there gaps "
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
        "freshness": "live_only",
        "function": {
            "name": "find_joinable_tables",
            "description": (
                "Given ONE table, return the tables it can be joined with. "
                "Default ``strategy='auto'`` cascades four METADATA tiers cheap-to-"
                "expensive: (1) declared foreign keys, (2) rarity-weighted name "
                "overlap (with a live information_schema rescue when the catalog is "
                "stale for this table — SAP/legacy schemas typically land here), "
                "(3) semantic similarity over column descriptions. The result "
                "includes ``inference_source`` and ``strategies_tried`` so you can "
                "see which tier won (or that every tier returned empty — in that "
                "case ``inference_source`` is ``null``). "
                "If the metadata tiers come back empty OR you need DATA-LEVEL "
                "proof a join actually works, re-call with "
                "``strategy='value_overlap'`` — this hits the database, samples "
                "distinct values from both sides of each name-overlap candidate, "
                "and reports an ``overlap_count`` + ``overlap_ratio`` per row. "
                "Use ``strategy='all'`` to run every tier and get a merged list "
                "with per-row ``inference_sources``. "
                "When you compose the final answer, ALWAYS state the inference "
                "source explicitly so the user knows whether the join is FK-"
                "verified, name-inferred, or value-verified. Do NOT fall back to "
                "your own domain knowledge to fill the list — call this tool again "
                "with a stronger strategy instead. "
                "Use for 'which tables can I join with vbrk?', 'tables that can "
                "join with X', 'find tables related to vbrk'. Different from "
                "get_join_candidates which needs both sides upfront. WITHIN-"
                "PROFILE only: for cross-profile JOIN candidates call "
                "find_joinable_across_profiles."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "Table as schema.table or just table_name (we'll resolve via find_table_by_name first).",
                    },
                    "strategy": {
                        "type": "string",
                        "enum": [
                            "auto",
                            "foreign_key",
                            "name_overlap",
                            "semantic",
                            "value_overlap",
                            "all",
                        ],
                        "description": (
                            "Which inference tier(s) to run. ``auto`` (default) "
                            "cascades FK → name_overlap → semantic. Individual "
                            "names run only that tier. ``value_overlap`` samples "
                            "actual column values to verify joinability (one "
                            "extra DB round-trip per candidate, opt-in). ``all`` "
                            "runs everything and merges results."
                        ),
                    },
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "freshness": "live_only",
        "function": {
            "name": "find_joinable_across_profiles",
            "description": (
                "CROSS-PROFILE join finder. Given ONE table on ONE profile, return "
                "ranked candidate columns from EVERY OTHER profile in scope that "
                "could be joined to it. Combines four signals: (1) column name "
                "token overlap (e.g. customer_id ↔ cust_id), (2) dtype "
                "compatibility (INT↔BIGINT OK, VARCHAR↔INT not), (3) vector "
                "similarity on column descriptions/names — multi-profile aware, "
                "(4) FK pattern heuristic (sender ends in `_id`, target column is "
                "PK or unique). Each candidate carries a 0-1 score and a "
                "``signal_breakdown`` so the LLM can caveat the answer. "
                "Aggressive by design — accepts that BYO-LLM cost and a few "
                "seconds of latency are acceptable for high-recall cross-DB "
                "discovery. RULE: scores ≥ 0.65 are usually genuine joins; 0.40-"
                "0.65 are weak guesses (caveat them); < 0.40 means the column "
                "names happen to overlap by coincidence and you should NOT "
                "recommend the join. Cite (source_profile.schema.table.column → "
                "target_profile.schema.table.column) explicitly."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": (
                            "Source table as ``profile::schema.table`` to lock the "
                            "source profile, or ``schema.table`` / ``table`` to "
                            "auto-resolve from the active anchor profile."
                        ),
                    },
                    "k": {
                        "type": "integer",
                        "description": (
                            "Max candidates to return across all target profiles "
                            "(default 12, cap 50)."
                        ),
                    },
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "freshness": "cache_ok",
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
        "freshness": "cache_ok",
        "function": {
            "name": "describe_run",
            "description": (
                "Return the full record for one past run by ID — settings snapshot, "
                "every per-column suggestion the LLM produced (top description + "
                "alternatives, confidence band, logprob_score, token_count), and the "
                "review decisions the user made. Use this AFTER list_past_runs has "
                "narrowed the candidate set, when the user wants details on a specific "
                "run ('show me run 42', 'what did the LLM suggest for adr6 in run 17', "
                "'why is run 13's avg logprob higher than run 12's'). "
                "Each result row carries an `applied` boolean and an `applied_at` "
                "epoch; the top-level `applied_columns` list pre-filters the rows that "
                "were actually committed to the database — quote it verbatim when the "
                "user asks which columns were applied. When summarising a run, render "
                "results as a single Markdown table with columns "
                "`Applied | Column | Confidence | Description` (use ✅ for applied "
                "rows, ⏭️ for proposed-but-not-applied rows). Lead with a header line: "
                "`Run #<id> — <status> (<applied_count> of <results_count> applied)`. "
                "Each result row may also carry an `alternatives_mode` field "
                "(`semantic` | `lexical`) and a `variations` array of v2/v3+ rows "
                "generated against that column via Re-Run / Variations. Each "
                "variation entry includes its own `mode`, the `seed_alternative_text` "
                "the user picked, the `descendant_run_id`, and its alternatives — use "
                "these when the user asks about a column's history, asks for an "
                "evaluation of the alternatives, or wants commentary on how semantic "
                "vs lexical variations differ. `semantic` mode means paraphrase of "
                "the seed (same factual content, different surface form); `lexical` "
                "mode means re-use of the seed's vocabulary with a DISTINCT CANDIDATE "
                "MEANING (a different interpretation a reviewer can tell apart)."
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
                    "include_variations": {
                        "type": "boolean",
                        "description": (
                            "When true (default), include each result's "
                            "Re-Run / Variations descendants under a `variations` "
                            "array on the row. Set false on long Ask sessions to "
                            "save tokens when the user isn't asking about variation "
                            "history."
                        ),
                    },
                },
                "required": ["run_id"],
            },
        },
    },
    {
        "type": "function",
        "freshness": "cache_ok",
        "function": {
            "name": "compare_runs",
            "description": (
                "Side-by-side comparison of two or more past /run "
                "invocations. Wraps the same payload the CLI "
                "/history compare and the Studio Compare modal use — "
                "summary per run (model, profiles, duration, status), "
                "aggregate metrics (model time, tokens, cost, "
                "confidence band split, approval rate, avg logprob), "
                "and per-column LLM descriptions. Use this when the "
                "user asks 'compare runs 58, 59, 60', 'which run "
                "produced better descriptions for the address table', "
                "'compare my last 3 runs on sales.orders'. If the "
                "user gives a scope hint instead of run IDs ('I ran "
                "analyze on the address table last week — compare "
                "those runs'), call list_past_runs FIRST to resolve "
                "candidate run IDs, then call this tool with the "
                "matching ids. Returns a SUMMARY by default — runs, "
                "summary_rows, aggregates, and a 3-row "
                "per_column_sample plus per_column_count. Pass "
                "include_per_column=true to fetch every per-column "
                "row (large; only do this when the user asks for "
                "specific descriptions). Pass column_filter="
                "'<col_name>' to drill into one column across every "
                "run — much cheaper than the full pivot when the "
                "question is about a single field."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "run_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": (
                            "Two or more run IDs to compare. The list must have at least 2 entries."
                        ),
                    },
                    "include_per_column": {
                        "type": "boolean",
                        "description": (
                            "When true, return every per-column row "
                            "(asset × run). Default false: returns "
                            "the first 3 rows as a sample plus the "
                            "total per_column_count so the LLM can "
                            "decide whether to drill in."
                        ),
                    },
                    "column_filter": {
                        "type": "string",
                        "description": (
                            "Optional: restrict per-column rows to "
                            "one column name across every run. "
                            "Cheaper than include_per_column=true "
                            "when the user is asking about one "
                            "specific field."
                        ),
                    },
                    "quality_tier": {
                        "type": "integer",
                        "description": (
                            "Academic text-quality metric tier. "
                            "0 (default): no quality analysis — "
                            "just the standard summary / aggregate "
                            "pivot. 1: + Tier 0 offline metrics "
                            "(chrF, ROUGE-L, schema grounding, "
                            "length, type-token ratio) — free, "
                            "fast. 2: + Tier 1 local sentence "
                            "embeddings + Tier 2 LLM-as-judge "
                            "pairwise tournament (G-Eval, Liu et "
                            "al. 2023). Tier 2 runs C(N,2) judge "
                            "calls per asset and writes their "
                            "tokens into the run's tokens_json. "
                            "Use 1 when the user asks 'compare "
                            "quality' / 'which is more accurate'; "
                            "use 2 when they explicitly request a "
                            "rigorous / academic comparison."
                        ),
                    },
                    "ground_truth_run_id": {
                        "type": "integer",
                        "description": (
                            "Optional: pin one of the runs as the "
                            "ground-truth baseline for reference-"
                            "based metrics (chrF / ROUGE-L). "
                            "Overrides the live DB COMMENT → "
                            "catalog-applied → none waterfall."
                        ),
                    },
                },
                "required": ["run_ids"],
            },
        },
    },
    {
        "type": "function",
        "freshness": "cache_ok",
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
    {
        "type": "function",
        "freshness": "cache_ok",
        "function": {
            "name": "search_docs",
            "description": (
                "Semantic search over user-ingested documentation (markdown, "
                "PDF, DOCX, HTML, RST, txt) — the doc RAG. Use this when the "
                "user asks about business meaning, process descriptions, "
                "design intent, KPI definitions, or anything the **schema "
                "alone cannot answer** (e.g. 'how is churn defined?', 'what "
                "does the contracts table represent?'). Scope is automatic: "
                "doc profiles linked to the current DB scope (or all global "
                "profiles when none are linked). When the resolved scope "
                "yields zero indexed chunks, the tool returns "
                '``reason: "no_docs_for_scope"`` — surface that fact, do '
                "NOT invent business descriptions. Each hit carries "
                "``source`` (file path) so cite it in the answer."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "Natural-language question or topic. Pass the "
                            "user's words (translated if helpful) — the RAG "
                            "store handles tokenisation."
                        ),
                    },
                    "n_results": {
                        "type": "integer",
                        "description": "Top-N chunks to return (default 5, max 10).",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "freshness": "cache_ok",
        "function": {
            "name": "search_code",
            "description": (
                "Semantic search over the user's scanned codebase (the "
                "``amx_code`` Chroma index). Use this when the user asks "
                "**where / how a table or column is used in code** — read "
                "vs write callsites, ETL job names, file:line references, "
                "transformation logic. Scope is automatic: code profiles "
                "linked to the current DB scope. Empty scope returns "
                '``reason: "no_code_for_scope"``. Each hit carries '
                "``source`` (file path) and may carry ``table`` metadata.\n"
                "DO NOT use this tool to write a long code review or to "
                "summarise transformations across many files — for "
                "table-level deep analysis suggest the user run "
                "``/code-analyze --tables <X>`` (CLI) or open the Code "
                "Analyze page in Studio. Keep your answer to citing the "
                "snippets this tool returned."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural-language code-intent query.",
                    },
                    "n_results": {
                        "type": "integer",
                        "description": "Top-N snippets (default 5, max 10).",
                    },
                    "table_filter": {
                        "type": "string",
                        "description": (
                            "Optional table name to bias the search "
                            "towards snippets that mention that table."
                        ),
                    },
                },
                "required": ["query"],
            },
        },
    },
    # ── scheduled runs (read-only) ────────────────────────────
    #
    # Two tools the Ask agent can use to answer questions about
    # the user's scheduled metadata runs. Read-only: the agent
    # cannot create, edit, pause, or delete schedules from these
    # tools -- direct the user to the Schedules page or `amx
    # schedule` CLI for those actions.
    {
        "type": "function",
        "freshness": "cache_ok",
        "function": {
            "name": "list_schedules",
            "description": (
                "Return upcoming, past, or paused scheduled metadata runs. "
                "Use this when the user asks about plans, upcoming runs, "
                "missed schedules, or the outcome of past scheduled runs. "
                "Each row includes id, name, fire_at_utc, fire_at_tz, "
                "status, db_profile, scope_json, llm_profile, "
                "review_strategy, triggered_run_id, and last_error."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "filter": {
                        "type": "string",
                        "enum": ["active", "past", "all"],
                        "description": (
                            "'active' = pending/paused/missed/running "
                            "(default); 'past' = completed/failed/"
                            "cancelled; 'all' = everything."
                        ),
                    },
                    "db_profile": {
                        "type": "string",
                        "description": "Optional DB profile filter.",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "freshness": "cache_ok",
        "function": {
            "name": "get_schedule",
            "description": (
                "Return the full record of a specific scheduled run "
                "by id, including any linked analysis_runs.id and "
                "last_error. Use when the user asks about a "
                "specific schedule the agent saw via list_schedules."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "schedule_id": {
                        "type": "integer",
                        "description": "scheduled_runs.id",
                    },
                },
                "required": ["schedule_id"],
            },
        },
    },
]


def tool_schemas() -> list[dict[str, Any]]:
    """Return a fresh copy of the tool-schema list for LLM tool-calling.

    Callers must not mutate the returned list in place beyond their own
    request scope, but the deep copy ensures local edits cannot leak
    into other consumers.
    """
    return copy.deepcopy(_TOOL_SCHEMAS)
