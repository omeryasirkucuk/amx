"""Tool-calling ``/ask`` agent.

Replaces the regex-driven LLM-Pass1/Pass2 router in ``amx/search/agent.py``
with a thin loop: hand the LLM a fixed set of metadata tools, let it pick
which one(s) to call, then synthesize a final answer from the gathered
results. The deterministic short-circuits (chitchat, meta-query,
reaffirmation) and the catalog-grounded target resolver remain the
responsibility of the caller (``SearchService``) — this module only owns
the tool-loop step.

Why this design exists (see CHANGELOG.md): the previous router classified
the whole question through a JSON schema and then we patched LLM mistakes
with regex overrides. Every new phrasing required a new regex, and the
regex overrides kept causing collateral bugs ("under" being captured from
"tables under sap_test"). The tool-calling loop pushes the routing
decision back into the model, but unlike the original prompt-only design,
the model now has actual catalog/live-DB tools to ground its answer
against — so it doesn't have to hallucinate.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from amx.config import AMXConfig
from amx.llm.provider import LLMProvider
from amx.search.agent_tools import ToolBox
from amx.search.catalog import SearchCatalog
from amx.utils.token_tracker import estimate_tokens
from amx.utils.token_tracker import tracker as token_tracker

if TYPE_CHECKING:
    from amx.utils.live_display import LiveDisplay

# Maximum number of tool-call iterations before we force the LLM to answer.
# A typical question takes 1–3 tool calls; 6 gives headroom for chained
# discovery (list_schemas → list_tables_in_schema → describe_table) without
# letting a confused model spin forever.
_MAX_ITERATIONS = 6

# Cap on tokens per LLM round in the agent loop. Tool answers are JSON
# blobs; we trim them in agent_tools._safe_json so the prompt stays bounded.
_AGENT_MAX_TOKENS = 1500


class ToolAgentResult:
    """Container for what the agent loop produced."""

    def __init__(
        self,
        *,
        answer: str,
        tool_calls: list[dict[str, Any]],
        iterations: int,
        usage: dict[str, Any],
        finish_reason: str | None,
        scope_profiles: list[str] | None = None,
        focus_profile: str | None = None,
        total_latency_ms: int | None = None,
        per_tool_latency_ms: dict[str, int] | None = None,
    ) -> None:
        self.answer = answer
        self.tool_calls = tool_calls
        self.iterations = iterations
        self.usage = usage
        self.finish_reason = finish_reason
        # Multi-profile observability (PR-D): each /ask reports the
        # resolved scope, the auto-detected focus, the wall-clock
        # latency, and the per-tool latency breakdown so users (and
        # the SPA's footer badge) can spot slow profiles + measure
        # the cost of multi-profile retrieval.
        self.scope_profiles = scope_profiles
        self.focus_profile = focus_profile
        self.total_latency_ms = total_latency_ms
        self.per_tool_latency_ms = per_tool_latency_ms or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "answer": self.answer,
            "tool_calls": self.tool_calls,
            "iterations": self.iterations,
            "usage": dict(self.usage),
            "finish_reason": self.finish_reason,
            "scope_profiles": list(self.scope_profiles or []),
            "focus_profile": self.focus_profile,
            "total_latency_ms": self.total_latency_ms,
            "per_tool_latency_ms": dict(self.per_tool_latency_ms),
        }


def _agent_system_prompt(
    cfg: AMXConfig,
    schema_hint: list[str],
    *,
    scope_profiles: list[str] | None = None,
    focus_profile: str | None = None,
) -> str:
    """The single system prompt the LLM sees throughout the loop.

    Includes live ground-truth (databases, schemas) so the model can route
    without us having to regex-classify the question.

    ``scope_profiles`` is the multi-profile retrieval scope (every
    profile the LLM is allowed to surface data from for THIS turn).
    When 2+ entries, a multi-profile guidance block is injected.
    ``focus_profile`` is the auto-detected conversation focus computed
    from prior turns; the LLM uses it as a soft default when the
    user's question is ambiguous about which profile to look at.
    """
    db_name = cfg.db.database or cfg.db.catalog or cfg.db.project or "(active database)"
    db_unpinned_hint = ""
    if not (cfg.db.database or cfg.db.catalog or cfg.db.project):
        backend = (cfg.db.backend or "").lower()
        if backend in {"databricks", "bigquery"}:
            db_unpinned_hint = (
                "  ⚠ No catalog/project is pinned for this profile. Just call list_schemas "
                "(no catalog argument) — when there's a single non-system user catalog "
                "the tool auto-resolves it and returns schemas directly. Only fall back "
                "to list_catalogs if list_schemas comes back with `needs_catalog=true`. "
                "Never compose a 'I see N catalogs' paragraph at the user — the auto-"
                "pick handles it silently and you should just answer their actual "
                "question (which tables / schemas / etc.)."
            )
        elif backend:
            db_unpinned_hint = (
                "  Note: this profile has no database pinned — that is a normal,\n"
                "  fully-supported state. The user did NOT have to commit to a single\n"
                "  database (Studio's multi-profile browse sidebar uses the same\n"
                "  unpinned mode). NEVER ask the user to switch via /use-db or pin\n"
                "  one with /edit; that's a blocker, not an answer. To answer\n"
                "  reach / coverage questions ('which tables can we reach', 'what\n"
                "  databases do I have'), call list_databases with `with_counts=true`\n"
                "  — it fans out per profile and returns each reachable database\n"
                "  with schema/table counts already aggregated. Then render the\n"
                "  STATS-EXAMPLE-DRILL pattern (stats line, 5–8 examples, drill-in\n"
                "  invitation). For schema/table tools that need a database to\n"
                "  scope, pass `database=` explicitly per call."
            )
    schema_line = (
        ", ".join(schema_hint)
        if schema_hint
        else "(none indexed yet — use list_schemas to discover)"
    )
    current_schema = cfg.current_schema or "(none — user has not pinned a schema)"
    current_table = cfg.current_table or "(none — user has not pinned a table)"
    metadata_lang = cfg.llm.language or "english"

    # Build a one-line summary of every connected DB profile so the model
    # can mention "this lives in your SAP profile; you also have WAREHOUSE
    # connected" when the user asks cross-DB questions. ``in_scope`` flags
    # which profiles the current question is allowed to retrieve from —
    # the LLM should only return data from those.
    #
    # The ``→ <name>`` after the backend is the profile's PINNED default
    # database/catalog/project (not the only one reachable). Pinned
    # values are connection-time defaults; each profile can list MANY
    # databases/catalogs via ``list_databases`` — the LLM must not
    # treat the pinned name as the full reach.
    in_scope = set(scope_profiles or [])
    profile_lines: list[str] = []
    active_name = cfg.active_db_profile or "default"
    for profile_name, db_cfg in sorted(cfg.db_profiles.items()):
        markers: list[str] = []
        if profile_name == active_name:
            markers.append("active")
        if in_scope and profile_name in in_scope:
            markers.append("in scope")
        marker = f" ({', '.join(markers)})" if markers else ""
        db_target = db_cfg.database or db_cfg.catalog or db_cfg.project or "?"
        backend = db_cfg.backend or "?"
        profile_lines.append(
            f"  - {profile_name}{marker}: {backend} (default db/catalog: {db_target})"
        )
    profiles_block = (
        "\n".join(profile_lines)
        if profile_lines
        else "  (none configured — only the active connection is reachable)"
    )

    # Token budget guard: ``schema_hint`` can balloon when the active
    # profile has 200+ schemas (saw ~5K tokens on a Snowflake account).
    # Cap at the first 50 names — the LLM can always call list_schemas
    # for the full picture. Only kicks in when we'd actually overshoot;
    # short profiles render in full as before.
    if schema_hint and len(schema_hint) > 50:
        truncated = list(schema_hint[:50])
        schema_line = (
            ", ".join(truncated)
            + f", … ({len(schema_hint) - 50} more — call list_schemas to see all)"
        )

    # Multi-profile guidance block.
    scope_block = ""
    focus_block = ""
    if scope_profiles and len(scope_profiles) > 1:
        scope_block = (
            "\nMULTI-PROFILE MODE — the question can touch any of "
            f"{len(scope_profiles)} profiles: {', '.join(scope_profiles)}.\n"
            "Catalog tools (search_tables_by_concept, search_columns_by_concept, "
            "find_table_by_name, find_columns_by_dtype, find_joinable_tables) "
            "automatically span every profile in scope. Live-DB tools "
            "(list_schemas, list_tables_in_schema) fan out across profiles in "
            "parallel and return per-profile breakdowns. Profile-targeted tools "
            "(describe_table, sample_column_values, profile_table) take a "
            "``db_profile`` argument — pass it to disambiguate the same-named "
            "table across profiles.\n"
            "EVERY citation must include the db_profile so the user knows where "
            "the data lives. Tool result rows already carry it — just surface it. "
            'When the user asks an open question ("which tables do I have", '
            '"which databases", "which columns are X") aggregate ACROSS every '
            "profile in scope rather than picking one — give a stats summary that "
            "names every profile's count, then 5–8 examples, then invite drill-in "
            "(see the Style section's STATS-EXAMPLE-DRILL pattern). Don't pick a "
            "single profile silently.\n"
            "When you need cross-profile JOIN candidates ('what can I join this "
            "table with from a different DB?'), call "
            "find_joinable_across_profiles — it scores name + dtype + vector "
            "similarity + FK signals across every profile in scope and returns "
            "ranked candidates.\n"
        )
    if focus_profile:
        focus_block = (
            f"\nCONVERSATION FOCUS — the user has been mostly working with "
            f"profile **{focus_profile}** in this chat. Default to that "
            "profile when the question is ambiguous about scope. The user can "
            "still ask cross-profile questions explicitly ('compare across all "
            "profiles', 'is this in any other profile') — switch context "
            "smoothly when they do.\n"
        )

    return (
        "You are AMX's metadata-search assistant. Answer the user's question by calling the "
        "tools available to you. NEVER guess; ALWAYS ground every claim in a tool result.\n\n"
        f"Active database: {db_name}\n"
        + (f"{db_unpinned_hint}\n" if db_unpinned_hint else "")
        + f"Schemas in this DB: {schema_line}\n"
        f"User's pinned schema: {current_schema}\n"
        f"User's pinned table: {current_table}\n"
        f"User's language preference: {metadata_lang}\n"
        "Connected DB profiles:\n"
        f"{profiles_block}\n"
        + scope_block
        + focus_block
        + "\nCache-vs-live provenance: every read tool surfaces `source` and `age_seconds`.\n"
        "  `source=catalog` means the data came from /search sync's persisted index — surface\n"
        "  the age in one short hint when `age_seconds` is over a week ('catalog is X days old,\n"
        "  consider /search sync for fresh data') and continue with the question. `source=live_cache`\n"
        "  means we served from a 24h memo; treat as authoritative. `source=live` means we just\n"
        "  paid for a live round-trip — quote the result confidently. Pass `force_fresh=true` ONLY\n"
        "  when the user explicitly asks for the current live state ('right now', 'after my\n"
        "  apply', 'fresh from the warehouse') or when the cached age is incompatible with the\n"
        "  question. Don't add `force_fresh=true` defensively — it's metered.\n"
        + "\nRouting guidance — choose the smallest correct path:\n"
        "* User names an exact identifier ('vbrk', 'adrc') → call find_table_by_name first; if it\n"
        "  returns one match, call describe_table on it. If multiple, surface ALL matches and ask.\n"
        "* User asks 'tables in <schema>' / 'tables under <schema>' / 'list tables of <schema>' → \n"
        "  call list_tables_in_schema with that exact schema. The user said 'tables', not\n"
        "  'a table named X'.\n"
        "* User asks 'which schemas / what schemas / how many schemas' → call list_schemas.\n"
        "  If list_schemas comes back with `needs_catalog=true` (3-level backend whose\n"
        "  active profile has no catalog pinned), IMMEDIATELY call list_schemas again\n"
        "  with `catalog` set to the most likely entry in the `catalogs` field — do NOT\n"
        "  narrate the choice to the user, do NOT compose 'I see N catalogs, let me\n"
        "  check X first' prose. The tool already auto-picks when there's exactly one\n"
        "  user catalog; if it punted, just pick the obviously-non-system one and\n"
        "  recurse. Same applies for list_tables_in_schema: pass `catalog` to scope\n"
        "  the listing when the profile is unpinned. When `auto_picked_catalog` is\n"
        "  present in a list_schemas response, just mention briefly which catalog\n"
        "  the schemas live in (one sentence) and continue with the user's actual\n"
        "  question — don't dwell.\n"
        "* User asks 'which databases do I have / what databases / show me all\n"
        "  databases' → call list_databases. The tool fans\n"
        "  out per profile and returns the FULL list of databases (or catalogs\n"
        "  on 3-level backends) reachable through each connection — NOT just\n"
        "  the one pinned in each profile's config. When composing the answer,\n"
        "  enumerate every entry per profile, grouped by profile name, with\n"
        "  per-profile counts. If a profile errored / timed out, mention it\n"
        "  honestly — don't pretend the partial result is the full picture.\n"
        "  Do NOT just regurgitate the 'default db/catalog' shown in the\n"
        "  profiles header above; that's the connection-time default, not the\n"
        "  full reach.\n"
        "* User asks 'which catalogs / show catalogs' → call list_catalogs.\n"
        "  Same tool also rescues 'show me tables' on a catalog-less Databricks profile.\n"
        "* User asks 'which databases live on this server / show databases'\n"
        "  on a 2-level backend (PostgreSQL, Snowflake, MySQL, MSSQL, Redshift,\n"
        "  ClickHouse) → call list_server_databases. Different from list_databases\n"
        "  (which on multi-profile fans out across every profile).\n"
        "* User asks about Databricks Volumes ('any volumes', 'managed/external\n"
        "  volumes', 'volumes under <schema>', 'unity catalog\n"
        "  volume', 'storage volumes') → call list_volumes. Volumes are NOT exposed\n"
        "  by list_tables_in_schema or describe_table — they live in their own\n"
        "  Unity Catalog namespace. NEVER answer 'I can't see volumes' on Databricks;\n"
        "  the tool runs SHOW VOLUMES across the active catalog (auto-picked when\n"
        "  unpinned). For non-Databricks backends the tool returns supported=false —\n"
        "  surface that as 'volumes don't apply to this backend' instead of\n"
        "  inventing a query.\n"
        "* User asks 'tables with boolean columns' / 'date columns' / 'all int columns' → \n"
        "  call find_columns_by_dtype with the type token. NEVER fall back to\n"
        "  search_columns_by_concept for dtype questions; concept search matches NAMES, not types.\n"
        "* User asks 'which tables don't have a comment?', 'tables without description',\n"
        "  'do all tables have descriptions?'\n"
        "  → call find_assets_missing_comment. This queries the LIVE DB directly because\n"
        "  catalog can lag right after /run-apply. NEVER use search_tables_by_concept or\n"
        "  search_columns_by_concept for coverage questions — those are concept-based and\n"
        "  return stale results when descriptions were just written.\n"
        "* User asks 'which tables can I join with X' →\n"
        "  call find_joinable_tables with the single table.\n"
        "* User asks how X and Y join (both named) → call get_join_candidates.\n"
        "* User asks about past /RUN history ('what runs have I done on\n"
        "  sales.orders', 'which settings did I use yesterday', 'has this table\n"
        "  been analyzed before', 'past runs') → call list_past_runs (optionally\n"
        "  with schema/table filters), then describe_run for specific run IDs.\n"
        "  The returned rows include human-readable started_at and duration_human\n"
        "  fields — USE THOSE, never raw epoch or raw float seconds. When the\n"
        "  user asks for a table, render at most 6 columns (Run ID, Started,\n"
        "  Duration, Status, LLM model, Total tokens) and put longer fields\n"
        "  (settings, scope) inline as text below.\n"
        "* User asks to COMPARE runs ('compare runs 58, 59, 60', 'compare my\n"
        "  last 3 runs', 'compare my last 2 runs on the address table', 'which\n"
        "  run produced better descriptions') → call compare_runs with the run\n"
        "  IDs. If the user gives a SCOPE HINT instead of explicit IDs ('I ran\n"
        "  analyze on the address table last week — compare those'), call\n"
        "  list_past_runs FIRST with the schema/table filter to resolve\n"
        "  candidate IDs, then pass them to compare_runs. compare_runs returns\n"
        "  a SUMMARY by default (runs + summary_rows + aggregates + 3-row\n"
        "  per_column_sample); only re-call with include_per_column=true (or a\n"
        "  column_filter) when the user actually asks about per-column\n"
        "  descriptions — the full pivot is large.\n"
        "  When the user mentions QUALITY ('which is more accurate', 'compare\n"
        "  quality', 'which descriptions are better grounded') call\n"
        "  compare_runs with quality_tier=1 (free Tier 0 metrics: chrF\n"
        "  Popović 2015, ROUGE-L Lin 2004, schema grounding, length, type-\n"
        "  token ratio). For an explicitly RIGOROUS / academic comparison\n"
        "  use quality_tier=2 (LLM-as-judge tournament — G-Eval Liu et al.\n"
        "  2023; Tier 2 costs LLM tokens — only call when the user knows).\n"
        "  When compare_runs returns a quality_metrics field, DO NOT just\n"
        "  dump the numbers. For each metric where runs differ meaningfully,\n"
        "  cite the underlying signal in one short sentence — '#58 wins on\n"
        "  schema grounding (0.84 vs 0.52) because its descriptions reference\n"
        "  the column name and dtype, whereas #59 stays generic.' Pair the\n"
        "  human-readable takeaway with the academic metric name (chrF /\n"
        "  ROUGE-L / schema grounding / judge win-rate) so the user knows\n"
        "  WHICH method drove which conclusion. Surface the citation list\n"
        "  the tool returns under the answer so they can dig into the\n"
        "  papers if they want.\n"
        "* User asks about past /ASK chat history ('my chat history', 'previous\n"
        "  asks', 'continue our last chat', 'resumable sessions') → call\n"
        "  list_chat_sessions (NOT list_past_runs). /ask invocations form\n"
        "  stateful conversation threads stored in chat_sessions; the user can\n"
        "  resume any ended session via `/session resume <id>` — surface that.\n"
        "* NEVER reply 'I don't have access to your past runs' — you DO via\n"
        "  list_past_runs and list_chat_sessions. The local SQLite history at\n"
        "  ~/.amx/history.db is fair game and includes the full settings snapshot.\n"
        "* User asks 'is X a primary key' / 'are PKs duplicated' /\n"
        "  '(id, time) unique?' / 'is a composite PK required or is id enough?' →\n"
        "  call check_uniqueness with the table and column tuple.\n"
        "* User asks 'is there duplication in TABLE' / 'are there duplicate\n"
        "  rows' WITHOUT naming a candidate key → call\n"
        "  inspect_data_quality first to read per-column distinct_ratio,\n"
        "  THEN propose the most likely composite key (columns with\n"
        "  distinct_ratio ≈ 1.0) and offer to verify it with check_uniqueness.\n"
        "  NEVER bounce back with 'give me columns' — surface what the data\n"
        "  shows and let the user confirm.\n"
        "* User asks 'how nullable is column X' / 'duplication ratio' /\n"
        "  'what is the date format' / 'min and max of created_at' /\n"
        "  'how far back is data retained' →\n"
        "  call inspect_data_quality. The result includes detected_format for\n"
        "  varchar columns that look like dates ('20240315' → 'YYYYMMDD',\n"
        "  '15-03-2024' → 'DD-MM-YYYY', etc.). For temporal coverage read\n"
        "  min_value/max_value of date/timestamp columns.\n"
        "* User asks 'give me an example / a sample value' / 'show me a value\n"
        "  from X' / 'what does this column look like' → call\n"
        "  sample_column_values (light direct SELECT — no full profile).\n"
        "  CRITICAL: when the user names just the table without the schema,\n"
        "  call find_table_by_name FIRST so you don't blindly pick the wrong\n"
        "  schema (e.g. 'public' for a SAP-only table that lives in 'sap_s6p').\n"
        "  Only fall through to /search sync hints when find_table_by_name\n"
        "  truly returns no exact AND no fuzzy matches.\n"
        "* User asks 'what's the main / fact table here' / 'which tables look\n"
        "  like dimensions' / 'is this a star schema or snowflake' / 'what is\n"
        "  the main table of this schema' / 'fact and dimension tables' / 'are\n"
        "  there lookup tables' → call detect_dimensional_role. Pass ``schema`` only\n"
        "  to rank EVERY table in the schema and get the overall pattern\n"
        "  (star_schema / snowflake_schema / flat / fact_only / unknown);\n"
        "  pass ``schema`` + ``table`` to classify ONE table. The result\n"
        "  carries role_hypothesis + confidence + evidence + indicators\n"
        "  (naming, row-count percentile, FK fan-out / fan-in,\n"
        "  partition / clustering / temporal column presence). Always\n"
        "  quote the evidence in the answer; the role label alone is\n"
        "  misleading without the why.\n"
        "* User asks 'how does X hold history' / 'is this SCD2' / 'are changes\n"
        "  kept in the same row or as new rows' / 'how are old values stored' /\n"
        "  'history pattern' / 'is there a history table for X' → call\n"
        "  detect_scd_pattern. The result includes scd_type_hypothesis\n"
        "  (type_1 / type_2 / type_3 / type_4 / type_6 / append_only /\n"
        "  unknown), confidence, evidence (bullet list — ALWAYS quote\n"
        "  these in the answer; the hypothesis alone is misleading), and\n"
        "  alternative_hypotheses for hybrid cases. When evidence is\n"
        "  empty, suggest the user provide a candidate business_key so\n"
        "  the rows-per-key probe can disambiguate Type 1 vs Type 2.\n"
        "* User asks 'when was X last updated' / 'is there an update soon' /\n"
        "  'last refresh', 'next refresh', 'when did the ETL run' → call\n"
        "  describe_table and read ``analytics.last_modified``. NEVER answer\n"
        "  'I don't know about future updates' as a flat response — instead\n"
        "  surface the LAST known modification time AND state explicitly:\n"
        "  'AMX can see vbak was last modified at <ts> (from <backend's\n"
        "  freshness signal>). Scheduled future updates require an ETL /\n"
        "  orchestrator tap (Airflow / Dagster / dbt Cloud) that AMX doesn't\n"
        "  currently expose — that's a planned v0.11 feature.' Be precise\n"
        "  about what AMX can vs cannot see.\n"
        "* User asks 'how is X uploaded / loaded / populated / ingested /\n"
        "  refreshed' / 'how does this table get loaded' / 'how is it fed' /\n"
        "  'how does the ETL pipeline work' / 'where does data come from' →\n"
        "  this is the LOAD\n"
        "  MECHANISM question, NOT the history-retention question. Do NOT\n"
        "  call detect_scd_pattern (that answers 'how is history kept' which\n"
        "  is a different concern). Instead, synthesize from what AMX CAN\n"
        "  see:\n"
        "    1. describe_table → analytics.last_modified (when was the most\n"
        "       recent write).\n"
        "    2. inspect_data_quality on the table's main temporal column\n"
        "       (created_at / erdat / load_date / ingestion_ts / etc.) →\n"
        "       min_value (when did data first appear) and max_value\n"
        "       (most recent record). The gap between min and max +\n"
        "       row_count gives a rough load-cadence hint.\n"
        "    3. If you spot CDC-shaped columns (created_at AND updated_at,\n"
        "       deleted_at flag) call them out as an in-band CDC signal.\n"
        "  Then state explicitly what AMX CANNOT see: 'Direct visibility\n"
        "  into the orchestrator (Airflow / Dagster / dbt Cloud / Snowflake\n"
        "  Snowpipe / BigQuery Data Transfer / Databricks DLT) is a v0.11\n"
        "  planned feature — AMX currently infers from the data, not from\n"
        "  the load job.' This question pattern surfaces every week and\n"
        "  the wrong tool route (SCD detection) wastes a turn.\n"
        "* User asks about a concept ('pricing tables', 'address columns', 'customer info') →\n"
        "  call search_tables_by_concept or search_columns_by_concept.\n"
        "* User asks about a TABLE BY NAME, even inside a sentence ('do you have the\n"
        "  vbak table?', 'show me orders', 'is there a customers table?', 'what is\n"
        "  the addr_master table about?'): the named token is a TABLE LOOKUP, not a\n"
        "  concept query. ALWAYS call find_table_by_name(name=<the_table_token>) FIRST\n"
        "  before any concept search. Concept search uses embeddings and misses exact\n"
        "  name hits in vocabularies where the table name is short, abbreviated, or\n"
        "  domain-specific (SAP names like ``vbak`` / ``mara`` / ``bseg``, custom\n"
        "  prefixes like ``t_``, ``stg_``, snake_case names). Only fall through to\n"
        "  search_tables_by_concept when find_table_by_name returns no exact AND\n"
        "  no fuzzy matches. This is the #1 cause of 'said it didn't exist but it\n"
        "  does' regressions; do not skip it.\n"
        "* User asks about BUSINESS MEANING / DEFINITIONS / PROCESS / DESIGN INTENT\n"
        "  ('what does the contracts table represent', 'how is churn defined',\n"
        "  'write me the description', 'what is the business definition',\n"
        "  'why does X exist', 'what is the KPI definition') → schema metadata\n"
        "  is rarely enough on its own. Call\n"
        "  search_docs(query=<user question>) to pull from the user's ingested\n"
        "  documentation (RAG). When it returns reason='no_docs_for_scope', say\n"
        "  so plainly — do NOT fabricate definitions. When it returns hits, cite\n"
        "  the ``source`` file path so the user can trace back.\n"
        "* User asks WHERE / HOW a table or column is USED IN CODE ('where is\n"
        "  customers written', 'which job populates orders', 'where is this used\n"
        "  in code', 'which ETL produces this', 'read-write callsites for X')\n"
        "  → call search_code(query=<phrase>, table_filter=<table when known>).\n"
        "  When it returns hits, cite ``source`` (file path) and any ``symbol``\n"
        "  metadata. When it returns reason='no_code_for_scope', say so.\n"
        "  IMPORTANT: search_code is a RETRIEVAL tool — for a comprehensive\n"
        "  table-level read/write summary across many files, do NOT try to\n"
        "  synthesise it yourself; recommend the user run\n"
        "  ``/code-analyze --tables <X>`` (CLI) or open the **Code → Analyze**\n"
        "  page in Studio. End your answer with that one-line tip.\n"
        "* When the question is a follow-up (pronoun like 'it', 'this table',\n"
        "  short replies like 'only those?', 'really?'), resolve it from the\n"
        "  prior assistant turn(s) BEFORE calling a tool. Read the conversation\n"
        "  history above; previous answers stay relevant.\n"
        "* Input does NOT match any of the patterns above (single token, vague phrase,\n"
        "  unclear intent — e.g. `vbak`, `pricing`, `test`, `?`): DO NOT\n"
        "  fabricate a welcome / onboarding / capabilities message. The frontend\n"
        "  already shows an empty-state placeholder; your job is to answer, not to\n"
        "  re-introduce yourself. Instead, treat the literal input as a search\n"
        "  probe in this fixed order, stopping at the first non-empty result:\n"
        "    1. find_table_by_name(name=<input>)\n"
        "    2. search_tables_by_concept(concept=<input>)\n"
        "    3. search_columns_by_concept(concept=<input>)\n"
        "  If a probe returns matches OR fuzzy_matches, surface them concretely\n"
        "  ('I found `schema.table` matching `<input>` — want me to describe it?').\n"
        "  Only when ALL THREE probes return fully empty (no matches, no fuzzy,\n"
        "  no concept hits), respond with ONE short clarifying sentence naming\n"
        "  exactly what you searched for and inviting the user to rephrase —\n"
        "  e.g. 'I searched the catalog for a table, concept, or column matching\n"
        "  `test` and didn't find anything. What would you like to know?'. NEVER\n"
        "  list invented example queries (`describe X.Y`, `tables in <schema>`,\n"
        "  `find vbak`, etc.) — the user can see the placeholder; padding the\n"
        "  empty answer with fabricated examples is worse than admitting the\n"
        "  miss.\n\n"
        "When you have enough information, STOP calling tools and return a short, direct answer.\n\n"
        "Partial-catalog signal — ALWAYS read it:\n"
        '  Tool results may include ``"partial": true`` along with a\n'
        "  ``partial_reason`` field. That means AMX's catalog skeleton sync\n"
        "  for the profile is still running, so the rows you see were fetched\n"
        "  live and are NOT guaranteed to cover every schema / table the user\n"
        "  owns. NEVER answer with phrasing that implies the listed rows are\n"
        '  the complete set ("4 schemas with 9 tables total"). Instead say\n'
        '  things like "I can see N schemas right now; AMX is still indexing\n'
        '  this profile in the background, so the full set may be larger."\n'
        "  The user is aware of the syncing pill in the top bar; your job is\n"
        "  to stay honest about what the tool actually showed you.\n\n"
        "Result validation — CRITICAL:\n"
        "  Tools (especially search_*_by_concept) return rows ranked by lexical/semantic\n"
        "  similarity. Many returned rows are FALSE POSITIVES. Before composing your\n"
        "  answer you MUST go through every returned row and ask: 'does this column\n"
        "  actually fit the user's intent?'. Drop rows whose name AND description don't\n"
        "  clearly match.\n\n"
        "  Example: user asks 'which tables have phone-number columns'. Tool returns\n"
        "  `addrnumber`, `consnumber`, `persnumber`, `roomnumber`, `tel_number`,\n"
        "  `fax_number`. Only `tel_number` and `fax_number` are phone numbers; the\n"
        "  rest are different kinds of numbers (address number, consecutive number,\n"
        "  person number, room number). Drop them. Don't echo the raw tool list.\n\n"
        "  Be especially strict when the user's concept word is generic ('number',\n"
        "  'code', 'id', 'date', 'amount') — the catalog has thousands of columns\n"
        "  matching those tokens but only a handful actually mean what the user\n"
        "  asked. Read the description text, not just the column name fragment.\n\n"
        "Interpretive answering — NEVER reply with a flat 'no':\n"
        "  When a tool returns no exact match, look for adjacent fields\n"
        "  before declaring 'nothing found'. Examples:\n"
        "    * find_table_by_name → matches=[] but fuzzy_matches has entries.\n"
        "      Surface the fuzzy_matches: 'No exact `trog` table; closest\n"
        "      candidates by partial-name match are `trogr`, `trogt`. Did\n"
        "      you mean one of these?'\n"
        "    * describe_table → columns_truncated=true. Read columns_by_dtype\n"
        "      to answer 'which columns are dtype X' even when the columns\n"
        "      list is truncated. dtype_summary is the authoritative count.\n"
        "    * find_joinable_tables → inference_source='name_overlap' or\n"
        "      'semantic_similarity' instead of 'foreign_key'. Surface the\n"
        "      tier explicitly so the user knows whether the join is FK-\n"
        "      verified or name-inferred.\n"
        "  General rule: 'no exact match' is almost never the user's actual\n"
        "  question. Look at the WHOLE tool response (every field in the\n"
        "  JSON, not just the primary list) before deciding the answer is\n"
        "  empty. If you're going to say 'no X', double-check that no\n"
        "  related field (fuzzy_matches, dtype_summary, columns_by_dtype,\n"
        "  inference_source, kind) carries the answer in another shape.\n\n"
        "Push-back handling — CRITICAL:\n"
        "  When the user replies with 'that's wrong', 'are you sure?', 'I think some\n"
        "  of those are not correct', etc. — DO NOT\n"
        "  repeat the same answer. They're telling you the previous tool result was\n"
        "  over-broad. Take ONE of these actions:\n"
        "    1. Re-call the tool with a more specific query (e.g. concept='telephone'\n"
        "       instead of 'phone number'). Different keywords surface different rows.\n"
        "    2. Call describe_table on each candidate to read the actual column\n"
        "       descriptions and filter manually. Drop entries whose description does\n"
        "       NOT mention the concept the user asked about.\n"
        "    3. Acknowledge the limitation: 'You're right; my previous list was over-\n"
        "       broad. Looking again, only `X.Y.Z` actually carries phone semantics.'\n"
        "  NEVER reply with 'Thank you for your patience!' followed by the same list.\n"
        "  That's worse than admitting you're not sure — it pretends nothing went wrong.\n\n"
        "Style — answer-shape adapts to the data you have:\n"
        "  - Both Studio and CLI render Markdown (lists, tables, bold, inline\n"
        "    code). Use that — readability is the goal.\n"
        "  - **Short answer (≤ 4 items, or single fact)**: one natural-language\n"
        "    paragraph or sentence. Don't bullet two things.\n"
        "  - **5–10 items**: Markdown bullet list, one per line, sorted (alpha\n"
        "    for raw lists, by relevance/score for ranked tool output).\n"
        "  - **More than ~10 items — STATS-EXAMPLE-DRILL pattern (CRITICAL)**:\n"
        '    NEVER dump the full list (no "top 30", no "first 50"). Reading\n'
        "    50 names is just as bad as reading 70. Three-part scannable\n"
        "    answer instead:\n\n"
        "        1. **Stats line.** State the total + scope. Example:\n"
        '             "You have **70 schemas** across the `bird_train_desc`\n'
        '             database in `bird-pg`."\n'
        "           Multi-profile: aggregate per profile in the same line —\n"
        '             "You have **140 schemas** total: 70 in `dbr`,\n'
        '             70 in `test-postgre` (same set in both)."\n\n'
        "        2. **5–8 example names.** Just enough for the user to\n"
        "           recognise the shape of the data, in `inline code`,\n"
        "           comma-separated. Example:\n"
        '             "Examples: `address`, `airline`, `books`, `cars`,\n'
        '             `food_inspection`, `movie`, `sales`, `world`."\n'
        "           Pick a representative spread (don't just take the first\n"
        "           5 alphabetically when that misses interesting ones).\n\n"
        "        3. **Drill-in invitation.** ONE short follow-up question.\n"
        "           Examples:\n"
        '             "Tell me a schema name to list its tables, or ask\n'
        "             'tables in `sales`'.\"\n"
        '             "Which database or schema do you want to focus on?"\n'
        "           This is a discussion, not a clipboard dump — invite, not\n"
        "           a forced picker.\n\n"
        "    The same pattern applies to ANY large set, not just schemas:\n"
        '      * "Which tables do I have" with 5,000 tables → "You have\n'
        "        **5,142 tables** across 70 schemas. Largest schemas:\n"
        "        `sales` (412), `inventory` (308), `events` (257). Examples\n"
        "        from `sales`: `orders`, `customers`, `line_items`, … . Want\n"
        '        a specific schema?"\n'
        '      * "Which columns are date-shaped" with 800 hits → "**~800\n'
        "        columns** look temporal across this DB. Examples:\n"
        "        `customers.created_at`, `orders.shipped_at`, … (5 more).\n"
        '        Which table or schema do you want to narrow to?"\n'
        '      * "Which schemas have customer data" with 12 hits →\n'
        "        **12 schemas** mention 'customer'. Examples: `sales`,\n"
        "        `support`, `crm`. Want me to list every column matching\n"
        "        'customer' in one specific schema?\"\n\n"
        "    NEVER ask the user to pick a database/schema FIRST as a\n"
        "    blocker before answering. Always lead with stats + examples,\n"
        "    THEN invite drill-in. The user came with a question, not for\n"
        "    a multi-step picker.\n\n"
        "    Worked example — UNPINNED 2-level profile in scope (the user's\n"
        "    profile has no `database` pinned; common for PostgreSQL /\n"
        "    Snowflake / MySQL setups where the user wants to browse the\n"
        "    server, not commit to one DB):\n"
        '        User: "which tables can we reach"\n'
        "        You: call list_databases(with_counts=true), then render:\n"
        '          "You have **3 reachable databases** on `postgre`:\n'
        "          `SAP` (4 schemas, ~142 tables), `bird_train` (70 schemas,\n"
        "          ~2,451 tables), `bird_train_desc` (70 schemas, ~2,451\n"
        "          tables). Total: **~5,044 tables** across 3 databases on 1\n"
        "          profile. Want me to drill into one — e.g. 'tables in\n"
        "          `bird_train_desc`'?\"\n"
        '        Do NOT say "please /use-db <database> first" or "pin a\n'
        "        database with /edit\". The unpinned state is the user's\n"
        "        choice; answer their question against the full reach.\n\n"
        "  - **Profile-boundary discipline (anti-hallucination)** — this\n"
        "    is a HARD rule. Each profile has its OWN pinned_database and\n"
        "    pinned_catalog (or none). Tool result rows carry these\n"
        "    per-profile fields explicitly (`pinned_database`,\n"
        "    `pinned_catalog`). NEVER copy one profile's pinned name onto\n"
        "    another profile's row. Concretely:\n"
        "      * If profile `dbr` has `pinned_catalog: amx_test` and\n"
        "        profile `postgre` has `pinned_database: null`, then\n"
        "        `postgre`'s scope is the SERVER (3 reachable databases),\n"
        "        NOT `amx_test.public`. Do not write\n"
        '        "postgre (amx_test.public): 0 tables" — `amx_test` is\n'
        "        not `postgre`'s catalog/database.\n"
        "      * Read pinned_database / pinned_catalog from each row\n"
        "        SEPARATELY. If a per-profile row says `unpinned: true`\n"
        "        or carries `pinned_database: null`, treat that as\n"
        '        "this profile spans the server" and use\n'
        "        list_databases(with_counts=true) to enumerate the\n"
        "        reach — never assume a sibling profile's catalog\n"
        "        applies.\n\n"
        "  - **Multi-profile breakdowns** (per-profile counts, per-profile\n"
        "    errors). When the data set is small (≤ 10 items per profile)\n"
        "    use a bullet list with the profile name **bold** at the start.\n"
        "    When the data is large (20+ items per profile) use the\n"
        "    STATS-EXAMPLE-DRILL pattern above with the profile breakdown\n"
        "    as the stats line. Dedupe: if profiles share content, say so\n"
        "    once and don't repeat per profile.\n"
        "  - **Tabular data** (columns + dtype + nullable + comment, scored\n"
        "    join candidates, etc.): use a Markdown GFM table when 3+ rows\n"
        "    have the same shape AND the total fits ≤ 15 rows on screen.\n"
        "    Past 15, fall back to STATS-EXAMPLE-DRILL.\n"
        "  - **Quote identifiers in backticks** (`schema.table`, `column_name`)\n"
        "    so they pop on both surfaces.\n"
        f"  - Match the user's language; default to {metadata_lang}.\n"
        "  - If a tool reports 'found: false' or empty matches, say so plainly. NEVER invent a\n"
        "    table name. NEVER substitute a similar-sounding one without flagging it.\n"
        "  - For follow-up questions, you MAY answer from prior context without a new tool call\n"
        "    if the prior tool result already contains the answer.\n"
        "  - If a tool is unavailable, explain what's missing and suggest the user run\n"
        "    `/search sync` (catalog refresh) or check their DB connection."
    )


def _convert_message_for_litellm(message: dict[str, Any]) -> dict[str, Any]:
    """LiteLLM expects the OpenAI message shape verbatim — nothing extra."""
    msg = {k: v for k, v in message.items() if v is not None}
    return msg


def _looks_partial(tool_result: str) -> bool:
    """Cheap textual check for the ``"partial": true`` marker on a
    tool result. The result is the JSON string the catalog tools
    return; parsing every result would be wasteful when only a
    minority carry the flag. ``"partial": true`` (with single or
    double quotes) is unambiguous enough."""
    if not tool_result:
        return False
    if '"partial": true' in tool_result or '"partial":true' in tool_result:
        return True
    return "'partial': True" in tool_result or "'partial':True" in tool_result


def _summarise_tool_call(tool_call: Any, result: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "name": tool_call.name,
        "arguments": tool_call.arguments,
        "result_preview": result[:280] + ("…" if len(result) > 280 else ""),
    }
    # PR E: surface structured citations for ``search_docs`` calls so the
    # /ask SSE stream can render a Sources block under the answer and a
    # per-hit table inside the tool-call expander. We parse the JSON
    # tool-result here (toolbox.invoke returns a ``_safe_json`` string)
    # and pull each hit's source / chunk_idx / score / snippet. The
    # extraction is best-effort — any parse failure falls back to an
    # empty list so existing callers keep working unchanged.
    # PR γ: ``search_code`` joins ``search_docs`` here so the /ask SSE
    # stream renders the same Sources block + per-call hit table for
    # code retrievals as it does for docs. The hit shape differs
    # (code hits carry ``rel_path`` + ``start_line`` / ``end_line``
    # instead of ``source`` + plain ``chunk_idx``); the citation dict
    # is the unified shape both surfaces emit.
    if tool_call.name in ("search_docs", "search_code"):
        citations: list[dict[str, Any]] = []
        try:
            import json as _json

            parsed = _json.loads(result) if isinstance(result, str) else result
            hits = parsed.get("hits") if isinstance(parsed, dict) else None
            if isinstance(hits, list):
                for hit in hits:
                    if not isinstance(hit, dict):
                        continue
                    raw_meta = hit.get("metadata")
                    meta: dict[str, Any] = raw_meta if isinstance(raw_meta, dict) else {}
                    # ``rel_path`` is the user-friendly key for code
                    # hits (``src/foo.py``); fall back to the absolute
                    # ``source`` only when ``rel_path`` is absent.
                    source = str(
                        hit.get("rel_path")
                        or hit.get("source")
                        or meta.get("rel_path")
                        or meta.get("source")
                        or ""
                    )
                    chunk_idx = hit.get("chunk_idx")
                    if chunk_idx is None:
                        chunk_idx = meta.get("chunk_idx")
                    try:
                        chunk_idx_int = int(chunk_idx) if chunk_idx is not None else 0
                    except (TypeError, ValueError):
                        chunk_idx_int = 0
                    distance = hit.get("distance")
                    score_val = hit.get("score")
                    if score_val is None and isinstance(distance, (int, float)):
                        # Chroma reports distance (lower = better); flip
                        # to a normalised 0..1 similarity score so the
                        # frontend can render a single "score 0.84" line
                        # consistent with PR C's RunDetail citations.
                        score_val = max(0.0, 1.0 - float(distance))
                    try:
                        score_float = float(score_val) if score_val is not None else 0.0
                    except (TypeError, ValueError):
                        score_float = 0.0
                    snippet = str(hit.get("snippet") or hit.get("text") or "")
                    if len(snippet) > 200:
                        snippet = snippet[:200]
                    # PR γ: pull line range from the hit (code-RAG)
                    # falling back to metadata. ``None`` when the
                    # chunk pre-dates the line-bound rollout so the
                    # frontend can fall back to ``path:chunk_idx``.
                    start_line = hit.get("start_line")
                    if start_line is None:
                        start_line = meta.get("start_line")
                    end_line = hit.get("end_line")
                    if end_line is None:
                        end_line = meta.get("end_line")
                    line_range: list[int] | None = None
                    if start_line is not None:
                        try:
                            sl = int(start_line)
                            el = int(end_line) if end_line is not None else sl
                            if sl > 0:
                                line_range = [sl, el]
                        except (TypeError, ValueError):
                            line_range = None
                    citations.append(
                        {
                            "source": source,
                            "chunk_idx": chunk_idx_int,
                            "score": score_float,
                            "snippet": snippet,
                            "line_range": line_range,
                        }
                    )
        except Exception:
            citations = []
        summary["citations"] = citations
    return summary


def run_tool_agent(
    *,
    cfg: AMXConfig,
    catalog: SearchCatalog,
    llm: LLMProvider,
    question: str,
    answer_language: str,
    session_memory: list[dict[str, Any]] | None = None,
    display: LiveDisplay | None = None,
    on_thinking_delta: Callable[[str], None] | None = None,
    on_tool_call: Callable[[dict[str, Any]], None] | None = None,
    on_content_delta: Callable[[str], None] | None = None,
    cancel_token: threading.Event | None = None,
    db_profiles: list[str] | None = None,
    doc_profiles: list[str] | None = None,
    code_profiles: list[str] | None = None,
) -> ToolAgentResult:
    """Run the tool-calling loop and return the final synthesised answer.

    ``session_memory`` carries the recap of prior turns the planner already
    builds in ``SearchAgent._memory_summary``. We forward it as a prelude
    user/assistant exchange so the model can resolve "it" / "that table" /
    "bu tablo" without re-asking the user.

    ``display`` is forwarded to the LLM call so reasoning models can stream
    their thinking text into the live thinking panel; for models that don't
    expose reasoning, it's a no-op.

    ``db_profiles`` is the multi-profile retrieval scope — every catalog
    tool call (``search_tables_by_concept``, ``find_joinable_tables``, …)
    expands its WHERE clause to ``db_profile IN (?, ?, …)`` so a single
    question can union evidence from N profiles. Empty / ``None`` falls
    back to ``cfg.active_db_profile`` (legacy single-profile behaviour) so
    pre-multi-profile callers keep working unchanged.

    The remaining kwargs are additive hooks AMX Studio's
    ``/api/ask`` SSE endpoint plugs into:

    * ``on_thinking_delta(text)`` — invoked for every reasoning chunk
      a thinking-model streams. Non-thinking models simply never call
      it. The CLI's ``LiveDisplay`` already receives the same chunks
      via ``display.update_thinking``; this kwarg gives the web layer
      a parallel hook so the SPA can render a streaming spinner.
    * ``on_tool_call({"name", "arguments", "result_preview"})`` —
      fired right after each tool returns. Lets the SPA render a
      "ran ``list_tables_in_schema``" log row in real time.
    * ``cancel_token`` — checked at the top of every iteration of the
      tool loop. When set, the loop bails before the next LLM call,
      raising :class:`amx.agents.orchestrator.RunCancelled` so the
      JobRegistry can flip the job to ``cancelled``.

    All three are optional and default to ``None`` so existing
    callers (CLI ``/ask``, batch scripts) keep working unchanged.
    """
    # Use ``with`` so the live DB connector (SQLAlchemy engine + connection
    # pool) is disposed at the end of every question. Without this, each
    # ``/ask`` turn leaks a few file descriptors; after enough turns the
    # process hits ``OSError: Too many open files`` (the user-reported case).
    with ToolBox(
        cfg,
        catalog,
        db_profiles=db_profiles,
        doc_profiles=doc_profiles,
        code_profiles=code_profiles,
    ) as toolbox:
        return _run_tool_loop(
            toolbox=toolbox,
            cfg=cfg,
            llm=llm,
            question=question,
            answer_language=answer_language,
            session_memory=session_memory,
            display=display,
            on_thinking_delta=on_thinking_delta,
            on_tool_call=on_tool_call,
            on_content_delta=on_content_delta,
            cancel_token=cancel_token,
        )


def _compute_focus_profile(
    session_memory: list[dict[str, Any]] | None,
    scope: list[str],
) -> str | None:
    """Detect the conversation's focus profile from prior turns.

    Heuristic: scan the last ~3 assistant turns' answer text for
    ``db_profile=NAME`` or ``profile NAME`` mentions. If one profile
    accounts for ≥60% of the mentions, return it. Otherwise return
    ``None`` and let the LLM pick. Lightweight on purpose — we don't
    re-parse tool_call traces (those aren't carried in session_memory)
    so the heuristic operates on what the LLM has already said.

    Skipped entirely when scope is single-profile (focus is implicit).
    """
    if not scope or len(scope) < 2 or not session_memory:
        return None
    last_turns = [t for t in session_memory if t.get("role") == "assistant"][-3:]
    if not last_turns:
        return None
    counts: dict[str, int] = dict.fromkeys(scope, 0)
    text_blob = " ".join(str(t.get("content") or "") for t in last_turns).lower()
    for name in scope:
        # Word-boundary-ish: name surrounded by whitespace, punctuation, or quotes.
        # Cheap substring count is correct enough for short profile names.
        counts[name] = text_blob.count(name.lower())
    total = sum(counts.values())
    if total < 2:  # too few mentions → don't bias
        return None
    top_name, top_count = max(counts.items(), key=lambda kv: kv[1])
    if top_count == 0 or top_count / total < 0.60:
        return None
    return top_name


def _run_tool_loop(
    *,
    toolbox: ToolBox,
    cfg: AMXConfig,
    llm: LLMProvider,
    question: str,
    answer_language: str,
    session_memory: list[dict[str, Any]] | None,
    display: LiveDisplay | None = None,
    on_thinking_delta: Callable[[str], None] | None = None,
    on_tool_call: Callable[[dict[str, Any]], None] | None = None,
    on_content_delta: Callable[[str], None] | None = None,
    cancel_token: threading.Event | None = None,
) -> ToolAgentResult:
    # Pre-fetch the schema list once; if it succeeds we put it into the
    # system prompt so the LLM doesn't have to spend a tool call discovering
    # what schemas exist before answering simple "list tables in X" queries.
    schemas_hint: list[str] = []
    try:
        schemas_hint = [str(s) for s in toolbox._live_db().list_schemas()]  # noqa: SLF001
    except Exception:
        schemas_hint = []

    # Multi-profile system-prompt enrichment: scope (which profiles the
    # LLM may surface) + focus (which the conversation has gravitated
    # toward over recent turns). Both are advisory — tool_box catalog
    # calls already enforce scope; focus only nudges the LLM's framing.
    scope_profiles: list[str] = list(toolbox.db_profiles)
    focus_profile = _compute_focus_profile(session_memory, scope_profiles)

    messages: list[dict[str, Any]] = [
        {
            "role": "system",
            "content": _agent_system_prompt(
                cfg,
                schemas_hint,
                scope_profiles=scope_profiles,
                focus_profile=focus_profile,
            ),
        }
    ]
    # Inject prior conversation context so follow-ups resolve.
    for turn in session_memory or []:
        role = str(turn.get("role") or "")
        content = str(turn.get("content") or "")
        if role in {"user", "assistant"} and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": question})

    aggregated_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    tool_call_log: list[dict[str, Any]] = []
    # Per-tool latency accumulator: name → total ms across all calls
    # in this question (a single tool may be called multiple times).
    # Surfaced in the response payload so the SPA's footer can show
    # which tool dominated wall-clock — invaluable for debugging
    # "why did this take 9 seconds" without having to attach a
    # profiler.
    per_tool_latency_ms: dict[str, int] = {}
    import time as _time

    loop_start = _time.monotonic()
    final_answer = ""
    finish_reason: str | None = None
    last_thinking_content: str = ""
    iterations = 0
    tools_schema = ToolBox.schemas()

    # Single closure shared across iterations so the thinking panel keeps
    # streaming continuously even as we hop between LLM calls + tool calls.
    # The web layer ``on_thinking_delta`` runs alongside the CLI
    # ``display.update_thinking`` — both fire on the same chunk so /ask
    # in the terminal and /studio in the browser stay in sync.
    def _forward_thinking(text: str) -> None:
        if display is not None:
            display.update_thinking(text)
        if on_thinking_delta is not None:
            try:
                on_thinking_delta(text)
            except Exception:
                # Never let a UI hook crash the agent loop.
                pass

    on_thinking = (
        _forward_thinking if (display is not None or on_thinking_delta is not None) else None
    )

    for iteration in range(_MAX_ITERATIONS):
        iterations = iteration + 1
        if cancel_token is not None and cancel_token.is_set():
            from amx.agents.orchestrator import RunCancelled

            raise RunCancelled(f"Cancelled before iteration {iteration}")
        chat_messages = [_convert_message_for_litellm(m) for m in messages]
        # Pre-call token estimate so the tracker can label this step's
        # input cost. Falls back to 0 silently when tiktoken can't
        # tokenise the message shape (some structured tool messages).
        est = estimate_tokens(chat_messages)

        # Visible-content streaming. The web layer's on_content_delta
        # forwards per-token deltas as ``answer.delta`` SSE events live.
        # Interim narration ("Let me search the catalog…") that precedes
        # a tool call is also streamed — the SPA resets its answer buffer
        # whenever a ``tool.call`` event arrives, so by the time the
        # final iteration streams its real answer the user sees only
        # that. This keeps the streaming feel without buffering inside
        # the agent loop.
        def _forward_content(chunk: str) -> None:
            if on_content_delta is None:
                return
            try:
                on_content_delta(chunk)
            except Exception:
                # Never let a UI hook crash the agent loop.
                pass

        result = llm.chat(
            chat_messages,
            temperature=0.0,
            max_tokens=_AGENT_MAX_TOKENS,
            use_logprobs=False,
            tools=tools_schema,
            tool_choice="auto",
            on_thinking=on_thinking,
            on_content=_forward_content if on_content_delta is not None else None,
            # Threading the cancel token through ``llm.chat`` means the
            # stream consumer can bail between chunks, not just at the
            # next iteration boundary. Without this a Cancel click during
            # a long streamed answer waits for the whole answer to drain.
            cancel_token=cancel_token,
        )
        # Per-step record so the Run detail Metrics card can render
        # an honest tool_agent.iter row -- the previous wiring summed
        # ``aggregated_usage`` for the SSE answer.final event but never
        # wrote into ``analysis_runs.tokens_json``.
        token_tracker.record_for(
            f"tool_agent.iter{iterations}",
            est,
            llm,
            getattr(result, "usage", None),
        )
        # Aggregate usage across iterations.
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            aggregated_usage[key] += int((result.usage or {}).get(key, 0) or 0)
        finish_reason = result.finish_reason or finish_reason

        # Some thinking-channel models (gpt-oss family on Ollama, etc.)
        # emit their visible answer into ``reasoning_content`` and leave
        # ``content`` empty when the prompt is short or the harmony
        # ``final`` channel never fires. Capture the last seen reasoning
        # so the empty-content branch below can surface it instead of a
        # bare "(empty response)".
        if getattr(result, "thinking_content", "") or "":
            last_thinking_content = str(result.thinking_content)

        if not result.tool_calls:
            final_answer = (result.content or "").strip()
            break

        # Append the assistant's tool-call request, then the tool results.
        messages.append(
            {
                "role": "assistant",
                "content": result.content or None,
                "tool_calls": [
                    {
                        "id": tc.id or f"tool_{iteration}_{idx}",
                        "type": "function",
                        "function": {"name": tc.name, "arguments": tc.arguments or "{}"},
                    }
                    for idx, tc in enumerate(result.tool_calls)
                ],
            }
        )
        for tc in result.tool_calls:
            tool_t0 = _time.monotonic()
            tool_result = toolbox.invoke(tc.name, tc.arguments or "{}")
            tool_elapsed_ms = int((_time.monotonic() - tool_t0) * 1000)
            per_tool_latency_ms[tc.name] = per_tool_latency_ms.get(tc.name, 0) + tool_elapsed_ms
            summary = _summarise_tool_call(tc, tool_result)
            summary["latency_ms"] = tool_elapsed_ms
            tool_call_log.append(summary)
            if on_tool_call is not None:
                try:
                    on_tool_call(summary)
                except Exception:
                    pass
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id or f"tool_{iteration}_{tc.name}",
                    "content": tool_result,
                }
            )
            # Partial-catalog honesty injection. When a tool returns
            # ``"partial": true`` the catalog skeleton hasn't finished
            # syncing this profile yet, the result came from a live
            # DB query and may not enumerate every table the user
            # actually owns. Prepend a system-flavoured user note so
            # the LLM can't confidently report "9 tables total" when
            # the real count is unknown. The note is injected as a
            # ``user`` role (system-style messages aren't accepted
            # mid-conversation on every provider) and is dropped from
            # the session memory by the recap logic that already
            # filters synthetic messages.
            if _looks_partial(tool_result):
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "(system note) AMX's catalog skeleton sync for the "
                            "active DB profile is still running, so the tool "
                            "result above came from a live database query. "
                            "Treat the listed rows as a snapshot — not the "
                            "complete picture. Mention this explicitly in "
                            'your answer ("the catalog is still syncing, so '
                            'this list may be incomplete") instead of '
                            "implying the rows are the full set."
                        ),
                    }
                )
    else:
        # Hit the iteration cap without a final answer — force a closing call
        # without ``tools`` so the LLM returns plain text from whatever it
        # gathered.
        closing_messages = [_convert_message_for_litellm(m) for m in messages] + [
            {
                "role": "user",
                "content": (
                    "You've reached the tool-call budget. Compose your final answer now, in "
                    f"{answer_language or 'english'}, based on the tool results above."
                ),
            }
        ]
        est = estimate_tokens(closing_messages)
        result = llm.chat(
            closing_messages,
            temperature=0.0,
            max_tokens=_AGENT_MAX_TOKENS,
            use_logprobs=False,
            on_thinking=on_thinking,
        )
        token_tracker.record_for(
            "tool_agent.final",
            est,
            llm,
            getattr(result, "usage", None),
        )
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            aggregated_usage[key] += int((result.usage or {}).get(key, 0) or 0)
        final_answer = (result.content or "").strip()
        finish_reason = result.finish_reason or finish_reason
        if getattr(result, "thinking_content", "") or "":
            last_thinking_content = str(result.thinking_content)

    total_latency_ms = int((_time.monotonic() - loop_start) * 1000)
    # When the model produced no visible content, surface *why* instead
    # of a bare "(empty response)". Three observed shapes:
    #   1. Reasoning-channel models (gpt-oss on Ollama, Kimi K2.x
    #      thinking, …) emit their entire answer into the thinking
    #      channel — show the last thinking block (truncated) so the
    #      user has something to react to and points at the model as
    #      the root cause.
    #   2. finish_reason == "length" means the budget was exhausted
    #      mid-generation — say so explicitly with the configured
    #      max_tokens in the message.
    #   3. Otherwise: just "(empty response)" with a hint to try a
    #      different model.
    if not final_answer:
        if last_thinking_content.strip():
            thinking_preview = last_thinking_content.strip()
            if len(thinking_preview) > 1200:
                thinking_preview = thinking_preview[:1200].rstrip() + "…"
            final_answer = (
                "_The model returned reasoning text but no visible answer "
                "— likely a thinking-channel model whose final channel "
                "never fired. Showing the reasoning summary below; try a "
                "different model for a clean answer._\n\n"
                f"{thinking_preview}"
            )
        elif finish_reason == "length":
            final_answer = (
                "_The model hit its output-token budget before producing "
                "a visible answer. Raise ``max_tokens`` under Settings → "
                "LLM (or set ``AMX_LLM_MIN_MAX_TOKENS``) and retry._"
            )
        else:
            final_answer = (
                "_Empty response from the model. The provider returned no "
                "tool calls and no content — try rephrasing the question "
                "or switching to a different model._"
            )

    return ToolAgentResult(
        answer=final_answer,
        tool_calls=tool_call_log,
        iterations=iterations,
        usage=aggregated_usage,
        finish_reason=finish_reason,
        scope_profiles=list(toolbox.db_profiles),
        focus_profile=focus_profile,
        total_latency_ms=total_latency_ms,
        per_tool_latency_ms=per_tool_latency_ms,
    )
