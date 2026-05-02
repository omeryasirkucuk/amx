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

from typing import Any

from amx.config import AMXConfig
from amx.llm.provider import LLMProvider
from amx.search.agent_tools import ToolBox
from amx.search.catalog import SearchCatalog

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
    ) -> None:
        self.answer = answer
        self.tool_calls = tool_calls
        self.iterations = iterations
        self.usage = usage
        self.finish_reason = finish_reason

    def as_dict(self) -> dict[str, Any]:
        return {
            "answer": self.answer,
            "tool_calls": self.tool_calls,
            "iterations": self.iterations,
            "usage": dict(self.usage),
            "finish_reason": self.finish_reason,
        }


def _agent_system_prompt(cfg: AMXConfig, schema_hint: list[str]) -> str:
    """The single system prompt the LLM sees throughout the loop.

    Includes live ground-truth (databases, schemas) so the model can route
    without us having to regex-classify the question.
    """
    db_name = cfg.db.database or cfg.db.catalog or cfg.db.project or "(active database)"
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
    # connected" when the user asks cross-DB questions.
    profile_lines: list[str] = []
    active_name = cfg.active_db_profile or "default"
    for profile_name, db_cfg in sorted(cfg.db_profiles.items()):
        marker = " (active)" if profile_name == active_name else ""
        db_target = db_cfg.database or db_cfg.catalog or db_cfg.project or "?"
        backend = db_cfg.backend or "?"
        profile_lines.append(f"  - {profile_name}{marker}: {backend} → {db_target}")
    profiles_block = (
        "\n".join(profile_lines)
        if profile_lines
        else "  (none configured — only the active connection is reachable)"
    )

    return (
        "You are AMX's metadata-search assistant. Answer the user's question by calling the "
        "tools available to you. NEVER guess; ALWAYS ground every claim in a tool result.\n\n"
        f"Active database: {db_name}\n"
        f"Schemas in this DB: {schema_line}\n"
        f"User's pinned schema: {current_schema}\n"
        f"User's pinned table: {current_table}\n"
        f"User's language preference: {metadata_lang}\n"
        "Connected DB profiles:\n"
        f"{profiles_block}\n"
        "Tools currently target the ACTIVE profile only — if the user asks about another "
        "profile, mention which profiles you can see and ask them to switch with `/use-db <name>`.\n\n"
        "Routing guidance — choose the smallest correct path:\n"
        "* User names an exact identifier ('vbrk', 'adrc') → call find_table_by_name first; if it\n"
        "  returns one match, call describe_table on it. If multiple, surface ALL matches and ask.\n"
        "* User asks 'tables in <schema>' / 'tables under <schema>' / 'list tables of <schema>' → \n"
        "  call list_tables_in_schema with that exact schema. The user said 'tables', not\n"
        "  'a table named X'.\n"
        "* User asks 'which schemas / what schemas / how many schemas' → call list_schemas.\n"
        "* User asks 'which databases / hangi veritabanları' → call list_databases.\n"
        "* User asks 'tables with boolean columns' / 'date columns' / 'all int columns' → \n"
        "  call find_columns_by_dtype with the type token. NEVER fall back to\n"
        "  search_columns_by_concept for dtype questions; concept search matches NAMES, not types.\n"
        "* User asks 'which tables don't have a comment?', 'tables without description',\n"
        "  'açıklaması olmayan tablolar', 'eksik comment', 'do all tables have descriptions?'\n"
        "  → call find_assets_missing_comment. This queries the LIVE DB directly because\n"
        "  catalog can lag right after /run-apply. NEVER use search_tables_by_concept or\n"
        "  search_columns_by_concept for coverage questions — those are concept-based and\n"
        "  return stale results when descriptions were just written.\n"
        "* User asks 'which tables can I join with X' / 'X ile birleşebilecek tablolar' → \n"
        "  call find_joinable_tables with the single table.\n"
        "* User asks how X and Y join (both named) → call get_join_candidates.\n"
        "* User asks ANY question about THEIR OWN past runs — 'compare my last 3 runs',\n"
        "  'what runs have I done on sales.orders', 'which settings did I use yesterday',\n"
        "  'son 3 koşumu karşılaştır', 'hangi llm modeli daha iyi sonuç verdi', 'has this\n"
        "  table been analyzed before', 'past runs', 'my history' → call list_past_runs\n"
        "  (optionally with schema/table filters), then describe_run for the specific run\n"
        "  IDs the user wants to drill into. NEVER reply 'I don't have access to your past\n"
        "  runs' — you DO have access via these tools, the local SQLite history at\n"
        "  ~/.amx/history.db is fair game and includes the full settings snapshot for\n"
        "  every run (prompt_detail, batch size, dedup, llm_profile, etc.).\n"
        "* User asks 'is X a primary key' / 'are PKs duplicated' / '(id, time)\n"
        "  unique mi' / 'composite PK gerekli mi yoksa id yeter mi' → \n"
        "  call check_uniqueness with the table and column tuple.\n"
        "* User asks 'is there duplication in TABLE' / 'are there duplicate\n"
        "  rows' / 'çoklama var mı' WITHOUT naming a candidate key → call\n"
        "  inspect_data_quality first to read per-column distinct_ratio,\n"
        "  THEN propose the most likely composite key (columns with\n"
        "  distinct_ratio ≈ 1.0) and offer to verify it with check_uniqueness.\n"
        "  NEVER bounce back with 'give me columns' — surface what the data\n"
        "  shows and let the user confirm.\n"
        "* User asks 'how nullable is column X' / 'çoklama oranı' / 'date format\n"
        "  nedir' / 'min and max of created_at' / 'ne zamandır tutuluyor' → \n"
        "  call inspect_data_quality. The result includes detected_format for\n"
        "  varchar columns that look like dates ('20240315' → 'YYYYMMDD',\n"
        "  '15-03-2024' → 'DD-MM-YYYY', etc.). For temporal coverage read\n"
        "  min_value/max_value of date/timestamp columns.\n"
        "* User asks 'give me an example / a sample value' / 'show me a value\n"
        "  from X' / 'aedat'tan bir örnek ver' / 'kolon nasıl görünüyor' → call\n"
        "  sample_column_values (light direct SELECT — no full profile).\n"
        "  CRITICAL: when the user names just the table without the schema,\n"
        "  call find_table_by_name FIRST so you don't blindly pick the wrong\n"
        "  schema (e.g. 'public' for a SAP-only table that lives in 'sap_s6p').\n"
        "  Only fall through to /search sync hints when find_table_by_name\n"
        "  truly returns no exact AND no fuzzy matches.\n"
        "* User asks 'what's the main / fact table here' / 'which tables look\n"
        "  like dimensions' / 'is this a star schema or snowflake' / 'bu\n"
        "  schemanın ana tablosu nedir' / 'fact ve dim tabloları' / 'lookup\n"
        "  tablo var mı' → call detect_dimensional_role. Pass ``schema`` only\n"
        "  to rank EVERY table in the schema and get the overall pattern\n"
        "  (star_schema / snowflake_schema / flat / fact_only / unknown);\n"
        "  pass ``schema`` + ``table`` to classify ONE table. The result\n"
        "  carries role_hypothesis + confidence + evidence + indicators\n"
        "  (naming, row-count percentile, FK fan-out / fan-in,\n"
        "  partition / clustering / temporal column presence). Always\n"
        "  quote the evidence in the answer; the role label alone is\n"
        "  misleading without the why.\n"
        "* User asks 'how does X hold history' / 'is this SCD2' / 'değişiklik\n"
        "  tek satırda mı yeni satır mı' / 'eski değerler nasıl tutuluyor' /\n"
        "  'history pattern' / 'is there a history table for X' → call\n"
        "  detect_scd_pattern. The result includes scd_type_hypothesis\n"
        "  (type_1 / type_2 / type_3 / type_4 / type_6 / append_only /\n"
        "  unknown), confidence, evidence (bullet list — ALWAYS quote\n"
        "  these in the answer; the hypothesis alone is misleading), and\n"
        "  alternative_hypotheses for hybrid cases. When evidence is\n"
        "  empty, suggest the user provide a candidate business_key so\n"
        "  the rows-per-key probe can disambiguate Type 1 vs Type 2.\n"
        "* User asks 'when was X last updated' / 'is there an update soon' /\n"
        "  'son güncelleme', 'next refresh', 'ETL ne zaman çalıştı' → call\n"
        "  describe_table and read ``analytics.last_modified``. NEVER answer\n"
        "  'I don't know about future updates' as a flat response — instead\n"
        "  surface the LAST known modification time AND state explicitly:\n"
        "  'AMX can see vbak was last modified at <ts> (from <backend's\n"
        "  freshness signal>). Scheduled future updates require an ETL /\n"
        "  orchestrator tap (Airflow / Dagster / dbt Cloud) that AMX doesn't\n"
        "  currently expose — that's a planned v0.11 feature.' Be precise\n"
        "  about what AMX can vs cannot see.\n"
        "* User asks 'how is X uploaded / loaded / populated / ingested /\n"
        "  refreshed' / 'bu tablo nasıl yükleniyor' / 'hangi şekilde besleniyor' /\n"
        "  'ETL süreci nasıl' / 'data nasıl geliyor' → this is the LOAD\n"
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
        "* User asks about a concept ('pricing tables', 'address columns', 'müşteri bilgisi') → \n"
        "  call search_tables_by_concept or search_columns_by_concept.\n"
        "* When the question is a follow-up (pronoun like 'it', 'this table', 'o tablo', 'bu',\n"
        "  short replies like 'only those?', 'sadece bunlar mı?', 'gerçekten?'), resolve it\n"
        "  from the prior assistant turn(s) BEFORE calling a tool. Read the conversation\n"
        "  history above; previous answers stay relevant.\n\n"
        "When you have enough information, STOP calling tools and return a short, direct answer.\n\n"
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
        "  of those are not correct', 'emin misin?', 'doğru değil', etc. — DO NOT\n"
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
        "Style:\n"
        "  - One natural-language paragraph.\n"
        "  - Quote schema.table identifiers in backticks.\n"
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


def _summarise_tool_call(tool_call: Any, result: str) -> dict[str, Any]:
    return {
        "name": tool_call.name,
        "arguments": tool_call.arguments,
        "result_preview": result[:280] + ("…" if len(result) > 280 else ""),
    }


def run_tool_agent(
    *,
    cfg: AMXConfig,
    catalog: SearchCatalog,
    llm: LLMProvider,
    question: str,
    answer_language: str,
    session_memory: list[dict[str, Any]] | None = None,
) -> ToolAgentResult:
    """Run the tool-calling loop and return the final synthesised answer.

    ``session_memory`` carries the recap of prior turns the planner already
    builds in ``SearchAgent._memory_summary``. We forward it as a prelude
    user/assistant exchange so the model can resolve "it" / "that table" /
    "bu tablo" without re-asking the user.
    """
    # Use ``with`` so the live DB connector (SQLAlchemy engine + connection
    # pool) is disposed at the end of every question. Without this, each
    # ``/ask`` turn leaks a few file descriptors; after enough turns the
    # process hits ``OSError: Too many open files`` (the user-reported case).
    with ToolBox(cfg, catalog) as toolbox:
        return _run_tool_loop(
            toolbox=toolbox,
            cfg=cfg,
            llm=llm,
            question=question,
            answer_language=answer_language,
            session_memory=session_memory,
        )


def _run_tool_loop(
    *,
    toolbox: ToolBox,
    cfg: AMXConfig,
    llm: LLMProvider,
    question: str,
    answer_language: str,
    session_memory: list[dict[str, Any]] | None,
) -> ToolAgentResult:
    # Pre-fetch the schema list once; if it succeeds we put it into the
    # system prompt so the LLM doesn't have to spend a tool call discovering
    # what schemas exist before answering simple "list tables in X" queries.
    schemas_hint: list[str] = []
    try:
        schemas_hint = [str(s) for s in toolbox._live_db().list_schemas()]  # noqa: SLF001
    except Exception:
        schemas_hint = []

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": _agent_system_prompt(cfg, schemas_hint)}
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
    final_answer = ""
    finish_reason: str | None = None
    iterations = 0
    tools_schema = ToolBox.schemas()

    for iteration in range(_MAX_ITERATIONS):
        iterations = iteration + 1
        result = llm.chat(
            [_convert_message_for_litellm(m) for m in messages],
            temperature=0.0,
            max_tokens=_AGENT_MAX_TOKENS,
            use_logprobs=False,
            tools=tools_schema,
            tool_choice="auto",
        )
        # Aggregate usage across iterations.
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            aggregated_usage[key] += int((result.usage or {}).get(key, 0) or 0)
        finish_reason = result.finish_reason or finish_reason

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
            tool_result = toolbox.invoke(tc.name, tc.arguments or "{}")
            tool_call_log.append(_summarise_tool_call(tc, tool_result))
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id or f"tool_{iteration}_{tc.name}",
                    "content": tool_result,
                }
            )
    else:
        # Hit the iteration cap without a final answer — force a closing call
        # without ``tools`` so the LLM returns plain text from whatever it
        # gathered.
        result = llm.chat(
            [_convert_message_for_litellm(m) for m in messages]
            + [
                {
                    "role": "user",
                    "content": (
                        "You've reached the tool-call budget. Compose your final answer now, in "
                        f"{answer_language or 'english'}, based on the tool results above."
                    ),
                }
            ],
            temperature=0.0,
            max_tokens=_AGENT_MAX_TOKENS,
            use_logprobs=False,
        )
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            aggregated_usage[key] += int((result.usage or {}).get(key, 0) or 0)
        final_answer = (result.content or "").strip()
        finish_reason = result.finish_reason or finish_reason

    return ToolAgentResult(
        answer=final_answer or "(empty response)",
        tool_calls=tool_call_log,
        iterations=iterations,
        usage=aggregated_usage,
        finish_reason=finish_reason,
    )
