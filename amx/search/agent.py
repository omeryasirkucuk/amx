"""Production-oriented search agent orchestration for AMX /search."""

from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable

from amx.agents.tools import SchemaExplorer
from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector, ProfilingError
from amx.llm.provider import LLMProvider
from amx.search.catalog import SearchAnswer, SearchCatalog
from amx.search.session_store import ChatSessionStore
from amx.storage.sqlite_store import history_store
from amx.utils.console import step_spinner
from amx.utils.logging import get_logger
from amx.utils.token_tracker import estimate_tokens

log = get_logger("search.agent")


class _SessionMemoryShim:
    """Backwards-compat alias for tests that called ``_SESSION_MEMORY.clear()``.

    The real conversation memory now lives in SQLite ``chat_sessions`` /
    ``chat_turns``; this shim wipes those tables so existing tests stay
    isolated without needing to rewrite each setUp/tearDown.
    """

    def clear(self) -> None:
        store = history_store()
        if store is None:
            return
        try:
            ChatSessionStore(store).reset_for_test()
        except Exception:
            pass


_SESSION_MEMORY = _SessionMemoryShim()


# Conservative input-token budget per LLM family. The /synthesize_answer
# step builds a JSON payload that includes potentially many retrieval
# rows; without a budget guard, large catalogs blow the model's context
# window with an opaque LLM error. The numbers leave headroom for the
# system prompt, plan, policy, retrieval_details, verification, and the
# generated answer max_tokens.
_DEFAULT_INPUT_TOKEN_BUDGET = 60_000


def _input_token_budget_for(model: str | None) -> int:
    """Conservative input-token budget for the active LLM model.

    Frontier models with very large context windows (Claude 3.5/4,
    Gemini 1.5/2.0 pro) get a higher budget; everything else uses the
    default 60K which fits OpenAI gpt-4o, gpt-4o-mini, DeepSeek, and
    most local servers.
    """
    if not model:
        return _DEFAULT_INPUT_TOKEN_BUDGET
    name = model.lower()
    if any(token in name for token in (
        "claude-3-5", "claude-sonnet-4", "claude-opus-4", "claude-3-opus",
        "claude-haiku-4",
    )):
        return 150_000  # Claude family: 200K context window.
    if any(token in name for token in (
        "gemini-1.5-pro", "gemini-2.0-pro", "gemini-2.0-flash",
        "gemini-1.5-flash",
    )):
        return 250_000  # Gemini family: 1M-2M context.
    return _DEFAULT_INPUT_TOKEN_BUDGET


def _trim_rows_to_token_budget(
    rows: list[dict[str, Any]],
    *,
    system_text: str,
    base_payload: dict[str, Any],
    budget: int,
) -> tuple[list[dict[str, Any]], int]:
    """Drop lowest-scored rows until the prompt fits ``budget`` tokens.

    Computes the per-row cost from a single full-payload encoding plus
    a no-rows encoding (O(n) total) rather than re-encoding inside a
    loop, so large row sets do not pay quadratic cost.

    Returns ``(kept_rows, dropped_count)``. The result is sorted by
    descending ``match_score`` so the highest-confidence rows survive.
    """
    if not rows:
        return rows, 0

    sorted_rows = sorted(
        rows, key=lambda row: float(row.get("match_score") or 0.0), reverse=True
    )

    full_payload = dict(base_payload, rows=sorted_rows, result_count=len(sorted_rows))
    full_msgs = [
        {"role": "system", "content": system_text},
        {"role": "user", "content": json.dumps(full_payload, ensure_ascii=True)},
    ]
    full_tokens = estimate_tokens(full_msgs)
    if full_tokens <= budget:
        return sorted_rows, 0

    empty_payload = dict(base_payload, rows=[], result_count=0)
    empty_msgs = [
        {"role": "system", "content": system_text},
        {"role": "user", "content": json.dumps(empty_payload, ensure_ascii=True)},
    ]
    base_tokens = estimate_tokens(empty_msgs)

    rows_token_cost = max(1, full_tokens - base_tokens)
    avg_per_row = max(1, rows_token_cost // len(sorted_rows))
    available_for_rows = max(0, budget - base_tokens)
    keep_count = max(0, available_for_rows // avg_per_row)
    keep_count = min(keep_count, len(sorted_rows))

    if keep_count >= len(sorted_rows):
        return sorted_rows, 0
    return sorted_rows[:keep_count], len(sorted_rows) - keep_count


@dataclass
class SearchPlan:
    intent: str
    out_of_domain: bool
    normalized_question: str
    search_mode: str
    question_class: str
    target_entity: str
    entity_hints: list[str]
    search_queries: list[str]
    needs_typo_recovery: bool
    answer_language: str
    ambiguity_flags: list[str]
    reason: str
    decision_confidence: str = "high"
    needs_clarification: bool = False
    clarification_question: str = ""
    review_notes: str = ""
    # Answer-shape hints. Empty/zero defaults mean "no signal from interpretation"
    # — the policy/derivation step picks a shape based on question_class.
    aggregation_op: str = ""        # "" | "max" | "min" | "top_k" | "bottom_k" | "count"
    aggregation_field: str = ""     # "" | "row_count" | "column_count" | "table_count"
    aggregation_limit: int = 0      # 0 = no aggregation; 1 for superlatives; N for top-K
    answer_shape: str = ""          # See _ANSWER_SHAPES below; "" = derive from policy.


# Closed set of presentation shapes the agent + renderer dispatch on.
# Kept as a module-level constant so tests and the renderer share the same vocabulary.
_ANSWER_SHAPES = {
    "single_fact",     # one-sentence headline, no list, no rich table
    "short_table",     # headline + 2-5 row markdown table inline in summary
    "full_table",      # broad inventory dump (existing behaviour)
    "ranked_list",     # headline + rich Search matches table (filtered to non-zero scores)
    "table_summary",   # headline + key-columns rich table for table_explain
    "join_candidates", # existing join Rich table dispatch
    "prose",           # 2-4 sentence explanation, no table
}


@dataclass
class SearchPolicy:
    question_class: str
    retrieval_policy: str
    requires_catalog: bool
    deterministic_answer: bool
    verify_live: bool
    allow_vector: bool
    allow_code: bool
    answer_format: str
    fallback_behavior: str
    answer_shape: str = ""  # Derived from plan.answer_shape or question_class.


@dataclass
class SearchActionSuggestion:
    action: str
    reason: str


@dataclass
class LiveProbePlan:
    needs_live_probe: bool
    reason: str
    operations: list[dict[str, str]]


@dataclass
class ResolvedTarget:
    requested: str
    resolved_path: str
    source: str
    is_exact: bool
    confidence: str
    warnings: list[str]
    candidates: list[str]


def _question_language_hint(text: str) -> str:
    sample = (text or "").strip()
    if not sample:
        return "english"
    lower = sample.lower()
    if re.search(r"[\u0600-\u06FF]", sample):
        return "arabic"
    if re.search(r"[\u3040-\u30FF\u4E00-\u9FFF]", sample):
        return "japanese"
    if re.search(r"[\uAC00-\uD7AF]", sample):
        return "korean"
    if re.search(r"[\u0400-\u04FF]", sample):
        return "russian"
    if any(ch in lower for ch in "çğıöşü"):
        return "turkish"
    return "english"


def _json_block(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]
    return json.loads(raw)


def _merge_usage(*payloads: dict[str, Any] | None) -> dict[str, Any]:
    merged = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "model_processing_sec": 0.0,
    }
    for payload in payloads:
        if not payload:
            continue
        merged["prompt_tokens"] += int(payload.get("prompt_tokens") or 0)
        merged["completion_tokens"] += int(payload.get("completion_tokens") or 0)
        merged["total_tokens"] += int(payload.get("total_tokens") or 0)
        merged["model_processing_sec"] += float(payload.get("model_processing_sec") or 0.0)
    return merged


class SearchAgent:
    """Multi-step metadata reasoning agent for /search."""

    def __init__(
        self,
        cfg: AMXConfig,
        catalog: SearchCatalog,
        *,
        llm_factory: type[LLMProvider] = LLMProvider,
        inventory_db_factory: Callable[[], DatabaseConnector] | None = None,
    ) -> None:
        self.cfg = cfg
        self.catalog = catalog
        self.settings = catalog.get_settings(cfg.active_db_profile or "default")
        self.db_profile = cfg.active_db_profile or "default"
        self._llm_factory = llm_factory
        self._inventory_db_factory = inventory_db_factory or (lambda: DatabaseConnector(self.cfg.db))
        self._llm: LLMProvider | None = None
        self._llm_profile = cfg.active_llm_profile or "default"
        self._session_store: ChatSessionStore | None = None
        self._session_id: int | None = None
        # Per-process fallback used when no SQLiteHistoryStore has been
        # initialised (some unit-test paths). Keyed by db_profile:llm_profile.
        self._fallback_memory: list[dict[str, Any]] = []

    def _llm_available(self) -> bool:
        if self.settings.get("llm_enabled", "true").lower() != "true":
            return False
        return bool(getattr(self.cfg.llm, "provider", "") and getattr(self.cfg.llm, "model", ""))

    def _llm_provider(self) -> LLMProvider:
        if self._llm is None:
            self._llm = self._llm_factory(self.cfg.llm)
        return self._llm

    def _memory_turns(self) -> int:
        try:
            return max(0, int(self.settings.get("conversation_memory_turns", "4")))
        except Exception:
            return 4

    def _ensure_session_store(self) -> ChatSessionStore | None:
        if self._session_store is not None:
            return self._session_store
        store = history_store()
        if store is None:
            return None
        self._session_store = ChatSessionStore(store)
        return self._session_store

    def _ensure_session_id(self) -> int | None:
        """Resolve the active chat session id.

        Each REPL boot starts fresh: ``cfg.active_chat_session_id`` is None
        until a `/ask` runs (or the user explicitly `/session resume`-d).
        We lazily call ``start_session`` so users who never run `/ask` don't
        accumulate empty session rows.
        """
        store = self._ensure_session_store()
        if store is None:
            return None
        existing = getattr(self.cfg, "active_chat_session_id", None)
        if existing:
            self._session_id = int(existing)
            return self._session_id
        if self._session_id is not None:
            return self._session_id
        sid = store.start_session(
            db_profile=self.db_profile,
            llm_profile=self._llm_profile,
        )
        self._session_id = sid
        try:
            self.cfg.active_chat_session_id = sid
        except Exception:
            pass
        return sid

    def _memory(self) -> list[dict[str, Any]]:
        store = self._ensure_session_store()
        sid = getattr(self.cfg, "active_chat_session_id", None) or self._session_id
        if store is None or not sid:
            return list(self._fallback_memory)
        turns = store.recent_turns(int(sid), limit=self._memory_turns(), include_summary=True)
        # Project to the legacy turn-shape used by callers
        # (_last_tables, _memory_summary, planner payloads).
        out: list[dict[str, Any]] = []
        for t in turns:
            role = str(t.get("role") or "")
            if role == "summary":
                out.append({
                    "question": "",
                    "intent": "compaction",
                    "topic": "previous_context_summary",
                    "tables": list(t.get("tables") or []),
                    "columns": list(t.get("columns") or []),
                    "answer_summary": str(t.get("answer_summary") or ""),
                })
                continue
            if role == "user":
                # Pair the user turn with the next assistant turn; we'll fill
                # answer_summary from there in a second pass below.
                out.append({
                    "question": str(t.get("question") or ""),
                    "intent": "",
                    "topic": "",
                    "tables": [],
                    "columns": [],
                    "answer_summary": "",
                })
                continue
            # assistant
            plan = t.get("plan") or {}
            payload = {
                "question": "",
                "intent": str(t.get("intent") or ""),
                "topic": str(t.get("topic") or plan.get("normalized_question") or ""),
                "tables": list(t.get("tables") or []),
                "columns": list(t.get("columns") or []),
                "answer_summary": str(t.get("answer_summary") or ""),
            }
            # Backfill question onto the most recent user-only entry if any.
            if out and out[-1].get("question") and not out[-1].get("intent"):
                out[-1]["intent"] = payload["intent"]
                out[-1]["topic"] = payload["topic"]
                out[-1]["tables"] = payload["tables"]
                out[-1]["columns"] = payload["columns"]
                out[-1]["answer_summary"] = payload["answer_summary"]
            else:
                out.append(payload)
        return out

    def _remember(self, turn: dict[str, Any]) -> None:
        """Persist an assistant turn (back-compat shape).

        ``turn`` carries: question, intent, topic, tables, columns, and
        optionally answer_summary, confidence, plan, tokens, request_id,
        run_id. The user-side row was already inserted at the top of
        ``ask()`` via ``append_user_turn``; this writes the matching
        assistant row.
        """
        store = self._ensure_session_store()
        sid = self._ensure_session_id()
        if store is None or not sid:
            self._fallback_memory.append(dict(turn))
            max_turns = self._memory_turns()
            if max_turns > 0:
                self._fallback_memory = self._fallback_memory[-max_turns:]
            return
        store.append_assistant_turn(
            int(sid),
            run_id=turn.get("run_id"),
            answer_summary=str(turn.get("answer_summary") or "")[:1000],
            intent=str(turn.get("intent") or ""),
            topic=str(turn.get("topic") or ""),
            confidence=str(turn.get("confidence") or ""),
            tables=list(turn.get("tables") or []),
            columns=list(turn.get("columns") or []),
            plan=turn.get("plan"),
            tokens=turn.get("tokens"),
            request_id=turn.get("request_id"),
        )

    def _memory_summary(self) -> list[dict[str, Any]]:
        summary: list[dict[str, Any]] = []
        for turn in self._memory():
            # 200 chars used to be enough for the JSON planner payload, but
            # the tool agent feeds these straight into a chat history — long
            # answers (e.g. "12 tables have boolean columns: ...") were being
            # cut off and the LLM failed to resolve "Only those?" follow-ups.
            # 1000 chars is comfortably under the 24K-input budget even with
            # 6+ pairs in scope.
            summary.append(
                {
                    "question": turn.get("question", ""),
                    "intent": turn.get("intent", ""),
                    "topic": turn.get("topic", ""),
                    "tables": turn.get("tables", []),
                    "columns": turn.get("columns", []),
                    "answer_summary": str(turn.get("answer_summary") or "")[:1000],
                }
            )
        return summary

    def _last_tables(self) -> list[str]:
        tables: list[str] = []
        for turn in reversed(self._memory()):
            for table in turn.get("tables", []) or []:
                if table and table not in tables:
                    tables.append(str(table))
            if tables:
                break
        return tables

    def _catalog_ready(self) -> tuple[bool, dict[str, Any]]:
        status = self.catalog.sync_status(self.db_profile)
        total = int((status.get("entities") or {}).get("total_entities") or 0)
        return total > 0, status

    def _plan_with_overrides(self, *, question: str, base: SearchPlan | None, question_language: str) -> SearchPlan:
        chosen = base or SearchPlan(
            intent="find_columns",
            out_of_domain=False,
            normalized_question=question,
            search_mode="semantic_concept",
            question_class="semantic_discovery",
            target_entity="column",
            entity_hints=[],
            search_queries=[question],
            needs_typo_recovery=False,
            answer_language=question_language,
            ambiguity_flags=[],
            reason="default semantic discovery fallback",
        )
        chosen = self._align_answer_language(chosen, question_language)
        return self._align_plan_shape(chosen, question)
    def _plan_from_payload(self, payload: dict[str, Any], question: str) -> SearchPlan:
        routing_keys = {"intent", "search_mode", "question_class", "target_entity"}
        if not any(key in payload for key in routing_keys):
            raise ValueError("payload does not include routing fields")
        search_mode = str(payload.get("search_mode") or "semantic_concept").strip() or "semantic_concept"
        question_class = str(payload.get("question_class") or "").strip() or self._class_from_mode(search_mode)
        request_type = str(payload.get("request_type") or "").strip().lower()
        if request_type == "coverage_audit":
            search_mode = "check_coverage"
            question_class = "inventory"
        confidence = str(payload.get("decision_confidence") or "high").strip().lower() or "high"
        if confidence not in {"high", "medium", "low"}:
            confidence = "medium"
        resolved_intent = str(payload.get("intent") or "find_columns")
        resolved_target = str(payload.get("target_entity") or "unknown").strip() or "unknown"
        if request_type == "coverage_audit":
            resolved_intent = "check_coverage"
            resolved_target = "database"
        aggregation_op = str(payload.get("aggregation_op") or "").strip().lower()
        if aggregation_op not in {"", "max", "min", "top_k", "bottom_k", "count"}:
            aggregation_op = ""
        aggregation_field = str(payload.get("aggregation_field") or "").strip().lower()
        if aggregation_field not in {"", "row_count", "column_count", "table_count"}:
            aggregation_field = ""
        try:
            aggregation_limit = int(payload.get("aggregation_limit") or 0)
        except (TypeError, ValueError):
            aggregation_limit = 0
        if aggregation_limit < 0:
            aggregation_limit = 0
        if aggregation_op in {"max", "min"} and aggregation_limit == 0:
            aggregation_limit = 1
        answer_shape = str(payload.get("answer_shape") or "").strip().lower()
        if answer_shape not in _ANSWER_SHAPES:
            answer_shape = ""
        return SearchPlan(
            intent=resolved_intent,
            out_of_domain=bool(payload.get("out_of_domain")),
            normalized_question=str(payload.get("normalized_question") or question).strip() or question,
            search_mode=search_mode,
            question_class=question_class,
            target_entity=resolved_target,
            entity_hints=[str(item).strip() for item in (payload.get("entity_hints") or []) if str(item).strip()],
            search_queries=[str(item).strip() for item in (payload.get("search_queries") or []) if str(item).strip()]
            or [str(payload.get("normalized_question") or question).strip() or question],
            needs_typo_recovery=bool(payload.get("needs_typo_recovery")),
            answer_language=str(payload.get("answer_language") or "").strip(),
            ambiguity_flags=[str(item).strip() for item in (payload.get("ambiguity_flags") or []) if str(item).strip()],
            reason=str(payload.get("reason") or "").strip(),
            decision_confidence=confidence,
            needs_clarification=bool(payload.get("needs_clarification")),
            clarification_question=str(payload.get("clarification_question") or "").strip(),
            review_notes=str(payload.get("review_notes") or "").strip(),
            aggregation_op=aggregation_op,
            aggregation_field=aggregation_field,
            aggregation_limit=aggregation_limit,
            answer_shape=answer_shape,
        )

    def _interpret_question_pass1(self, question: str) -> tuple[SearchPlan, dict[str, Any]]:
        llm = self._llm_provider()
        memory = self._memory_summary()
        metadata_language = self.cfg.llm.language or "english"
        system = (
            "You classify metadata-search questions for AMX /search.\n"
            "Return JSON only. Do not answer the question.\n"
            "You are choosing the smallest correct routing decision, not writing prose.\n"
            "Allowed search_mode values: semantic_concept, name_lookup, join_candidates, joinable_tables, "
            "table_explain, list_databases, list_schemas, count_tables, schema_inventory, compare_entities, unsupported.\n"
            "Allowed question_class values: inventory, entity_lookup, semantic_discovery, join_discovery, "
            "table_understanding, comparative_reasoning, unsupported.\n"
            "Allowed target_entity values: column, table, schema, database, aggregate, join_path, unknown.\n"
            "Allowed intent values: find_columns, join_candidates, explain_table, list_databases, list_schemas, "
            "count_tables, schema_inventory, compare_entities, unsupported.\n"
            "Allowed request_type values: metadata_discovery, coverage_audit, inventory, join, table_understanding, comparative_reasoning, unsupported.\n"
            "Core rules:\n"
            "- Set out_of_domain=true STRICTLY ONLY for greetings (e.g. hello, hi), small talk, or requests entirely unrelated to any kind of database or data context (e.g. write me Python code, tell me a joke).\n"
            "- Infer answer_language from the user question itself; do not rely on metadata_generation_language.\n"
            "- Think through alternatives before deciding, then output only final JSON.\n"
            "- Prefer exact metadata intent over broad semantic search when the user names a field, table, schema, or join target.\n"
            "- Preserve entity_hints exactly as the user wrote them, even when they look misspelled. Include table or column names here.\n"
            "- Always use session_memory to resolve context! If the user asks a follow-up (e.g. 'what about its columns?', 'and the other table?'), map it to the active topic.\n"
            "- Always include search_queries. Put the original wording first. For non-English questions, also include one concise English retrieval phrase.\n"
            "- normalized_question should be the best retrieval phrase, usually English when it improves recall.\n"
            "Routing rules:\n"
            "- request_type MUST be set first, then intent/search_mode/question_class/target_entity must align with it.\n"
            "- Field/code lookup where the user provides a technical identifier -> search_mode=name_lookup, question_class=entity_lookup, target_entity=column.\n"
            "- 'How do these two tables join' -> join_candidates, join_discovery, target_entity=join_path.\n"
            "- 'Which tables can join with X' -> joinable_tables, join_discovery, target_entity=table.\n"
            "- 'What does this table do' or 'what is ADRC table' -> table_explain, table_understanding, target_entity=table.\n"
            "- 'Which databases/schemas are known' or 'how many tables' -> inventory routes.\n"
            "- 'How many columns per table', 'column counts by table', or broad structural table inventories -> search_mode=schema_inventory, question_class=inventory, target_entity=table.\n"
            "- Broad missing-comment requests such as 'veri tabanlarımızda comment kısmı eksik olanlar var mı' are coverage_audit.\n"
            "- For coverage_audit use intent=check_coverage and search_mode=check_coverage.\n"
            "- Conceptual search for tables containing a business concept such as address details, pricing, customer identifiers, or dates -> semantic_concept, semantic_discovery, target_entity=table.\n"
            "- Conceptual search for fields/columns -> semantic_concept, semantic_discovery, target_entity=column.\n"
            "- Table comparison or equivalence -> compare_entities, comparative_reasoning.\n"
            "Quality rules:\n"
            "- NEVER use unsupported unless absolutely impossible to map. Default to search_mode=semantic_concept for ambiguous data requests.\n"
            "- Use ambiguity_flags for real risks such as missing_scope, ambiguous_table_name, cross_schema_risk, followup_scope_guess.\n"
            "- Set answer_language to the exact language the user wrote the question in.\n"
            "- Always output request_type.\n"
            "- Output decision_confidence (high|medium|low).\n"
            "- Set needs_clarification=true only when proceeding without clarification would likely misroute retrieval.\n"
            "- If needs_clarification=true, provide one short clarification_question.\n"
            "Answer shape rules (always emit these fields):\n"
            "- aggregation_op: \"\" | \"max\" | \"min\" | \"top_k\" | \"bottom_k\" | \"count\". Detect superlatives and rankings in any language: most/least/highest/lowest/biggest/smallest/largest, top N, first/last, leading, bottom; Turkish: en fazla, en az, en buyuk, en kucuk, en yuksek, en dusuk, en cok, en az; Spanish: el mayor, el menor; etc. Use \"count\" only for pure how-many questions.\n"
            "- aggregation_field: \"\" | \"row_count\" | \"column_count\" | \"table_count\". Pick the numeric facet the user is ranking by (rows/satir = row_count; columns/kolon = column_count; tables/tablo = table_count).\n"
            "- aggregation_limit: integer. 1 for superlatives (the X with the most Y); N for top-N/bottom-N; 0 if no aggregation.\n"
            "- answer_shape: pick one of single_fact, short_table, full_table, ranked_list, table_summary, prose. Or \"\" to let policy derive.\n"
            "  * single_fact: user wants ONE answer (a name, a number, a single ranked entity). Examples: superlatives with limit 1, count_tables, exact name lookups, list_databases when likely small.\n"
            "  * short_table: top-K (limit 2-10), small ranked comparisons, side-by-side of <=10 entities.\n"
            "  * full_table: broad dump-everything inventories (\"list all tables in X\", \"columns per table\", \"show me everything in X\").\n"
            "  * ranked_list: open-ended semantic_discovery (\"tables about pricing\").\n"
            "  * table_summary: table_understanding / \"what is table X\".\n"
            "  * prose: why/how/explanatory questions, comparative reasoning without an explicit entity list.\n"
            "  * Leave \"\" if you genuinely cannot tell."
        )
        user = json.dumps(
            {
                "question": question,
                "session_memory": memory,
                "current_schema": self.cfg.current_schema or "",
                "current_table": self.cfg.current_table or "",
                "metadata_generation_language": metadata_language,
                "active_db_profile": self.db_profile,
            },
            ensure_ascii=True,
        )
        result = llm.chat(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=900,
            use_logprobs=False,
        )
        payload = _json_block(result.content)
        return (self._plan_from_payload(payload, question), result.usage or {})

    def _review_question_plan_pass2(self, question: str, draft: SearchPlan) -> tuple[SearchPlan, dict[str, Any]]:
        llm = self._llm_provider()
        system = (
            "You are a strict reviewer for AMX /search routing decisions.\n"
            "Return JSON only.\n"
            "You receive a draft routing plan. Validate it against the question and session context.\n"
            "Correct the plan when needed, but keep smallest valid route change.\n"
            "Always output request_type and ensure it aligns with intent/search_mode/question_class.\n"
            "Broad missing-comment coverage questions must be request_type=coverage_audit with check_coverage route.\n"
            "If uncertainty remains, set needs_clarification=true with one short clarification_question.\n"
            "Always output decision_confidence (high|medium|low).\n"
            "Infer answer_language from question; keep multilingual behavior without hardcoded language lists.\n"
            "Always re-emit aggregation_op, aggregation_field, aggregation_limit, and answer_shape.\n"
            "Detect superlatives/rankings in any language (most/least/top-N/bottom-N; Turkish en fazla/en az/en cok). Set aggregation_limit=1 for superlatives, N for top-N, 0 if no aggregation.\n"
            "Pick answer_shape from: single_fact (one specific answer), short_table (top-K <=10), full_table (broad inventory dump), ranked_list (semantic_discovery), table_summary (table_understanding), prose (explanation), or \"\" if uncertain.\n"
        )
        user = json.dumps(
            {
                "question": question,
                "draft_plan": asdict(draft),
                "session_memory": self._memory_summary(),
                "current_schema": self.cfg.current_schema or "",
                "current_table": self.cfg.current_table or "",
                "active_db_profile": self.db_profile,
            },
            ensure_ascii=True,
        )
        result = llm.chat(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.0,
            max_tokens=700,
            use_logprobs=False,
        )
        payload = _json_block(result.content)
        payload.setdefault("review_notes", "reviewed_by_pass2")
        return (self._plan_from_payload(payload, question), result.usage or {})

    def _interpret_question_balanced(self, question: str) -> tuple[SearchPlan, dict[str, Any]]:
        draft, usage_1 = self._interpret_question_pass1(question)
        should_review = (
            draft.decision_confidence in {"low", "medium"}
            or draft.needs_clarification
            or bool(draft.ambiguity_flags)
            or draft.search_mode in {"unsupported", "check_coverage"}
        )
        if not should_review:
            return draft, usage_1
        try:
            reviewed, usage_2 = self._review_question_plan_pass2(question, draft)
            return reviewed, _merge_usage(usage_1, usage_2)
        except Exception:
            return draft, usage_1

    def _class_from_mode(self, search_mode: str) -> str:
        if search_mode in {"list_databases", "list_schemas", "count_tables", "schema_inventory"}:
            return "inventory"
        if search_mode == "name_lookup":
            return "entity_lookup"
        if search_mode in {"join_candidates", "joinable_tables"}:
            return "join_discovery"
        if search_mode == "table_explain":
            return "table_understanding"
        if search_mode == "compare_entities":
            return "comparative_reasoning"
        if search_mode == "unsupported":
            return "unsupported"
        return "semantic_discovery"

    def _align_answer_language(self, plan: SearchPlan, question_language: str) -> SearchPlan:
        # Trust LLM answer_language unless empty/unknown.
        if plan.answer_language and plan.answer_language.lower() not in {"unknown", ""}:
            return plan

        normalized = (question_language or "english").strip().lower() or "english"
        return SearchPlan(
            intent=plan.intent,
            out_of_domain=plan.out_of_domain,
            normalized_question=plan.normalized_question,
            search_mode=plan.search_mode,
            question_class=plan.question_class,
            target_entity=plan.target_entity,
            entity_hints=list(plan.entity_hints),
            search_queries=list(plan.search_queries),
            needs_typo_recovery=plan.needs_typo_recovery,
            answer_language=normalized,
            ambiguity_flags=list(plan.ambiguity_flags),
            reason=plan.reason,
            decision_confidence=plan.decision_confidence,
            needs_clarification=plan.needs_clarification,
            clarification_question=plan.clarification_question,
            review_notes=plan.review_notes,
            aggregation_op=plan.aggregation_op,
            aggregation_field=plan.aggregation_field,
            aggregation_limit=plan.aggregation_limit,
            answer_shape=plan.answer_shape,
        )

    # Tokens that — alone or in a short greeting — should not be sent to the
    # LLM-driven planner. ``_handle_chitchat`` short-circuits these with a
    # one-line friendly redirect so the user doesn't get a confusing
    # "Could you clarify the exact scope?" reply for "nasılsın".
    _CHITCHAT_TOKENS: frozenset[str] = frozenset({
        "hi", "hello", "hey", "hola", "yo", "sup", "howdy",
        "merhaba", "selam", "slm", "naber", "nbr", "nasilsin", "nasılsın",
        "iyimisin", "iyi", "misin", "musun", "musunuz", "miydin", "iyiydin",
        "teşekkür", "teşekkürler", "tesekkur", "tesekkurler", "teşekkurler",
        "thanks", "thank", "ty", "thx", "ok", "okay", "tamam",
        "günaydın", "gunaydin", "günler", "gunler", "akşamlar", "aksamlar",
        "good", "morning", "evening", "afternoon", "night",
        "what's", "whats", "wassup", "up",
    })

    def _handle_chitchat(self, question: str, question_language: str) -> SearchAnswer | None:
        """Recognise greetings / "how are you" / thanks and reply directly.

        Without this short-circuit the LLM planner sometimes flags the input
        as ``needs_clarification=True``, yielding "Could you clarify the
        exact scope (database/schema/table)?" — a confusing reply when the
        user just typed "nasılsın".
        """
        sample = (question or "").strip().lower()
        if not sample:
            return None
        # Must be short and contain only chitchat tokens — punctuation aside.
        words = [tok for tok in re.split(r"[\s\?!.,;:]+", sample) if tok]
        if not words or len(words) > 4:
            return None
        if not all(word in self._CHITCHAT_TOKENS for word in words):
            return None
        if (question_language or "").lower() == "turkish":
            summary = (
                "Merhaba! Ben AMX'in metadata arama asistanıyım — sohbete eşlik etmem yerine "
                "veritabanı şeması/kolonları/tabloları hakkında sorularınızı cevaplamak için varım. "
                "Örnek: `vbrk tablosu nedir?`, `pricing ile ilgili tablolar hangileri?`."
            )
        else:
            summary = (
                "Hi! I'm AMX's metadata search assistant — I'm built to answer questions about "
                "your database schemas, tables, and columns rather than chat. "
                "Try: `what is the vbrk table?`, `which tables relate to pricing?`."
            )
        self._record_short_circuit_assistant(summary=summary, intent="chitchat")
        return SearchAnswer(
            intent="chitchat",
            question=question,
            rows=[],
            confidence="high",
            summary=summary,
            provenance=["client_side_short_circuit"],
            details={
                "reason": "chitchat_short_circuit",
                "answer_language": question_language or "english",
                "answer_shape": "single_fact",
                "stage_metrics": [],
            },
        )

    def _record_short_circuit_assistant(self, *, summary: str, intent: str) -> None:
        """Persist a synthetic assistant turn for chitchat / meta / reaffirm.

        Without this, ``ask()`` writes the user-side row at the top of the
        call but no matching assistant row gets written for the deterministic
        short-circuits — leaving the session memory unbalanced and confusing
        the next planner pass. We record a small assistant turn carrying just
        the answer text + the short-circuit kind so memory stays paired.
        """
        store = self._ensure_session_store()
        sid = self.cfg.active_chat_session_id
        if store is None or not sid:
            return
        try:
            store.append_assistant_turn(
                int(sid),
                run_id=None,
                answer_summary=str(summary or "")[:480],
                intent=intent,
                plan={"agent": "short_circuit", "kind": intent},
                confidence="high",
            )
        except Exception as exc:
            log.warning("Failed to record %s assistant turn: %s", intent, exc)

    def _handle_meta_query(self, question: str, question_language: str) -> SearchAnswer | None:
        """Answer questions ABOUT the conversation itself (no LLM call).

        Patterns: "what was my previous question?", "what did I ask?",
        "bir önceki sorum neydi", "ben ne sormuştum". Resolves against
        ``ChatSessionStore.recent_turns`` so the user gets the literal prior
        question text rather than a clarification prompt.
        """
        sample = (question or "").strip().lower()
        if not sample:
            return None
        meta_patterns = (
            r"\b(?:bir\s+)?(?:o)?(?:n|ö)nce(?:ki)?\s+sor(?:u(?:m|n)?|ulardan)\b",
            r"\bben\s+ne\s+sor(?:du|mu[sş]tum|du[mn])\b",
            r"\b(?:son|previous|prior|last)\s+(?:question|sor(?:u|um))\b",
            r"\bwhat\s+(?:did|was)\s+(?:i|my)\s+(?:last\s+|previous\s+|prior\s+)?(?:question|ask)\b",
            r"\bwhat\s+have\s+i\s+(?:asked|been\s+asking)\b",
            r"\bne\s+sor(?:du(?:m|n)|mu[sş]tum)\b",
        )
        if not any(re.search(p, sample) for p in meta_patterns):
            return None
        prior_question = ""
        store = self._ensure_session_store()
        sid = self.cfg.active_chat_session_id
        if store is not None and sid:
            try:
                turns = store.recent_turns(int(sid), include_summary=False, limit=8)
            except Exception:
                turns = []
            user_turns = [t for t in turns if str(t.get("role") or "") == "user"]
            # The latest user turn IS this very question (just appended);
            # we want the one BEFORE it.
            if len(user_turns) >= 2:
                prior_question = str(user_turns[-2].get("question") or "").strip()
        is_turkish = (question_language or "").lower() == "turkish"
        if not prior_question:
            summary = (
                "Bu oturumdaki ilk sorunuz; daha önce hiçbir soru kaydedilmemiş."
                if is_turkish
                else "This is the first question in this session; no prior question is on record."
            )
        else:
            summary = (
                f"Bir önceki sorunuz: \"{prior_question}\""
                if is_turkish
                else f"Your previous question was: \"{prior_question}\""
            )
        self._record_short_circuit_assistant(summary=summary, intent="meta_query")
        return SearchAnswer(
            intent="meta_query",
            question=question,
            rows=[],
            confidence="high",
            summary=summary,
            provenance=["chat_session_store"],
            details={
                "reason": "meta_query_short_circuit",
                "answer_language": question_language or "english",
                "answer_shape": "single_fact",
                "prior_question": prior_question,
                "stage_metrics": [],
            },
        )

    # Short reaffirmation / doubt phrasings the user uses to push back on the
    # PRIOR answer. Without a deterministic handler, the LLM planner reads
    # them as fresh questions with no scope and falls into clarification.
    _AFFIRM_FOLLOWUP_RE: tuple[str, ...] = (
        # English
        r"^\s*(?:are\s+you\s+sure|you\s+sure|really|seriously|sure\?+)\s*[\.\?\!]*\s*$",
        r"^\s*(?:is\s+that\s+(?:right|correct|true)|you\s+positive|positive\?+)\s*[\.\?\!]*\s*$",
        r"^\s*(?:why|why\??|how\s+come|how)\s*[\.\?\!]*\s*$",
        # Turkish
        r"^\s*(?:emin\s+misin|gercekten\s+mi|gerçekten\s+mi|kesin\s+mi|öyle\s+mi|oyle\s+mi|sahi\s+mi|hadi\s+ya)\s*[\.\?\!]*\s*$",
        r"^\s*(?:neden|niye|niçin|nicin|nasıl|nasil)\s*[\.\?\!]*\s*$",
    )

    def _handle_followup_reaffirmation(
        self, question: str, question_language: str
    ) -> SearchAnswer | None:
        """Restate the prior assistant turn when the user pushes back briefly.

        The user types "Are you sure?" / "emin misin?" / "really?" — these are
        too short for the planner to map to anything meaningful and we don't
        want to fall through to "Could you clarify the exact scope?". Pull
        the last assistant turn out of the session store and re-confirm it
        verbatim.
        """
        sample = (question or "").strip().lower()
        if not sample:
            return None
        if not any(re.match(pattern, sample) for pattern in self._AFFIRM_FOLLOWUP_RE):
            return None
        store = self._ensure_session_store()
        sid = self.cfg.active_chat_session_id
        if store is None or not sid:
            return None
        try:
            turns = store.recent_turns(int(sid), include_summary=False, limit=8)
        except Exception:
            return None
        # Find the most recent assistant turn (the one we want to confirm).
        prior_assistant = ""
        for turn in reversed(turns):
            if str(turn.get("role") or "") == "assistant":
                prior_assistant = str(turn.get("answer_summary") or turn.get("answer") or "").strip()
                if prior_assistant:
                    break
        if not prior_assistant:
            return None
        is_turkish = (question_language or "").lower() == "turkish"
        if is_turkish:
            summary = (
                "Eminim — önceki cevap canlı veritabanı metadata'sından geldi. Yeniden: "
                + prior_assistant
            )
        else:
            summary = (
                "Yes, I'm sure — the previous answer came from live database metadata. To restate: "
                + prior_assistant
            )
        self._record_short_circuit_assistant(summary=summary, intent="reaffirmation")
        return SearchAnswer(
            intent="reaffirmation",
            question=question,
            rows=[],
            confidence="high",
            summary=summary,
            provenance=["chat_session_store", "reaffirm_short_circuit"],
            details={
                "reason": "followup_reaffirmation",
                "answer_language": question_language or "english",
                "answer_shape": "prose",
                "prior_assistant": prior_assistant,
                "stage_metrics": [],
            },
        )

    def _answer_via_tool_agent(
        self,
        *,
        question: str,
        clean_question: str,
        question_language: str,
    ) -> SearchAnswer | None:
        """Run the tool-calling loop and return a SearchAnswer.

        Returns ``None`` on any unexpected failure so the caller can fall
        back to the legacy LLM-Pass-1 path. The legacy path stays in place
        as a deliberate safety net during this rollout.
        """
        try:
            # Lazy import keeps a circular path between agent.py / tool_agent.py
            # impossible — tool_agent imports from agent_tools and catalog only.
            from amx.search.tool_agent import run_tool_agent
        except Exception as exc:
            log.warning("tool_agent unavailable, falling back to legacy router: %s", exc)
            return None
        # Convert the existing memory summary into the {role, content} pairs
        # the tool agent expects for context. ``_memory_summary`` returns
        # the most recent turns in chronological order; we keep both user
        # questions and assistant answer summaries so follow-ups resolve.
        # IMPORTANT: ``ask()`` already wrote the *current* user question to
        # the session store at the top of the call, so the latest entry in
        # ``_memory_summary()`` IS the question we're about to ask the LLM.
        # If we forward it here, ``run_tool_agent`` would then append it a
        # second time as the live user message — duplication confuses the
        # model ("Only those?" became unrecognisable). Drop the trailing
        # entry whose ``question`` matches the current one and which has no
        # paired assistant answer yet.
        memory_turns = list(self._memory_summary())
        if memory_turns:
            tail = memory_turns[-1]
            tail_q = str(tail.get("question") or "").strip()
            tail_ans = str(tail.get("answer_summary") or "").strip()
            if tail_q == clean_question and not tail_ans:
                memory_turns = memory_turns[:-1]
        prior_turns: list[dict[str, str]] = []
        for turn in memory_turns:
            user_q = str(turn.get("question") or "").strip()
            if user_q:
                prior_turns.append({"role": "user", "content": user_q})
            assistant_summary = str(turn.get("answer_summary") or "").strip()
            if assistant_summary:
                prior_turns.append({"role": "assistant", "content": assistant_summary})
        try:
            t0 = time.monotonic()
            with step_spinner("Search Agent: thinking with tools"):
                result = run_tool_agent(
                    cfg=self.cfg,
                    catalog=self.catalog,
                    llm=self._llm_provider(),
                    question=clean_question,
                    answer_language=question_language,
                    session_memory=prior_turns,
                )
            elapsed = round(time.monotonic() - t0, 4)
        except Exception as exc:
            log.warning("Tool agent failed (%s); falling back to legacy router.", exc)
            return None

        # Persist the assistant turn so follow-up turns can read the recap.
        sid = self.cfg.active_chat_session_id
        store = self._ensure_session_store()
        if store is not None and sid:
            try:
                store.append_assistant_turn(
                    int(sid),
                    run_id=None,
                    answer_summary=result.answer[:480],
                    intent="tool_agent",
                    plan={"agent": "tool_agent", "iterations": result.iterations},
                    tokens=result.usage,
                    confidence="high",
                )
            except Exception as exc:
                log.warning("Failed to record tool-agent assistant turn: %s", exc)

        return SearchAnswer(
            intent="tool_agent",
            question=question,
            rows=[],
            confidence="high",
            summary=result.answer,
            provenance=["tool_calling_agent"],
            details={
                "answer_shape": "prose",
                "answer_language": question_language or "english",
                "agent": "tool_calling",
                "iterations": result.iterations,
                "tool_calls": result.tool_calls,
                "tokens": result.usage,
                "stage_metrics": [{"stage": "tool_agent", "duration_sec": elapsed}],
                "evidence_sources": [
                    f"tool:{call.get('name','')}" for call in result.tool_calls if call.get("name")
                ],
            },
        )

    def _catalog_resolvable_subject(self, question: str) -> str | None:
        """Return the first explicit subject token we can confirm is a
        table — either because the user explicitly called it a "table"
        ("vbrk table" / "tablo X"), or because the catalog / live DB has
        it under that exact name.

        Used to ground-truth re-routing: we override the LLM's mode to
        ``table_explain`` when the user named a real table.
        Column-shaped tokens like "vbrk_id" don't reach a strong-mention
        branch and won't be confirmed by the catalog as a table, so they
        skip the override.
        """
        for mention in self._explicit_table_mentions_for_question(question):
            requested = str(mention.get("requested") or "").strip()
            if not requested:
                continue
            # Strong mentions (user said "X table" / "table X" / "schema.table")
            # don't need extra confirmation. The user explicitly called the
            # noun a table, so we trust the route. ``_resolve_table_targets``
            # will still surface "not found" cleanly if the catalog and
            # live DB both come up empty.
            if str(mention.get("strength") or "") == "strong":
                return requested
            try:
                rows = self.catalog.find_tables_by_exact_name(self.db_profile, requested, limit=2)
            except Exception:
                rows = []
            if rows:
                return requested
            # Weak mention not in catalog — last chance is the live DB.
            # Cheap when ``current_schema`` is set; we skip the schema
            # iteration when it isn't (would require N HEAD queries).
            current_schema = (self.cfg.current_schema or "").strip()
            if current_schema:
                try:
                    if self._live_table_exists(current_schema, requested) is True:
                        return requested
                except Exception:
                    pass
        return None

    def _align_plan_shape(self, plan: SearchPlan, question: str) -> SearchPlan:
        sample = (question or "").strip().lower()
        # Guard: if the user clearly named a table-like subject ("what's the
        # vbrk", "describe customers", "vbrk nedir") and the LLM happened to
        # route this somewhere that won't run target resolution, force
        # ``table_explain`` so we either find the table or surface a clear
        # "not found / ambiguous" message instead of drifting to unrelated
        # rows. We require the token to be a CATALOG-CONFIRMED table name so
        # column-shaped tokens like "vbrk_id" don't get incorrectly mapped to
        # a missing table.
        explicit_subjects = self._explicit_table_mentions_for_question(question)
        catalog_subject = self._catalog_resolvable_subject(question) if explicit_subjects else None
        # Modes we never override: explicit join/coverage/counting questions.
        # Everything else (semantic_concept, name_lookup, list_databases,
        # list_schemas, schema_inventory, compare_entities, unsupported)
        # gets re-routed when the user named a real catalog table — those
        # modes won't run target resolution and would otherwise drift.
        protected_modes = {
            "table_explain",
            "join_candidates",
            "joinable_tables",
            "count_tables",
            "check_coverage",
        }
        if catalog_subject and plan.search_mode not in protected_modes:
            asks_join_word = any(
                token in sample
                for token in ("join", "link", "relate", "relationship", "bağ", "bag", "ilişk", "iliski")
            )
            if not asks_join_word:
                hints_with_subjects: list[str] = list(plan.entity_hints)
                # Prefer the catalog-confirmed subject as the primary hint so
                # ``_resolve_table_targets`` finds it first, then merge any
                # remaining tokens behind it.
                if catalog_subject and catalog_subject not in hints_with_subjects:
                    hints_with_subjects.insert(0, catalog_subject)
                for mention in explicit_subjects:
                    requested = str(mention.get("requested") or "").strip()
                    if requested and requested not in hints_with_subjects:
                        hints_with_subjects.append(requested)
                plan = SearchPlan(
                    intent="explain_table",
                    out_of_domain=plan.out_of_domain,
                    normalized_question=plan.normalized_question or question,
                    search_mode="table_explain",
                    question_class="table_understanding",
                    target_entity="table",
                    entity_hints=hints_with_subjects,
                    search_queries=list(plan.search_queries) or [question],
                    needs_typo_recovery=plan.needs_typo_recovery,
                    answer_language=plan.answer_language,
                    ambiguity_flags=list(plan.ambiguity_flags),
                    reason=(plan.reason + "; rerouted to table_explain because the question names a catalog-confirmed subject").strip("; "),
                    decision_confidence="high",
                    needs_clarification=False,
                    clarification_question="",
                    review_notes=plan.review_notes,
                    aggregation_op=plan.aggregation_op,
                    aggregation_field=plan.aggregation_field,
                    aggregation_limit=plan.aggregation_limit,
                    answer_shape=plan.answer_shape or "table_summary",
                )
        asks_count = any(token in sample for token in ("kaç", "kac", "how many", "count"))
        asks_table_word = any(token in sample for token in ("tablo", "tablolar", "table", "tables"))
        asks_column_word = any(token in sample for token in ("kolon", "kolonlar", "column", "columns", "field", "fields"))
        asks_listing = any(token in sample for token in ("hangi", "tüm", "tum", "list", "show", "söyle", "soyle", "tell", "getir", "listele", "bul"))
        asks_per_table = any(token in sample for token in ("per table", "by table", "which table", "hangi tabl", "tablo baz", "her tablo"))
        asks_comment_coverage = any(token in sample for token in ("comment", "comments", "açıklama", "aciklama", "yorum"))
        asks_relationship = any(token in sample for token in ("join", "link", "relationship", "relate", "connect", "bağ", "bag"))
        asks_semantic_table_concept = any(
            token in sample for token in ("içinde", "icinde", "alak", "related", "detail", "detay", "contain", "containing", "with", "olan")
        )
        if asks_column_word and asks_table_word and (asks_count or asks_per_table) and not asks_comment_coverage and not asks_relationship:
            return SearchPlan(
                intent="schema_inventory",
                out_of_domain=plan.out_of_domain,
                normalized_question=plan.normalized_question or question,
                search_mode="schema_inventory",
                question_class="inventory",
                target_entity="table",
                entity_hints=list(plan.entity_hints),
                search_queries=list(plan.search_queries) or [question],
                needs_typo_recovery=plan.needs_typo_recovery,
                answer_language=plan.answer_language,
                ambiguity_flags=list(plan.ambiguity_flags),
                reason=(plan.reason + "; routed to SchemaExplorer structural inventory").strip("; "),
                decision_confidence=plan.decision_confidence,
                needs_clarification=False,
                clarification_question="",
                review_notes=plan.review_notes,
                aggregation_op=plan.aggregation_op,
                aggregation_field=plan.aggregation_field,
                aggregation_limit=plan.aggregation_limit,
                answer_shape=plan.answer_shape,
            )
        if asks_column_word and asks_listing and plan.search_mode == "table_explain" and not self._explicit_table_paths_for_question(question):
            return SearchPlan(
                intent="find_columns",
                out_of_domain=plan.out_of_domain,
                normalized_question=plan.normalized_question,
                search_mode="semantic_concept",
                question_class="semantic_discovery",
                target_entity="column",
                entity_hints=[],
                search_queries=list(plan.search_queries),
                needs_typo_recovery=plan.needs_typo_recovery,
                answer_language=plan.answer_language,
                ambiguity_flags=list(plan.ambiguity_flags),
                reason=(plan.reason + "; rerouted from table explanation to column discovery").strip("; "),
                decision_confidence=plan.decision_confidence,
                needs_clarification=plan.needs_clarification,
                clarification_question=plan.clarification_question,
                review_notes=plan.review_notes,
            )
        if plan.search_mode == "count_tables" and asks_table_word and not asks_count and (asks_listing or asks_semantic_table_concept):
            return SearchPlan(
                intent="find_tables",
                out_of_domain=plan.out_of_domain,
                normalized_question=plan.normalized_question,
                search_mode="semantic_concept",
                question_class="semantic_discovery",
                target_entity="table",
                entity_hints=list(plan.entity_hints),
                search_queries=list(plan.search_queries),
                needs_typo_recovery=plan.needs_typo_recovery,
                answer_language=plan.answer_language,
                ambiguity_flags=list(plan.ambiguity_flags),
                reason=(plan.reason + "; rerouted to table semantic discovery").strip("; "),
                decision_confidence=plan.decision_confidence,
                needs_clarification=plan.needs_clarification,
                clarification_question=plan.clarification_question,
                review_notes=plan.review_notes,
            )
        normalized_mode = (plan.search_mode or "semantic_concept").strip()
        normalized_class = (plan.question_class or "").strip() or self._class_from_mode(normalized_mode)
        normalized_target = (plan.target_entity or "unknown").strip() or "unknown"
        if normalized_target not in {"column", "table", "schema", "database", "aggregate", "join_path", "unknown"}:
            normalized_target = "unknown"
        if not plan.search_queries:
            search_queries = [plan.normalized_question or question]
        else:
            search_queries = list(plan.search_queries)
        normalized_confidence = (plan.decision_confidence or "medium").strip().lower() or "medium"
        if normalized_confidence not in {"high", "medium", "low"}:
            normalized_confidence = "medium"
        if plan.needs_clarification and not plan.clarification_question:
            fallback_clarify = (
                "Could you clarify the exact schema/table scope so I can route this correctly?"
                if (plan.answer_language or "english").lower() == "english"
                else "Kapsami netlestirebilir misiniz (schema/tablo) ki dogru sekilde yonlendireyim?"
            )
            clarification_question = fallback_clarify
        else:
            clarification_question = plan.clarification_question
        return SearchPlan(
            intent=plan.intent,
            out_of_domain=plan.out_of_domain,
            normalized_question=plan.normalized_question or question,
            search_mode=normalized_mode,
            question_class=normalized_class,
            target_entity=normalized_target,
            entity_hints=list(plan.entity_hints),
            search_queries=search_queries,
            needs_typo_recovery=plan.needs_typo_recovery,
            answer_language=plan.answer_language,
            ambiguity_flags=list(plan.ambiguity_flags),
            reason=plan.reason,
            decision_confidence=normalized_confidence,
            needs_clarification=plan.needs_clarification,
            clarification_question=clarification_question,
            review_notes=plan.review_notes,
            aggregation_op=plan.aggregation_op,
            aggregation_field=plan.aggregation_field,
            aggregation_limit=plan.aggregation_limit,
            answer_shape=plan.answer_shape,
        )

    def _should_remember_table_scope(self, plan: SearchPlan, retrieval_details: dict[str, Any], question: str) -> bool:
        if retrieval_details.get("resolved_tables"):
            return True
        if plan.search_mode in {"table_explain", "join_candidates", "joinable_tables"}:
            return True
        if self._explicit_table_paths_for_question(question):
            return True
        row_tables = {
            f"{row.get('schema_name')}.{row.get('table_name')}"
            for row in retrieval_details.get("visible_rows", [])
            if row.get("schema_name") and row.get("table_name")
        }
        return len(row_tables) == 1 and bool(row_tables)

    def _should_use_llm_probe_planner(self) -> bool:
        return False

    def _context_detail(self) -> str:
        value = str(self.settings.get("context_detail", "standard") or "standard").strip().lower()
        return value if value in {"minimal", "standard", "rich", "deep"} else "standard"

    def _policy_for_plan(self, plan: SearchPlan) -> SearchPolicy:
        context_detail = self._context_detail()
        allow_vector = self.settings.get("allow_vector_support", "true").lower() == "true" and context_detail != "minimal"
        allow_code = self.settings.get("allow_code_evidence", "true").lower() == "true"
        if plan.question_class == "inventory":
            policy = SearchPolicy(plan.question_class, "live_inventory_first", False, True, True, False, False, "aggregate", "disclose_scope")
        elif plan.question_class == "entity_lookup":
            policy = SearchPolicy(plan.question_class, "lexical_name_first", True, False, True, False, False, "ranked_matches", "suggest_narrow_scope")
        elif plan.question_class == "join_discovery":
            policy = SearchPolicy(
                plan.question_class,
                "verified_fk_then_semantic_join",
                True,
                plan.search_mode == "joinable_tables",
                True,
                allow_vector,
                allow_code,
                "join_candidates",
                "return_confidence_bands",
            )
        elif plan.question_class == "table_understanding":
            policy = SearchPolicy(plan.question_class, "table_context_plus_neighbors", True, False, True, allow_vector, allow_code, "table_summary", "suggest_sync_if_sparse")
        elif plan.question_class == "comparative_reasoning":
            policy = SearchPolicy(plan.question_class, "semantic_then_structural_compare", True, False, True, allow_vector, allow_code, "comparative", "ask_follow_up")
        elif plan.question_class == "semantic_discovery" and plan.target_entity == "table":
            policy = SearchPolicy(plan.question_class, "semantic_table_search", True, False, False, allow_vector, allow_code, "table_matches", "suggest_sync_if_sparse")
        else:
            policy = SearchPolicy(plan.question_class, "semantic_catalog_search", True, False, False, allow_vector, allow_code, "ranked_matches", "suggest_sync_if_sparse")
        policy.answer_shape = self._derive_answer_shape(plan, policy)
        return policy

    def _derive_answer_shape(self, plan: SearchPlan, policy: SearchPolicy) -> str:
        """Pick a presentation shape for the answer.

        Trusts plan.answer_shape when the LLM emitted a valid one. Otherwise
        derives from question_class / search_mode / aggregation hints. Centralised
        so the deterministic formatters, the LLM synth prompt, and the renderer
        all see the same value.
        """
        if plan.answer_shape in _ANSWER_SHAPES:
            return plan.answer_shape
        has_aggregation = plan.aggregation_op in {"max", "min", "top_k", "bottom_k"}
        if plan.search_mode == "schema_inventory":
            if has_aggregation:
                return "single_fact" if plan.aggregation_limit <= 1 else "short_table"
            return "full_table"
        if plan.search_mode in {"count_tables", "list_databases", "list_schemas"}:
            return "single_fact"
        if plan.question_class == "entity_lookup":
            return "single_fact"
        if plan.question_class == "join_discovery":
            return "join_candidates"
        if plan.question_class == "table_understanding":
            return "table_summary"
        if plan.question_class == "semantic_discovery":
            return "ranked_list"
        if plan.question_class == "comparative_reasoning":
            return "prose"
        return "ranked_list"

    def _inventory_db(self) -> DatabaseConnector:
        return self._inventory_db_factory()

    def _known_database_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for profile_name, db_cfg in sorted(self.cfg.db_profiles.items()):
            database_name = db_cfg.database or db_cfg.catalog or db_cfg.project or ""
            key = (profile_name, database_name)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "row_type": "database",
                    "db_profile": profile_name,
                    "database_name": database_name,
                    "backend": db_cfg.backend,
                    "source": "config",
                }
            )
        if not rows:
            rows.append(
                {
                    "row_type": "database",
                    "db_profile": self.db_profile,
                    "database_name": self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or "",
                    "backend": self.cfg.db.backend,
                    "source": "config",
                }
            )
        return rows

    def _live_schema_rows(self) -> list[dict[str, Any]]:
        db = self._inventory_db()
        rows: list[dict[str, Any]] = []
        for schema_name in db.list_schemas():
            try:
                table_count = len(db.list_tables(schema_name))
            except Exception:
                table_count = 0
            rows.append(
                {
                    "row_type": "schema",
                    "database_name": self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or "",
                    "schema_name": schema_name,
                    "table_count": table_count,
                    "source": "live_db",
                }
            )
        return rows

    def _live_table_count(self, schema_name: str | None) -> tuple[int, dict[str, Any]]:
        db = self._inventory_db()
        schemas = db.list_schemas()
        schema_lookup = {str(item).lower(): str(item) for item in schemas}
        if schema_name:
            resolved = schema_lookup.get(str(schema_name).strip().lower())
            if resolved:
                count = len(db.list_tables(resolved))
                return count, {
                    "scope_kind": "schema",
                    "schema_name": resolved,
                    "database_name": self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or "",
                    "scope_assumption": "current_schema",
                }
        total = 0
        for item in schemas:
            try:
                total += len(db.list_tables(item))
            except Exception:
                continue
        return total, {
            "scope_kind": "database",
            "database_name": self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or "",
            "schema_count": len(schemas),
            "scope_assumption": "active_database" if not schema_name else "invalid_current_schema_fallback",
        }

    def _live_joinable_tables(self, table_path: str, limit: int) -> list[dict[str, Any]]:
        if "." not in table_path:
            return []
        schema_name, table_name = table_path.split(".", 1)
        db = self._inventory_db()
        try:
            profile = db.profile_table(schema_name, table_name, sample_size=0)
        except ProfilingError:
            return []
        rows: list[dict[str, Any]] = []
        for fk in profile.foreign_keys:
            rows.append(
                {
                    "row_type": "joinable_table",
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "target_schema_name": str(fk.get("referred_schema") or schema_name),
                    "target_table_name": str(fk.get("referred_table") or ""),
                    "left_column": ", ".join(str(item) for item in (fk.get("constrained_columns") or []) if str(item)),
                    "right_column": ", ".join(str(item) for item in (fk.get("referred_columns") or []) if str(item)),
                    "relationship_type": "foreign_key",
                    "source": "live_db",
                    "score": 10.0,
                    "confidence_band": "verified",
                    "verified_live": True,
                }
            )
        for fk in profile.referenced_by:
            rows.append(
                {
                    "row_type": "joinable_table",
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "target_schema_name": str(fk.get("source_schema") or schema_name),
                    "target_table_name": str(fk.get("source_table") or ""),
                    "left_column": ", ".join(str(item) for item in (fk.get("referred_columns") or []) if str(item)),
                    "right_column": ", ".join(str(item) for item in (fk.get("source_columns") or fk.get("constrained_columns") or []) if str(item)),
                    "relationship_type": "incoming_foreign_key",
                    "source": "live_db",
                    "score": 10.0,
                    "confidence_band": "verified",
                    "verified_live": True,
                }
            )
        seen: set[tuple[str, str, str, str]] = set()
        out: list[dict[str, Any]] = []
        for row in rows:
            key = (
                str(row.get("target_schema_name") or "").lower(),
                str(row.get("target_table_name") or "").lower(),
                str(row.get("relationship_type") or "").lower(),
                f"{row.get('left_column','')}|{row.get('right_column','')}",
            )
            if key in seen or not row.get("target_table_name"):
                continue
            seen.add(key)
            out.append(row)
        return out[:limit]

    def _explicit_table_mentions_for_question(self, question: str) -> list[dict[str, str]]:
        mentions: list[dict[str, str]] = []
        seen: set[str] = set()
        # Inline ``schema.table`` references — strongest possible signal.
        for inline in re.findall(r"\b([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\b", question or ""):
            parts = inline.split(".", 1)
            if len(parts) != 2:
                continue
            path = f"{parts[0]}.{parts[1]}"
            if path.lower() not in seen:
                seen.add(path.lower())
                mentions.append(
                    {
                        "requested": inline,
                        "path": path,
                        "source": "explicit_schema_table",
                        # ``strength`` distinguishes catch-strength so the
                        # alignment guard knows when the user explicitly
                        # called the noun a "table" (high — override LLM
                        # unconditionally) vs. just named a subject in a
                        # "what's the X" form (medium — require catalog or
                        # live confirmation before overriding).
                        "strength": "strong",
                    }
                )
        # Strong-signal tokens: user explicitly says "X table" / "table X" /
        # "X tablo" / "tablo X". User CALLED IT A TABLE, so we don't need
        # extra catalog or live confirmation to trust the routing.
        strong_tokens: list[str] = []
        strong_tokens.extend(
            item
            for item in re.findall(
                r"\b([A-Za-z_][A-Za-z0-9_]{1,127})\s+(?:table|tablo|tablolar|tablosu|tablosunda|tablosuna|tablosundan|tabloları|tablosunu)\b",
                question or "",
                flags=re.IGNORECASE,
            )
        )
        strong_tokens.extend(
            item
            for item in re.findall(
                r"\b(?:table|tables|tablo|tablolar|tablosu)\s+([A-Za-z_][A-Za-z0-9_]{1,127})\b",
                question or "",
                flags=re.IGNORECASE,
            )
        )
        # Weak-signal tokens: user said "what's the X" / "describe X" /
        # "X nedir" — the noun MIGHT be a column or a generic entity, so
        # the alignment guard should require catalog / live confirmation
        # before overriding the LLM's chosen mode.
        weak_tokens: list[str] = []
        subject_patterns = (
            # English: what's/what is/describe/explain/tell me about/show me X
            r"\b(?:what'?s|what is|what are|whats|describe|explain|define|tell\s+me\s+about|"
            r"show\s+me|info\s+(?:on|about)|details?\s+(?:on|about)|definition\s+of|"
            r"meaning\s+of|purpose\s+of)\s+(?:the\s+|a\s+|an\s+)?"
            r"`?([A-Za-z_][A-Za-z0-9_]{1,127})`?\b",
            # English: what does X do/store/contain/mean
            r"\b(?:what\s+does|what\s+do)\s+`?([A-Za-z_][A-Za-z0-9_]{1,127})`?\s+"
            r"(?:do|mean|store|contain|hold|represent)\b",
            # Turkish: <X> nedir / hakkında / hakkinda / ne işe yarar / ne demek
            r"\b`?([A-Za-z_][A-Za-z0-9_]{1,127})`?\s+(?:nedir|ne\s+demek|"
            r"hakk[ıi]nda|ne\s+i[şs]e\s+yarar|ne\s+i[şs]\s+yapar)\b",
            # Turkish: bana <X> hakkında bilgi ver / <X>'i anlat / <X>'i açıkla
            r"\b(?:bana\s+)?(?:bahset|anlat|a[çc][ıi]kla|tan[ıi]t)\s+"
            r"(?:bana\s+)?`?([A-Za-z_][A-Za-z0-9_]{1,127})`?\b",
        )
        for pattern in subject_patterns:
            weak_tokens.extend(
                item
                for item in re.findall(pattern, question or "", flags=re.IGNORECASE)
            )
        table_token_stopwords = {
            "nedir",
            "ne",
            "what",
            "is",
            "are",
            "hangi",
            "hangileri",
            "var",
            "mi",
            "mı",
            "mu",
            "mü",
            # Question/quantifier words that precede "table" without naming one.
            # Without these, a question like "which table has the most rows"
            # is misread as a request for a literal table named "which".
            "which",
            "this",
            "that",
            "these",
            "those",
            "each",
            "every",
            "any",
            "all",
            "some",
            "no",
            "many",
            "much",
            # Superlatives often paired with "table" in aggregations.
            "biggest",
            "largest",
            "smallest",
            "best",
            "worst",
            "top",
            "bottom",
            "first",
            "last",
            "primary",
            "main",
            "the",
            # English verbs/prepositions that commonly follow "table" without
            # being a table name (e.g., "the table has the most rows" -> "has").
            "has",
            "have",
            "had",
            "with",
            "in",
            "on",
            "of",
            "for",
            "by",
            "from",
            "to",
            "into",
            "and",
            "or",
            "but",
            "contains",
            "contain",
            "shows",
            "show",
            "named",
            "called",
            # The new subject-form regex captures the noun that follows
            # "what's the / describe / explain". Filter generic meta-words
            # so e.g. "describe table" does not extract "table" as a name.
            "table",
            "tables",
            "tablo",
            "tablolar",
            # All inflected Turkish forms of "tablo" we already accept in the
            # other regex branch — they must also drop out of subject capture.
            "tablosu",
            "tablosunda",
            "tablosuna",
            "tablosundan",
            "tablosunu",
            "tabloları",
            "tablolarını",
            "tablolardan",
            "column",
            "columns",
            "kolon",
            "kolonlar",
            "field",
            "fields",
            "alan",
            "alanlar",
            "data",
            "info",
            "information",
            "metadata",
            "veri",
            "bilgi",
            "schema",
            "schemas",
            "sema",
            "şema",
            "şemalar",
            "semalar",
            "database",
            "databases",
            "veritaban",
            "veritabani",
            "veritabanı",
            # Generic adjectives that might land after "what's the".
            "most",
            "least",
            "popular",
            "common",
            "single",
            "multiple",
            "total",
            "average",
            "newest",
            "oldest",
            "recent",
            "older",
            "newer",
        }
        strong_tokens = [t for t in strong_tokens if t.lower() not in table_token_stopwords]
        weak_tokens = [t for t in weak_tokens if t.lower() not in table_token_stopwords]
        # Emit strong tokens first so they appear before weak ones — both
        # the alignment guard and ``_resolve_table_targets`` walk this list
        # in order and we want the high-confidence match to win when both
        # branches captured the same noun.
        for tokens, strength, source_qualified, source_unqualified in (
            (strong_tokens, "strong", "explicit_current_schema", "explicit_unqualified_table"),
            (weak_tokens, "weak", "subject_form_current_schema", "subject_form_unqualified"),
        ):
            for token in tokens:
                if self.cfg.current_schema:
                    path = f"{self.cfg.current_schema}.{token}"
                    key = path.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    mentions.append(
                        {"requested": token, "path": path, "source": source_qualified, "strength": strength}
                    )
                else:
                    key = token.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    mentions.append(
                        {"requested": token, "path": "", "source": source_unqualified, "strength": strength}
                    )
        return mentions

    def _explicit_table_paths_for_question(self, question: str) -> list[str]:
        paths: list[str] = []
        seen: set[str] = set()
        for mention in self._explicit_table_mentions_for_question(question):
            path = str(mention.get("path") or "")
            if path and path.lower() not in seen:
                seen.add(path.lower())
                paths.append(path)
        return paths

    def _live_table_exists(self, schema_name: str, table_name: str) -> bool | None:
        """Return exact live existence when cheap metadata APIs are available."""
        db = self._inventory_db()
        target = table_name.lower()
        try:
            if hasattr(db, "list_assets"):
                return any(str(name).lower() == target for name, _kind in db.list_assets(schema_name))
        except Exception:
            pass
        checks = ("list_tables", "list_views", "list_materialized_views")
        found_any_api = False
        for method_name in checks:
            method = getattr(db, method_name, None)
            if not callable(method):
                continue
            found_any_api = True
            try:
                if any(str(name).lower() == target for name in method(schema_name)):
                    return True
            except Exception:
                return None
        return False if found_any_api else None

    def _table_candidate_paths(self, hint: str, *, limit: int = 5) -> list[str]:
        paths: list[str] = []
        seen: set[str] = set()
        for candidate in self.catalog.find_table_candidates(self.db_profile, hint, limit=limit):
            schema_name = str(candidate.get("schema_name") or "")
            table_name = str(candidate.get("table_name") or "")
            path = f"{schema_name}.{table_name}" if schema_name and table_name else ""
            if path and path.lower() not in seen:
                seen.add(path.lower())
                paths.append(path)
        return paths

    def _resolve_table_targets(self, hints: list[str], question: str) -> list[ResolvedTarget]:
        targets: list[ResolvedTarget] = []
        seen: set[str] = set()
        explicit_mentions = self._explicit_table_mentions_for_question(question)
        for mention in explicit_mentions:
            path = str(mention.get("path") or "")
            requested = str(mention.get("requested") or path)
            if "." not in path:
                # Unqualified mention (e.g. user typed "what's the vbrk"
                # without a current_schema). Look up the bare token in the
                # catalog: if it lives in exactly one schema, resolve to it;
                # if it lives in several, surface them as ambiguity
                # candidates instead of silently picking one; if it lives
                # in none, mark as "explicit_table_not_found_live" so the
                # deterministic answer template explains that to the user.
                bare = requested.strip()
                if not bare:
                    continue
                exact_rows = self.catalog.find_tables_by_exact_name(self.db_profile, bare, limit=20)
                exact_paths = [
                    f"{str(row.get('schema_name') or '')}.{str(row.get('table_name') or '')}".strip(".")
                    for row in exact_rows
                    if str(row.get("schema_name") or "") and str(row.get("table_name") or "")
                ]
                if len(exact_paths) == 1:
                    schema_name, table_name = exact_paths[0].split(".", 1)
                    exists = self._live_table_exists(schema_name, table_name)
                    target = ResolvedTarget(
                        requested=requested,
                        resolved_path=exact_paths[0],
                        source=str(mention.get("source") or "explicit_unqualified_table"),
                        is_exact=True,
                        confidence="high" if exists is True else "medium",
                        warnings=[] if exists is True else ["live_table_existence_unknown"],
                        candidates=[],
                    )
                elif len(exact_paths) >= 2:
                    target = ResolvedTarget(
                        requested=requested,
                        resolved_path="",
                        source=str(mention.get("source") or "explicit_unqualified_table"),
                        is_exact=False,
                        confidence="medium",
                        warnings=["ambiguous_unqualified_table"],
                        candidates=exact_paths[:5],
                    )
                else:
                    fuzzy = self._table_candidate_paths(bare, limit=3)
                    target = ResolvedTarget(
                        requested=requested,
                        resolved_path="",
                        source=str(mention.get("source") or "explicit_unqualified_table"),
                        is_exact=False,
                        confidence="low",
                        warnings=["explicit_table_not_found_live"],
                        candidates=fuzzy,
                    )
                key = (target.resolved_path or target.requested).lower()
                if key not in seen:
                    seen.add(key)
                    targets.append(target)
                continue
            schema_name, table_name = path.split(".", 1)
            exists = self._live_table_exists(schema_name, table_name)
            candidates = self._table_candidate_paths(table_name, limit=3)
            if exists is False:
                target = ResolvedTarget(
                    requested=requested,
                    resolved_path="",
                    source=str(mention.get("source") or "explicit"),
                    is_exact=False,
                    confidence="low",
                    warnings=["explicit_table_not_found_live"],
                    candidates=candidates,
                )
            else:
                warnings = [] if exists is True else ["live_table_existence_unknown"]
                target = ResolvedTarget(
                    requested=requested,
                    resolved_path=path,
                    source=str(mention.get("source") or "explicit"),
                    is_exact=True,
                    confidence="high" if exists is True else "medium",
                    warnings=warnings,
                    candidates=[],
                )
            key = (target.resolved_path or target.requested).lower()
            if key not in seen:
                seen.add(key)
                targets.append(target)
        if targets:
            return targets

        for path in self._resolve_table_paths(hints, question):
            if "." not in path or path.lower() in seen:
                continue
            schema_name, table_name = path.split(".", 1)
            exists = self._live_table_exists(schema_name, table_name)
            targets.append(
                ResolvedTarget(
                    requested=table_name,
                    resolved_path=path,
                    source="hint_or_memory",
                    is_exact=exists is not False,
                    confidence="medium" if exists is not False else "low",
                    warnings=[] if exists is True else ["live_table_existence_unknown" if exists is None else "resolved_table_not_found_live"],
                    candidates=[],
                )
            )
            seen.add(path.lower())
        return targets

    def _target_resolution_details(self, targets: list[ResolvedTarget]) -> dict[str, Any]:
        has_resolved = any(bool(target.resolved_path) for target in targets)
        has_unresolved_explicit = any(
            not target.resolved_path
            and (
                "explicit_table_not_found_live" in target.warnings
                or "ambiguous_unqualified_table" in target.warnings
            )
            for target in targets
        )
        has_ambiguous = any(
            not target.resolved_path and "ambiguous_unqualified_table" in target.warnings
            for target in targets
        )
        return {
            "targets": [asdict(target) for target in targets],
            "unresolved_explicit": has_unresolved_explicit and not has_resolved,
            "ambiguous_unqualified": has_ambiguous and not has_resolved,
        }

    def _candidate_table_paths_for_question(self, hints: list[str], question: str) -> list[str]:
        candidates = self._explicit_table_paths_for_question(question)
        seen = {item.lower() for item in candidates}
        for path in self._resolve_table_paths(hints, question):
            if path.lower() not in seen:
                seen.add(path.lower())
                candidates.append(path)
        explicit_tokens = {
            path.split(".", 1)[1].lower()
            for path in candidates
            if "." in path
        }
        tokens = [
            token
            for token in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]{1,127}\b", question or "")
            if token.lower()
            not in {
                "table",
                "tablo",
                "tablosu",
                "tablosunda",
                "column",
                "columns",
                "kolon",
                "kolonlar",
                "comment",
                "commentler",
                "yorum",
                "yorumlar",
            }
            and token.lower() not in explicit_tokens
        ]
        for token in tokens:
            for candidate in self.catalog.find_table_candidates(self.db_profile, token, limit=2):
                path = f"{candidate.get('schema_name', '')}.{candidate.get('table_name', '')}"
                if path == "." or path.lower() in seen:
                    continue
                seen.add(path.lower())
                candidates.append(path)
        return candidates[:6]

    def _resolve_table_paths(self, hints: list[str], question: str) -> list[str]:
        resolved: list[str] = []
        seen: set[str] = set()
        inline = re.findall(r"\b([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\b", question)
        for item in inline + hints + self._last_tables():
            value = str(item or "").strip()
            if not value:
                continue
            if "." in value:
                parts = value.split(".")
                if len(parts) == 2:
                    path = f"{parts[0]}.{parts[1]}"
                    if path not in seen:
                        seen.add(path)
                        resolved.append(path)
                    continue
            for candidate in self.catalog.find_table_candidates(self.db_profile, value, limit=3):
                path = f"{candidate.get('schema_name', '')}.{candidate.get('table_name', '')}"
                if path == "." or path in seen:
                    continue
                seen.add(path)
                resolved.append(path)
                break
        return resolved

    def _candidate_limit(self, question_class: str) -> int:
        configured = str(self.settings.get("max_retrieved_entities", self.settings.get("max_results", "8")) or "8")
        try:
            base = max(1, int(configured))
        except Exception:
            base = 8
        detail = self._context_detail()
        if detail == "minimal":
            return min(base, 6)
        if detail == "rich":
            return max(base, 10)
        if detail == "deep":
            return max(base, 14)
        if question_class == "join_discovery":
            return max(base, 10)
        return base

    def _asks_column_name_listing(self, question: str, plan: SearchPlan) -> bool:
        sample = (question or "").strip().lower()
        asks_column = any(token in sample for token in ("kolon", "kolonlar", "column", "columns", "field", "fields"))
        asks_names = any(token in sample for token in ("isim", "isimleri", "name", "names", "getir", "listele", "list"))
        asks_comment_coverage = any(token in sample for token in ("comment", "comments", "commentler", "yorum", "yorumlar", "girili", "coverage"))
        return asks_column and asks_names and not asks_comment_coverage and plan.question_class == "semantic_discovery" and plan.target_entity in {"column", "unknown", ""}

    def _column_name_lookup_terms(self, question: str, plan: SearchPlan) -> list[str]:
        stopwords = {
            "ile",
            "alakali",
            "alakalı",
            "ilgili",
            "tüm",
            "tum",
            "kolon",
            "kolonlar",
            "kolonu",
            "column",
            "columns",
            "field",
            "fields",
            "isim",
            "isimleri",
            "name",
            "names",
            "getir",
            "listele",
            "list",
            "all",
            "which",
            "hangi",
            "related",
            "with",
        }
        terms: list[str] = []
        for token in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]{1,127}\b", question or ""):
            normalized = token.lower()
            if normalized in stopwords or normalized in terms:
                continue
            terms.append(normalized)
        return terms[:4]

    def _should_plan_live_probe(self, question: str, plan: SearchPlan, table_paths: list[str]) -> bool:
        if not table_paths or plan.search_mode in {"list_databases", "list_schemas", "count_tables", "join_candidates", "joinable_tables"}:
            return False
        if plan.search_mode == "table_explain" or plan.question_class == "table_understanding":
            return True
        if not self._explicit_table_mentions_for_question(question):
            return False
        sample = (question or "").strip().lower()
        metadata_terms = {
            "comment",
            "comments",
            "commentler",
            "yorum",
            "yorumlar",
            "description",
            "descriptions",
            "açıklama",
            "aciklama",
            "metadata",
        }
        short_verification_tokens = {"mi", "mı", "mu", "mü"}
        verification_terms = {
            "var mı",
            "girili",
            "dolu",
            "tüm",
            "tum",
            "all",
            "every",
            "whether",
            "has",
            "have",
            "exists",
            "complete",
            "coverage",
        }
        tokens = set(re.findall(r"\w+", sample, flags=re.UNICODE))
        return (
            any(term in sample for term in metadata_terms)
            or any(term in sample for term in verification_terms)
            or bool(tokens.intersection(short_verification_tokens))
        )

    def _default_live_probe_operations(self, question: str, table_paths: list[str]) -> list[dict[str, str]]:
        if not table_paths:
            return []
        sample = (question or "").strip().lower()
        comments_question = any(
            term in sample
            for term in (
                "comment",
                "comments",
                "commentler",
                "yorum",
                "yorumlar",
                "description",
                "descriptions",
                "açıklama",
                "aciklama",
            )
        )
        operation = "column_comments" if comments_question else "table_metadata_snapshot"
        return [
            {
                "operation": operation,
                "table_path": table_path,
                "rationale": "Default live probe for a table-scoped factual metadata question.",
            }
            for table_path in table_paths[:2]
        ]

    def _merge_probe_operations(self, *groups: list[dict[str, str]]) -> list[dict[str, str]]:
        merged: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for group in groups:
            for op in group:
                operation = str(op.get("operation") or "").strip()
                table_path = str(op.get("table_path") or "").strip()
                key = (operation, table_path)
                if not operation or key in seen:
                    continue
                seen.add(key)
                merged.append(
                    {
                        "operation": operation,
                        "table_path": table_path,
                        "rationale": str(op.get("rationale") or "").strip(),
                    }
                )
        return merged[:4]

    def _plan_live_probe(
        self,
        question: str,
        plan: SearchPlan,
        policy: SearchPolicy,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> tuple[LiveProbePlan, dict[str, Any]]:
        target_resolution = retrieval_details.get("target_resolution") or {}
        resolved_targets = [
            str(item.get("resolved_path") or "")
            for item in target_resolution.get("targets", [])
            if isinstance(item, dict) and str(item.get("resolved_path") or "")
        ]
        if target_resolution.get("unresolved_explicit") and not resolved_targets:
            return LiveProbePlan(False, "Explicit table target was not found in live metadata.", []), {}
        explicit_table_paths = self._explicit_table_paths_for_question(question)
        if plan.search_mode == "table_explain" or plan.question_class == "table_understanding":
            table_paths = resolved_targets or explicit_table_paths or self._candidate_table_paths_for_question(plan.entity_hints, question)
        else:
            table_paths = resolved_targets or explicit_table_paths
            if not table_paths:
                seen_paths: set[str] = set()
                for mention in self._explicit_table_mentions_for_question(question):
                    requested = str(mention.get("requested") or "").strip()
                    if not requested:
                        continue
                    for path in self._table_candidate_paths(requested, limit=2):
                        if path.lower() not in seen_paths:
                            seen_paths.add(path.lower())
                            table_paths.append(path)
        if not self._should_plan_live_probe(question, plan, table_paths):
            return LiveProbePlan(False, "", []), {}
        default_ops = self._default_live_probe_operations(question, explicit_table_paths or table_paths)
        already_verified_comments = any(
            row.get("row_type") == "live_probe" and row.get("probe_operation") == "column_comments"
            for row in rows
        )
        already_verified_snapshot = any(
            row.get("row_type") == "live_probe" and row.get("probe_operation") == "table_metadata_snapshot"
            for row in rows
        )
        ops = default_ops
        if already_verified_comments or already_verified_snapshot:
            ops = []
        elif plan.search_mode == "table_explain" and rows:
            table_row = next((row for row in rows if row.get("row_type") == "table"), None)
            if table_row and table_row.get("effective_description"):
                ops = self._merge_probe_operations(
                    [
                        {
                            "operation": "table_metadata_snapshot",
                            "table_path": path,
                            "rationale": "Verify structural table facts from live metadata before answering.",
                        }
                        for path in (resolved_targets or explicit_table_paths or table_paths)[:1]
                    ]
                )
        merged_ops = self._merge_probe_operations(ops)
        return (
            LiveProbePlan(
                needs_live_probe=bool(merged_ops),
                reason="Deterministic live probe selected for a table-scoped factual metadata question." if merged_ops else "",
                operations=merged_ops,
            ),
            {},
        )

    def _execute_live_probe(self, probe_plan: LiveProbePlan) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if not probe_plan.needs_live_probe:
            return [], {"executed": False, "reason": probe_plan.reason, "operations": []}
        db = self._inventory_db()
        rows: list[dict[str, Any]] = []
        executed: list[dict[str, Any]] = []
        for op in probe_plan.operations:
            operation = op.get("operation", "")
            table_path = op.get("table_path", "")
            if operation in {"column_comments", "table_metadata_snapshot"} and "." in table_path:
                schema_name, table_name = table_path.split(".", 1)
                query_text = (
                    db.column_comments_probe_query(schema_name, table_name)
                    if operation == "column_comments"
                    else db.table_metadata_probe_query(schema_name, table_name)
                )
                snapshot = (
                    {"columns": [{"name": name, "comment": comment or ""} for name, comment in db.get_column_comments(schema_name, table_name).items()], "table_comment": ""}
                    if operation == "column_comments"
                    else db.get_table_metadata_snapshot(schema_name, table_name)
                )
                columns = list(snapshot.get("columns") or [])
                comments = {str(col.get("name") or ""): str(col.get("comment") or "") for col in columns if str(col.get("name") or "")}
                total = len(comments)
                filled = sum(1 for value in comments.values() if str(value or "").strip())
                missing = [name for name, value in comments.items() if not str(value or "").strip()]
                rows.append(
                    {
                        "row_type": "live_probe",
                        "probe_operation": operation,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "metric": "table_metadata_snapshot" if operation == "table_metadata_snapshot" else "column_comment_coverage",
                        "value": filled,
                        "total_columns": total,
                        "commented_columns": filled,
                        "missing_columns": missing,
                        "all_columns_commented": total > 0 and filled == total,
                        "table_comment": snapshot.get("table_comment", ""),
                        "source": "live_db",
                        "verified_live": True,
                        "executed_query": query_text,
                    }
                )
                for col in columns:
                    column_name = str(col.get("name") or "")
                    comment = str(col.get("comment") or "")
                    if not column_name:
                        continue
                    rows.append(
                        {
                            "row_type": "live_column_comment",
                            "schema_name": schema_name,
                            "table_name": table_name,
                            "column_name": column_name,
                            "dtype": col.get("dtype", ""),
                            "nullable": col.get("nullable", ""),
                            "effective_description": comment,
                            "has_comment": bool(comment.strip()),
                            "source": "live_db",
                            "verified_live": True,
                        }
                    )
                executed.append(
                    {
                        "operation": operation,
                        "table_path": table_path,
                        "query": query_text,
                        "rationale": op.get("rationale", ""),
                    }
                )
        return rows, {"executed": bool(executed), "reason": probe_plan.reason, "operations": executed}

    def _retrieve(self, question: str, plan: SearchPlan, policy: SearchPolicy) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        limit = self._candidate_limit(policy.question_class)
        details: dict[str, Any] = {
            "search_mode": plan.search_mode,
            "question_class": plan.question_class,
            "retrieval_policy": policy.retrieval_policy,
            "entity_hints": list(plan.entity_hints),
            "search_queries": list(plan.search_queries),
            "evidence_sources": [],
            "resolved_scope": {},
        }
        if plan.search_mode == "join_candidates":
            table_paths = self._resolve_table_paths(plan.entity_hints, question)
            details["resolved_tables"] = table_paths[:2]
            if len(table_paths) < 2:
                details["ambiguity_flags"] = ["missing_join_table"]
                return [], details
            verified = self.catalog.join_candidates(self.db_profile, table_paths[0], table_paths[1], limit=limit)
            semantic = self.catalog.semantic_join_candidates(self.db_profile, table_paths[0], table_paths[1], limit=limit)
            details["evidence_sources"] = ["catalog_relationships", "semantic_join_inference"]
            rows = self._merge_join_rows(verified, semantic, limit)
            return rows, details
        if plan.search_mode == "joinable_tables":
            table_paths = self._resolve_table_paths(plan.entity_hints, question)
            details["resolved_tables"] = table_paths[:1]
            if not table_paths:
                details["ambiguity_flags"] = ["missing_join_base_table"]
                return [], details
            live_rows = self._live_joinable_tables(table_paths[0], limit=limit)
            catalog_rows = self.catalog.joinable_tables(self.db_profile, table_paths[0], limit=limit)
            semantic_rows = self.catalog.semantic_joinable_tables(self.db_profile, table_paths[0], limit=limit)
            details["display_rows"] = True
            details["evidence_sources"] = ["live_db", "catalog_relationships", "semantic_join_inference"]
            rows = self._merge_joinable_rows(live_rows, catalog_rows, semantic_rows, limit)
            return rows, details
        if plan.search_mode == "table_explain":
            targets = self._resolve_table_targets(plan.entity_hints, question)
            details["target_resolution"] = self._target_resolution_details(targets)
            resolved_paths = [target.resolved_path for target in targets if target.resolved_path]
            details["resolved_tables"] = resolved_paths[:1]
            if targets and not resolved_paths:
                details["ambiguity_flags"] = ["explicit_table_not_found_live"]
                details["evidence_sources"] = ["live_target_resolution"]
                return [], details
            if not resolved_paths:
                details["ambiguity_flags"] = ["missing_table_scope"]
                return [], details
            explained = self.catalog.explain_table(self.db_profile, resolved_paths[0])
            if not explained:
                details["evidence_sources"] = ["live_target_resolution"]
                return [], details
            table_row = dict(explained["table"])
            table_row["row_type"] = "table"
            table_row["relationship_count"] = len(explained["relationships"])
            table_row["column_count"] = len(explained["columns"])
            top_columns = [dict(row) for row in explained["columns"][:limit]]
            for row in top_columns:
                row["row_type"] = "column"
            details["table_context"] = {
                "table": table_row,
                "relationship_count": len(explained["relationships"]),
                "column_count": len(explained["columns"]),
            }
            details["evidence_sources"] = ["effective_metadata", "catalog_relationships"]
            return [table_row, *top_columns], details
        if plan.search_mode == "name_lookup":
            query_text = plan.entity_hints[0] if plan.entity_hints else plan.normalized_question
            rows = self.catalog.name_search_columns(self.db_profile, query_text, limit=limit)
            details["query_text"] = query_text
            details["evidence_sources"] = ["lexical_index"]
            return rows, details
        if plan.search_mode == "list_databases":
            rows = self._known_database_rows()
            details["display_rows"] = False
            details["result_kind"] = "catalog_overview"
            details["evidence_sources"] = ["config", "catalog_entities"]
            return rows, details
        if plan.search_mode == "list_schemas":
            rows = self._live_schema_rows()
            details["database_name"] = self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or ""
            details["display_rows"] = False
            details["result_kind"] = "catalog_overview"
            details["evidence_sources"] = ["live_db"]
            return rows, details
        if plan.search_mode == "count_tables":
            schema_name = ""
            database_name = ""
            schema_lookup = {str(item).lower(): str(item) for item in self._inventory_db().list_schemas()}
            db_lookup = {
                str(row.get("database_name") or "").lower(): str(row.get("database_name") or "")
                for row in self._known_database_rows()
            }
            explicit_scope = False
            for hint in plan.entity_hints:
                normalized = str(hint or "").strip().lower()
                if not normalized or "." in normalized:
                    continue
                if normalized in schema_lookup and not schema_name:
                    schema_name = schema_lookup[normalized]
                    explicit_scope = True
                    continue
                if normalized in db_lookup and not database_name:
                    database_name = db_lookup[normalized]
                    explicit_scope = True
                    continue
                if self.catalog.find_table_candidates(self.db_profile, normalized, limit=1):
                    table_paths = self._resolve_table_paths([normalized], question)
                    if table_paths and not schema_name:
                        schema_name = table_paths[0].split(".", 1)[0]
                        explicit_scope = True
            if not schema_name and self.cfg.current_schema:
                normalized_current = str(self.cfg.current_schema).strip().lower()
                if normalized_current in schema_lookup:
                    schema_name = schema_lookup[normalized_current]
            count, scope_meta = self._live_table_count(schema_name or None)
            if explicit_scope:
                scope_meta["scope_assumption"] = ""
            details.update(scope_meta)
            details["display_rows"] = False
            details["result_kind"] = "aggregate"
            details["evidence_sources"] = ["live_db"]
            return [
                {
                    "row_type": "aggregate",
                    "metric": "table_count",
                    "value": count,
                    "schema_name": scope_meta.get("schema_name", ""),
                    "database_name": scope_meta.get("database_name", ""),
                    "source": "live_db",
                    "verified_live": True,
                }
            ], details
        if plan.search_mode == "schema_inventory":
            schema_name = self.cfg.current_schema or ""
            database_name = self.cfg.db.database or self.cfg.db.catalog or self.cfg.db.project or ""
            try:
                schema_lookup = {str(item).lower(): str(item) for item in self._inventory_db().list_schemas()}
            except Exception:
                schema_lookup = {}
            normalized_current = str(schema_name).strip().lower()
            if normalized_current and normalized_current in schema_lookup:
                schema_name = schema_lookup[normalized_current]
            elif normalized_current:
                schema_name = ""
            for hint in plan.entity_hints:
                normalized = str(hint or "").strip().lower()
                if normalized in schema_lookup:
                    schema_name = schema_lookup[normalized]
                    break
            explorer = SchemaExplorer(self.cfg, self.catalog, db_factory=self._inventory_db_factory)
            inventory = explorer.explore(
                schema_name=schema_name or None,
                database_name=database_name or None,
                limit=max(limit * 20, 500),
            )
            rows = [dict(row) for row in inventory.get("rows", [])]
            summary = dict(inventory.get("summary") or {})
            scope = dict(inventory.get("scope") or {})
            details["display_rows"] = True
            details["result_kind"] = "schema_inventory"
            details["tool"] = "SchemaExplorer"
            details["schema_explorer_summary"] = summary
            details["schema_name"] = str(scope.get("schema_name") or schema_name or "")
            details["database_name"] = str(scope.get("database_name") or database_name or "")
            details["evidence_sources"] = ["schema_explorer", str(inventory.get("source") or "effective_metadata")]
            details["gap_fill_operations"] = int(summary.get("gap_fill_operations") or 0)
            return rows, details
        if plan.search_mode == "compare_entities":
            rows = self.catalog.search_columns(
                self.db_profile,
                plan.normalized_question or question,
                limit=limit,
                entity_hints=plan.entity_hints,
                query_variants=plan.search_queries,
            )
            details["evidence_sources"] = ["effective_metadata", "vector_support"]
            return rows, details
        if self._asks_column_name_listing(question, plan):
            lookup_limit = max(limit, 50)
            merged: list[dict[str, Any]] = []
            seen_ids: set[int] = set()
            for term in self._column_name_lookup_terms(question, plan):
                candidates = self.catalog.name_search_columns(self.db_profile, term, limit=lookup_limit)
                strict = [row for row in candidates if term in str(row.get("column_name") or "").lower()]
                for row in strict or candidates:
                    entity_id = int(row.get("id") or 0)
                    if entity_id and entity_id in seen_ids:
                        continue
                    if entity_id:
                        seen_ids.add(entity_id)
                    merged.append(row)
            if merged:
                details["display_rows"] = True
                details["result_kind"] = "exact_column_name_matches"
                details["evidence_sources"] = ["lexical_index", "effective_metadata"]
                return merged[:lookup_limit], details
        if plan.question_class == "semantic_discovery" and plan.target_entity == "table":
            rows = self.catalog.search_tables(
                self.db_profile,
                plan.normalized_question or question,
                limit=limit,
                entity_hints=plan.entity_hints,
                query_variants=plan.search_queries,
            )
            details["display_rows"] = True
            details["result_kind"] = "table_matches"
            details["evidence_sources"] = ["effective_metadata", "aggregated_column_metadata", "vector_support"]
            return rows, details
        rows = self.catalog.search_columns(
            self.db_profile,
            plan.normalized_question or question,
            limit=limit,
            entity_hints=plan.entity_hints,
            query_variants=plan.search_queries,
        )
        details["evidence_sources"] = ["effective_metadata", "vector_support"]
        return rows, details

    def _merge_join_rows(
        self,
        verified_rows: list[dict[str, Any]],
        semantic_rows: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for row in verified_rows + semantic_rows:
            key = (
                str(row.get("left_column") or "").lower(),
                str(row.get("right_column") or "").lower(),
                str(row.get("relationship_type") or "").lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(dict(row))
        merged.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("relationship_type") or "")))
        return merged[:limit]

    def _merge_joinable_rows(
        self,
        live_rows: list[dict[str, Any]],
        catalog_rows: list[dict[str, Any]],
        semantic_rows: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str, str]] = set()
        for row in live_rows + catalog_rows + semantic_rows:
            key = (
                str(row.get("target_schema_name") or "").lower(),
                str(row.get("target_table_name") or "").lower(),
                str(row.get("left_column") or "").lower(),
                str(row.get("right_column") or "").lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(dict(row))
        merged.sort(
            key=lambda item: (
                {"verified": 0, "high_likelihood": 1, "possible": 2, "weak_hypothesis": 3}.get(str(item.get("confidence_band") or ""), 4),
                -float(item.get("score") or 0.0),
            )
        )
        return merged[:limit]

    def _verify_rows(
        self,
        plan: SearchPlan,
        policy: SearchPolicy,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        verification: dict[str, Any] = {
            "live_verified": False,
            "checks": [],
        }
        if any(row.get("row_type") == "live_probe" and row.get("verified_live") for row in rows):
            verification["live_verified"] = True
            verification["checks"].append("agent_planned_live_metadata_probe")
        if not policy.verify_live:
            return rows, verification
        if plan.question_class == "inventory":
            verification["live_verified"] = True
            verification["checks"].append("inventory_live_truth")
            return rows, verification
        if plan.question_class == "join_discovery":
            verification["checks"].append("join_relationship_classification")
            verified = False
            for row in rows:
                if str(row.get("relationship_type") or "") in {"foreign_key", "incoming_foreign_key"}:
                    row["verified_live"] = bool(row.get("source") == "live_db" or row.get("verified_live"))
                    row["confidence_band"] = "verified"
                    verified = verified or bool(row.get("verified_live"))
                elif not row.get("confidence_band"):
                    score = float(row.get("score") or 0.0)
                    row["confidence_band"] = (
                        "high_likelihood" if score >= 8.0 else "possible" if score >= 6.0 else "weak_hypothesis"
                    )
            verification["live_verified"] = verified
            return rows, verification
        if plan.search_mode == "table_explain" and retrieval_details.get("resolved_tables"):
            verification["checks"].append("table_resolution")
        return rows, verification

    def _rows_for_prompt(self, rows: list[dict[str, Any]], policy: SearchPolicy) -> list[dict[str, Any]]:
        detail = self._context_detail()
        base_cap = {"minimal": 8, "standard": 16, "rich": 24, "deep": 32}.get(detail, 16)
        cap = min(max(base_cap, len(rows)), 40)
        payload: list[dict[str, Any]] = []
        for idx, row in enumerate(rows[:cap], 1):
            item = {
                "result_index": idx,
                "total_results": len(rows),
                "schema": row.get("schema_name", ""),
                "table": row.get("table_name", ""),
                "column": row.get("column_name", ""),
                "target_schema": row.get("target_schema_name", ""),
                "target_table": row.get("target_table_name", ""),
                "left_column": row.get("left_column", ""),
                "right_column": row.get("right_column", ""),
                "source": row.get("effective_source_kind", row.get("source", "")),
                "confidence": row.get("current_confidence", row.get("confidence", "")),
                "rank_score": row.get("rank_score", row.get("score", 0)),
                "description": row.get("effective_description", ""),
                "relationship_type": row.get("relationship_type", ""),
                "row_type": row.get("row_type", "column"),
                "metric": row.get("metric", ""),
                "value": row.get("value", ""),
                "database_name": row.get("database_name", ""),
                "table_count": row.get("table_count", ""),
                "confidence_band": row.get("confidence_band", ""),
                "verified_live": bool(row.get("verified_live")),
                "probe_operation": row.get("probe_operation", ""),
                "total_columns": row.get("total_columns", ""),
                "commented_columns": row.get("commented_columns", ""),
                "missing_columns": row.get("missing_columns", []),
                "all_columns_commented": row.get("all_columns_commented", ""),
                "executed_query": row.get("executed_query", ""),
                "has_comment": row.get("has_comment", ""),
                "dtype": row.get("dtype", ""),
                "nullable": row.get("nullable", ""),
                "table_comment": row.get("table_comment", ""),
            }
            payload.append(item)
        return payload

    def _normalize_rows(self, plan: SearchPlan, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for idx, raw in enumerate(rows):
            row = dict(raw)
            reason = "semantic_match"
            tier = "strong"
            role = "supporting"
            if row.get("row_type") == "schema_explorer_table":
                tier = "strong"
                reason = "schema_explorer_inventory"
                role = "primary" if idx == 0 else "supporting"
            elif bool(row.get("verified_live")) or row.get("row_type") == "live_probe":
                tier = "verified"
                reason = "live_verified"
                role = "primary" if idx == 0 or row.get("row_type") == "live_probe" else "supporting"
            elif str(row.get("relationship_type") or "") in {"foreign_key", "incoming_foreign_key"}:
                tier = "verified"
                reason = "verified_relationship"
                role = "primary"
            elif row.get("vector_only"):
                tier = "weak"
                reason = "vector_only_match"
                role = "diagnostic"
            elif plan.search_mode == "name_lookup":
                tier = "strong" if float(row.get("rank_score") or row.get("match_score") or 0.0) >= 8.0 else "weak"
                reason = "lexical_name_match"
                role = "primary" if idx == 0 else "supporting"
            elif float(row.get("rank_score") or row.get("match_score") or row.get("score") or 0.0) < 4.5:
                tier = "weak"
                reason = "low_score_match"
                role = "diagnostic"
            elif idx == 0:
                role = "primary"
                reason = "top_ranked_match"
            row["evidence_tier"] = tier
            row["answer_role"] = role
            row["match_reason"] = reason
            normalized.append(row)
        return normalized

    def _suppress_rows(self, plan: SearchPlan, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
        visible: list[dict[str, Any]] = []
        suppressed = 0
        for idx, row in enumerate(rows):
            if plan.search_mode == "schema_inventory":
                visible.append(row)
                continue
            if plan.question_class == "join_discovery":
                visible.append(row)
                continue
            if row.get("answer_role") == "diagnostic":
                if row.get("vector_only") and float(row.get("rank_score") or row.get("match_score") or 0.0) < 3.2:
                    suppressed += 1
                    continue
                if idx >= 3:
                    suppressed += 1
                    continue
            visible.append(row)
        return visible, suppressed

    def _deterministic_ranked_answer(
        self,
        question: str,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
        actions: list[SearchActionSuggestion],
    ) -> str | None:
        if not rows:
            return None
        lang = (plan.answer_language or _question_language_hint(question)).lower()
        primary = rows[0]
        if plan.question_class == "join_discovery":
            if plan.search_mode == "joinable_tables":
                target = f"{primary.get('target_schema_name')}.{primary.get('target_table_name')}".strip(".")
                left = str(primary.get("left_column") or "").strip()
                right = str(primary.get("right_column") or "").strip()
                if lang == "turkish":
                    answer = f"En guclu join adayi `{target}`."
                    if left or right:
                        answer += f" Ana kolonlar: `{left}` -> `{right}`."
                    return answer
                answer = f"The strongest join target is `{target}`."
                if left or right:
                    answer += f" Primary columns: `{left}` -> `{right}`."
                return answer
            left = str(primary.get("left_column") or "").strip()
            right = str(primary.get("right_column") or "").strip()
            band = str(primary.get("confidence_band") or "").strip()
            if left or right:
                if lang == "turkish":
                    return f"En guclu join kolonu eslesmesi `{left}` -> `{right}`. Guven seviyesi: `{band or 'unknown'}`."
                return f"The strongest join-column match is `{left}` -> `{right}`. Confidence: `{band or 'unknown'}`."
        if plan.search_mode == "table_explain" and retrieval_details.get("resolved_tables"):
            table_path = retrieval_details["resolved_tables"][0]
            column_count = primary.get("column_count") or retrieval_details.get("table_context", {}).get("column_count")
            if lang == "turkish":
                answer = f"`{table_path}` tablosu icin en guclu aciklama bulundu."
                if column_count:
                    answer += f" Katalogda **{int(column_count)}** kolon gorunuyor."
                return answer
            answer = f"The strongest match is the table `{table_path}`."
            if column_count:
                answer += f" The catalog shows **{int(column_count)}** columns."
            return answer
        if plan.target_entity == "table":
            table_path = ".".join(part for part in (str(primary.get("schema_name") or ""), str(primary.get("table_name") or "")) if part)
            if not table_path:
                return None
            if lang == "turkish":
                answer = f"En guclu tablo eslesmesi `{table_path}`."
                if primary.get("effective_description"):
                    answer += f" Kisa anlam: {str(primary.get('effective_description')).strip()}."
                return answer
            answer = f"The strongest table match is `{table_path}`."
            if primary.get("effective_description"):
                answer += f" Summary: {str(primary.get('effective_description')).strip()}."
            return answer
        column_path = ".".join(
            part
            for part in (
                str(primary.get("schema_name") or ""),
                str(primary.get("table_name") or ""),
                str(primary.get("column_name") or ""),
            )
            if part
        )
        if not column_path:
            return None
        if lang == "turkish":
            answer = f"En guclu kolon eslesmesi `{column_path}`."
            if primary.get("effective_description"):
                answer += f" Kisa anlam: {str(primary.get('effective_description')).strip()}."
            elif actions:
                answer += f" Sonraki adim: {actions[0].reason}"
            return answer
        answer = f"The strongest column match is `{column_path}`."
        if primary.get("effective_description"):
            answer += f" Summary: {str(primary.get('effective_description')).strip()}."
        elif actions:
            answer += f" Next step: {actions[0].reason}"
        return answer

    def _synthesize_answer(
        self,
        question: str,
        plan: SearchPlan,
        policy: SearchPolicy,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
        verification: dict[str, Any],
        actions: list[SearchActionSuggestion],
    ) -> tuple[str, dict[str, Any]]:
        llm = self._llm_provider()
        target_language = plan.answer_language or _question_language_hint(question)
        target_shape = (policy.answer_shape or plan.answer_shape or "").strip() or "ranked_list"
        system = (
            "You are AMX /search, a grounded metadata copilot.\n"
            "Lead with one direct sentence that answers the question. Only add supporting detail if it changes the answer or IS the answer.\n"
            "Match the requested answer_shape:\n"
            "  single_fact   -> one sentence, no list, no table.\n"
            "  short_table   -> one sentence + a 2-5 row markdown table.\n"
            "  full_table    -> one sentence + the inventory markdown table you are given.\n"
            "  ranked_list   -> one sentence + 3-5 bullet matches, one line each.\n"
            "  table_summary -> one sentence + key columns as a markdown table (<=8 rows).\n"
            "  prose         -> 2-4 sentence explanation, no table.\n"
            "For ranked_list answers, the headline sentence should name the 1-3 best-matching tables and weave in WHY each matched, citing specific `matched_columns` from the rows when present (e.g., \"matched on supplier_id and vendor_name\"). Keep the rationale to one sentence; do not duplicate it in the bullets below.\n"
            "Answer only from the retrieved metadata evidence you are given.\n"
            "Treat verified/live evidence as stronger than semantic or vector-only evidence.\n"
            "If evidence is weak or empty (e.g. no direct match), do NOT just say 'I found nothing'. Instead, be constructive: present the closest semantic matches or diagnostic rows provided as related/alternative suggestions.\n"
            "Do not invent table names, joins, counts, or column meanings not present in the evidence.\n"
            "Consider all provided rows. Summarize decisive evidence but also mention helpful related hints if direct answers are missing.\n"
            "When join evidence includes confidence bands, explain them.\n"
            "When scope was assumed, state that assumption.\n"
            "If action suggestions exist, mention only the most relevant one briefly.\n"
            f"Write the final answer naturally in {target_language}."
        )
        # Pre-trim retrieval rows to fit the LLM's input token budget.
        # Without this guard, large result sets exceeded the context
        # window and surfaced as an opaque LLM error to the user; now
        # we drop the lowest-scored rows until the prompt fits and log
        # how many were trimmed so the user can correlate with the
        # `evidence_sources` count in the answer.
        prompt_rows = self._rows_for_prompt(rows, policy)
        base_payload = {
            "question": question,
            "answer_shape": target_shape,
            "plan": asdict(plan),
            "policy": asdict(policy),
            "session_memory": (
                self._memory_summary()
                if self._context_detail() in {"rich", "deep"}
                else self._memory_summary()[-2:]
            ),
            "retrieval_details": retrieval_details,
            "verification": verification,
            "actions": [asdict(item) for item in actions],
        }
        budget = _input_token_budget_for(self.cfg.llm.model)
        trimmed_rows, dropped = _trim_rows_to_token_budget(
            prompt_rows,
            system_text=system,
            base_payload=base_payload,
            budget=budget,
        )
        if dropped:
            log.warning(
                "synthesize_answer: dropped %d lowest-scored row(s) to fit %d-token budget for model=%s",
                dropped,
                budget,
                self.cfg.llm.model,
            )
        user = json.dumps(
            dict(base_payload, rows=trimmed_rows, result_count=len(trimmed_rows)),
            ensure_ascii=True,
        )
        result = llm.chat(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.1,
            max_tokens=1600 if len(rows) > 12 else 1200,
            use_logprobs=False,
        )
        return result.content.strip(), result.usage or {}

    def _deterministic_aggregate_inventory_answer(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> str | None:
        """Headline answer for superlative/top-K questions over schema_inventory.

        Returns None when the request isn't an aggregation or the field isn't
        usable, so the caller can fall back to the broad inventory dump.
        """
        if plan.search_mode != "schema_inventory":
            return None
        op = (plan.aggregation_op or "").lower()
        if op not in {"max", "min", "top_k", "bottom_k"}:
            return None
        field = (plan.aggregation_field or "").lower()
        if field not in {"row_count", "column_count"}:
            # table_count or "" don't index into per-table rows; let dump path handle.
            return None
        usable_rows = [row for row in rows if row.get(field) is not None]
        if not usable_rows:
            return None
        descending = op in {"max", "top_k"}
        ordered = sorted(
            usable_rows,
            key=lambda row: (int(row.get(field) or 0), str(row.get("table_name") or "")),
            reverse=descending,
        )
        limit = plan.aggregation_limit if plan.aggregation_limit > 0 else 1
        limit = min(limit, len(ordered))
        top = ordered[:limit]
        lang = (plan.answer_language or "english").lower()
        schema_name = str(retrieval_details.get("schema_name") or top[0].get("schema_name") or "").strip()
        database_name = str(retrieval_details.get("database_name") or top[0].get("database_name") or "").strip()
        scope_label = (
            f"`{schema_name}`" if schema_name
            else f"`{database_name}`" if database_name
            else ""
        )
        # Single-fact branch: one headline sentence, no table.
        if limit <= 1:
            row = top[0]
            table_name = str(row.get("table_name") or "")
            value = int(row.get(field) or 0)
            column_count = int(row.get("column_count") or 0)
            cluster = str(row.get("semantic_cluster") or "Unclustered")
            if lang == "turkish":
                facet = "satira" if field == "row_count" else "kolona"
                facet_unit = "satir" if field == "row_count" else "kolon"
                superlative = "en cok" if op == "max" else "en az"
                scope_phrase = f"{scope_label} icinde " if scope_label else ""
                value_fmt = f"{value:,}".replace(",", ".")
                # For column_count answers, do not duplicate the column count in trailing context.
                if field == "row_count":
                    return (
                        f"{scope_phrase}{superlative} {facet} sahip tablo `{table_name}`: "
                        f"**{value_fmt} {facet_unit}**, {column_count} kolon (kume: `{cluster}`)."
                    )
                return (
                    f"{scope_phrase}{superlative} {facet} sahip tablo `{table_name}`: "
                    f"**{value_fmt} {facet_unit}** (kume: `{cluster}`)."
                )
            facet = "rows" if field == "row_count" else "columns"
            superlative = "the most" if op == "max" else "the fewest"
            scope_phrase = f" in {scope_label}" if scope_label else ""
            value_fmt = f"{value:,}"
            if field == "row_count":
                return (
                    f"`{table_name}` has {superlative} {facet}{scope_phrase}: "
                    f"**{value_fmt}** rows, {column_count} columns, cluster `{cluster}`."
                )
            return (
                f"`{table_name}` has {superlative} {facet}{scope_phrase}: "
                f"**{value_fmt}** columns, cluster `{cluster}`."
            )
        # Short-table branch: headline + tiny markdown table of top K.
        facet_label_en = "Rows" if field == "row_count" else "Columns"
        facet_label_tr = "Satir" if field == "row_count" else "Kolon"
        if lang == "turkish":
            superlative = "en cok" if op in {"max", "top_k"} else "en az"
            scope_phrase = f" {scope_label} icinde" if scope_label else ""
            facet_word = "satira" if field == "row_count" else "kolona"
            header = f"{superlative} {facet_word} sahip ilk **{limit}** tablo{scope_phrase}:"
        else:
            superlative = "most" if op in {"max", "top_k"} else "fewest"
            scope_phrase = f" in {scope_label}" if scope_label else ""
            facet_word = "rows" if field == "row_count" else "columns"
            header = f"Top **{limit}** tables by {superlative} {facet_word}{scope_phrase}:"
        col_label = facet_label_tr if lang == "turkish" else facet_label_en
        table_lines = [
            f"| Schema | Table | {col_label} | Cluster |",
            "|---|---|---:|---|",
        ]
        for row in top:
            value = int(row.get(field) or 0)
            value_fmt = f"{value:,}".replace(",", ".") if lang == "turkish" else f"{value:,}"
            table_lines.append(
                "| {schema} | {table} | {value} | {cluster} |".format(
                    schema=str(row.get("schema_name") or ""),
                    table=str(row.get("table_name") or ""),
                    value=value_fmt,
                    cluster=str(row.get("semantic_cluster") or "Unclustered"),
                )
            )
        return header + "\n\n" + "\n".join(table_lines)

    def _deterministic_inventory_answer(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> str | None:
        lang = (plan.answer_language or "english").lower()
        if plan.search_mode == "schema_inventory":
            aggregate = self._deterministic_aggregate_inventory_answer(plan, rows, retrieval_details)
            if aggregate is not None:
                return aggregate
            summary = dict(retrieval_details.get("schema_explorer_summary") or {})
            table_count = int(summary.get("table_count") or len(rows))
            total_columns = int(summary.get("total_columns") or sum(int(row.get("column_count") or 0) for row in rows))
            schema_name = str(retrieval_details.get("schema_name") or "").strip()
            database_name = str(retrieval_details.get("database_name") or "").strip()
            scope_label = f"`{schema_name}` schema" if schema_name else f"`{database_name}` database" if database_name else "the active namespace"
            cluster_counts: dict[str, int] = {}
            for row in rows:
                cluster = str(row.get("semantic_cluster") or "Unclustered")
                cluster_counts[cluster] = cluster_counts.get(cluster, 0) + 1
            cluster_summary = ", ".join(
                f"{cluster}: {count}" for cluster, count in sorted(cluster_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
            )
            header = (
                f"{scope_label} icin **{table_count}** tablo ve toplam **{total_columns}** kolon bulundu."
                if lang == "turkish"
                else f"SchemaExplorer found **{table_count}** tables and **{total_columns}** total columns for {scope_label}."
            )
            if cluster_summary:
                header += (
                    f" Semantik kumeler: {cluster_summary}."
                    if lang == "turkish"
                    else f" Semantic clusters: {cluster_summary}."
                )
            table_lines = [
                "| Schema | Table | Columns | Rows | Cluster |",
                "|---|---:|---:|---:|---|",
            ]
            for row in rows[:50]:
                table_lines.append(
                    "| {schema} | {table} | {columns} | {rows_count} | {cluster} |".format(
                        schema=str(row.get("schema_name") or ""),
                        table=str(row.get("table_name") or ""),
                        columns=int(row.get("column_count") or 0),
                        rows_count=int(row.get("row_count") or 0),
                        cluster=str(row.get("semantic_cluster") or "Unclustered"),
                    )
                )
            if len(rows) > 50:
                table_lines.append(f"| ... | {len(rows) - 50} more tables |  |  |  |")
            return header + "\n\n" + "\n".join(table_lines)
        if plan.search_mode == "count_tables" and rows:
            value = int(rows[0].get("value") or 0)
            schema_name = str(retrieval_details.get("schema_name") or rows[0].get("schema_name") or "")
            database_name = str(retrieval_details.get("database_name") or rows[0].get("database_name") or "")
            assumption = str(retrieval_details.get("scope_assumption") or "").strip()
            if lang == "turkish":
                if schema_name:
                    answer = f"`{schema_name}` schema'sinda toplam **{value}** tablo var."
                elif database_name:
                    answer = f"`{database_name}` veritabaninda toplam **{value}** tablo var."
                else:
                    answer = f"Toplam **{value}** tablo var."
                if assumption == "current_schema":
                    answer += f" Acik scope verilmedigi icin aktif schema `{schema_name}` varsayildi."
                elif assumption == "active_database":
                    answer += f" Acik schema verilmedigi icin aktif veritabani/profil `{self.db_profile}` kullanildi."
                return answer
            if schema_name:
                answer = f"There are **{value}** tables in the `{schema_name}` schema."
            elif database_name:
                answer = f"There are **{value}** tables in the `{database_name}` database."
            else:
                answer = f"There are **{value}** tables."
            if assumption == "current_schema":
                answer += f" No explicit scope was given, so the current schema `{schema_name}` was used."
            elif assumption == "active_database":
                answer += f" No explicit schema was given, so the active database/profile `{self.db_profile}` was used."
            return answer
        if plan.search_mode == "list_databases":
            names = [str(row.get("database_name") or "").strip() for row in rows if str(row.get("database_name") or "").strip()]
            if not names:
                return "Bilinen veritabani bulunamadi." if lang == "turkish" else "No known databases were found."
            joined = ", ".join(f"`{name}`" for name in names)
            return (
                f"Su anda su veritabanlari hakkinda bilgi var: {joined}."
                if lang == "turkish"
                else f"I currently have information about these databases: {joined}."
            )
        if plan.search_mode == "list_schemas":
            names = [str(row.get("schema_name") or "").strip() for row in rows if str(row.get("schema_name") or "").strip()]
            if not names:
                return "Schema bulunamadi." if lang == "turkish" else "No schemas were found."
            database_name = str(retrieval_details.get("database_name") or "").strip()
            joined = ", ".join(f"`{name}`" for name in names[:25])
            if lang == "turkish":
                lead = f"`{database_name}` veritabanindaki schemalar" if database_name else "Bulunan schemalar"
                return f"{lead}: {joined}."
            lead = f"Schemas in `{database_name}`" if database_name else "Schemas found"
            return f"{lead}: {joined}."
        return None

    def _deterministic_column_name_answer(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        retrieval_details: dict[str, Any],
    ) -> str | None:
        if retrieval_details.get("result_kind") != "exact_column_name_matches" or not rows:
            return None
        lang = (plan.answer_language or "english").lower()
        names: list[str] = []
        for row in rows:
            schema_name = str(row.get("schema_name") or "")
            table_name = str(row.get("table_name") or "")
            column_name = str(row.get("column_name") or "")
            if not column_name:
                continue
            label = f"{schema_name}.{table_name}.{column_name}" if schema_name and table_name else column_name
            if label not in names:
                names.append(label)
        if not names:
            return None
        joined = ", ".join(f"`{name}`" for name in names)
        if lang == "turkish":
            return f"Kolon adi eslesmelerine gore bulunan kolonlar: {joined}."
        return f"Column-name matches found: {joined}."

    def _deterministic_live_probe_answer(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        live_probe: dict[str, Any],
    ) -> str | None:
        lang = (plan.answer_language or "english").lower()
        snapshot = next(
            (
                row
                for row in rows
                if row.get("row_type") == "live_probe"
                and row.get("probe_operation") == "table_metadata_snapshot"
            ),
            None,
        )
        if snapshot and (plan.search_mode == "table_explain" or plan.question_class == "table_understanding"):
            schema_name = str(snapshot.get("schema_name") or "")
            table_name = str(snapshot.get("table_name") or "")
            table_path = f"{schema_name}.{table_name}" if schema_name and table_name else table_name
            total = int(snapshot.get("total_columns") or 0)
            table_comment = str(snapshot.get("table_comment") or "").strip()
            columns = [
                row
                for row in rows
                if row.get("row_type") == "live_column_comment"
                and row.get("schema_name") == schema_name
                and row.get("table_name") == table_name
            ]
            preview = ", ".join(
                f"`{str(row.get('column_name') or '')}`"
                + (f" ({str(row.get('dtype') or '')})" if str(row.get("dtype") or "") else "")
                for row in columns[:12]
                if str(row.get("column_name") or "")
            )
            if lang == "turkish":
                answer = f"Canli DB metadata'sina gore `{table_path}` tablosunda **{total}** kolon var."
                if table_comment:
                    answer += f" Tablo comment'i: {table_comment}."
                else:
                    answer += " Tablo comment'i live metadata'da bos gorunuyor; bu yuzden is anlamini kesinlestirmiyorum."
                if preview:
                    answer += f" Ilk kolonlar: {preview}."
                return answer
            answer = f"Live DB metadata shows **{total}** columns on `{table_path}`."
            if table_comment:
                answer += f" Table comment: {table_comment}."
            else:
                answer += " The table comment is empty in live metadata, so I am not inferring a business meaning."
            if preview:
                answer += f" First columns: {preview}."
            return answer

        coverage = next(
            (
                row
                for row in rows
                if row.get("row_type") == "live_probe"
                and row.get("probe_operation") == "column_comments"
            ),
            None,
        )
        if not coverage:
            return None
        schema_name = str(coverage.get("schema_name") or "")
        table_name = str(coverage.get("table_name") or "")
        total = int(coverage.get("total_columns") or 0)
        filled = int(coverage.get("commented_columns") or 0)
        missing = [str(item) for item in (coverage.get("missing_columns") or []) if str(item)]
        query_text = str(coverage.get("executed_query") or "")
        all_done = bool(coverage.get("all_columns_commented"))
        table_path = f"{schema_name}.{table_name}" if schema_name and table_name else table_name
        if lang == "turkish":
            if all_done:
                answer = f"Evet. `{table_path}` tablosundaki **{total}/{total}** kolonun comment'i live DB metadata'sinda girili gorunuyor."
            else:
                answer = f"Hayir. `{table_path}` tablosunda **{filled}/{total}** kolonun comment'i girili; **{len(missing)}** kolon eksik."
                if missing:
                    answer += " Eksik kolonlar: " + ", ".join(f"`{name}`" for name in missing[:25]) + "."
            if query_text:
                answer += f" Kontrol icin kullanilan probe: `{query_text}`."
            return answer
        if all_done:
            answer = f"Yes. Live DB metadata shows comments for **{total}/{total}** columns on `{table_path}`."
        else:
            answer = f"No. Live DB metadata shows comments for **{filled}/{total}** columns on `{table_path}`; **{len(missing)}** columns are missing comments."
            if missing:
                answer += " Missing columns: " + ", ".join(f"`{name}`" for name in missing[:25]) + "."
        if query_text:
            answer += f" Probe used: `{query_text}`."
        return answer

    def _deterministic_target_resolution_answer(
        self,
        plan: SearchPlan,
        retrieval_details: dict[str, Any],
        live_probe: dict[str, Any],
    ) -> str | None:
        target_resolution = retrieval_details.get("target_resolution") or {}
        if not target_resolution.get("unresolved_explicit"):
            if live_probe.get("error"):
                if (plan.answer_language or "english").lower() == "turkish":
                    return f"Canli metadata kontrolu calistirilamadi: {live_probe.get('error')}. Bu nedenle kesin cevap vermiyorum."
                return f"The live metadata check could not run: {live_probe.get('error')}. I am not returning a definitive answer."
            return None
        targets = [item for item in target_resolution.get("targets", []) if isinstance(item, dict)]
        target = targets[0] if targets else {}
        requested = str(target.get("requested") or "").strip() or "requested table"
        candidates = [str(item) for item in (target.get("candidates") or []) if str(item)]
        warnings = [str(w) for w in (target.get("warnings") or [])]
        is_ambiguous = "ambiguous_unqualified_table" in warnings
        is_turkish = (plan.answer_language or "english").lower() == "turkish"
        if is_ambiguous:
            if is_turkish:
                if candidates:
                    return (
                        f"`{requested}` adında bir tablo birden fazla şemada mevcut. "
                        "Hangisini kastettiğinizi netleştirir misiniz? Adaylar: "
                        + ", ".join(f"`{item}`" for item in candidates[:5])
                        + "."
                    )
                return f"`{requested}` adı birden fazla şemada geçiyor; lütfen tam yolu belirtin (schema.table)."
            if candidates:
                return (
                    f"`{requested}` exists as a table in more than one schema. "
                    "Could you clarify which one you mean? Candidates: "
                    + ", ".join(f"`{item}`" for item in candidates[:5])
                    + "."
                )
            return (
                f"`{requested}` is the name of more than one table; please qualify it as `schema.table`."
            )
        if is_turkish:
            answer = (
                f"`{requested}` adında bir tablo bu DB profili için katalog veya canlı metadata'da bulunamadı."
            )
            if candidates:
                answer += " Benzer adlar (kesin değil, öneri): " + ", ".join(f"`{item}`" for item in candidates[:5]) + "."
            else:
                answer += " Önce `/search sync` çalıştırarak katalogu güncellemeyi deneyebilirsiniz."
            return answer
        answer = (
            f"I could not find a table named `{requested}` in this DB profile's catalog or live metadata."
        )
        if candidates:
            answer += " Similar names (suggestions, not confirmed): " + ", ".join(f"`{item}`" for item in candidates[:5]) + "."
        else:
            answer += " You may want to run `/search sync` to refresh the catalog first."
        return answer

    def _provenance(self, plan: SearchPlan, rows: list[dict[str, Any]], verification: dict[str, Any]) -> list[str]:
        labels: list[str] = []
        if any((row.get("source") or "") == "live_db" for row in rows):
            labels.append("live database introspection")
        if any((row.get("row_type") or "") == "live_probe" for row in rows):
            labels.append("agent-planned live metadata probe")
        if any((row.get("source") or "") == "config" for row in rows):
            labels.append("configured database profiles")
        if plan.question_class == "inventory":
            labels.append("live structural truth")
        if any((row.get("row_type") or "") == "schema_explorer_table" for row in rows):
            labels.append("schema explorer structural inventory")
        if plan.question_class == "join_discovery":
            labels.append("structural relationships")
            if any(str(row.get("confidence_band") or "") in {"high_likelihood", "possible", "weak_hypothesis"} for row in rows):
                labels.append("semantic join inference")
        if plan.search_mode == "table_explain":
            labels.append("effective table metadata")
        if plan.search_mode == "name_lookup":
            labels.append("exact or fuzzy field-name matching")
        elif plan.question_class in {"semantic_discovery", "comparative_reasoning"}:
            labels.append("effective metadata")
        if any((row.get("source") or "") == "code" for row in rows):
            labels.append("behavioral code evidence")
        if verification.get("live_verified"):
            labels.append("live verification")
        if self.settings.get("allow_vector_support", "true").lower() == "true" and plan.search_mode in {"semantic_concept", "compare_entities"}:
            labels.append("vector support")
        out: list[str] = []
        for label in labels:
            if label not in out:
                out.append(label)
        return out

    def _confidence(self, plan: SearchPlan, rows: list[dict[str, Any]], verification: dict[str, Any], retrieval_details: dict[str, Any] | None = None) -> str:
        retrieval_details = retrieval_details or {}
        if (retrieval_details.get("target_resolution") or {}).get("unresolved_explicit"):
            return "low"
        if plan.out_of_domain or not rows:
            return "low"
        if plan.question_class == "inventory":
            return "high" if verification.get("live_verified") else "medium"
        if any(bool(row.get("verified_live")) for row in rows):
            return "high"
        if plan.search_mode == "table_explain":
            targets = (retrieval_details.get("target_resolution") or {}).get("targets") or []
            if any(isinstance(target, dict) and target.get("is_exact") for target in targets):
                return "medium"
        top = rows[0]
        if top.get("evidence_tier") == "weak":
            return "low"
        if plan.question_class == "join_discovery":
            band = str(top.get("confidence_band") or "")
            if band == "verified":
                return "high"
            if band == "high_likelihood":
                return "medium"
            return "low"
        if retrieval_details.get("resolved_tables"):
            return "medium"
        score = float(top.get("rank_score") or top.get("score") or 0.0)
        if plan.search_mode == "name_lookup":
            return "high" if score >= 8.0 else "medium"
        if score >= 7.5:
            return "high"
        if score >= 4.0:
            return "medium"
        return "low"

    def _action_suggestions(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        ready: bool,
        retrieval_details: dict[str, Any],
        confidence: str,
    ) -> list[SearchActionSuggestion]:
        actions: list[SearchActionSuggestion] = []
        if (retrieval_details.get("target_resolution") or {}).get("unresolved_explicit"):
            return [SearchActionSuggestion("narrow_scope", "Specify the schema.table exactly or switch to the DB/schema where the requested table exists.")]
        if not ready and plan.question_class in {"semantic_discovery", "join_discovery", "table_understanding", "comparative_reasoning"}:
            actions.append(SearchActionSuggestion("sync_catalog", "Search catalog is empty for semantic reasoning."))
        if ready and not rows and plan.question_class in {"semantic_discovery", "entity_lookup", "table_understanding", "comparative_reasoning"}:
            actions.append(SearchActionSuggestion("sync_catalog", "Refresh catalog structure and comments, then retry the question."))
            actions.append(SearchActionSuggestion("refresh_code_evidence", "Refresh code evidence so practical table and column usage can support retrieval."))
        if confidence == "low" and plan.question_class == "join_discovery":
            actions.append(SearchActionSuggestion("refresh_code_evidence", "Code evidence may reveal practical joins that are not explicit in metadata."))
        if confidence == "low" and plan.question_class in {"semantic_discovery", "table_understanding"}:
            resolved = retrieval_details.get("resolved_tables") or []
            if resolved:
                actions.append(SearchActionSuggestion("analyze_table", f"Generate richer metadata for `{resolved[0]}` to improve search quality."))
        if retrieval_details.get("scope_assumption") in {"current_schema", "active_database"}:
            actions.append(SearchActionSuggestion("narrow_scope", "Specify a schema or table to avoid scope assumptions."))
        return actions[:3]

    def ask(self, question: str) -> SearchAnswer:
        clean_question = (question or "").strip()
        question_language = _question_language_hint(clean_question)
        if not clean_question:
            return SearchAnswer(
                intent="unsupported",
                question=question,
                rows=[],
                confidence="low",
                summary="Ask a metadata question inside /search.",
                provenance=[],
                details={"reason": "empty_question"},
            )
        if not self._llm_available():
            return SearchAnswer(
                intent="unsupported",
                question=question,
                rows=[],
                confidence="low",
                summary=(
                    "`/search` tartisma modu aktif bir LLM profili gerektirir. Once `/llm` altinda bir profil tanimlayin."
                    if question_language == "turkish"
                    else "`/search` discussion requires an active LLM profile. Configure one under `/llm` first."
                ),
                provenance=[],
                details={"reason": "no_llm"},
            )

        # Persist the user side of the turn early so the planner sees a fully-
        # formed conversation history (including this question) and so the
        # compaction call below works on a complete picture.
        sid = self._ensure_session_id()
        store = self._ensure_session_store()
        if store is not None and sid:
            try:
                store.append_user_turn(int(sid), question=clean_question)
                store.maybe_compact(
                    int(sid),
                    model=getattr(self.cfg.llm, "model", ""),
                    llm_provider=self._llm_provider() if self._llm_available() else None,
                )
            except Exception as exc:
                log.warning("Chat session bookkeeping failed: %s", exc)

        # Deterministic short-circuits for things the LLM-driven planner
        # tends to fumble: greetings/chitchat (returns clarification by
        # default, which is unfriendly) and meta-queries about the chat
        # itself ("what was my previous question?", "bir önceki sorum
        # neydi"). Both are answered locally before any LLM call.
        chitchat = self._handle_chitchat(clean_question, question_language)
        if chitchat is not None:
            return chitchat
        meta_answer = self._handle_meta_query(clean_question, question_language)
        if meta_answer is not None:
            return meta_answer
        reaffirm = self._handle_followup_reaffirmation(clean_question, question_language)
        if reaffirm is not None:
            return reaffirm

        # Tool-calling agent path (default). The LLM is given real catalog /
        # live-DB tools and decides itself how to answer. Replaces the prior
        # regex-routed Pass1 / alignment / retrieval cascade for everything
        # the deterministic short-circuits don't already handle. Set
        # ``/search config use_tool_agent false`` to fall back to the legacy
        # path during transition.
        use_tool_agent = (
            str(self.settings.get("use_tool_agent", "true") or "true").strip().lower() == "true"
        )
        if use_tool_agent:
            tool_answer = self._answer_via_tool_agent(
                question=question,
                clean_question=clean_question,
                question_language=question_language,
            )
            if tool_answer is not None:
                return tool_answer

        stage_metrics: list[dict[str, Any]] = []
        thought_trace: list[dict[str, str]] = []
        interpretation_usage: dict[str, Any] = {}
        try:
            t0 = time.monotonic()
            with step_spinner("Search Agent: interpreting question"):
                interpretation_mode = str(self.settings.get("interpretation_mode", "balanced") or "balanced").strip().lower()
                if interpretation_mode == "single":
                    llm_plan, interpretation_usage = self._interpret_question_pass1(clean_question)
                else:
                    llm_plan, interpretation_usage = self._interpret_question_balanced(clean_question)
                plan = self._plan_with_overrides(question=clean_question, base=llm_plan, question_language=question_language)
            thought_trace.append(
                {
                    "step": "interpret_question",
                    "observation": f"Route selected: {plan.search_mode}/{plan.question_class} targeting {plan.target_entity}.",
                }
            )
            stage_metrics.append({"stage": "interpretation", "duration_sec": round(time.monotonic() - t0, 4)})
        except Exception as exc:
            return SearchAnswer(
                intent="unsupported",
                question=question,
                rows=[],
                confidence="low",
                summary=f"`/search` could not interpret the question with the active LLM profile: {exc}",
                provenance=[],
                details={"reason": "llm_failure", "stage": "interpretation"},
            )

        should_clarify = (
            str(self.settings.get("clarification_on_low_confidence", "true")).lower() == "true"
            and (plan.needs_clarification or plan.decision_confidence == "low")
        )
        # If the question contains a token the catalog confirms is a real
        # table, we have enough to answer — skip the clarification round
        # rather than asking "could you clarify the exact scope?" right
        # after the user just named a table by exact name.
        if should_clarify and self._catalog_resolvable_subject(clean_question):
            should_clarify = False
            plan = self._align_plan_shape(plan, clean_question)
        if should_clarify:
            clarification = (
                plan.clarification_question.strip()
                or (
                    "Could you clarify the exact scope (database/schema/table) so I can route this correctly?"
                    if (plan.answer_language or "english").lower() == "english"
                    else "Dogru yonlendirme icin tam kapsami (veritabani/sema/tablo) netlestirebilir misiniz?"
                )
            )
            return SearchAnswer(
                intent="clarification",
                question=question,
                rows=[],
                confidence="medium",
                summary=clarification,
                provenance=["llm_interpretation"],
                details={
                    "reason": "clarification_required",
                    "plan": asdict(plan),
                    "question_class": plan.question_class,
                    "tokens": interpretation_usage,
                    "stage_metrics": stage_metrics,
                },
            )

        if plan.out_of_domain or plan.search_mode == "unsupported":
            has_hints = bool(plan.entity_hints) or bool(self._explicit_table_mentions_for_question(clean_question))
            if has_hints:
                # Soften rejection gate: fallback to semantic search if there are valid metadata keywords/hints
                plan = SearchPlan(
                    intent="find_columns",
                    out_of_domain=False,
                    normalized_question=plan.normalized_question,
                    search_mode="semantic_concept",
                    question_class="semantic_discovery",
                    target_entity="unknown",
                    entity_hints=list(plan.entity_hints),
                    search_queries=list(plan.search_queries),
                    needs_typo_recovery=plan.needs_typo_recovery,
                    answer_language=plan.answer_language,
                    ambiguity_flags=list(plan.ambiguity_flags),
                    reason=(plan.reason + "; recovered from unsupported classification").strip("; "),
                )
            else:
                return SearchAnswer(
                    intent="unsupported",
                    question=question,
                    rows=[],
                    confidence="low",
                    summary=(
                        "Bu soru veritabanı veya kod metadata konseptine uymuyor gibi görünüyor. Aramalar tablo, kolon ve yapı detayları üzerinde çalışır."
                        if plan.answer_language == "turkish"
                        else "This request doesn't appear related to database or code metadata. Search applies to tables, columns, and structural details."
                    ),
                    provenance=[],
                    details={
                        "reason": "out_of_domain",
                        "plan": asdict(plan),
                        "question_class": plan.question_class,
                        "tokens": interpretation_usage,
                        "stage_metrics": stage_metrics,
                    },
                )

        if plan.intent == "check_coverage" or plan.search_mode == "check_coverage":
            return SearchAnswer(
                intent="check_coverage",
                question=question,
                rows=[],
                confidence="high",
                summary=(
                    "Veritabanında henüz açıklama girilmemiş (comment eksik olan) tabloları ve kolonları listelemek / denetlemek için sistemin `/analyze pending` komutunu kullanmanız gerekir. Arama (Search) modülü sadece halihazırda var olan metadataları bulmaya odaklanır."
                    if plan.answer_language == "turkish"
                    else "To scan the database for tables or columns missing comments, please use the `/analyze pending` command. Search is optimized for finding actual metadata content, not hunting for blanks."
                ),
                provenance=["system_rules"],
                details={
                    "reason": "redirect_to_analyze",
                    "plan": asdict(plan),
                    "stage_metrics": stage_metrics
                },
            )

        t0 = time.monotonic()
        with step_spinner("Search Agent: planning retrieval"):
            policy = self._policy_for_plan(plan)
            ready, status = self._catalog_ready()
        thought_trace.append(
            {
                "step": "plan_retrieval",
                "observation": f"Retrieval policy: {policy.retrieval_policy}; catalog_ready={ready}.",
            }
        )
        stage_metrics.append({"stage": "planning", "duration_sec": round(time.monotonic() - t0, 4)})

        if policy.requires_catalog and not ready:
            actions = [SearchActionSuggestion("sync_catalog", "Search catalog is empty for semantic reasoning.")]
            return SearchAnswer(
                intent=plan.intent,
                question=question,
                rows=[],
                confidence="low",
                summary=(
                    "`/search` katalogu bos. Metadata sorulari icin once `/search sync` veya `/search rebuild` calistirin."
                    if plan.answer_language == "turkish"
                    else "`/search` catalog is empty. Run `/search sync` or `/search rebuild` before asking metadata questions."
                ),
                provenance=[],
                details={
                    "reason": "catalog_not_ready",
                    "status": status,
                    "plan": asdict(plan),
                    "policy": asdict(policy),
                    "actions": [asdict(item) for item in actions],
                    "ambiguity_flags": list(plan.ambiguity_flags),
                    "evidence_sources": [],
                    "stage_metrics": stage_metrics,
                },
            )

        t0 = time.monotonic()
        with step_spinner("Search Agent: retrieving grounded evidence"):
            rows, retrieval_details = self._retrieve(clean_question, plan, policy)
        if retrieval_details.get("tool") == "SchemaExplorer":
            trace_step = "schema_explorer"
            summary = retrieval_details.get("schema_explorer_summary") or {}
            trace_observation = (
                f"SchemaExplorer returned {summary.get('table_count', len(rows))} table(s), "
                f"{summary.get('total_columns', 0)} column(s), and "
                f"{retrieval_details.get('gap_fill_operations', 0)} gap-fill operation(s)."
            )
        else:
            trace_step = "metadata_query"
            trace_observation = (
                f"Retrieved {len(rows)} candidate row(s) from "
                f"{', '.join(retrieval_details.get('evidence_sources') or []) or 'metadata sources'}."
            )
        thought_trace.append(
            {
                "step": trace_step,
                "observation": trace_observation,
            }
        )
        stage_metrics.append({"stage": "retrieval", "duration_sec": round(time.monotonic() - t0, 4)})
        rows = self._normalize_rows(plan, rows)

        live_probe_usage: dict[str, Any] = {}
        live_probe: dict[str, Any] = {"executed": False, "operations": []}
        try:
            t0 = time.monotonic()
            with step_spinner("Search Agent: checking evidence gaps"):
                probe_plan, live_probe_usage = self._plan_live_probe(clean_question, plan, policy, rows, retrieval_details)
                live_rows, live_probe = self._execute_live_probe(probe_plan)
                retrieval_details["live_probe"] = live_probe
                if live_rows:
                    rows = live_rows + rows
                    retrieval_details.setdefault("evidence_sources", [])
                    if "live_db" not in retrieval_details["evidence_sources"]:
                        retrieval_details["evidence_sources"].append("live_db")
                    if "agent_planned_live_probe" not in retrieval_details["evidence_sources"]:
                        retrieval_details["evidence_sources"].append("agent_planned_live_probe")
                    rows = self._normalize_rows(plan, rows)
            thought_trace.append(
                {
                    "step": "data_peek",
                    "observation": (
                        f"Executed {len(live_probe.get('operations') or [])} live probe operation(s)."
                        if live_probe.get("executed")
                        else "No live probe was required for this question."
                    ),
                }
            )
            stage_metrics.append({"stage": "live_probe", "duration_sec": round(time.monotonic() - t0, 4)})
        except Exception as exc:
            live_probe = {"executed": False, "error": str(exc), "operations": []}
            retrieval_details["live_probe"] = live_probe
            retrieval_details.setdefault("ambiguity_flags", [])
            retrieval_details["ambiguity_flags"].append("live_probe_failed")

        t0 = time.monotonic()
        with step_spinner("Search Agent: verifying high-risk claims"):
            rows, verification = self._verify_rows(plan, policy, rows, retrieval_details)
        thought_trace.append(
            {
                "step": "verify_evidence",
                "observation": f"Verification checks: {', '.join(verification.get('checks') or []) or 'none'}; live_verified={bool(verification.get('live_verified'))}.",
            }
        )
        stage_metrics.append({"stage": "verification", "duration_sec": round(time.monotonic() - t0, 4)})
        rows = self._normalize_rows(plan, rows)
        rows, suppressed_rows_count = self._suppress_rows(plan, rows)
        retrieval_details["visible_rows"] = rows

        answer_usage: dict[str, Any] = {}
        answer_strategy = "deterministic"
        answer_text = None
        allow_language_optimized_deterministic = (plan.answer_language or "english").strip().lower() in {"english", "turkish"}
        if allow_language_optimized_deterministic:
            answer_text = self._deterministic_target_resolution_answer(plan, retrieval_details, live_probe)
        if policy.deterministic_answer and allow_language_optimized_deterministic:
            if answer_text is None:
                answer_text = self._deterministic_inventory_answer(plan, rows, retrieval_details)
            if answer_text is None:
                answer_text = self._deterministic_column_name_answer(plan, rows, retrieval_details)
        if answer_text is None and live_probe.get("executed") and allow_language_optimized_deterministic:
            answer_text = self._deterministic_live_probe_answer(plan, rows, live_probe)
        
        confidence = self._confidence(plan, rows, verification, retrieval_details)
        actions = self._action_suggestions(plan, rows, ready, retrieval_details, confidence)
        executed_actions = list(live_probe.get("operations") or [])
        if allow_language_optimized_deterministic:
            answer_text = answer_text or self._deterministic_ranked_answer(clean_question, plan, rows, retrieval_details, actions)
        
        if answer_text is None:
            try:
                t0 = time.monotonic()
                with step_spinner("Search Agent: synthesizing answer"):
                    answer_text, answer_usage = self._synthesize_answer(clean_question, plan, policy, rows, retrieval_details, verification, actions)
                    answer_strategy = "llm_synthesis"
                stage_metrics.append({"stage": "synthesis", "duration_sec": round(time.monotonic() - t0, 4)})
            except Exception as exc:
                return SearchAnswer(
                    intent=plan.intent,
                    question=question,
                    rows=rows,
                    confidence="low",
                    summary=f"`/search` could not synthesize an answer with the active LLM profile: {exc}",
                    provenance=self._provenance(plan, rows, verification),
                    details={
                        "reason": "llm_failure",
                        "stage": "answer",
                        "plan": asdict(plan),
                        "policy": asdict(policy),
                        "retrieval": retrieval_details,
                        "verification": verification,
                        "stage_metrics": stage_metrics,
                    },
                )

        provenance = self._provenance(plan, rows, verification)
        tables = retrieval_details.get("resolved_tables") or []
        if not tables and self._should_remember_table_scope(plan, retrieval_details, clean_question):
            seen_tables: list[str] = []
            for row in rows:
                schema_name = str(row.get("schema_name") or "")
                table_name = str(row.get("table_name") or "")
                if schema_name and table_name:
                    path = f"{schema_name}.{table_name}"
                    if path not in seen_tables:
                        seen_tables.append(path)
            tables = seen_tables[:4]
        columns = [str(row.get("column_name") or "") for row in rows if row.get("column_name")][:6]
        self._remember(
            {
                "question": clean_question,
                "intent": plan.intent,
                "topic": plan.normalized_question or clean_question,
                "tables": tables if self._should_remember_table_scope(plan, retrieval_details, clean_question) else [],
                "columns": [col for col in columns if col],
                "answer_summary": (answer_text or "")[:1000],
                "confidence": confidence,
                "plan": {
                    "intent": plan.intent,
                    "search_mode": plan.search_mode,
                    "question_class": plan.question_class,
                    "target_entity": plan.target_entity,
                    "normalized_question": plan.normalized_question,
                },
                "tokens": _merge_usage(interpretation_usage, live_probe_usage, answer_usage),
            }
        )
        # Suppress the bottom rich table for shapes whose answer summary already
        # carries the data inline. The shapes that benefit from a separate Rich
        # table (ranked_list, table_summary, join_candidates) keep display_rows=True.
        retrieval_display = bool(retrieval_details.get("display_rows", True))
        shape_wants_rich_table = policy.answer_shape in {"ranked_list", "table_summary", "join_candidates"}
        display_rows = retrieval_display and shape_wants_rich_table
        return SearchAnswer(
            intent=plan.intent,
            question=question,
            rows=rows,
            confidence=confidence,
            summary=answer_text,
            provenance=provenance,
            details={
                "plan": asdict(plan),
                "policy": asdict(policy),
                "question_class": plan.question_class,
                "answer_shape": policy.answer_shape,
                "retrieval": retrieval_details,
                "verification": verification,
                "display_rows": display_rows,
                "scope": self._scope_from_tables(tables),
                "actions": [asdict(item) for item in actions],
                "suggested_actions": [asdict(item) for item in actions],
                "executed_actions": executed_actions,
                "suppressed_rows_count": suppressed_rows_count,
                "answer_strategy": answer_strategy,
                "ambiguity_flags": list(plan.ambiguity_flags) + list(retrieval_details.get("ambiguity_flags") or []),
                "evidence_sources": retrieval_details.get("evidence_sources", []),
                "stage_metrics": stage_metrics,
                "thought_trace": thought_trace,
                "tokens": _merge_usage(interpretation_usage, live_probe_usage, answer_usage),
                "llm_usage": {
                    "interpretation": interpretation_usage,
                    "live_probe": live_probe_usage,
                    "answer": answer_usage,
                },
            },
        )

    def _scope_from_tables(self, tables: list[str]) -> dict[str, list[str]]:
        scope: dict[str, list[str]] = {}
        for path in tables:
            if "." not in path:
                continue
            schema_name, table_name = path.split(".", 1)
            scope.setdefault(schema_name, [])
            if table_name not in scope[schema_name]:
                scope[schema_name].append(table_name)
        return scope
