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
from amx.search._agent import (
    AnsweringMixin,
    DeterministicAnswersMixin,
    PlanningMixin,
    ResolutionMixin,
    RetrievalMixin,
    SessionMemoryMixin,
    ShortCircuitsMixin,
)
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


class SearchAgent(
    SessionMemoryMixin,
    ShortCircuitsMixin,
    AnsweringMixin,
    RetrievalMixin,
    ResolutionMixin,
    PlanningMixin,
    DeterministicAnswersMixin,
):
    """Multi-step metadata reasoning agent for /search.

    Composed of mixin modules under ``amx/search/_agent/`` that group
    related methods (deterministic answers, planning, retrieval, etc.).
    The class itself owns construction + the ``ask()`` orchestrator;
    everything else is delegated to a mixin.
    """

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
