"""LLM-based question planning + plan-shape alignment for ``SearchAgent``.

These methods turn a free-form user question into a structured
:class:`SearchPlan` and a :class:`SearchPolicy` that downstream
retrieval/synthesis steps consume. The cluster covers:

* Two-pass interpretation (``_interpret_question_pass1`` /
  ``_review_question_plan_pass2``) and a balanced wrapper
  (``_interpret_question_balanced``).
* Plan loading from raw LLM payloads (``_plan_from_payload``) and from
  cached overrides (``_plan_with_overrides``).
* Plan-shape alignment (``_align_plan_shape``) that defends against
  hallucinated table names + tightens shape decisions when the catalog
  contradicts the LLM's plan.
* Answer-language detection (``_align_answer_language``).
* Question-class derivation from search mode (``_class_from_mode``).
* Policy + answer-shape derivation (``_policy_for_plan``,
  ``_derive_answer_shape``).

The mixin reads from ``self.cfg``, ``self.catalog`` and ``self.db``; it
calls back into ``self._llm_provider()`` and resolution helpers
(``_resolve_table_paths``, ``_explicit_table_paths_for_question``,
``_target_resolution_details``) which live in sibling mixins.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from amx.search._agent._types import (
    _ANSWER_SHAPES,
    SearchPlan,
    SearchPolicy,
    _json_block,
    _merge_usage,
)
from amx.utils.logging import get_logger

log = get_logger("search.agent.planning")


class PlanningMixin:
    """LLM-based planning + plan-shape alignment methods for ``SearchAgent``."""

    def _plan_with_overrides(
        self, *, question: str, base: SearchPlan | None, question_language: str
    ) -> SearchPlan:
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
        search_mode = (
            str(payload.get("search_mode") or "semantic_concept").strip() or "semantic_concept"
        )
        question_class = str(payload.get("question_class") or "").strip() or self._class_from_mode(
            search_mode
        )
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
            normalized_question=str(payload.get("normalized_question") or question).strip()
            or question,
            search_mode=search_mode,
            question_class=question_class,
            target_entity=resolved_target,
            entity_hints=[
                str(item).strip()
                for item in (payload.get("entity_hints") or [])
                if str(item).strip()
            ],
            search_queries=[
                str(item).strip()
                for item in (payload.get("search_queries") or [])
                if str(item).strip()
            ]
            or [str(payload.get("normalized_question") or question).strip() or question],
            needs_typo_recovery=bool(payload.get("needs_typo_recovery")),
            answer_language=str(payload.get("answer_language") or "").strip(),
            ambiguity_flags=[
                str(item).strip()
                for item in (payload.get("ambiguity_flags") or [])
                if str(item).strip()
            ],
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
            '- aggregation_op: "" | "max" | "min" | "top_k" | "bottom_k" | "count". Detect superlatives and rankings in any language: most/least/highest/lowest/biggest/smallest/largest, top N, first/last, leading, bottom; Turkish: en fazla, en az, en buyuk, en kucuk, en yuksek, en dusuk, en cok, en az; Spanish: el mayor, el menor; etc. Use "count" only for pure how-many questions.\n'
            '- aggregation_field: "" | "row_count" | "column_count" | "table_count". Pick the numeric facet the user is ranking by (rows/satir = row_count; columns/kolon = column_count; tables/tablo = table_count).\n'
            "- aggregation_limit: integer. 1 for superlatives (the X with the most Y); N for top-N/bottom-N; 0 if no aggregation.\n"
            '- answer_shape: pick one of single_fact, short_table, full_table, ranked_list, table_summary, prose. Or "" to let policy derive.\n'
            "  * single_fact: user wants ONE answer (a name, a number, a single ranked entity). Examples: superlatives with limit 1, count_tables, exact name lookups, list_databases when likely small.\n"
            "  * short_table: top-K (limit 2-10), small ranked comparisons, side-by-side of <=10 entities.\n"
            '  * full_table: broad dump-everything inventories ("list all tables in X", "columns per table", "show me everything in X").\n'
            '  * ranked_list: open-ended semantic_discovery ("tables about pricing").\n'
            '  * table_summary: table_understanding / "what is table X".\n'
            "  * prose: why/how/explanatory questions, comparative reasoning without an explicit entity list.\n"
            '  * Leave "" if you genuinely cannot tell.'
        )
        user = json.dumps(
            {
                "question": question,
                "session_memory": memory,
                "current_schema": self.cfg.current_schema or "",
                "current_table": self.cfg.current_table or "",
                "metadata_generation_language": metadata_language,
                "active_db_profile": self.db_profile,
                # 0.11.0: surface the full multi-DB scope so the planner
                # can mention all configured profiles in its answer when
                # the user has opted into ``/use-db a b c`` semantics.
                "active_db_profiles": list(self.db_profiles),
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

    def _review_question_plan_pass2(
        self, question: str, draft: SearchPlan
    ) -> tuple[SearchPlan, dict[str, Any]]:
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
            'Pick answer_shape from: single_fact (one specific answer), short_table (top-K <=10), full_table (broad inventory dump), ranked_list (semantic_discovery), table_summary (table_understanding), prose (explanation), or "" if uncertain.\n'
        )
        user = json.dumps(
            {
                "question": question,
                "draft_plan": asdict(draft),
                "session_memory": self._memory_summary(),
                "current_schema": self.cfg.current_schema or "",
                "current_table": self.cfg.current_table or "",
                "active_db_profile": self.db_profile,
                "active_db_profiles": list(self.db_profiles),
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
                for token in (
                    "join",
                    "link",
                    "relate",
                    "relationship",
                    "bağ",
                    "bag",
                    "ilişk",
                    "iliski",
                )
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
                    reason=(
                        plan.reason
                        + "; rerouted to table_explain because the question names a catalog-confirmed subject"
                    ).strip("; "),
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
        asks_column_word = any(
            token in sample
            for token in ("kolon", "kolonlar", "column", "columns", "field", "fields")
        )
        asks_listing = any(
            token in sample
            for token in (
                "hangi",
                "tüm",
                "tum",
                "list",
                "show",
                "söyle",
                "soyle",
                "tell",
                "getir",
                "listele",
                "bul",
            )
        )
        asks_per_table = any(
            token in sample
            for token in (
                "per table",
                "by table",
                "which table",
                "hangi tabl",
                "tablo baz",
                "her tablo",
            )
        )
        asks_comment_coverage = any(
            token in sample for token in ("comment", "comments", "açıklama", "aciklama", "yorum")
        )
        asks_relationship = any(
            token in sample
            for token in ("join", "link", "relationship", "relate", "connect", "bağ", "bag")
        )
        asks_semantic_table_concept = any(
            token in sample
            for token in (
                "içinde",
                "icinde",
                "alak",
                "related",
                "detail",
                "detay",
                "contain",
                "containing",
                "with",
                "olan",
            )
        )
        if (
            asks_column_word
            and asks_table_word
            and (asks_count or asks_per_table)
            and not asks_comment_coverage
            and not asks_relationship
        ):
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
                reason=(plan.reason + "; routed to SchemaExplorer structural inventory").strip(
                    "; "
                ),
                decision_confidence=plan.decision_confidence,
                needs_clarification=False,
                clarification_question="",
                review_notes=plan.review_notes,
                aggregation_op=plan.aggregation_op,
                aggregation_field=plan.aggregation_field,
                aggregation_limit=plan.aggregation_limit,
                answer_shape=plan.answer_shape,
            )
        if (
            asks_column_word
            and asks_listing
            and plan.search_mode == "table_explain"
            and not self._explicit_table_paths_for_question(question)
        ):
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
                reason=(
                    plan.reason + "; rerouted from table explanation to column discovery"
                ).strip("; "),
                decision_confidence=plan.decision_confidence,
                needs_clarification=plan.needs_clarification,
                clarification_question=plan.clarification_question,
                review_notes=plan.review_notes,
            )
        if (
            plan.search_mode == "count_tables"
            and asks_table_word
            and not asks_count
            and (asks_listing or asks_semantic_table_concept)
        ):
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
        normalized_class = (plan.question_class or "").strip() or self._class_from_mode(
            normalized_mode
        )
        normalized_target = (plan.target_entity or "unknown").strip() or "unknown"
        if normalized_target not in {
            "column",
            "table",
            "schema",
            "database",
            "aggregate",
            "join_path",
            "unknown",
        }:
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

    def _policy_for_plan(self, plan: SearchPlan) -> SearchPolicy:
        context_detail = self._context_detail()
        allow_vector = (
            self.settings.get("allow_vector_support", "true").lower() == "true"
            and context_detail != "minimal"
        )
        allow_code = self.settings.get("allow_code_evidence", "true").lower() == "true"
        if plan.question_class == "inventory":
            policy = SearchPolicy(
                plan.question_class,
                "live_inventory_first",
                False,
                True,
                True,
                False,
                False,
                "aggregate",
                "disclose_scope",
            )
        elif plan.question_class == "entity_lookup":
            policy = SearchPolicy(
                plan.question_class,
                "lexical_name_first",
                True,
                False,
                True,
                False,
                False,
                "ranked_matches",
                "suggest_narrow_scope",
            )
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
            policy = SearchPolicy(
                plan.question_class,
                "table_context_plus_neighbors",
                True,
                False,
                True,
                allow_vector,
                allow_code,
                "table_summary",
                "suggest_sync_if_sparse",
            )
        elif plan.question_class == "comparative_reasoning":
            policy = SearchPolicy(
                plan.question_class,
                "semantic_then_structural_compare",
                True,
                False,
                True,
                allow_vector,
                allow_code,
                "comparative",
                "ask_follow_up",
            )
        elif plan.question_class == "semantic_discovery" and plan.target_entity == "table":
            policy = SearchPolicy(
                plan.question_class,
                "semantic_table_search",
                True,
                False,
                False,
                allow_vector,
                allow_code,
                "table_matches",
                "suggest_sync_if_sparse",
            )
        else:
            policy = SearchPolicy(
                plan.question_class,
                "semantic_catalog_search",
                True,
                False,
                False,
                allow_vector,
                allow_code,
                "ranked_matches",
                "suggest_sync_if_sparse",
            )
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


__all__ = ["PlanningMixin"]
