"""LLM-based answer synthesis + provenance/confidence/actions.

The LLM-call answering tier:

* ``_synthesize_answer`` — final natural-language answer composer.
  Calls the LLM with a strict shape-constrained prompt; respects an
  input-token budget computed by ``_input_token_budget_for`` so wide
  catalogs do not blow the model's context window.
* ``_provenance`` — labels the data sources behind the rows ("live
  database introspection", "catalog FTS match", etc.).
* ``_confidence`` — derives a confidence band from the row mix.
* ``_action_suggestions`` — proposes /search sync / /run-apply etc.
  next-step actions when retrieval is thin.

Reads ``self.cfg`` for the active LLM model + verbosity. Calls
``self._llm_provider()`` for the LLM client.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from amx.search._agent._types import (
    SearchActionSuggestion,
    SearchPlan,
    SearchPolicy,
    _input_token_budget_for,
    _question_language_hint,
    _trim_rows_to_token_budget,
)
from amx.utils.logging import get_logger
from amx.utils.token_tracker import estimate_tokens
from amx.utils.token_tracker import tracker as token_tracker

log = get_logger("search.agent.answering")


class AnsweringMixin:
    """LLM synthesis + provenance/confidence/actions for ``SearchAgent``."""

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
            'For ranked_list answers, the headline sentence should name the 1-3 best-matching tables and weave in WHY each matched, citing specific `matched_columns` from the rows when present (e.g., "matched on supplier_id and vendor_name"). Keep the rationale to one sentence; do not duplicate it in the bullets below.\n'
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
        # ``retrieval_details["visible_rows"]`` carries the FULL row list
        # for diagnostic purposes — the actual rows the LLM sees come
        # through ``prompt_rows`` (capped to 40). Including both in the
        # JSON payload doubles the row footprint and forces the token-
        # budget trim to discard more rows than necessary. Drop the
        # duplicate before estimating the prompt size; the answer panel
        # already renders rows from the ``rows=`` field below.
        payload_details = dict(retrieval_details)
        payload_details.pop("visible_rows", None)
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
            "retrieval_details": payload_details,
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
        synthesize_messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        est = estimate_tokens(synthesize_messages)
        result = llm.chat(
            synthesize_messages,
            temperature=0.1,
            max_tokens=1600 if len(rows) > 12 else 1200,
            use_logprobs=False,
        )
        token_tracker.record_for(
            "answer.synthesize",
            est,
            llm,
            getattr(result, "usage", None),
        )
        return result.content.strip(), result.usage or {}

    def _provenance(
        self, plan: SearchPlan, rows: list[dict[str, Any]], verification: dict[str, Any]
    ) -> list[str]:
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
            if any(
                str(row.get("confidence_band") or "")
                in {"high_likelihood", "possible", "weak_hypothesis"}
                for row in rows
            ):
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
        if self.settings.get(
            "allow_vector_support", "true"
        ).lower() == "true" and plan.search_mode in {"semantic_concept", "compare_entities"}:
            labels.append("vector support")
        out: list[str] = []
        for label in labels:
            if label not in out:
                out.append(label)
        return out

    def _confidence(
        self,
        plan: SearchPlan,
        rows: list[dict[str, Any]],
        verification: dict[str, Any],
        retrieval_details: dict[str, Any] | None = None,
    ) -> str:
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
            return [
                SearchActionSuggestion(
                    "narrow_scope",
                    "Specify the schema.table exactly or switch to the DB/schema where the requested table exists.",
                )
            ]
        if not ready and plan.question_class in {
            "semantic_discovery",
            "join_discovery",
            "table_understanding",
            "comparative_reasoning",
        }:
            actions.append(
                SearchActionSuggestion(
                    "sync_catalog", "Search catalog is empty for semantic reasoning."
                )
            )
        if (
            ready
            and not rows
            and plan.question_class
            in {
                "semantic_discovery",
                "entity_lookup",
                "table_understanding",
                "comparative_reasoning",
            }
        ):
            actions.append(
                SearchActionSuggestion(
                    "sync_catalog",
                    "Refresh catalog structure and comments, then retry the question.",
                )
            )
            actions.append(
                SearchActionSuggestion(
                    "refresh_code_evidence",
                    "Refresh code evidence so practical table and column usage can support retrieval.",
                )
            )
        if confidence == "low" and plan.question_class == "join_discovery":
            actions.append(
                SearchActionSuggestion(
                    "refresh_code_evidence",
                    "Code evidence may reveal practical joins that are not explicit in metadata.",
                )
            )
        if confidence == "low" and plan.question_class in {
            "semantic_discovery",
            "table_understanding",
        }:
            resolved = retrieval_details.get("resolved_tables") or []
            if resolved:
                actions.append(
                    SearchActionSuggestion(
                        "analyze_table",
                        f"Generate richer metadata for `{resolved[0]}` to improve search quality.",
                    )
                )
        if retrieval_details.get("scope_assumption") in {"current_schema", "active_database"}:
            actions.append(
                SearchActionSuggestion(
                    "narrow_scope", "Specify a schema or table to avoid scope assumptions."
                )
            )
        return actions[:3]


__all__ = ["AnsweringMixin"]
